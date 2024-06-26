import os
import json
import httpx
import asyncio
import logging
import pandas as pd
from retry import retry
from datetime import datetime
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO, format=' %(asctime)s - %(levelname)s - %(message)s')
# Create a logger instance
logger = logging.getLogger(__name__)  # __name__ is a common convention
logging.disable(logging.CRITICAL)  # Comment to view logging // Uncomment to disable all logging

load_dotenv()
APP_API_KEY: str = os.getenv("APP_API_KEY", "your app-api-key was not imported")

HOSTNAME: str = "app.OneTrust.com/api"
VERSION: str = "v2"
OT_HEADERS: dict = {"Authorization": f"Bearer {APP_API_KEY}",
                    "accept": "application/json", }

DEFAULT_OWNER_ID: str = "owner_id_not_set"
DEFAULT_CATEGORY: str = "category_not_set"

SHAREPOINT_PATH_MACOS = "~/Library/CloudStorage/OneDrive-SharedLibraries-DBInc/OneTrust"
SHAREPOINT_PATH_WINDOWS = r"~\OneDrive - DBInc\OneTrust"

unique_filename: bool = False  # Change this value to have a timestamped filename
timeout: float = 30.0  # Chang this value to change the GET request timeouts
fetch_individual_users: bool = True  # If False, script will fetch ALL users (takes more time)


@retry(tries=3, delay=1, backoff=2, logger=logger)  # 3 retries, 1s initial delay, doubling backoff
async def get_http_response(url: str, headers: dict, client: httpx.AsyncClient) -> httpx.Response:
    """
    Asynchronously retrieves an HTTP response from a given URL with retry logic.

    This function uses `httpx.AsyncClient` to make a GET request to the specified URL. It includes
    automatic retry functionality to handle common network errors, retrying up to 3 times with exponential backoff.

    Args:
        url (str): The URL to request.
        headers (dict): A dictionary of headers to include in the request.
        client (httpx.AsyncClient): The AsyncClient instance to use for making the request.

    Returns:
        httpx.Response: The HTTP response object returned by the server if the request is successful.

    Raises:
        httpx.ConnectTimeout: If a connection cannot be established to the server.
        httpx.ReadTimeout: If the server does not send data within the timeout defined within the AsyncClient.
        httpx.TimeoutException: If the entire request (connect + read) takes longer than the timeout defined
                                within the AsyncClient.
        httpx.NetworkError: For general network-related errors.
        httpx.HTTPError: For other HTTP errors (e.g., status codes 4xx and 5xx).
    """
    logging.info(f"Sending request to {url}")
    try:
        # async with httpx.AsyncClient(timeout=timeout) as client:
        response = await client.get(url, headers=headers)
        logging.info(f"Received response from URL: {url}, status code: {response.status_code}")
        return response
    except (httpx.ConnectTimeout,
            httpx.ReadTimeout,
            httpx.TimeoutException,
            httpx.NetworkError,
            httpx.HTTPError,
            ) as e:
        logging.error(f"Request to {url} failed: {type(e).__name__}: {e}")
        raise


def handle_response_status(response: httpx.Response) -> None:
    """
    Checks the status of an HTTP response and logs a corresponding message.
    Raises an exception for unsuccessful responses.

    This function handles both successful (2xx) and unsuccessful (4xx, 5xx) HTTP responses.
    For successful responses, it logs an informative message indicating the specific status code.
    For unsuccessful responses, it raises an exception with a descriptive error message, including details
    from the response content.

    Args:
        response (httpx.Response): The HTTP response object.

    Raises:
        httpx.HTTPStatusError: If the response status code is not in the 2xx range, along with a detailed error message.
    """
    try:
        response.raise_for_status()  # Raise an exception for any non-2xx status code

        # Handle specific success codes (200, 201, 202, 204)
        success_messages = {
            200: "OK - The request has been successfully processed",
            201: "Created - The request has been fulfilled and has resulted in one or more new resources being created",
            202: "Accepted - The request has been accepted for processing, but the processing has not been completed",
            204: "No Content - The request has been successfully fulfilled by the server and "
                 "there is no additional content to send in the response payload body.",
        }

        message = success_messages.get(response.status_code)
        if message:
            # Process the successful response data here (common logic)
            # Provide more descriptive messages based on specific status codes
            logging.info(f"Status Code: {response.status_code} => {message}")

    except httpx.HTTPStatusError as exc:  # Catch HTTP errors
        logging.error(f"An HTTP error occurred: {exc}")
        logging.error(f"HTTP Status Code: {exc.response.status_code}")

        # Simplified error handling using a dictionary
        error_messages = {
            # 4xx Client Error Codes indicate that there was an error in either the request or the data.
            # The following are the most common 4xx status codes returned by OneTrust web servers:
            400: "Bad Request - Invalid parameter passed.",
            401: "Unauthorized - Invalid credentials (Please check your API token) or URI.",
            403: "Forbidden - Operation not allowed. You do not have permission to access this resource.",
            404: "Not Found - Resource does not exist or cannot be found.",
            409: "Conflict - Resource already exists.",
            429: "Too Many Requests - Rate limit exceeded.",
            # 5xx Server Error Codes indicate that there was an internal error with the server.
            # The following are the most common 5xx status codes returned by OneTrust web servers:
            500: "Internal Server Error - Error within the API.",
            503: "Service Unavailable - System is unavailable. Try again later.",
        }

        message = error_messages.get(exc.response.status_code, "Unexpected HTTP Error")
        # 401 Unauthorized suggests invalid credentials, it's good practice not to log the API token in plain text.
        logging.error(f"{message} (Response: "
                      f"{exc.response.text if exc.response.status_code not in [401] else '<sensitive_data_removed>'})")
        if message == "Unexpected HTTP Error":
            logging.error(response.text)
            logging.info("Check https://developer.onetrust.com/onetrust/reference/quick-start-guide")


def log_rate_limit_headers(response: httpx.Response) -> int:
    """
    Logs rate limit headers from an HTTP 429 response.

    This function examines the provided response object and extracts specific
    rate limit headers, if they exist. The logged headers typically provide information
    about rate limiting status and retry instructions.

    Args:
        response (httpx.Response): The HTTP response object (ideally a 429 Too Many Requests response).
    Returns:
        int: The value of the "Retry-After" header, or 1 if the header is not present. This indicates the
             number of seconds to wait before retrying the request.
    """
    headers_to_log = ["Retry-After",
                      "ot-period",
                      "ot-ratelimit-event-id",
                      "ot-requests-allowed",
                      "ot-request-made",
                      ]
    retry_after = 1  # Default to 1 second if Retry-After is not present

    for header in headers_to_log:
        value = response.headers.get(header)
        if value:
            if header == "Retry-After":
                retry_after = int(value)
            logging.info(f"{header}: {value}")

    return retry_after


def get_normalized_json_response_df(response: httpx.Response) -> pd.DataFrame:
    """
    Fetches JSON data from a given URL, normalizes it, and returns a DataFrame.

    Args:
        response: The http response object.

    Returns:
        pd.DataFrame: A DataFrame containing the normalized JSON data.
    """
    parsed_response = json.loads(response.text)  # Parse the JSON response
    return pd.json_normalize(parsed_response)  # Normalize and return as DataFrame


async def get_microservice_df(microservice: str) -> pd.DataFrame:
    """
    Asynchronously retrieves paginated data from the OneTrust API for a specified microservice
    ("scim" for a complete list of users, or "inventory" for a complete list of vendors).

    This asynchronous function fetches data from the OneTrust API in paginated requests,
    handling pagination and potential rate limiting,
    normalizes the JSON response into a Pandas DataFrame.

    Args:
        microservice (str): The name of the microservice. Must be one of the following:
            * "scim": Retrieves user data.
            * "inventory": Retrieves vendor data.

    Returns:
        pd.DataFrame: A DataFrame containing the retrieved data in a normalized format.

    Raises:
        KeyError: If an invalid microservice name is provided.
        httpx.ConnectTimeout: If a connection to the server cannot be established within the given timeout.
        httpx.ReadTimeout: If the server does not send data within the given timeout.
        httpx.TimeoutException: If the entire request (connect + read) takes longer than the given timeout.
        httpx.NetworkError: For general network-related errors.
        httpx.HTTPError: For other HTTP errors (e.g., status codes 4xx and 5xx).
    """
    if microservice not in ["scim", "inventory"]:
        raise KeyError(f"Invalid microservice provided: {microservice}. Choose 'scim' or 'inventory'")

    logging.info(f"{microservice.capitalize()} list".center(20, '='))

    microservice_col_name = {"scim": "Resources", "inventory": "data"}

    if microservice == "scim":
        url_t = f"https://{HOSTNAME}/{microservice}/{VERSION}/Users?startIndex={{current_index}}&count={{count}}"
        count_parameter: str = "itemsPerPage"
        initial_index = 1
        page_size = 500  # Fetch users in page groups of 500
    else:  # microservice == "inventory"
        url_t = f"https://{HOSTNAME}/{microservice}/{VERSION}/inventories/vendors?page={{current_index}}&size={{count}}"
        count_parameter: str = "meta.page.size"
        initial_index = 0
        page_size = 50  # Fetch vendors in page groups of 50 (It seems this is the max OneTrust allows)

    df_all_fetched = pd.DataFrame()
    current_index = initial_index
    has_more_pages = True

    async with httpx.AsyncClient(timeout=httpx.Timeout(timeout=timeout)) as client:
        while has_more_pages:
            url = url_t.format(current_index=current_index, count=page_size)
            # Retry logic for handling timeouts
            response = await get_http_response(url, OT_HEADERS, client)  # Passing the client object
            while response.status_code == 429:
                logging.warning("Rate limit exceeded. Retrying after delay...")
                retry_after = log_rate_limit_headers(response)  # Default to 1 second if not provided
                await asyncio.sleep(retry_after)  # Sleep for the time indicated in the response header before retrying
                response = await get_http_response(url, OT_HEADERS, client)  # Retry the request

            handle_response_status(response)  # Check for errors and raise exceptions if needed.

            df_from_normalized_json = get_normalized_json_response_df(response)
            df_all_fetched = pd.concat([df_all_fetched, df_from_normalized_json], ignore_index=True)

            if microservice == "scim":
                items_fetched = df_from_normalized_json[count_parameter][0]
                logging.info(
                    f"Fetched data from {url} | items from {current_index} to {current_index + items_fetched - 1}")
                has_more_pages = items_fetched >= page_size  # Check if there are more pages
                current_index += items_fetched
            else:  # microservice == "inventory"
                total_pages_parameter: str = 'meta.page.totalPages'
                total_elements_parameter: str = 'meta.page.totalElements'
                start_entry = current_index * page_size + 1
                total_elements = df_from_normalized_json[total_elements_parameter][0]
                end_entry = min(page_size * (current_index + 1), total_elements)
                logging.info(f"Fetched data from {url} | items from {start_entry} to {end_entry}")
                current_index += 1
                total_pages = df_from_normalized_json[total_pages_parameter][0]
                has_more_pages = current_index < total_pages

        logging.info("".center(20, '='))

    df_temp = df_all_fetched.explode(microservice_col_name[microservice], ignore_index=True)
    microservice_df = pd.json_normalize(df_temp[microservice_col_name[microservice]])
    return microservice_df


@retry(tries=3, delay=1, backoff=2, logger=logger)  # Retry logic for network issues
async def fetch_user_name(client: httpx.AsyncClient, user_id: str) -> str | None:
    """
    Asynchronously fetches the userName associated with a given userID from the OneTrust API.

    Args:
        client (httpx.AsyncClient): An initialized httpx AsyncClient instance for making API requests.
        user_id (str): The unique identifier of the user whose userName is to be retrieved.

    Returns:
        str | None: The userName of the user if found, otherwise None.

    Raises:
        httpx.ConnectTimeout: If a connection to the server cannot be established.
        httpx.ReadTimeout: If the server does not send data within the timeout defined within the AsyncClient.
        httpx.TimeoutException: If the entire request (connect + read) takes longer than the timeout defined
                                within the AsyncClient.
        httpx.NetworkError: For general network-related errors.
        httpx.HTTPError: For other HTTP errors (e.g., status codes 4xx and 5xx).
    """
    url = f"https://{HOSTNAME}/scim/{VERSION}/Users/{user_id}"
    try:
        response = await client.get(url, headers=OT_HEADERS)
        handle_response_status(response)
        df_from_normalized_json = get_normalized_json_response_df(response)
        return df_from_normalized_json['userName'].values[0]
    except (KeyError, IndexError):
        logging.warning(f"userName not found for userID: {user_id}")
        return None


def process_dataframes(df_users: pd.DataFrame, df_vendors: pd.DataFrame) -> pd.DataFrame:
    """
    Processes and merges vendor and user dataframes, preparing them for further analysis.

    1. Extracts relevant data from the `df_vendors` DataFrame:
        - Sets default values category fields.
        - Filters to include only active vendors in the 'Live' workflow stage.
    2. Processes `df_users`:
        - Converts usernames (emails) to lowercase.
    3. Merges the dataframes:
        - Performs an inner join on 'owner' (vendors) and 'id' (users) columns.
    4. Selects and reorders columns:
        - Retains a subset of columns.
        - Arranges them in a specific order.
    5. Renames columns:
        - Provides more descriptive and informative column names.

    Args:
        df_users (pd.DataFrame): A Pandas DataFrame containing user data.
        df_vendors (pd.DataFrame): A Pandas DataFrame containing vendor data.

    Returns:
        pd.DataFrame: The processed and merged DataFrame, ready for analysis.
    """
    # Extracting the business owner for each vendor entry
    df_vendors['owner'] = df_vendors['owner'].apply(lambda x: x[0]['id'])
    # Extracting the Category value for each vendor entry
    # If no category has been set, it will display "category_not_set"
    df_vendors['customField1000'] = df_vendors['customField1000'].fillna(
        {i: [{"value": DEFAULT_CATEGORY}] for i in df_vendors.index}
    )
    df_vendors['customField1000'] = df_vendors['customField1000'].apply(lambda x: x[0]['value'])

    # Filtering the vendors data frame to only consider entries that are active and that Live
    df_vendors_filtered = df_vendors[
        (df_vendors['status.key'] == 'active') & (df_vendors['workflowStage.stage.value'] == 'Live')
        ]

    # Have the userName (emails) values be all lower case
    df_users['userName'] = df_users['userName'].apply(lambda x: x.lower())
    # Inner join of the vendors and users dataframe on the owner and id columns respectively
    df_merged = pd.merge(df_vendors_filtered, df_users, left_on='owner', right_on='id')

    # Rearrange columns
    new_column_order = ['number',
                        'name',
                        'userName',
                        'organization.value',
                        'description',
                        'customField1000',
                        'customField1001',
                        ]
    df = (
        df_merged[new_column_order]
        .rename(  # rename columns
            columns={
                "number": "ID",
                "name": "Vendor Name",
                "userName": "Business Owner",
                "organization.value": "Organization",
                "description": "Description",
                "customField1000": "Vendor Category",
                "customField1001": "Website",
            }
        )
    )

    df.index = range(1, len(df) + 1)  # Setting 1-based index instead of the default 0-based index
    print(f"There are {df.shape[0]} approved vendors.")  # get number of vendors approved
    return df


def set_filename(is_unique: bool) -> str:
    """
    Generates a filename for a file of approved vendors.

    Args:
        is_unique (bool): If True, appends a timestamp to the filename to ensure uniqueness.

    Returns:
        str: The generated filename string.

        * If `is_unique` is True, the format is "Approved_Vendors_YYYYMMDDHHMMSS".
        * If `is_unique` is False, the format is "Approved_Vendors".
    """
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    if is_unique:
        return f"Approved_Vendors_{timestamp}"
    else:
        return f"Approved_Vendors"


def set_path(win_sp_path: str, mac_sp_path: str) -> str:
    """
    Determines the appropriate path for saving files based on the operating system and SharePoint sync status.

    This function checks whether the specified SharePoint library (`win_sp_path` for Windows or `mac_sp_path`
    for macOS or Linux) is synchronized. If so, it returns the path to that library. Otherwise, it returns
    the user's Downloads directory.

    Args:
        win_sp_path (str): The path to the SharePoint library for Windows users.
        mac_sp_path (str): The path to the SharePoint library for macOS users.

    Returns:
        str: The final path to use for saving files, which will be either the SharePoint library path or
             the user's Downloads directory, depending on sync status and operating system.

    Raises:
        SystemError: If the operating system is not Windows or macOS/Linux.
    """
    # Validating that Sharepoint library is synchronized (base_dir)
    if os.name == 'posix':  # macOS and Linux
        sharepoint_path = os.path.expanduser(mac_sp_path)
    elif os.name == 'nt':  # Windows
        sharepoint_path = os.path.expanduser(win_sp_path)
    else:
        raise SystemError("Unsupported operating system")
    # Checking if the user is synchronizing the sharepoint folder to save the file
    # Otherwise, we will save the files in the user's Download's folder
    if os.path.exists(sharepoint_path):
        return sharepoint_path

    downloads_path = os.path.join(os.path.expanduser("~"), "Downloads")
    return downloads_path


def save_styled_dataframe_as_html(df: pd.DataFrame, save_dir: str, name: str) -> None:
    """
    Creates and saves a styled HTML table from a pandas DataFrame.

    This function converts the input DataFrame into an HTML table and applies CSS styling to improve
    its appearance. The styled table is then saved as an HTML file in the specified directory.

    Args:
        df (pd.DataFrame): The Pandas DataFrame containing the data to be displayed in the table.
        save_dir (str): The directory where the HTML file will be saved.
        name (str): The name of the HTML file (without the '.html' extension).
    """
    html = df.to_html(justify='left',  # How to justify the column labels.
                      render_links=True,  # Convert URLs to HTML links
                      index=False,  # Whether to print index (row) labels.
                      )  # save it as html file
    # Adding CSS to the HTML string
    html = """
    <html>
    <head>
        <style>
            table {
                font-family: Arial, Helvetica, sans-serif;
                font-size: 15px;
                width: 100%;
                border: 0;  /* Remove borders */
            }
            th, td {
                padding: 7px;  /* Add padding */
                border: none;  /* Remove cell borders */
            }
            thead th {
                background-color: #808080;  /* Header background color */
                color: white;  /* Set text color to white for better contrast */
            }
            tr:nth-child(even) {
                background-color: #f2f2f2;  /* Even row background color */
            }
            tr:nth-child(odd) {
                background-color: #ffffff;  /* Odd row background color */
            }
        </style>
    </head>
    <body>
        """ + html + """
    </body>
    </html>
    """
    # Writing the HTML string to a file
    with open(os.path.join(save_dir, f"{name}.html"), "w") as f:
        f.write(html)


async def main() -> None:
    df_vendors_data = await get_microservice_df("inventory")
    # Sets default values for owner field if it finds NaN values
    df_vendors_data['owner'] = df_vendors_data['owner'].fillna(
        {i: [{"id": DEFAULT_OWNER_ID}] for i in df_vendors_data.index}
    )
    if fetch_individual_users:
        unique_owners = df_vendors_data['owner'].apply(lambda x: x[0]['id']).unique()
        filtered_owners = [owner for owner in unique_owners if owner != DEFAULT_OWNER_ID]

        async with httpx.AsyncClient(timeout=httpx.Timeout(timeout=timeout)) as client:
            tasks = [fetch_user_name(client, user_id) for user_id in filtered_owners]
            user_data = await asyncio.gather(*tasks)

        # Create df_users_data DataFrame
        df_users_data = pd.DataFrame({
            'id': filtered_owners,
            'userName': user_data
        })
    else:
        df_users_data = await get_microservice_df("scim")

    df_approved_vendors = process_dataframes(df_users_data, df_vendors_data)

    path = set_path(SHAREPOINT_PATH_WINDOWS, SHAREPOINT_PATH_MACOS)  # set the path
    filename = set_filename(unique_filename)  # set the file name

    df_approved_vendors.to_excel(os.path.join(path, f"{filename}.xlsx"),
                                 sheet_name='ApprovedVendors',
                                 index=True,
                                 )  # save it as Excel file

    save_styled_dataframe_as_html(df_approved_vendors, path, filename)

    print("Done!")


if __name__ == '__main__':
    asyncio.run(main())
