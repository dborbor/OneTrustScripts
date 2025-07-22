import json
import httpx
import logging
import asyncio
import pandas as pd
from retry import retry
from utils.data_utils import process_dataframes
from config.config_utils import ONETRUST_HOSTNAME, ONETRUST_VERSION, ONETRUST_HEADERS, TIMEOUT
from config.config_utils import DEFAULT_OWNER_ID

# Configuring Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# Create a logger instance
logger = logging.getLogger(__name__)
# logging.disable(logging.CRITICAL)  # Comment to view logging // Uncomment to disable all logging


async def get_microservice_df(microservice: str, inventory_type: str = "vendors") -> pd.DataFrame:
    """
    Asynchronously retrieves paginated data from the OneTrust API for a specified microservice.

    This function fetches data from the OneTrust API in paginated requests, handling
    pagination and potential rate limiting. It supports retrieving user data ("scim"
    microservice) and various types of inventory data (e.g., "vendors", "assessments")
    using the "inventory" microservice.

    The retrieved data is normalized from the JSON response into a Pandas DataFrame.

    Args:
        microservice (str): The name of the microservice. Must be one of:
                            "scim": Retrieves user data.
                            "inventory": Retrieves inventory data.
        inventory_type (str): The type of inventory to retrieve (e.g., "vendors", "assessments").
                                Required only when `microservice` is "inventory".

    Returns:
        pd.DataFrame: A DataFrame containing the normalized data.

    Raises:
        KeyError: If an invalid microservice name is provided.
        ValueError: If `microservice` is "inventory" and `inventory_type` is not provided.
        httpx.ConnectTimeout: If a connection to the server cannot be established within the timeout.
        httpx.ReadTimeout: If the server does not send data within the timeout.
        httpx.TimeoutException: If the entire request (connect plus read) takes longer than the timeout.
        httpx.NetworkError: For general network-related errors.
        httpx.HTTPError: For HTTP errors (e.g., status codes 4xx and 5xx).
    """
    if microservice not in ["scim", "inventory"]:
        raise KeyError(f"Invalid microservice provided: {microservice}. Choose 'scim' or 'inventory'")

    logging.info(f"{microservice.capitalize()} list".center(20, '='))

    microservice_col_name = {"scim": "Resources", "inventory": "data"}

    if microservice == "scim":
        url_t = f"https://{ONETRUST_HOSTNAME}/{microservice}/{ONETRUST_VERSION}/Users?startIndex={{current_index}}&count={{count}}"
        count_parameter: str = "itemsPerPage"
        initial_index = 1
        page_size = 500  # Fetch users in page groups of 500
    else:  # microservice == "inventory"
        url_t = f"https://{ONETRUST_HOSTNAME}/{microservice}/{ONETRUST_VERSION}/inventories/{inventory_type}?page={{current_index}}&size={{count}}"
        count_parameter: str = "meta.page.size"
        initial_index = 0
        page_size = 50  # Fetch vendors in page groups of 50 (It seems this is the max OneTrust allows)

    df_all_fetched = pd.DataFrame()
    current_index = initial_index
    has_more_pages = True

    async with httpx.AsyncClient(timeout=httpx.Timeout(timeout=TIMEOUT)) as client:
        while has_more_pages:
            url = url_t.format(current_index=current_index, count=page_size)
            # Retry logic for handling timeouts
            response = await get_http_response(url, ONETRUST_HEADERS, client)  # Passing the client object
            while response.status_code == 429:
                logging.warning("Rate limit exceeded. Retrying after delay...")
                retry_after = log_rate_limit_headers(response)  # Default to 1 second if not provided
                await asyncio.sleep(retry_after)  # Sleep for the time indicated in the response header before retrying
                response = await get_http_response(url, ONETRUST_HEADERS, client)  # Retry the request

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
        httpx.TimeoutException: If the entire request (connect plus read) takes longer than the timeout defined
                                within the AsyncClient.
        httpx.NetworkError: For general network-related errors.
        httpx.HTTPError: For other HTTP errors (e.g., status codes 4xx and 5xx).
    """
    url = f"https://{ONETRUST_HOSTNAME}/scim/{ONETRUST_VERSION}/Users/{user_id}"
    try:
        response = await client.get(url, headers=ONETRUST_HEADERS)
        handle_response_status(response)
        df_from_normalized_json = get_normalized_json_response_df(response)
        return df_from_normalized_json['userName'].values[0]
    except (KeyError, IndexError):
        logging.warning(f"userName not found for userID: {user_id}")
        return None


@retry(tries=3, delay=1, backoff=2, logger=logger)  # 3 retries, 1 s initial delay, doubling backoff
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
        httpx.TimeoutException: If the entire request (connect plus read) takes longer than the timeout defined
                                within the AsyncClient.
        httpx.NetworkError: For general network-related errors.
        httpx.HTTPError: For other HTTP errors (e.g., status codes 4xx and 5xx).
    """
    logging.info(f"Sending request to {url}")
    try:
        # async with httpx.AsyncClient(timeout=timeout) as a client:
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
    Log rate limit headers from an HTTP 429 response.

    This function examines the provided response object and extracts specific
    rate limit headers if they exist. The logged headers typically provide information
    about rate limiting status and retry instructions.

    Args:
        response (httpx.Response): The HTTP response object (ideally a "429 Too Many Requests" response).
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


async def get_assessments_df() -> pd.DataFrame:
    """
        Asynchronously retrieves a list of assessments from the OneTrust API.

        This function fetches assessment data from the OneTrust API, handles pagination
        and potential rate limiting, and normalizes the JSON response into a Pandas
        DataFrame. It specifically retrieves assessments of the type "VENDOR_RISK_ASSESSMENT"
        with a status of "APPROVED."

        Returns:
            pd.DataFrame: A DataFrame containing the normalized assessment data, including
                          details like assessment ID, vendor ID, assessment type, status,
                          and associated risks.

        Raises:
            httpx.ConnectTimeout: If a connection to the server cannot be established within the timeout.
            httpx.ReadTimeout: If the server does not send data within the timeout.
            httpx.TimeoutException: If the entire request (connect plus read) takes longer than the timeout.
            httpx.NetworkError: For general network-related errors.
            httpx.HTTPError: For HTTP errors (e.g., status codes 4xx and 5xx).
    """

    url_t = f"https://{ONETRUST_HOSTNAME}/assessment/{ONETRUST_VERSION}/assessments?page={{current_index}}&size={{count}}"
    # count_parameter: str = "page.size"
    initial_index = 0
    page_size = 50  # Fetch vendors in page groups of 50 (It seems this is the max OneTrust allows)

    df_all_fetched = pd.DataFrame()
    current_index = initial_index
    has_more_pages = True

    async with httpx.AsyncClient(timeout=httpx.Timeout(timeout=TIMEOUT)) as client:
        while has_more_pages:
            url = url_t.format(current_index=current_index, count=page_size)
            # Retry logic for handling timeouts
            response = await get_http_response(url, ONETRUST_HEADERS, client)  # Passing the client object
            while response.status_code == 429:
                logging.warning("Rate limit exceeded. Retrying after delay...")
                retry_after = log_rate_limit_headers(response)  # Default to 1 second if not provided
                await asyncio.sleep(retry_after)  # Sleep for the time indicated in the response header before retrying
                response = await get_http_response(url, ONETRUST_HEADERS, client)  # Retry the request

            handle_response_status(response)  # Check for errors and raise exceptions if needed.

            df_fetched = get_normalized_json_response_df(response)
            df_all_fetched = pd.concat([df_all_fetched, df_fetched], ignore_index=True)

            total_pages_parameter: str = "page.totalPages"
            total_elements_parameter: str = "page.totalElements"
            start_entry = current_index * page_size + 1
            total_elements = df_fetched[total_elements_parameter][0]
            end_entry = min(page_size * (current_index + 1), total_elements)
            print(f"Fetched data from {url} | items from {start_entry} to {end_entry}")
            current_index += 1
            total_pages = df_fetched[total_pages_parameter][0]
            has_more_pages = current_index < total_pages

        logging.info("".center(20, '='))

    return pd.json_normalize(df_all_fetched['content'].explode().reset_index(drop=True))


@retry(tries=3, delay=1, backoff=2, logger=logger)
async def fetch_assessment(client: httpx.AsyncClient, assessment_id: str) -> dict | None:
    """
        Asynchronously fetches details for a specific assessment from the OneTrust API.

        This function uses the provided `httpx.AsyncClient` to make a request to the OneTrust
        API to retrieve details for the assessment with the given `assessment_id`.

        Args:
            client (httpx.AsyncClient): An initialized `httpx.AsyncClient` object for making
                                        the API request.
            assessment_id (str): The unique ID of the assessment to retrieve.

        Returns:
            Dict[str, Any]: A dictionary containing the assessment details retrieved from the
                            OneTrust API, or an empty dictionary if the assessment is not found.

        Raises:
            httpx.HTTPError: If the API request fails with an HTTP error status code.
    """

    url = f"https://{ONETRUST_HOSTNAME}/assessment/{ONETRUST_VERSION}/assessments/{assessment_id}/export"
    try:
        response = await client.get(url, headers=ONETRUST_HEADERS)
        handle_response_status(response)
        df_assessment = get_normalized_json_response_df(response)

        def get_primary_entity_id(primary_entity_details):
            try:
                return f"{primary_entity_details[0][0]['id']}"
            except (IndexError, KeyError, TypeError):  # Catch potential errors
                return 'N/A'

        assessment_info ={
            "assessment_id" : f"{df_assessment['assessmentId'].tolist()[0]}",
            "assessment_number" : f"{df_assessment['assessmentNumber'].tolist()[0]}",
            "assessment_name" : f"{df_assessment['name'].tolist()[0]}",
            "assessment_status" : f"{df_assessment['status'].tolist()[0] if df_assessment['status'].tolist()[0] is not None else 'N/A'}",
            "created_date" : f"{df_assessment['createdDT'].tolist()[0].split('T')[0]}",
            "completed_date" : f"{df_assessment['completedOn'].tolist()[0].split('T')[0] if df_assessment['completedOn'].tolist()[0] is not None else 'N/A'}",
            "organization" : f"{df_assessment['orgGroup.name'].tolist()[0]}",
            "low_risk_count" : f"{df_assessment['lowRisk'].tolist()[0]}",
            "medium_risk_count" : f"{df_assessment['mediumRisk'].tolist()[0]}",
            "high_risk_count" : f"{df_assessment['highRisk'].tolist()[0]}",
            "very_high_risk_count" : f"{df_assessment['veryHighRisk'].tolist()[0]}",
            "primary_entity_id": f"{get_primary_entity_id(df_assessment['primaryEntityDetails'])}",
        }

        df_assessment_sections = pd.json_normalize(
            df_assessment['sections'].explode().reset_index(drop=True))

        df_assessment_questions = pd.json_normalize(
            df_assessment_sections['questions'].explode().reset_index(drop=True))
        df_assessment_question_options = pd.json_normalize(
            df_assessment_questions['question.options'].explode().reset_index(drop=True))[
            ['score', 'id', 'option']]

        df_assessment_question_responses = pd.json_normalize(
            df_assessment_questions['questionResponses'].explode().reset_index(drop=True))
        if df_assessment_question_responses.empty:
            assessment_info["assessment_score"] = 0
            return assessment_info
        df_assessment_responses = pd.json_normalize(
            df_assessment_question_responses['responses'].explode().reset_index(drop=True))[
            ['responseId', 'response']]

        df_assessment_score = pd.merge(
            df_assessment_responses,
            df_assessment_question_options,
            left_on='responseId', right_on='id',
        )

        assessment_info["assessment_score"] = df_assessment_score['score'].sum() if df_assessment_score['score'] is not None else 0

        return assessment_info
    except (KeyError, IndexError) as exception:
        logging.warning(f"There was an error: {exception}")
        return None


@retry(tries=5, delay=3, backoff=1, logger=logger)
async def get_vendors_data_df() -> pd.DataFrame:
    """
        Retrieves vendor data from the 'inventory' microservice and processes it.

        This function fetches a pandas DataFrame from the 'inventory' microservice.
        It then handles missing 'owner' values by populating them with a default owner ID.

        Returns:
            pd.DataFrame: A DataFrame containing vendor data, with missing 'owner' values filled.

        Raises:
            Any exceptions raised by `get_microservice_df` will propagate.
        The `retry` decorator will handle retries as configured.
    """
    df_vendors_data = await get_microservice_df("inventory")

    # Sets default values for the owner field if it finds NaN values
    df_vendors_data['owner'] = df_vendors_data['owner'].fillna(
        {i: [{"id": DEFAULT_OWNER_ID}] for i in df_vendors_data.index}
    )

    return df_vendors_data


@retry(tries=5, delay=5, backoff=2, logger=logger)
async def get_users_data_df(df_vendors_data: pd.DataFrame) -> pd.DataFrame:
    """
        Retrieves user data based on unique owner IDs from the provided vendor DataFrame.

        This function extracts unique owner IDs from the 'owner' column of the input
        `df_vendors_data` DataFrame, excluding the default owner ID. It then fetches
        usernames corresponding to these owner IDs using an external API.

        Args:
            df_vendors_data (pd.DataFrame): A DataFrame containing vendor data with an 'owner' column.

        Returns:
            pd.DataFrame: A DataFrame containing user IDs and their corresponding usernames.

        Raises:
            httpx.HTTPError: If there are issues with the HTTP requests to the user API.
            Any exceptions raised by `fetch_user_name` will propagate.
            The `retry` decorator will handle retries as configured.
    """
    unique_owners = df_vendors_data['owner'].apply(lambda x: x[0]['id']).unique()
    filtered_owners = [owner for owner in unique_owners if owner != DEFAULT_OWNER_ID]

    # get emails from unique owners by using OneTrust API
    async with httpx.AsyncClient(timeout=httpx.Timeout(timeout=TIMEOUT)) as client:
        tasks = [fetch_user_name(client, user_id) for user_id in filtered_owners]
        user_data = await asyncio.gather(*tasks)

    # Create df_users_data DataFrame
    df_users_data = pd.DataFrame({
        'id': filtered_owners,
        'userName': user_data
    })

    return df_users_data


@retry(tries=5, delay=5, backoff=2, logger=logger)
async def get_filtered_assessment_df(inventory_type: str) -> pd.DataFrame:
    """
        Retrieves and filters assessment data based on the specified inventory type.

        This function fetches assessment data, filters it based on the provided
        `inventory_type`, retrieves additional details for the filtered assessments,
        and returns the processed data as a pandas DataFrame.

        Args:
            inventory_type (str): The type of inventory to filter assessments by.
                Valid values are "ai_assessments" and "offline_sw_assessments".

        Returns:
            pd.DataFrame: A DataFrame containing the filtered and processed assessment data.
                          The structure of the DataFrame depends on the `inventory_type`.

        Raises:
            httpx.HTTPError: If there are issues with the HTTP requests to the OneTrust API.
            KeyError: If the `inventory_type` is not one of the valid values.
            Any exceptions raised by `get_assessments_df` or `fetch_assessment` will propagate.
            The `retry` decorator will handle retries as configured.
    """
    df_assessments = await get_assessments_df()
    df_filtered_assessments = pd.DataFrame()
    df_asset_info = pd.DataFrame()

    if inventory_type == "ai_assessments":
        df_filtered_assessments = df_assessments[df_assessments['templateName'] == "AI Service Risk Assessment"]
    elif inventory_type == "offline_sw_assessments":
        df_filtered_assessments = df_assessments[
            (df_assessments['templateName'] == "Offline Software Validation")
            & (df_assessments['status'] == "Completed")
        ]

    filtered_assessments_list = df_filtered_assessments['assessmentId'].dropna(how='all').tolist()

    # get details from assessments by using OneTrust API
    async with httpx.AsyncClient(timeout=httpx.Timeout(timeout=TIMEOUT)) as client:
        tasks = [fetch_assessment(client, assessment_id) for assessment_id in
                 filtered_assessments_list]
        filtered_assessments = await asyncio.gather(*tasks)

    df_filtered_assessments = pd.json_normalize(filtered_assessments)

    if inventory_type == "ai_assessments":
        df_filtered_assessments = process_dataframes(df_filtered_assessments, df_filtered_assessments,
                                                     inventory_type=inventory_type)
    elif inventory_type == "offline_sw_assessments":
        filtered_primary_entities_list = df_filtered_assessments['primary_entity_id'].dropna(how='all').tolist()

        # get the description field from each asset
        async with httpx.AsyncClient(timeout=httpx.Timeout(timeout=TIMEOUT)) as client:
            tasks = [fetch_inventory_description(client=client, id=primary_entity_id, type="assets")
                     for primary_entity_id in filtered_primary_entities_list]
            inventory_description = await asyncio.gather(*tasks)

        # Create df_asset DataFrame
        df_asset_info = pd.DataFrame({
            'entity_id': filtered_primary_entities_list,
            'description': inventory_description,
        })
        df_asset_info['ticket'] = df_asset_info['description'].str.split('/').str[-1]

        df_filtered_assessments = process_dataframes(df_asset_info, df_filtered_assessments,
                                                     inventory_type=inventory_type)

    return df_filtered_assessments


async def fetch_inventory_description(client: httpx.AsyncClient, id: str, type: str):
    # The supporting inventory types as are 'processing-activities', 'vendors', 'assets', and 'entities'
    url = f"https://{ONETRUST_HOSTNAME}/inventory/{ONETRUST_VERSION}/inventories/{type}/{id}"
    try:
        response = await client.get(url, headers=ONETRUST_HEADERS)
        handle_response_status(response)
        df_from_normalized_json = get_normalized_json_response_df(response)
        return df_from_normalized_json['data.description'].values[0] if df_from_normalized_json['data.description'].values[0] is not None else 'N/A'
    except (KeyError, IndexError):
        logging.warning(f"inventory not found for {type}: {id}")
        return None
