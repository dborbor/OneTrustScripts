import os
import pandas as pd
from datetime import datetime
from utils.confluence_utils import update_confluence_table
from config.config_utils import SHAREPOINT_AI_PATH_MACOS, SHAREPOINT_AI_PATH_WINDOWS
from config.config_utils import SHAREPOINT_ASSETS_PATH_MACOS, SHAREPOINT_ASSETS_PATH_WINDOWS
from config.config_utils import SHAREPOINT_VENDORS_PATH_MACOS, SHAREPOINT_VENDORS_PATH_WINDOWS
from config.config_utils import SHAREPOINT_OFFLINE_SW_PATH_MACOS, SHAREPOINT_OFFLINE_SW_PATH_WINDOWS

def set_filename(inventory_type:str, status: str = "", is_unique: bool=False) -> str:
    """
    Generates a filename for a file of approved vendors.

    Args:
        inventory_type (str): Type of inventory ('ai_assessments', 'vendors', or 'assets')
        status (str, optional): The status of the vendor (e.g., "approved"). Defaults to "".
        is_unique (bool): If True, appends a timestamp to the filename to ensure uniqueness.

    Returns:
        str: The generated filename string.

        * If `is_unique` is True, the format is "Approved_Vendors_YYYYMMDDHHMMSS".
        * If `is_unique` is False, the format is "Approved_Vendors".
    """
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    if is_unique:
        return f"{status.title().replace(" ","_")}_{inventory_type.capitalize()}_{timestamp}"
    else:
        return f"{status.title().replace(" ","_")}_{inventory_type.capitalize()}"


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
    html_styled = df.to_html(justify='left',  # How to justify the column labels.
                             render_links=True,
                             index=False,  # Whether to print index (row) labels.
                             )  # save it as html file
    # Adding CSS to the HTML string
    html_styled = """
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
        """ + html_styled + """
    </body>
    </html>
    """
    # Writing the HTML string to a file
    with open(os.path.join(save_dir, f"{name}.html"), "w") as f:
        f.write(html_styled)


def save_all_data(df: pd.DataFrame, inventory_type: str, status: str ="") -> None:
    """
        Save a copy of the dataframe as an Excel file and as an HTML table.
        Additionally, it updates the Confluence table.

        Args:
            df (pd.DataFrame): The Pandas DataFrame containing the data to be displayed in the table.
            inventory_type (str): Type of inventory ('ai_assessments', 'vendors', or 'assets')
            status (str, optional): The status of the vendor (e.g., "approved"). Defaults to "".
    """
    path = os.path.join(os.path.expanduser("~"), "Downloads")  # placeholder path that will be updated later
    if inventory_type == "ai_assessments":
        path = set_path(SHAREPOINT_AI_PATH_WINDOWS, SHAREPOINT_AI_PATH_MACOS)
    elif inventory_type == "offline_sw_assessments":
        path = set_path(SHAREPOINT_OFFLINE_SW_PATH_WINDOWS, SHAREPOINT_OFFLINE_SW_PATH_MACOS)
    elif inventory_type == "assets":
        path = set_path(SHAREPOINT_ASSETS_PATH_WINDOWS, SHAREPOINT_ASSETS_PATH_MACOS)
    elif inventory_type == "vendors":
        path = set_path(SHAREPOINT_VENDORS_PATH_WINDOWS, SHAREPOINT_VENDORS_PATH_MACOS)

    filename = set_filename(inventory_type=inventory_type, status=status)  # set the file name

    update_confluence_table(df, inventory_type=inventory_type, status=status)

    df.to_excel(
        os.path.join(path, f"{filename}.xlsx"),
        sheet_name=f"{status.title().replace(" ", "")}{inventory_type.capitalize()}",
        index=True,
    )  # save it as an Excel file

    # df['Description'] = df['Description'].replace('\n', '<br/>', regex=True)
    save_styled_dataframe_as_html(df, path, filename)

    print(f"Copy saved to {path}.")
