# OneTrustScripts
Scripts to use with the OneTrust API

## OneTrustScripts
Scripts to use with the OneTrust API

This Python script automates extracting vendor data from the OneTrust API. It fetches vendor details, optionally includes user information (with two fetching modes for flexibility), and generates reports in Excel (`.xlsx`) and styled HTML (`.html`) formats.

### Features

* **Efficient Data Retrieval:** Uses asynchronous operations to optimize data fetching from the OneTrust API, minimizing the impact of large datasets and rate limits.
* **Flexible User Fetching:** Offers two modes for user data retrieval:
    - **Targeted Fetching:**  Quickly retrieves only users linked to approved vendors.
    - **Complete List:**  Fetches the entire list of OneTrust users (may take longer).
* **Error Handling and Retries:** Includes retry mechanisms for network issues and API rate limits. Detailed logging aids in troubleshooting.
* **Data Transformation:** Converts complex JSON responses into Pandas DataFrames for easy analysis and reporting.
* **Customizable Output:**  
    - Control filename format (with or without timestamps).
    - Choose to save in a designated SharePoint library or your local Downloads folder.
    - Adjust the timeout duration for API requests.
* **Enhanced HTML Report:** Produces a visually appealing HTML report with basic CSS styling.

### Prerequisites

* **Python:** Requires Python 3.7 or later.
* **Libraries:** Install with `pip install pandas httpx python-dotenv retry`
* **OneTrust API Key:** Obtain a valid API key from your OneTrust account.
* **.env File:** Create a `.env` file in the script's directory containing: `APP_API_KEY=your_api_key`

### Configuration

* **File Paths:** Update `SHAREPOINT_PATH_MACOS` and `SHAREPOINT_PATH_WINDOWS` in the script if using SharePoint.
* **Unique Filenames:** Set `unique_filename` to `True` for timestamped filenames.
* **User Fetching:** Choose between targeted or complete user fetching by setting `fetch_individual_users` to `True` or `False`.

### Usage

1. **Clone or Download:** Obtain the script files.
2. **Install Dependencies:** Run `pip install pandas httpx python-dotenv retry`.
3. **Run:** Execute `python your_script_name.py`. The script will create reports in the specified output formats and location.

### Logging (Optional)

Logging is disabled by default. To enable it, comment out the line `logging.disable(logging.CRITICAL)`.

### License

This project is licensed under the MIT License – see the LICENSE file for details.

### Key Script Functions

* **`get_microservice_df`:** Fetches paginated data (users or vendors) from the OneTrust API.
* **`get_http_response`:**  Handles API requests with retry logic.
* **`handle_response_status`:**  Verifies response status and provides error details.
* **`fetch_user_name`:**  (Optional) Fetches user names for specific user IDs.
* **`process_dataframes`:** Processes and combines data into the final format.
* **`set_filename` and `set_path`:** Manage output file naming and location.
* **`save_styled_dataframe_as_html`:**  Creates the styled HTML report.

### Disclaimer
This script is provided as-is, without any warranty. Use at your own risk.
