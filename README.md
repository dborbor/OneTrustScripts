# OneTrustScripts
Scripts to use with the OneTrust API

## OneTrust Vendor Data Extraction Tool

This Python script is designed to automate the extraction of vendor data from the OneTrust API, along with user information, and merge it into a consolidated report. The report includes details like vendor ID, name, business owner, organization, description, category, and website.

### Features

- **Efficient Data Retrieval:** Uses asynchronous programming with `asyncio` and `httpx` to fetch data concurrently, optimizing performance.
- **Automatic Pagination:** Handles paginated responses from the OneTrust API to retrieve all available data.
- **Error Handling:** Includes robust error handling to gracefully manage network issues, rate limiting, and other potential errors.
- **Customizable:**  Allows for filtering vendors based on specific criteria (e.g., active vendors in the 'Live' stage).
- **Output Formats:** Generates both Excel (.xlsx) and HTML (.html) reports for easy sharing and analysis.
- **SharePoint Integration:** Can save reports directly to a specified SharePoint library if it's synchronized.

### Prerequisites

- **Python:** Requires Python 3.7 or later.
- **Libraries:**  
    - Install the required libraries using:
      ```bash
      pip install pandas httpx python-dotenv retry
      ```
- **OneTrust API Key:** You'll need a valid API key for authentication with the OneTrust API. Obtain this from your OneTrust account.
- **.env File:** Create a `.env` file in the same directory as the script and add the following line, replacing `your_api_key` with your actual API key:  APP_API_KEY=your_api_key
### Configuration

- **File Paths:** Update `SHAREPOINT_PATH_MACOS` and `SHAREPOINT_PATH_WINDOWS` in the script to reflect the correct paths to your SharePoint library (if you want to use SharePoint).
- **Unique Filenames:** Set `unique_filename` to `True` if you want each report to have a unique timestamped filename.

### Usage

1. **Clone or Download:** Clone this repository or download the script file.
2. **Install Dependencies:** Run the following command to install the required libraries:

  ```bash
  pip install pandas httpx python-dotenv retry
  ```
3. **Run the Script:** Execute the script using the following command:
  ```bash
  python your_script_name.py
  ```
4. **Report Generation:** The script will fetch data from the OneTrust API, process it, and generate the reports in the specified output formats. The reports will be saved in the designated location (SharePoint library or Downloads folder).


### Logging (Optional)
The script includes logging for debugging and troubleshooting. By default, logging is disabled. To enable logging, comment out the line `logging.disable(logging.CRITICAL)` in the script. Log messages will be written to the console.


### License
This project is licensed under the MIT License - see the LICENSE file for details.

### Acknowledgements
The script uses the following libraries:
- `httpx` (for making asynchronous HTTP requests)
- `pandas` (for data manipulation and analysis)
- `python-dotenv` (for loading environment variables from `.env` file)
- `retry` (for retrying failed requests)


### Disclaimer
This script is provided as-is, without any warranty. Use at your own risk.
