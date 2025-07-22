import pandas as pd
import html
import logging
from atlassian import Confluence
from bs4 import BeautifulSoup
from config.config_utils import CONFLUENCE_USERNAME, CONFLUENCE_PASSWORD, CONFLUENCE_URL, CONFLUENCE_SPACE

# Configure logging
logging.basicConfig(level=logging.INFO, format=' %(asctime)s - %(levelname)s - %(message)s')
# Create a logger instance
logger = logging.getLogger(__name__)  # __name__ is a common convention
# logging.disable(logging.CRITICAL)  # Comment to view logging // Uncomment to disable all logging

def update_confluence_table(df_assets: pd.DataFrame, inventory_type: str, status: str = "") -> None:
    """
        Updates a Confluence table with data from a Pandas DataFrame.

        This function connects to a Confluence instance using provided credentials, locates
        a specific page containing a table, and replaces the table's body with newly generated
        rows based on the input DataFrame. The page is then updated in Confluence with the
        modified content.

        Args:
            df_assets (pd.DataFrame): The DataFrame containing the data to populate the Confluence table.
            inventory_type (str): The type of inventory data being updated (e.g., "assets", "ai_assessments", "vendors")
            status (str, optional): The status of the table (e.g., "approved"). Defaults to "".

        Raises:
            ConfluenceError: If there's any issue connecting to Confluence, retrieving the page,
                             or updating the page.
    """
    # Confluence setup
    confluence = Confluence(
        url=CONFLUENCE_URL,
        username=CONFLUENCE_USERNAME,
        password=CONFLUENCE_PASSWORD,
        cloud=True
    )
    confluence_page_title = ""
    if inventory_type == "assets":
        confluence_page_title = f"M365 {inventory_type.capitalize()} Registry"
    elif inventory_type == "ai_assessments":
        confluence_page_title = "Third-Party AI Risk Maturity Assessments"
    elif inventory_type == "offline_sw_assessments":
        confluence_page_title = "Offline Software Validation Assessments"
    elif inventory_type == "vendors":
        if status == "in progress":
            confluence_page_title = "In-Progress VRAs"
        elif status == "rejected_terminated":
            confluence_page_title = "Rejected or Terminated Vendor Registry"
        else:
            confluence_page_title = f"Vendor Registry (Approved Vendors)"

    # Confluence page details
    page_id = confluence.get_page_id(space=CONFLUENCE_SPACE, title=confluence_page_title)
    page = confluence.get_page_by_id(page_id, expand='body.storage,version')
    page_body_storage_value = page['body']['storage']['value']

    try:
        # Extract and update table content
        soup = BeautifulSoup(page_body_storage_value, 'html.parser')
        table = soup.find('table')

        # Get all rows within <tbody>
        rows = table.tbody.find_all('tr')
        # Confluence puts the headers inside a table row tag, we keep this.
        header_row = rows[0]  # header_row is of type: bs4.element.Tag
        # Generate a new table body, starting from the second row (index 1)
        new_table_body = '<tbody>'
        new_table_body += str(header_row)
        for _, row in df_assets.iterrows():
            new_table_body += generate_table_row(row, inventory_type=inventory_type, status=status)
        new_table_body += '</tbody>'

        # Replace the entire table body <tbody>
        table.tbody.replace_with(BeautifulSoup(new_table_body, 'html.parser'))

        # Update Confluence page
        confluence.update_page(
            page['id'],
            page['title'],
            body=str(soup),
            type='page',
            representation='storage',
            minor_edit=False,
            version_comment='Updated table data',
            full_width=True,
        )
    except Exception as e:
        logging.error(f"Error updating Confluence table: {e}")
    finally:
        # Close Confluence connection
        print("Table updated successfully.")
        confluence.close()


def generate_table_row(row: pd.Series, inventory_type: str, status: str = "") -> str | None:
    """
    Generates an HTML table row string from a Pandas Series.

    This function takes a Pandas Series (representing a row in a DataFrame) and
    an inventory type string, and generates an HTML table row (`<tr>`) string
    with appropriate data formatted for Confluence. The specific data included
    in the row depends on the `inventory_type`:

    - `vendors`: Includes ID, Vendor Name, Business Owner (as email link), Organization,
                 Description, Vendor Category (with "egory" removed from "Category"),
                 and Website (as hyperlink).
    - `assets`: Includes Asset Name, Technical Owner (as email link), Organization,
                Description, Asset Type, and MS Graph API Annual Permissions Review.
    - `ai_assessments`: Includes Vendor - Assessment Name (as hyperlink to OneTrust),
                        Organization, Status, Score, and Grade.

    Args:
        row (pd.Series): A Pandas Series representing a single row of data.
        inventory_type (str): The type of inventory data being processed,
                              which determines the columns to include in the row.
        status (str): The status of the vendor

    Returns:
        str: An HTML string representing a table row (`<tr>`).
    """
    if inventory_type == "vendors":
        if status == "rejected_terminated":
            return f'<tr>' \
                   f'<td class="numberingColumn">{row.name}</td>' \
                   f'<td><p>{html.escape(str(row["Vendor Name"]))}</p></td>' \
                   f'<td><p>{html.escape(str(row["Description"]))}</p></td>' \
                   f'<td><p><a data-card-appearance="inline" href="{html.escape(str(row["Jira Ticket"]))}">{html.escape(str(row["Jira Ticket"]))}</a></p></td>' \
                   f'<td><p>{html.escape(str(row["Created Date"]))}</p></td>' \
                   f'<td><p>{html.escape(str(row["Last Updated"]))}</p></td>' \
                   f'<td><p><a data-card-appearance="inline" href="{html.escape(str(row["Website"]))}">{html.escape(str(row["Website"]))}</a></p></td>' \
                   f'</tr>'
        else:
            return f'<tr>' \
                   f'<td class="numberingColumn">{row.name}</td>' \
                   f'<td><p>{html.escape(str(row["ID"]))}</p></td>' \
                   f'<td><p>{html.escape(str(row["Vendor Name"]))}</p></td>' \
                   f'<td><p><a href="mailto:{row["Business Owner"]}">{row["Business Owner"]}</a></p></td>' \
                   f'<td><p>{html.escape(str(row["Organization"]))}</p></td>' \
                   f'<td><p>{html.escape(str(row["Description"]))}</p></td>' \
                   f'<td><p>{row["Vendor Category"].replace('egory','')}</p></td>' \
                   f'<td><p><a data-card-appearance="inline" href="{html.escape(str(row["Website"]))}">{html.escape(str(row["Website"]))}</a></p></td>' \
                   f'</tr>'
    elif inventory_type == "assets":
        return f'<tr>' \
               f'<td class="numberingColumn">{row.name}</td>' \
               f'<td><p>{html.escape(str(row["Asset Name"]))}</p></td>' \
               f'<td><p><a href="mailto:{row["Technical Owner"]}">{row["Technical Owner"]}</a></p></td>' \
               f'<td><p>{html.escape(str(row["Organization"]))}</p></td>' \
               f'<td><p>{html.escape(str(row["Description"]))}</p></td>' \
               f'<td><p>{row["Asset Type"]}</p></td>' \
               f'<td><p>{row["MS Graph API Annual Permissions Review"]}</p></td>' \
               f'</tr>'
    elif inventory_type == "ai_assessments":
        link = (f"https://app.onetrust.com/vendor/assessments/details/{row['assessment_id']}?"
                f"type=vendors&tabId=assessments&module=vendor&recordId={row['primary_entity_id']}")
        return f'<tr>' \
               f'<td class="numberingColumn">{row.name}</td>' \
               f'<td><p><a href={link}>{row["Vendor - Assessment Name"]}</a></p></td>' \
               f'<td><p>{html.escape(str(row["Organization"]))}</p></td>' \
               f'<td><p>{html.escape(str(row["Status"]))}</p></td>' \
               f'<td><p>{html.escape(str(row["Date"]))}</p></td>' \
               f'<td><p>{html.escape(str(row["Score"]))}</p></td>' \
               f'<td><p>{html.escape(str(row["Grade"]))}</p></td>' \
               f'</tr>'
    elif inventory_type == "offline_sw_assessments":
        link = (f"https://app.onetrust.com/vendor/assessments/details/{row['assessment_id']}?"
                f"type=assets&tabId=assessments&module=vendor&recordId={row['primary_entity_id']}")
        return f'<tr>' \
               f'<td class="numberingColumn">{row.name}</td>' \
               f'<td><p><a href={link}>{row["Software Name"]}</a></p></td>' \
               f'<td><p>{html.escape(str(row["Organization"]))}</p></td>' \
               f'<td><p><a data-card-appearance="inline" href="{html.escape(str(row["Ticket URL"]))}">{html.escape(str(row["Ticket Number"]))}</a></p></td>' \
               f'<td><p>{html.escape(str(row["Software Risk Level"]))}</p></td>' \
               f'</tr>'
    return None
