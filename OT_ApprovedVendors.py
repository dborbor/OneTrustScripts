import asyncio
import logging
from utils.file_utils import save_all_data
from utils.data_utils import process_dataframes
from utils.onetrust_api import get_vendors_data_df, get_users_data_df


# Configuring Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# Create a logger instance
logger = logging.getLogger(__name__)
# logging.disable(logging.CRITICAL)  # Comment to view logging // Uncomment to disable all logging


async def main() -> None:
    inventory_type = "vendors"
    status = "approved"

    # get dataframe of all vendors from OneTrust
    df_vendors = await get_vendors_data_df()
    # get dataframe of id and username
    df_users = await get_users_data_df(df_vendors)

    df_in_progress_vendors = process_dataframes(df_users, df_vendors, inventory_type=inventory_type, status=status)

    save_all_data(df_in_progress_vendors, inventory_type=inventory_type, status=status)


if __name__ == "__main__":
    asyncio.run(main())
