import httpx
import asyncio
import logging
import pandas as pd
from config.config_utils import TIMEOUT
from utils.file_utils import save_all_data
from utils.data_utils import process_dataframes
from utils.onetrust_api import get_microservice_df, fetch_user_name


# Configuring Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# Create a logger instance
logger = logging.getLogger(__name__)
# logging.disable(logging.CRITICAL)  # Comment to view logging // Uncomment to disable all logging


async def main() -> None:
    inventory_type = "assets"
    df_assets_data = await get_microservice_df(microservice="inventory",
                                               inventory_type=inventory_type,
                                               )
    # If no technical owner is set, set "owner_id_not_set"
    df_assets_data['technicalOwner'] = (
        df_assets_data['technicalOwner']
        .fillna(
            {i: [{"id": "technical_owner_id_not_set"}] for i in df_assets_data.index}
        )
    )
    # Get list of unique technicalOwner IDs
    unique_owners = (df_assets_data['technicalOwner']
                     .apply(lambda x: x[0]['id'])
                     .unique()
                     )
    filtered_owners = unique_owners[unique_owners != "technical_owner_id_not_set"].tolist()

    # get emails from unique owners by using OneTrust API
    async with httpx.AsyncClient(timeout=httpx.Timeout(timeout=TIMEOUT)) as client:
        tasks = [fetch_user_name(client, user_id) for user_id in filtered_owners]
        user_data = await asyncio.gather(*tasks)

    # Create df_users_data DataFrame
    df_users_data = pd.DataFrame({
        'id': filtered_owners,
        'userName': user_data
    })

    df_approved_assets = process_dataframes(df_users_data, df_assets_data, inventory_type=inventory_type)

    # Save all offline software assessments to Excel, HTML, and updates the Confluence table
    save_all_data(df_approved_assets, inventory_type=inventory_type, status="approved")


if __name__ == "__main__":
    asyncio.run(main())
