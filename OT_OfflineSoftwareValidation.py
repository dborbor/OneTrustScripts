import asyncio
import logging
from utils.file_utils import save_all_data
from utils.onetrust_api import get_filtered_assessment_df

# Configuring logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# Create a logger instance
logger = logging.getLogger(__name__)
logging.disable(logging.CRITICAL)  # Comment to view logging // Uncomment to disable all logging

async def main() -> None:
    inventory_type = "offline_sw_assessments"
    # Get the list of offline software assessments from OneTrust API
    df_offline_sw_assessments = await get_filtered_assessment_df(inventory_type=inventory_type)
    # Save all offline software assessments to Excel, HTML, and updates the Confluence table
    save_all_data(df_offline_sw_assessments, inventory_type=inventory_type, status="approved")
    print("done")

if __name__ == "__main__":
    asyncio.run(main())
