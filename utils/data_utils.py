import pandas as pd


def process_dataframes(df_users: pd.DataFrame, df_inventory: pd.DataFrame, inventory_type: str, status: str = "") -> pd.DataFrame:
    """
    Processes and prepares inventory and user data for Confluence table update.

    This function performs data transformations and merging based on the `inventory_type`:

    For 'ai_assessments':
        1. Calculates a 'status_date' column combining assessment status and date.
        2. Classifies assessment scores into grades ('A - Excellent' to 'F - Fail').
        3. Selects, reorders, and renames columns relevant to AI assessments.

    For 'vendors':
        1. Converts usernames in `df_users` to lowercase.
        2. Extracts and cleans vendor category and owner information from `df_inventory`.
        3. Filters `df_inventory` for active vendors in the 'Live' workflow stage.
        4. Merges `df_inventory` and `df_users` on owner ID.
        5. Selects, reorders, and renames columns relevant to vendors.

    For 'assets':
        1. Converts usernames in `df_users` to lowercase.
        2. Extracts and cleans technical owner, description, asset type, and
           MS Graph API review information from `df_inventory`.
        3. Filters `df_inventory` for active or pending assets with
           'MS Graph API Annual Permissions Review' set to 'Yes'.
        4. Merges `df_inventory` and `df_users` on technical owner ID.
        5. Selects, reorders, and renames columns relevant to assets.

    Finally, sets a 1-based index for the resulting DataFrame.

    Args:
        df_users (pd.DataFrame): DataFrame containing user data.
        df_inventory (pd.DataFrame): DataFrame containing inventory data.
        inventory_type (str): Type of inventory ('ai_assessments', 'vendors', or 'assets').
        status (str, optional): The status of the vendor (e.g., "approved"). Defaults to "".

    Returns:
        pd.DataFrame: Processed and merged DataFrame ready for Confluence table update.
    """
    df = pd.DataFrame()
    df_merged = pd.DataFrame()
    # df_inventory_filtered = pd.DataFrame()
    message_suffix = ""
    new_column_order = []

    if inventory_type.split('_')[-1] == 'assessments':
        # Transform the values for assessment status
        def transform_status(stat: str) -> str:
            # replace underscores with spaces and capitalize the first letter
            return stat.replace('_', ' ').title()

        # Apply transformation to column
        df_inventory['assessment_status'] = df_inventory['assessment_status'].apply(transform_status)

        # check for status
        def get_status_date(row):
            if row['assessment_status'] == 'Completed':
                return f"{row['assessment_status']} on {row['completed_date']}"
            else:
                return f"{row['assessment_status']}. Assessment created on {row['created_date']}."
        # Apply get_status_date
        df_inventory['status_date'] = df_inventory.apply(get_status_date, axis=1)

        # check
        df_inventory['completed_date'] = pd.to_datetime(df_inventory['completed_date'], errors='coerce')
        df_inventory['created_date'] = pd.to_datetime(df_inventory['created_date'], errors='coerce')
        # new column with the newest date from the two columns
        df_inventory['date'] = df_inventory[['completed_date', 'created_date']].max(axis=1)
        df_inventory['date'] = df_inventory['date'].dt.date

        # Score classification
        df_inventory['assessment_score'].astype(int).fillna(0, inplace=True)

        def get_ai_grade(score):
            if 68 <= score <= 80:
                return 'A - Excellent'
            elif 57 <= score <= 67:
                return 'B - Good'
            elif 46 <= score <= 56:
                return 'C - Average'
            elif 35 <= score <= 45:
                return 'D - Below Average'
            elif score < 34:
                if score == 0:
                    return 'Not Yet Started'
                else:
                    return 'F - Fail'
            return None

        def get_offline_grade(score):
            if score <= 10:
                if score == 0:
                    return 'Not Yet Started'
                else:
                    return 'Low'
            elif 11 <= score <= 25:
                return 'Medium'
            elif 26 <= score <= 35:
                return 'High'
            elif score > 35:
                return 'Very High'
            return None

        if inventory_type.split('_')[0] == 'ai':
            df_inventory['grade'] = df_inventory['assessment_score'].apply(get_ai_grade)
            message_suffix = "AI Assessments"
            df_merged = df_inventory.copy()

            # Rearrange columns
            new_column_order = ['assessment_name',
                                'organization',
                                'status_date',
                                'assessment_status',
                                'created_date',
                                'completed_date',
                                'assessment_score',
                                'date',
                                'grade',
                                'assessment_id',
                                'primary_entity_id'
                                ]

        elif inventory_type.split('_')[0] == 'offline':
            df_inventory['grade'] = df_inventory['assessment_score'].apply(get_offline_grade)
            message_suffix = "Offline Software Assessments"
            # Inner join of the vendors and users dataframe on the owner and id columns respectively
            df_merged = pd.merge(df_inventory, df_users, left_on='primary_entity_id', right_on='entity_id')
            # Rearrange columns
            new_column_order = ['assessment_name',
                                'organization',
                                'status_date',
                                'assessment_status',
                                'created_date',
                                'completed_date',
                                'assessment_score',
                                'date',
                                'grade',
                                'assessment_id',
                                'primary_entity_id',
                                'description',
                                'ticket',
                                ]
        df = (
            df_merged[new_column_order]
            .rename(  # rename columns
                columns={
                    "assessment_name": "Vendor - Assessment Name" if inventory_type.split('_')[0] == 'ai' else "Software Name",
                    "organization": "Organization",
                    "status_date": "Status and Date",
                    "assessment_status": "Status",
                    "date": "Date",
                    "assessment_score": "Score",
                    "grade": "Grade" if inventory_type.split('_')[0] == 'ai' else "Software Risk Level",
                    "description": "Ticket URL",
                    "ticket": "Ticket Number",
                }
            )
        )

    else:
        # Have the userName (emails) values be all lower case
        df_users['userName'] = df_users['userName'].apply(lambda x: x.lower())

        if inventory_type == 'vendors':
            # Extracting the business owner for each vendor entry
            df_inventory['owner'] = df_inventory['owner'].apply(lambda x: x[0]['id'])
            # Extracting the Vendor Category value for each vendor entry
            # If no category has been set, it will display "category_not_set"
            df_inventory['customField1000'] = df_inventory['customField1000'].fillna(
                {i: [{"value": "vendor_category_not_set"}] for i in df_inventory.index}
            )
            df_inventory['customField1000'] = df_inventory['customField1000'].apply(lambda x: x[0]['value'])
            # If no Vendor ID has been set, it will display "Vendor ID not set"
            df_inventory['vendorId'] = df_inventory['vendorId'].fillna("Vendor ID Not Set")
            # Set grab only the date from time
            df_inventory['createdDate'] = pd.to_datetime(df_inventory['createdDate'], errors='coerce')
            df_inventory['createdDate'] = df_inventory['createdDate'].dt.date
            df_inventory['updatedDate'] = pd.to_datetime(df_inventory['updatedDate'], errors='coerce')
            df_inventory['updatedDate'] = df_inventory['updatedDate'].dt.date

            if status == "in progress":
                df_inventory_filtered = df_inventory[
                    (df_inventory['workflowStage.stage.value'] == 'Under Evaluation')
                    |
                    (df_inventory['workflowStage.stage.value'] == 'In Review')
                ]
                message_suffix = "Vendor Assessments in Progress"
            elif status == "rejected_terminated":
                df_inventory_filtered = df_inventory[
                    (df_inventory['workflowStage.stage.value'] == 'Rejected')
                    |
                    (df_inventory['workflowStage.stage.value'] == 'Terminated')
                    ]
                message_suffix = "Rejected or Terminated Vendors"
            else:
                # Filtering the vendors data frame to only consider entries that are active and that Live
                df_inventory_filtered = df_inventory[
                    (df_inventory['status.key'] == 'active')
                    &
                    (df_inventory['workflowStage.stage.value'] == 'Live')
                    ]
                message_suffix = "Approved Vendors"
            # Inner join of the vendors and users dataframe on the owner and id columns respectively
            df_merged = pd.merge(df_inventory_filtered, df_users, left_on='owner', right_on='id')

            # Rearrange columns
            new_column_order = ['number',
                                'name',
                                'userName',
                                'organization.value',
                                'description',
                                'customField1000',
                                'customField1001',
                                'vendorId',
                                'createdDate',
                                'updatedDate',
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
                        "vendorId": "Jira Ticket",
                        "createdDate": "Created Date",
                        "updatedDate": "Last Updated",
                    }
                )
            )

        elif inventory_type == 'assets':
            # Extracting the "technical owner" for each asset entry
            df_inventory['technicalOwner'] = df_inventory['technicalOwner'].apply(lambda x: x[0]['id'])

            # Filling NAN entries in the "Description" field with "asset_description_not_set" value
            df_inventory['description'] = df_inventory['description'].fillna("asset_description_not_set")

            # Filling NAN entries in the "Asset Type" field with "asset_not_set" value
            df_inventory['type'] = df_inventory['type'].fillna(
                {i: [{'id': 'id_not_set', "value": "asset_type_not_set", 'valueKey': 'value_key_not_set'}]
                 for i in df_inventory.index}
            )
            # Extracting the "Asset type" for each asset entry
            df_inventory['type'] = df_inventory['type'].apply(lambda x: x[0]['value'])

            # Filling NAN entries in the "MS Graph API Annual Permissions Review" field with "not_set" value
            df_inventory['customField1001'] = df_inventory['customField1001'].fillna(
                {i: [{'id': 'id_not_set', "value": "not_set", 'valueKey': 'value_key_not_set'}]
                 for i in df_inventory.index}
            )
            # Extracting the "MS Graph API Annual Permissions Review" for each asset entry
            df_inventory['customField1001'] = df_inventory['customField1001'].apply(lambda x: x[0]['value'])

            # Filtering the inventory data frame to only consider entries that are active or pending
            df_inventory_filtered = df_inventory[
                ((df_inventory['status.key'] == 'active') | (df_inventory['status.key'] == 'pending'))
                & (df_inventory['customField1001'] == 'Yes')
                ]

            # Inner join of the vendors and users dataframe on the owner and id columns respectively
            df_merged = pd.merge(df_inventory_filtered, df_users, left_on='technicalOwner', right_on='id')

            # Rearrange columns
            new_column_order = ['name',
                                'userName',
                                'organization.value',
                                'description',
                                'type',
                                'customField1001',
                                ]
            df = (
                df_merged[new_column_order]
                .rename(  # rename columns
                    columns={
                        "name": "Asset Name",
                        "userName": "Technical Owner",
                        "organization.value": "Organization",
                        "description": "Description",
                        "type": "Asset Type",
                        "customField1001": "MS Graph API Annual Permissions Review",
                    }
                )
            )
            message_suffix = "Approved Assets"

    df.index = range(1, len(df) + 1)  # Setting 1-based index instead of the default 0-based index
    # print(df.columns)
    print(f"There are {df.shape[0]} {message_suffix}.")  # get number of approved vendors/assets/AI assessments
    return df
