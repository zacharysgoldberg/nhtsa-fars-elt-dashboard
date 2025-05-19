import pandas as pd
from io import StringIO
from azure.storage.blob import BlobServiceClient


def clean_accident_data(df: pd.DataFrame, blob_service_client: BlobServiceClient) -> pd.DataFrame:
    container_name = 'processed-data'
    blob_path = 'cleaned/cleaned_accident.csv'

    # 1. Remove columns with >= 50% missing values
    threshold = 0.5 * len(df)
    df_cleaned = df.dropna(axis=1, thresh=threshold)

    # 2. Drop unnecessary columns if they exist
    columns_to_drop = [
        'tway_id', 'tway_id2', 'weather1', 'weather2',
        'cf1', 'cf2', 'cf3', 'Ã¯Â»Â¿state'
    ]
    df_cleaned = df_cleaned.drop(
        columns=[col for col in columns_to_drop if col in df_cleaned.columns],
        errors='ignore'
    )

    # 3. Upload cleaned data to Azure Blob Storage
    try:
        csv_buffer = StringIO()
        df_cleaned.to_csv(csv_buffer, index=False)

        processed_client = blob_service_client.get_container_client(
            container_name)
        processed_client.get_blob_client(blob_path).upload_blob(
            csv_buffer.getvalue(), overwrite=True
        )

        print(
            f"\n✅ Cleaned accident data uploaded to Azure at {container_name}/{blob_path}\n")
    except Exception as e:
        print(f"❌ Failed to upload cleaned accident data to Azure: {e}")

    return df_cleaned


def clean_vehicle_data(df: pd.DataFrame, blob_service_client: BlobServiceClient) -> pd.DataFrame:
    container_name = 'processed-data'
    blob_path = 'cleaned/cleaned_vehicle.csv'

    # Remove columns with 50% or more missing values
    threshold = 0.5 * len(df)  # 50% of the total rows
    df_cleaned = df.dropna(axis=1, thresh=threshold)

    # Handle specific values (e.g., replace 999 in 'travel_spd_mph' with "Unknown")
    if 'trav_sp' in df_cleaned.columns:
        df_cleaned['trav_sp'] = df_cleaned['trav_sp'].apply(
            lambda x: 0 if x is not None and x >= 997 else x)

    if 'mod_year' in df_cleaned.columns:
        df_cleaned['mod_year'] = df_cleaned['mod_year'].apply(
            lambda x: None if isinstance(x, str) and not x.isdigit() else (
                int(x) if str(x).isdigit() else None)
        )

    # Drop columns that are not useful for analysis
    columns_to_drop = ['gvwr', 'vehicle_config', 'mak_mod', 'vin_1', 'vin_2', 'vin_3', 'vin_4',
                       'vin_5', 'vin_6', 'vin_7', 'vin_8', 'vin_9', 'vin_10', 'vin_11', 'vin_12',
                       'trlr1vin', 'trlr2vin', 'trlr3vin', 'mcarr_i1', 'mcarr_i2', 'mcarr_i3',
                       'mcarr_id']

    # Drop columns from the list, if they exist in the dataframe
    df_cleaned = df_cleaned.drop(
        columns=[col for col in columns_to_drop if col in df_cleaned.columns])

    # Ensure the column names are standardized (optional)
    df_cleaned.columns = df_cleaned.columns.str.strip(
    ).str.lower()  # Make all column names lowercase

    # Handle any other necessary cleaning (e.g., set certain numeric columns to NaN if out-of-range)
    # For example, if 'deaths' column has impossible values, it can be cleaned
    if 'deaths' in df_cleaned.columns:
        df_cleaned['deaths'] = df_cleaned['deaths'].apply(
            lambda x: x if x >= 0 else None)

    # 3. Upload cleaned data to Azure Blob Storage
    try:
        csv_buffer = StringIO()
        df_cleaned.to_csv(csv_buffer, index=False)

        processed_client = blob_service_client.get_container_client(
            container_name)
        processed_client.get_blob_client(blob_path).upload_blob(
            csv_buffer.getvalue(), overwrite=True
        )

        print(
            f"\n✅ Cleaned accident data uploaded to Azure at {container_name}/{blob_path}\n")
    except Exception as e:
        print(f"❌ Failed to upload cleaned accident data to Azure: {e}")

    return df_cleaned
