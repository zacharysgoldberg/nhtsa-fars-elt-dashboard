import pandas as pd
from io import StringIO
from azure.storage.blob import BlobServiceClient
from elt.ingest.get_fars_data import load_blob_file_names, load_raw_data
from util.helpers import upload_to_blob

CONTAINER_NAME = 'processed-data'
ACCIDENT_BLOB_PATH = "standardized/standardized_accident.csv"
VEHICLE_BLOB_PATH = "standardized/standardized_vehicle.csv"


def _filter_and_rename(df: pd.DataFrame, numeric_whitelist: set) -> pd.DataFrame:
    """
    1) Keep any column in numeric_whitelist.
    2) Keep any column that endswith 'name' *only if* its base column (col[:-4]) is NOT in numeric_whitelist.
    3) Keep any other column that has NO corresponding 'name' variant.
    4) Drop numeric columns that have a 'name' variant but are NOT whitelisted.
    5) Finally, rename kept 'name' columns by stripping the 'name' suffix.
    """
    cols = df.columns.tolist()
    filtered = []
    for col in cols:
        # 1) numeric whitelist
        if col in numeric_whitelist:
            filtered.append(col)
            continue

        # 2) name columns
        if col.endswith('name'):
            base = col[:-4]
            if base not in numeric_whitelist:
                filtered.append(col)
            continue

        # 3) other columns: keep if they have no 'name' counterpart
        if f"{col}name" not in cols:
            filtered.append(col)
        # else: numeric col with a name-variant, drop it

    # Subset DF
    df = df[filtered]

    # 5) rename name-columns
    rename_map = {col: col[:-4] for col in filtered if col.endswith('name')}

    if rename_map:
        df = df.rename(columns=rename_map)

    return df


def standardize_fars_accident_data(blob_service_client: BlobServiceClient) -> pd.DataFrame:
    numeric_whitelist = {
        'month', 'day', 'hour', 'minute', 'not_hour', 'not_minute',
        'arr_hour', 'arr_minute', 'hosp_hour', 'hosp_minute'
    }

    all_dfs = []

    raw_container_client = blob_service_client.get_container_client('raw-data')

    blob_file_names = load_blob_file_names(raw_container_client, 'accident')

    for blob_name in blob_file_names:
        try:
            # Load raw data
            df = load_raw_data(raw_container_client, blob_name)

            df = _filter_and_rename(df, numeric_whitelist)

            all_dfs.append(df)

            print(f"Processed: {blob_name}")

        except Exception as e:
            print(f"Failed to process {blob_name}: {e}")

    if not all_dfs:
        print("No accident data files were processed.")
        return None

    final_df = pd.concat(all_dfs, ignore_index=True)

    if final_df.empty:
        print("No data after filtering; the final dataframe is empty.")
        return None

    # Prepare in-memory buffer for upload
    buffer = StringIO()
    final_df.to_csv(buffer, index=False)

    try:
        upload_to_blob(
            data=buffer,
            blob_path=ACCIDENT_BLOB_PATH,
            blob_service_client=blob_service_client,
            container_name=CONTAINER_NAME,
            content_type="text/csv"
        )
        print(
            f"Standardized accident data uploaded to {CONTAINER_NAME}/{ACCIDENT_BLOB_PATH}")

    except Exception as e:
        print(f"Failed to upload standardized accident data: {e}")

    return final_df


def standardize_fars_vehicle_data(blob_service_client: BlobServiceClient) -> pd.DataFrame:
    numeric_whitelist = {
        'month', 'day', 'hour', 'minute', 'trav_sp', 'mod_year', 'dr_weight', 'tow_veh', 'j_knife'
    }

    all_dfs = []

    raw_container_client = blob_service_client.get_container_client('raw-data')

    blob_file_names = load_blob_file_names(raw_container_client, 'vehicle')

    for blob_name in blob_file_names:
        try:
            # Load raw data
            df = load_raw_data(raw_container_client, blob_name)

        except Exception as e:
            print(f"Failed to read {blob_name}: {e}")
            continue

        # Extract model from mak_modname if present
        if 'mak_modname' in df.columns:
            def extract_model(val):
                if isinstance(val, str):
                    parts = val.split(' ', 1)
                    return parts[1].strip() if len(parts) > 1 else None
                return None
            df['model'] = df['mak_modname'].apply(extract_model)

        df = _filter_and_rename(df, numeric_whitelist)

        all_dfs.append(df)

    if not all_dfs:
        print("No vehicle data files were processed.")
        return None

    final_df = pd.concat(all_dfs, ignore_index=True)

    if final_df.empty:
        print("No data after filtering; the final dataframe is empty.")
        return None

    # Upload standardized vehicle data CSV to blob storage
    buffer = StringIO()
    final_df.to_csv(buffer, index=False)

    try:
        upload_to_blob(
            data=buffer,
            blob_path=VEHICLE_BLOB_PATH,
            blob_service_client=blob_service_client,
            container_name=CONTAINER_NAME,
            content_type="text/csv"
        )
        print(
            f"Standardized vehicle data uploaded to {CONTAINER_NAME}/{VEHICLE_BLOB_PATH}")

    except Exception as e:
        print(f"Failed to upload standardized vehicle data: {e}")

    return final_df
