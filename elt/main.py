import psycopg2
import traceback
from datetime import datetime
from db.init_db import init_db
from ingest.get_fars_data import download_and_extract_fars_data, load_data
from process.clean_fars_data import clean_accident_data, clean_vehicle_data
from process.transform_fars_data import standardize_fars_accident_data, standardize_fars_vehicle_data
from storage.save_data import save_to_db
from util.helpers import should_download_year, get_latest_downloaded_year
from azure.storage.blob import BlobServiceClient
from config.config import AZURE_STORAGE_CONNECTION_STRING, DB_CONFIG


"""
def original_elt():
    ''' Extract and Load '''

    # --- Download (Extract and Load) raw data into Blob Storage ---
    current_year = datetime.now().year
    target_year = current_year - 1  # FARS data is typically published the year after

    blob_service_client = BlobServiceClient.from_connection_string(
        AZURE_STORAGE_CONNECTION_STRING)

    # years = range(2016, target_year)
    # for year in years:
    #     result = download_and_extract_fars_data(year, blob_service_client)
    #     print(f"{year} files: {result}")

    try:
        if should_download_year(target_year, blob_service_client):
            result = download_and_extract_fars_data(
                target_year, blob_service_client)
        else:
            print(
                f"\nYear {target_year} already downloaded. Skipping download.\n")
            print("Result: ", result)

        ''' Transform '''
        # --- Standardize raw data ---
        standardized_accident_df = standardize_fars_accident_data(
            blob_service_client)
        standardized_vehicle_df = standardize_fars_vehicle_data(
            blob_service_client)

        standardized_accident_file_path = 'standardized/standardized_accident.csv'
        standardized_vehicle_file_path = 'standardized/standardized_vehicle.csv'

        # standardized_accident_df = load_data(
        #     standardized_accident_file_path, blob_service_client)
        # standardized_vehicle_df = load_data(
        #     standardized_vehicle_file_path, blob_service_client)

        # --- Clean standardized data ---
        cleaned_accident_df = clean_accident_data(
            standardized_accident_df, blob_service_client)
        cleaned_vehicle_df = clean_vehicle_data(
            standardized_vehicle_df, blob_service_client)

        cleaned_accident_file_path = 'cleaned/cleaned_accident.csv'
        cleaned_vehicle_file_path = 'cleaned/cleaned_vehicle.csv'

        # cleaned_accident_df = load_data(
        #     cleaned_accident_file_path, blob_service_client)
        # cleaned_vehicle_df = load_data(
        #     cleaned_vehicle_file_path, blob_service_client)

        # --- Save to database ---
        conn = psycopg2.connect(**DB_CONFIG)

        init_db(cleaned_accident_df, cleaned_vehicle_df, conn)

        save_to_db(cleaned_accident_df, 'accident', conn)
        save_to_db(cleaned_vehicle_df, 'vehicle', conn)

        conn.close()

    except:
        print()
"""


def main():
    ''' Extract and Load '''

    current_year = datetime.now().year
    target_year = current_year - 1  # FARS data is typically published the year after

    try:
        blob_service_client = BlobServiceClient.from_connection_string(
            AZURE_STORAGE_CONNECTION_STRING
        )

        if should_download_year(target_year, blob_service_client):
            try:
                result = download_and_extract_fars_data(
                    target_year, blob_service_client)
                print(
                    f"\nDownloaded and extracted data for year {target_year}.\n")
            except Exception as e:
                print(
                    f"\n❌ Failed to download/extract FARS data for year {target_year}.")
                print(f"Reason: {e}")
                traceback.print_exc()
                return  # Exit early, since there's no point in continuing
        else:
            print(
                f"\nYear {target_year} already downloaded. Skipping download.\n")
            print("Result: ", result)

        ''' Transform '''
        # Standardize raw data
        standardized_accident_df = standardize_fars_accident_data(
            blob_service_client)
        standardized_vehicle_df = standardize_fars_vehicle_data(
            blob_service_client)

        # Clean standardized data
        cleaned_accident_df = clean_accident_data(
            standardized_accident_df, blob_service_client)
        cleaned_vehicle_df = clean_vehicle_data(
            standardized_vehicle_df, blob_service_client)

        # Save to database
        conn = psycopg2.connect(**DB_CONFIG)
        init_db(cleaned_accident_df, cleaned_vehicle_df, conn)
        save_to_db(cleaned_accident_df, 'accident', conn)
        save_to_db(cleaned_vehicle_df, 'vehicle', conn)
        conn.close()

    except Exception as e:
        print("\n❌ A general error occurred during ELT pipeline execution.")
        print(f"Reason: {e}")
        traceback.print_exc()


if __name__ == "__main__":
    main()
