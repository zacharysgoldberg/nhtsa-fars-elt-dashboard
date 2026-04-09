import sys
import traceback
from datetime import datetime
from ingest.get_fars_data import download_and_extract_fars_data
from process.clean_fars_data import clean_accident_data, clean_vehicle_data
from process.transform_fars_data import standardize_fars_accident_data, standardize_fars_vehicle_data
from elt.storage.publish_to_adf import publish_adf_manifest
from util.helpers import should_download_year
from azure.storage.blob import BlobServiceClient
from config.config import AZURE_STORAGE_CONNECTION_STRING


def main():
    ''' Extract and Load '''

    current_year = datetime.now().year
    # target_year = current_year - 1
    target_year = 2023

    extracted_files = {}

    blob_service_client = BlobServiceClient.from_connection_string(
        AZURE_STORAGE_CONNECTION_STRING
    )

    if should_download_year(target_year, blob_service_client):
        extracted_files = download_and_extract_fars_data(
            target_year, blob_service_client)

        print(f"\nResult: {extracted_files}\n")

    else:
        print(
            f"\nℹ️ Year {target_year} already downloaded. Skipping download.\n")

    ''' Transform '''

    if extracted_files:
        print(
            f"\n✅ Downloaded and extracted data for year {target_year}.\n")
        try:
            print(
                "\nProceeding with data transformations\n")
            standardized_accident_df = standardize_fars_accident_data(
                blob_service_client)
            standardized_vehicle_df = standardize_fars_vehicle_data(
                blob_service_client)

            cleaned_accident_df = clean_accident_data(
                standardized_accident_df, blob_service_client)
            cleaned_vehicle_df = clean_vehicle_data(
                standardized_vehicle_df, blob_service_client)
            publish_adf_manifest(
                blob_service_client=blob_service_client,
                target_year=target_year,
                accident_rows=len(cleaned_accident_df),
                vehicle_rows=len(cleaned_vehicle_df),
            )
            print(
                "\nADF handoff complete. Load the cleaned Blob files into Azure SQL staging with Azure Data Factory.\n"
            )

        except Exception as e:
            print(f"\n❌ Error during transform/load phases: {e}")
            traceback.print_exc()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n🚨 Uncaught exception in main: {e}")
        traceback.print_exc()
        sys.exit(0)  # Exit successfully even on error
