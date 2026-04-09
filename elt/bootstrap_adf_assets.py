import pyodbc

from config.config import conn_str
from db.adf_setup import create_merge_procedures, create_stage_tables
from db.init_db import init_db
from ingest.get_fars_data import load_processed_data
from azure.storage.blob import BlobServiceClient
from config.config import AZURE_STORAGE_CONNECTION_STRING


def main():
    blob_service_client = BlobServiceClient.from_connection_string(
        AZURE_STORAGE_CONNECTION_STRING
    )

    cleaned_accident_df = load_processed_data(
        "cleaned/cleaned_accident.csv", blob_service_client
    )
    cleaned_vehicle_df = load_processed_data(
        "cleaned/cleaned_vehicle.csv", blob_service_client
    )

    conn = pyodbc.connect(conn_str)
    try:
        init_db(cleaned_accident_df, cleaned_vehicle_df, conn)
        create_stage_tables(conn, cleaned_accident_df, cleaned_vehicle_df)
        create_merge_procedures(conn, cleaned_accident_df, cleaned_vehicle_df)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
