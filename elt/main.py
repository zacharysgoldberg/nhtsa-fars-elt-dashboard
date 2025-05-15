# main.py
from datetime import datetime
import pandas as pd
from db.init_db import init_db
from ingest.get_fars_data import download_and_extract_fars_data, load_data
from process.clean_fars_data import clean_accident_data, clean_vehicle_data
from process.transform_fars_data import standardize_fars_accident_data, standardize_fars_vehicle_data
from storage.save_data import save_to_db
from util.helpers import summarize_accident_data, summarize_vehicle_data, should_download_year, get_latest_downloaded_year


def main():
    # --- Download raw data ---
    current_year = datetime.now().year
    target_year = current_year - 1  # FARS data is typically published the year after

    if should_download_year(target_year):
        result = download_and_extract_fars_data(target_year)
    else:
        print(f"\nYear {target_year} already downloaded. Skipping download.\n")
        print("Result: ", result)

    # years = range(2016, current_year)
    # for year in years:
    #     result = download_and_extract_fars_data(year)
    #     print(f"{year} files: {result}")

    # --- Standardize raw data ---
    standardize_fars_accident_data()
    standardize_fars_vehicle_data()

    # --- Load standardized data ---
    standardized_accident_file_path = 'data/processed/fars_data/standardized/standardized_accident_data.csv'
    standardized_vehicle_file_path = 'data/processed/fars_data/standardized/standardized_vehicle_data.csv'

    standardized_accident_df = load_data(standardized_accident_file_path)
    standardized_vehicle_df = load_data(standardized_vehicle_file_path)

    # # --- Clean standardized data ---
    clean_accident_data(standardized_accident_df)
    clean_vehicle_data(standardized_vehicle_df)

    cleaned_accident_file_path = 'data/processed/fars_data/cleaned/cleaned_accident_data.csv'
    cleaned_vehicle_file_path = 'data/processed/fars_data/cleaned/cleaned_vehicle_data.csv'

    # --- Initialize Database ---
    cleaned_accident_df = load_data(cleaned_accident_file_path)
    cleaned_vehicle_df = load_data(cleaned_vehicle_file_path)

    # summarize_accident_data(cleaned_accident_df)
    # summarize_vehicle_data(cleaned_vehicle_df)

    init_db(cleaned_accident_df, cleaned_vehicle_df)

    # --- Save to database ---
    save_to_db(cleaned_accident_df, table='accident')
    save_to_db(cleaned_vehicle_df, table='vehicle')


if __name__ == "__main__":
    main()
