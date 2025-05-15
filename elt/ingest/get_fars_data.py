import requests
import zipfile
import pandas as pd
from pathlib import Path
import os
import sys


def download_and_extract_fars_data(year: int, base_dir: str = "data/raw/fars_data"):
    """
    Downloads and extracts only ACCIDENT.CSV and VEHICLE.CSV from the FARS dataset for a given year.
    Files are extracted to fars_data/<year>/accident.csv and vehicle.csv.
    The vehicle CSV will be modified to include a 'year' column.

    Parameters:
        year (int): Year of the FARS dataset to download.
        base_dir (str): Base directory to store the data.

    Returns:
        dict: Paths to the extracted files, e.g., {'accident': Path(...), 'vehicle': Path(...)}
    """
    url = f"https://static.nhtsa.gov/nhtsa/downloads/FARS/{year}/National/FARS{year}NationalCSV.zip"
    year_dir = Path(base_dir) / str(year)
    year_dir.mkdir(parents=True, exist_ok=True)
    zip_path = year_dir / f"FARS{year}NationalCSV.zip"

    try:
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        with open(zip_path, "wb") as f:
            f.write(response.content)
        print(f"Downloaded: {zip_path}")
    except requests.RequestException as e:
        print(f"[ERROR] Failed to download data for {year}: {e}")
        sys.exit(1)  # Cancel the program

    extracted_files = {}

    try:
        with zipfile.ZipFile(zip_path, 'r') as zf:
            expected_files = {"accident.csv", "vehicle.csv"}
            available_files = set(Path(name).name.lower()
                                  for name in zf.namelist())

            if not expected_files & available_files:
                print(
                    f"[ERROR] Expected files not found in the ZIP archive for {year}. Exiting.")
                sys.exit(1)  # Cancel the program

            for member in zf.namelist():
                filename = Path(member).name.lower()
                if filename in expected_files:
                    target_path = year_dir / filename
                    with zf.open(member) as source:
                        with open(target_path, "wb") as target:
                            target.write(source.read())
                    print(f"Extracted: {target_path}")

                    if filename == "vehicle.csv":
                        df = pd.read_csv(
                            target_path, encoding='latin1', low_memory=False)
                        df["year"] = year
                        df.to_csv(target_path, index=False)
                        print(
                            f"Appended 'year' column to vehicle.csv for {year}")

                    extracted_files[filename.split('.')[0]] = target_path

    except zipfile.BadZipFile as e:
        print(f"[ERROR] Failed to extract {zip_path}: {e}")
        sys.exit(1)  # Cancel the program

    return extracted_files


def load_data(file_path: str, sample_size: int = None) -> pd.DataFrame:
    """
    Load and optionally sample the vehicle and accident data.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    # Load the CSV data into a DataFrame
    df = pd.read_csv(file_path, low_memory=False)

    print(f"\n\nLoaded {len(df):,} rows from {file_path}\n\n")

    if sample_size:
        df = df.sample(sample_size, random_state=42).copy()
        print(f"Sampled down to {sample_size} rows.")

    return df
