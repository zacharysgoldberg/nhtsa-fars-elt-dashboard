import sys
import requests
import zipfile
import pandas as pd
from pathlib import Path
from io import BytesIO
import shutil
from azure.storage.blob import BlobServiceClient
from util.helpers import upload_to_blob


def download_and_extract_fars_data(year: int, blob_service_client: BlobServiceClient) -> dict:
    url = f"https://www.nhtsa.gov/file-downloads?p=nhtsa/downloads/FARS/{year}/National/FARS{year}NationalCSV.zip"

    temp_dir = "tmp/fars_data"
    year_dir = Path(temp_dir) / str(year)
    year_dir.mkdir(parents=True, exist_ok=True)
    zip_path = year_dir / f"FARS{year}NationalCSV.zip"

    try:
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        with open(zip_path, "wb") as f:
            f.write(response.content)
        print(f"Downloaded: {zip_path}")

    except requests.RequestException as e:
        raise RuntimeError(f"Failed to download data for {year}: {e}")

    extracted_files = {}

    try:
        with zipfile.ZipFile(zip_path, 'r') as zf:
            expected_files = {"accident.csv", "vehicle.csv"}
            available_files = set(Path(name).name.lower()
                                  for name in zf.namelist())

            if not expected_files & available_files:
                raise RuntimeError(
                    f"Expected files not found in ZIP archive for {year}")

            for member in zf.namelist():
                filename = Path(member).name.lower()
                if filename in expected_files:
                    temp_path = year_dir / filename
                    with zf.open(member) as source:
                        with open(temp_path, "wb") as target:
                            target.write(source.read())
                    print(f"Extracted: {temp_path}")

                    if filename == "vehicle.csv":
                        df = pd.read_csv(
                            temp_path, encoding='latin1', low_memory=False)
                        df["year"] = year
                        df.to_csv(temp_path, index=False)
                        print(
                            f"Appended 'year' column to vehicle.csv for {year}")

                    # Upload to blob storage
                    blob_path = f"{year}/{filename}"

                    upload_to_blob(
                        temp_path, blob_path, blob_service_client, container_name='raw-data')

                    extracted_files[filename.split('.')[0]] = blob_path

    except zipfile.BadZipFile as e:
        raise RuntimeError(f"Failed to extract {zip_path}: {e}")

    finally:
        # Clean up temp directory for the year (not individual files)
        if temp_path.exists():
            shutil.rmtree(temp_path)
            print(f"Remove temporary dir: {temp_path}")

    return extracted_files


def load_data(file_path: str, blob_service_client: BlobServiceClient) -> pd.DataFrame:
    """
    Load and optionally sample the vehicle and accident data.
    """

    try:
        container_client = blob_service_client.get_container_client(
            'processed-data')
        blob_client = container_client.get_blob_client(file_path)
        blob_data = blob_client.download_blob().readall()

        df = pd.read_csv(BytesIO(blob_data), low_memory=False)
        print(f"\nLoaded {len(df):,} rows from Azure Blob: {file_path}\n")

    except Exception as e:
        raise FileNotFoundError(
            f"Could not find file: {file_path}. Error: {e}")

    return df
