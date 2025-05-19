from io import BytesIO, StringIO
from typing import Union
from pathlib import Path
from typing import Optional, Union
from azure.storage.blob import BlobServiceClient
from azure.storage.blob import ContentSettings


def should_download_year(year: int, blob_service_client) -> bool:
    """
    Check if blobs exist under the 'folder' (prefix) for the given year in Azure Blob Storage.
    Returns True if no blobs exist under that prefix (i.e., data should be downloaded).
    """

    prefix = f"raw-data/{year}/"
    container_client = blob_service_client.get_container_client('raw-data')
    blobs_list = list(container_client.list_blobs(name_starts_with=prefix))

    return len(blobs_list) == 0


def get_latest_downloaded_year(blob_service_client, base_path: str = "raw-data/") -> Optional[int]:
    """
    Find the latest year folder present in the blob container under base_path.
    """

    blob_prefixes = set()
    container_client = blob_service_client.get_container_client('raw-data')
    blobs = container_client.list_blobs(name_starts_with=base_path)

    for blob in blobs:
        # Blob name looks like raw-data/2023/filename.csv
        # Extract the folder part after base_path
        relative_path = blob.name[len(base_path):]
        parts = relative_path.split('/')
        if parts and parts[0].isdigit():
            blob_prefixes.add(int(parts[0]))

    return max(blob_prefixes) if blob_prefixes else None


def upload_to_blob(
    data: Union[Path, str, BytesIO, StringIO],
    blob_path: str,
    blob_service_client: BlobServiceClient,
    container_name: str,
    content_type: str = "text/csv"
):
    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(blob_path)

    if isinstance(data, (Path, str)):
        # Assume local file path
        with open(data, "rb") as f:
            blob_client.upload_blob(
                f, overwrite=True, content_settings=ContentSettings(content_type=content_type)
            )
    else:
        # Assume in-memory buffer (StringIO or BytesIO)
        data.seek(0)
        blob_client.upload_blob(
            data.read(), overwrite=True, content_settings=ContentSettings(content_type=content_type)
        )

    print(f"Uploaded to blob: {blob_path}")
