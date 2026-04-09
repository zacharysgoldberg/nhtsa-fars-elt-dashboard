import json
from datetime import datetime, timezone
from io import StringIO

from azure.storage.blob import BlobServiceClient

from util.helpers import upload_to_blob

CONTAINER_NAME = "processed-data"
MANIFEST_BLOB_PATH = "metadata/latest_adf_manifest.json"


def publish_adf_manifest(
    blob_service_client: BlobServiceClient,
    target_year: int,
    accident_rows: int,
    vehicle_rows: int,
):
    manifest = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "target_year": target_year,
        "cleaned_files": {
            "accident": {
                "container": CONTAINER_NAME,
                "blob_path": "cleaned/cleaned_accident.csv",
                "row_count": accident_rows,
            },
            "vehicle": {
                "container": CONTAINER_NAME,
                "blob_path": "cleaned/cleaned_vehicle.csv",
                "row_count": vehicle_rows,
            },
        },
        "adf_targets": {
            "accident_stage_table": "dbo.accident_stage",
            "vehicle_stage_table": "dbo.vehicle_stage",
            "accident_merge_procedure": "dbo.usp_merge_accident_stage",
            "vehicle_merge_procedure": "dbo.usp_merge_vehicle_stage",
        },
    }

    buffer = StringIO(json.dumps(manifest, indent=2))
    upload_to_blob(
        data=buffer,
        blob_path=MANIFEST_BLOB_PATH,
        blob_service_client=blob_service_client,
        container_name=CONTAINER_NAME,
        content_type="application/json",
    )

    print(
        f"ADF handoff manifest uploaded to {CONTAINER_NAME}/{MANIFEST_BLOB_PATH}"
    )
