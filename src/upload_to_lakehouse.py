import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient


load_dotenv(Path(__file__).parent.parent / ".env", override=True)

ACCOUNT_NAME    = os.getenv("ONELAKE_ACCOUNT_NAME", "onelake")
WORKSPACE       = os.getenv("ONELAKE_WORKSPACE_NAME")
LAKEHOUSE       = os.getenv("ONELAKE_LAKEHOUSE_NAME")
CLIENT_ID       = os.getenv("FABRIC_CLIENT_ID")
CLIENT_SECRET   = os.getenv("FABRIC_CLIENT_SECRET")
TENANT_ID       = os.getenv("FABRIC_TENANT_ID")


def get_client():
    credential = ClientSecretCredential(TENANT_ID, CLIENT_ID, CLIENT_SECRET)
    return DataLakeServiceClient(
        account_url=f"https://{ACCOUNT_NAME}.dfs.fabric.microsoft.com",
        credential=credential
    )

def upload_file(fs_client, local_path: Path, remote_path: str):
    """Upload a single file to OneLake."""
    file_client = fs_client.get_file_client(remote_path)
    with open(local_path, "rb") as f:
        data = f.read()
        file_client.upload_data(data, overwrite=True)
    print(f"  uploaded: {local_path.name}")

def upload_folder(local_folder: str, remote_folder: str):
    """Upload all files in a local folder recursively to OneLake."""
    client      = get_client()
    fs_client   = client.get_file_system_client(WORKSPACE)
    local_path  = Path(local_folder)

    files = list(local_path.rglob("*"))
    files = [f for f in files if f.is_file()]

    print(f"\nUploading {len(files)} files from {local_folder}")
    print(f"Destination: {LAKEHOUSE}/Files/{remote_folder}\n")

    for i, file in enumerate(files, 1):
        relative  = file.relative_to(local_path)
        dest_path = f"{LAKEHOUSE}/Files/{remote_folder}/{relative}"
        upload_file(fs_client, file, dest_path)
        print(f"  [{i}/{len(files)}] {relative}")

    print(f"\nDone. {len(files)} files uploaded.")

# ── Dataset map: local folder → OneLake destination ──────────────────────────
DATASETS = {
    "police_crime":    ("ingestion/police_crime/raw",       "bronze/police_crime"),
    "companies_house": ("ingestion/companies_house/raw",    "bronze/companies_house"),
    "population":      ("ingestion/ons/population",         "bronze/ons/population"),
    "boundaries":      ("ingestion/ons/boundaries",         "bronze/ons/boundaries"),
    "deprivation":     ("ingestion/ons/deprivation",        "bronze/ons/deprivation"),
}

if __name__ == "__main__":
    # Run all datasets or pass a specific one as argument
    # Usage:
    #   python upload_to_onelake.py                  ← uploads everything
    #   python upload_to_onelake.py police_crime     ← uploads one dataset

    target = sys.argv[1] if len(sys.argv) > 1 else None

    if target:
        if target not in DATASETS:
            print(f"Unknown dataset: {target}")
            print(f"Available: {list(DATASETS.keys())}")
            sys.exit(1)
        local, remote = DATASETS[target]
        upload_folder(local, remote)
    else:
        for name, (local, remote) in DATASETS.items():
            print(f"\n{'='*50}")
            print(f"Dataset: {name}")
            upload_folder(local, remote)