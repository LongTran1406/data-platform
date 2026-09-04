import requests
from google.transit import gtfs_realtime_pb2
import zipfile
import io
from pathlib import Path
import argparse
import sys
from datetime import datetime, timezone
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient
from azure.keyvault.secrets import SecretClient

parser = argparse.ArgumentParser()
parser.add_argument("--env", default="staging")

args = parser.parse_args()
env = args.env

try:
    _script_path = Path(__file__).resolve()
except NameError:
    # Databricks spark_python_task execs the file without setting __file__,
    # but populates sys.argv[0] with the script's absolute workspace path.
    _script_path = Path(sys.argv[0]).resolve()

PROJECT_ROOT = _script_path.parents[2]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from dataplatform.config import load_config


ENDPOINTS = load_config(env)

KEY_VAULT_NAME = ENDPOINTS["key_vault_name"]
KV_URI = f"https://{KEY_VAULT_NAME}.vault.azure.net"

credential = DefaultAzureCredential()
secret_client = SecretClient(vault_url=KV_URI, credential=credential)

API_KEY = secret_client.get_secret("nsw-transport-api-key").value


HEADERS = {
    "Authorization": f"apikey {API_KEY}"
}


run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

# Set up ADLS client
storage_account = ENDPOINTS["storage_account"]
account_url = f"https://{storage_account}.dfs.core.windows.net"
service_client = DataLakeServiceClient(account_url, credential)
file_system_client = service_client.get_file_system_client("landing")

def upload_bytes(path: str, data: bytes):
    file_client = file_system_client.get_file_client(path)
    file_client.upload_data(data, overwrite=True)
    print(f"Uploaded {len(data)} bytes -> {path}")


def fetch_schedule():
    resp = requests.get(ENDPOINTS["apis"]["schedule"], headers=HEADERS)
    resp.raise_for_status()

    fetched_at = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    zip_buffer = io.BytesIO(resp.content)
    files_to_extract = ["stops.txt", "routes.txt", "trips.txt", "stop_times.txt"]

    with zipfile.ZipFile(zip_buffer) as z:
        print("Contains:", z.namelist())
        for filename in files_to_extract:
            try:
                with z.open(filename) as f:
                    file_bytes = f.read()

                output_path = (
                    f"gtfs/schedule/"
                    f"run_id={run_id}/"
                    f"fetched_at={fetched_at}/"
                    f"{filename}"
                )
                upload_bytes(output_path, file_bytes)
            except KeyError:
                print(f"{filename}: not found in ZIP")


def fetch_realtime_feed(name):
    resp = requests.get(ENDPOINTS["apis"][name], headers=HEADERS)
    resp.raise_for_status()

    fetched_at = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_path = (
        f"gtfs/{name}/"
        f"run_id={run_id}/"
        f"fetched_at={fetched_at}/"
        f"data.pb"
    )
    upload_bytes(output_path, resp.content)


if __name__ == "__main__":
    fetch_schedule()
    for feed_name in ["vehiclepos", "realtime", "alerts"]:
        fetch_realtime_feed(feed_name)