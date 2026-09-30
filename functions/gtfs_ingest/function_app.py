import io
import logging
import os
import zipfile
from datetime import datetime, timezone

import azure.functions as func
import requests
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.filedatalake import DataLakeServiceClient

app = func.FunctionApp()

REALTIME_FEEDS = ["vehiclepos", "realtime", "alerts"]
SCHEDULE_FILES = ["stops.txt", "routes.txt", "trips.txt", "stop_times.txt"]

# Created lazily and reused across invocations on a warm instance
_credential = None
_landing_client = None
_headers = None


def _timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def _get_credential():
    global _credential
    if _credential is None:
        # On Azure this resolves to the function app's managed identity
        _credential = DefaultAzureCredential()
    return _credential


def _get_landing_client():
    global _landing_client
    if _landing_client is None:
        account_url = f"https://{os.environ['STORAGE_ACCOUNT']}.dfs.core.windows.net"
        service_client = DataLakeServiceClient(account_url, _get_credential())
        _landing_client = service_client.get_file_system_client("landing")
    return _landing_client


def _get_headers() -> dict:
    global _headers
    if _headers is None:
        kv_uri = f"https://{os.environ['KEY_VAULT_NAME']}.vault.azure.net"
        secret_client = SecretClient(vault_url=kv_uri, credential=_get_credential())
        api_key = secret_client.get_secret("nsw-transport-api-key").value
        _headers = {"Authorization": f"apikey {api_key}"}
    return _headers


def _upload_bytes(path: str, data: bytes) -> None:
    file_client = _get_landing_client().get_file_client(path)
    file_client.upload_data(data, overwrite=True)
    logging.info("Uploaded %d bytes -> %s", len(data), path)


def _fetch(name: str, timeout: int) -> bytes:
    url = os.environ[f"API_URL_{name.upper()}"]
    resp = requests.get(url, headers=_get_headers(), timeout=timeout)
    resp.raise_for_status()
    return resp.content


@app.timer_trigger(
    schedule="0 */5 * * * *",  # every 5 minutes (seconds field comes first)
    arg_name="timer",
    run_on_startup=False,
    use_monitor=False,
)
def fetch_realtime(timer: func.TimerRequest) -> None:
    run_id = _timestamp()
    failed = []

    for name in REALTIME_FEEDS:
        # One failing feed shouldn't stop the others from being fetched
        try:
            content = _fetch(name, timeout=30)
            fetched_at = _timestamp()
            _upload_bytes(
                f"gtfs/{name}/run_id={run_id}/fetched_at={fetched_at}/data.pb",
                content,
            )
        except Exception:
            logging.exception("Failed to fetch feed %s", name)
            failed.append(name)

    if failed:
        raise RuntimeError(f"Feeds failed: {failed}")


@app.timer_trigger(
    # schedule="0 0 17 * * *",  # daily at 17:00 UTC, about 03:00-04:00 in Sydney
    schedule="0 */5 * * * *",  # every 5 minutes (seconds field comes first)
    arg_name="timer",
    run_on_startup=False,
    use_monitor=False,
)
def fetch_schedule(timer: func.TimerRequest) -> None:
    run_id = _timestamp()
    content = _fetch("schedule", timeout=300)
    fetched_at = _timestamp()

    with zipfile.ZipFile(io.BytesIO(content)) as z:
        logging.info("Zip contains: %s", z.namelist())
        for filename in SCHEDULE_FILES:
            try:
                file_bytes = z.read(filename)
            except KeyError:
                logging.warning("%s: not found in ZIP", filename)
                continue

            _upload_bytes(
                f"gtfs/schedule/run_id={run_id}/fetched_at={fetched_at}/{filename}",
                file_bytes,
            )