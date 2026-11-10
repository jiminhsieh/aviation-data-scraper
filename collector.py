"""OpenSky source-shaped snapshot collector; run with python -m collector."""
import argparse
from datetime import datetime, timezone
from io import BytesIO
import json
import logging
import math
import os
from pathlib import Path
import time
from uuid import uuid4

from azure.core.exceptions import ResourceExistsError
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from fastavro import reader, writer
import requests

SCHEMA = json.loads((Path(__file__).parent / "schema/states.avsc").read_text())
TOKEN_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
STATES_URL = "https://opensky-network.org/api/states/all"
BOX = {"lamin": -11, "lamax": 21, "lomin": 92, "lomax": 141}
LOG = logging.getLogger("collector")


class ConflictError(ValueError):
    def __init__(self, path):
        super().__init__("conflicting canonical snapshot")
        self.quarantine_path = path


def validate(data):
    if not isinstance(data, dict) or set(data) != {"time", "states"}:
        raise ValueError("response must contain only time and states")
    stamp, states = data["time"], data["states"]
    if type(stamp) is not int or not 0 <= stamp < 2**63:
        raise ValueError("invalid snapshot time")
    try:
        datetime.fromtimestamp(stamp, timezone.utc)
    except (ValueError, OverflowError, OSError) as exc:
        raise ValueError("invalid snapshot time") from exc
    if states is not None and not isinstance(states, list):
        raise ValueError("states must be an array or null")
    for state in states or []:
        if not isinstance(state, list) or len(state) != 17:
            raise ValueError("state must have 17 positions")
        for i, value in enumerate(state):
            if value is None and i in (1, 3, 5, 6, 7, 9, 10, 11, 12, 13, 14):
                continue
            if i in (0, 1, 2, 14):
                ok = isinstance(value, str)
            elif i in (3, 4, 16):
                ok = type(value) is int
            elif i in (8, 15):
                ok = type(value) is bool
            elif i == 12:
                ok = isinstance(value, list) and all(type(v) is int and -(2**63) <= v < 2**63 for v in value)
            else:
                ok = type(value) in (int, float)
            if ok and type(value) is int and not -(2**63) <= value < 2**63:
                ok = False
            if ok and type(value) is float and not math.isfinite(value):
                ok = False
            if not ok:
                raise ValueError(f"invalid state position {i}")
    return data


def encode(data):
    output = BytesIO()
    writer(output, SCHEMA, [validate(data)], codec="snappy")
    return output.getvalue()


def decode(payload):
    records = list(reader(BytesIO(payload)))
    if len(records) != 1:
        raise ValueError("expected exactly one Avro record")
    return validate(records[0])


def path_for(stamp):
    dt = datetime.fromtimestamp(stamp, timezone.utc)
    return f"opensky/states/year={dt:%Y}/month={dt:%m}/day={dt:%d}/hour={dt:%H}/{stamp}.avro"


def quarantine(container, original, reason):
    name = f"opensky/{reason}/{datetime.now(timezone.utc):%Y/%m/%d}/{uuid4()}.json"
    container.upload_blob(name, original, overwrite=False)
    return name


def publish(bronze, restricted, data, original):
    path = path_for(data["time"])
    try:
        bronze.upload_blob(path, encode(data), overwrite=False)
        return "created", path
    except ResourceExistsError:
        # Azure's conditional Put Blob creates a complete blob or none; a read is not a lock.
        existing = decode(bronze.download_blob(path).readall())
        if existing == data:
            return "identical", path
        raise ConflictError(quarantine(restricted, original, "conflict"))


def fetch(session, client_id, client_secret, deadline, now=time.monotonic, sleep=time.sleep):
    token = session.post(TOKEN_URL, data={"grant_type": "client_credentials", "client_id": client_id,
                                          "client_secret": client_secret}, timeout=(5, 10))
    token.raise_for_status()
    bearer = token.json()["access_token"]
    # One retry on one of 240 six-minute slots per UTC day; the pod itself never retries.
    retry_slot = datetime.now(timezone.utc).minute % 6 == 0
    for attempt in range(1 + int(retry_slot)):
        remaining = None
        try:
            response = session.get(STATES_URL, params=BOX, headers={"Authorization": f"Bearer {bearer}"},
                                   timeout=(5, 10))
            remaining = response.headers.get("X-Rate-Limit-Remaining")
            if response.status_code == 429:
                delay = float(response.headers["X-Rate-Limit-Retry-After-Seconds"])
                if delay < 0:
                    raise ValueError("invalid retry delay")
            elif 500 <= response.status_code < 600:
                delay = 2
            else:
                response.raise_for_status()
                return response.content, remaining
        except (requests.Timeout, requests.ConnectionError):
            delay = 2
        if attempt + 1 >= 1 + int(retry_slot) or now() + delay + 15 >= deadline:
            raise RuntimeError("states request failed or retry exceeds deadline")
        sleep(delay)
    raise RuntimeError("states request failed")


def run(account, bronze_name, quarantine_name, client_id_file, client_secret_file,
        session=None, service=None, now=time.monotonic):
    started = now()
    result = {"event": "archival", "outcome": "failed"}
    try:
        client_id = Path(client_id_file).read_text().strip()
        client_secret = Path(client_secret_file).read_text().strip()
        if not client_id or not client_secret:
            raise ValueError("empty client credentials")
        session = session or requests.Session()
        original, credits = fetch(session, client_id, client_secret, started + 65, now=now)
        result["fetch_duration_seconds"] = round(now() - started, 3)
        result["fetch_at_utc"] = datetime.now(timezone.utc).isoformat()
        result["remaining_credits"] = credits
        service = service or BlobServiceClient(f"https://{account}.blob.core.windows.net",
                                                credential=DefaultAzureCredential())
        bronze = service.get_container_client(bronze_name)
        restricted = service.get_container_client(quarantine_name)
        try:
            data = validate(json.loads(original))
        except (ValueError, TypeError, UnicodeDecodeError) as exc:
            result["quarantine_path"] = quarantine(restricted, original, "invalid")
            raise ValueError("invalid source response") from exc
        result["source_time"] = data["time"]
        result["source_age_seconds"] = int(datetime.now(timezone.utc).timestamp()) - data["time"]
        result["aircraft_count"] = len(data["states"] or [])
        result["bronze_path"] = path_for(data["time"])
        upload_started = now()
        outcome, path = publish(bronze, restricted, data, original)
        result["upload_duration_seconds"] = round(now() - upload_started, 3)
        result.update(outcome=outcome, bronze_path=path)
        return result
    except Exception as exc:
        # SDK/HTTP exception messages may include URLs, response bodies or secrets.
        if isinstance(exc, ConflictError):
            result.update(outcome="conflict", quarantine_path=exc.quarantine_path)
        result["error_type"] = type(exc).__name__
        raise
    finally:
        result["duration_seconds"] = round(now() - started, 3)
        LOG.info(json.dumps(result, sort_keys=True))


def main():
    parser = argparse.ArgumentParser(description="Archive one OpenSky states response in Bronze")
    parser.add_argument("--account", default=os.environ.get("AZURE_STORAGE_ACCOUNT"), required=False)
    parser.add_argument("--bronze-container", default=os.environ.get("BRONZE_CONTAINER", "bronze"))
    parser.add_argument("--quarantine-container", default=os.environ.get("QUARANTINE_CONTAINER", "quarantine"))
    parser.add_argument("--client-id-file", default=os.environ.get("OPENSKY_CLIENT_ID_FILE", "/mnt/secrets/client_id"))
    parser.add_argument("--client-secret-file", default=os.environ.get("OPENSKY_CLIENT_SECRET_FILE", "/mnt/secrets/client_secret"))
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    try:
        if not args.account:
            raise ValueError("missing --account or AZURE_STORAGE_ACCOUNT")
        run(args.account, args.bronze_container, args.quarantine_container,
            args.client_id_file, args.client_secret_file)
    except Exception:
        # Never let requests/Azure SDK traceback disclose authentication material.
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
