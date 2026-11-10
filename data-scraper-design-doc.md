# Aviation Data Scraper

Collect OpenSky aircraft snapshots and archive them directly to the ADLS Gen2 Bronze zone. Ingestion ends at durable storage: a separate continuous Spark stream owns Bronze → Silver, and an independent scheduled SQL job owns Silver → Gold. This collector does not publish to Kafka or trigger Databricks jobs.

**Status:** This is design background. For the runnable collector, actual schema, tests, chart and verified local commands see [README.md](README.md). No live deployment is claimed.

## Tech Stack

Python, Docker, `fastavro` with Snappy support, Azure SDK, AKS CronJob, ADLS Gen2, and Azure Key Vault with Workload Identity / Secrets Store CSI.

## Core Logic

1. Read OpenSky client credentials from mounted Key Vault secrets and obtain an OAuth2 client-credentials bearer token. Keep the token in memory.
2. Fetch `GET https://opensky-network.org/api/states/all?lamin=-11&lamax=21&lomin=92&lomax=141`. The bounding box is fixed by the assignment.
3. Validate the response timestamp and all 17 state-vector positions/types. Preserve source values, including nulls and padded callsigns; cleansing and observation-level deduplication belong downstream.
4. Encode the original response as one Snappy-compressed Avro object container file, without adding fields or reshaping `states`.
5. Atomically create the canonical Bronze object if absent. Report success only after durable upload or verification that an identical snapshot already exists.

## Scheduling and API Budget

- Run a Kubernetes CronJob on `*/2 * * * *`, with `timeZone: Etc/UTC`, `concurrencyPolicy: Forbid`, and a short total execution deadline.
- One normal states request per run gives **720 requests/day × 4 credits = 2,880 credits/day** against the registered-account allowance of **4,000 credits/day**.
- The remaining **1,120 credits** allow at most **280 additional states requests/day**. Application retries, Kubernetes pod retries and manual validation calls all consume that headroom; do not give each retry layer an independent budget.
- Bound network/5xx retries. On `429`, respect `X-Rate-Limit-Retry-After-Seconds` rather than immediately retrying. Record remaining credits when available.
- A two-minute polling cadence is not a guarantee of end-to-end Gold freshness.

## Bronze Data Contract

Each file contains one Avro record with **exactly the original response structure**, using only these two top-level fields:

| Field | Type / meaning |
|---|---|
| `time` | Long; original API snapshot timestamp in Unix seconds |
| `states` | Null or an array of positional state-vector arrays, exactly as returned by OpenSky |

Do not add metadata fields or convert aircraft arrays into named records. Fetch times and operational status belong in logs, not the Bronze payload.

Each state vector retains its 17 positions: `icao24`, `callsign`, `origin_country`, `time_position`, `last_contact`, `longitude`, `latitude`, `baro_altitude`, `on_ground`, `velocity`, `true_track`, `vertical_rate`, `sensors`, `geo_altitude`, `squawk`, `spi`, `position_source`. These names describe positions only; they do not become fields in Bronze.

Because state vectors contain mixed types, their Avro array item schema must use a union supporting the source scalar types, nulls and the nested `sensors` array. Validate the expected type at each position before encoding; an unexpected field count or type is a contract failure, not permission to truncate or coerce data.

Decoding the Avro record must reproduce the original JSON structure and values, including nulls, nested arrays and padded callsigns. The binary format changes, so it does **not** preserve the exact original JSON bytes. Snappy compression benefits must be measured rather than assumed.

### Hourly Partition Layout

```text
abfss://bronze@<storage-account>.dfs.core.windows.net/
  opensky/states/year=YYYY/month=MM/day=DD/hour=HH/<response.time>.avro
```

Derive **year, month, day and hour from `response.time` in UTC**, never from upload time or an aircraft's `last_contact`. Late arrivals and retries must use the original snapshot's hour, including across hour/day boundaries.

For the fixture below, `1793013825` is `2026-10-26T11:23:45Z`, so its object path is:

```text
opensky/states/year=2026/month=10/day=26/hour=11/1793013825.avro
```

Hourly partitioning groups snapshot files; it does not combine them into one file or overwrite the hour. At the normal cadence, each hour contains approximately 30 files.

### Duplicate, Conflict and Failure Handling

- Use conditional create-if-absent uploads; **never overwrite canonical Bronze objects** or expose a partially uploaded final object.
- If the path exists, decode its Avro record and compare its `time` and `states` content with the incoming response. Do not compare Avro bytes: container framing can differ between encodes.
- Identical content: idempotent success. Track repeated source timestamps so successful requests do not hide stale source data.
- Different content for the same timestamp: preserve the canonical object, archive the conflicting response under a unique path in the restricted quarantine container, alert, and fail the run. Quarantine must be outside normal Bronze discovery.
- Invalid timestamp, malformed payload or schema mismatch: retain the original response in quarantine, report the failure, and do not publish a canonical object.
- Valid `states: null` or `states: []`: archive the snapshot with zero aircraft. Do not fabricate observations.
- Authentication, Key Vault, quarantine or Bronze upload failures must produce a nonzero exit; never report an unarchived response as successful. Redact credentials and bearer tokens from errors.

## Example Source Response

[Sample API payload](example_response.json). Bronze stores this same structure and values in Avro format, with no added fields:
```json
{
  "time": 1793013825,
  "states": [
    [
      "8a044f",
      "GIA190  ",
      "Indonesia",
      1793013823,
      1793013825,
      99.9767,
      1.7853,
      10927.08,
      false,
      227.31,
      322.17,
      -5.85,
      null,
      11673.84,
      null,
      false,
      0
    ]
  ]
}
```

## Identity and Configuration

- Use a dedicated Kubernetes ServiceAccount federated to an Azure managed identity through AKS Workload Identity.
- Mount OpenSky `client_id` and `client_secret` from Key Vault through Secrets Store CSI. Never place secret values in Git, images, logs or Terraform state.
- Grant the ingestion identity scoped Bronze create/read access for uploads and collision checks, plus quarantine write access. It needs no Silver/Gold privileges or storage-account keys. Databricks uses a separate identity.
- Deployment inputs and exact CLI/environment variable names are in [README.md](README.md); the source path prefix is fixed by the contract.

## Implementation and Verification Checklist

Runnable setup, image-build and Helm commands are in [README.md](README.md); live provisioned validation remains outstanding.

- Add the collector, Avro schema, pinned dependencies, Dockerfile, ServiceAccount, SecretProviderClass and CronJob manifests.
- Test that the Avro round trip exactly preserves the fixture's `time`/`states` structure, all 17 positional values and their types, padded strings, nulls and nested arrays, without adding fields. Cover empty snapshots and rejection of incompatible schemas.
- Test UTC hourly paths at hour/day/month boundaries, duplicate uploads, conflicting content for the same timestamp, and preservation of canonical objects.
- Test `429`, timeouts/5xx, secret redaction and storage failures, including nonzero exits and bounded retries.
- Verify an AKS run creates one readable Bronze file using Workload Identity; repeating the same snapshot must not create another canonical file.
- Emit structured logs for snapshot/fetch times, outcome, aircraft count, path, request/upload duration and remaining credits when available. Keep these operational details out of the Bronze payload. Check the age of the latest distinct source snapshot as well as successful-run status.

Source contract: [OpenSky REST API documentation](https://openskynetwork.github.io/opensky-api/rest.html).
