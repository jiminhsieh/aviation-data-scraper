# OpenSky → immutable Bronze

**Status:** Runnable collector and Helm chart; no live AKS/ADLS run has been verified. The foundation/identity grants must be provisioned separately; this repository does not create cloud resources or inject secrets.

## Run locally / test

Python 3.12+ (tested locally with 3.15), an OpenSky registered OAuth2 client, an Azure storage account with `bronze` and restricted `quarantine` containers, and an Azure identity authorized to create/read Bronze and create quarantine blobs are required. No storage key is used. Avoid putting secrets or local state in Git.

```sh
cd ingestion
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
python -m unittest discover -s tests -v
python -m collector --help
# Provision secret files outside this repository with permissions 0600.
export AZURE_STORAGE_ACCOUNT='<account>'
export OPENSKY_CLIENT_ID_FILE='/secure/path/client_id'
export OPENSKY_CLIENT_SECRET_FILE='/secure/path/client_secret'
# Authenticate Azure SDK through az login (local), or Azure Workload Identity (AKS).
python -m collector
```

The CLI accepts `--account`, `--bronze-container`, `--quarantine-container`, `--client-id-file` and `--client-secret-file` (or the corresponding `AZURE_STORAGE_ACCOUNT`, `BRONZE_CONTAINER`, `QUARANTINE_CONTAINER`, `OPENSKY_CLIENT_ID_FILE`, `OPENSKY_CLIENT_SECRET_FILE` environment variables). It never accepts secret values on the command line. Exits 0 only for successfully created or decoded-identical Bronze; all failures exit nonzero with structured, redacted logs. Errors never print HTTP/SDK exceptions (which might contain secrets).

## Build and deploy

Requires Docker daemon, Helm, AKS Kubernetes ≥1.27 with Azure Workload Identity and Secrets Store CSI Azure provider, an ACR pull grant, Key Vault `opensky-client-id` and `opensky-client-secret` secrets, and federated identity subject `system:serviceaccount:aviation:collector-collector`. The identity needs Key Vault get and narrowly scoped Bronze create/read and quarantine create; the platform team must verify actual RBAC denials. The Helm release owns only ServiceAccount, SecretProviderClass and CronJob. Supply non-secret identifiers at deploy time:

```sh
cd ingestion
docker build -t "$ACR.azurecr.io/opensky-collector:0.1.0" .
docker push "$ACR.azurecr.io/opensky-collector:0.1.0"
kubectl create namespace aviation --dry-run=client -o yaml | kubectl apply -f -
helm lint helm/collector --set image.repository="$ACR.azurecr.io/opensky-collector" --set image.tag=0.1.0 --set account="$ACCOUNT" --set vault="$VAULT" --set tenantId="$TENANT_ID" --set identity.clientId="$COLLECTOR_CLIENT_ID"
helm template collector helm/collector --namespace aviation --set image.repository="$ACR.azurecr.io/opensky-collector" --set image.tag=0.1.0 --set account="$ACCOUNT" --set vault="$VAULT" --set tenantId="$TENANT_ID" --set identity.clientId="$COLLECTOR_CLIENT_ID"
helm upgrade --install collector helm/collector --namespace aviation --set image.repository="$ACR.azurecr.io/opensky-collector" --set image.tag=0.1.0 --set account="$ACCOUNT" --set vault="$VAULT" --set tenantId="$TENANT_ID" --set identity.clientId="$COLLECTOR_CLIENT_ID"
kubectl -n aviation get cronjob collector-collector
kubectl -n aviation get jobs --sort-by=.metadata.creationTimestamp
kubectl -n aviation logs job/<latest-job-name> --all-containers=true
az storage blob list --auth-mode login --account-name "$ACCOUNT" --container-name bronze --prefix opensky/states/ --query '[].name' -o tsv
```

For a **manual validation call (uses four credits)**: `kubectl -n aviation create job --from=cronjob/collector-collector collector-check-$(date +%s)`. Use `az storage blob download --auth-mode login --account-name "$ACCOUNT" --container-name bronze --name '<listed-name>' --file /tmp/opensky-check.avro` then `python -c 'from collector import decode; from pathlib import Path; print(decode(Path("/tmp/opensky-check.avro").read_bytes()))'` to verify a complete single record. Choose a safe temporary path and delete the local data when finished. An invalid or conflicting response is archived under a unique `opensky/invalid/` or `opensky/conflict/` **quarantine** path, not the Bronze discovery root. Alert on nonzero Job exit, conflicts, source age and missing files; monitor the distinct source timestamp, not just successful HTTP requests.

## Contract and budget

`GET https://opensky-network.org/api/states/all` uses bearer token from the OAuth2 client-credentials endpoint and **exactly** `lamin=-11&lamax=21&lomin=92&lomax=141`. The original response must contain only `time` (Unix seconds, Avro long) and `states` (null or a list of 17-element positional lists). `schema/states.avsc` defines a heterogeneous nullable union for the state values (strings, longs, doubles, booleans, and integer sensor lists). Per-position validation prevents a permissive union from silently changing meaning. One Snappy Avro container holds exactly one record, including valid null/empty `states`; decoding must reproduce values/types, optional nulls and padded strings, not original JSON or binary bytes. No fetch timestamp, hash or schema version is inserted into Bronze.

The canonical name, **entirely from source time in UTC**, is `opensky/states/year=YYYY/month=MM/day=DD/hour=HH/<time>.avro`. For `example_response.json` (`1793013825`), it is `opensky/states/year=2026/month=10/day=26/hour=11/1793013825.avro`. Conditional `overwrite=False` creates the complete object or none; collisions compare decoded values. Identical is idempotent; differing content preserves canonical Bronze and archives the conflicting original in restricted quarantine before failing. Invalid originals are quarantined. Quarantine/read/storage failures also fail; never infer archive success solely from an HTTP 200.

One CronJob runs `*/2 * * * *` UTC with `Forbid`, no pod retries and a 75-second active deadline. A single normal states call per run is 720 × four = **2,880 credits/day** of the documented **4,000/day**. The remaining 1,120 credits allow at most **280 extra states calls/day shared by application retries, manual calls, and every environment on the same account**. The collector permits at most one application retry only in one of 240 six-minute slots/day, leaving at least 40 extra calls/day for manually budgeted calls when one environment runs normally. It respects `X-Rate-Limit-Retry-After-Seconds` on 429 when a retry fits the deadline; otherwise it fails without another call. HTTP 5xx/timeouts use the same bound. Do **not** start multiple live environments at two-minute cadence: three alone cost 8,640 credits/day before retries. Other API consumers/manual jobs consume the shared headroom; operators must count them and pause or reduce collector cadence if quota is at risk. There is no cross-environment automated quota ledger.

The collector only lands Bronze. A **separate** continuous Spark Silver stream and an **independent** scheduled SQL Gold Job own downstream data. `source_time`, source age and `fetch_at_utc` in redacted logs separate stale API snapshots from request success; Bronze landing, Silver and Gold freshness must be measured separately. A two-minute schedule does not promise <5-minute queryable Gold. Fixture tests do not prove AKS/ADLS operation, cloud role boundaries, consumer compatibility or live freshness.
