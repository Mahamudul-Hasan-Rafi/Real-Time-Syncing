# Operations

## Prerequisites

* **Supplemental logging.** The service relies on the `ALWAYS` log groups on the `EMOB` tables (`G_CDC_<TABLE>` /
  `CDCT_<TABLE>`). Only the columns used to **find** documents (`direct` and `params` in the index files) need to be in
  a log group; `check` tells you if one is missing. Columns that only feed fields do not: Debezium reports the old and
  new value of every column that changed (tested, including a change to NULL).
* Kafka topics `emob.EMOB.<TABLE>` from the Debezium connector `emob-oracle-cdc`.
* The three Elasticsearch indices exist with their templates (created by the Logstash pipelines).
* Python 3.10+. The Elasticsearch **client must be 8.x** to match the 8.4.3 server (pinned in `pyproject.toml`).

## Deploy with Docker

Files: `Dockerfile`, `docker-compose.yml`, `docker-compose.stack.yml` (optional), `requirements.lock` (exact versions).
The quick start is in the [README](../README.md).

| Command | What it does |
|---|---|
| `docker compose run --rm cdc-es-sync check` | The 9 start-up checks. All must PASS before going live. |
| `docker compose run --rm cdc-es-sync reconcile` | Document count and one amount total per index, Oracle vs Elasticsearch (the document query itself is used on the Oracle side). Exit 1 on a mismatch. Run it when no changes are flowing, or allow for a few documents. |
| `docker compose run --rm cdc-es-sync resync customer_data` | Rebuilds one index from Oracle (closes the gap before CDC started). |
| `docker compose restart cdc-es-sync` | Reloads `config.yaml` and `indices/` (mounted read-only, so no rebuild is needed). |
| `docker compose stop` | SIGTERM: the batch in progress finishes and the consumer leaves its group (up to 60 s). |
| `curl http://localhost:9108/health` | The container's healthcheck. `/metrics` is Prometheus. |

* The container runs as a non-root user, restarts unless stopped, and is limited to 2 CPUs / 1 GB.
* `logs/` and `dlq/` are named volumes. Container output is also rotated by Docker (5 x 20 MB).
* **Run one instance.** The topics have one partition each, so a second consumer in the same group would sit idle.
* To reach Kafka over the Debezium stack's Docker network (`kafka:29092`) instead of the host address:
  `docker compose -f docker-compose.yml -f docker-compose.stack.yml up -d` (network name in `STACK_NETWORK`, default `cdc_default`).

### Upgrading from 1.0.x

1.1.0 moved the field map out of `config.yaml` into `indices/<name>.yaml` and the SQL from `sql/` to `indices/`.
`docker-compose.yml` now mounts `./indices` instead of `./sql`. `.env` is unchanged. Rebuild the image
(`docker compose build`), run `check`, then `docker compose up -d`. The consumer group and its offsets are unchanged,
so the new version continues where the old one stopped.

## Failure handling

| Situation | Behaviour |
|---|---|
| Elasticsearch down or slow (connection error, timeout, 429/502/503/504) | Retries with exponential backoff (max 30 s), forever. Consumption is paused, offsets are not committed, nothing is lost. |
| Elasticsearch refuses access (401/403: wrong login, missing permission, or an index made read-only by the disk flood stage) | Waits and retries like an outage, forever: nothing is dead-lettered and no offset is committed. It logs at `ERROR` ("refused access") and does **not** refresh `/health`, so `/health` turns 503 after `health_stale_seconds`. Fix the login or permission, or free disk space on the cluster; restart the service after changing the password, because the login is read at start-up. |
| Oracle down or connection lost | The whole batch is retried with backoff. Only connection-type errors are retried. |
| An event that cannot be applied (malformed, key columns missing, SQL error) | The batch is split and processed event by event; the failing event goes to the dead letter topic and the rest continue. |
| A row that cannot become a valid document (bad JSON column, empty key) | That document goes to the dead letter topic; the others in the batch are written. |
| Elasticsearch refuses a document (mapping conflict) | That document goes to the dead letter topic. |
| Kafka dead letter topic unavailable | Dead letters are written to `dlq/dlq-YYYYMMDD.ndjson`. |
| Service killed or crashed | On restart it resumes from the committed offset. Re-applying events is harmless. |
| Ctrl+C / SIGTERM | The current batch finishes; the consumer leaves the group cleanly. |
| A start-up check fails | `run` exits with code 2 and `/health` reports `failed`, so a mapping problem cannot go unnoticed. |

The dead letter topic is `cdc.es-sync.dlq`: JSON records with `ts`, `stage` (`parse`, `missing_key`, `processing`,
`transform`, `elasticsearch`), `reason` and the event or document.

## Observability

* `/health` returns 200 while the loop is alive (503 after `health_stale_seconds` without a heartbeat or in the failed state).
  Waiting for an Elasticsearch or Oracle that is down or busy keeps the heartbeat, so a normal outage is not reported as
  unhealthy. Being refused access by Elasticsearch does not, so a stuck service is visible.
* `/metrics` is Prometheus text: `cdc_events_total{table,op}`, `cdc_events_skipped_total{reason}`,
  `cdc_docs_indexed_total{index}` (whole documents), `cdc_docs_updated_total{index}` (partial updates),
  `cdc_docs_deleted_total{index}`, `cdc_fanout_keys_total{index}`, `cdc_errors_total{stage}`, `cdc_dlq_total{stage}`,
  `cdc_batches_total`, `cdc_batch_seconds`, `cdc_consumer_lag{topic,partition}`, `cdc_last_batch_timestamp`.
* JSON logs on the console (`docker compose logs`) and in `logs/cdc-es-sync.log` (rotated, 5 x 20 MB). The `logger`
  field is the module, e.g. `cdc_es_sync.pipeline.consumer` for the `batch committed` lines.

A document rebuilt whole by CDC carries `etl_pipeline: cdc_<index>` and a fresh `etl_loaded_at`. A partial update
changes only the fields it names, so `etl_loaded_at` keeps its value: it means "last fully loaded", not "last changed".

## Operational notes

* **After a hard kill, takeover takes up to 45 seconds.** Kafka waits out the old member's `session.timeout.ms` before
  it gives the partitions to a new instance. A clean stop leaves the group at once.
* **`age` in `ac_balance_data` goes stale on its own.** The balance query computes it from `SYSDATE` at load time, and
  CDC only rebuilds a document when one of its rows changes. Compute the age in the dashboard, or run
  `resync ac_balance_data` daily.

## Known limits

* **Inserts are not applied** (`sync.operations: [u, d]`). New transactions, new daily balance rows and new child rows
  (nominee, address, document, operator) do not reach Elasticsearch through this service until an update to the same
  row makes the service write the document, or until you run `resync <index>` or load them another way. A new row that
  is deleted again never appears at all.
* The `EMOB` schema has no primary keys, so a document key is the logical key (`AC_NO`, `TRAN_ID`, `AC_ID` + date).
  Duplicate logical keys in Oracle would collapse into one document, exactly as in the batch load.
* CDC starts at the point Debezium's `snapshot.mode: no_data` started. Rows changed between the batch load and that
  moment are not in Kafka; run `resync <index>` once to close that gap.
* Lookups for `ST_AGENT_POINT` and `ST_POINT_CATEGORY` use an OR across the account point and the customer agent
  point, so a few more documents than strictly needed are refreshed. Harmless, and never misses one.
* A single lookup that matches hundreds of thousands of documents runs on `sync.workers` threads (about `workers` times
  faster than one), and still blocks other events behind it. It is safe to interrupt. If Oracle or Elasticsearch
  struggles, lower `sync.workers` or set `sync.throttle_ms`.
