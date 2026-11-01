# EMOB Oracle -> Elasticsearch pipelines

Source: dev Oracle `10.11.200.37:1521/emob`. Target: Elasticsearch `http://10.11.200.109:9200` (no auth).

| Pipeline | Config | Index | Document ID |
|---|---|---|---|
| `ac_balance_data` | `pipeline\Offline_balance_data_transfer.conf` | `ac_balance_data` | `ac_id` + `balance_date` |
| `customer_data` | `pipeline\Offline_data_transfer.conf` | `customer_data` | `ac_no` |
| `transaction_history` | `pipeline\Offline_transaction_data_transfer.conf` | `transaction_history_` | `tran_id` |

## Run

Start Logstash with the scripts below, not `bin\logstash` directly. The pipelines use paths relative to the Logstash folder
(`pipeline/`, `templates/`, `rejects/`), so the scripts change into it first. The JDBC driver is the exception: the plugin loads
it through JRuby, which rejects a relative `.jar` path ("no such file to load"), so the scripts also set `LS_ROOT` to the
absolute folder and the pipelines use `${LS_ROOT}/drivers/ojdbc8-21.1.0.0.jar`.

```
powershell -NoProfile -ExecutionPolicy Bypass -File scripts\setup.ps1      once: downloads Logstash 8.4.3 and the Oracle JDBC driver (Linux: scripts/setup.sh)
.\scripts\run_pipelines.cmd --test                                         validate configs (Linux: scripts/run_pipelines.sh --test)
.\scripts\run_pipelines.cmd                                               run all three; Logstash exits when they finish
```

Credentials and addresses come from the environment or from `Packages/cdc/elastic/.env` (copy `.env.example` there):
`ORACLE_USER`, `ORACLE_PASSWORD`, and optionally `ORACLE_JDBC_URL` and `ES_HOSTS` (the defaults are the dev servers).
The check that the load is complete is `docker compose run --rm cdc-es-sync reconcile` in `cdc-es-sync/`
(no Oracle client needed); `scripts\reconcile.ps1` does the same with `sqlplus` if you have it.

Logstash reports a normal exit even if the database connection drops mid-read (tested). Treat `reconcile.ps1` as the pass/fail check.

## SQL changes (nothing else in the queries was touched)

- Customer: removed `TRUNC(A.OPEN_DATE) BETWEEN ...`.
- Transaction: removed the two `A.CREATE_DATE` range conditions.
- Balance: table is `mb_account_daily_balance` on dev. `mb_account_daily_balance_test` does not exist there.

## Settings beyond the defaults

### `config\logstash.yml`
| Setting | Why |
|---|---|
| `queue.type: persisted`, `queue.max_bytes: 2gb` | Buffer on disk between Oracle and Elasticsearch. Survives restarts and Elasticsearch outages. Default is an in-memory queue that loses data on a crash. |
| `queue.drain: true` | One-shot loads: wait for the queue to empty before exiting. |
| `dead_letter_queue.enable: true`, `max_bytes: 1gb` | Documents Elasticsearch rejects (HTTP 400/404) are kept in `data\dead_letter_queue\<pipeline>\*.log` instead of dropped. Default is off. |
| `pipeline.ordered: false` | IDs are deterministic, order is irrelevant, so skip ordering cost. |
| `pipeline.ecs_compatibility: disabled` | Field names match the strict templates exactly. |
| `pipeline.separate_logs: true` | One log file per pipeline in `logs\`. |
| `api.http.host: 127.0.0.1`, `api.http.port: 9600` | Fixed, local-only monitoring API (`/_node/stats/pipelines`). |
| `xpack.monitoring.*` | Pipeline metrics appear in Kibana Stack Monitoring. |

### `config\pipelines.yml`
Three isolated pipelines, each with its own queue and DLQ, so one failure does not stall the others. Workers and batch size are tuned per pipeline (balance 2000, transaction 1000, customer 500 events per batch; customer documents are the largest) to stay within the 2 GB Elasticsearch heap.

### `config\jvm.options`
`-Xms4g -Xmx4g` (default 1g) and a heap-dump path for OutOfMemory.

### Credentials
`ORACLE_USER` and `ORACLE_PASSWORD` are referenced as `${...}` in the `.conf` files, so no credentials are in them. They are read
from the environment / `Packages/cdc/elastic/.env`. Logstash also looks in `config/logstash.keystore` (create it with
`bin\logstash-keystore create` and `add ORACLE_PASSWORD`); that file is git-ignored and must never be committed.

### JDBC input
| Setting | Why |
|---|---|
| `jdbc_fetch_size` (1000-5000) | Oracle's driver default is 10 rows per round trip. |
| `ENABLE=BROKEN`, `CONNECT_TIMEOUT`, `RETRY_COUNT` in the connect string | Detects a dead connection, bounded connect time, driver-level retry. |
| `connection_retry_attempts: 5`, `..._wait_time: 30` | Plugin-level retry when the database is unreachable at start. |
| `jdbc_validate_connection: true` | Test the connection before use. |
| `jdbc_default_timezone: Asia/Dhaka` | Oracle DATE has no zone. Without this, all dates are 6 hours off. |
| `record_last_run: false` | No incremental column, so no state file to go stale. |

### Filters
- `etl_loaded_at` and `etl_pipeline` lineage fields.
- `@timestamp` set to the business date (`create_date` / `balance_date`) so Kibana's time filter works. `@version` removed.
- `json` filters turn the `doc`, `address`, `operator`, `nominee` JSON strings into real arrays; a `ruby` filter adds the `+06:00` offset to dates inside them.
- Missing document ID is tagged `_missing_doc_id` rather than becoming the literal ID `%{...}`.

### Elasticsearch output
| Setting | Why |
|---|---|
| `document_id` from a deterministic key | Re-runs overwrite, never duplicate (tested: second run left 300,000 documents). |
| Index templates in `templates\` (`manage_template`, `template_overwrite`) | `dynamic: strict` mapping, keyword/date/scaled_float types, 1 shard, 0 replicas, `refresh_interval: 30s`. Default is dynamic mapping, which guesses types. |
| `timeout: 120`, `retry_max_interval: 30` | Slow bulks queue instead of failing; backoff when Elasticsearch is down. |
| `ilm_enabled: false`, `data_stream: false` | Plain indices, as in the originals. |
| Rejects `file` output | Events with no ID or a bad JSON column go to `rejects\<pipeline>.ndjson`. |

## Fault-tolerance tests (run against real Oracle and Elasticsearch)

| Test | Result |
|---|---|
| Elasticsearch down for 45 s mid-load | Logstash stayed up, queued 400 MB on disk, resumed, 300,000/300,000 |
| Logstash hard-killed mid-load, restarted | Final 300,000/300,000 |
| Oracle link cut mid-read | Logstash exited 0 with partial data (35,000); re-run gave 300,000. Use `reconcile.ps1`. |
| Re-run into existing index | 300,000 stayed 300,000 |
| Document violating the strict mapping | Landed in the DLQ, not lost |

## Performance: the six Oracle indexes are required

The scalar subqueries in the customer and transaction queries look up other tables once per row. Without indexes on those lookup columns each row triggers full table scans. Measured before indexing: `customer_data` about 7 rows/s (about 11 hours for 299,998 rows), `transaction_history` about 2 rows/s (about 37 hours for 296,958 rows).

`scripts\recommended_indexes.sql` creates six indexes (`IX_ETL_*`) on the dev schema. They are in place, and all three pipelines now load together in about 5 minutes. Apply the same script to any other environment before running these pipelines there.

The DLQ reader plugin in this Logstash zip is missing its jar, so DLQ files cannot be read through Logstash. They are plain text and can be opened directly.
