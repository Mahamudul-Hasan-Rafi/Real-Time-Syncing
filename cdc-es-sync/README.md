# cdc-es-sync

Keeps the Elasticsearch indices `customer_data`, `ac_balance_data` and `transaction_history_` in step with the
Oracle schema `EMOB`. It reads the change events Debezium publishes to Kafka (one topic per table) and updates the
affected documents.

```
Oracle (archive log) -> Debezium/LogMiner -> Kafka  emob.EMOB.<TABLE>  ->  cdc-es-sync  ->  Elasticsearch
                                                                              |
                                                                              +-- reads Oracle to rebuild each affected document
```

## How it works

Your documents are not copies of single tables. Each one joins several tables, with lookups and JSON arrays.
A change event tells us **which row changed and which columns**, so the service works in three steps:

1. **Which columns changed?** An update event carries the old and the new value of every column that changed. The
   service compares them (an empty string and NULL count as equal). If none of the changed columns is listed for an
   index in `config.yaml`, that index is not touched at all.
2. **Which documents, and which fields of them?** The changed row is mapped to document keys (a `direct` key or a
   short lookup query). The listed column says which document fields it feeds.
3. **Fetch the current values from Oracle and update only those fields.** The values come from the same SQL the
   Logstash load uses, restricted to the wanted output columns (the other sub-selects are not evaluated), and are sent
   to Elasticsearch as a partial `update` by document id. The service never writes the values carried by the event.

A key or join column that changed (account number, customer number, product, point), a new row in an inner-join
table, or a row that disappeared rebuilds the **whole** document instead, with the full SQL. A key Oracle no longer
returns is deleted from Elasticsearch. A partial update of a document that is not in Elasticsearch falls back to a
whole rebuild.

| Property | Why |
|---|---|
| Documents are identical to the batch load | Same SQL, same conversions. Checked by `tests/live/parity.py`. |
| Replaying an event is harmless | Values are read from the current database state, never taken from the event, so an old event cannot overwrite a newer value. |
| Event order does not matter | Same reason. Ten updates of one row cause one refresh. |
| Only the changed fields are written | The other fields, including `etl_loaded_at`, stay byte-identical. Documents of other indices are not written (checked with `_seq_no`). |
| Deletes, moves and key changes just work | Both the `before` and `after` row images feed the key lookup. |
| At-least-once delivery | Kafka offsets are committed only after Elasticsearch accepted the batch. |

## The field map (`config.yaml`, `indices.<name>.sources`)

Each index lists the tables its query reads, and for each table which columns feed which document fields:

```yaml
indices:
  transaction_history:
    sources:
      MB_CUSTOMER_MST:
        - params: {CUST_NO: int}          # how to find the affected documents: a lookup query ...
          sql: >-
            SELECT t.tran_id FROM emob.mb_transaction_dtl t
            JOIN emob.mb_account_mst a ON a.ac_id = t.ac_id
            WHERE a.cust_no = :CUST_NO
          columns:                         # ... and what each changed column refreshes
            MOBILE_NO: same                #   same = the field with the same name (mobile_no)
            MOBILE_NO2: same
            TOKEN_NO: [aml_token_no]       #   or a list of fields
            OCCUPATION_CODE: [occupation]
```

| Column value | Meaning |
|---|---|
| `same` | Refresh the field with the same (lower case) name. |
| `[f1, f2]` | Refresh these fields. |
| `rebuild` | Key or join column: rebuild the whole document (and delete the old one if the key moved). |

An entry is either `direct: [COLS]` (the changed row is the document itself, its event columns form the key) or a
lookup (`sql` + `params`, returning document keys). `join: inner | outer` marks the table the query joins on.

**Example: a customer changes `MOBILE_NO`.**

| Index | Result |
|---|---|
| `customer_data` | the customer's account documents get the new `mobile_no` |
| `transaction_history_` | every transaction of the customer's accounts gets the new `mobile_no` |
| `ac_balance_data` | not touched: it has no mobile number field |

Changing `MOBILE_NO2` updates `transaction_history_` only. Changing a column nobody reads writes nothing.

The `transaction_history_` documents carry the account, customer, product, point and code values **as they are now**
(the Logstash query reads them when it runs), so a change to any of those tables updates the affected fields of every
transaction that uses it, by document id.

### The mapping is checked, not trusted

| Check | Where | What it proves |
|---|---|---|
| Field-map audit | `check` command, start-up and `tests/test_mapping.py` | The SQL of each index is scanned to find which table column feeds which output field. Every column the query reads must be listed here with every field it feeds, nothing listed may be a column the query does not read, and join columns must be `rebuild`. Forget a column or mistype a field and the service refuses to start. |
| Coverage audit | same | Every table a query reads is mapped (or listed under `not_propagated`). |
| Query compile check | `check` command | All 3 document queries (in full and as field projections) and all 36 lookup queries parse against Oracle, and the parsed output columns equal Oracle's. |
| Log group check | `check` command and start-up | Every event column used to find documents (`direct` / `params`) is in one of your `ALWAYS` supplemental log groups. |
| Lookup completeness | `tests/live/lookup_crosscheck.py` | For real rows of every child and dimension table, works out which documents depend on the row (from Elasticsearch, independently of the lookup SQL) and proves the lookup returns all of them. |
| Field-level live test | `tests/live/field_e2e.py` | Real changes: the right fields of the right documents change, nothing else does, other indices are not written, and a revert restores byte-identical documents. |
| Parity | `tests/live/parity.py` | Documents rebuilt by this project equal the Logstash-loaded documents, field by field, including `_id`. |

## Deploy with Docker

Files: `Dockerfile`, `docker-compose.yml`, `docker-compose.stack.yml` (optional), `requirements.lock` (exact versions).

```
copy .env.example .env                 fill in ORACLE_USER / ORACLE_PASSWORD; set KAFKA_BOOTSTRAP / ES_URL / ORACLE_DSN if they differ
docker compose build
docker compose run --rm cdc-es-sync check       all start-up checks must pass
docker compose up -d
docker compose logs -f cdc-es-sync
```

| Command | What it does |
|---|---|
| `docker compose run --rm cdc-es-sync resync customer_data` | Rebuilds one index from Oracle (closes the gap before CDC started). |
| `docker compose restart cdc-es-sync` | Reload `config.yaml` / `sql/` (they are mounted read-only into the container, so no rebuild is needed). |
| `docker compose stop` | Sends SIGTERM; the batch in progress finishes and the consumer leaves its group (up to 60 s). |
| `curl http://localhost:9108/health` | Same health endpoint the container's healthcheck uses. `/metrics` is Prometheus. |

* The container runs as a non-root user, restarts unless stopped, and is limited to 2 CPUs / 1 GB.
* `logs/` and `dlq/` are named volumes. Container output is also rotated by Docker (5 x 20 MB).
* **Run one instance.** The topics have one partition each, so a second consumer in the same group would sit idle. Do not
  also run `python -m cdc_es_sync run` on a workstation against the same group, or the two take turns.
* To reach Kafka over the Debezium stack's Docker network (`kafka:29092`) instead of the host address:
  `docker compose -f docker-compose.yml -f docker-compose.stack.yml up -d` (network name in `STACK_NETWORK`, default `cdc_default`).
* Oracle and Elasticsearch must be reachable from the container (they are addressed by IP in `config.yaml` / `.env`).

## Prerequisites

* **Supplemental logging.** The service relies on the `ALWAYS` log groups already on the `EMOB` tables (named
  `G_CDC_<TABLE>` / `CDCT_<TABLE>`). Each group holds the identifying columns of its table. The `EMOB` schema has no
  primary keys, so these groups are what identifies a row in the event, including in the `before` image of a delete.
  Only the columns used to **find** documents (`direct` and `params` in `config.yaml`) need to be in a log group; `check`
  tells you if one is missing. The columns that merely feed fields do not: Debezium reports the old and new value of
  every column that changed (tested, including a change to NULL), which is all the changed-column comparison needs.
* Kafka topics `emob.EMOB.<TABLE>` from the Debezium connector `emob-oracle-cdc`.
* The three Elasticsearch indices exist with their templates (created by the Logstash pipelines).
* Python 3.10+. The Elasticsearch **client must be 8.x** to match the 8.4.3 server (`requirements.txt` pins it).

## Setup and run

```
cd /d D:\Kafka\cdc-es-sync
python -m venv .venv
.venv\Scripts\python -m pip install -r requirements.txt
.venv\Scripts\python -m pip install -e .
copy .env.example .env        and fill in ORACLE_USER / ORACLE_PASSWORD
```

| Command (from the project folder, dev only; production uses Docker above) | What it does |
|---|---|
| `.venv\Scripts\python -m cdc_es_sync check` | Verifies Kafka, Elasticsearch, Oracle, the SQL, the log groups and the field map. Exit 0 = all pass. |
| `.venv\Scripts\python -m cdc_es_sync run` | Runs the service in the foreground. Ctrl+C stops it cleanly; events not yet applied are re-read on the next start. |
| `.venv\Scripts\python -m cdc_es_sync resync <index>` | Rebuilds every document of one index from Oracle. Use it to close the gap between a batch load and the moment CDC started. |
| `.venv\Scripts\python tests\dump_columns.py` | Refreshes `tests\fixtures\table_columns.json`, the table columns the offline audit test needs. Run it after a schema change. |

`run` refuses to start (exit 2) if a start-up check fails, so a mapping problem cannot go unnoticed.

## Configuration

`config.yaml`; `${VAR}` and `${VAR:default}` are read from the environment or from `.env`.

| Setting | Purpose |
|---|---|
| `kafka.auto_offset_reset: earliest` | A new consumer group, or a newly appearing table topic, is read from its first message, so no first event is missed. |
| `batch.max_events`, `batch.max_wait_seconds` | A batch is up to 500 events or 1 second. Duplicates inside a batch collapse. |
| `sync.chunk_size: 500` | Keys per Oracle query. IN lists stay well below Oracle's 1,000 limit. |
| `sync.max_fanout`, `sync.throttle_ms` | When one change touches more than 50,000 documents, a warning is logged and the rebuild is throttled between chunks. It runs inline, so a crash re-reads the event and nothing is lost. |
| `kafka.max_poll_interval_ms: 1800000` | A very large rebuild may keep the loop busy for minutes without being thrown out of the group. |
| `oracle.timezone: Asia/Dhaka` | Oracle `DATE` has no zone. Dates are written as UTC ISO strings, as the Logstash pipelines do. |
| `sync.nested_date_offset: +06:00` | Added to dates inside the JSON columns, as the Logstash `ruby` filter does. |
| `ORACLE_DSN`, `ES_URL`, `KAFKA_BOOTSTRAP`, `CDC_GROUP`, `CDC_HTTP_PORT` | Environment overrides. |

`elasticsearch.targets` accepts several clusters; each index names its `target`.

## Failure handling

| Situation | Behaviour |
|---|---|
| Elasticsearch down or slow (connection error, timeout, 429/502/503/504) | Retries with exponential backoff (max 30 s), forever. Consumption is paused, offsets are not committed, nothing is lost. |
| Oracle down or connection lost | The whole batch is retried with backoff. Only connection-type errors are retried. |
| An event that cannot be applied (malformed, key columns missing, SQL error) | The batch is split and processed event by event; the failing event goes to the dead letter topic and the rest continue. |
| A row that cannot become a valid document (bad JSON column, empty key) | That document goes to the dead letter topic; the others in the batch are written. |
| Elasticsearch refuses a document (mapping conflict) | That document goes to the dead letter topic. |
| Kafka dead letter topic unavailable | Dead letters are written to `dlq\dlq-YYYYMMDD.ndjson`. |
| Service killed or crashed | On restart it resumes from the committed offset. Re-applying events is harmless. |
| Ctrl+C / SIGTERM | The current batch finishes; the consumer leaves the group cleanly. |

The dead letter topic is `cdc.es-sync.dlq` (JSON records with `stage`, `reason` and the event).

## Observability

* `http://localhost:9108/health` returns 200 while the loop is alive (503 after `health_stale_seconds` without a heartbeat or in the failed state).
* `http://localhost:9108/metrics` is Prometheus text: `cdc_events_total{table,op}`, `cdc_events_skipped_total{reason}`,
  `cdc_docs_indexed_total{index}` (whole documents), `cdc_docs_updated_total{index}` (partial updates), `cdc_docs_deleted_total{index}`, `cdc_fanout_keys_total{index}`, `cdc_errors_total{stage}`,
  `cdc_dlq_total{stage}`, `cdc_batches_total`, `cdc_batch_seconds`, `cdc_consumer_lag{topic,partition}`, `cdc_last_batch_timestamp`.
* JSON logs on the console (`docker compose logs`) and in `logs/cdc-es-sync.log` inside the container's `cdc-logs` volume (rotated). The folder is created on demand.

A document rebuilt whole by CDC carries `etl_pipeline: cdc_<index>` and a fresh `etl_loaded_at`. A partial update
changes only the fields it names, so `etl_loaded_at` keeps its value: it means "last fully loaded", not "last changed".
`cdc_events_skipped_total{reason="no_relevant_column_changed"}` counts the entries skipped because none of their
columns changed.

## Tests

```
.venv\Scripts\python -m pytest                       unit tests, no network (mapping, parsing, transform, engine)
.venv\Scripts\python tests\live\parity.py 300        read-only: rebuilt documents vs the Logstash documents
.venv\Scripts\python tests\live\lookup_crosscheck.py read-only: every lookup returns every dependent document
.venv\Scripts\python tests\live\e2e.py               service must be running; makes real, reverted changes in dev
.venv\Scripts\python tests\live\field_e2e.py         service must be running; field-level changes (mobile number etc.), checks what is and is not written
.venv\Scripts\python tests\live\fault.py             stops the service and runs outage, crash, poison and replay tests
```

`e2e.py` and `fault.py` change data in the dev schema. Every change registers its revert first and the reverts also
run at exit, so an aborted run still restores the data.

## Test results (dev, 1 Nov 2026)

| Test | Result |
|---|---|
| Unit tests | 46 passed |
| `check` | 8 of 8 pass: Kafka, Elasticsearch, 3 indices, Oracle, 3 document queries, 21 lookup queries, log groups, mapping coverage |
| Parity | 6,000 random documents (2,000 per index) rebuilt from Oracle: identical to the Logstash-loaded documents, including `_id` |
| Parity, CDC-written documents only | 10,159 sampled of about 130,000 written by CDC: all identical to what Oracle produces now |
| Lookup completeness | 22 lookup groups over real rows of every child and dimension table: every dependent document returned, none missing |
| End-to-end, real changes | 24 scenarios, all pass (in the last full run 23 passed first time; `union` failed once because the test took its "before" snapshot from a search that was up to 30 s behind the previous scenario. The test now refreshes first, and `union` passed 6 more times, including back-to-back with the scenarios that share its documents): 17 updates (direct, child array element, composite date key, fan-out up to 23,367 documents), 7 insert/delete scenarios (transaction, balance with a new date, nominee, document, address, operator, account insert + key change + delete). Each change was reverted and the documents returned to their original content. Document counts unchanged. |
| Fan-out speed | about 230 documents per second (23,367 documents in about 100 s each way) |
| Elasticsearch outage | Nothing applied while down, retries logged, everything applied 18 s after the link returned |
| Oracle outage | Nothing applied while down, retries logged, everything applied after the link returned |
| Kill -9 with changes waiting, and in the middle of a 3,176-document rebuild (killed at 500) | After restart all documents completed, no loss |
| Poison events (not JSON, key columns missing, unsupported operation) | 3 dead letters, service kept running, a later real change applied; a delete of a row that never existed was harmless; 40 duplicate events harmless |
| Replay of every event ever published (new consumer group) | 460 events, 79,525 documents rewritten, 0 errors, counts unchanged, documents identical |
| Reconcile against Oracle (counts and amount totals) | all three indices OK |

## Operational notes

* **After a hard kill, takeover takes up to 45 seconds.** Kafka waits out the old member's `session.timeout.ms` before it
  gives the partitions to a new instance. A clean stop (Ctrl+C) leaves the group at once. Lower
  `kafka.session_timeout_ms` if faster failover matters more than tolerance of GC pauses and network blips.
* **`age` in `ac_balance_data` goes stale on its own.** The balance query computes it from `SYSDATE` at load time, and CDC
  only rebuilds a document when one of its rows changes. On the day tested, 681 untouched documents (birthday on 1 Nov)
  had a stale `age`. That is a property of the query, not of CDC. Compute the age in the dashboard, or run
  `resync ac_balance_data` daily.
* A document rebuilt whole by CDC carries `etl_pipeline: cdc_<index>`; a partially updated one keeps whatever it had.

## Known limits

* The `EMOB` schema has no primary keys, so a document key is the logical key (`AC_NO`, `TRAN_ID`, `AC_ID` + date).
  Duplicate logical keys in Oracle would collapse into one document, exactly as in the batch load.
* CDC starts at the point Debezium's `snapshot.mode: no_data` started. Rows changed between the batch load and that
  moment are not in Kafka; run `resync <index>` once to close that gap.
* Lookups for `ST_AGENT_POINT` and `ST_POINT_CATEGORY` use an OR across the account point and the customer agent
  point, so a few more documents than strictly needed are refreshed. That is harmless and never misses one.
* A single lookup that matches hundreds of thousands of documents runs for minutes. It is throttled and safe to
  interrupt, but it blocks other events behind it.
