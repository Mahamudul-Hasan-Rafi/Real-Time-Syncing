# Testing

| Layer | Where | Needs | Run |
|---|---|---|---|
| Unit | `tests/unit/` | nothing: offline, fakes for Kafka, Oracle and Elasticsearch | `.venv\Scripts\pytest` |
| Static | the whole tree | nothing | `.venv\Scripts\ruff check .` and `.venv\Scripts\mypy` |
| Start-up checks | `cdc-es-sync check` | the dev systems, read-only | `docker compose run --rm cdc-es-sync check` |
| Live, read-only | `tests/live/parity.py`, `tests/live/lookup_crosscheck.py`, `cdc-es-sync reconcile` | the dev systems | see below |
| Live, changes data | `tests/live/e2e.py`, `field_e2e.py`, `fault.py` | the dev systems and a running service | see below |

The unit tests load the real `config.yaml` and index files, so a broken field map fails them. The offline audit
needs the table columns in `tests/fixtures/table_columns.json`; refresh it with `scripts/dump_columns.py` after a
schema change.

| Unit test module | Covers |
|---|---|
| `test_settings.py` | Typed settings, `${VAR}` expansion, unknown/missing keys, value types |
| `test_events.py`, `test_keys.py` | Debezium parsing; key values from events and from Oracle agree |
| `test_mapping.py` | Routing: which documents and fields an event affects |
| `test_audit.py` | The field map against the SQL, and that the audit catches each kind of mistake |
| `test_sqlscan.py`, `test_transform.py` | SQL scanner; row to document as Logstash writes it |
| `test_engine.py` | Rebuild vs partial refresh, fan-out, deletes, retries, poison isolation, `sync.operations` |
| `test_consumer.py` | Offsets committed only after a batch is applied; malformed events and tombstones |
| `test_connectors.py` | Elasticsearch bulk retries; dead letter topic and file fallback |
| `test_kafka.py`, `test_reconcile.py`, `test_observability.py` | Consumer lag, reconcile, health, check fatality, CLI exit codes |

## Live tests

```
.venv\Scripts\python tests\live\parity.py 300        read-only: rebuilt documents vs the documents in Elasticsearch
.venv\Scripts\python tests\live\lookup_crosscheck.py read-only: every lookup returns every dependent document
.venv\Scripts\python tests\live\e2e.py               service must be running; makes real, reverted changes in dev
.venv\Scripts\python tests\live\field_e2e.py         service must be running; field-level changes, checks what is and is not written
.venv\Scripts\python tests\live\fault.py             stops the service and runs outage, crash, poison and replay tests
```

`e2e.py`, `field_e2e.py` and `fault.py` change data in the dev schema. Every change registers its revert first and
the reverts also run at exit, so an aborted run still restores the data. `fault.py` starts the service on the host
(`python -m cdc_es_sync run`), so stop the container first or the two share the consumer group.

## Results

### 4 Nov 2026, version 1.1.0 (after the restructuring)

| Test | Result |
|---|---|
| Unit tests | 144 passed (1.0.0 on `main`: 108 passed, 6 failed on the missing `reconcile_sum_field`) |
| ruff, mypy | clean (1.0.0: mypy reported 14 errors) |
| `check`, on the host and inside the rebuilt image | 9 of 9 pass |
| `reconcile` | all three indices OK, now including the `customer_data` total (`initial_amt`) |
| Parity | 900 random documents (300 per index) rebuilt by 1.1.0: identical to the documents in Elasticsearch |
| Lookup completeness | 33 lookup groups over real rows of every child and dimension table, all three indices: 0 dependent documents missing (a few extra refreshed, the known OR lookups) |
| Field maps moved to `indices/` | parsed content identical to the 1.0.0 `config.yaml`, apart from the added `reconcile_sum_field` |
| Not re-run | `e2e.py`, `field_e2e.py`, `fault.py`: they change dev data and stop/run the service, and the 1.0.0 container was serving live. The code they exercise moved but its logic did not change; run them before deploying 1.1.0 |

### 1 Nov 2026, version 1.0.0

| Test | Result |
|---|---|
| Parity | 6,000 random documents (2,000 per index) rebuilt from Oracle: identical to the Logstash-loaded documents, including `_id`. 10,159 sampled of about 130,000 CDC-written documents: all identical to what Oracle produces now |
| Lookup completeness | 33 lookup groups over real rows of every child and dimension table, for all three indices: every dependent document returned, none missing |
| End-to-end (`e2e.py`) | 24 scenarios; 23 passed, 1 skipped ("account: update title" found no row with documents in all three indices; the same change passes in `field_e2e.py`). Document counts unchanged |
| Field-level (`field_e2e.py`) | 16 scenarios pass: e.g. `mobile_no` reaches `customer_data` and `transaction_history_` and writes nothing to `ac_balance_data`; clearing to NULL; a column no index reads writes nothing. Every change reverted to byte-identical documents |
| Fan-out speed | Whole-document rebuilds about 230 documents/s; field-level updates about 2,700 documents/s (68,000 documents in 25 s) |
| Faults (`fault.py`, on the whole-document version) | Elasticsearch and Oracle outages: nothing applied while down, all applied after. Kill -9 mid-rebuild: completed after restart. Poison events: 3 dead letters, service kept running. Replay of every event (new group): 460 events, 79,525 documents, 0 errors |
