# cdc-es-sync

Keeps the Elasticsearch indices `customer_data`, `ac_balance_data` and `transaction_history_` in step with the
Oracle schema `EMOB`. It reads the change events Debezium publishes to Kafka (one topic per table) and updates the
affected documents, with values read from Oracle rather than from the event.

```
Oracle (archive log) -> Debezium/LogMiner -> Kafka  emob.EMOB.<TABLE>  ->  cdc-es-sync  ->  Elasticsearch
                                                                              |
                                                                              +-- reads Oracle to rebuild each affected document
```

| Read next | For |
|---|---|
| [docs/architecture.md](docs/architecture.md) | How a change event becomes a document update, the guarantees, the code layout |
| [docs/configuration.md](docs/configuration.md) | `config.yaml`, the index files and their field map, environment variables |
| [docs/operations.md](docs/operations.md) | Deploying, commands, failure handling, metrics, known limits |
| [docs/testing.md](docs/testing.md) | Unit and live tests, latest results |
| [CHANGELOG.md](CHANGELOG.md) | What changed between versions (1.1.0 moved files: read it before upgrading) |

## Quick start (Docker)

```
if (-not (Test-Path .env)) { Copy-Item .env.example .env }     # never overwrite an existing .env (Linux: cp -n .env.example .env)
# edit .env: ORACLE_USER, ORACLE_PASSWORD, KAFKA_BOOTSTRAP, ES_URL, ES_PASSWORD, ORACLE_DSN (the machine's IP, not localhost)
docker compose build
docker compose run --rm cdc-es-sync check        # all 9 start-up checks must PASS
docker compose run --rm cdc-es-sync reconcile    # Oracle vs Elasticsearch counts and totals: every index OK
docker compose up -d
docker compose logs -f cdc-es-sync
```

| Command | What it does |
|---|---|
| `cdc-es-sync run` | Consumes change events and keeps Elasticsearch in step (the container's default). |
| `cdc-es-sync check` | Verifies Kafka, Elasticsearch, Oracle, the SQL, the supplemental log groups and the field map. |
| `cdc-es-sync reconcile` | Compares document counts and one amount total per index between Oracle and Elasticsearch. |
| `cdc-es-sync resync <index>` | Rebuilds every document of one index from Oracle. |

In Docker prefix them with `docker compose run --rm`; `python -m cdc_es_sync ...` is the same command.
Exit codes: `0` success, `1` a check or comparison failed, `2` configuration error or failed preconditions.

## Project layout

```
config.yaml                 runtime settings (Kafka, Oracle, Elasticsearch, batching, observability)
indices/                    one Elasticsearch index per pair of files
  <name>.yaml                 identity, conversions and field map of the index
  <name>.sql                  its document query (the same SQL as the Logstash load)
src/cdc_es_sync/
  cli.py                    command line (run | check | reconcile | resync)
  app.py                    composition root: builds the components from the settings
  settings.py               typed, validated settings; ${VAR} expansion and .env
  errors.py                 the exception hierarchy
  lifecycle.py              stop signal and retry backoff
  core/                     pure logic, no I/O
    events.py                 Debezium JSON -> Change
    keys.py                   key normalization, "did the column change"
    model.py                  IndexSpec, SourceRule, ChangePlan
    registry.py               compiles the index files; routes a change to documents and fields
    audit.py                  field map vs SQL consistency checks
    sqlscan.py                reads output columns and column lineage from the document SQL
    transform.py              Oracle row -> Elasticsearch document (as Logstash writes it)
  connectors/               the outside systems
    kafka.py  oracle.py  elastic.py  dlq.py
  pipeline/                 orchestration
    engine.py                 a batch of changes -> Oracle reads -> Elasticsearch writes
    consumer.py               the Kafka loop: collect, apply, commit
    preflight.py              the start-up checks
    reconcile.py              Oracle vs Elasticsearch counts and totals
  observability/            JSON logs, Prometheus metrics, /health
tests/unit/                 offline tests (pytest)
tests/live/                 scripts against the dev systems (run by hand)
scripts/dump_columns.py     refreshes tests/fixtures/table_columns.json after a schema change
```

Dependencies point one way: `cli -> app -> pipeline -> connectors / core`, and `core` imports no client library.

## Development

```
python -m venv .venv
.venv\Scripts\python -m pip install -r requirements.lock
.venv\Scripts\python -m pip install -e ".[dev]"
.venv\Scripts\pre-commit install          # optional: lint, type-check and test on every commit

.venv\Scripts\pytest                      # unit tests, offline
.venv\Scripts\ruff check .                # lint
.venv\Scripts\mypy                        # types
```

`requirements.lock` holds the exact runtime versions the image installs; `pyproject.toml` holds the ranges and the
`dev` extras. CI (`.github/workflows/cdc-es-sync.yml` at the repository root) runs lint, types, unit tests and an
image build on every change under `cdc-es-sync/`.

Run **one** instance per consumer group. Do not also run `cdc-es-sync run` on a workstation against the group the
container uses, or the two take turns.
