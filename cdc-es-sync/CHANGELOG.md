# Changelog

## Unreleased

### Fixed
* A refused Elasticsearch login or permission (401/403) no longer drains the stream into the dead letter topic. Before,
  the error was raised out of the writer, the batch was split, every event was dead-lettered as `processing` and the
  offsets were committed, so a password change or a permission change silently emptied the topics into the DLQ. Now the
  events wait and retry, nothing is committed, and the log says "refused access" at `ERROR`. The same applies to a
  403 on a bulk item, which is how Elasticsearch reports an index it made read-only at the disk flood stage.
* `/health` no longer turns 503 during a long Elasticsearch outage in which the service is correctly waiting: the
  Elasticsearch retry wait now keeps the heartbeat, as the Oracle retry already did. A refused login deliberately does
  not, so `/health` goes 503 after `health_stale_seconds` and a stuck service is noticed.

### Changed
* A large fan-out is processed by `sync.workers` threads (default 4) instead of one chunk at a time. Each worker reads
  Oracle, builds the documents and sends its own bulk request, so Oracle, Python and Elasticsearch overlap; at most
  2 x workers chunks are in flight. The sleep between chunks above `max_fanout` is off by default (`throttle_ms: 0`).
  `oracle.pool_max` must be at least `workers + 1` (default raised from 4 to 8). The dead letter queue is now locked.

### Added
* 8 unit tests for the access handling and 6 for the parallel fan-out, including one that checks the 2 x workers read-ahead cap (158 in total).

## 1.1.0 (2026-11-04)

Restructuring for readability and verifiability; the sync behaviour is unchanged. Rationale and the full rename table:
[docs/adr/0001-project-structure.md](docs/adr/0001-project-structure.md).

### Fixed
* `customer_data` now has `reconcile_sum_field: initial_amt`. Before, `reconcile` compared that index by document
  count only, and 6 unit tests failed. The report now has a "sum of" column, so a count-only index is visible.
* The tables a query reads are found with the configured `oracle.schema`, not a hard-coded `emob.` prefix.

### Changed (action needed when upgrading)
* The field maps moved from `config.yaml` to `indices/<name>.yaml`; the SQL moved from `sql/` to `indices/`.
  `config.yaml` lists the index files under `indices:`. `docker-compose.yml` mounts `./indices` instead of `./sql`.
  Rebuild the image.
* Settings are typed and validated at start-up: unknown keys, missing required keys and wrong types are reported with
  their path and exit code 2. `sync.operations: []` is an error (leave the setting out to apply every change type).
* Log lines name the module in `logger` (e.g. `cdc_es_sync.pipeline.consumer`).

### Added
* `cdc-es-sync` console command (same as `python -m cdc_es_sync`); the image's entry point uses it.
* Unit tests for the consume loop (offsets committed only after a batch is applied), the dead letter file fallback,
  the Elasticsearch bulk retries, settings validation, start-up check fatality and health: 144 tests, from 108 passing.
* ruff and mypy configuration, pre-commit hooks, a CI workflow, `docs/`, this changelog.

### Removed
* `requirements.txt`. Use `pip install -e ".[dev]"`; the image still installs `requirements.lock`.

## 1.0.0 (2026-11-01)

First version, up to commit `452352d`: field-level updates, `sync.operations` (inserts off), the `run`, `check`,
`resync` and `reconcile` commands, Docker image and compose files, dead letter topic, Prometheus metrics and `/health`.
