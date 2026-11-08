# ADR 0001: Layered package, typed settings, one file per index

* Status: accepted
* Date: 2026-11-04
* Version: 1.1.0

## Context

Version 1.0 worked: the sync logic (read-through rebuilds, at-least-once commits, a dead letter output, an audited
field map) is sound and was proven against the dev systems. A review against the way data engineering services are
usually organized found that it was harder to read, change and verify than it needed to be:

| Area | 1.0 | Usual practice in data engineering services |
|---|---|---|
| Package layout | 14 modules side by side; pure logic, client code and wiring mixed. `service.py` built every client, ran the start-up checks, created the DLQ topic and ran the consume loop. | Layers by responsibility: pure transformation logic, connectors to each source/sink, orchestration, a composition root. |
| Configuration | One untyped `dict` passed everywhere; each module read its own keys with its own defaults (`retry_max_backoff_seconds` defaulted in two places). A mistyped key was silently ignored. | Typed settings validated once at start-up; unknown keys are errors. |
| Pipeline definitions | One 728-line `config.yaml` mixing runtime settings with three field maps; SQL in a separate folder. | One definition per dataset, next to its SQL (as dbt models pair `.sql` and `.yml`). |
| Errors | Seven exception classes spread over six modules. | One hierarchy, with the handling of each kind stated. |
| Hard-coded values | The schema name `emob.` was hard-coded in the table scanner and in a script. | Driven by configuration. |
| Tests | Strong for mapping and engine. Untested: the consume loop's commit-after-apply (the delivery guarantee), the dead letter fallback, the Elasticsearch retry loop, settings. A utility script lived in `tests/`. | Unit tests for every delivery guarantee; tools in `scripts/`. |
| Tooling | pytest only. No lint or type-check configuration, no CI, no console entry point; the version was kept in three places. | Lint, type-check and tests on every change (pre-commit and CI); a console script; one version source. |
| Documentation | A 300-line README that had drifted: it said certificates are always verified (they are not, `ES_VERIFY_CERTS` defaults to false), quoted 98 unit tests, and repeated table rows. | A short README, topic documents, decision records, a changelog. |
| Defect | `customer_data` had no `reconcile_sum_field`, so `reconcile` compared that index by count only, and 6 unit tests failed on `main`. | |

## Decision

1. **Layered package.** `core/` (pure: events, keys, model, registry, audit, sqlscan, transform), `connectors/`
   (kafka, oracle, elastic, dlq), `pipeline/` (engine, consumer, preflight, reconcile), `observability/` (logs,
   metrics), plus `settings.py`, `errors.py`, `lifecycle.py`, `app.py` (composition root) and `cli.py`.
   Dependencies point one way; `core` imports no client library.
2. **Typed settings** (`settings.py`): frozen dataclasses per section, coercion of values that arrive as text from
   `${VAR}` expansion, unknown keys rejected, defaults declared once. Standard library only, so the image and its
   locked dependencies are unchanged.
3. **One file per index**: `indices/<name>.yaml` next to `indices/<name>.sql`; `config.yaml` lists them. The YAML keys
   inside are unchanged, so the field maps were moved, not rewritten (verified: the parsed content is identical).
4. **Names that say what a thing is** (table below). Metric names, CLI commands, exit codes, environment variables
   and the YAML keys did **not** change: they are contracts with operators, dashboards and the deployment.
5. **Tooling**: ruff, mypy, pre-commit, a GitHub Actions workflow, a `cdc-es-sync` console script, the version read
   from `cdc_es_sync.__version__`. `requirements.txt` (which mixed pytest into the runtime list) is replaced by
   `pip install -e ".[dev]"`; `requirements.lock` stays as the image's exact pins.

| 1.0 | 1.1 |
|---|---|
| `config.load_config` (dict) | `settings.load_settings` (`Settings`) |
| `mapping.IndexCfg`, `.key_cols` | `core.model.IndexSpec`, `.key_columns` |
| `mapping.Impact` | `core.model.SourceRule` |
| `mapping.Plan` (`full`, `part`, `lookups` as lists) | `core.model.ChangePlan` (`rebuild`, `partial`, `lookups` of `PendingLookup`) |
| `mapping.norm`, `mapping.differs` | `core.keys.normalize_key`, `core.keys.column_changed` |
| `Registry.tables`, `Registry.audit()` | `Registry.rules_by_table`, `core.audit.audit_field_map()` |
| `engine.Engine` | `pipeline.engine.SyncEngine` |
| `service.Service` | `app.Application` + `pipeline.consumer.ChangeConsumer` + `pipeline.preflight.Preflight` |
| `es_writer.EsWriter`, `Op` | `connectors.elastic.ElasticsearchWriter`, `BulkOp` |
| `dlq.Dlq` | `connectors.dlq.DeadLetterQueue` |
| `util.Stop` | `lifecycle.StopSignal` |
| `BadEvent`, `RejectError` | `errors.InvalidEventError`, `errors.DocumentRejectedError` |

## Consequences

* Deployment: `docker-compose.yml` mounts `./indices` instead of `./sql`; the image must be rebuilt. Kafka offsets,
  topics, metrics and `.env` are unaffected.
* Log lines name the module (`cdc_es_sync.pipeline.consumer`) instead of `service`/`engine`.
* `sync.operations: []` is now an error instead of meaning "all"; leaving the setting out still means all.
* Code that imported the 1.0 modules (only `tests/live/` and `scripts/`) was updated.

Considered and not done: Pydantic for settings (a new runtime dependency for what ~150 lines of standard library
do), converting `tests/live/` to pytest (they change dev data and need the running service; they stay scripts), and a
second partition per topic (a capacity change, not a structural one).
