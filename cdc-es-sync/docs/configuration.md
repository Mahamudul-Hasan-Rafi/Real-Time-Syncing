# Configuration

Two kinds of file, both YAML, both with `${VAR}` and `${VAR:default}` read from the environment or from `.env`
next to `config.yaml` (real environment variables win):

| File | Holds | Changed when |
|---|---|---|
| `config.yaml` | Runtime settings: where Kafka, Oracle and Elasticsearch are, batching, retries, logging | Deploying somewhere else, tuning |
| `indices/<name>.yaml` + `indices/<name>.sql` | One Elasticsearch index: its document query, key, conversions and field map | The query or the schema changes |

Settings are loaded into typed objects (`cdc_es_sync.settings`) once at start-up. A mistyped or unknown key, a
missing required key, or a value of the wrong type stops the service with exit code 2 and the path of the bad
setting, e.g. `config error: oracle: unknown setting(s) ['pool_mx']`. A key left empty (`key:`) keeps its default.

In Docker both are mounted read-only (`./config.yaml`, `./indices`): edit them and `docker compose restart`, no rebuild.

## `config.yaml`

| Setting | Default | Purpose |
|---|---|---|
| `kafka.bootstrap_servers` | required | `KAFKA_BOOTSTRAP`. Must match `KAFKA_HOST` in `Packages/cdc/.env`. |
| `kafka.group_id` | required | `CDC_GROUP`, default `cdc-es-sync`. One running instance per group. |
| `kafka.topic_pattern` | required | `^emob\.EMOB\..*`: one Debezium topic per table. |
| `kafka.dlq_topic` | required | `cdc.es-sync.dlq`. Created at start-up if missing. |
| `kafka.auto_offset_reset` | `earliest` | A new group, or a newly appearing table topic, is read from its first message, so no first event is missed. |
| `kafka.session_timeout_ms` | `45000` | After a hard kill, takeover waits this long. |
| `kafka.max_poll_interval_ms` | `1800000` | A very large rebuild may keep the loop busy for minutes without being thrown out of the group. |
| `batch.max_events`, `batch.max_wait_seconds` | `500`, `1.0` | A batch is up to 500 events or 1 second. Duplicates inside a batch collapse. |
| `oracle.user`, `oracle.password`, `oracle.dsn` | required | `ORACLE_USER`, `ORACLE_PASSWORD`, `ORACLE_DSN`. `ENABLE=BROKEN` in the DSN turns on TCP keepalive. |
| `oracle.schema` | `EMOB` | Owner of the tables; used to find the tables a query reads and for the log group check. |
| `oracle.pool_min`, `pool_max`, `fetch_array_size`, `call_timeout_ms` | `1`, `8`, `1000`, `300000` | Connection pool and statement limits. `pool_max` must be at least `sync.workers + 1`. |
| `oracle.timezone` | `UTC` (config: `Asia/Dhaka`) | Oracle `DATE` has no zone. Dates are written as UTC ISO strings, as the Logstash pipelines do. |
| `elasticsearch.targets.<name>` | at least one | `hosts`, `username` (`ES_USER`, default `elastic`), `password` (`ES_PASSWORD`), `verify_certs`, `ca_certs`, `request_timeout`. Each index names its `target`. |
| `elasticsearch.targets.<name>.verify_certs` | `true` (config: `${ES_VERIFY_CERTS:false}`) | **Certificate checking is currently off** in `config.yaml`. Set `ES_VERIFY_CERTS=true` and `ES_CA_CERT` (in Docker `/app/certs/ca.crt`, mounted from `Packages/cdc/elastic/ca/ca.crt`) to verify. |
| `sync.chunk_size` | `500` | Keys per Oracle query. IN lists stay well below Oracle's 1,000 limit. |
| `sync.bulk_max_actions` | `1000` | Operations per Elasticsearch bulk request. |
| `sync.workers` | `4` | Chunks of a rebuild or fan-out processed in parallel: each worker reads Oracle, builds the documents and sends its own bulk request. `1` processes chunks one after another. At most `2 x workers` chunks are in flight, which is what bounds the load on Oracle and Elasticsearch. |
| `sync.max_fanout`, `sync.throttle_ms` | `50000`, `0` | When one change touches more than `max_fanout` documents a warning is logged. `throttle_ms` adds a pause between chunks above that size; `0` (default) means none, because `workers` is the limit. |
| `sync.retry_max_backoff_seconds` | `30` | Cap of the exponential backoff for Oracle and Elasticsearch retries. |
| `sync.nested_date_offset` | `+06:00` | Added to dates inside the JSON columns, as the Logstash `ruby` filter does. |
| `sync.operations` | all of `c u d r` (config: `[u, d]`) | Change types that are applied: `u` update, `d` delete, `c` insert, `r` snapshot read. Other events are read and committed but nothing is written; they are counted in `cdc_events_skipped_total{reason="operation_not_enabled"}`. |
| `observability.http_port` | `9108` | `CDC_HTTP_PORT`. `/health` and `/metrics`. |
| `observability.health_stale_seconds` | `120` | `/health` returns 503 when the loop has not beaten for this long. |
| `observability.log_level`, `log_file` | `INFO`, none | Relative paths are relative to `config.yaml`. |
| `observability.dlq_file_dir` | `dlq` | Dead letters go here when the DLQ topic cannot be written. |
| `startup_checks.fail_on_missing_logging` | `true` | When false, a missing supplemental log group is logged but does not stop the service. |
| `indices` | required | The index files to load, e.g. `- indices/customer_data.yaml`. Comment a line out to stop syncing that index. |

## Index files: `indices/<name>.yaml`

A commented skeleton with a checklist is in [index-template.yaml](index-template.yaml); copy it when you add an index.

The file name (without `.yaml`) is the index's name in this service: `resync customer_data`, metric labels, rule ids.

| Key | Required | Meaning |
|---|---|---|
| `es_index` | yes | The Elasticsearch index written to (`transaction_history_` for `transaction_history`). |
| `target` | no, `default` | Which `elasticsearch.targets` entry. |
| `sql_file` | yes | The document query, relative to this file. The Logstash query, unchanged. |
| `key_columns` | yes | `[{col: ac_no, type: str}]`: output columns of the query forming the key; types `int`, `str`, `date`. |
| `key_filter` | yes | The SQL expression of each key column in the query (`A.AC_NO`), used to restrict it to a chunk of keys. |
| `doc_id` | yes | Format of the document id over the key columns: `"{ac_id}_{balance_date}"`. |
| `timestamp_field` | no | Output field copied to `@timestamp`; otherwise `@timestamp` is the load time. |
| `json_columns` | no | Columns holding JSON text, parsed into arrays/objects. |
| `nested_date_fields` | no | `column: [fields]`: dates inside those JSON arrays get `sync.nested_date_offset`. |
| `etl_pipeline` | no, the name | Written into every rebuilt document (`cdc_<index>`). |
| `reconcile_sum_field` | no | Numeric output field totalled by `reconcile`. Without it, `reconcile` compares counts only (the report shows `-` under "sum of"). |
| `not_propagated` | no | `TABLE: reason` for tables the query reads whose changes are deliberately not followed. |
| `sources` | yes | The field map, below. |

### The field map (`sources`)

For every table the query reads, which of its columns feed which document fields:

```yaml
sources:
  MB_CUSTOMER_MST:
    - params: {CUST_NO: int}          # how to find the affected documents: a lookup query ...
      sql: >-
        SELECT t.tran_id FROM emob.mb_transaction_dtl t
        JOIN emob.mb_account_mst a ON a.ac_id = t.ac_id
        WHERE a.cust_no = :CUST_NO
      columns:                         # ... and what each changed column refreshes
        CUST_NO: rebuild
        MOBILE_NO: same                #   same = the field with the same name (mobile_no)
        TOKEN_NO: [aml_token_no]       #   or a list of fields
```

An entry finds the affected documents in one of two ways:

| Entry | Meaning |
|---|---|
| `direct: [COLS]` | The changed row *is* the document; these event columns form the key (in `key_columns` order). |
| `sql` + `params` | A lookup query returning the affected document keys (same column order as `key_columns`). `:NAME` binds are filled from the event columns named in `params`, with their type. |
| `join: inner \| outer` | Set on the entry for a table in the query's top-level FROM list. Inserting or deleting a row of an inner-join table creates or removes the document, so it is rebuilt whole. |

and says what each column does when it changes:

| Column value | Meaning |
|---|---|
| `same` | Refresh the field with the same (lower case) name. |
| `[f1, f2]` | Refresh these fields. |
| `rebuild` | Key or join column: rebuild the whole document (and delete the old one if the key moved). |

An update event is compared before/after: entries whose listed columns did not change do nothing. Every event column
used in `direct` / `params` must be in an `ALWAYS` supplemental log group, because that is what identifies a row in
the event (the `EMOB` schema has no primary keys).

### The field map is checked, not trusted

| Check | Where | What it proves |
|---|---|---|
| Field-map audit | `check`, start-up, `tests/unit/test_audit.py` | The SQL is scanned to find which table column feeds which output field. Every column the query reads must be listed with every field it feeds, nothing listed may be a column the query does not read, and join columns must be `rebuild`. |
| Coverage audit | same | Every table a query reads is mapped (or listed under `not_propagated`). |
| Compile check | `check` | All document queries (in full and as field projections) and all lookup queries parse against Oracle, and the parsed output columns equal Oracle's. |
| Log group check | `check`, start-up | Every event column used to find documents is in an `ALWAYS` supplemental log group. |
| Lookup completeness | `tests/live/lookup_crosscheck.py` | For real rows of every child and dimension table, the lookup returns every document that depends on the row. |

After a schema change run `scripts/dump_columns.py` so the offline audit sees the new columns.

## Environment variables

| Variable | Used for |
|---|---|
| `ORACLE_USER`, `ORACLE_PASSWORD` | Required. |
| `ORACLE_DSN`, `KAFKA_BOOTSTRAP`, `ES_URL` | Addresses. Inside a container `localhost` is the container: use the machine's IP or the stack network. |
| `ES_USER`, `ES_PASSWORD`, `ES_VERIFY_CERTS`, `ES_CA_CERT` | Elasticsearch login and TLS. |
| `CDC_GROUP`, `CDC_OFFSET_RESET`, `CDC_HTTP_PORT` | Consumer group, first-start position, metrics port. |
| `ES_CA_FILE`, `METRICS_PORT`, `CDC_ES_SYNC_VERSION`, `TZ`, `STACK_NETWORK` | Docker Compose only. |

Secrets live only in the git-ignored `.env`. Never commit it.
