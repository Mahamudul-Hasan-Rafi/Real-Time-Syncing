# Architecture

## How a change event becomes a document update

The documents are not copies of single tables. Each one joins several tables, with lookups and JSON arrays.
A change event tells us **which row changed and which columns**, so the service works in three steps:

1. **Which columns changed?** An update event carries the old and the new value of every column that changed. The
   service compares them (an empty string and NULL count as equal). If none of the changed columns is listed for an
   index in its index file, that index is not touched at all.
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
| Only the changed fields are written | The other fields, including `etl_loaded_at`, stay byte-identical. Documents of other indices are not written. |
| Deletes, moves and key changes just work | Both the `before` and `after` row images feed the key lookup. |
| At-least-once delivery | Kafka offsets are committed only after Elasticsearch accepted the batch (`tests/unit/test_consumer.py`). |

**Example: a customer changes `MOBILE_NO`.** The customer's account documents in `customer_data` and every
transaction of the customer's accounts in `transaction_history_` get the new `mobile_no`. `ac_balance_data` has no
mobile number field and is not touched. Changing a column nobody reads writes nothing.

## One batch, step by step

```
Kafka messages ──► ChangeConsumer.collect()     up to batch.max_events, or batch.max_wait_seconds
                   ChangeConsumer.handle()      parse_event() each; malformed ones -> dead letter
                       │
                       ▼
                   SyncEngine.process()
                       Registry.route()         every Change -> ChangePlan  (duplicates collapse)
                         ├─ rebuild:  index -> keys                     full document query, `index` op
                         ├─ partial:  index -> key -> fields            projected query, `update` op
                         └─ lookups:  rule + bind values -> fields      lookup SQL -> keys -> one of the above
                       OracleReader.fetch_rows / fetch_fields           chunks of sync.chunk_size keys
                       transform.build_document / build_partial
                       ElasticsearchWriter.apply()                      bulk, retried while ES is down
                       │
                       ▼
                   consumer.commit(offset + 1 per partition)            only now
```

Failure isolation: a transient Oracle error retries the whole batch with backoff; any other error splits the batch
and processes it event by event, so only the poison event goes to the dead letter output.

## Code layout and the dependency rule

```
cli ──► app ──► pipeline ──► connectors ──► (confluent_kafka, oracledb, elasticsearch)
                    │             │
                    └──────► core ◄┘        core imports no client library and does no I/O
```

| Package | Holds | Rule |
|---|---|---|
| `core/` | Event parsing, key normalization, the compiled field map (`IndexSpec`, `SourceRule`, `ChangePlan`), routing, the SQL scanner, the field-map audit, the row-to-document transform | Pure functions and data. Everything here is unit-tested without fakes for the outside systems. |
| `connectors/` | One module per outside system: `kafka`, `oracle`, `elastic`, `dlq` | Retries and error classification for that system live here (`is_transient`, the bulk retry loop). |
| `pipeline/` | `SyncEngine` (apply a batch), `ChangeConsumer` (the Kafka loop), `Preflight` (start-up checks), `reconcile` | Depends on `core` and on connector interfaces; tests pass fakes in. |
| `observability/` | JSON logs, Prometheus metrics, `/health` | Metric names are a contract with dashboards and alerts. |
| `settings.py` | Typed settings | Loaded and validated once; nothing else reads YAML or the environment. |
| `app.py` | `Application`, the composition root | The only place that constructs clients. |

## Vocabulary

| Term | Meaning |
|---|---|
| Index definition | `indices/<name>.yaml`: one Elasticsearch index, compiled into an `IndexSpec`. |
| Source rule | One entry under `sources.<TABLE>` (`SourceRule`): how a change of that table locates documents and which fields each column feeds. |
| Direct rule | The changed row *is* the document; its event columns form the key. |
| Lookup rule | A SQL query, bound with event columns, returns the keys of the affected documents. |
| Rebuild | Re-run the full document query for a key and index the whole document. |
| Partial refresh | Re-run the query projected to some fields and send an `update` with only those fields. |
| Fan-out | The number of documents one change affects through a lookup (throttled above `sync.max_fanout`). |
