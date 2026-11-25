# Kafka & CDC platform monitoring (Grafana)

Central monitoring for Kafka, Kafka Connect and every Debezium connector deployed on it, whichever team owns the
connector. Four dashboards in the Grafana folder **CDC**, linked to each other in the top bar:

| Dashboard | uid | What it answers |
|---|---|---|
| Kafka & CDC Operations Center | `cdc-ops` | Is the platform healthy? Banner verdict, KPI tiles, **connector fleet table**, **Debezium engine table**, health cards for connectors / Kafka / consumers, 24h alert history, JVM resources |
| Debezium Connectors | `cdc-connectors` | Which connector or task is failing, and since when? Connector and task state timelines, source lag, events by operation, queue usage, snapshot, Oracle LogMiner, task throughput, errors / retries / DLQ, workers and rebalances |
| Kafka Cluster | `kafka-cluster` | Is the broker healthy and fast enough? KRaft controller, offline / under-replicated / under-min-ISR partitions, throughput, request latency breakdown, thread-pool saturation, replication, storage, JVM and containers |
| Topics & Consumer Groups | `kafka-topics` | Who is falling behind? Topic inventory (partitions, replicas, rate, retained messages, size), consumer group lag, consume rate, catch-up time, inactive groups |

## Connector health, as shown everywhere

A connector can be `RUNNING` while all its tasks are `FAILED` (Kafka Connect never restarts a failed task), so the
dashboards and alerts use a combined **Health**:

| Health | Meaning |
|---|---|
| OK | connector RUNNING and every task RUNNING |
| PAUSED | paused or stopped on purpose |
| DEGRADED | RUNNING but a task FAILED or is missing, or UNASSIGNED / RESTARTING |
| FAILED | the connector itself FAILED |

Debezium's own metrics are keyed by the **logical server name** (`topic.prefix`), not the connector name. To show the
connector name in the engine table, add `"custom.metric.tags": "connector=<connector name>"` to the connector config.

## Start

```
docker compose build kafka connect          # bakes the JMX exporter rules into the images
docker compose up -d
docker compose --profile monitoring up -d   # kafka-exporter, cAdvisor, Prometheus, Grafana
```

Grafana: `http://<host>:3000` (user `admin`, password `GRAFANA_PASSWORD`, default `change-me`: change it).
Prometheus: `http://127.0.0.1:${PROMETHEUS_PORT:-9090}`. Rebuilding `kafka` restarts the broker: plan a short window.

## Where the data comes from

| Prometheus job | Source | Gives |
|---|---|---|
| `kafka` | kafka-exporter | Brokers, topics, partitions, consumer group offsets, lag and members |
| `kafka-jmx` | broker JMX exporter (`dockerfiles/kafka/jmx-exporter.yml`) | KRaft controller, partition health, throughput, request latency, saturation, log size, JVM |
| `connect-jmx` | Connect JMX exporter (`dockerfiles/connect/jmx-exporter.yml`) | Connector / task state, task throughput, errors, DLQ, producer, rebalances, every Debezium engine, JVM |
| `containers` | cAdvisor | Container CPU and memory (Linux Docker hosts only) |

The JMX rules are explicit and anchored: label values never end up in metric names, cumulative `*-total` attributes
are counters (use `rate()`), and anything not listed is not exported. Add every extra Connect worker to the
`connect-jmx` job in `prometheus.yml`.

### Using a central Grafana instead

Point the central Grafana at this Prometheus (or remote-write this Prometheus to the central one), import the four
JSON files from `grafana/dashboards/`, and choose that data source in the **Data source** drop-down.

## Alerts

`alerts.yml` has 26 rules in five groups (kafka, kafka-connect, debezium, consumers, monitoring), shown in the alert
history table. Every connector-level rule carries the `connector` or Debezium `name` label, so the same rules cover
all teams' connectors. Prometheus only evaluates them; sending email / chat needs Alertmanager or Grafana contact points.

## Changing a dashboard

Edit `grafana/build-dashboards.mjs`, run `node build-dashboards.mjs`, commit the script and the JSON. The script
deletes and rewrites every JSON file, so a removed dashboard disappears from Grafana too. Grafana reloads the files
within 30 seconds. Dashboards are read-only in the UI (`allowUiUpdates: false`).
