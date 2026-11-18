# CDC monitoring (Grafana)

Three dashboards in the Grafana folder **CDC**, fed by Prometheus and linked to each other in the top bar:

| Dashboard | uid | What it shows |
|---|---|---|
| Oracle CDC Operations Center | `cdc-ops` | Executive overview: health banner, 7 icon tiles (Canvas), a health gauge + check list + trend for Oracle/LogMiner, Debezium and Kafka, retention and CDC lag, target apply, and a 24h alert history with FIRING / Resolved |
| CDC Pipeline Details | `cdc-details` | Every Oracle, Debezium, Kafka and cdc-es-sync metric as stats and charts (the "View details" target) |
| CDC Archive Log Storage | `cdc-archive` | Recovery area usage, archive generation rate, retention, archive destination status, and whether the connector can still resume |

The section gauges show the share of health checks that pass: Oracle (open, ARCHIVELOG, archive destination valid, FRA under 90%), Debezium (connected, task running, no failed task, lag under 30 s), Kafka (broker up, no under-replicated partitions, consumer lag under 1000, exporter up).

## Start

1. Create the read-only Oracle user (once, as SYSDBA): `sqlplus / as sysdba @dba_scripts/dba_cdc_04_monitoring_user.sql`
2. In `Packages/cdc/.env` set `ORACLE_MON_USER=c%23%23cdcmon` (the `#` is written `%23`) and `ORACLE_MON_PASSWORD`.
3. `docker compose --profile monitoring up -d`
4. Grafana: `http://<host>:3000` (user `admin`, password `GRAFANA_PASSWORD`, default `change-me`: change it). Prometheus: `http://127.0.0.1:9090`.

Without step 1-2 everything works except the Oracle panels, which show NO DATA, and the overall status tile shows "Attention needed".

## Where the data comes from

| Prometheus job | Source | Gives |
|---|---|---|
| `oracle` | `oracle-exporter` running `oracle/custom-metrics.toml` | FRA usage, archive log volume and retention, archive destination, DB/PDB state, SCN |
| `connect-jmx` | Connect JMX exporter | Debezium lag, LogMiner connection, connector/task state |
| `kafka`, `kafka-jmx` | kafka-exporter, Kafka JMX | Brokers, topics, consumer lag |
| `cdc-es-sync` | `host.docker.internal:9108` | Events, documents written, errors, dead letters, lag |

`cdc-es-sync` runs in its own compose project, so Prometheus reaches it through the host's published port 9108.

The `containers` job (cAdvisor) is scraped but no panel uses it. On Docker Desktop cAdvisor reports containers by cgroup id only, so there are no container names to chart. On a Linux host the `name` label is present and a CPU/memory row can be added.

Elasticsearch itself is not monitored here (only the sync service that writes to it). `check-cdc.ps1` covers Elasticsearch health.

When Oracle cannot be reached, every Oracle panel shows a grey NO DATA and the top banner reads "Attention needed". Both are intended.

## Archive log storage (what the client asked to track)

- **FRA used %**: warning at 80%, critical at 90%. When the recovery area is full Oracle stops archiving.
- **Hours until full**: free plus reclaimable space divided by the last hour's archive rate.
- **Archive destination**: must be `VALID`. `ERROR` (for example ORA-16032) stops Debezium from starting.
- **Retention**: age of the oldest archive log still on disk.
- **Connector can resume**: `AT RISK` means the archive log holding the connector's position was deleted. After a restart the connector fails with "no longer available on the server" and needs an offset reset (runbook item 1).

## Alerts

`alerts.yml` has 16 rules, shown in the "Firing alerts" table and counted in "Active alerts". Prometheus only evaluates them. Nothing sends email or chat yet: that needs Alertmanager or Grafana contact points, which depends on where the client wants notifications.

## Changing a dashboard

Edit `grafana/build-dashboards.mjs`, run `node build-dashboards.mjs`, commit the script and the JSON. Grafana reloads the files within 30 seconds. Dashboards are read-only in the UI (`allowUiUpdates: false`).
