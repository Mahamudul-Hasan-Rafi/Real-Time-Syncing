# Real-Time-Syncing

Real-time change data capture (CDC) pipeline that keeps Elasticsearch in step with an Oracle schema (`EMOB`).

```
Oracle (archive log) -> Debezium (LogMiner) -> Kafka  emob.EMOB.<TABLE>  ->  cdc-es-sync  -> Elasticsearch -> Kibana
                                                                                 ^
                                  Logstash: one-off initial load of the three indices -+
```

## Repository layout

| Path | Purpose |
|---|---|
| [Packages/cdc/](Packages/cdc) | Kafka (KRaft), Kafka Connect with the Debezium Oracle connector, Kafka UI, Prometheus and Grafana monitoring |
| [Packages/cdc/dba_scripts/](Packages/cdc/dba_scripts) | SQL run by a DBA: prerequisite checks, archive log mode, CDC user setup |
| [Packages/cdc/emob-oracle-connector.json](Packages/cdc/emob-oracle-connector.json) | Debezium connector configuration, a template: `connector-init` fills in the `${...}` values from `Packages/cdc/.env` |
| [Packages/cdc/check-cdc.ps1](Packages/cdc/check-cdc.ps1) | Health check of the whole pipeline |
| [Packages/cdc/elastic/](Packages/cdc/elastic) | Elasticsearch and Kibana compose file, plus the Logstash load (`config/`, `pipeline/`, `templates/`, `scripts/`) |
| [cdc-es-sync/](cdc-es-sync) | Python service that consumes the change events and updates the affected Elasticsearch documents. See its [README](cdc-es-sync/README.md) |

## Components

- **Oracle**: source database, with archive log mode and a CDC user prepared by the DBA scripts.
- **Debezium Oracle connector**: reads changes via LogMiner and publishes one Kafka topic per table.
- **Kafka**: transports the change events.
- **Logstash**: initial/batch load of the indices `customer_data`, `ac_balance_data` and `transaction_history_`.
- **cdc-es-sync**: incremental sync; updates only the changed fields (or rebuilds whole documents) from Oracle.
- **Elasticsearch / Kibana 8.4.3**: search store and UI.

## Before you start

| You need | Notes |
|---|---|
| Docker with Compose v2 | Internet access on the first run: images are built from `Packages/cdc/dockerfiles/` and pulled. Podman: see the note at step 3. |
| About 16 GB RAM free, 20 GB disk | Elasticsearch asks for 8 GB heap / 12 GB by default; lower it with `ES_HEAP` and `ES_MEM_LIMIT` in `Packages/cdc/elastic/.env`. On Linux also run `sysctl -w vm.max_map_count=262144`. |
| PowerShell 5.1+ (Windows) or `sh` (Linux/macOS) | Commands below are for PowerShell, run from the repository root. Linux equivalents are noted. |
| Oracle with archive logging and the CDC user | Step 1. The CDC user's password and the Oracle host are needed at step 2. |
| An Oracle user that can read the `EMOB` tables | Used by the initial load and by `cdc-es-sync`. |
| Free ports | 9222 Kafka, 8083 Connect, 8085 Kafka UI, 9090 Prometheus, 3000 Grafana, 9200 Elasticsearch, 5601 Kibana, 9108 sync service. Change them in the `.env` files. |

Secrets live only in git-ignored `.env` files (one next to each `.env.example`). Never commit them.

## Getting started

Run everything from the repository root. Each step ends with how to see that it worked.

**1. Prepare Oracle.** A DBA runs the scripts in `Packages/cdc/dba_scripts/` in order `01` to `03`
(check, archive log mode, CDC user `c##cdcuser`).

**2. Configure and start Kafka, Connect and monitoring.**

```powershell
if (-not (Test-Path Packages/cdc/.env)) { Copy-Item Packages/cdc/.env.example Packages/cdc/.env }   # Linux: cp -n Packages/cdc/.env.example Packages/cdc/.env
# edit Packages/cdc/.env: KAFKA_HOST (this machine's IP), ORACLE_HOST, CDC_USER_PASSWORD
docker compose -f Packages/cdc/docker-compose.yml up -d
```

`KAFKA_HOST` is required: compose stops with a clear message if it is empty. `connector-init` registers the Debezium
connector for you once Connect is healthy, filling `ORACLE_HOST` and `CDC_USER_PASSWORD` into the connector template, so
there is nothing to edit in a tracked file.

**3. Check the connector.**

```powershell
curl.exe -s http://localhost:8083/connectors/emob-oracle-cdc/status     # connector and task: "state":"RUNNING"
docker compose -f Packages/cdc/docker-compose.yml logs connector-init   # "connector emob-oracle-cdc registered"
```

If `connector-init` printed an error (Oracle unreachable, empty password), fix `Packages/cdc/.env` and run
`docker compose -f Packages/cdc/docker-compose.yml run --rm connector-init`. To change a registered connector, delete it first
(`curl.exe -X DELETE http://localhost:8083/connectors/emob-oracle-cdc`), then run that command again.
Podman: `docker-compose.podman.yml` has no `connector-init`; register by hand with
`envsubst < Packages/cdc/emob-oracle-connector.json | curl -X POST -H "Content-Type: application/json" --data @- http://localhost:8083/connectors`
(with `ORACLE_HOST`, `ORACLE_PORT` and `CDC_USER_PASSWORD` exported).

**4. Start Elasticsearch and Kibana.**

```powershell
if (-not (Test-Path Packages/cdc/elastic/.env)) { Copy-Item Packages/cdc/elastic/.env.example Packages/cdc/elastic/.env }
docker compose -f Packages/cdc/elastic/docker-compose.yml up -d
curl.exe -s "http://localhost:9200/_cluster/health?pretty"             # "status" : "green"
```

**5. Initial load with Logstash** (about 15 minutes for the dev data).

```powershell
# edit Packages/cdc/elastic/.env: ORACLE_USER, ORACLE_PASSWORD (and ORACLE_JDBC_URL / ES_HOSTS if not the dev servers)
cd Packages/cdc/elastic/logstash-8.4.3/logstash-8.4.3
powershell -NoProfile -ExecutionPolicy Bypass -File scripts\setup.ps1   # once: downloads Logstash 8.4.3 and the Oracle JDBC driver, verifies their checksums
.\scripts\run_pipelines.cmd --test   # optional: validates the three pipelines
.\scripts\run_pipelines.cmd          # loads customer_data, ac_balance_data, transaction_history_; exits when done
cd ../../../../..
```

Linux/macOS: `scripts/setup.sh`, `scripts/run_pipelines.sh`. The three indices are created with the strict mappings in
`templates/`. Logstash exits normally even if a database read failed midway, so confirm the load with `reconcile` in step 6.

**6. Configure and start the sync service.**

```powershell
cd cdc-es-sync
if (-not (Test-Path .env)) { Copy-Item .env.example .env }     # never overwrite an existing .env
# edit .env: ORACLE_USER, ORACLE_PASSWORD, KAFKA_BOOTSTRAP, ES_URL, ORACLE_DSN
docker compose build
docker compose run --rm cdc-es-sync check        # 9 checks, all must PASS
docker compose run --rm cdc-es-sync reconcile    # Oracle vs Elasticsearch counts and totals: all OK
docker compose up -d
cd ..
```

More in [cdc-es-sync/README.md](cdc-es-sync/README.md).

**7. Check the whole pipeline.**

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File Packages/cdc/check-cdc.ps1      # "CDC OK ..." and exit code 0
```

It checks the containers, the connector and its task, Elasticsearch health, that the three indices exist and hold
documents, the sync service (`/health` and consumer lag) and free disk. Add `-SkipSync` before step 6.

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `required variable KAFKA_HOST is missing a value` | `Packages/cdc/.env` is missing or `KAFKA_HOST` is empty (step 2). |
| `connector-init`: `cannot reach Oracle` / `ORACLE_HOST` not set | Fix `Packages/cdc/.env`, then `docker compose -f Packages/cdc/docker-compose.yml run --rm connector-init`. |
| Connector status `FAILED` | `docker logs connect`. Usual causes: wrong CDC user password, archive logging off, missing privileges (step 1). |
| `up -d` restarted containers | Compose recreates a container when its compose file changed. Data is in named volumes and survives. |
| Logstash: `Logstash is not installed` / `JDBC driver is missing` | Run `scripts\setup.ps1` (step 5). |
| PowerShell: `running scripts is disabled on this system` | Start the script as `powershell -NoProfile -ExecutionPolicy Bypass -File <script>`, as shown above. |
| Logstash: `ORACLE_PASSWORD` empty / ORA-01017 | Set the credentials in `Packages/cdc/elastic/.env` (step 5). |
| `reconcile` shows MISMATCH right after the load | Run it again when no changes are flowing; if it persists, re-run the load. |
| Sync service `check` fails on Kafka / Elasticsearch | `localhost` inside a container is the container: use the machine's IP in `cdc-es-sync/.env`, or the Docker network (`docker-compose.stack.yml`). |
| Logstash dies at once: `insufficient memory`, `paging file is too small` | The JVM could not reserve its 4 GB heap (`config/jvm.options`). Add `LS_JAVA_OPTS=-Xms1g -Xmx4g` to `Packages/cdc/elastic/.env` and run the load again. |
| Elasticsearch exits at start | Not enough memory: lower `ES_HEAP` / `ES_MEM_LIMIT`; on Linux set `vm.max_map_count`. |

## Notes

- The Logstash program, its bundled JDK and the Oracle JDBC driver are not in the repository; `scripts/setup.*` downloads them.
- Grafana's admin password defaults to `change-me`: set `GRAFANA_PASSWORD` in `Packages/cdc/.env`.
- Elasticsearch runs with security disabled (plain HTTP, no login): keep its ports off untrusted networks.
- Logstash details: [README_PIPELINES.md](Packages/cdc/elastic/logstash-8.4.3/logstash-8.4.3/README_PIPELINES.md).
