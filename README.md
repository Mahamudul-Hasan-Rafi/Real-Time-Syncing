# Real-Time-Syncing

Real-time change data capture (CDC) pipeline that keeps Elasticsearch in step with an Oracle schema (`EMOB`).

```
Oracle (archive log) -> Debezium (LogMiner) -> Kafka  emob.EMOB.<TABLE>  ->  cdc-es-sync  -> Elasticsearch -> Kibana
```

## Repository layout

| Path | Purpose |
|---|---|
| [Packages/cdc/](Packages/cdc) | Kafka (KRaft), Kafka Connect with the Debezium Oracle connector, Kafka UI, Prometheus and Grafana monitoring |
| [Packages/cdc/dba_scripts/](Packages/cdc/dba_scripts) | SQL run by a DBA: prerequisite checks, archive log mode, CDC user setup |
| [Packages/cdc/emob-oracle-connector.json](Packages/cdc/emob-oracle-connector.json) | Debezium connector configuration (registered via the Kafka Connect REST API) |
| [Packages/cdc/elastic/](Packages/cdc/elastic) | Elasticsearch and Kibana compose file, plus Logstash `config/` and `pipeline/` for the batch (full) load |
| [cdc-es-sync/](cdc-es-sync) | Python service that consumes the change events and updates the affected Elasticsearch documents. See its [README](cdc-es-sync/README.md) |

## Components

- **Oracle**: source database, with archive log mode and a CDC user prepared by the DBA scripts.
- **Debezium Oracle connector**: reads changes via LogMiner and publishes one Kafka topic per table.
- **Kafka**: transports the change events.
- **Logstash**: initial/batch load of the indices `customer_data`, `ac_balance_data` and `transaction_history_`.
- **cdc-es-sync**: incremental sync; rebuilds only the changed fields (or whole documents) from Oracle.
- **Elasticsearch / Kibana 8.4.3**: search store and UI.

## Getting started

1. Run the scripts in `Packages/cdc/dba_scripts/` (in order `01` to `03`) on Oracle.
2. Start Kafka, Connect and monitoring: `docker compose -f Packages/cdc/docker-compose.yml up -d`
   (or `docker-compose.podman.yml` for Podman).
3. Replace the `<ORACLE_HOST>` and `<CDC_USER_PASSWORD>` placeholders in the connector config (do not commit the real values), then register it:
   `curl -X POST -H "Content-Type: application/json" --data @Packages/cdc/emob-oracle-connector.json http://localhost:8083/connectors`
4. Start Elasticsearch and Kibana: `docker compose -f Packages/cdc/elastic/docker-compose.yml up -d`
5. Run the initial load with Logstash using the pipelines in `Packages/cdc/elastic/logstash-8.4.3/logstash-8.4.3/pipeline/`.
6. Configure and start the sync service: copy `cdc-es-sync/.env.example` to `.env`, then follow
   [cdc-es-sync/README.md](cdc-es-sync/README.md).
7. Check the pipeline health with `Packages/cdc/check-cdc.ps1`.

## Notes

- The Logstash distribution (JDK, binaries) and `.env` files are not tracked; download Logstash 8.4.3 separately.
- Do not commit real credentials; use `.env` files and keep the connector config's secrets out of version control.
