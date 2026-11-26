# connect-guardian

Automatic recovery for every connector on the Kafka Connect cluster. Kafka Connect never restarts a FAILED task,
and a Debezium task whose stored position is gone fails again on every restart. The guardian fixes both, per
connector, so one application's connector problem never needs a stack restart or a volume wipe.

It runs as a service in `../docker-compose.yml`, checks `GET /connectors?expand=status&expand=info` every 15 s and
covers new connectors without any configuration.

## What it does

| Situation | Detected by | Action |
|---|---|---|
| Transient failure: database or Kafka unreachable, LogMiner error, slow archiver | task/connector `FAILED`, any other trace | Restart failed tasks with backoff 15 s → 30 s → … → 5 min, forever |
| Permanent failure: bad password, locked account, missing privilege, no valid archive destination, supplemental logging off, record too large | ORA-01017 / 28000 / 28001 / 01031 / 01950 / 00942 and similar in the trace | Restart every 5 min (recovers by itself once fixed); condition *needs attention* |
| **Position lost**: archive logs with the stored SCN purged, schema history topic deleted, snapshot interrupted with snapshots disabled | `…is no longer available on the server`, `db history topic is missing`, … | **Stop → delete offsets → resume** (policy `reset`), or leave it FAILED and alert (policy `alert`) |
| Stalled capture: task RUNNING but writes nothing, not even heartbeats | `source_record_write_total` unchanged for `STALL_SECONDS` (only for connectors with `heartbeat.interval.ms` set) | Restart that task |
| Task UNASSIGNED for 2 min | task state | Restart that task |
| Connector PAUSED or STOPPED by an operator | connector state | Nothing: deliberate |

A failed task is considered recovered after 10 minutes of RUNNING. A second position loss within 30 minutes of a reset
is not reset again: the connector is left for a person (condition *needs attention*).

A purged log in the middle of streaming first shows up as `LogFileNotFoundException`, which a slow archiver also
causes. That is restarted as transient; if the log really is gone, Debezium's own start-up check then fails with
"no longer available" and the reset follows on the next check.

## What the application gets after a reset: its own `snapshot.mode`

After the offsets are deleted the connector starts like a new one, so the owning application decides, through the
connector config it deploys:

| `snapshot.mode` | After a reset |
|---|---|
| `initial` (Debezium default) | Full snapshot of the captured tables (`op: r` events), then streaming |
| `when_needed` | Same as `initial`. Debezium already does this by itself when the position is lost, so the guardian never needs to reset it |
| `no_data` | Streams from the current database position. Changes made during the gap are **not** captured |

Consumers keep running throughout: no topic, consumer group or other connector is touched. A full snapshot re-sends
existing rows, so consumers must upsert. Rows deleted during the gap are not re-sent by any mode.

## Settings for the application, in the connector config

Kafka Connect passes unknown keys through, so these go in the same JSON as the rest of the connector:

| Key | Values | Default |
|---|---|---|
| `guardian.enabled` | `true` / `false` | `true` |
| `guardian.on.position.lost` | `reset` / `alert` | `ON_POSITION_LOST` (`reset`) |
| `guardian.stall.seconds` | seconds, `0` = off | `STALL_SECONDS` (900) |

Recommended for every Debezium connector: `heartbeat.interval.ms` (e.g. `10000`). Without it, the stored SCN stops
moving while the captured tables are quiet, so a restart after a quiet weekend needs logs that may already be purged,
and stall detection stays off.

## Events: `connect-guardian-events`

One JSON message per action, keyed by connector name, kept 30 days. Applications that need to react (start a reload,
raise a ticket) consume this topic.

```json
{"type": "offsets_reset", "connector": "emob-oracle-cdc", "time": "2026-11-26T12:59:11Z", "dry_run": false,
 "reason": "position_lost", "error": "io.debezium.DebeziumException: … no longer available on the server …",
 "offsets_before": [{"partition": {"server": "emob"}, "offset": {"scn": "33798104234751", "…": "…"}}],
 "snapshot_mode": "no_data",
 "effect": "streaming from the current database position; changes made while the position was lost are not captured"}
```

`type` is one of:
- `offsets_reset`
- `position_lost`: not reset, because of the policy or the cooldown.
- `needs_attention`: a permanent error.
- `restarted`: sent on the 1st and 5th restart, then every 20th. `failure_class` is `transient`, `permanent`, `stalled` or `unassigned`.
- `recovered`

`offsets_before` is where the gap starts.

## Metrics (`:9405`, Prometheus job `connect-guardian`)

| Metric | Meaning |
|---|---|
| `connect_guardian_connector_condition{connector}` | 0 healthy, 1 recovering, 2 needs attention, 3 parked, 4 disabled |
| `connect_guardian_connector_failure{connector,class}` | 1 for the current failure class |
| `connect_guardian_consecutive_restarts{connector}` | Restarts since it last ran stably |
| `connect_guardian_actions_total{connector,action,reason}` | `restart`, `reset`, `restart_stalled`, `restart_unassigned` |
| `connect_guardian_last_reset_timestamp_seconds{connector}` | Time of the last offset reset |
| `connect_guardian_connect_up`, `connect_guardian_last_check_timestamp_seconds` | Guardian liveness |

Alerts are in `../monitoring/alerts.yml`, group `connect-guardian`.

## Operating it

- **First UAT run**: set `DRY_RUN: "true"`. It logs and publishes what it would do and changes nothing.
- **Logs**: `docker logs connect-guardian`.
- **State**: `/data/state.json` in the `guardian-data` volume. If the guardian dies between stop and resume, it
  resumes the connector when it starts again.
- **Taking a connector out**: set `guardian.enabled=false`, or PAUSE or STOP it (the guardian leaves those alone).
- **Single owner**: only one process should restart connectors. Application-side watchdogs (for example the
  `connector-watchdog` service of cdc-es-sync) should be removed once this runs.
