"""connect-guardian: keeps every connector on the Kafka Connect cluster running, without a person.

Kafka Connect never restarts a FAILED task by itself, and a Debezium task whose stored position is gone (the DBA
purged the archive logs it needed) fails again on every restart. The guardian watches all connectors through the
Connect REST API and, per connector:

  transient failure   (database or Kafka unreachable, mining error, ...)  restart with exponential backoff
  permanent failure   (bad password, missing privilege, ...)              restart every RETRY_MAX_SECONDS, alert
  position lost       (archive logs purged, schema history missing)       stop, delete the offsets, resume
  stalled task        (RUNNING but writes nothing, heartbeat enabled)      restart the task
  task UNASSIGNED     (longer than UNASSIGNED_SECONDS)                     restart the task

After an offset reset the connector starts as if new, so its own snapshot.mode decides what the application gets:
initial / when_needed take a full snapshot, no_data streams from the current database position (changes made in the
gap are not captured). That choice belongs to the application that owns the connector, not to the platform.

Every action is published to the EVENTS_TOPIC topic (keyed by connector) so dependent applications can react, and is
exported as Prometheus metrics on METRICS_PORT. PAUSED and STOPPED connectors are deliberate and are left alone.

Per-connector settings (in the connector config; Kafka Connect passes unknown keys through untouched):
  guardian.enabled            true | false       default true
  guardian.on.position.lost   reset | alert      default ON_POSITION_LOST
  guardian.stall.seconds      seconds, 0 = off   default STALL_SECONDS
"""
from __future__ import annotations

import json
import logging
import os
import re
import signal
import socket
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

from confluent_kafka import KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from prometheus_client import Counter, Gauge, start_http_server

log = logging.getLogger("connect-guardian")


def _env_bool(name: str, default: bool) -> bool:
    return os.environ.get(name, str(default)).strip().lower() in ("1", "true", "yes")


CONNECT_URL = os.environ.get("CONNECT_URL", "http://connect:8083").rstrip("/")
CONNECT_METRICS_URL = os.environ.get("CONNECT_METRICS_URL", "http://connect:9404/metrics")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092")
EVENTS_TOPIC = os.environ.get("EVENTS_TOPIC", "connect-guardian-events")
EVENTS_REPLICATION_FACTOR = int(os.environ.get("EVENTS_REPLICATION_FACTOR", "1"))
EVENTS_RETENTION_DAYS = int(os.environ.get("EVENTS_RETENTION_DAYS", "30"))
CHECK_INTERVAL = float(os.environ.get("CHECK_INTERVAL_SECONDS", "15"))
RETRY_MIN = float(os.environ.get("RETRY_MIN_SECONDS", "15"))
RETRY_MAX = float(os.environ.get("RETRY_MAX_SECONDS", "300"))
STABLE_SECONDS = float(os.environ.get("STABLE_SECONDS", "600"))
ON_POSITION_LOST = os.environ.get("ON_POSITION_LOST", "reset").strip().lower()
RESET_COOLDOWN = float(os.environ.get("RESET_COOLDOWN_SECONDS", "1800"))
STALL_SECONDS = float(os.environ.get("STALL_SECONDS", "900"))
UNASSIGNED_SECONDS = float(os.environ.get("UNASSIGNED_SECONDS", "120"))
STOP_TIMEOUT = float(os.environ.get("STOP_TIMEOUT_SECONDS", "120"))
DRY_RUN = _env_bool("DRY_RUN", False)
STATE_FILE = Path(os.environ.get("STATE_FILE", "/data/state.json"))
METRICS_PORT = int(os.environ.get("METRICS_PORT", "9405"))

# Failure classes, matched against the connector or task stack trace in this order.
POSITION_LOST = "position_lost"
PERMANENT = "permanent"
TRANSIENT = "transient"

_PATTERNS = {
    # Debezium refuses to start: the stored offset cannot be resumed. Restarting never helps; a reset does.
    POSITION_LOST: [
        r"is no longer available on the server",               # archive / redo log with the offset SCN purged
        r"db[- ]history topic is missing",                      # schema history topic deleted
        r"previously stopped while taking a snapshot",          # snapshot interrupted, snapshots now disabled
        r"Could not find existing transaction log information",  # snapshot.mode=recovery without offsets
    ],
    # Needs a person (DBA or connector owner). Retried slowly so the connector recovers by itself once fixed.
    PERMANENT: [
        r"ORA-01017", r"ORA-28000", r"ORA-28001",               # bad credentials, account locked, password expired
        r"ORA-01031", r"ORA-01950", r"ORA-00942",               # missing privilege / quota / table access
        r"Failed to locate a local and valid archive destination",
        r"[Ss]upplemental logging",
        r"RecordTooLargeException",
        r"ConfigException",
        r"ClassNotFoundException",
    ],
}
_COMPILED = {cls: [re.compile(p) for p in pats] for cls, pats in _PATTERNS.items()}


def classify(trace: str) -> str:
    for cls in (POSITION_LOST, PERMANENT):
        if any(p.search(trace) for p in _COMPILED[cls]):
            return cls
    # Everything else, including LogFileNotFoundException mid-stream: a slow archiver also raises it. If the log is
    # really gone, the restart fails at start-up with "no longer available" and is then handled as position lost.
    return TRANSIENT


def trace_head(trace: str, limit: int = 600) -> str:
    """The exception line plus the first 'Caused by', which is what an operator needs."""
    lines = [line.strip() for line in trace.splitlines() if line.strip()]
    if not lines:
        return ""
    head = [lines[0]] + [line for line in lines[1:] if line.startswith("Caused by:")][:1]
    return " | ".join(head)[:limit]


# ---- metrics ------------------------------------------------------------------------------------------------------
# condition: 0 healthy, 1 recovering (failed, guardian retrying), 2 needs attention (permanent error, or position
# lost with policy alert), 3 parked (PAUSED/STOPPED by an operator), 4 guardian disabled for this connector
HEALTHY, RECOVERING, ATTENTION, PARKED, DISABLED = range(5)

M_CONNECT_UP = Gauge("connect_guardian_connect_up", "1 when the Kafka Connect REST API answers")
M_LAST_LOOP = Gauge("connect_guardian_last_check_timestamp_seconds", "Time of the last completed check")
M_CONDITION = Gauge("connect_guardian_connector_condition",
                    "0 healthy, 1 recovering, 2 needs attention, 3 parked, 4 disabled", ["connector"])
M_FAILURE = Gauge("connect_guardian_connector_failure", "1 for the class of the current failure", ["connector", "class"])
M_RESTARTS = Gauge("connect_guardian_consecutive_restarts", "Restarts since the connector last ran stably",
                   ["connector"])
M_LAST_RESET = Gauge("connect_guardian_last_reset_timestamp_seconds", "Time of the last offset reset", ["connector"])
M_ACTIONS = Counter("connect_guardian_actions", "Recovery actions taken", ["connector", "action", "reason"])
M_EVENT_ERRORS = Counter("connect_guardian_event_publish_errors", "Events that could not be written to Kafka")


# ---- Kafka Connect REST -------------------------------------------------------------------------------------------
class ConnectUnavailable(Exception):
    pass


def http(method: str, path: str, timeout: float = 15.0) -> tuple[int, object]:
    request = Request(f"{CONNECT_URL}{path}", method=method, headers={"Accept": "application/json"})
    try:
        with urlopen(request, timeout=timeout) as response:
            body = response.read().decode("utf-8", "replace")
            return response.status, json.loads(body) if body else None
    except HTTPError as exc:
        body = exc.read().decode("utf-8", "replace")
        try:
            return exc.code, json.loads(body)
        except json.JSONDecodeError:
            return exc.code, body
    except (URLError, socket.timeout, ConnectionError) as exc:
        raise ConnectUnavailable(str(exc)) from exc


def connector_path(name: str) -> str:
    return f"/connectors/{quote(name, safe='')}"


def source_writes() -> dict[tuple[str, str], float]:
    """Records written per (connector, task) from the Connect worker's Prometheus endpoint; {} if unavailable."""
    pattern = re.compile(r'^kafka_connect_source_task_source_record_write_total\{([^}]*)\}\s+(\S+)')
    try:
        with urlopen(CONNECT_METRICS_URL, timeout=10) as response:
            text = response.read().decode("utf-8", "replace")
    except (URLError, socket.timeout, ConnectionError):
        return {}
    writes = {}
    for line in text.splitlines():
        match = pattern.match(line)
        if match:
            labels = dict(re.findall(r'(\w+)="([^"]*)"', match.group(1)))
            writes[(labels.get("connector", ""), labels.get("task", ""))] = float(match.group(2))
    return writes


# ---- events -------------------------------------------------------------------------------------------------------
class Events:
    """Recovery events for dependent applications, one JSON message per action, keyed by connector name."""

    def __init__(self) -> None:
        self.producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP, "client.id": "connect-guardian",
                                  "acks": "all", "message.timeout.ms": 60000})
        self.topic_ready = False

    def _ensure_topic(self) -> None:
        if self.topic_ready:
            return
        admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
        if EVENTS_TOPIC not in admin.list_topics(timeout=10).topics:
            topic = NewTopic(EVENTS_TOPIC, num_partitions=1, replication_factor=EVENTS_REPLICATION_FACTOR,
                             config={"retention.ms": str(EVENTS_RETENTION_DAYS * 86400 * 1000)})
            for future in admin.create_topics([topic]).values():
                try:
                    future.result()
                    log.info("created events topic %s", EVENTS_TOPIC)
                except KafkaException as exc:
                    if "TOPIC_ALREADY_EXISTS" not in str(exc):
                        raise
        self.topic_ready = True

    def publish(self, connector: str, event_type: str, **fields: object) -> None:
        event = {"type": event_type, "connector": connector,
                 "time": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "dry_run": DRY_RUN, **fields}
        log.info("event %s", json.dumps(event, default=str))
        try:
            self._ensure_topic()
            self.producer.produce(EVENTS_TOPIC, key=connector.encode(), value=json.dumps(event, default=str).encode(),
                                  on_delivery=self._delivered)
            self.producer.poll(0)
        except (KafkaException, BufferError) as exc:
            M_EVENT_ERRORS.inc()
            log.warning("could not publish event to %s: %s", EVENTS_TOPIC, exc)

    @staticmethod
    def _delivered(err, _msg) -> None:
        if err is not None:
            M_EVENT_ERRORS.inc()
            log.warning("event delivery failed: %s", err)

    def flush(self) -> None:
        self.producer.flush(10)


# ---- per-connector state ------------------------------------------------------------------------------------------
@dataclass
class ConnectorState:
    restarts: int = 0                 # consecutive restarts without STABLE_SECONDS of RUNNING in between
    next_try: float = 0.0             # no restart before this time (backoff)
    running_since: float | None = None
    last_reset: float = 0.0
    reset_pending: bool = False       # stopped by the guardian for a reset; must be resumed even after a crash
    alerted: str = ""                 # failure class already announced, so an event is sent once per incident
    unassigned_since: dict[str, float] = field(default_factory=dict)
    writes: dict[str, list[float]] = field(default_factory=dict)  # task -> [records written, unchanged since]


@dataclass
class Policy:
    enabled: bool
    on_position_lost: str
    stall_seconds: float

    @classmethod
    def from_config(cls, config: dict[str, str]) -> Policy:
        heartbeat = float(config.get("heartbeat.interval.ms", "0") or 0)
        # Without a heartbeat a connector on quiet tables writes nothing for hours, which is not a stall.
        stall = float(config.get("guardian.stall.seconds", STALL_SECONDS)) if heartbeat > 0 else 0.0
        return cls(enabled=config.get("guardian.enabled", "true").strip().lower() != "false",
                   on_position_lost=config.get("guardian.on.position.lost", ON_POSITION_LOST).strip().lower(),
                   stall_seconds=stall)


class Guardian:
    def __init__(self, events: Events) -> None:
        self.events = events
        self.states: dict[str, ConnectorState] = self._load()

    # state survives a guardian restart, above all reset_pending: a connector the guardian stopped must not stay
    # stopped because the guardian died between "stop" and "resume"
    def _load(self) -> dict[str, ConnectorState]:
        try:
            raw = json.loads(STATE_FILE.read_text(encoding="utf-8"))
            return {name: ConnectorState(**value) for name, value in raw.items()}
        except FileNotFoundError:
            return {}
        except (json.JSONDecodeError, TypeError) as exc:
            log.warning("ignoring unreadable state file %s: %s", STATE_FILE, exc)
            return {}

    def _save(self) -> None:
        try:
            STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
            tmp = STATE_FILE.with_suffix(".tmp")
            tmp.write_text(json.dumps({n: asdict(s) for n, s in self.states.items()}), encoding="utf-8")
            tmp.replace(STATE_FILE)
        except OSError as exc:
            log.warning("could not save state to %s: %s", STATE_FILE, exc)

    def act(self, method: str, path: str) -> int:
        if DRY_RUN:
            log.info("dry run: %s %s", method, path)
            return 0
        code, body = http(method, path)
        if code >= 300:
            log.warning("%s %s returned HTTP %s: %s", method, path, code, body)
        return code

    # ---- main loop ------------------------------------------------------------------------------------------------
    def check(self) -> None:
        try:
            code, connectors = http("GET", "/connectors?expand=status&expand=info")
            if code != 200 or not isinstance(connectors, dict):
                raise ConnectUnavailable(f"HTTP {code}")
        except ConnectUnavailable as exc:
            M_CONNECT_UP.set(0)
            log.warning("Kafka Connect not reachable: %s", exc)
            return
        M_CONNECT_UP.set(1)
        writes = source_writes()

        for name, data in connectors.items():
            state = self.states.setdefault(name, ConnectorState())
            config = (data.get("info") or {}).get("config") or {}
            try:
                self.check_connector(name, data.get("status") or {}, config, state, writes)
            except ConnectUnavailable as exc:
                log.warning("Kafka Connect not reachable while handling %s: %s", name, exc)
                break

        for name in set(self.states) - set(connectors):   # deleted connectors
            del self.states[name]
            for gauge, labels in [(M_CONDITION, [name]), (M_RESTARTS, [name]), (M_LAST_RESET, [name])] + \
                    [(M_FAILURE, [name, c]) for c in (TRANSIENT, PERMANENT, POSITION_LOST)]:
                try:
                    gauge.remove(*labels)
                except KeyError:
                    pass
        self._save()
        M_LAST_LOOP.set_to_current_time()

    def check_connector(self, name: str, status: dict, config: dict[str, str], state: ConnectorState,
                        writes: dict[tuple[str, str], float]) -> None:
        policy = Policy.from_config(config)
        connector_state = (status.get("connector") or {}).get("state", "UNKNOWN")
        tasks = status.get("tasks") or []
        now = time.time()

        if state.reset_pending:
            if connector_state == "STOPPED":
                self.finish_reset(name, config, state, reason="resumed after guardian restart")
            else:
                state.reset_pending = False   # someone resumed or reconfigured it meanwhile
            return

        if not policy.enabled:
            self.set_condition(name, DISABLED)
            return
        if connector_state in ("PAUSED", "STOPPED"):
            self.set_condition(name, PARKED)
            state.running_since = None
            return

        failed = [t for t in tasks if t.get("state") == "FAILED"]
        if connector_state == "FAILED" or failed:
            trace = (status.get("connector") or {}).get("trace") if connector_state == "FAILED" else None
            trace = trace or next((t.get("trace") for t in failed if t.get("trace")), "") or ""
            self.handle_failure(name, trace, config, policy, state, now)
            return

        self.set_failure(name, None)
        self.restart_unassigned(name, tasks, state, now)
        self.restart_stalled(name, tasks, policy, state, writes, now)

        if connector_state == "RUNNING" and tasks and all(t.get("state") == "RUNNING" for t in tasks):
            state.running_since = state.running_since or now
            if state.restarts and now - state.running_since >= STABLE_SECONDS:
                self.events.publish(name, "recovered", restarts=state.restarts,
                                    running_seconds=int(now - state.running_since))
                state.restarts, state.next_try, state.alerted = 0, 0.0, ""
            self.set_condition(name, HEALTHY if not state.restarts else RECOVERING)

    # ---- failures -------------------------------------------------------------------------------------------------
    def handle_failure(self, name: str, trace: str, config: dict[str, str], policy: Policy,
                       state: ConnectorState, now: float) -> None:
        cls = classify(trace)
        state.running_since = None
        self.set_failure(name, cls)

        if cls == POSITION_LOST:
            if policy.on_position_lost == "reset" and now - state.last_reset >= RESET_COOLDOWN:
                self.reset(name, trace, config, state)
                return
            # policy alert, or a second loss within the cooldown (a reset did not help): leave it to a person
            self.set_condition(name, ATTENTION)
            if state.alerted != cls:
                why = "policy is alert" if policy.on_position_lost != "reset" else "already reset within the cooldown"
                self.events.publish(name, "position_lost", action="none", why=why, error=trace_head(trace))
                state.alerted = cls
            return

        self.set_condition(name, ATTENTION if cls == PERMANENT else RECOVERING)
        if cls == PERMANENT and state.alerted != cls:
            self.events.publish(name, "needs_attention", error=trace_head(trace))
            state.alerted = cls
        if now < state.next_try:
            return

        self.act("POST", f"{connector_path(name)}/restart?includeTasks=true&onlyFailed=true")
        state.restarts += 1
        delay = RETRY_MAX if cls == PERMANENT else min(RETRY_MIN * 2 ** (state.restarts - 1), RETRY_MAX)
        state.next_try = now + delay
        M_ACTIONS.labels(name, "restart", cls).inc()
        M_RESTARTS.labels(name).set(state.restarts)
        log.warning("%s failed (%s), restart %d, next try in %ds: %s", name, cls, state.restarts, delay,
                    trace_head(trace, 300))
        if state.restarts in (1, 5) or state.restarts % 20 == 0:   # tell applications, without flooding the topic
            self.events.publish(name, "restarted", failure_class=cls, restarts=state.restarts, error=trace_head(trace))

    def reset(self, name: str, trace: str, config: dict[str, str], state: ConnectorState) -> None:
        """Stop the connector, delete its stored offsets and resume it: it then starts as a new connector."""
        log.warning("%s: stored position no longer usable, resetting offsets: %s", name, trace_head(trace, 300))
        state.reset_pending = True
        self._save()
        self.act("PUT", f"{connector_path(name)}/stop")
        if not DRY_RUN and not self.wait_for(name, "STOPPED"):
            log.error("%s did not stop within %ds; will retry the reset", name, STOP_TIMEOUT)
            return   # reset_pending stays set: the next check finishes the reset once the connector is STOPPED
        self.finish_reset(name, config, state, reason=POSITION_LOST, error=trace_head(trace))

    def finish_reset(self, name: str, config: dict[str, str], state: ConnectorState, reason: str,
                     error: str = "") -> None:
        path = connector_path(name)
        # kept in the event: the position the connector had, i.e. where the application's gap starts
        code, offsets = http("GET", f"{path}/offsets")
        offsets_before = offsets.get("offsets") if code == 200 and isinstance(offsets, dict) else None
        for attempt in range(5):   # 409 while the stop is still propagating through the worker group
            code = self.act("DELETE", f"{path}/offsets")
            if code < 300:
                break
            time.sleep(5 * (attempt + 1))
        else:
            log.error("%s: offsets could not be deleted; connector left STOPPED, will retry", name)
            return
        self.act("PUT", f"{path}/resume")
        state.reset_pending = False
        state.last_reset = time.time()
        state.restarts, state.next_try, state.alerted, state.running_since = 0, 0.0, "", None
        M_ACTIONS.labels(name, "reset", reason).inc()
        M_LAST_RESET.labels(name).set(state.last_reset)
        self.set_condition(name, RECOVERING)
        self._save()
        mode = config.get("snapshot.mode", "initial")
        log.warning("%s: offsets deleted and connector resumed (snapshot.mode=%s)", name, mode)
        self.events.publish(name, "offsets_reset", reason=reason, error=error, offsets_before=offsets_before,
                            snapshot_mode=mode, effect=snapshot_effect(mode))

    def wait_for(self, name: str, wanted: str) -> bool:
        deadline = time.monotonic() + STOP_TIMEOUT
        while time.monotonic() < deadline:
            code, status = http("GET", f"{connector_path(name)}/status")
            if code == 200 and (status.get("connector") or {}).get("state") == wanted:
                return True
            time.sleep(2)
        return False

    # ---- running connectors ---------------------------------------------------------------------------------------
    def restart_unassigned(self, name: str, tasks: list[dict], state: ConnectorState, now: float) -> None:
        current = {str(t.get("id")) for t in tasks if t.get("state") == "UNASSIGNED"}
        state.unassigned_since = {k: v for k, v in state.unassigned_since.items() if k in current}
        for task_id in current:
            since = state.unassigned_since.setdefault(task_id, now)
            if now - since >= UNASSIGNED_SECONDS:
                self.act("POST", f"{connector_path(name)}/tasks/{task_id}/restart")
                M_ACTIONS.labels(name, "restart_unassigned", "unassigned").inc()
                self.events.publish(name, "restarted", failure_class="unassigned", task=int(task_id))
                state.unassigned_since[task_id] = now

    def restart_stalled(self, name: str, tasks: list[dict], policy: Policy, state: ConnectorState,
                        writes: dict[tuple[str, str], float], now: float) -> None:
        if policy.stall_seconds <= 0 or not writes:
            state.writes = {}
            return
        for task in tasks:
            task_id = str(task.get("id"))
            total = writes.get((name, task_id))
            if task.get("state") != "RUNNING" or total is None:
                state.writes.pop(task_id, None)
                continue
            last = state.writes.get(task_id)
            if last is None or total != last[0]:
                state.writes[task_id] = [total, now]
            elif now - last[1] >= policy.stall_seconds:
                # heartbeats alone should write every heartbeat.interval.ms: nothing at all means capture is stuck
                log.warning("%s task %s wrote nothing for %ds, restarting it", name, task_id, now - last[1])
                self.act("POST", f"{connector_path(name)}/tasks/{task_id}/restart")
                M_ACTIONS.labels(name, "restart_stalled", "stalled").inc()
                self.events.publish(name, "restarted", failure_class="stalled", task=int(task_id),
                                    idle_seconds=int(now - last[1]))
                state.writes[task_id] = [total, now]

    # ---- metrics helpers ------------------------------------------------------------------------------------------
    @staticmethod
    def set_condition(name: str, condition: int) -> None:
        M_CONDITION.labels(name).set(condition)

    @staticmethod
    def set_failure(name: str, cls: str | None) -> None:
        for c in (TRANSIENT, PERMANENT, POSITION_LOST):
            M_FAILURE.labels(name, c).set(1 if c == cls else 0)


def snapshot_effect(mode: str) -> str:
    mode = mode.strip().lower()
    if mode in ("initial", "when_needed", "always"):
        return "full snapshot of the captured tables, then streaming"
    if mode == "initial_only":
        return "full snapshot of the captured tables, no streaming"
    if mode in ("no_data", "schema_only", "never"):
        return "streaming from the current database position; changes made while the position was lost are not captured"
    return f"as configured by snapshot.mode={mode}"


def main() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"),
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    if ON_POSITION_LOST not in ("reset", "alert"):
        raise SystemExit(f"ON_POSITION_LOST must be reset or alert, not {ON_POSITION_LOST!r}")
    start_http_server(METRICS_PORT)
    events = Events()
    guardian = Guardian(events)
    log.info("started: connect=%s on_position_lost=%s stall=%ss dry_run=%s", CONNECT_URL, ON_POSITION_LOST,
             STALL_SECONDS, DRY_RUN)

    stopping = False

    def stop(*_args: object) -> None:
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    while not stopping:
        started = time.monotonic()
        try:
            guardian.check()
        except Exception:   # never let one bad response end the guardian
            log.exception("check failed")
        while not stopping and time.monotonic() - started < CHECK_INTERVAL:
            time.sleep(0.5)
    events.flush()
    log.info("stopped")


if __name__ == "__main__":
    main()
