"""Application-owned Debezium connector deployment and failed-task recovery."""
from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

log = logging.getLogger(__name__)


def _resolve_template(template: str) -> tuple[str, dict[str, object]]:
    path = Path(template)
    if not path.is_absolute():
        path = Path.cwd() / path
    if not path.is_file():
        raise RuntimeError(f"connector template not found: {path}")

    document = json.loads(path.read_text(encoding="utf-8"))
    name = os.environ.get("DEBEZIUM_CONNECTOR_NAME", document["name"])
    config = document["config"].copy()

    for key, value in list(config.items()):
        if not isinstance(value, str):
            continue
        if value.startswith("${") and value.endswith("}"):
            env_name = value[2:-1]
            resolved = os.environ.get(env_name)
            if not resolved:
                raise RuntimeError(f"required environment variable is not set: {env_name}")
            config[key] = resolved

    return name, config


def _request(
    method: str,
    url: str,
    body: dict[str, object] | None = None,
    timeout: float = 10.0,
) -> tuple[int, str]:
    data = None
    headers: dict[str, str] = {}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = Request(url, data=data, headers=headers, method=method)
    try:
        with urlopen(request, timeout=timeout) as response:
            return response.status, response.read().decode("utf-8", "replace")
    except HTTPError as exc:
        return exc.code, exc.read().decode("utf-8", "replace")
    except URLError as exc:
        raise RuntimeError(f"cannot reach Kafka Connect: {exc.reason}") from exc


def deploy_connector(template: str, connect_url: str, timeout: float) -> None:
    name, config = _resolve_template(template)
    connect_url = connect_url.rstrip("/")

    deadline = time.monotonic() + timeout
    while True:
        try:
            code, _ = _request("GET", f"{connect_url}/", timeout=min(10.0, timeout))
            if code < 300:
                break
        except RuntimeError:
            pass
        if time.monotonic() >= deadline:
            raise RuntimeError("Kafka Connect did not become reachable")
        time.sleep(2)

    code, response = _request(
        "PUT",
        f"{connect_url}/connectors/{name}/config",
        config,
        timeout=timeout,
    )
    if code not in (200, 201):
        raise RuntimeError(f"Kafka Connect returned HTTP {code}: {response}")

    deadline = time.monotonic() + timeout
    while True:
        code, response = _request("GET", f"{connect_url}/connectors/{name}/status", timeout=10)
        if code == 200:
            status = json.loads(response)
            connector_state = status.get("connector", {}).get("state")
            tasks = [task.get("state") for task in status.get("tasks", [])]
            if connector_state == "RUNNING" and tasks and all(task == "RUNNING" for task in tasks):
                log.info("connector deployed and running: %s", name)
                return
        if time.monotonic() >= deadline:
            raise RuntimeError(f"connector did not reach RUNNING state: {response}")
        time.sleep(3)


def run_watchdog(connect_url: str, name: str) -> int:
    """Restart failed tasks without resetting offsets or changing connector configuration."""
    max_attempts = int(os.environ.get("DEBEZIUM_MAX_ATTEMPTS", "10"))
    check_interval = float(os.environ.get("DEBEZIUM_CHECK_INTERVAL_SECONDS", "30"))
    retry_step = float(os.environ.get("DEBEZIUM_RETRY_STEP_SECONDS", "30"))
    retry_max = float(os.environ.get("DEBEZIUM_RETRY_MAX_SECONDS", "300"))
    stable_seconds = float(os.environ.get("DEBEZIUM_STABLE_SECONDS", "600"))
    attempts = 0
    running_since: float | None = None

    log.info("connector watchdog started: connector=%s url=%s", name, connect_url)
    while True:
        try:
            code, response = _request("GET", f"{connect_url.rstrip('/')}/connectors/{name}/status")
            if code != 200:
                running_since = None
            else:
                status = json.loads(response)
                failed_tasks = [task for task in status.get("tasks", []) if task.get("state") == "FAILED"]
                connector_state = status.get("connector", {}).get("state")
                if connector_state == "RUNNING" and not failed_tasks:
                    running_since = running_since or time.monotonic()
                    if attempts and time.monotonic() - running_since >= stable_seconds:
                        log.info("connector recovered; restart counter reset: %s", name)
                        attempts = 0
                elif failed_tasks and attempts < max_attempts:
                    attempts += 1
                    running_since = None
                    code, _ = _request(
                        "POST",
                        f"{connect_url.rstrip('/')}/connectors/{name}/restart"
                        "?includeTasks=true&onlyFailed=true",
                    )
                    log.warning("connector task restart %d/%d (HTTP %d)", attempts, max_attempts, code)
                    time.sleep(min(attempts * retry_step, retry_max))
                    continue
                else:
                    running_since = None
        except (RuntimeError, json.JSONDecodeError) as exc:
            log.warning("connector status check failed: %s", exc)
        time.sleep(check_interval)
