"""Health, start-up check fatality and the command line, without any network."""
import json
import logging
from dataclasses import replace

from cdc_es_sync.cli import main
from cdc_es_sync.observability.logs import JsonFormatter
from cdc_es_sync.observability.metrics import Metrics
from cdc_es_sync.pipeline.preflight import SUPPLEMENTAL_LOGGING, CheckResult, Preflight


def test_health_is_ok_while_the_heartbeat_is_fresh():
    m = Metrics()
    m.beat("running")
    assert m.health(stale_seconds=60)[0] is True
    assert m.health(stale_seconds=0)[0] is False


def test_failed_state_is_unhealthy():
    m = Metrics()
    m.beat("failed")
    ok, detail = m.health(stale_seconds=60)
    assert not ok and detail["state"] == "failed"


def test_json_log_line_carries_the_context():
    record = logging.LogRecord("x", logging.ERROR, __file__, 1, "dead letter: %s", ("parse",), None)
    record.ctx = {"detail": {"offset": 3}}
    line = json.loads(JsonFormatter().format(record))
    assert line["msg"] == "dead letter: parse" and line["detail"] == {"offset": 3}


def test_missing_log_groups_can_be_made_non_fatal(settings, registry):
    failed = CheckResult(SUPPLEMENTAL_LOGGING, False, "gap")
    strict = Preflight(settings, registry, None, None)
    lenient = Preflight(replace(settings, startup_checks=replace(settings.startup_checks,
                                                                  fail_on_missing_logging=False)), registry, None, None)
    assert strict.is_fatal(failed) and not lenient.is_fatal(failed)
    assert lenient.is_fatal(CheckResult("oracle reachable", False, "down"))


def test_config_error_exits_with_2(tmp_path, capsys):
    assert main(["--config", str(tmp_path / "missing.yaml"), "check"]) == 2
    assert "config file not found" in capsys.readouterr().err
