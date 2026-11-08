"""Settings: typed, validated once, unknown keys rejected, ${VAR} expansion."""
from pathlib import Path

import pytest

from cdc_es_sync.errors import ConfigError
from cdc_es_sync.settings import ALL_OPERATIONS, load_settings

INDEX = """\
es_index: things
sql_file: things.sql
key_columns: [{col: id, type: int}]
key_filter: ["T.ID"]
doc_id: "{id}"
sources: {}
"""

CONFIG = """\
kafka:
  bootstrap_servers: "${TEST_KAFKA:localhost:9092}"
  group_id: g
  topic_pattern: '^x\\..*'
  dlq_topic: dlq
oracle:
  user: u
  password: "${TEST_ORACLE_PASSWORD}"
  dsn: d
elasticsearch:
  targets:
    default:
      hosts: ["https://es:9200"]
      verify_certs: "false"
observability:
  http_port: "9200"
  log_file: logs/x.log
indices:
  - indices/things.yaml
"""


def write_project(tmp_path: Path, config: str = CONFIG, index: str = INDEX) -> Path:
    (tmp_path / "indices").mkdir()
    (tmp_path / "indices" / "things.yaml").write_text(index, encoding="utf-8")
    (tmp_path / "indices" / "things.sql").write_text("SELECT t.id FROM s.t t WHERE 1 = 1", encoding="utf-8")
    path = tmp_path / "config.yaml"
    path.write_text(config, encoding="utf-8")
    return path


@pytest.fixture(autouse=True)
def password(monkeypatch):
    monkeypatch.setenv("TEST_ORACLE_PASSWORD", "secret")
    monkeypatch.delenv("TEST_KAFKA", raising=False)


def test_the_real_config_loads(settings):
    assert [d.name for d in settings.indices] == ["customer_data", "ac_balance_data", "transaction_history"]
    assert settings.oracle.timezone == "Asia/Dhaka" and settings.sync.operations == {"u", "d"}


def test_defaults_types_and_env_expansion(tmp_path):
    s = load_settings(write_project(tmp_path))
    assert s.kafka.bootstrap_servers == "localhost:9092"          # ${VAR:default}
    assert s.oracle.password == "secret" and "secret" not in repr(s.oracle)
    assert s.observability.http_port == 9200                      # "9200" from YAML/env becomes an int
    assert s.elasticsearch.targets["default"].verify_certs is False
    assert s.sync.operations == ALL_OPERATIONS and s.batch.max_events == 500
    assert s.observability.log_file == tmp_path.resolve() / "logs" / "x.log"
    assert s.indices[0].name == "things" and s.indices[0].spec["es_index"] == "things"


def test_environment_wins_over_the_default(tmp_path, monkeypatch):
    monkeypatch.setenv("TEST_KAFKA", "kafka:29092")
    assert load_settings(write_project(tmp_path)).kafka.bootstrap_servers == "kafka:29092"


def test_unset_variable_without_default_is_an_error(tmp_path, monkeypatch):
    monkeypatch.delenv("TEST_ORACLE_PASSWORD")
    with pytest.raises(ConfigError, match="TEST_ORACLE_PASSWORD is not set"):
        load_settings(write_project(tmp_path))


def test_a_mistyped_key_is_rejected(tmp_path):
    with pytest.raises(ConfigError, match=r"oracle: unknown setting\(s\) \['pool_mx'\]"):
        load_settings(write_project(tmp_path, CONFIG.replace("  dsn: d\n", "  dsn: d\n  pool_mx: 9\n")))


def test_a_required_key_is_reported(tmp_path):
    with pytest.raises(ConfigError, match="kafka.group_id is required"):
        load_settings(write_project(tmp_path, CONFIG.replace("  group_id: g\n", "")))


def test_a_bad_number_is_reported_with_its_path(tmp_path):
    with pytest.raises(ConfigError, match="observability.http_port must be a whole number"):
        load_settings(write_project(tmp_path, CONFIG.replace('"9200"', '"nine"')))


def test_a_bad_boolean_is_rejected(tmp_path):
    with pytest.raises(ConfigError, match="verify_certs must be true or false"):
        load_settings(write_project(tmp_path, CONFIG.replace('"false"', '"maybe"')))


def test_unknown_operation_is_rejected(tmp_path):
    with pytest.raises(ConfigError, match="sync.operations: unknown"):
        load_settings(write_project(tmp_path, CONFIG + "sync:\n  operations: [u, x]\n"))


def test_missing_index_file_is_reported(tmp_path):
    with pytest.raises(ConfigError, match="index file not found"):
        load_settings(write_project(tmp_path, CONFIG.replace("things.yaml", "other.yaml")))


def test_missing_config_file_is_reported(tmp_path):
    with pytest.raises(ConfigError, match="config file not found"):
        load_settings(tmp_path / "nope.yaml")
