"""Typed settings, loaded once at start-up and validated before anything connects.

`config.yaml` holds the runtime settings and lists the index files (`indices/<name>.yaml`); each index file holds one
index's identity and field map, next to its document query. In every YAML file `${VAR}` and `${VAR:default}` are read
from the environment, or from a `.env` file next to `config.yaml` (real environment variables win).

Unknown keys are an error, so a mistyped setting cannot be silently ignored.
"""
from __future__ import annotations

import os
import re
import types
from dataclasses import MISSING, dataclass, field, fields, replace
from pathlib import Path
from typing import Any, Union, get_args, get_origin, get_type_hints

import yaml

from .errors import ConfigError

ALL_OPERATIONS = frozenset({"c", "u", "d", "r"})        # Debezium: create, update, delete, snapshot read

_ENV = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::([^}]*))?\}")
_TRUE = {"true", "1", "yes", "on"}
_FALSE = {"false", "0", "no", "off"}


# -- sections ---------------------------------------------------------------------------------------------------
@dataclass(frozen=True, kw_only=True)
class KafkaSettings:
    bootstrap_servers: str
    group_id: str
    topic_pattern: str
    dlq_topic: str
    auto_offset_reset: str = "earliest"
    session_timeout_ms: int = 45_000
    max_poll_interval_ms: int = 1_800_000


@dataclass(frozen=True, kw_only=True)
class BatchSettings:
    max_events: int = 500
    max_wait_seconds: float = 1.0


@dataclass(frozen=True, kw_only=True)
class OracleSettings:
    user: str
    password: str = field(repr=False)
    dsn: str
    schema: str = "EMOB"
    pool_min: int = 1
    pool_max: int = 8
    fetch_array_size: int = 1000
    call_timeout_ms: int = 300_000
    timezone: str = "UTC"                   # zone of Oracle DATE values (they carry none)


@dataclass(frozen=True, kw_only=True)
class ElasticsearchTarget:
    hosts: tuple[str, ...]
    username: str | None = None
    password: str = field(default="", repr=False)
    verify_certs: bool = True
    ca_certs: str | None = None
    request_timeout: float = 120.0


@dataclass(frozen=True, kw_only=True)
class ElasticsearchSettings:
    targets: dict[str, ElasticsearchTarget]


@dataclass(frozen=True, kw_only=True)
class SyncSettings:
    chunk_size: int = 500
    bulk_max_actions: int = 1000
    max_fanout: int = 50_000
    throttle_ms: float = 0                  # optional pause between chunks above max_fanout
    workers: int = 4                        # chunks refreshed in parallel (each: Oracle read, transform, bulk write)
    retry_max_backoff_seconds: float = 30.0
    nested_date_offset: str = "+06:00"
    operations: frozenset[str] = ALL_OPERATIONS

    def __post_init__(self) -> None:
        if self.workers < 1:
            raise ConfigError("sync.workers: must be at least 1 (1 processes chunks one after another)")
        unknown = self.operations - ALL_OPERATIONS
        if unknown:
            raise ConfigError(f"sync.operations: unknown {sorted(unknown)}; use any of {sorted(ALL_OPERATIONS)}")
        if not self.operations:
            raise ConfigError("sync.operations: name at least one change type, or leave the setting out for all")


@dataclass(frozen=True, kw_only=True)
class ObservabilitySettings:
    http_port: int = 9108
    health_stale_seconds: float = 120.0
    log_level: str = "INFO"
    log_file: Path | None = None            # relative paths are relative to config.yaml
    dlq_file_dir: Path = Path("dlq")


@dataclass(frozen=True, kw_only=True)
class StartupCheckSettings:
    fail_on_missing_logging: bool = True


@dataclass(frozen=True)
class IndexDefinition:
    """One index file, as written. `core.registry` checks it and compiles it into an `IndexSpec`."""

    name: str                   # the file name without .yaml
    path: Path
    spec: dict[str, Any]


@dataclass(frozen=True, kw_only=True)
class Settings:
    kafka: KafkaSettings
    batch: BatchSettings
    oracle: OracleSettings
    elasticsearch: ElasticsearchSettings
    sync: SyncSettings
    observability: ObservabilitySettings
    startup_checks: StartupCheckSettings
    indices: tuple[IndexDefinition, ...]
    base_dir: Path

    def __post_init__(self) -> None:
        # The lookup query keeps one connection open while the workers use the others.
        if self.oracle.pool_max < self.sync.workers + 1:
            raise ConfigError(f"oracle.pool_max ({self.oracle.pool_max}) must be at least sync.workers + 1 "
                              f"({self.sync.workers + 1}), or the workers wait for connections")


_SECTIONS = {
    "kafka": (KafkaSettings, True),
    "batch": (BatchSettings, False),
    "oracle": (OracleSettings, True),
    "sync": (SyncSettings, False),
    "observability": (ObservabilitySettings, False),
    "startup_checks": (StartupCheckSettings, False),
}


# -- loading ----------------------------------------------------------------------------------------------------
def load_settings(path: str | Path) -> Settings:
    """Reads config.yaml and every index file it lists. Raises ConfigError."""
    path = Path(path).resolve()
    if not path.is_file():
        raise ConfigError(f"config file not found: {path}")
    load_dotenv(path.parent / ".env")
    raw = _read_yaml(path)
    base = path.parent

    known = set(_SECTIONS) | {"elasticsearch", "indices"}
    unknown = sorted(set(raw) - known)
    if unknown:
        raise ConfigError(f"unknown config section(s) {unknown}")
    for name, (_, required) in _SECTIONS.items():
        if required and name not in raw:
            raise ConfigError(f"config section missing: {name}")
    for name in ("elasticsearch", "indices"):
        if name not in raw:
            raise ConfigError(f"config section missing: {name}")

    sections = {name: _section(cls, raw.get(name), name) for name, (cls, _) in _SECTIONS.items()}
    obs: ObservabilitySettings = sections["observability"]
    sections["observability"] = replace(
        obs,
        log_file=base / obs.log_file if obs.log_file else None,
        dlq_file_dir=base / obs.dlq_file_dir,
    )
    return Settings(
        **sections,
        elasticsearch=_elasticsearch(raw["elasticsearch"]),
        indices=_index_definitions(raw["indices"], base),
        base_dir=base,
    )


def load_dotenv(path: Path) -> None:
    """KEY=VALUE lines into os.environ, without overriding variables that are already set."""
    if not path.is_file():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def expand_env(node: Any) -> Any:
    """Replaces ${VAR} and ${VAR:default} in every string of a parsed YAML tree."""
    if isinstance(node, str):
        def sub(match: re.Match) -> str:
            value = os.environ.get(match.group(1), match.group(2))
            if value is None:
                raise ConfigError(f"environment variable {match.group(1)} is not set")
            return value
        return _ENV.sub(sub, node)
    if isinstance(node, dict):
        return {k: expand_env(v) for k, v in node.items()}
    if isinstance(node, list):
        return [expand_env(v) for v in node]
    return node


def _read_yaml(path: Path) -> dict[str, Any]:
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise ConfigError(f"{path.name}: not valid YAML: {exc}") from exc
    if not isinstance(data, dict):
        raise ConfigError(f"{path.name}: expected a mapping at the top level")
    return expand_env(data)


def _elasticsearch(raw: Any) -> ElasticsearchSettings:
    if not isinstance(raw, dict) or not isinstance(raw.get("targets"), dict) or not raw["targets"]:
        raise ConfigError("elasticsearch.targets must map at least one target name to its settings")
    unknown = sorted(set(raw) - {"targets"})
    if unknown:
        raise ConfigError(f"elasticsearch: unknown setting(s) {unknown}")
    targets = {name: _section(ElasticsearchTarget, t, f"elasticsearch.targets.{name}")
               for name, t in raw["targets"].items()}
    return ElasticsearchSettings(targets=targets)


def _index_definitions(raw: Any, base: Path) -> tuple[IndexDefinition, ...]:
    if not isinstance(raw, list) or not raw:
        raise ConfigError("indices must list at least one index file (e.g. indices/customer_data.yaml)")
    out: list[IndexDefinition] = []
    for entry in raw:
        path = (base / str(entry)).resolve()
        if not path.is_file():
            raise ConfigError(f"index file not found: {path}")
        if any(d.name == path.stem for d in out):
            raise ConfigError(f"index {path.stem} is listed twice")
        out.append(IndexDefinition(name=path.stem, path=path, spec=_read_yaml(path)))
    return tuple(out)


# -- typed sections from YAML mappings ------------------------------------------------------------------------
def _section(cls: type, raw: Any, where: str) -> Any:
    """Builds one settings dataclass. A key set to nothing (`key:`) keeps its default."""
    raw = {} if raw is None else raw
    if not isinstance(raw, dict):
        raise ConfigError(f"{where} must be a mapping")
    hints = get_type_hints(cls)
    declared = {f.name: f for f in fields(cls)}
    unknown = sorted(set(raw) - set(declared))
    if unknown:
        raise ConfigError(f"{where}: unknown setting(s) {unknown}")
    values = {}
    for name, f in declared.items():
        has_default = f.default is not MISSING or f.default_factory is not MISSING
        value = raw.get(name)
        if value is None and has_default:
            continue
        if name not in raw:
            raise ConfigError(f"{where}.{name} is required")
        values[name] = _coerce(value, hints[name], f"{where}.{name}")
    return cls(**values)


def _coerce(value: Any, hint: Any, where: str) -> Any:
    origin = get_origin(hint)
    if origin in (Union, types.UnionType):
        options = [a for a in get_args(hint) if a is not type(None)]
        return None if value is None else _coerce(value, options[0], where)
    if value is None:
        raise ConfigError(f"{where} must have a value")
    if hint is bool:
        if isinstance(value, bool):
            return value
        text = str(value).strip().lower()
        if text in _TRUE | _FALSE:
            return text in _TRUE
        raise ConfigError(f"{where} must be true or false, not {value!r}")
    if hint in (int, float):
        if isinstance(value, bool):
            raise ConfigError(f"{where} must be a number, not {value!r}")
        try:
            return hint(str(value).strip())
        except ValueError:
            raise ConfigError(f"{where} must be {'a whole number' if hint is int else 'a number'}, "
                              f"not {value!r}") from None
    if hint is str:
        return str(value)
    if hint is Path:
        return Path(str(value))
    if origin in (tuple, frozenset):
        items = [value] if isinstance(value, str) else value
        if not isinstance(items, list):
            raise ConfigError(f"{where} must be a list")
        item_type = get_args(hint)[0]
        return origin(_coerce(v, item_type, f"{where}[{i}]") for i, v in enumerate(items))
    raise TypeError(f"{where}: settings type {hint!r} is not supported")
