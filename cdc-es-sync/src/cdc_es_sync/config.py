"""YAML config with ${VAR} and ${VAR:default} expansion and an optional .env file next to it."""
from __future__ import annotations

import os
import re
from pathlib import Path

import yaml


class ConfigError(Exception):
    pass


_ENV = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::([^}]*))?\}")


def load_dotenv(path: Path) -> None:
    if not path.is_file():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def _expand(node):
    if isinstance(node, str):
        def sub(match):
            value = os.environ.get(match.group(1), match.group(2))
            if value is None:
                raise ConfigError(f"environment variable {match.group(1)} is not set")
            return value
        return _ENV.sub(sub, node)
    if isinstance(node, dict):
        return {k: _expand(v) for k, v in node.items()}
    if isinstance(node, list):
        return [_expand(v) for v in node]
    return node


def load_config(path: str | Path) -> dict:
    path = Path(path).resolve()
    if not path.is_file():
        raise ConfigError(f"config file not found: {path}")
    load_dotenv(path.parent / ".env")
    cfg = _expand(yaml.safe_load(path.read_text(encoding="utf-8")))
    cfg["_base"] = path.parent
    for section in ("kafka", "oracle", "elasticsearch", "sync", "indices"):
        if section not in cfg:
            raise ConfigError(f"config section missing: {section}")
    return cfg
