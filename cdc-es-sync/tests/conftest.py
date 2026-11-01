import json
from pathlib import Path

import pytest

from cdc_es_sync.config import load_config
from cdc_es_sync.mapping import Registry

ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(scope="session")
def cfg(monkeypatch_session=None):
    import os
    os.environ.setdefault("ORACLE_USER", "u")
    os.environ.setdefault("ORACLE_PASSWORD", "p")
    return load_config(ROOT / "config.yaml")


@pytest.fixture(scope="session")
def registry(cfg):
    return Registry.from_config(cfg)


@pytest.fixture(scope="session")
def table_columns():
    """Columns of every table the queries read (tests/fixtures/table_columns.json, from tests/dump_columns.py)."""
    raw = json.loads((ROOT / "tests" / "fixtures" / "table_columns.json").read_text(encoding="utf-8"))
    return {t: set(c) for t, c in raw.items()}
