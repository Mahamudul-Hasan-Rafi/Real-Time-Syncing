"""Shared fixtures. Unit tests are offline: they load the real config.yaml and index files, never connect."""
import json
import os
from pathlib import Path

import pytest

from cdc_es_sync.core.registry import Registry
from cdc_es_sync.settings import Settings, load_settings

ROOT = Path(__file__).resolve().parent.parent

# Required by config.yaml; set before .env is read, so the tests never depend on real credentials.
for name in ("ORACLE_USER", "ORACLE_PASSWORD", "ES_PASSWORD"):
    os.environ.setdefault(name, "test")


@pytest.fixture(scope="session")
def settings() -> Settings:
    return load_settings(ROOT / "config.yaml")


@pytest.fixture(scope="session")
def registry(settings):
    return Registry.from_settings(settings)


@pytest.fixture(scope="session")
def table_columns():
    """Columns of every table the queries read (tests/fixtures/table_columns.json, from scripts/dump_columns.py)."""
    raw = json.loads((ROOT / "tests" / "fixtures" / "table_columns.json").read_text(encoding="utf-8"))
    return {t: set(c) for t, c in raw.items()}
