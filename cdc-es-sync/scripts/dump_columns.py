"""Writes tests/fixtures/table_columns.json: the columns of every table the document queries read.

    .venv\\Scripts\\python scripts\\dump_columns.py

The offline audit of the field map (tests/unit/test_audit.py) needs these to resolve unqualified column names in the
SQL. Re-run it after a schema change; the `check` command does the same against the live database.
"""
import json
from pathlib import Path

from cdc_es_sync.connectors.oracle import OracleReader
from cdc_es_sync.core.registry import Registry
from cdc_es_sync.settings import load_settings

ROOT = Path(__file__).resolve().parents[1]

settings = load_settings(ROOT / "config.yaml")
registry = Registry.from_settings(settings)
tables = {t for name in registry.indices for t in registry.tables_in_sql(name)}
reader = OracleReader(settings.oracle)
try:
    found = reader.table_columns(settings.oracle.schema, sorted(tables))
finally:
    reader.close()
out = ROOT / "tests" / "fixtures" / "table_columns.json"
out.parent.mkdir(parents=True, exist_ok=True)
out.write_text(json.dumps({t: sorted(c) for t, c in sorted(found.items())}, indent=1), encoding="utf-8")
print(f"{len(found)} tables written to {out}")
