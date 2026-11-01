"""Writes tests/fixtures/table_columns.json: the columns of every table the document queries read.

    .venv\\Scripts\\python.exe tests\\dump_columns.py

The audit of the field map needs these to resolve unqualified column names in the SQL. Re-run it after a
schema change; the startup check does the same against the live database.
"""
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from cdc_es_sync.config import load_config            # noqa: E402
from cdc_es_sync.oracle_reader import OracleReader     # noqa: E402

cfg = load_config(ROOT / "config.yaml")
tables = set()
for c in cfg["indices"].values():
    sql = (ROOT / c["sql_file"]).read_text(encoding="utf-8")
    tables |= {m.upper() for m in re.findall(r"(?i)\bemob\.(\w+)", sql)}
reader = OracleReader(cfg["oracle"])
found = reader.table_columns(cfg["oracle"].get("schema", "EMOB"), sorted(tables))
reader.close()
out = ROOT / "tests" / "fixtures" / "table_columns.json"
out.parent.mkdir(parents=True, exist_ok=True)
out.write_text(json.dumps({t: sorted(c) for t, c in sorted(found.items())}, indent=1), encoding="utf-8")
print(f"{len(found)} tables written to {out}")
