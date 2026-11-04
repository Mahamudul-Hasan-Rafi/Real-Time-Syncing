"""Read-only parity test: documents rebuilt by this project vs the documents the Logstash load wrote.

    python tests/live/parity.py [sample size per index]
"""
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import elasticsearch as es

from cdc_es_sync.config import load_config
from cdc_es_sync.es_writer import make_client
from cdc_es_sync.mapping import Registry, norm
from cdc_es_sync.oracle_reader import OracleReader
from cdc_es_sync.transform import build_document

ROOT = Path(__file__).resolve().parents[2]
cfg = load_config(ROOT / "config.yaml")
reg = Registry.from_config(cfg)
tz = ZoneInfo(cfg["oracle"]["timezone"])
client = make_client(cfg["elasticsearch"]["targets"]["default"], request_timeout=120)
reader = OracleReader(cfg["oracle"])
N = int(sys.argv[1]) if len(sys.argv) > 1 else 200
IGNORE = {"etl_loaded_at", "etl_pipeline"}
TOLERATE = {"age"}          # computed from SYSDATE, so a birthday can pass between the two loads


def key_from_source(ix, src):
    out = []
    for col, typ in ix.key_cols:
        v = src[col]
        if typ == "date":
            dt = datetime.fromisoformat(v.replace("Z", "+00:00")).astimezone(tz)
            v = dt.replace(tzinfo=None)
        out.append(norm(v, typ))
    return tuple(out)


def same(a, b):
    if isinstance(a, float) or isinstance(b, float):
        try:
            return math.isclose(float(a), float(b), rel_tol=1e-9, abs_tol=1e-9)
        except (TypeError, ValueError):
            return False
    if isinstance(a, dict) and isinstance(b, dict):
        return a.keys() == b.keys() and all(same(a[k], b[k]) for k in a)
    if isinstance(a, list) and isinstance(b, list):
        return len(a) == len(b) and all(same(x, y) for x, y in zip(a, b))
    return a == b


total_bad = 0
for name, ix in reg.indices.items():
    client.indices.refresh(index=ix.es_index)       # refresh interval is 30 s; search must see the latest writes
    base_query = {"prefix": {"etl_pipeline": "cdc_"}} if "cdc" in sys.argv[2:] else {"match_all": {}}
    body = {"size": N, "query": {"function_score": {"query": base_query, "random_score": {}}}}
    hits = client.search(index=ix.es_index, body=body)["hits"]["hits"]
    keys = [key_from_source(ix, h["_source"]) for h in hits]
    by_key = {k: h for k, h in zip(keys, hits)}
    rows = {}
    for i in range(0, len(keys), 200):
        for row in reader.fetch_rows(ix, keys[i:i + 200]):
            rows[ix.key_from_row(row)] = row
    checked = diffs = missing = 0
    samples = []
    for key, hit in by_key.items():
        row = rows.get(key)
        if row is None:
            missing += 1
            samples.append((key, "no row in Oracle"))
            continue
        doc_id, doc = build_document(ix, row, tz, cfg["sync"]["nested_date_offset"])
        checked += 1
        problems = []
        if doc_id != hit["_id"]:
            problems.append(f"_id {doc_id!r} != {hit['_id']!r}")
        src = hit["_source"]
        for field in (set(doc) | set(src)) - IGNORE:
            if (field in TOLERATE and doc.get(field) is not None and src.get(field) is not None
                    and abs(doc[field] - src[field]) <= 1):
                continue
            if field not in src:
                problems.append(f"{field}: only in rebuilt ({str(doc[field])[:60]})")
            elif field not in doc:
                problems.append(f"{field}: only in Logstash doc ({str(src[field])[:60]})")
            elif not same(doc[field], src[field]):
                problems.append(f"{field}: rebuilt={str(doc[field])[:70]} logstash={str(src[field])[:70]}")
        if problems:
            diffs += 1
            if len(samples) < 6:
                samples.append((key, problems[:5]))
    total_bad += diffs + missing
    print(f"{name:22} sampled={len(hits):4} compared={checked:4} identical={checked - diffs:4} "
          f"different={diffs} not_in_oracle={missing}")
    for s in samples:
        print("    ", s)
reader.close()
sys.exit(1 if total_bad else 0)
