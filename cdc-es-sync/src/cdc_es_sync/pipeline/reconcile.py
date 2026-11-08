"""Compares Oracle and Elasticsearch: document counts and the total of one amount field per index.

The Oracle side runs the index's own document query (restricted to the amount column), so the count is exactly the
number of documents the query produces. Logstash reports a normal finish even when a JDBC read fails midway; this is
the check that tells you a load is complete. Run it when no changes are in flight, or allow for a few documents.
"""
from __future__ import annotations

from decimal import Decimal
from typing import Any

from ..connectors.elastic import ElasticsearchWriter
from ..connectors.oracle import OracleReader
from ..core.registry import Registry


def reconcile(registry: Registry, reader: OracleReader, writer: ElasticsearchWriter,
              tolerance: float = 1.0) -> tuple[list[dict[str, Any]], bool]:
    """Returns (one report row per index, all_ok). An index with no `reconcile_sum_field` is compared by count only."""
    rows, all_ok = [], True
    for spec in registry.indices.values():
        sum_field = spec.reconcile_sum_field
        select = spec.select.project([sum_field] if sum_field else spec.key_names[:1])
        total = f"SUM({sum_field})" if sum_field else "0"
        with reader.cursor() as cur:
            cur.execute(f"SELECT COUNT(*), {total} FROM ({select})")
            ora_rows, ora_sum = cur.fetchone()
        client = writer.client(spec.target)
        client.indices.refresh(index=spec.es_index)
        body: dict[str, Any] = {"size": 0, "track_total_hits": True}
        if sum_field:
            body["aggs"] = {"total": {"sum": {"field": sum_field}}}
        result = client.search(index=spec.es_index, body=body)
        es_rows = int(result["hits"]["total"]["value"])
        es_sum = float(result.get("aggregations", {}).get("total", {}).get("value") or 0)
        ora_sum = Decimal(ora_sum or 0)
        ok = es_rows == int(ora_rows) and abs(Decimal(str(es_sum)) - ora_sum) < Decimal(str(tolerance))
        all_ok = all_ok and ok
        rows.append({"index": spec.es_index, "oracle_rows": int(ora_rows), "es_rows": es_rows,
                     "oracle_sum": ora_sum, "es_sum": es_sum, "sum_field": sum_field or "-",
                     "status": "OK" if ok else "MISMATCH"})
    return rows, all_ok


def format_report(rows: list[dict[str, Any]]) -> str:
    head = f"{'index':24}{'oracle rows':>13}{'es rows':>10}{'sum of':>14}{'oracle sum':>22}{'es sum':>22}  status"
    lines = [head, "-" * len(head)]
    for r in rows:
        lines.append(f"{r['index']:24}{r['oracle_rows']:>13}{r['es_rows']:>10}{r['sum_field']:>14}"
                     f"{r['oracle_sum']:>22.3f}{r['es_sum']:>22.3f}  {r['status']}")
    return "\n".join(lines)
