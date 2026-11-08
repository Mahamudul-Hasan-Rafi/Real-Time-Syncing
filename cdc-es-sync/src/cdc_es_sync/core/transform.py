"""Row to document, matching what the Logstash pipelines put in Elasticsearch."""
from __future__ import annotations

import json
from collections.abc import Iterable
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

from ..errors import DocumentRejectedError
from .model import IndexSpec, Key


def iso(dt: datetime, tz: ZoneInfo) -> str:
    """Oracle DATE (no zone, database wall clock) to the UTC string Logstash writes: 2020-11-11T18:00:00.000Z."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=tz)
    utc = dt.astimezone(timezone.utc)
    return f"{utc:%Y-%m-%dT%H:%M:%S}.{utc.microsecond // 1000:03d}Z"


def convert(value: Any, tz: ZoneInfo) -> Any:
    if isinstance(value, datetime):
        return iso(value, tz)
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).hex()
    return value


def now_iso() -> str:
    n = datetime.now(timezone.utc)
    return f"{n:%Y-%m-%dT%H:%M:%S}.{n.microsecond:06d}Z"


def key_doc_id(spec: IndexSpec, key: Key, tz: ZoneInfo) -> str:
    """Document id for a key that no longer exists in Oracle, so its document can be deleted."""
    values = {col: convert(val, tz) for (col, _), val in zip(spec.key_columns, key)}
    return spec.doc_id.format(**values)


def convert_fields(spec: IndexSpec, row: dict[str, Any], tz: ZoneInfo, nested_offset: str) -> dict[str, Any]:
    """Column values to document values: dates to UTC text, numbers to int/float, JSON columns parsed."""
    # Empty columns stay as explicit nulls: that is how the Logstash load wrote them.
    doc = {k: convert(v, tz) for k, v in row.items()}
    for column in spec.json_columns:
        raw = doc.get(column)
        if raw in (None, ""):
            continue
        try:
            doc[column] = json.loads(raw) if isinstance(raw, str) else raw
        except ValueError as exc:
            raise DocumentRejectedError(f"column {column} is not valid JSON: {exc}") from exc
    for column, date_fields in spec.nested_date_fields.items():
        rows = doc.get(column)
        if isinstance(rows, list):
            for item in rows:
                if isinstance(item, dict):
                    for name in date_fields:
                        if item.get(name):
                            item[name] = f"{item[name]}{nested_offset}"
    return doc


def _doc_id(spec: IndexSpec, doc: dict[str, Any]) -> str:
    empty = [col for col in spec.key_names if doc.get(col) is None]
    if empty:
        raise DocumentRejectedError(f"key column {empty} is empty")
    return spec.doc_id.format(**{col: doc[col] for col in spec.key_names})


def build_document(spec: IndexSpec, row: dict[str, Any], tz: ZoneInfo, nested_offset: str,
                   loaded_at: str | None = None) -> tuple[str, dict[str, Any]]:
    """Returns (document id, document). Raises DocumentRejectedError."""
    doc = convert_fields(spec, row, tz, nested_offset)
    loaded_at = loaded_at or now_iso()
    doc["etl_loaded_at"] = loaded_at
    doc["etl_pipeline"] = spec.etl_pipeline
    business_time = doc.get(spec.timestamp_field) if spec.timestamp_field else None
    doc["@timestamp"] = business_time or loaded_at
    return _doc_id(spec, doc), doc


def build_partial(spec: IndexSpec, row: dict[str, Any], fields: Iterable[str], tz: ZoneInfo,
                  nested_offset: str) -> tuple[str, dict[str, Any]]:
    """Returns (document id, partial document holding only `fields`). Raises DocumentRejectedError.

    `row` came from the projected query, so it holds the key columns and the wanted fields.
    """
    full = convert_fields(spec, row, tz, nested_offset)
    doc_id = _doc_id(spec, full)
    part = {f: full.get(f) for f in fields}
    if spec.timestamp_field and spec.timestamp_field in part:
        part["@timestamp"] = part[spec.timestamp_field] or now_iso()
    return doc_id, part
