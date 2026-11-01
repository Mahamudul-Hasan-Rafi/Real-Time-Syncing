"""Row to document, matching what the Logstash pipelines put in Elasticsearch."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo


class RejectError(Exception):
    """The row cannot become a valid document. It goes to the dead letter topic."""


def iso(dt: datetime, tz: ZoneInfo) -> str:
    """Oracle DATE (no zone, database wall clock) to the UTC string Logstash writes: 2020-11-11T18:00:00.000Z."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=tz)
    utc = dt.astimezone(timezone.utc)
    return f"{utc:%Y-%m-%dT%H:%M:%S}.{utc.microsecond // 1000:03d}Z"


def convert(value, tz: ZoneInfo):
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


def key_doc_id(ix, key: tuple, tz: ZoneInfo) -> str:
    """Document id for a key that no longer exists in Oracle, so its document can be deleted."""
    values = {col: convert(val, tz) for (col, _), val in zip(ix.key_cols, key)}
    return ix.doc_id.format(**values)


def convert_fields(ix, row: dict, tz: ZoneInfo, nested_offset: str) -> dict:
    """Column values to document values: dates to UTC text, numbers to int/float, JSON columns parsed."""
    # Empty columns stay as explicit nulls: that is how the Logstash load wrote them.
    doc = {k: convert(v, tz) for k, v in row.items()}
    for column in ix.json_columns:
        raw = doc.get(column)
        if raw in (None, ""):
            continue
        try:
            doc[column] = json.loads(raw) if isinstance(raw, str) else raw
        except ValueError as exc:
            raise RejectError(f"column {column} is not valid JSON: {exc}") from exc
    for column, date_fields in ix.nested_date_fields.items():
        rows = doc.get(column)
        if isinstance(rows, list):
            for item in rows:
                if isinstance(item, dict):
                    for name in date_fields:
                        if item.get(name):
                            item[name] = f"{item[name]}{nested_offset}"
    return doc


def _doc_id(ix, doc: dict) -> str:
    empty = [col for col, _ in ix.key_cols if doc.get(col) is None]
    if empty:
        raise RejectError(f"key column {empty} is empty")
    return ix.doc_id.format(**{col: doc[col] for col, _ in ix.key_cols})


def build_document(ix, row: dict, tz: ZoneInfo, nested_offset: str, loaded_at: str | None = None):
    """Returns (document id, document). Raises RejectError."""
    doc = convert_fields(ix, row, tz, nested_offset)
    loaded_at = loaded_at or now_iso()
    doc["etl_loaded_at"] = loaded_at
    doc["etl_pipeline"] = ix.etl_pipeline
    doc["@timestamp"] = doc.get(ix.timestamp_field) or loaded_at if ix.timestamp_field else loaded_at
    return _doc_id(ix, doc), doc


def build_partial(ix, row: dict, fields, tz: ZoneInfo, nested_offset: str):
    """Returns (document id, partial document holding only `fields`). Raises RejectError.

    `row` came from the projected query, so it holds the key columns and the wanted fields.
    """
    full = convert_fields(ix, row, tz, nested_offset)
    doc_id = _doc_id(ix, full)
    part = {f: full.get(f) for f in fields}
    if ix.timestamp_field and ix.timestamp_field in part:
        part["@timestamp"] = part[ix.timestamp_field] or now_iso()
    return doc_id, part
