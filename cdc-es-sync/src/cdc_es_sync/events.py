"""Debezium change event parsing."""
from __future__ import annotations

import json
from dataclasses import dataclass


class BadEvent(Exception):
    pass


@dataclass(frozen=True)
class Change:
    table: str
    op: str                  # c create, u update, d delete, r snapshot read
    before: dict | None
    after: dict | None
    scn: int | None
    ts_ms: int | None
    topic: str
    partition: int
    offset: int


def parse_event(value: bytes | str | None, topic: str, partition: int, offset: int) -> Change | None:
    """Returns None for tombstones and for events that carry no row change (schema changes, heartbeats)."""
    if value is None:
        return None
    try:
        message = json.loads(value)
    except (ValueError, UnicodeDecodeError) as exc:
        raise BadEvent(f"not JSON: {exc}") from exc
    if not isinstance(message, dict):
        raise BadEvent("event is not a JSON object")
    payload = message.get("payload", message)
    if not isinstance(payload, dict) or "op" not in payload:
        return None
    op = payload["op"]
    if op not in ("c", "u", "d", "r"):
        raise BadEvent(f"unsupported operation {op!r}")
    source = payload.get("source") or {}
    table = (source.get("table") or topic.rsplit(".", 1)[-1] or "").upper()
    if not table:
        raise BadEvent("event has no table name")
    before, after = payload.get("before"), payload.get("after")
    if op == "d" and not isinstance(before, dict):
        raise BadEvent("delete event without a before image")
    if op != "d" and not isinstance(after, dict):
        raise BadEvent(f"{op} event without an after image")
    scn = source.get("scn")
    return Change(
        table=table, op=op,
        before=before if isinstance(before, dict) else None,
        after=after if isinstance(after, dict) else None,
        scn=int(scn) if scn not in (None, "") else None,
        ts_ms=source.get("ts_ms"),
        topic=topic, partition=partition, offset=offset,
    )
