"""Debezium change event parsing."""
import json

import pytest

from cdc_es_sync.core.events import parse_event
from cdc_es_sync.errors import InvalidEventError


def event(op, before=None, after=None, table="NOMINEE_MST"):
    return json.dumps({"schema": {}, "payload": {"op": op, "before": before, "after": after,
                                                  "source": {"table": table, "scn": "99", "ts_ms": 5}}})


def test_parse_insert_update_delete():
    c = parse_event(event("c", after={"A": 1}), "t", 0, 7)
    assert (c.op, c.table, c.after, c.scn, c.offset) == ("c", "NOMINEE_MST", {"A": 1}, 99, 7)
    assert parse_event(event("u", {"A": 1}, {"A": 2}), "t", 0, 8).before == {"A": 1}
    assert parse_event(event("d", before={"A": 1}), "t", 0, 9).after is None


def test_parse_tombstone_and_schema_change_are_ignored():
    assert parse_event(None, "t", 0, 1) is None
    assert parse_event('{"payload": {"ddlType": "ALTER"}}', "t", 0, 1) is None


def test_parse_bad_events():
    for bad in ("not json", '{"payload": {"op": "x"}}', event("d"), event("c")):
        with pytest.raises(InvalidEventError):
            parse_event(bad, "t", 0, 1)
