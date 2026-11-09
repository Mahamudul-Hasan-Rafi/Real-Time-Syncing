"""Engine behaviour with fake Oracle and Elasticsearch."""
import threading
import time
from dataclasses import replace
from datetime import datetime

import pytest

from cdc_es_sync.core.events import Change
from cdc_es_sync.errors import ConfigError, ShutdownRequested
from cdc_es_sync.lifecycle import StopSignal
from cdc_es_sync.observability.metrics import Metrics
from cdc_es_sync.pipeline.engine import SyncEngine
from cdc_es_sync.settings import ALL_OPERATIONS, SyncSettings


class FakeReader:
    def __init__(self):
        self.rows = {}              # index -> {key tuple: full row}
        self.lookups = {}           # (text in the lookup SQL, params tuple) -> key tuples
        self.fail_next = []         # exceptions raised by the next fetch calls
        self.fetches = []           # (index, keys) of whole-document reads
        self.field_fetches = []     # (index, keys, fields) of partial reads

    def _maybe_fail(self):
        if self.fail_next:
            raise self.fail_next.pop(0)

    def fetch_rows(self, ix, keys):
        self.fetches.append((ix.name, list(keys)))
        self._maybe_fail()
        return [self.rows[ix.name][k] for k in keys if k in self.rows.get(ix.name, {})]

    def fetch_fields(self, ix, keys, fields):
        self.field_fetches.append((ix.name, list(keys), list(fields)))
        self._maybe_fail()
        wanted = set(fields) | set(ix.key_names)
        return [{c: v for c, v in self.rows[ix.name][k].items() if c in wanted}
                for k in keys if k in self.rows.get(ix.name, {})]

    def lookup_chunks(self, sql, params, chunk):
        ptuple = tuple(sorted(params.items()))
        keys = []
        for (needle, p), found in self.lookups.items():
            if needle in sql and p == ptuple:
                keys = found
                break
        for i in range(0, len(keys), chunk):
            yield keys[i:i + chunk]

    def stream_all(self, ix, chunk):
        rows = list(self.rows.get(ix.name, {}).values())
        for i in range(0, len(rows), chunk):
            yield rows[i:i + chunk]


class FakeWriter:
    def __init__(self):
        self.ops = []
        self.refuse = set()
        self.not_in_es = set()      # doc ids a partial update cannot find

    def apply(self, target, ops):
        self.ops.extend(ops)
        failed = [(o, "mapper_parsing_exception") for o in ops if o.doc_id in self.refuse]
        failed += [(o, "document_missing_exception") for o in ops if o.action == "update" and o.doc_id in self.not_in_es]
        return failed


class FakeDlq:
    def __init__(self):
        self.items = []

    def send(self, stage, reason, **detail):
        self.items.append((stage, reason, detail))


def change(table, op, before=None, after=None, offset=0):
    return Change(table, op, before, after, 1, 1, f"emob.EMOB.{table}", 0, offset)


def make_engine(registry, sync, reader=None, writer=None, dlq=None, stop=None, metrics=None):
    return SyncEngine(registry, reader or FakeReader(), writer or FakeWriter(), dlq or FakeDlq(),
                      sync=sync, timezone="Asia/Dhaka", stop=stop or StopSignal(), metrics=metrics or Metrics())


def fast(sync: SyncSettings, **changes) -> SyncSettings:
    """The configured sync settings with retries and throttling shortened for tests."""
    return replace(sync, retry_max_backoff_seconds=0.01, throttle_ms=1, **changes)


@pytest.fixture
def env(registry, settings):
    reader, writer, dlq, stop = FakeReader(), FakeWriter(), FakeDlq(), StopSignal()
    # These tests cover every change type; config.yaml itself applies only updates and deletes (see the tests at the end).
    engine = make_engine(registry, fast(settings.sync, operations=ALL_OPERATIONS), reader, writer, dlq, stop)
    return engine, reader, writer, dlq, stop


@pytest.fixture
def configured(registry, settings):
    """An engine with the operations exactly as config.yaml sets them."""
    reader, writer, dlq, metrics = FakeReader(), FakeWriter(), FakeDlq(), Metrics()
    engine = make_engine(registry, fast(settings.sync), reader, writer, dlq, metrics=metrics)
    return engine, reader, writer, dlq, metrics


def tran_row(tran_id, amount=10, **more):
    return {"tran_id": tran_id, "amount": amount, "create_date": datetime(2021, 3, 29, 16, 33, 39), **more}


def tran_changed(tran_id, col="TRAN_POINT_ID", offset=0):
    """An update that forces a whole-document rebuild (a join column changed)."""
    return change("MB_TRANSACTION_DTL", "u", {"TRAN_ID": str(tran_id), col: "1"}, {"TRAN_ID": str(tran_id), col: "2"}, offset)


# ---------------------------------------------------------------- whole-document rebuilds
def test_insert_is_indexed(env):
    engine, reader, writer, dlq, _ = env
    reader.rows["transaction_history"] = {(5,): tran_row(5)}
    engine.process([change("MB_TRANSACTION_DTL", "c", after={"TRAN_ID": "5"})])
    assert [(o.action, o.index, o.doc_id) for o in writer.ops] == [("index", "transaction_history_", "5")]
    assert writer.ops[0].doc["amount"] == 10


def test_row_gone_from_oracle_becomes_delete(env):
    engine, reader, writer, *_ = env
    engine.process([change("MB_TRANSACTION_DTL", "d", before={"TRAN_ID": "5"})])
    assert [(o.action, o.doc_id) for o in writer.ops] == [("delete", "5")]


def test_many_updates_of_one_row_rebuild_once(env):
    engine, reader, writer, *_ = env
    reader.rows["transaction_history"] = {(5,): tran_row(5)}
    engine.process([tran_changed(5, offset=i) for i in range(20)])
    assert len(writer.ops) == 1 and len(reader.fetches) == 1


def test_join_column_change_rebuilds_the_whole_document(env):
    engine, reader, writer, *_ = env
    reader.rows["transaction_history"] = {(5,): tran_row(5)}
    engine.process([tran_changed(5)])
    assert [o.action for o in writer.ops] == ["index"] and not reader.field_fetches


# ---------------------------------------------------------------- partial updates
def test_narration_update_writes_only_that_field(env):
    engine, reader, writer, *_ = env
    reader.rows["transaction_history"] = {(5,): tran_row(5, naration="new text")}
    engine.process([tran_changed(5, col="NARATION")])
    assert reader.field_fetches == [("transaction_history", [(5,)], ["naration"])] and not reader.fetches
    assert [(o.action, o.doc_id, o.doc) for o in writer.ops] == [("update", "5", {"naration": "new text"})]


def test_partial_update_never_touches_etl_fields(env):
    engine, reader, writer, *_ = env
    reader.rows["transaction_history"] = {(5,): tran_row(5, naration="x")}
    engine.process([tran_changed(5, col="NARATION")])
    assert not {"etl_loaded_at", "etl_pipeline"} & set(writer.ops[0].doc)


def test_changed_timestamp_field_moves_the_timestamp_too(env):
    engine, reader, writer, *_ = env
    reader.rows["transaction_history"] = {(5,): tran_row(5)}
    engine.process([tran_changed(5, col="CREATE_DATE")])
    doc = writer.ops[0].doc
    assert set(doc) == {"tran_time", "create_date", "@timestamp"}
    assert doc["@timestamp"] == doc["create_date"] == "2021-03-29T10:33:39.000Z"


def test_mobile_number_change_updates_customer_and_transactions_by_id(env):
    engine, reader, writer, dlq, _ = env
    reader.lookups[("a.ac_no", (("CUST_NO", 5),))] = [("A1",), ("A2",)]
    reader.lookups[("t.tran_id", (("CUST_NO", 5),))] = [(11,), (12,), (13,)]
    reader.rows["customer_data"] = {("A1",): {"ac_no": "A1", "mobile_no": "018"}, ("A2",): {"ac_no": "A2", "mobile_no": "018"}}
    reader.rows["transaction_history"] = {(t,): tran_row(t, mobile_no="018") for t in (11, 12, 13)}
    engine.process([change("MB_CUSTOMER_MST", "u", {"CUST_NO": "5", "MOBILE_NO": "017"}, {"CUST_NO": "5", "MOBILE_NO": "018"})])
    assert not dlq.items and not reader.fetches                  # no whole-document reads at all
    assert {(o.action, o.index, o.doc_id) for o in writer.ops} == {
        ("update", "customer_data", "A1"), ("update", "customer_data", "A2"),
        ("update", "transaction_history_", "11"), ("update", "transaction_history_", "12"),
        ("update", "transaction_history_", "13")}
    assert all(o.doc == {"mobile_no": "018"} for o in writer.ops)
    assert {i for i, *_ in reader.field_fetches} == {"customer_data", "transaction_history"}   # balance index not touched


def test_event_values_are_never_written_oracle_wins(env):
    # A replayed old event says 017, but Oracle already holds 019: the document gets 019.
    engine, reader, writer, *_ = env
    reader.lookups[("a.ac_no", (("CUST_NO", 5),))] = [("A1",)]
    reader.lookups[("t.tran_id", (("CUST_NO", 5),))] = []
    reader.rows["customer_data"] = {("A1",): {"ac_no": "A1", "mobile_no": "019"}}
    engine.process([change("MB_CUSTOMER_MST", "u", {"CUST_NO": "5", "MOBILE_NO": "016"}, {"CUST_NO": "5", "MOBILE_NO": "017"})])
    assert writer.ops[0].doc == {"mobile_no": "019"}


def test_cleared_value_is_written_as_null(env):
    engine, reader, writer, *_ = env
    reader.lookups[("t.tran_id", (("CUST_NO", 5),))] = [(11,)]
    reader.rows["transaction_history"] = {(11,): tran_row(11, mobile_no2=None)}
    engine.process([change("MB_CUSTOMER_MST", "u", {"CUST_NO": "5", "MOBILE_NO2": "017"}, {"CUST_NO": "5", "MOBILE_NO2": None})])
    assert writer.ops[0].doc == {"mobile_no2": None}


def test_update_that_changes_no_mapped_column_does_nothing(env):
    engine, reader, writer, dlq, _ = env
    engine.process([change("MB_CUSTOMER_MST", "u", {"CUST_NO": "5", "UNMAPPED": "a"}, {"CUST_NO": "5", "UNMAPPED": "b"})])
    assert not writer.ops and not reader.fetches and not reader.field_fetches and not dlq.items


def test_partial_update_of_a_missing_document_falls_back_to_a_rebuild(env):
    engine, reader, writer, *_ = env
    reader.rows["transaction_history"] = {(5,): tran_row(5, naration="n")}
    writer.not_in_es = {"5"}
    engine.process([tran_changed(5, col="NARATION")])
    assert [o.action for o in writer.ops] == ["update", "index"]
    assert reader.fetches == [("transaction_history", [(5,)])]


def test_partial_update_of_a_row_gone_from_oracle_deletes(env):
    engine, reader, writer, *_ = env
    engine.process([tran_changed(5, col="NARATION")])
    assert [(o.action, o.doc_id) for o in writer.ops] == [("delete", "5")]


def test_whole_rebuild_wins_over_partial_for_the_same_key(env):
    engine, reader, writer, *_ = env
    reader.rows["transaction_history"] = {(5,): tran_row(5, naration="n")}
    engine.process([tran_changed(5, col="NARATION", offset=1), tran_changed(5, col="TRAN_POINT_ID", offset=2)])
    assert [o.action for o in writer.ops] == ["index"] and not reader.field_fetches


def test_child_change_updates_only_its_array(env):
    engine, reader, writer, *_ = env
    reader.lookups[("a.ac_no", (("AC_ID", 9),))] = [("A9",)]
    reader.rows["customer_data"] = {("A9",): {"ac_no": "A9", "nominee": '[{"nominee_id": 1}]'}}
    engine.process([change("NOMINEE_MST", "d", before={"AC_ID": "9"})])
    assert [(o.action, o.index, o.doc_id) for o in writer.ops] == [("update", "customer_data", "A9")]
    assert writer.ops[0].doc == {"nominee": [{"nominee_id": 1}]}          # JSON column parsed, nothing else sent


def test_dimension_change_fans_out_in_chunks(env):
    engine, reader, writer, *_ = env
    keys = [(f"A{i}",) for i in range(1200)]
    reader.lookups[("a.ac_no", (("POINT_ID", 3),))] = keys
    reader.rows["customer_data"] = {k: {"ac_no": k[0], "point_name": "p"} for k in keys}
    engine.max_fanout = 600
    engine.process([change("ST_AGENT_POINT", "u", {"POINT_ID": "3", "POINT_NAME": "a"}, {"POINT_ID": "3", "POINT_NAME": "b"})])
    customer_ops = [o for o in writer.ops if o.index == "customer_data"]
    assert len(customer_ops) == 1200 and {o.action for o in customer_ops} == {"update"}
    assert max(len(k) for n, k, f in reader.field_fetches if n == "customer_data") <= 500


# ---------------------------------------------------------------- failure handling
def test_poison_row_goes_to_dlq_and_others_continue(env):
    engine, reader, writer, dlq, _ = env
    bad = {"ac_no": "A1", "doc": "[{broken"}
    good = {"ac_no": "A2"}
    reader.rows["customer_data"] = {("A1",): bad, ("A2",): good}
    engine.process([change("MB_ACCOUNT_MST", "u", {"AC_NO": "A1", "AC_ID": "1", "POINT_ID": "1"}, {"AC_NO": "A1", "AC_ID": "1", "POINT_ID": "2"}),
                    change("MB_ACCOUNT_MST", "u", {"AC_NO": "A2", "AC_ID": "2", "POINT_ID": "1"}, {"AC_NO": "A2", "AC_ID": "2", "POINT_ID": "2"})])
    assert [d[0] for d in dlq.items] == ["transform"]
    assert [o.doc_id for o in writer.ops if o.index == "customer_data"] == ["A2"]


def test_es_refusal_goes_to_dlq(env):
    engine, reader, writer, dlq, _ = env
    reader.rows["transaction_history"] = {(5,): tran_row(5)}
    writer.refuse = {"5"}
    engine.process([change("MB_TRANSACTION_DTL", "c", after={"TRAN_ID": "5"})])
    assert dlq.items and dlq.items[0][0] == "elasticsearch"


def test_missing_key_column_goes_to_dlq(env):
    engine, _, writer, dlq, _ = env
    engine.process([change("NOMINEE_MST", "u", {"NOMINEE_ID": "1", "DOB": "a"}, {"NOMINEE_ID": "1", "DOB": "b"})])
    assert dlq.items[0][0] == "missing_key" and not writer.ops


def test_transient_oracle_failure_is_retried_until_it_works(env):
    engine, reader, writer, dlq, _ = env
    reader.rows["transaction_history"] = {(5,): tran_row(5)}
    reader.fail_next = [ConnectionError("link down"), TimeoutError("slow")]
    engine.process([change("MB_TRANSACTION_DTL", "c", after={"TRAN_ID": "5"})])
    assert len(writer.ops) == 1 and not dlq.items


def test_transient_failure_during_a_partial_refresh_is_retried(env):
    engine, reader, writer, dlq, _ = env
    reader.rows["transaction_history"] = {(5,): tran_row(5, naration="n")}
    reader.fail_next = [ConnectionError("link down")]
    engine.process([tran_changed(5, col="NARATION")])
    assert [o.action for o in writer.ops] == ["update"] and not dlq.items


def test_stop_interrupts_a_retry_loop(env):
    engine, reader, _, _, stop = env
    reader.fail_next = [ConnectionError("down")] * 100
    stop.set()
    with pytest.raises(ShutdownRequested):
        engine.process([change("MB_TRANSACTION_DTL", "c", after={"TRAN_ID": "5"})])


def test_permanent_failure_isolates_the_poison_event(env):
    engine, reader, writer, dlq, _ = env
    reader.rows["transaction_history"] = {(5,): tran_row(5), (6,): tran_row(6)}
    original = reader.fetch_rows

    def picky(ix, keys):
        if (5,) in keys:
            raise ValueError("ORA-00904 invalid identifier")
        return original(ix, keys)
    reader.fetch_rows = picky
    engine.process([change("MB_TRANSACTION_DTL", "c", after={"TRAN_ID": "5"}, offset=1),
                    change("MB_TRANSACTION_DTL", "c", after={"TRAN_ID": "6"}, offset=2)])
    assert [o.doc_id for o in writer.ops] == ["6"]
    assert len(dlq.items) == 1 and dlq.items[0][0] == "processing"


def test_resync_writes_every_row(env):
    engine, reader, writer, *_ = env
    reader.rows["transaction_history"] = {(i,): tran_row(i) for i in range(1, 1201)}
    assert engine.resync("transaction_history") == 1200
    assert len(writer.ops) == 1200 and {o.action for o in writer.ops} == {"index"}


# ---------------------------------------------------------------- which change types are applied
def skipped(metrics, reason="operation_not_enabled"):
    return metrics.skipped.labels(reason)._value.get()


def test_config_applies_updates_and_deletes_only(settings):
    assert settings.sync.operations == {"u", "d"}


def test_insert_is_read_but_not_applied(configured):
    engine, reader, writer, dlq, metrics = configured
    reader.rows["transaction_history"] = {(5,): tran_row(5)}
    engine.process([change("MB_TRANSACTION_DTL", "c", after={"TRAN_ID": "5"})])
    assert not writer.ops and not reader.fetches and not reader.field_fetches and not dlq.items
    assert skipped(metrics) == 1


def test_child_row_insert_is_ignored_too(configured):
    engine, reader, writer, _, metrics = configured
    reader.lookups[("a.ac_no", (("AC_ID", 9),))] = [("A9",)]
    reader.rows["customer_data"] = {("A9",): {"ac_no": "A9", "nominee": "[]"}}
    engine.process([change("NOMINEE_MST", "c", after={"AC_ID": "9", "NOMINEE_ID": "1"})])
    assert not writer.ops and skipped(metrics) == 1


def test_snapshot_read_is_ignored(configured):
    engine, reader, writer, _, metrics = configured
    engine.process([change("MB_TRANSACTION_DTL", "r", after={"TRAN_ID": "5"})])
    assert not writer.ops and skipped(metrics) == 1


def test_update_is_still_applied(configured):
    engine, reader, writer, *_ = configured
    reader.rows["transaction_history"] = {(5,): tran_row(5, naration="new")}
    engine.process([tran_changed(5, col="NARATION")])
    assert [(o.action, o.doc) for o in writer.ops] == [("update", {"naration": "new"})]


def test_delete_is_still_applied(configured):
    engine, reader, writer, *_ = configured
    engine.process([change("MB_TRANSACTION_DTL", "d", before={"TRAN_ID": "5"})])
    assert [(o.action, o.doc_id) for o in writer.ops] == [("delete", "5")]


def test_a_mixed_batch_applies_only_updates_and_deletes(configured):
    engine, reader, writer, _, metrics = configured
    reader.rows["transaction_history"] = {(6,): tran_row(6, naration="n")}
    engine.process([change("MB_TRANSACTION_DTL", "c", after={"TRAN_ID": "5"}, offset=1),
                    tran_changed(6, col="NARATION", offset=2),
                    change("MB_TRANSACTION_DTL", "d", before={"TRAN_ID": "7"}, offset=3)])
    assert sorted((o.action, o.doc_id) for o in writer.ops) == [("delete", "7"), ("update", "6")]
    assert skipped(metrics) == 1


def test_inserts_can_be_switched_back_on(registry, settings):
    reader, writer = FakeReader(), FakeWriter()
    reader.rows["transaction_history"] = {(5,): tran_row(5)}
    engine = make_engine(registry, replace(settings.sync, operations=frozenset("cud")), reader, writer)
    engine.process([change("MB_TRANSACTION_DTL", "c", after={"TRAN_ID": "5"})])
    assert [o.action for o in writer.ops] == ["index"]


def test_unknown_operation_in_config_is_rejected(settings):
    with pytest.raises(ConfigError, match="unknown"):
        replace(settings.sync, operations=frozenset({"u", "x"}))


def test_without_the_setting_every_type_is_applied(registry):
    engine = make_engine(registry, SyncSettings())
    assert engine.operations == {"c", "u", "d", "r"}


# ---------------------------------------------------------------- nested arrays (address, operator, nominee, doc)
ADDRESS = '[{"address_id": 1, "village": "V", "district": "Dhaka", "create_date": "2020-01-01T00:00:00"}]'


def customer_rows(*accounts, **fields):
    return {(a,): {"ac_no": a, **fields} for a in accounts}


def test_child_row_update_refreshes_only_its_array(configured):
    engine, reader, writer, *_ = configured
    reader.lookups[("a.ac_no", (("REF_NO", 5),))] = [("A1",), ("A2",)]
    reader.rows["customer_data"] = customer_rows("A1", "A2", address=ADDRESS)
    engine.process([change("MB_ADDRESS_MST", "u", {"REF_NO": "5", "VILLAGE": "old"}, {"REF_NO": "5", "VILLAGE": "V"})])
    assert sorted((o.action, o.doc_id) for o in writer.ops) == [("update", "A1"), ("update", "A2")]
    for op in writer.ops:
        assert set(op.doc) == {"address"}                       # nothing else in the document is sent
        assert op.doc["address"][0]["village"] == "V"           # parsed array, not a string
        assert op.doc["address"][0]["create_date"].endswith("+06:00")   # nested date gets its offset


def test_child_row_delete_refreshes_the_array_without_it(configured):
    engine, reader, writer, *_ = configured
    reader.lookups[("a.ac_no", (("REF_NO", 5),))] = [("A1",)]
    reader.rows["customer_data"] = customer_rows("A1", address="[]")          # Oracle no longer has the row
    engine.process([change("MB_ADDRESS_MST", "d", before={"REF_NO": "5", "ADDRESS_ID": "1"})])
    assert [(o.action, o.doc_id, o.doc) for o in writer.ops] == [("update", "A1", {"address": []})]   # the element is gone


def test_child_row_moved_to_another_parent_refreshes_both_arrays(configured):
    engine, reader, writer, *_ = configured
    reader.lookups[("a.ac_no", (("REF_NO", 5),))] = [("A1",)]
    reader.lookups[("a.ac_no", (("REF_NO", 6),))] = [("A2",)]
    reader.rows["customer_data"] = customer_rows("A1", "A2", address=ADDRESS)
    engine.process([change("MB_ADDRESS_MST", "u", {"REF_NO": "5"}, {"REF_NO": "6"})])
    assert sorted(o.doc_id for o in writer.ops) == ["A1", "A2"] and all(set(o.doc) == {"address"} for o in writer.ops)


def test_dimension_rename_refreshes_the_address_array_of_every_customer_in_it(configured):
    engine, reader, writer, *_ = configured
    reader.lookups[("a.ac_no", (("DISTRICT_CODE", "01"),))] = [("A1",), ("A2",), ("A3",)]
    reader.rows["customer_data"] = customer_rows("A1", "A2", "A3", address=ADDRESS)
    engine.process([change("DISTRICT_INFO", "u", {"DISTRICT_CODE": "01", "DISTRICT_NAME": "Old"},
                           {"DISTRICT_CODE": "01", "DISTRICT_NAME": "Dhaka"})])
    assert sorted(o.doc_id for o in writer.ops if o.index == "customer_data") == ["A1", "A2", "A3"]
    assert all(set(o.doc) == {"address"} for o in writer.ops if o.index == "customer_data")


def test_operator_name_change_refreshes_the_operator_array_of_accounts_they_operate(configured):
    engine, reader, writer, *_ = configured
    reader.lookups[("op.cust_no", (("CUST_NO", 9),))] = [("A7",)]
    reader.rows["customer_data"] = customer_rows("A7", operator='[{"ac_id": 1, "name": "New Name"}]')
    engine.process([change("MB_CUSTOMER_MST", "u", {"CUST_NO": "9", "FIRST_NAME": "Old"}, {"CUST_NO": "9", "FIRST_NAME": "New"})])
    ops = [o for o in writer.ops if o.index == "customer_data" and o.doc_id == "A7"]
    assert ops and "operator" in ops[0].doc and ops[0].doc["operator"][0]["name"] == "New Name"


def test_child_row_insert_does_not_touch_the_array_with_inserts_off(configured):
    engine, reader, writer, _, metrics = configured
    reader.lookups[("a.ac_no", (("REF_NO", 5),))] = [("A1",)]
    reader.rows["customer_data"] = customer_rows("A1", address=ADDRESS)
    engine.process([change("MB_ADDRESS_MST", "c", after={"REF_NO": "5", "ADDRESS_ID": "2"})])
    assert not writer.ops and not reader.field_fetches


# ---------------------------------------------------------------- parallel chunks
class SlowReader(FakeReader):
    """Counts how many field reads run at the same time, so the test can see the chunks overlap."""

    def __init__(self, delay=0.05):
        super().__init__()
        self.delay, self.running, self.peak = delay, 0, 0
        self._lock = threading.Lock()

    def fetch_fields(self, ix, keys, fields):
        with self._lock:
            self.running += 1
            self.peak = max(self.peak, self.running)
        try:
            time.sleep(self.delay)
            return super().fetch_fields(ix, keys, fields)
        finally:
            with self._lock:
                self.running -= 1


def fanout_engine(registry, settings, reader, workers, **kw):
    return make_engine(registry, fast(settings.sync, operations=ALL_OPERATIONS, workers=workers, **kw), reader)


def point_rename():
    return change("ST_AGENT_POINT", "u", {"POINT_ID": "3", "POINT_NAME": "a"}, {"POINT_ID": "3", "POINT_NAME": "b"})


def load_fanout(reader, n):
    keys = [(f"A{i}",) for i in range(n)]
    reader.lookups[("a.ac_no", (("POINT_ID", 3),))] = keys
    reader.rows["customer_data"] = {k: {"ac_no": k[0], "point_name": "p"} for k in keys}
    return keys


def test_fanout_chunks_run_in_parallel_and_every_key_is_written_once(registry, settings):
    reader = SlowReader()
    keys = load_fanout(reader, 4000)                       # 8 chunks of 500
    engine = fanout_engine(registry, settings, reader, workers=4)
    engine.process([point_rename()])
    written = [o.doc_id for o in engine.writer.ops if o.index == "customer_data"]
    assert sorted(written) == sorted(k[0] for k in keys)   # none lost, none repeated
    assert 1 < reader.peak <= 4                            # overlapped, never more than `workers`


def test_one_worker_processes_chunks_one_after_another(registry, settings):
    reader = SlowReader(delay=0.01)
    load_fanout(reader, 2000)
    fanout_engine(registry, settings, reader, workers=1).process([point_rename()])
    assert reader.peak == 1


def test_a_failure_in_a_worker_stops_the_batch_and_is_isolated_as_before(registry, settings):
    reader = SlowReader(delay=0.01)
    load_fanout(reader, 3000)
    reader.fail_next = [ValueError("bad row")]             # one chunk fails with a non-transient error
    engine = fanout_engine(registry, settings, reader, workers=4)
    engine.process([point_rename()])                       # the single-event path dead-letters it, then it is done
    assert engine.dlq.items and engine.dlq.items[0][0] == "processing"


def test_stop_interrupts_a_parallel_fanout(registry, settings):
    reader = SlowReader(delay=0.01)
    load_fanout(reader, 5000)
    engine = fanout_engine(registry, settings, reader, workers=4)
    engine.stop.set()
    with pytest.raises(ShutdownRequested):
        engine.process([point_rename()])


def test_workers_must_fit_in_the_connection_pool(settings):
    with pytest.raises(ConfigError, match="pool_max"):
        replace(settings, sync=replace(settings.sync, workers=settings.oracle.pool_max))
    with pytest.raises(ConfigError, match="workers"):
        replace(settings.sync, workers=0)


class CountingWriter(FakeWriter):
    """Counts finished bulk writes: one per chunk, the last step of a chunk."""

    def __init__(self):
        super().__init__()
        self.finished, self._lock = 0, threading.Lock()

    def apply(self, target, ops):
        failed = super().apply(target, ops)
        with self._lock:
            self.finished += 1
        return failed


class WatchedReader(SlowReader):
    """Records how many chunks have been read from the lookup stream but not yet written."""

    def __init__(self, writer, delay):
        super().__init__(delay)
        self.writer, self.produced, self.peak_outstanding = writer, 0, 0

    def lookup_chunks(self, sql, params, chunk):
        for part in super().lookup_chunks(sql, params, chunk):
            self.produced += 1
            self.peak_outstanding = max(self.peak_outstanding, self.produced - self.writer.finished)
            yield part


def test_the_lookup_stream_is_read_only_as_fast_as_the_workers_drain_it(registry, settings):
    """Backpressure: never more than 2 x workers chunks are read ahead, however long the stream is."""
    writer = CountingWriter()
    reader = WatchedReader(writer, delay=0.03)
    load_fanout(reader, 10_000)                                  # 20 chunks of 500
    engine = make_engine(registry, fast(settings.sync, operations=ALL_OPERATIONS, workers=2), reader, writer)
    engine.process([point_rename()])
    assert reader.produced == 20 and writer.finished == 20
    assert 2 < reader.peak_outstanding <= 4                      # the buffer is used, and the cap holds
