from datetime import datetime

import pytest

from cdc_es_sync.events import BadEvent, Change, parse_event
from cdc_es_sync.mapping import FULL, MissingKeyError, Plan, norm


def change(table, op, before=None, after=None):
    return Change(table, op, before, after, 1, 1, f"emob.EMOB.{table}", 0, 0)


# ---------------------------------------------------------------- key normalisation
def test_norm_int_from_debezium_string():
    assert norm("3124", "int") == 3124
    assert norm("3124.0", "int") == 3124
    assert norm(3124, "int") == 3124


def test_norm_int_rejects_fraction():
    with pytest.raises(ValueError):
        norm("3124.5", "int")


def test_norm_date_epoch_millis_is_wall_clock():
    # Debezium sends a DATE as the wall clock expressed as epoch millis.
    assert norm(1605117600000, "date") == datetime(2020, 11, 11, 18, 0, 0)
    assert norm("1605117600000", "date") == datetime(2020, 11, 11, 18, 0, 0)


def test_norm_date_from_oracle_drops_microseconds():
    assert norm(datetime(2020, 11, 11, 18, 0, 0, 5), "date") == datetime(2020, 11, 11, 18, 0, 0)


def test_norm_str_keeps_leading_zeros():
    assert norm("007", "str") == "007"


# ---------------------------------------------------------------- event parsing
def event(op, before=None, after=None, table="NOMINEE_MST"):
    import json
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
        with pytest.raises(BadEvent):
            parse_event(bad, "t", 0, 1)


# ---------------------------------------------------------------- routing
def route(registry, ch):
    plan = Plan()
    registry.route(ch, plan)
    return plan


def lookups_by_index(plan):
    """index -> list of field sets (None = rebuild) of the lookups the plan holds."""
    out = {}
    for impact, fields in plan.lookups.values():
        out.setdefault(impact.index, []).append(None if fields is None else frozenset(fields))
    return out


def test_transaction_insert_rebuilds_the_document(registry):
    plan = route(registry, change("MB_TRANSACTION_DTL", "c", after={"TRAN_ID": "55"}))
    assert plan.full == {"transaction_history": {(55,)}}
    assert not plan.lookups and not plan.part


def test_transaction_delete_uses_before_image(registry):
    plan = route(registry, change("MB_TRANSACTION_DTL", "d", before={"TRAN_ID": "55"}))
    assert plan.full == {"transaction_history": {(55,)}}


def test_transaction_narration_update_touches_only_that_field(registry):
    plan = route(registry, change("MB_TRANSACTION_DTL", "u",
                                  {"TRAN_ID": "55", "NARATION": "old"}, {"TRAN_ID": "55", "NARATION": "new"}))
    assert plan.part == {"transaction_history": {(55,): {"naration"}}}
    assert not plan.full and not plan.lookups


def test_transaction_point_change_rebuilds_the_document(registry):
    plan = route(registry, change("MB_TRANSACTION_DTL", "u",
                                  {"TRAN_ID": "55", "TRAN_POINT_ID": "1"}, {"TRAN_ID": "55", "TRAN_POINT_ID": "2"}))
    assert plan.full == {"transaction_history": {(55,)}}


def test_balance_key_has_date(registry):
    plan = route(registry, change("MB_ACCOUNT_DAILY_BALANCE", "c",
                                  after={"AC_ID": "3124", "VALUE_DATE": 1605117600000}))
    assert plan.full == {"ac_balance_data": {(3124, datetime(2020, 11, 11, 18, 0))}}


def test_balance_amount_update_is_partial(registry):
    ev = {"AC_ID": "3124", "VALUE_DATE": 1605117600000}
    plan = route(registry, change("MB_ACCOUNT_DAILY_BALANCE", "u", {**ev, "BALANCE": "1"}, {**ev, "BALANCE": "2"}))
    assert plan.part == {"ac_balance_data": {(3124, datetime(2020, 11, 11, 18, 0)): {"balance"}}}


def test_account_title_change_reaches_all_three_indices(registry):
    plan = route(registry, change("MB_ACCOUNT_MST", "u",
                                  {"AC_ID": "3", "AC_NO": "A1", "AC_TITLE": "x"}, {"AC_ID": "3", "AC_NO": "A1", "AC_TITLE": "y"}))
    assert plan.part == {"customer_data": {("A1",): {"ac_title"}}}
    assert lookups_by_index(plan) == {"ac_balance_data": [frozenset({"ac_title"})],
                                      "transaction_history": [frozenset({"ac_title"})]}


def test_account_number_change_rebuilds_everywhere(registry):
    plan = route(registry, change("MB_ACCOUNT_MST", "u",
                                  {"AC_ID": "3", "AC_NO": "A1"}, {"AC_ID": "3", "AC_NO": "A2"}))
    assert plan.full == {"customer_data": {("A1",), ("A2",)}}      # the document key: old one is found absent and deleted
    assert lookups_by_index(plan) == {"ac_balance_data": [frozenset({"ac_no"})],       # a plain field there
                                      "transaction_history": [frozenset({"ac_no"})]}


def test_child_row_moved_between_parents_refreshes_both(registry):
    plan = route(registry, change("NOMINEE_MST", "u", {"AC_ID": "1"}, {"AC_ID": "2"}))
    assert {dict(p)["AC_ID"] for (_, p) in plan.lookups} == {1, 2}
    assert lookups_by_index(plan) == {"customer_data": [frozenset({"nominee"}), frozenset({"nominee"})]}


def test_child_delete_uses_before_image_only(registry):
    plan = route(registry, change("NOMINEE_MST", "d", before={"AC_ID": "9"}))
    assert [dict(p)["AC_ID"] for (_, p) in plan.lookups] == [9]
    assert lookups_by_index(plan) == {"customer_data": [frozenset({"nominee"})]}


def test_duplicate_events_collapse(registry):
    plan = Plan()
    for i in range(5):
        registry.route(change("NOMINEE_MST", "u", {"AC_ID": "1", "NOMINEE_NAME": f"a{i}"},
                              {"AC_ID": "1", "NOMINEE_NAME": f"b{i}"}), plan)
    assert len(plan.lookups) == 1


def test_lookup_fields_accumulate_and_rebuild_wins(registry):
    plan = Plan()
    registry.route(change("NOMINEE_MST", "u", {"AC_ID": "1", "DOB": "1"}, {"AC_ID": "1", "DOB": "2"}), plan)
    registry.route(change("MB_CUSTOMER_MST", "u", {"CUST_NO": "5", "MOBILE_NO": "1"}, {"CUST_NO": "5", "MOBILE_NO": "2"}), plan)
    registry.route(change("MB_CUSTOMER_MST", "u", {"CUST_NO": "5", "FIRST_NAME": "a"}, {"CUST_NO": "5", "FIRST_NAME": "b"}), plan)
    own = [f for imp, f in plan.lookups.values() if imp.index == "customer_data" and "CUST_NO" in imp.params
           and imp.join == "inner"]
    assert own == [{"mobile_no", "first_name"}]
    registry.route(change("MB_CUSTOMER_MST", "u", {"CUST_NO": "5", "AGENT_POINT_ID": "1"}, {"CUST_NO": "5", "AGENT_POINT_ID": "2"}), plan)
    own = [f for imp, f in plan.lookups.values() if imp.index == "customer_data" and "CUST_NO" in imp.params
           and imp.join == "inner"]
    assert own == [None]


# ---------------------------------------------------------------- the mobile number scenarios
def test_mobile_no_change_reaches_customer_and_transaction_only(registry):
    plan = route(registry, change("MB_CUSTOMER_MST", "u", {"CUST_NO": "5", "MOBILE_NO": "017"}, {"CUST_NO": "5", "MOBILE_NO": "018"}))
    assert lookups_by_index(plan) == {"customer_data": [frozenset({"mobile_no"})],
                                      "transaction_history": [frozenset({"mobile_no"})]}
    assert not plan.full and not plan.part          # ac_balance_data does not carry the number: not touched


def test_second_mobile_number_reaches_transaction_history_only(registry):
    plan = route(registry, change("MB_CUSTOMER_MST", "u", {"CUST_NO": "5", "MOBILE_NO2": "017"}, {"CUST_NO": "5", "MOBILE_NO2": "018"}))
    assert lookups_by_index(plan) == {"transaction_history": [frozenset({"mobile_no2"})]}


def test_mobile_number_cleared_is_a_change(registry):
    plan = route(registry, change("MB_CUSTOMER_MST", "u", {"CUST_NO": "5", "MOBILE_NO2": "017"}, {"CUST_NO": "5", "MOBILE_NO2": None}))
    assert lookups_by_index(plan) == {"transaction_history": [frozenset({"mobile_no2"})]}


def test_mobile_number_set_from_nothing_is_a_change(registry):
    plan = route(registry, change("MB_CUSTOMER_MST", "u", {"CUST_NO": "5", "MOBILE_NO2": ""}, {"CUST_NO": "5", "MOBILE_NO2": "018"}))
    assert "transaction_history" in lookups_by_index(plan)


def test_empty_string_and_null_are_the_same(registry):
    plan = Plan()
    registry.route(change("MB_CUSTOMER_MST", "u", {"CUST_NO": "5", "MOBILE_NO2": ""}, {"CUST_NO": "5", "MOBILE_NO2": None}), plan)
    assert not plan.lookups and plan.unchanged > 0


def test_update_that_changes_nothing_mapped_does_nothing(registry):
    plan = Plan()
    assert registry.route(change("MB_CUSTOMER_MST", "u", {"CUST_NO": "5", "SOMETHING": "a"}, {"CUST_NO": "5", "SOMETHING": "b"}), plan)
    assert not (plan.full or plan.part or plan.lookups) and plan.unchanged > 0


def test_customer_insert_rebuilds_accounts(registry):
    plan = route(registry, change("MB_CUSTOMER_MST", "c", after={"CUST_NO": "5"}))
    by = lookups_by_index(plan)
    assert None in by["customer_data"] and None in by["ac_balance_data"]       # inner-join table


def test_outer_join_table_insert_only_refreshes_its_fields(registry):
    plan = route(registry, change("ST_AGENT_POINT", "c", after={"POINT_ID": "3"}))
    customer = [f for imp, f in plan.lookups.values() if imp.index == "customer_data"]
    assert customer == [{"point_name", "area_name", "point_category", "location_type"}]


def test_missing_key_column_is_reported(registry):
    with pytest.raises(MissingKeyError):
        route(registry, change("NOMINEE_MST", "u", {"NOMINEE_ID": "1", "NOMINEE_NAME": "a"},
                               {"NOMINEE_ID": "1", "NOMINEE_NAME": "b"}))


def test_unmapped_table_is_skipped(registry):
    plan = Plan()
    assert registry.route(change("SOMETHING_ELSE", "c", after={"X": 1}), plan) is False


def test_dimension_changes_reach_transaction_history(registry):
    for table in ("MB_ACCOUNT_MST", "MB_CUSTOMER_MST", "MB_PRODUCT_MST", "ST_AGENT_POINT", "STCODMAS",
                  "ST_POINT_CATEGORY", "DISTRICT_INFO", "UPAZILA_INFO", "UNION_INFO", "ST_SECTOR_CODE",
                  "ST_TRANSACTION_CODE"):
        assert any(i.index == "transaction_history" for i in registry.tables[table]), table


# ---------------------------------------------------------------- mapping completeness (offline audit)
@pytest.mark.parametrize("index", ["customer_data", "ac_balance_data", "transaction_history"])
def test_every_table_in_each_query_is_mapped_or_excluded(registry, index):
    ix = registry.indices[index]
    mapped = {t for t, imps in registry.tables.items() if any(i.index == index for i in imps)}
    loose = registry.tables_in_sql(index) - mapped - set(ix.not_propagated)
    assert not loose, f"{index} reads {sorted(loose)} but nothing maps them"


@pytest.mark.parametrize("index", ["customer_data", "ac_balance_data", "transaction_history"])
def test_exclusions_are_real_tables_of_the_query(registry, index):
    ix = registry.indices[index]
    stale = set(ix.not_propagated) - registry.tables_in_sql(index)
    assert not stale, f"{index} excludes {sorted(stale)} which the query does not use"


def test_every_lookup_returns_one_column_per_key_column(registry):
    import re
    for table, impacts in registry.tables.items():
        for imp in impacts:
            if imp.sql:
                select = re.match(r"(?is)\s*select\s+(.*?)\s+from\s", imp.sql).group(1)
                assert len(select.split(",")) == len(registry.indices[imp.index].key_cols), imp.id


def test_field_map_matches_the_sql(registry, table_columns):
    """Every column a query reads is mapped to the fields it feeds, and nothing mapped is unused."""
    assert registry.audit(table_columns) == []


def test_audit_catches_a_forgotten_column(registry, table_columns):
    imp = next(i for i in registry.tables["MB_CUSTOMER_MST"] if i.index == "customer_data" and i.join)
    saved = imp.columns.pop("MOBILE_NO")
    try:
        problems = registry.audit(table_columns)
    finally:
        imp.columns["MOBILE_NO"] = saved
    assert any("MB_CUSTOMER_MST.MOBILE_NO" in p and "mobile_no" in p for p in problems)


def test_audit_catches_a_field_a_column_does_not_feed(registry, table_columns):
    imp = next(i for i in registry.tables["MB_CUSTOMER_MST"] if i.index == "customer_data" and i.join)
    saved = imp.columns["MOBILE_NO"]
    imp.columns["MOBILE_NO"] = ("mobile_no", "first_name")
    try:
        problems = registry.audit(table_columns)
    finally:
        imp.columns["MOBILE_NO"] = saved
    assert any("MOBILE_NO does not feed ['first_name']" in p for p in problems)


def test_audit_catches_a_column_the_query_never_reads(registry, table_columns):
    imp = next(i for i in registry.tables["MB_CUSTOMER_MST"] if i.index == "customer_data" and i.join)
    imp.columns["MOBILE_NO2"] = ("mobile_no",)
    try:
        problems = registry.audit(table_columns)
    finally:
        del imp.columns["MOBILE_NO2"]
    assert any("MOBILE_NO2 is not read by the query" in p for p in problems)


def test_audit_catches_a_join_column_that_is_not_a_rebuild(registry, table_columns):
    imp = next(i for i in registry.tables["MB_ACCOUNT_MST"] if i.index == "customer_data")
    saved = imp.columns["CUST_NO"]
    imp.columns["CUST_NO"] = ("cust_no",)
    try:
        problems = registry.audit(table_columns)
    finally:
        imp.columns["CUST_NO"] = saved
    assert any("join column CUST_NO must be 'rebuild'" in p for p in problems)


def test_audit_catches_an_unmapped_table(registry, table_columns):
    saved = registry.tables.pop("NOMINEE_MST")
    try:
        problems = registry.audit(table_columns)
    finally:
        registry.tables["NOMINEE_MST"] = saved
    assert any("NOMINEE_MST" in p and "no source maps it" in p for p in problems)
