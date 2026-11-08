"""Routing: which documents, and which fields of them, a change event affects."""
from datetime import datetime

import pytest

from cdc_es_sync.core.events import Change
from cdc_es_sync.core.model import ChangePlan
from cdc_es_sync.errors import MissingKeyError


def change(table, op, before=None, after=None):
    return Change(table, op, before, after, 1, 1, f"emob.EMOB.{table}", 0, 0)


def route(registry, ch):
    plan = ChangePlan()
    registry.route(ch, plan)
    return plan


def lookups_by_index(plan):
    """index -> list of field sets (None = rebuild) of the lookups the plan holds."""
    out = {}
    for pending in plan.lookups.values():
        out.setdefault(pending.rule.index, []).append(None if pending.fields is None else frozenset(pending.fields))
    return out


def test_transaction_insert_rebuilds_the_document(registry):
    plan = route(registry, change("MB_TRANSACTION_DTL", "c", after={"TRAN_ID": "55"}))
    assert plan.rebuild == {"transaction_history": {(55,)}}
    assert not plan.lookups and not plan.partial


def test_transaction_delete_uses_before_image(registry):
    plan = route(registry, change("MB_TRANSACTION_DTL", "d", before={"TRAN_ID": "55"}))
    assert plan.rebuild == {"transaction_history": {(55,)}}


def test_transaction_narration_update_touches_only_that_field(registry):
    plan = route(registry, change("MB_TRANSACTION_DTL", "u",
                                  {"TRAN_ID": "55", "NARATION": "old"}, {"TRAN_ID": "55", "NARATION": "new"}))
    assert plan.partial == {"transaction_history": {(55,): {"naration"}}}
    assert not plan.rebuild and not plan.lookups


def test_transaction_point_change_rebuilds_the_document(registry):
    plan = route(registry, change("MB_TRANSACTION_DTL", "u",
                                  {"TRAN_ID": "55", "TRAN_POINT_ID": "1"}, {"TRAN_ID": "55", "TRAN_POINT_ID": "2"}))
    assert plan.rebuild == {"transaction_history": {(55,)}}


def test_balance_key_has_date(registry):
    plan = route(registry, change("MB_ACCOUNT_DAILY_BALANCE", "c",
                                  after={"AC_ID": "3124", "VALUE_DATE": 1605117600000}))
    assert plan.rebuild == {"ac_balance_data": {(3124, datetime(2020, 11, 11, 18, 0))}}


def test_balance_amount_update_is_partial(registry):
    ev = {"AC_ID": "3124", "VALUE_DATE": 1605117600000}
    plan = route(registry, change("MB_ACCOUNT_DAILY_BALANCE", "u", {**ev, "BALANCE": "1"}, {**ev, "BALANCE": "2"}))
    assert plan.partial == {"ac_balance_data": {(3124, datetime(2020, 11, 11, 18, 0)): {"balance"}}}


def test_account_title_change_reaches_all_three_indices(registry):
    plan = route(registry, change("MB_ACCOUNT_MST", "u",
                                  {"AC_ID": "3", "AC_NO": "A1", "AC_TITLE": "x"}, {"AC_ID": "3", "AC_NO": "A1", "AC_TITLE": "y"}))
    assert plan.partial == {"customer_data": {("A1",): {"ac_title"}}}
    assert lookups_by_index(plan) == {"ac_balance_data": [frozenset({"ac_title"})],
                                      "transaction_history": [frozenset({"ac_title"})]}


def test_account_number_change_rebuilds_everywhere(registry):
    plan = route(registry, change("MB_ACCOUNT_MST", "u",
                                  {"AC_ID": "3", "AC_NO": "A1"}, {"AC_ID": "3", "AC_NO": "A2"}))
    assert plan.rebuild == {"customer_data": {("A1",), ("A2",)}}      # the document key: old one is found absent and deleted
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
    plan = ChangePlan()
    for i in range(5):
        registry.route(change("NOMINEE_MST", "u", {"AC_ID": "1", "NOMINEE_NAME": f"a{i}"},
                              {"AC_ID": "1", "NOMINEE_NAME": f"b{i}"}), plan)
    assert len(plan.lookups) == 1


def test_lookup_fields_accumulate_and_rebuild_wins(registry):
    plan = ChangePlan()
    registry.route(change("NOMINEE_MST", "u", {"AC_ID": "1", "DOB": "1"}, {"AC_ID": "1", "DOB": "2"}), plan)
    registry.route(change("MB_CUSTOMER_MST", "u", {"CUST_NO": "5", "MOBILE_NO": "1"}, {"CUST_NO": "5", "MOBILE_NO": "2"}), plan)
    registry.route(change("MB_CUSTOMER_MST", "u", {"CUST_NO": "5", "FIRST_NAME": "a"}, {"CUST_NO": "5", "FIRST_NAME": "b"}), plan)
    own = [p.fields for p in plan.lookups.values() if p.rule.index == "customer_data" and "CUST_NO" in p.rule.params
           and p.rule.join == "inner"]
    assert own == [{"mobile_no", "first_name"}]
    registry.route(change("MB_CUSTOMER_MST", "u", {"CUST_NO": "5", "AGENT_POINT_ID": "1"}, {"CUST_NO": "5", "AGENT_POINT_ID": "2"}), plan)
    own = [p.fields for p in plan.lookups.values() if p.rule.index == "customer_data" and "CUST_NO" in p.rule.params
           and p.rule.join == "inner"]
    assert own == [None]


# ---------------------------------------------------------------- the mobile number scenarios
def test_mobile_no_change_reaches_customer_and_transaction_only(registry):
    plan = route(registry, change("MB_CUSTOMER_MST", "u", {"CUST_NO": "5", "MOBILE_NO": "017"}, {"CUST_NO": "5", "MOBILE_NO": "018"}))
    assert lookups_by_index(plan) == {"customer_data": [frozenset({"mobile_no"})],
                                      "transaction_history": [frozenset({"mobile_no"})]}
    assert not plan.rebuild and not plan.partial          # ac_balance_data does not carry the number: not touched


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
    plan = ChangePlan()
    registry.route(change("MB_CUSTOMER_MST", "u", {"CUST_NO": "5", "MOBILE_NO2": ""}, {"CUST_NO": "5", "MOBILE_NO2": None}), plan)
    assert not plan.lookups and plan.unchanged > 0


def test_update_that_changes_nothing_mapped_does_nothing(registry):
    plan = ChangePlan()
    assert registry.route(change("MB_CUSTOMER_MST", "u", {"CUST_NO": "5", "SOMETHING": "a"}, {"CUST_NO": "5", "SOMETHING": "b"}), plan)
    assert not (plan.rebuild or plan.partial or plan.lookups) and plan.unchanged > 0


def test_customer_insert_rebuilds_accounts(registry):
    plan = route(registry, change("MB_CUSTOMER_MST", "c", after={"CUST_NO": "5"}))
    by = lookups_by_index(plan)
    assert None in by["customer_data"] and None in by["ac_balance_data"]       # inner-join table


def test_outer_join_table_insert_only_refreshes_its_fields(registry):
    plan = route(registry, change("ST_AGENT_POINT", "c", after={"POINT_ID": "3"}))
    customer = [p.fields for p in plan.lookups.values() if p.rule.index == "customer_data"]
    assert customer == [{"point_name", "area_name", "point_category", "location_type"}]


def test_missing_key_column_is_reported(registry):
    with pytest.raises(MissingKeyError):
        route(registry, change("NOMINEE_MST", "u", {"NOMINEE_ID": "1", "NOMINEE_NAME": "a"},
                               {"NOMINEE_ID": "1", "NOMINEE_NAME": "b"}))


def test_unmapped_table_is_skipped(registry):
    plan = ChangePlan()
    assert registry.route(change("SOMETHING_ELSE", "c", after={"X": 1}), plan) is False


def test_dimension_changes_reach_transaction_history(registry):
    for table in ("MB_ACCOUNT_MST", "MB_CUSTOMER_MST", "MB_PRODUCT_MST", "ST_AGENT_POINT", "STCODMAS",
                  "ST_POINT_CATEGORY", "DISTRICT_INFO", "UPAZILA_INFO", "UNION_INFO", "ST_SECTOR_CODE",
                  "ST_TRANSACTION_CODE"):
        assert any(i.index == "transaction_history" for i in registry.rules_by_table[table]), table
