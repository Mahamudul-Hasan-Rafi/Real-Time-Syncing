"""The field map is checked against the SQL, offline (tests/fixtures/table_columns.json)."""
import re

import pytest

from cdc_es_sync.core.audit import audit_field_map


@pytest.mark.parametrize("index", ["customer_data", "ac_balance_data", "transaction_history"])
def test_every_table_in_each_query_is_mapped_or_excluded(registry, index):
    ix = registry.indices[index]
    mapped = {t for t, imps in registry.rules_by_table.items() if any(i.index == index for i in imps)}
    loose = registry.tables_in_sql(index) - mapped - set(ix.not_propagated)
    assert not loose, f"{index} reads {sorted(loose)} but nothing maps them"


@pytest.mark.parametrize("index", ["customer_data", "ac_balance_data", "transaction_history"])
def test_exclusions_are_real_tables_of_the_query(registry, index):
    ix = registry.indices[index]
    stale = set(ix.not_propagated) - registry.tables_in_sql(index)
    assert not stale, f"{index} excludes {sorted(stale)} which the query does not use"


def test_every_lookup_returns_one_column_per_key_column(registry):
    for impacts in registry.rules_by_table.values():
        for imp in impacts:
            if imp.sql:
                select = re.match(r"(?is)\s*select\s+(.*?)\s+from\s", imp.sql).group(1)
                assert len(select.split(",")) == len(registry.indices[imp.index].key_columns), imp.id


def test_field_map_matches_the_sql(registry, table_columns):
    """Every column a query reads is mapped to the fields it feeds, and nothing mapped is unused."""
    assert audit_field_map(registry, table_columns) == []


def test_audit_catches_a_forgotten_column(registry, table_columns):
    imp = next(i for i in registry.rules_by_table["MB_CUSTOMER_MST"] if i.index == "customer_data" and i.join)
    saved = imp.columns.pop("MOBILE_NO")
    try:
        problems = audit_field_map(registry, table_columns)
    finally:
        imp.columns["MOBILE_NO"] = saved
    assert any("MB_CUSTOMER_MST.MOBILE_NO" in p and "mobile_no" in p for p in problems)


def test_audit_catches_a_field_a_column_does_not_feed(registry, table_columns):
    imp = next(i for i in registry.rules_by_table["MB_CUSTOMER_MST"] if i.index == "customer_data" and i.join)
    saved = imp.columns["MOBILE_NO"]
    imp.columns["MOBILE_NO"] = ("mobile_no", "first_name")
    try:
        problems = audit_field_map(registry, table_columns)
    finally:
        imp.columns["MOBILE_NO"] = saved
    assert any("MOBILE_NO does not feed ['first_name']" in p for p in problems)


def test_audit_catches_a_column_the_query_never_reads(registry, table_columns):
    imp = next(i for i in registry.rules_by_table["MB_CUSTOMER_MST"] if i.index == "customer_data" and i.join)
    imp.columns["MOBILE_NO2"] = ("mobile_no",)
    try:
        problems = audit_field_map(registry, table_columns)
    finally:
        del imp.columns["MOBILE_NO2"]
    assert any("MOBILE_NO2 is not read by the query" in p for p in problems)


def test_audit_catches_a_join_column_that_is_not_a_rebuild(registry, table_columns):
    imp = next(i for i in registry.rules_by_table["MB_ACCOUNT_MST"] if i.index == "customer_data")
    saved = imp.columns["CUST_NO"]
    imp.columns["CUST_NO"] = ("cust_no",)
    try:
        problems = audit_field_map(registry, table_columns)
    finally:
        imp.columns["CUST_NO"] = saved
    assert any("join column CUST_NO must be 'rebuild'" in p for p in problems)


def test_audit_catches_an_unmapped_table(registry, table_columns):
    saved = registry.rules_by_table.pop("NOMINEE_MST")
    try:
        problems = audit_field_map(registry, table_columns)
    finally:
        registry.rules_by_table["NOMINEE_MST"] = saved
    assert any("NOMINEE_MST" in p and "no source maps it" in p for p in problems)
