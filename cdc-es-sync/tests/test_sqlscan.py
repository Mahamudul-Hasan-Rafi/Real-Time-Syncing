"""The SQL scanner: output names, field projection and column references."""
import pytest

from cdc_es_sync.sqlscan import ScanError, column_refs, parse_select

COLS = {"T": {"A", "B", "C", "D"}, "U": {"A", "X", "Y"}}


def test_output_names_and_aliases():
    sel = parse_select("SELECT t.a, t.b AS bee, DECODE(t.c, 'x', 'y') cee, 'k' konst, "
                       "(SELECT u.x FROM u WHERE u.a = t.a) ex FROM t")
    assert sel.names == ["a", "bee", "cee", "konst", "ex"]


def test_commas_and_parens_inside_items_do_not_split():
    sel = parse_select("SELECT NVL(t.a, 0) nv, TO_CHAR(t.b, 'HH24:MI') hm FROM t")
    assert len(sel.items) == 2 and sel.names == ["nv", "hm"]


def test_string_with_comma_and_quote():
    sel = parse_select("SELECT 'a, b''c' txt, t.a FROM t")
    assert sel.names == ["txt", "a"]


def test_project_keeps_the_rest_of_the_query():
    sel = parse_select("SELECT t.a, t.b bee, t.c FROM t WHERE t.d = 1")
    assert sel.project(["c", "a"]).replace("\n", " ").split() == "SELECT t.a, t.c FROM t WHERE t.d = 1".split()


def test_project_unknown_field():
    with pytest.raises(KeyError):
        parse_select("SELECT t.a FROM t").project(["zzz"])


def test_duplicate_output_name_uses_the_first():
    sel = parse_select("SELECT t.a, t.b, t.a FROM t")
    assert sel.names == ["a", "b"] and sel.project(["a"]).count("t.a") == 1


def test_item_without_name_is_an_error():
    with pytest.raises(ScanError):
        parse_select("SELECT t.a + 1 FROM t")


def test_refs_direct_and_correlated_subselect():
    sel = parse_select("SELECT t.a, (SELECT y FROM u WHERE u.a = t.b) why, DECODE(t.c, 1, 2) cee FROM t WHERE t.d = 5")
    refs = column_refs(sel, COLS)
    assert refs.fields["a"] == {("T", "A")}
    assert refs.fields["why"] == {("U", "Y"), ("U", "A"), ("T", "B")}
    assert refs.fields["cee"] == {("T", "C")}
    assert refs.join == {("T", "D")}
    assert refs.top_tables == ["T"]


def test_alias_is_not_a_column():
    # the output alias "b" is also a column of T, but here it names the result of an expression on column C
    sel = parse_select("SELECT DECODE(t.c, 1, 2) b FROM t")
    assert column_refs(sel, COLS).fields["b"] == {("T", "C")}


def test_unqualified_column_in_single_table_subselect():
    sel = parse_select("SELECT (SELECT x FROM u WHERE a = t.a) ex FROM t")
    assert column_refs(sel, COLS).fields["ex"] == {("U", "X"), ("U", "A"), ("T", "A")}


def test_real_queries_parse_with_expected_sizes(registry):
    assert len(registry.indices["customer_data"].fields) == 91
    assert len(registry.indices["ac_balance_data"].fields) == 23
    assert len(registry.indices["transaction_history"].fields) == 85
