"""Key values from Debezium and from Oracle normalize to the same Python value."""
from datetime import datetime

import pytest

from cdc_es_sync.core.keys import column_changed, normalize_key


def test_normalize_int_from_debezium_string():
    assert normalize_key("3124", "int") == 3124
    assert normalize_key("3124.0", "int") == 3124
    assert normalize_key(3124, "int") == 3124


def test_normalize_int_rejects_fraction():
    with pytest.raises(ValueError):
        normalize_key("3124.5", "int")


def test_normalize_date_epoch_millis_is_wall_clock():
    # Debezium sends a DATE as the wall clock expressed as epoch millis.
    assert normalize_key(1605117600000, "date") == datetime(2020, 11, 11, 18, 0, 0)
    assert normalize_key("1605117600000", "date") == datetime(2020, 11, 11, 18, 0, 0)


def test_normalize_date_from_oracle_drops_microseconds():
    assert normalize_key(datetime(2020, 11, 11, 18, 0, 0, 5), "date") == datetime(2020, 11, 11, 18, 0, 0)


def test_normalize_str_keeps_leading_zeros():
    assert normalize_key("007", "str") == "007"


def test_empty_string_and_null_are_not_a_change():
    assert not column_changed("", None)
    assert column_changed("a", None)
