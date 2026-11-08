"""Document key values, made comparable whether they came from a Debezium event or from Oracle."""
from __future__ import annotations

import re
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

KEY_TYPES = ("int", "str", "date")
_EPOCH = datetime(1970, 1, 1)


def normalize_key(value: Any, typ: str) -> Any:
    """One canonical Python value per key type. Raises ValueError for a value that cannot be that type."""
    if value is None:
        return None
    if typ == "int":
        if isinstance(value, bool):
            raise ValueError("boolean is not a key")
        if isinstance(value, int):
            return value
        try:
            number = Decimal(str(value).strip())
        except InvalidOperation as exc:
            raise ValueError(f"not a number: {value!r}") from exc
        if number != number.to_integral_value():
            raise ValueError(f"not a whole number: {value!r}")
        return int(number)
    if typ == "str":
        return str(value)
    if typ == "date":
        if isinstance(value, datetime):
            return value.replace(microsecond=0, tzinfo=None)
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return _EPOCH + timedelta(milliseconds=value)       # Debezium: wall clock as epoch millis
        text = str(value).strip()
        if re.fullmatch(r"-?\d+", text):
            return _EPOCH + timedelta(milliseconds=int(text))
        return datetime.fromisoformat(text.replace("Z", "")).replace(microsecond=0, tzinfo=None)
    raise ValueError(f"unknown key type {typ!r}")


def column_changed(before: Any, after: Any) -> bool:
    """Did a column change? Oracle cannot tell an empty string from NULL, and neither can we."""
    if before == "":
        before = None
    if after == "":
        after = None
    return before != after
