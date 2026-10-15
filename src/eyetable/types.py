"""Column type inference for records pulled from any source.

Every value maps to a small set of portable kinds; core storage turns a kind
into a real column type, and widens it when later rows disagree.
"""

import datetime as dt
import decimal
from collections.abc import Iterable
from typing import Any

_INT64 = 2**63

_NUMERIC_WIDENING = {
    frozenset({"bigint", "double"}): "double",
    frozenset({"bigint", "numeric"}): "numeric",
    frozenset({"double", "numeric"}): "double",
    frozenset({"timestamp", "timestamptz"}): "timestamptz",
}


def infer_kind(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "bigint" if -_INT64 <= value < _INT64 else "numeric"
    if isinstance(value, float):
        return "double"
    if isinstance(value, decimal.Decimal):
        return "numeric"
    if isinstance(value, dt.datetime):
        return "timestamptz" if value.tzinfo else "timestamp"
    if isinstance(value, dt.date):
        return "date"
    if isinstance(value, dt.time):
        return "time"
    if isinstance(value, (dict, list)):
        return "json"
    if isinstance(value, (bytes, bytearray, memoryview)):
        return "binary"
    return "text"


def merge_kinds(a: str | None, b: str | None) -> str | None:
    if a is None:
        return b
    if b is None or a == b:
        return a
    return _NUMERIC_WIDENING.get(frozenset({a, b}), "text")


def infer_columns(records: Iterable[dict[str, Any]]) -> dict[str, str | None]:
    """Column name -> kind across all records, in first-seen order."""
    kinds: dict[str, str | None] = {}
    for record in records:
        for name, value in record.items():
            kinds[name] = merge_kinds(kinds.get(name), infer_kind(value))
    return kinds
