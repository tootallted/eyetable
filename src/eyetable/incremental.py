"""Incremental loads: fetch only the rows that are new since the last load.

A table's *incremental column* is usually a last-modified time, but it can be
any column whose values only grow as rows are added or changed (a creation
time, an increasing ID or version number). An incremental load:

1. reads the largest value of that column already loaded;
2. fetches only rows past it. A database source is asked for just those rows;
   an API or Google Sheet source is fetched in full and only those rows are
   kept;
3. appends them. If the table has a natural key (keys.py), each new row
   replaces the older copy of the same row, so a changed row is not kept twice.
   With a natural key the boundary value is fetched again (>= rather than >),
   so rows that share the last timestamp are not missed.

The key scan (keys.scan) suggests which columns could be the incremental column.
"""

from __future__ import annotations

import datetime as dt
import re
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

import sqlalchemy as sa

from .identifiers import column_name
from .store import RESERVED_COLUMNS, CoreStore

TIME_KINDS = {"timestamp", "timestamptz", "date"}
NUMBER_KINDS = {"bigint", "double", "numeric"}
TEXT_KINDS = {"text", "varchar"}
MAX_CANDIDATES = 8

_MODIFIED = re.compile(r"modif|updat|chang|(^|_)edit|lastmod|last_mod|sysstart|sys_start|(^|_)ts$|timestamp|synced",
                       re.IGNORECASE)
# a row version, not (say) timezoneruleversionnumber
_VERSION = re.compile(r"^(row_?)?version(_?(no|num|number))?$", re.IGNORECASE)
_CREATED = re.compile(r"creat|insert|added|entered|received|(^|_)date$", re.IGNORECASE)
_ID = re.compile(r"(^|_)(id|seq|sequence|number|no|num)$|id$", re.IGNORECASE)


class IncrementalProblem(ValueError):
    """An incremental column that can't be used, or a load that can't be incremental."""


# -- comparing values from a table and from a source ---------------------------------

def parse_time(text: str) -> dt.datetime | None:
    """An ISO-8601 date or time in text, as an aware datetime (UTC when no zone is given)."""
    s = text.strip()
    if len(s) < 10 or s[4:5] != "-" or s[7:8] != "-":
        return None
    try:
        value = dt.datetime.fromisoformat(s.replace("Z", "+00:00").replace("z", "+00:00"))
    except ValueError:
        return None
    return value if value.tzinfo else value.replace(tzinfo=dt.timezone.utc)


def ordered(value: Any) -> tuple[str, Any] | None:
    """A value in a form that compares across types: ("t", aware datetime), ("n", Decimal) or ("s", text)."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, dt.datetime):
        return "t", value if value.tzinfo else value.replace(tzinfo=dt.timezone.utc)
    if isinstance(value, dt.date):
        return "t", dt.datetime(value.year, value.month, value.day, tzinfo=dt.timezone.utc)
    if isinstance(value, (int, float, Decimal)):
        return "n", Decimal(str(value))
    if isinstance(value, str):
        when = parse_time(value)
        if when is not None:
            return "t", when
        try:
            return "n", Decimal(value.strip())
        except InvalidOperation:
            return "s", value
    return "s", str(value)


def is_past(value: Any, since: tuple[str, Any], inclusive: bool) -> bool:
    """Whether a source value comes after `since`; a value that can't be compared is kept (never lost)."""
    v = ordered(value)
    if v is None:
        return False  # no value: an incremental load can't tell whether the row is new
    if v[0] != since[0]:
        return True
    return v[1] >= since[1] if inclusive else v[1] > since[1]


def source_key(record_keys: Iterable[str], column: str) -> str | None:
    """Which key of a source record lands in the table column `column` (see _TableWriter._column_for)."""
    for k in record_keys:
        name = column_name(k)
        if name == column or (name.lower() in RESERVED_COLUMNS and f"{name}_src" == column):
            return k
    return None


def newer(batches: Iterable[list[dict[str, Any]]], column: str, since: Any, inclusive: bool) -> Iterator[list[dict[str, Any]]]:
    """Keep only the records whose `column` value comes after `since`."""
    mark = ordered(since)
    found: dict[frozenset, str | None] = {}
    for batch in batches:
        kept = []
        for record in batch:
            shape = frozenset(record)
            if shape not in found:
                found[shape] = source_key(record, column)
            key = found[shape]
            if key is not None and is_past(record[key], mark, inclusive):
                kept.append(record)
        if kept:
            yield kept


# -- finding candidates -------------------------------------------------------------

def candidates(conn: sa.Connection, store: CoreStore, table: str, columns: list[dict[str, Any]], rows: int) -> list[dict[str, Any]]:
    """The columns that could drive incremental loads, best first, each with why and its range.

    `columns` is the key scan's column list (name, kind, nulls, distinct, unique); each
    orderable column gets "min" and "max" added.
    """
    if not rows:
        return []
    prep = store.db.dialect.identifier_preparer
    q = store.qualified(table)
    ranged = [c for c in columns if c["kind"] in TIME_KINDS | NUMBER_KINDS | TEXT_KINDS and c["distinct"] > 1]
    if not ranged:
        return []
    parts = []
    for c in ranged:
        col = prep.quote(c["name"])
        parts += [f"MIN({col})", f"MAX({col})"]
    found = conn.execute(sa.text(f"SELECT {', '.join(parts)} FROM {q}")).one()
    out = []
    for i, c in enumerate(ranged):
        low, high = found[2 * i], found[2 * i + 1]
        kind = c["kind"]
        if kind in TEXT_KINDS:
            # text counts only if it holds ISO dates or times (as JSON from APIs usually does)
            if not (isinstance(low, str) and isinstance(high, str) and parse_time(low) and parse_time(high)):
                continue
            sample = conn.execute(sa.text(
                f"SELECT {prep.quote(c['name'])} FROM {q} WHERE {prep.quote(c['name'])} IS NOT NULL LIMIT 50")).scalars().all()
            if not all(isinstance(v, str) and parse_time(v) for v in sample):
                continue
            kind_word = "date or time (as text)"
            timeish = True
        else:
            timeish = kind in TIME_KINDS
            kind_word = "date or time" if timeish else "number"
        name = c["name"]
        if timeish and _MODIFIED.search(name):
            score, why = 0, "looks like a last-modified time: picks up new and changed rows"
        elif not timeish and _VERSION.search(name):
            score, why = 1, "looks like a row version number: picks up new and changed rows"
        elif timeish and _CREATED.search(name):
            score, why = 2, "looks like a creation time: picks up new rows, but not changes to older ones"
        elif not timeish and c["unique"] and _ID.search(name):
            score, why = 3, "an ID with no repeats: picks up new rows if IDs only grow, but not changes to older ones"
        elif timeish:
            score, why = 4, "a " + kind_word + " column"
        elif c["unique"] and c["kind"] == "bigint":
            score, why = 5, "a whole number with no repeats: picks up new rows if it only grows"
        else:
            continue
        if c["nulls"]:
            why += f"; {c['nulls']:,} row{'s' if c['nulls'] != 1 else ''} without a value would never be picked up"
        out.append({"name": name, "kind": c["kind"], "nulls": c["nulls"], "min": low, "max": high,
                    "why": why, "score": score + (0.5 if c["nulls"] else 0)})
    out.sort(key=lambda c: (c["score"], [x["name"] for x in columns].index(c["name"])))
    return out[:MAX_CANDIDATES]


def usable(kind: str) -> bool:
    return kind in TIME_KINDS | NUMBER_KINDS | TEXT_KINDS


# -- the column a table uses, and what has been loaded so far ------------------------

@dataclass
class Plan:
    """How to load a table incrementally: rows whose `column` is past `since`."""
    column: str
    since: Any  # the largest value loaded so far; None means the table is empty (load it all)
    inclusive: bool  # >= when a natural key will replace the boundary rows; > otherwise
    key: list[str] | None


def plan(store: CoreStore, table: str) -> Plan:
    column = store.get_increment(table)
    if column is None:
        raise IncrementalProblem(f"{table} has no incremental column; choose one in Preview Data")
    if not store.table_exists(table):
        return Plan(column, None, False, None)
    kinds = {c["name"]: c["type"] for c in store.columns(table)}
    if column not in kinds:
        raise IncrementalProblem(f"{table} no longer has its incremental column {column}; choose another")
    saved = store.get_table_key(table)
    key = saved["key"] if saved and saved["key"] else None
    since = store.max_value(table, column, kinds[column])
    return Plan(column, since, key is not None, key)


def describe_since(value: Any) -> str:
    if isinstance(value, dt.datetime):
        return value.isoformat(sep=" ", timespec="seconds")
    return str(value)
