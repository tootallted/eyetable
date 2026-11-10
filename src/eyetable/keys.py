"""Which columns could identify a row of a loaded table: its key candidates.

A scan counts each source column's nulls and distinct values (eyetable's own ET_
columns are left out: ET_id is always unique, but it is only eyetable's row
number). A column with no nulls and a different value in every row could be the
table's primary key. When no single column is, the scan looks for the smallest
sets of columns (two, then three) whose combined values are unique.

The natural key a person chooses from these is kept per table (see
CoreStore.save_table_key) and is checked again on every scan.
"""

from __future__ import annotations

import datetime as dt
import itertools
import re
import time
from typing import Any

import sqlalchemy as sa

from . import incremental
from .store import ID_COLUMN, META_COLUMNS, CoreStore, kind_of

MAX_SET_SIZE = 3
PAIR_POOL = 12  # the pairs come from this many of the most varied columns
TRIPLE_POOL = 8
MAX_SETS = 10  # report at most this many column sets
SET_SEARCH_SECONDS = 60.0  # stop looking for sets after this long and say the search was cut short
ET_COLUMNS = {ID_COLUMN, *META_COLUMNS}

_ID_NAME = re.compile(r"(^|_)(id|key|guid|uuid|code|number|no|num)$", re.IGNORECASE)


class KeyProblem(ValueError):
    """A column set that can't be a key, or a table that isn't there."""


def source_columns(store: CoreStore, table: str) -> dict[str, str]:
    """A loaded table's own columns (no ET_ ones), in table order: name -> kind."""
    if not store.table_exists(table):
        raise KeyProblem(f"there is no loaded table named {table}")
    cols = sa.inspect(store.db).get_columns(table, schema=store.data_schema)
    return {c["name"]: kind_of(c["type"]) for c in cols if c["name"] not in ET_COLUMNS}


def _expr(store: CoreStore, name: str, kind: str) -> str:
    """A column as SQL; JSON is compared as text (Postgres can't compare json values)."""
    col = store.db.dialect.identifier_preparer.quote(name)
    return f"CAST({col} AS TEXT)" if kind == "json" else col


def _rank(name: str, kind: str, position: int, table: str) -> tuple:
    """Which unique column to suggest first: id-like names, then whole numbers and text, then table order."""
    bare = re.sub(r"__.*$", "", table).lower()
    named = _ID_NAME.search(name) is not None or name.lower() in (f"{bare}id", f"{bare}_id")
    return (not named, kind not in ("bigint", "varchar", "text"), position)


def check_set(store: CoreStore, table: str, columns: list[str], kinds: dict[str, str] | None = None) -> dict[str, Any]:
    """How well `columns` identify the rows: rows with a null in them, and repeated values."""
    kinds = kinds if kinds is not None else source_columns(store, table)
    missing = [c for c in columns if c not in kinds]
    if missing:
        return {"columns": columns, "ok": False, "missing": missing}
    exprs = [_expr(store, c, kinds[c]) for c in columns]
    q = store.qualified(table)
    any_null = " OR ".join(f"{e} IS NULL" for e in exprs)
    none_null = " AND ".join(f"{e} IS NOT NULL" for e in exprs)
    group = ", ".join(exprs)
    with store.db.connect() as conn:
        rows = conn.execute(sa.text(f"SELECT COUNT(*) FROM {q}")).scalar_one()
        nulls = conn.execute(sa.text(f"SELECT COUNT(*) FROM {q} WHERE {any_null}")).scalar_one()
        dup = conn.execute(sa.text(
            f"SELECT COUNT(*), COALESCE(SUM(n), 0) FROM (SELECT COUNT(*) AS n FROM {q} WHERE {none_null} "
            f"GROUP BY {group} HAVING COUNT(*) > 1) d")).one()
    return {"columns": columns, "rows": rows, "null_rows": nulls, "duplicate_values": dup[0],
            "duplicate_rows": int(dup[1]), "ok": rows > 0 and nulls == 0 and dup[0] == 0}


def _unique(conn: sa.Connection, store: CoreStore, table: str, exprs: list[str]) -> bool:
    group = ", ".join(exprs)
    found = conn.execute(sa.text(
        f"SELECT 1 FROM {store.qualified(table)} GROUP BY {group} HAVING COUNT(*) > 1 LIMIT 1")).first()
    return found is None


def scan(store: CoreStore, table: str) -> dict[str, Any]:
    """Count each column's nulls and distinct values, and find the columns and sets that are unique."""
    started = time.monotonic()
    kinds = source_columns(store, table)
    names = list(kinds)
    q = store.qualified(table)
    with store.db.connect() as conn:
        parts = ["COUNT(*)"]
        for n in names:
            e = _expr(store, n, kinds[n])
            parts += [f"COUNT({e})", f"COUNT(DISTINCT {e})"]
        counts = conn.execute(sa.text(f"SELECT {', '.join(parts)} FROM {q}")).one()
        rows = counts[0]
        columns = []
        for i, n in enumerate(names):
            non_null, distinct = counts[1 + 2 * i], counts[2 + 2 * i]
            columns.append({"name": n, "kind": kinds[n], "nulls": rows - non_null, "distinct": distinct,
                            "unique": rows > 0 and non_null == rows and distinct == rows})

        single = sorted((c for c in columns if c["unique"]),
                        key=lambda c: _rank(c["name"], c["kind"], names.index(c["name"]), table))
        sets: list[list[str]] = []
        searched = 0
        cut_short = False
        if rows > 1 and not single:
            # Only columns with no nulls can be part of a key; a column with one value adds nothing.
            pool = sorted((c for c in columns if c["nulls"] == 0 and c["distinct"] > 1),
                          key=lambda c: (-c["distinct"], names.index(c["name"])))
            for size, limit in ((2, PAIR_POOL), (3, TRIPLE_POOL)):
                if sets or cut_short:
                    break
                for combo in itertools.combinations(pool[:limit], size):
                    if len(sets) >= MAX_SETS:
                        break
                    if time.monotonic() - started > SET_SEARCH_SECONDS:
                        cut_short = True
                        break
                    product = 1
                    for c in combo:
                        product *= c["distinct"]
                    if product < rows:  # can't have as many combinations as rows
                        continue
                    searched += 1
                    if _unique(conn, store, table, [_expr(store, c["name"], c["kind"]) for c in combo]):
                        sets.append(sorted((c["name"] for c in combo), key=names.index))
        copies = None
        if rows > 1 and not single and not sets and names:
            # Rows that are exact copies of another row: no set of columns can tell those apart.
            group = ", ".join(_expr(store, n, kinds[n]) for n in names)
            copies = int(conn.execute(sa.text(
                f"SELECT COALESCE(SUM(n - 1), 0) FROM (SELECT COUNT(*) AS n FROM {q} GROUP BY {group} HAVING COUNT(*) > 1) d"
            )).scalar_one())
        increments = incremental.candidates(conn, store, table, columns, rows)
    return {
        "table": table,
        "rows": rows,
        "columns": columns,
        "primary_key_candidates": [c["name"] for c in single],
        "column_sets": sets,
        "sets_searched": searched,
        "max_set_size": MAX_SET_SIZE,
        "cut_short": cut_short,
        "exact_copies": copies,  # counted only when nothing smaller identifies the rows
        "incremental_candidates": increments,
        "scanned_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "seconds": round(time.monotonic() - started, 2),
    }


def scan_and_save(store: CoreStore, table: str) -> dict[str, Any]:
    """Scan a table, keep the result, and check its chosen natural key against the new data."""
    result = scan(store, table)
    store.save_key_scan(table, result)
    return key_info(store, table)


def key_info(store: CoreStore, table: str) -> dict[str, Any]:
    """What the page shows: the latest scan, the chosen natural key, and how the key holds up now."""
    saved = store.get_table_key(table)
    key = saved["key"] if saved else None
    check = None
    if key and store.table_exists(table):
        check = check_set(store, table, key)
    return {
        "table": table,
        "scan": saved["scan"] if saved else None,
        "key": key,
        "key_set_at": saved["key_set_at"] if saved else None,
        "key_check": check,
        "incremental": increment_info(store, table),
    }


def increment_info(store: CoreStore, table: str) -> dict[str, Any] | None:
    """The table's incremental column, and the largest value of it loaded so far."""
    column = store.get_increment(table)
    if column is None:
        return None
    cols = {c["name"]: c["type"] for c in sa.inspect(store.db).get_columns(table, schema=store.data_schema)}         if store.table_exists(table) else {}
    if column not in cols:
        return {"column": column, "kind": None, "latest": None, "missing": True}
    return {"column": column, "kind": kind_of(cols[column]), "latest": store.max_value(table, column, cols[column]),
            "missing": False}


def choose_increment(store: CoreStore, table: str, column: str | None) -> dict[str, Any]:
    """Make `column` the table's incremental column (None stops incremental loads)."""
    if column:
        kinds = source_columns(store, table)
        if column in ET_COLUMNS:
            raise KeyProblem(f"{column} is eyetable's own; choose a column from the source")
        if column not in kinds:
            raise KeyProblem(f"{table} has no column {column}")
        if not incremental.usable(kinds[column]):
            raise KeyProblem(f"{column} holds {kinds[column]} values, which can't show which rows are new; "
                             "choose a date, time or number column")
    store.save_increment(table, column or None)
    return key_info(store, table)


def choose_key(store: CoreStore, table: str, columns: list[str]) -> dict[str, Any]:
    """Make `columns` the table's natural key, if every row has them and no two rows share them."""
    columns = list(dict.fromkeys(c for c in columns if c))
    if not columns:
        raise KeyProblem("choose at least one column")
    if len(columns) > 8:
        raise KeyProblem("a natural key can have at most 8 columns")
    et = [c for c in columns if c in ET_COLUMNS]
    if et:
        raise KeyProblem(f"{', '.join(et)} {'is' if len(et) == 1 else 'are'} eyetable's own; choose columns from the source")
    check = check_set(store, table, columns)
    if check.get("missing"):
        raise KeyProblem(f"{table} has no column {', '.join(check['missing'])}")
    if not check["ok"]:
        raise KeyProblem(describe_problem(check))
    store.save_table_key(table, columns)
    return key_info(store, table)


def describe_problem(check: dict[str, Any]) -> str:
    cols = " + ".join(check["columns"])
    if check.get("missing"):
        return f"{cols}: the table no longer has {', '.join(check['missing'])}"
    if not check["rows"]:
        return f"{cols}: the table is empty, so there is nothing to check the key against"
    problems = []
    if check["null_rows"]:
        problems.append(f"{check['null_rows']:,} row{'s' if check['null_rows'] != 1 else ''} with a blank (null) value")
    if check["duplicate_values"]:
        problems.append(f"{check['duplicate_values']:,} value{'s' if check['duplicate_values'] != 1 else ''} "
                        f"repeated across {check['duplicate_rows']:,} rows")
    return f"{cols} doesn't identify each row: {' and '.join(problems)}"
