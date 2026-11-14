"""A load's own log: the queries, requests and steps it makes, for the load queue to show.

While a load runs inside `capture(sink)`, everything on its thread reports here:
SQL sent to the source database and to eyetable's store (through SQLAlchemy's
events, timed, with row counts), HTTP requests an API source sends, files read,
and steps the engine takes (staging, columns added, the swap, incremental and
in-parts decisions). Nothing is recorded when no load is capturing, so loads
outside the queue (the CLI, tests) are unaffected.

Entries are dicts: {"at": unix time, "kind", "text", ...}. Kinds: "sql" (with
"db": "source" or "eyetable", "ms", "rows", maybe "params"), "http" (with
"status", "ms", "bytes"), "file", "step", "warn".
"""

from __future__ import annotations

import re
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any

import sqlalchemy as sa

Sink = Callable[[dict[str, Any]], None]
_local = threading.local()
MAX_SQL = 4000  # characters of a statement kept
MAX_PARAMS = 400
_SECRET = re.compile(r"key|token|secret|pass|pwd|sig|auth|code|session|credential", re.IGNORECASE)
# SQLAlchemy reading a table's description (its columns, keys, indexes): bookkeeping, not the load itself
_DESCRIBE = re.compile(r"^\s*PRAGMA\b|sqlite_master|INFORMATION_SCHEMA\.|\bsys\.(columns|tables|objects|indexes|types|"
                       r"schemas|index_columns|foreign_keys|foreign_key_columns|extended_properties|identity_columns|"
                       r"computed_columns|default_constraints|check_constraints|key_constraints)\b|\bpg_catalog\.",
                       re.IGNORECASE)


@contextmanager
def capture(sink: Sink | None, store_engine: sa.Engine | None = None, meta_tables: set[str] | None = None) -> Iterator[None]:
    """Send everything this thread does to `sink` until the block ends. Statements on
    `store_engine` are eyetable's own; any other database is the source. Statements on
    `meta_tables` (eyetable's records of sources, runs and so on) are marked "meta"."""
    previous = getattr(_local, "sink", None), getattr(_local, "store_engine", None), getattr(_local, "meta", None)
    _local.sink, _local.store_engine = sink, store_engine
    _local.meta = re.compile(r"\b(" + "|".join(sorted(map(re.escape, meta_tables))) + r")\b") if meta_tables else None
    try:
        yield
    finally:
        _local.sink, _local.store_engine, _local.meta = previous


def active() -> bool:
    return getattr(_local, "sink", None) is not None


def emit(kind: str, text: str, **fields: Any) -> dict[str, Any] | None:
    """Record one entry; returns it (so a caller can add a duration later), or None when nothing listens."""
    sink = getattr(_local, "sink", None)
    if sink is None:
        return None
    entry = {"at": time.time(), "kind": kind, "text": text, **fields}
    try:
        kept = sink(entry)  # a sink may fold a repeat into the entry before it, and return that one
    except Exception:  # the log must never break a load
        return None
    return kept if isinstance(kept, dict) else entry


def step(text: str, **fields: Any) -> dict[str, Any] | None:
    return emit("step", text, **fields)


def safe_url(url: Any) -> str:
    """A URL for the log: host, path and query, with values of key/token-like parameters masked."""
    import httpx

    u = httpx.URL(str(url))
    params = [(k, "…" if _SECRET.search(k) else v) for k, v in u.params.multi_items()]
    query = "&".join(f"{k}={v}" for k, v in params)
    return f"{u.scheme}://{u.host}{u.path}" + (f"?{query}" if query else "")


# -- every SQL statement on a capturing thread ---------------------------------------------

def _short(value: Any, limit: int) -> str:
    text = str(value)
    return text if len(text) <= limit else text[:limit] + f"… ({len(text):,} characters)"


@sa.event.listens_for(sa.engine.Engine, "before_cursor_execute")
def _before(conn, cursor, statement, parameters, context, executemany):  # noqa: ANN001
    if not active():
        return
    ours = conn.engine is getattr(_local, "store_engine", None)
    fields: dict[str, Any] = {"db": "eyetable" if ours else "source"}
    meta = getattr(_local, "meta", None)
    if (ours and meta is not None and meta.search(statement)) or _DESCRIBE.search(statement):
        fields["meta"] = True  # bookkeeping: eyetable's own records, or SQLAlchemy describing a table
    if executemany:
        fields["batch"] = len(parameters) if hasattr(parameters, "__len__") else None
    elif parameters and not ours:  # a source query's values (ranges, since) say what it asked for
        fields["params"] = _short(parameters, MAX_PARAMS)
    entry = emit("sql", _short(" ".join(statement.split()), MAX_SQL), **fields)
    if entry is not None and context is not None:
        context._eyetable_log = (entry, time.perf_counter())


@sa.event.listens_for(sa.engine.Engine, "after_cursor_execute")
def _after(conn, cursor, statement, parameters, context, executemany):  # noqa: ANN001
    started = getattr(context, "_eyetable_log", None) if context is not None else None
    if not started:
        return
    entry, t0 = started
    ms = round((time.perf_counter() - t0) * 1000, 1)
    entry["ms"] = ms
    entry["ms_total"] = round(entry.get("ms_total", 0) + ms, 1)  # a repeated statement adds up
    rowcount = getattr(cursor, "rowcount", -1)
    if isinstance(rowcount, int) and rowcount >= 0:
        entry["rows"] = rowcount
        entry["rows_total"] = entry.get("rows_total", 0) + rowcount
