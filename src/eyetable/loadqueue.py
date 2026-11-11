"""The load queue: reloads run in the background, each with live progress and its errors.

A reload asked for on the page becomes a queue item and the request returns at
once, so a big table no longer holds the page up. Items run on a small pool of
workers; two loads of the same source never run at the same time (the second
waits its turn). While an item runs, every batch written updates its tables'
row counts, shown against the row count each table had before ("15,000 of
30,000 rows"). When it ends, the item says succeeded, failed or cancelled, with
every error message and the full technical detail of each.

The queue lives in the API process: it starts empty when the service restarts
(the runs themselves are recorded in eyetable.runs as always).
"""

from __future__ import annotations

import datetime as dt
import secrets
import threading
import traceback
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from .connectors.file import DATASET_KEY
from .engine import Engine
from .identifiers import table_name
from .incremental import IncrementalProblem

ACTIVE = ("queued", "running")
KEEP_FINISHED = 40  # finished items listed until dismissed; older ones drop off


class LoadCancelled(Exception):
    """Raised inside a running load when it is cancelled from the queue."""


def _now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def describe(exc: BaseException) -> str:
    """An exception as the person reading the queue should see it."""
    text = str(exc).strip().strip("'\"")
    return f"{type(exc).__name__}: {text}" if text else type(exc).__name__


class LoadQueue:
    def __init__(self, engine: Engine, sheet_fetch: Callable[[str, str | None], Any] | None = None, workers: int = 2):
        self.engine = engine
        self.store = engine.store
        self._sheet_fetch = sheet_fetch
        self._items: list[dict[str, Any]] = []
        self._lock = threading.Lock()
        self._pool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="eyetable-load")
        self._source_locks: dict[str, threading.Lock] = {}
        self._tokens: dict[str, str] = {}  # a Google sign-in for one item; never listed
        self._done: dict[str, threading.Event] = {}

    # -- what an item will load ----------------------------------------------------

    def planned_tables(self, source: str, table: str | None, incremental: bool) -> list[dict[str, str]]:
        """The tables a reload of `source` (or of its `table`) loads: [{table, label}]."""
        from . import dbconnect

        src = self.store.get_source(source)
        if incremental and src.type == "file":
            raise IncrementalProblem("a file source gets new rows by loading a file with Append; there is nothing to fetch")
        if src.type == "dbtable":
            planned = [{"table": table_name(source, i["name"]), "label": f"{i['schema']}.{i['table']}"}
                       for i in dbconnect.db_tables(src.config)]
        else:
            planned = [{"table": table_name(source, DATASET_KEY), "label": source}]
        if table is not None:
            planned = [p for p in planned if p["table"] == table]
            if not planned:
                raise ValueError(f"{source} has no table {table}")
        if incremental:
            chosen = self.store.increments()
            planned = [p for p in planned if p["table"] in chosen]
            if not planned:
                raise IncrementalProblem("no incremental column is chosen for this table; choose one in Preview Data")
        return planned

    # -- adding, listing, cancelling ----------------------------------------------

    def add(self, source: str, table: str | None = None, incremental: bool = False,
            google_token: str | None = None) -> tuple[dict[str, Any], bool]:
        """Queue a reload; returns (the item, whether it is new). The same reload already waiting
        or running is returned instead of being queued twice."""
        planned = self.planned_tables(source, table, incremental)
        with self._lock:
            for item in self._items:
                if item["status"] in ACTIVE and (item["source"], item["table"], item["incremental"]) == (source, table, incremental):
                    return self._public(item), False
        tables = []
        for p in planned:
            # how many rows the table had: what a full load is measured against
            before = self.store.count_rows(p["table"])
            tables.append({**p, "before": before, "expected": None if incremental else before, "rows": 0,
                           "status": "waiting", "mode": None, "since": None, "replaced": 0, "error": None,
                           "started_at": None, "updated_at": None, "finished_at": None})
        item = {
            "id": secrets.token_hex(6), "source": source, "table": table, "incremental": incremental,
            "label": planned[0]["label"] if table else source,
            "status": "queued", "created_at": _now(), "started_at": None, "finished_at": None,
            "tables": tables, "errors": [], "cancel": False,
        }
        with self._lock:
            self._items.append(item)
            self._done[item["id"]] = threading.Event()
            if google_token:
                self._tokens[item["id"]] = google_token
        self._pool.submit(self._run, item["id"])
        return self._public(item), True

    def list(self) -> list[dict[str, Any]]:
        """Every item, newest first."""
        with self._lock:
            return [self._public(i) for i in reversed(self._items)]

    def get(self, item_id: str) -> dict[str, Any]:
        with self._lock:
            return self._public(self._find(item_id))

    def cancel(self, item_id: str) -> dict[str, Any]:
        """Stop an item: one still waiting never starts; a running one stops at its next batch."""
        with self._lock:
            item = self._find(item_id)
            if item["status"] in ACTIVE:
                item["cancel"] = True
                if item["status"] == "queued":
                    self._finish(item, "cancelled")
            return self._public(item)

    def dismiss(self, item_id: str) -> None:
        with self._lock:
            item = self._find(item_id)
            if item["status"] in ACTIVE:
                raise ValueError("this load hasn't finished; cancel it first")
            self._items.remove(item)

    def clear_finished(self) -> int:
        with self._lock:
            before = len(self._items)
            self._items = [i for i in self._items if i["status"] in ACTIVE]
            return before - len(self._items)

    def wait(self, item_id: str, timeout: float | None = None) -> dict[str, Any]:
        """Block until an item ends (for tests and scripts)."""
        self._done[item_id].wait(timeout)
        return self.get(item_id)

    # -- running -------------------------------------------------------------------

    def _run(self, item_id: str) -> None:
        with self._lock:
            item = self._find(item_id)
            lock = self._source_locks.setdefault(item["source"], threading.Lock())
        with lock:  # one load of a source at a time
            with self._lock:
                if item["status"] != "queued":  # cancelled while it waited
                    return
                item["status"], item["started_at"] = "running", _now()
                token = self._tokens.pop(item_id, None)

            def progress(table: str, rows: int) -> None:
                if item["cancel"]:
                    raise LoadCancelled("cancelled from the load queue")
                with self._lock:
                    entry = self._entry(item, table)
                    if entry["status"] == "waiting":
                        entry["status"], entry["started_at"] = "loading", _now()
                    entry["rows"], entry["updated_at"] = rows, _now()

            fetch = (lambda url: self._sheet_fetch(url, token)) if self._sheet_fetch else None
            try:
                with self.engine.reporting(progress):
                    kwargs = {"sheet_fetch": fetch} if fetch else {}
                    results = self.engine.reload_source(item["source"], incremental=item["incremental"],
                                                        table=item["table"], **kwargs)
            except Exception as exc:
                with self._lock:
                    item["errors"].append({"table": None, "message": describe(exc), "detail": traceback.format_exc()})
                    for entry in item["tables"]:
                        if entry["status"] in ("waiting", "loading"):
                            entry["status"] = "cancelled" if item["cancel"] else "failed"
                    self._finish(item, "cancelled" if item["cancel"] else "failed")
                return
            with self._lock:
                seen: set[str] = set()
                for r in results:
                    entry = self._entry(item, r.table)
                    # a file source's stored files all build one table: their rows add up
                    rows = r.rows + (entry["rows"] if r.table in seen and r.status == "succeeded" else 0)
                    seen.add(r.table)
                    entry.update(status=r.status, rows=rows if r.status == "succeeded" else entry["rows"], mode=r.mode,
                                 since=r.since, replaced=r.replaced, error=r.error, finished_at=_now())
                    if r.status != "succeeded":
                        cancelled = item["cancel"] and "LoadCancelled" in (r.error or "")
                        entry["status"] = "cancelled" if cancelled else "failed"
                        if not cancelled:
                            item["errors"].append({"table": r.table, "message": r.error or "failed", "detail": r.detail})
                for entry in item["tables"]:
                    if entry["status"] in ("waiting", "loading"):
                        entry["status"] = "cancelled" if item["cancel"] else "skipped"
                if item["cancel"]:
                    status = "cancelled"
                elif any(e["status"] == "failed" for e in item["tables"]) or item["errors"]:
                    status = "failed"
                else:
                    status = "succeeded"
                self._finish(item, status)

    # -- helpers (called with self._lock held) --------------------------------------

    def _finish(self, item: dict[str, Any], status: str) -> None:
        item["status"], item["finished_at"] = status, _now()
        self._tokens.pop(item["id"], None)
        event = self._done.get(item["id"])
        if event:
            event.set()
        finished = [i for i in self._items if i["status"] not in ACTIVE]
        for old in finished[:-KEEP_FINISHED]:
            self._items.remove(old)
            self._done.pop(old["id"], None)

    def _find(self, item_id: str) -> dict[str, Any]:
        for item in self._items:
            if item["id"] == item_id:
                return item
        raise KeyError(f"no load {item_id} in the queue (it may have been cleared, or eyetable restarted)")

    @staticmethod
    def _entry(item: dict[str, Any], table: str) -> dict[str, Any]:
        for entry in item["tables"]:
            if entry["table"] == table:
                return entry
        entry = {"table": table, "label": table, "before": None, "expected": None, "rows": 0, "status": "waiting",
                 "mode": None, "since": None, "replaced": 0, "error": None, "started_at": None, "updated_at": None,
                 "finished_at": None}
        item["tables"].append(entry)
        return entry

    @staticmethod
    def _public(item: dict[str, Any]) -> dict[str, Any]:
        out = {k: v for k, v in item.items() if k != "cancel"}
        out["cancelling"] = item["cancel"] and item["status"] in ACTIVE
        out["tables"] = [dict(t) for t in item["tables"]]
        out["errors"] = [dict(e) for e in item["errors"]]
        return out
