"""Resuming a load that failed partway through.

An API source reads page after page (and, with a statement set, statement after
statement). As each page has been written, its connector calls `mark(state)`:
"everything so far is in; to carry on, start from `state`" (the next page number,
row offset, cursor or link, and the statement). When a full load then fails, the
engine keeps the rows written up to the last mark in the staging table and
returns the last state, so the load queue can offer to retry from there instead
of starting again (see Engine._keep_for_resume and Engine.resume).

Nothing is recorded unless a load is capturing (see capture), so other callers
of the connectors are unaffected.
"""

from __future__ import annotations

import threading
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any

_local = threading.local()


@dataclass
class Marks:
    rows: int = 0  # rows passed on to be written so far (Engine._counted counts them)
    kept_rows: int = 0  # rows written when the last mark was made
    state: dict[str, Any] | None = None  # where to carry on from, as the connector described it
    pages: int = 0  # how many pages (marks) have been completed
    extra: dict[str, Any] = field(default_factory=dict)


@contextmanager
def capture() -> Iterator[Marks]:
    previous = getattr(_local, "marks", None)
    marks = Marks()
    _local.marks = marks
    try:
        yield marks
    finally:
        _local.marks = previous


def current() -> Marks | None:
    return getattr(_local, "marks", None)


def counted(rows: int) -> None:
    marks = current()
    if marks is not None:
        marks.rows += rows


def mark(state: dict[str, Any]) -> None:
    """Everything passed on so far is complete; a retry carries on from `state`."""
    marks = current()
    if marks is not None:
        marks.state = dict(state)
        marks.kept_rows = marks.rows
        if "page" in state:  # a page done (a statement's end marks again, without a page)
            marks.pages += 1


def describe(state: dict[str, Any] | None) -> str:
    """Where a retry would start, in words: "page 15", "statement 15 of 16, page 3"."""
    if not state:
        return "the start"
    parts = []
    if state.get("statement"):
        parts.append(f"statement {state['statement']}" + (f" of {state['statements']}" if state.get("statements") else ""))
    inner = state.get("page") or {}
    if inner.get("number", 1) > 1 or not parts:
        parts.append(f"page {inner.get('number', 1)}")
    return ", ".join(parts)
