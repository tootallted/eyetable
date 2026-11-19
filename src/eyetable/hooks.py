"""The run webhook: processes outside eyetable record their runs in eyetable.runs.

A call to /hook (GET with query parameters, or POST with JSON or a form) adds one
row to the runs table, so other jobs show up next to eyetable's own loads, in
"Show earlier runs" and in queries of eyetable.runs:

  * source        required; stored as "webhook - <source>"
  * target_table  required
  * mode          optional; blank is NULL
  * status        optional; "succeeded" when left out. Words for success (success,
                  successful, ok, ...) become "succeeded" and words for failure
                  (fail, failure, error, ...) become "failed", so they filter and
                  show like eyetable's own; anything else is kept as it is
  * rows_written  optional whole number; 0 when left out
  * started_at    optional ISO time; NULL when left out
  * finished_at   optional ISO time; now when left out
  * error         optional; blank is NULL

A time with no offset is read as this server's local time. The call needs the
webhook key (EYETABLE_HOOK_KEY), as `key=...`, an X-Hook-Key header or a
Bearer token; the API token works too.
"""

from __future__ import annotations

import datetime as dt
import re
from typing import Any

PREFIX = "webhook - "
SOURCE_MAX = 64 - len(PREFIX)  # runs.source is varchar(64)
SHORT_MAX = 16  # runs.mode and runs.status are varchar(16)
SUCCEEDED = {"success", "successful", "succeeded", "succeed", "ok", "pass", "passed", "done", "complete",
             "completed", "true", "1"}
FAILED = {"fail", "failed", "failure", "error", "errored", "false", "0"}
FIELDS = ("source", "target_table", "mode", "status", "rows_written", "started_at", "finished_at", "error")
_SPACED_OFFSET = re.compile(r"(T\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?) (\d{2}:?\d{2})$")


class HookError(ValueError):
    pass


def _text(values: dict[str, Any], name: str) -> str | None:
    value = values.get(name)
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _time(values: dict[str, Any], name: str) -> dt.datetime | None:
    text = _text(values, name)
    if text is None:
        return None
    # in a query string an unencoded "+05:00" arrives as " 05:00"
    text = _SPACED_OFFSET.sub(r"\1+\2", text)
    try:
        when = dt.datetime.fromisoformat(text)
    except ValueError:
        raise HookError(f"{name} must be a time like 2026-11-19T06:02:13-08:00 (got {text!r})") from None
    return when if when.tzinfo else when.astimezone()  # no offset: this server's local time


def run_from(values: dict[str, Any], now: dt.datetime | None = None) -> dict[str, Any]:
    """The runs row a webhook call describes; raises HookError saying what's wrong."""
    source, table = _text(values, "source"), _text(values, "target_table")
    missing = [n for n, v in (("source", source), ("target_table", table)) if v is None]
    if missing:
        raise HookError(f"{' and '.join(missing)} {'is' if len(missing) == 1 else 'are'} required")
    if len(source) > SOURCE_MAX:
        raise HookError(f"source can be at most {SOURCE_MAX} characters")
    mode = _text(values, "mode")
    if mode and len(mode) > SHORT_MAX:
        raise HookError(f"mode can be at most {SHORT_MAX} characters")
    status = _text(values, "status")
    if status is None or status.lower() in SUCCEEDED:
        status = "succeeded"
    elif status.lower() in FAILED:
        status = "failed"
    elif len(status) > SHORT_MAX:
        raise HookError(f"status can be at most {SHORT_MAX} characters")
    rows = _text(values, "rows_written")
    if rows is None:
        rows_written = 0
    elif re.fullmatch(r"\d+", rows.replace(",", "")):
        rows_written = int(rows.replace(",", ""))
    else:
        raise HookError(f"rows_written must be a whole number of rows (got {rows!r})")
    started, finished = _time(values, "started_at"), _time(values, "finished_at")
    finished = finished or now or dt.datetime.now(dt.timezone.utc)
    if started and started > finished:
        raise HookError("started_at is after finished_at")
    return {"source": PREFIX + source, "target_table": table, "mode": mode, "status": status,
            "rows_written": rows_written, "started_at": started, "finished_at": finished,
            "error": _text(values, "error")}
