"""Groups of sources that load together: on demand, or on a schedule.

Running a group reloads each member source in turn (see Engine.reload_source)
and records the outcome per source in eyetable.group_runs, updating it after
every source so the page can show progress. A run happens in a background
worker, one at a time, so a long group never holds up a web request.

The scheduler is a thread inside `eyetable serve`. Every 30 seconds it looks
for enabled groups whose next run time has passed, moves each to its next
slot (atomically, so a group can't be started twice), and runs it. If the
machine was off at a scheduled time, the group runs once when eyetable is
back ("catch-up"), not once per missed slot.

Scheduled runs have nobody signed in to Google, so a Google Sheet source in
a scheduled group must be readable by link; private sheets reload only from
"Run now" while you are signed in.
"""

import datetime as dt
import logging
import threading
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from eyetable import gsheets
from eyetable.engine import Engine
from eyetable.schedule import describe, next_runs, normalize
from eyetable.store import Group, SourceNotFound

log = logging.getLogger("eyetable.groups")

SheetFetch = Callable[[str, str | None], gsheets.Workbook]
CATCH_UP_AFTER = dt.timedelta(minutes=10)


def _now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _default_fetch(url: str, google_token: str | None = None) -> gsheets.Workbook:
    if google_token:
        return gsheets.fetch_workbook_signed_in(url, google_token)
    return gsheets.fetch_workbook(url)


class GroupRunner:
    def __init__(self, engine: Engine, sheet_fetch: SheetFetch = _default_fetch, clock: Callable[[], dt.datetime] = _now):
        self.engine = engine
        self.store = engine.store
        self.sheet_fetch = sheet_fetch
        self.clock = clock
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="eyetable-group")
        self._lock = threading.Lock()
        self._active: set[int] = set()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    # -- defining groups ---------------------------------------------------

    def _clean(self, name: str, members: list[str], schedule: dict | None) -> tuple[str, list[str], dict | None]:
        name = (name or "").strip()
        if not name or len(name) > 80:
            raise ValueError("a group needs a name of 1 to 80 characters")
        seen: list[str] = []
        for member in members or []:
            if member not in seen:
                seen.append(member)
        if not seen:
            raise ValueError("pick at least one source for the group")
        missing = [m for m in seen if not self.store.source_exists(m)]
        if missing:
            raise ValueError(f"no source named {', '.join(missing)}")
        return name, seen, normalize(schedule) if schedule else None

    def _next(self, schedule: dict | None, enabled: bool) -> dt.datetime | None:
        if not schedule or not enabled:
            return None
        upcoming = next_runs(schedule, self.clock())
        return upcoming[0] if upcoming else None

    def create(self, name: str, members: list[str], schedule: dict | None = None, enabled: bool = True) -> Group:
        name, members, schedule = self._clean(name, members, schedule)
        return self.store.create_group(name, members, schedule, enabled, self._next(schedule, enabled))

    def update(self, group_id: int, name: str, members: list[str], schedule: dict | None, enabled: bool) -> Group:
        name, members, schedule = self._clean(name, members, schedule)
        return self.store.update_group(group_id, name=name, members=members, schedule=schedule, enabled=enabled,
                                       next_run_at=self._next(schedule, enabled))

    def summary(self, group: Group) -> dict[str, Any]:
        runs = self.store.list_group_runs(group.id, limit=1)
        return {
            "id": group.id,
            "name": group.name,
            "members": group.members,
            "schedule": group.schedule,
            "schedule_text": describe(group.schedule),
            "enabled": group.enabled,
            "next_run_at": group.next_run_at if group.enabled else None,
            "running": group.id in self._active,
            "last_run": runs[0] if runs else None,
        }

    # -- running -----------------------------------------------------------

    def is_running(self, group_id: int) -> bool:
        with self._lock:
            return group_id in self._active

    def run(self, group_id: int, trigger: str = "manual", google_token: str | None = None, wait: bool = False) -> int:
        """Start a run of the group; returns the run id. `wait` runs it in this thread."""
        group = self.store.get_group(group_id)
        with self._lock:
            if group_id in self._active:
                raise ValueError(f"{group.name} is already running")
            self._active.add(group_id)
        try:
            run_id = self.store.start_group_run(group_id, trigger)
        except Exception:
            with self._lock:
                self._active.discard(group_id)
            raise
        if wait:
            self._execute(group, run_id, google_token)
        else:
            self._executor.submit(self._execute, group, run_id, google_token)
        return run_id

    def _execute(self, group: Group, run_id: int, google_token: str | None) -> None:
        results: list[dict[str, Any]] = []
        try:
            for source in group.members:
                entry: dict[str, Any] = {"source": source, "status": "running", "rows": 0, "error": None}
                results.append(entry)
                self.store.update_group_run(run_id, results)
                try:
                    loads = self.engine.reload_source(
                        source, sheet_fetch=lambda url: self.sheet_fetch(url, google_token)
                    )
                except SourceNotFound:
                    entry.update(status="failed", error="this source no longer exists")
                except Exception as exc:  # one source failing must not stop the rest
                    entry.update(status="failed", error=str(exc).strip("'\"") or type(exc).__name__)
                else:
                    failed = [r for r in loads if r.status != "succeeded"]
                    entry.update(
                        status="failed" if failed else "succeeded",
                        rows=sum(r.rows for r in loads),
                        error="; ".join(f"{r.dataset_key}: {r.error}" for r in failed) or None,
                    )
                self.store.update_group_run(run_id, results)
            ok = sum(1 for r in results if r["status"] == "succeeded")
            status = "succeeded" if ok == len(results) else "failed" if ok == 0 else "partial"
            self.store.update_group_run(run_id, results, status)
            log.info("group %s run %s (%s): %s", group.name, run_id, status, [r["source"] for r in results])
        except Exception:
            log.exception("group %s run %s crashed", group.name, run_id)
            self.store.update_group_run(run_id, results, "failed")
        finally:
            with self._lock:
                self._active.discard(group.id)

    # -- scheduling --------------------------------------------------------

    def tick(self) -> list[int]:
        """Start every group whose scheduled time has come; returns the run ids started."""
        now = self.clock()
        started = []
        for group in self.store.due_groups(now):
            upcoming = next_runs(group.schedule, now) if group.schedule else []
            if not self.store.claim_group(group.id, group.next_run_at, upcoming[0] if upcoming else None):
                continue
            if self.is_running(group.id):
                log.info("group %s is still running; skipping its %s slot", group.name, group.next_run_at)
                continue
            trigger = "catch-up" if now - group.next_run_at > CATCH_UP_AFTER else "schedule"
            started.append(self.run(group.id, trigger))
        return started

    def start(self, interval: float = 30) -> None:
        """Run the scheduler in a background thread (used by `eyetable serve`)."""
        stale = self.store.fail_stale_group_runs()
        if stale:
            log.warning("marked %s interrupted group run(s) as failed", stale)
        for group in self.store.list_groups():  # schedules saved without a next time get one
            if group.enabled and group.schedule and group.next_run_at is None:
                self.store.update_group(group.id, next_run_at=self._next(group.schedule, True))

        def loop() -> None:
            while not self._stop.is_set():
                try:
                    self.tick()
                except Exception:
                    log.exception("scheduler tick failed")
                self._stop.wait(interval)

        self._thread = threading.Thread(target=loop, name="eyetable-scheduler", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._executor.shutdown(wait=False, cancel_futures=True)
