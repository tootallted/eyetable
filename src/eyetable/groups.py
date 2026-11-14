"""Groups of sources that load together: on demand, or on a schedule.

Running a group reloads each member source in turn (see Engine.reload_source)
and records the outcome per source in eyetable.group_runs, updating it after
every source so the page can show progress. A run happens in a background
worker, one at a time, so a long group never holds up a web request.

A tag can be run the same way without a saved group: every source that has
the tag at that moment is reloaded.

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
from eyetable.identifiers import normalize_tag
from eyetable.schedule import describe, next_runs, normalize
from eyetable.store import Group, SourceNotFound

log = logging.getLogger("eyetable.groups")
# How a group run's load queue items say what started them
TRIGGER_WORDS = {"manual": "Run now", "schedule": "scheduled", "catch-up": "scheduled, catching up"}

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
        self.queue = None  # a LoadQueue: members load through it, so the page's Load queue shows them
        self.store = engine.store
        self.sheet_fetch = sheet_fetch
        self.clock = clock
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="eyetable-group")
        self._lock = threading.Lock()
        self._active: set[int | str] = set()  # group ids, and "#tag" for a tag run on its own
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    # -- defining groups ---------------------------------------------------

    def _clean(self, name: str, members: list[str], schedule: dict | None) -> tuple[str, list[str], dict | None]:
        name = (name or "").strip()
        if not name or len(name) > 80:
            raise ValueError("a group needs a name of 1 to 80 characters")
        seen: list[str] = []
        for member in members or []:
            member = (member or "").strip()
            if member.startswith("#"):  # a tag: every source with it, decided when the group runs
                member = "#" + normalize_tag(member)
            if member and member not in seen:
                seen.append(member)
        if not seen:
            raise ValueError("pick at least one source or #tag for the group")
        missing = [m for m in seen if not m.startswith("#") and not self.store.source_exists(m)]
        if missing:
            raise ValueError(f"no source named {', '.join(missing)}")
        return name, seen, normalize(schedule) if schedule else None

    def resolve(self, members: list[str]) -> tuple[list[str], list[str]]:
        """(sources to run, in order, each once; #tags that currently match no source)."""
        sources: list[str] = []
        empty: list[str] = []
        for member in members:
            if member.startswith("#"):
                tagged = self.store.sources_with_tag(member[1:])
                if not tagged:
                    empty.append(member)
                sources.extend(s for s in tagged if s not in sources)
            elif member not in sources:
                sources.append(member)
        return sources, empty

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
        sources, empty_tags = self.resolve(group.members)
        return {
            "id": group.id,
            "name": group.name,
            "members": group.members,
            "sources": sources,  # what a run would load right now (tags expanded)
            "empty_tags": empty_tags,
            "schedule": group.schedule,
            "schedule_text": describe(group.schedule),
            "enabled": group.enabled,
            "next_run_at": group.next_run_at if group.enabled else None,
            "running": group.id in self._active,
            "last_run": runs[0] if runs else None,
        }

    # -- running -----------------------------------------------------------

    def is_running(self, key: int | str) -> bool:
        with self._lock:
            return key in self._active

    def run(self, group_id: int, trigger: str = "manual", google_token: str | None = None, wait: bool = False,
            incremental: bool | None = None) -> int:
        """Start a run of the group; returns the run id. `wait` runs it in this thread.

        `incremental`: load each table incrementally where it has an incremental column and in
        full elsewhere; False loads everything in full. A scheduled run (incremental=None) does
        what its schedule says ("load": "incremental" or "full").
        """
        group = self.store.get_group(group_id)
        if incremental is None:
            incremental = bool(group.schedule) and group.schedule.get("load") == "incremental"
        by = f"Group {group.name} \u00b7 {TRIGGER_WORDS.get(trigger, trigger)}" + (", incremental" if incremental else "")
        return self._start(group_id, group.name, group.members,
                           lambda: self.store.start_group_run(group_id, trigger), google_token, wait, by, incremental)

    def run_tag(self, tag: str, google_token: str | None = None, wait: bool = False, incremental: bool = False) -> int:
        """Start a run of every source with a tag; returns the run id."""
        tag = normalize_tag(tag)
        if not self.store.sources_with_tag(tag):
            raise LookupError(f"no source is tagged #{tag}")
        return self._start(f"#{tag}", f"#{tag}", [f"#{tag}"], lambda: self.store.start_tag_run(tag), google_token, wait,
                           f"Tag #{tag} \u00b7 Run now" + (", incremental" if incremental else ""), incremental)

    def _start(self, key: int | str, name: str, members: list[str], record: Callable[[], int],
               google_token: str | None, wait: bool, by: str | None = None, incremental: bool = False) -> int:
        with self._lock:
            if key in self._active:
                raise ValueError(f"{name} is already running")
            self._active.add(key)
        try:
            run_id = record()
        except Exception:
            with self._lock:
                self._active.discard(key)
            raise
        if wait:
            self._execute(key, name, members, run_id, google_token, by, incremental)
        else:
            self._executor.submit(self._execute, key, name, members, run_id, google_token, by, incremental)
        return run_id

    def _load(self, source: str, google_token: str | None, by: str | None, incremental: bool = False) -> dict[str, Any]:
        """Load one member: through the load queue when there is one (so the page shows it, with its
        progress and log), else directly. Returns {status, rows, error} for the group run's results."""
        if self.queue is None:
            loads = self.engine.reload_source(source, sheet_fetch=lambda url: self.sheet_fetch(url, google_token),
                                              incremental="auto" if incremental else False)
            failed = [r for r in loads if r.status == "failed"]
            hidden = loads and all(r.status == "skipped" for r in loads)
            return {"status": "failed" if failed else "skipped" if hidden else "succeeded",
                    "rows": sum(r.rows for r in loads),
                    "load": "incremental" if any(r.mode == "incremental" for r in loads) else "full",
                    "error": "; ".join(f"{r.dataset_key}: {r.error}" for r in failed) or
                             ("hidden in Sources, so not loaded" if hidden else None)}
        try:
            item, _ = self.queue.add(source, google_token=google_token, by=by, incremental="auto" if incremental else False)
        except ValueError as exc:
            if "hidden" in str(exc):
                return {"status": "skipped", "rows": 0, "error": "hidden in Sources, so not loaded"}
            raise
        done = self.queue.wait(item["id"])  # one member at a time, as a group run always has
        rows = sum(t["rows"] for t in done["tables"] if t["status"] == "succeeded")
        # what this source actually did: incremental if any of its tables loaded incrementally
        load = "incremental" if any(t.get("mode") == "incremental" for t in done["tables"]) else "full"
        if done["status"] == "succeeded":
            return {"status": "succeeded", "rows": rows, "load": load, "error": None}
        if done["status"] == "cancelled":
            return {"status": "failed", "rows": rows, "load": load, "error": "cancelled in the load queue"}
        return {"status": "failed", "rows": rows, "load": load,
                "error": "; ".join(e["message"] for e in done["errors"]) or done["status"]}

    def _execute(self, key: int | str, name: str, members: list[str], run_id: int, google_token: str | None,
                 by: str | None = None, incremental: bool = False) -> None:
        results: list[dict[str, Any]] = []
        try:
            sources, empty_tags = self.resolve(members)
            for tag in empty_tags:
                results.append({"source": tag, "status": "failed", "rows": 0, "error": f"no source is tagged {tag}"})
            for source in sources:
                entry: dict[str, Any] = {"source": source, "status": "running", "rows": 0, "error": None,
                                         "load": "incremental" if incremental else "full"}
                results.append(entry)
                self.store.update_group_run(run_id, results)
                try:
                    entry.update(self._load(source, google_token, by, incremental))
                except SourceNotFound:
                    entry.update(status="failed", error="this source no longer exists")
                except Exception as exc:  # one source failing must not stop the rest
                    entry.update(status="failed", error=str(exc).strip("'\"") or type(exc).__name__)
                self.store.update_group_run(run_id, results)
            ok = sum(1 for r in results if r["status"] in ("succeeded", "skipped"))  # a hidden source isn't a failure
            status = "succeeded" if ok == len(results) else "failed" if ok == 0 else "partial"
            self.store.update_group_run(run_id, results, status)
            log.info("group %s run %s (%s): %s", name, run_id, status, [r["source"] for r in results])
        except Exception:
            log.exception("group %s run %s crashed", name, run_id)
            self.store.update_group_run(run_id, results, "failed")
        finally:
            with self._lock:
                self._active.discard(key)

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
