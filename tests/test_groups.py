import datetime as dt
import time
from zoneinfo import ZoneInfo

import pytest
from fastapi.testclient import TestClient

from eyetable.api import create_app
from eyetable.engine import Engine
from eyetable.groups import GroupRunner
from eyetable.schedule import ScheduleError, describe, next_runs, normalize

TOKEN = "test-token-0123456789"
AUTH = {"Authorization": f"Bearer {TOKEN}"}
LA = ZoneInfo("America/Los_Angeles")
DAILY_6AM = {"type": "daily", "times": ["06:00"], "days": [0, 1, 2, 3, 4, 5, 6], "timezone": "America/Los_Angeles"}
WORKDAY_HOURLY = {"type": "hourly", "every": 1, "start": "08:00", "end": "17:00", "days": [0, 1, 2, 3, 4],
                  "timezone": "America/Los_Angeles"}


def local(*args):
    return dt.datetime(*args, tzinfo=LA)


# -- schedules -------------------------------------------------------------

def test_daily_and_hourly_next_runs():
    wed_1130 = local(2026, 10, 21, 11, 30)
    assert next_runs(DAILY_6AM, wed_1130, 2) == [local(2026, 10, 22, 6), local(2026, 10, 23, 6)]
    assert next_runs(WORKDAY_HOURLY, wed_1130, 3) == [local(2026, 10, 21, 12), local(2026, 10, 21, 13), local(2026, 10, 21, 14)]
    assert next_runs(WORKDAY_HOURLY, local(2026, 10, 21, 17, 0))[0] == local(2026, 10, 22, 8)  # strictly after
    assert next_runs(WORKDAY_HOURLY, local(2026, 10, 23, 17, 30))[0] == local(2026, 10, 26, 8)  # Fri evening -> Mon


def test_every_two_hours_and_several_daily_times():
    every2 = {**WORKDAY_HOURLY, "every": 2}
    assert [t.astimezone(LA).hour for t in next_runs(every2, local(2026, 10, 21, 7), 6)] == [8, 10, 12, 14, 16, 8]
    twice = {**DAILY_6AM, "times": ["18:00", "06:00"], "days": [5, 6]}
    assert normalize(twice)["times"] == ["06:00", "18:00"]
    assert next_runs(twice, local(2026, 10, 21, 12), 3) == [local(2026, 10, 24, 6), local(2026, 10, 24, 18), local(2026, 10, 25, 6)]


def test_daylight_saving_runs_once():
    nightly = {**DAILY_6AM, "times": ["01:30"]}
    runs = next_runs(nightly, local(2026, 10, 31, 12), 3)  # clocks go back Nov 1
    assert [r.astimezone(LA).strftime("%m-%d %H:%M") for r in runs] == ["11-01 01:30", "11-02 01:30", "11-03 01:30"]


def test_descriptions():
    assert describe(DAILY_6AM) == "Every day at 6:00 AM"
    assert describe(WORKDAY_HOURLY) == "Every hour from 8:00 AM to 5:00 PM, Monday to Friday"
    assert describe({**DAILY_6AM, "times": ["06:00", "13:30"], "days": [0, 2, 4]}) == "Daily at 6:00 AM and 1:30 PM, on Mon, Wed, Fri"
    assert describe({**WORKDAY_HOURLY, "every": 3, "days": [5, 6]}) == "Every 3 hours from 8:00 AM to 5:00 PM, on weekends"
    assert describe(None) == "Not scheduled"


@pytest.mark.parametrize("bad", [
    {"type": "weekly"},
    {**DAILY_6AM, "times": []},
    {**DAILY_6AM, "times": ["6am"]},
    {**DAILY_6AM, "days": []},
    {**DAILY_6AM, "timezone": "Mars/Olympus"},
    {**WORKDAY_HOURLY, "every": 5},
    {**WORKDAY_HOURLY, "start": "18:00"},
])
def test_bad_schedules(bad):
    with pytest.raises(ScheduleError):
        normalize(bad)


# -- groups ----------------------------------------------------------------

@pytest.fixture
def loaded(store):
    engine = Engine(store)
    engine.ingest_upload("parts", "parts.csv", b"name\nbolt\nnut\n", create=True)
    engine.ingest_upload("orders", "orders.csv", b"name\no1\n", create=True)
    return engine


def test_run_reloads_every_member(loaded, store):
    runner = GroupRunner(loaded)
    group = runner.create("Morning", ["parts", "orders"])
    before = store.last_loaded("parts")
    time.sleep(0.01)
    run_id = runner.run(group.id, wait=True)
    run = store.get_group_run(run_id)
    assert run.status == "succeeded" and run.trigger == "manual" and run.finished_at
    assert [(r["source"], r["status"], r["rows"]) for r in run.results] == [("parts", "succeeded", 2), ("orders", "succeeded", 1)]
    assert store.last_loaded("parts") > before


def test_one_failing_source_does_not_stop_the_rest(loaded, store):
    runner = GroupRunner(loaded)
    group = runner.create("Mixed", ["parts", "orders"])
    store.remove_source("parts")
    run = store.get_group_run(runner.run(group.id, wait=True))
    assert run.status == "partial"
    assert run.results[0]["status"] == "failed" and "no longer exists" in run.results[0]["error"]
    assert run.results[1]["status"] == "succeeded"


def test_group_validation(loaded):
    runner = GroupRunner(loaded)
    with pytest.raises(ValueError, match="at least one source"):
        runner.create("Empty", [])
    with pytest.raises(ValueError, match="no source named ghost"):
        runner.create("Ghost", ["parts", "ghost"])
    runner.create("Morning", ["parts", "parts"])
    with pytest.raises(ValueError, match="already exists"):
        runner.create("Morning", ["orders"])


def test_scheduler_runs_due_groups_once_and_moves_on(loaded, store):
    clock = {"now": local(2026, 10, 21, 5, 59)}
    runner = GroupRunner(loaded, clock=lambda: clock["now"])
    group = runner.create("Daily", ["parts"], schedule=DAILY_6AM)
    assert group.next_run_at == local(2026, 10, 21, 6)

    assert runner.tick() == []  # not yet
    clock["now"] = local(2026, 10, 21, 6, 0, 20)
    [run_id] = runner.tick()
    for _ in range(100):
        if store.get_group_run(run_id).status != "running":
            break
        time.sleep(0.05)
    assert store.get_group_run(run_id).status == "succeeded"
    assert store.get_group_run(run_id).trigger == "schedule"
    assert store.get_group(group.id).next_run_at == local(2026, 10, 22, 6)
    assert runner.tick() == []  # the same slot never runs twice


def test_missed_runs_catch_up_once(loaded, store):
    clock = {"now": local(2026, 10, 20, 12)}
    runner = GroupRunner(loaded, clock=lambda: clock["now"])
    group = runner.create("Daily", ["parts"], schedule=DAILY_6AM)
    clock["now"] = local(2026, 10, 23, 9)  # PC was off for three 6 AM slots
    started = runner.tick()
    assert len(started) == 1
    for _ in range(100):
        if store.get_group_run(started[0]).status != "running":
            break
        time.sleep(0.05)
    assert store.get_group_run(started[0]).trigger == "catch-up"
    assert store.get_group(group.id).next_run_at == local(2026, 10, 24, 6)


def test_disabled_groups_are_not_scheduled(loaded, store):
    runner = GroupRunner(loaded, clock=lambda: local(2026, 10, 21, 12))
    group = runner.create("Paused", ["parts"], schedule=DAILY_6AM, enabled=False)
    assert group.next_run_at is None
    group = runner.update(group.id, "Paused", ["parts"], DAILY_6AM, True)
    assert group.next_run_at == local(2026, 10, 22, 6)


def test_group_api(loaded, store):
    client = TestClient(create_app(loaded, TOKEN))
    r = client.post("/api/groups", headers=AUTH, json={"name": "Morning", "members": ["parts", "orders"], "schedule": WORKDAY_HOURLY})
    assert r.status_code == 200, r.text
    group = r.json()
    assert group["schedule_text"] == "Every hour from 8:00 AM to 5:00 PM, Monday to Friday" and group["next_run_at"]

    assert client.post("/api/groups", headers=AUTH, json={"name": "Bad", "members": ["parts"], "schedule": {"type": "weekly"}}).status_code == 400

    r = client.post(f"/api/groups/{group['id']}/run", headers=AUTH, json={})
    assert r.status_code == 202
    run_id = r.json()["run_id"]
    for _ in range(100):
        run = client.get(f"/api/group-runs/{run_id}", headers=AUTH).json()
        if run["status"] != "running":
            break
        time.sleep(0.05)
    assert run["status"] == "succeeded" and len(run["results"]) == 2

    [listed] = client.get("/api/groups", headers=AUTH).json()
    assert listed["last_run"]["id"] == run_id and listed["running"] is False

    r = client.put(f"/api/groups/{group['id']}", headers=AUTH, json={"name": "Morning", "members": ["parts"], "schedule": None})
    assert r.json()["schedule_text"] == "Not scheduled" and r.json()["next_run_at"] is None

    preview = client.post("/api/schedule/preview", headers=AUTH, json={"schedule": DAILY_6AM}).json()
    assert preview["text"] == "Every day at 6:00 AM" and len(preview["next_runs"]) == 3

    assert client.delete(f"/api/groups/{group['id']}", headers=AUTH).json() == {"deleted": group["id"]}
    assert client.get("/api/groups", headers=AUTH).json() == []
    assert client.get("/api/groups").status_code == 401
