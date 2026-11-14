"""Group, tag and scheduled runs load their sources through the load queue, so the page shows them."""

import datetime as dt

from fastapi.testclient import TestClient

from eyetable.api import create_app
from eyetable.engine import Engine
from eyetable.groups import GroupRunner
from eyetable.loadqueue import LoadQueue

TOKEN = "test-token-0123456789"
AUTH = {"Authorization": f"Bearer {TOKEN}"}


def loaded(store):
    engine = Engine(store)
    engine.ingest_upload("parts", "parts.csv", b"name\nbolt\nnut\n", create=True)
    engine.ingest_upload("orders", "orders.csv", b"name\no1\n", create=True)
    return engine


def test_a_group_run_shows_each_member_in_the_queue(store):
    engine = loaded(store)
    runner = GroupRunner(engine)
    runner.queue = LoadQueue(engine)
    group = runner.create("Nightly", ["parts", "orders"])
    run = store.get_group_run(runner.run(group.id, wait=True))
    assert run.status == "succeeded"
    assert [(r["source"], r["status"], r["rows"]) for r in run.results] == [("parts", "succeeded", 2), ("orders", "succeeded", 1)]
    items = runner.queue.list()  # newest first
    assert [(i["source"], i["status"], i["by"]) for i in items] == [
        ("orders", "succeeded", "Group Nightly · Run now"), ("parts", "succeeded", "Group Nightly · Run now")]
    assert items[0]["log_count"] > 0  # with its queries and log, like any queued load


def test_scheduled_and_tag_runs_say_so_and_failures_and_hidden_sources_are_reported(store):
    engine = loaded(store)
    store.set_hidden("orders__data", True)
    store.set_source_tags("parts", ["nightly"])
    now = dt.datetime(2026, 11, 13, 6, 0, tzinfo=dt.timezone.utc)
    runner = GroupRunner(engine, clock=lambda: now)
    runner.queue = LoadQueue(engine)
    engine.ingest_upload("gone", "g.csv", b"x\n1\n", create=True)
    group = runner.create("Morning", ["parts", "orders", "gone"], schedule={
        "type": "daily", "times": ["06:00"], "days": [0, 1, 2, 3, 4, 5, 6], "timezone": "UTC"})
    store.remove_source("gone")  # removed after the group was made
    store.update_group(group.id, next_run_at=now - dt.timedelta(minutes=1))
    [run_id] = runner.tick()
    for _ in range(200):
        run = store.get_group_run(run_id)
        if run.status != "running":
            break
        import time
        time.sleep(0.05)
    by_source = {r["source"]: r for r in run.results}
    assert by_source["parts"]["status"] == "succeeded"
    assert by_source["orders"]["status"] == "skipped"  # hidden: not queued at all
    assert by_source["gone"]["status"] == "failed"
    assert [i["by"] for i in runner.queue.list()] == ["Group Morning · scheduled"]
    run = store.get_group_run(runner.run_tag("nightly", wait=True))
    assert run.status == "succeeded" and runner.queue.list()[0]["by"] == "Tag #nightly · Run now"


def test_the_api_wires_group_runs_into_its_queue(store):
    engine = loaded(store)
    client = TestClient(create_app(engine, TOKEN))
    gid = client.post("/api/groups", headers=AUTH, json={"name": "Both", "members": ["parts", "orders"]}).json()["id"]
    run_id = client.post(f"/api/groups/{gid}/run", headers=AUTH, json={}).json()["run_id"]
    runner = client.app.state.group_runner
    for _ in range(200):
        if client.get(f"/api/group-runs/{run_id}", headers=AUTH).json()["status"] != "running":
            break
        import time
        time.sleep(0.05)
    items = client.get("/api/queue", headers=AUTH).json()["items"]
    assert runner.queue is client.app.state.load_queue
    assert sorted(i["source"] for i in items) == ["orders", "parts"] and all(i["by"] == "Group Both · Run now" for i in items)


def test_incremental_group_runs_load_incrementally_where_set_up_and_in_full_elsewhere(store, tmp_path):
    import sqlite3

    from cryptography.fernet import Fernet

    path = tmp_path / "crm.db"
    conn = sqlite3.connect(path)
    conn.executescript("CREATE TABLE accounts (id INTEGER); INSERT INTO accounts VALUES (1), (2);"
                       "CREATE TABLE notes (b TEXT); INSERT INTO notes VALUES ('x');")
    conn.commit()
    conn.close()
    engine = Engine(store, environ={"EYETABLE_SECRET_KEY": Fernet.generate_key().decode()})
    engine.load_db_objects(f"sqlite:///{path.as_posix()}", [{"schema": "main", "name": "accounts", "kind": "table", "source": "crm"}])
    engine.set_db_tables("crm", [{"schema": "main", "name": t, "kind": "table"} for t in ("accounts", "notes")])
    engine.ingest_upload("parts", "parts.csv", b"name\nbolt\n", create=True)
    store.save_increment("crm__data", "id")
    conn = sqlite3.connect(path)
    conn.execute("INSERT INTO accounts VALUES (3)")
    conn.commit()
    conn.close()

    runner = GroupRunner(engine)
    runner.queue = LoadQueue(engine)
    group = runner.create("Both", ["crm", "parts"])
    run = store.get_group_run(runner.run(group.id, wait=True, incremental=True))
    assert run.status == "succeeded" and {r["source"]: r["load"] for r in run.results} == {"crm": "incremental", "parts": "full"}
    crm = next(i for i in runner.queue.list() if i["source"] == "crm")
    modes = {t["table"]: (t["mode"], t["rows"], t["expected"]) for t in crm["tables"]}
    assert modes == {"crm__data": ("incremental", 1, None), "crm__notes": ("replace", 1, 1)}  # 1 new account; notes in full
    assert crm["by"] == "Group Both \u00b7 Run now, incremental"
    assert store.count_rows("crm__data") == 3

    # a full run loads everything in full
    run = store.get_group_run(runner.run(group.id, wait=True, incremental=False))
    crm = runner.queue.list()[1]
    assert [t["mode"] for t in crm["tables"]] == ["replace", "replace"] and store.count_rows("crm__data") == 3


def test_a_schedule_says_whether_its_runs_are_incremental(store):
    from eyetable.schedule import describe, normalize

    sched = {"type": "daily", "times": ["06:00"], "days": [0, 1, 2, 3, 4, 5, 6], "timezone": "UTC"}
    assert normalize(sched)["load"] == "full"
    assert normalize({**sched, "load": "incremental"})["load"] == "incremental"
    assert describe({**sched, "load": "incremental"}) == "Every day at 6:00 AM, incremental where set up"
    import pytest

    from eyetable.schedule import ScheduleError
    with pytest.raises(ScheduleError):
        normalize({**sched, "load": "sometimes"})
    engine = loaded(store)
    now = dt.datetime(2026, 11, 13, 6, 0, tzinfo=dt.timezone.utc)
    runner = GroupRunner(engine, clock=lambda: now)
    runner.queue = LoadQueue(engine)
    group = runner.create("Morning", ["parts"], schedule={**sched, "load": "incremental"})
    store.update_group(group.id, next_run_at=now - dt.timedelta(minutes=1))
    [run_id] = runner.tick()
    import time
    for _ in range(200):
        if store.get_group_run(run_id).status != "running":
            break
        time.sleep(0.05)
    assert runner.queue.list()[0]["by"] == "Group Morning \u00b7 scheduled, incremental"
    assert store.get_group_run(run_id).results[0]["load"] == "full"  # a file source has no incremental load
