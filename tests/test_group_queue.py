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
