import time

import pytest
from fastapi.testclient import TestClient

from eyetable.api import create_app
from eyetable.engine import Engine
from eyetable.groups import GroupRunner
from eyetable.identifiers import normalize_tag

TOKEN = "test-token-0123456789"
AUTH = {"Authorization": f"Bearer {TOKEN}"}


@pytest.fixture
def engine(store):
    engine = Engine(store)
    for name in ("customers", "orders", "notes"):
        engine.ingest_upload(name, f"{name}.csv", b"id\n1\n2\n", create=True)
    return engine


def test_tag_spelling():
    assert normalize_tag("#Base") == normalize_tag(" base ") == normalize_tag("BASE") == "base"
    assert normalize_tag("#finance-2026") == "finance-2026"
    for bad in ("#", "# ", "#two words", "#a/b", "#" + "x" * 41, "#-lead"):
        with pytest.raises(ValueError):
            normalize_tag(bad)


def test_tags_are_stored_per_source(store, engine):
    store.set_source_tags("customers", ["base", "crm"])
    store.set_source_tags("orders", ["base"])
    assert store.tags_by_source() == {"customers": ["base", "crm"], "orders": ["base"]}
    assert store.sources_with_tag("base") == ["customers", "orders"]
    assert store.all_tags() == [("base", 2), ("crm", 1)]
    store.set_source_tags("customers", ["crm"])  # replaces
    assert store.sources_with_tag("base") == ["orders"]
    store.remove_source("orders")
    assert store.sources_with_tag("base") == []


def test_tag_group_picks_up_sources_tagged_later(store, engine):
    runner = GroupRunner(engine)
    store.set_source_tags("customers", ["base"])
    group = runner.create("Base load", ["#Base"])
    assert group.members == ["#base"]
    assert runner.summary(group)["sources"] == ["customers"]

    store.set_source_tags("orders", ["base"])  # tagged after the group was made
    run = store.get_group_run(runner.run(group.id, wait=True))
    assert run.status == "succeeded"
    assert [r["source"] for r in run.results] == ["customers", "orders"]


def test_tags_and_names_mix_without_duplicates(store, engine):
    runner = GroupRunner(engine)
    store.set_source_tags("customers", ["base"])
    store.set_source_tags("orders", ["base"])
    group = runner.create("Mixed", ["orders", "#base", "notes", "#nobody"])
    summary = runner.summary(group)
    assert summary["sources"] == ["orders", "customers", "notes"]
    assert summary["empty_tags"] == ["#nobody"]
    run = store.get_group_run(runner.run(group.id, wait=True))
    assert run.status == "partial"
    assert run.results[0] == {"source": "#nobody", "status": "failed", "rows": 0, "error": "no source is tagged #nobody"}
    assert [r["source"] for r in run.results[1:]] == ["orders", "customers", "notes"]


def test_group_validation_with_tags(engine):
    runner = GroupRunner(engine)
    with pytest.raises(ValueError, match="valid tag"):
        runner.create("Bad", ["#not ok"])
    with pytest.raises(ValueError, match="no source named ghost"):
        runner.create("Ghost", ["#base", "ghost"])


def test_tag_api(store, engine):
    client = TestClient(create_app(engine, TOKEN))
    r = client.put("/api/sources/customers/tags", headers=AUTH, json={"tags": ["#Base", "crm", "base"]})
    assert r.status_code == 200 and r.json()["tags"] == ["base", "crm"]
    client.put("/api/sources/orders/tags", headers=AUTH, json={"tags": ["base"]})
    assert client.put("/api/sources/orders/tags", headers=AUTH, json={"tags": ["no good"]}).status_code == 400
    assert client.put("/api/sources/ghost/tags", headers=AUTH, json={"tags": ["x"]}).status_code == 404

    summary = {s["name"]: s["tags"] for s in client.get("/api/sources", headers=AUTH).json()}
    assert summary == {"customers": ["base", "crm"], "notes": [], "orders": ["base"]}
    assert client.get("/api/tags", headers=AUTH).json() == [
        {"tag": "base", "sources": ["customers", "orders"], "running": False, "last_run": None},
        {"tag": "crm", "sources": ["customers"], "running": False, "last_run": None},
    ]

    group = client.post("/api/groups", headers=AUTH, json={"name": "#base", "members": ["#base"]}).json()
    assert group["members"] == ["#base"] and group["sources"] == ["customers", "orders"]
    run_id = client.post(f"/api/groups/{group['id']}/run", headers=AUTH, json={}).json()["run_id"]
    for _ in range(100):
        run = client.get(f"/api/group-runs/{run_id}", headers=AUTH).json()
        if run["status"] != "running":
            break
        time.sleep(0.05)
    assert run["status"] == "succeeded" and [r["source"] for r in run["results"]] == ["customers", "orders"]
    assert client.put("/api/sources/customers/tags", json={"tags": []}).status_code == 401


# -- running a tag without a group ---------------------------------------------------

def test_a_tag_runs_whatever_has_it_now(store, engine):
    runner = GroupRunner(engine)
    store.set_source_tags("customers", ["base"])
    first = store.get_group_run(runner.run_tag("#Base", wait=True))
    assert first.status == "succeeded" and first.trigger == "tag"
    assert [(r["source"], r["rows"]) for r in first.results] == [("customers", 2)]

    store.set_source_tags("orders", ["base"])
    second = store.get_group_run(runner.run_tag("base", wait=True))
    assert [r["source"] for r in second.results] == ["customers", "orders"]
    assert store.last_tag_runs()["base"].id == second.id  # only the latest is kept per tag
    assert not store.list_groups()  # no group was made
    assert not runner.is_running("#base")

    with pytest.raises(LookupError, match="no source is tagged #nobody"):
        runner.run_tag("nobody")


def test_one_failing_source_doesnt_stop_a_tag(store, engine):
    store.set_source_tags("customers", ["base"])
    store.set_source_tags("orders", ["base"])
    original = engine.reload_source

    def flaky(name, **kwargs):
        if name == "customers":
            raise RuntimeError("the file is gone")
        return original(name, **kwargs)

    engine.reload_source = flaky
    run = store.get_group_run(GroupRunner(engine).run_tag("base", wait=True))
    assert run.status == "partial"
    assert [(r["source"], r["status"]) for r in run.results] == [("customers", "failed"), ("orders", "succeeded")]


def test_a_tag_cant_run_twice_at_once(store, engine):
    runner = GroupRunner(engine)
    store.set_source_tags("customers", ["base"])
    runner._active.add("#base")
    with pytest.raises(ValueError, match="#base is already running"):
        runner.run_tag("base")


def test_tag_run_api(store, engine):
    client = TestClient(create_app(engine, TOKEN))
    client.put("/api/sources/customers/tags", headers=AUTH, json={"tags": ["base"]})
    client.put("/api/sources/orders/tags", headers=AUTH, json={"tags": ["base"]})

    r = client.post("/api/tags/base/run", headers=AUTH, json={})
    assert r.status_code == 202
    for _ in range(100):
        run = client.get(f"/api/group-runs/{r.json()['run_id']}", headers=AUTH).json()
        if run["status"] != "running":
            break
        time.sleep(0.05)
    assert run["status"] == "succeeded" and [x["source"] for x in run["results"]] == ["customers", "orders"]

    for _ in range(100):  # the worker lets go of the tag just after the run is recorded
        [tag] = client.get("/api/tags", headers=AUTH).json()
        if not tag["running"]:
            break
        time.sleep(0.05)
    assert tag["running"] is False and tag["last_run"]["id"] == run["id"] and tag["last_run"]["status"] == "succeeded"
    assert client.get("/api/groups", headers=AUTH).json() == []

    assert client.post("/api/tags/nobody/run", headers=AUTH, json={}).status_code == 404
    assert client.post("/api/tags/no%20good/run", headers=AUTH, json={}).status_code == 400
    assert client.post("/api/tags/base/run", json={}).status_code == 401


def test_sources_say_where_they_come_from(store, engine):
    store.save_source("crm_a", "dbtable", {"url": "sqlite://", "schema": "main", "table": "a", "kind": "table", "label": "DB1.local/crm"})
    store.save_source("crm_b", "dbtable", {"url": "sqlite://", "schema": "main", "table": "b", "kind": "table", "label": "db1.local/crm"})
    store.save_source("sheet_tab", "gsheet", {"spreadsheet_id": "abc123", "file_title": "Budget", "url": "https://docs.google.com/x"})
    from eyetable import curl
    store.save_source("api_jobs", "curl", curl.prepare("curl 'https://api.test/jobs?day=1'", None)[1])
    web = TestClient(create_app(engine, TOKEN))
    got = {s["name"]: s["connection"] for s in web.get("/api/sources", headers=AUTH).json()}
    assert got["crm_a"]["key"] == got["crm_b"]["key"] == "db:db1.local/crm" and got["crm_a"]["label"] == "DB1.local/crm"
    assert got["sheet_tab"] == {"key": "sheet:abc123", "label": "Budget"}
    assert got["api_jobs"] == {"key": "api:api.test", "label": "api.test"}
    assert got["customers"] is None  # a file comes from nowhere to group by
