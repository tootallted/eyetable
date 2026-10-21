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
        {"tag": "base", "sources": ["customers", "orders"]},
        {"tag": "crm", "sources": ["customers"]},
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
