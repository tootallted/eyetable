"""Hidden ("commented out") tables: kept out of Sources' list and skipped by every load."""

import sqlite3

import pytest
from cryptography.fernet import Fernet
from fastapi.testclient import TestClient

from eyetable.api import create_app
from eyetable.engine import Engine
from eyetable.groups import GroupRunner
from eyetable.loadqueue import LoadQueue

TOKEN = "test-token-0123456789"
AUTH = {"Authorization": f"Bearer {TOKEN}"}
KEY = Fernet.generate_key().decode()


@pytest.fixture
def engine(store, tmp_path):
    path = tmp_path / "crm.db"
    conn = sqlite3.connect(path)
    conn.executescript("""
        CREATE TABLE accounts (id INTEGER); INSERT INTO accounts VALUES (1), (2);
        CREATE TABLE notes (body TEXT); INSERT INTO notes VALUES ('hi');
    """)
    conn.commit()
    conn.close()
    engine = Engine(store, environ={"EYETABLE_SECRET_KEY": KEY})
    engine.load_db_objects(f"sqlite:///{path.as_posix()}", [{"schema": "main", "name": "accounts", "kind": "table", "source": "crm"}])
    engine.set_db_tables("crm", [{"schema": "main", "name": t, "kind": "table"} for t in ("accounts", "notes")])
    engine.ingest_upload("parts", "p.csv", b"n\n1\n", create=True)
    return engine


def test_a_hidden_table_is_skipped_and_the_rest_of_its_source_loads(engine, store):
    store.set_hidden("crm__notes", True)
    results = engine.reload_source("crm")
    assert [(r.table, r.status) for r in results] == [("crm__notes", "skipped"), ("crm__data", "succeeded")]
    assert results[0].error == "hidden in Sources"
    store.set_hidden("crm__data", True)
    assert [r.status for r in engine.reload_source("crm")] == ["skipped", "skipped"]
    store.set_hidden("parts__data", True)
    [r] = engine.reload_source("parts")
    assert (r.status, r.rows) == ("skipped", 0)
    assert store.hidden() == {"crm__notes", "crm__data", "parts__data"}
    store.set_hidden("parts__data", False)
    assert engine.reload_source("parts")[0].status == "succeeded"


def test_scheduled_and_group_runs_skip_hidden_sources_without_failing(engine, store):
    store.set_hidden("parts__data", True)
    runner = GroupRunner(engine)
    group = runner.create("Nightly", ["crm", "parts"])
    run = store.get_group_run(runner.run(group.id, wait=True))
    by_source = {r["source"]: r for r in run.results}
    assert by_source["parts"]["status"] == "skipped" and "hidden" in by_source["parts"]["error"]
    assert by_source["crm"]["status"] == "succeeded"
    assert run.status == "succeeded"  # skipping a hidden source isn't a failure


def test_the_queue_leaves_hidden_tables_out(engine, store):
    store.set_hidden("crm__notes", True)
    queue = LoadQueue(engine)
    done = queue.wait(queue.add("crm")[0]["id"], 30)
    assert done["status"] == "succeeded" and [t["table"] for t in done["tables"]] == ["crm__data"]
    with pytest.raises(ValueError, match="hidden"):
        queue.add("crm", table="crm__notes")


def test_hiding_through_the_api(engine, store):
    client = TestClient(create_app(engine, TOKEN))
    r = client.put("/api/tables/crm__notes/hidden", headers=AUTH, json={"hidden": True})
    assert r.json() == {"table": "crm__notes", "source": "crm", "hidden": True}
    sources = {s["name"]: s for s in client.get("/api/sources", headers=AUTH).json()}
    assert [t["hidden"] for t in sources["crm"]["tables"]] == [False, True]
    assert sources["parts"]["hidden"] is False
    client.put("/api/tables/parts__data/hidden", headers=AUTH, json={"hidden": True})
    assert {s["name"]: s for s in client.get("/api/sources", headers=AUTH).json()}["parts"]["hidden"] is True
    assert client.put("/api/tables/nope__data/hidden", headers=AUTH, json={"hidden": True}).status_code == 404
    # a reload of a source whose tables are all hidden loads nothing, and isn't an error
    client.put("/api/tables/crm__data/hidden", headers=AUTH, json={"hidden": True})
    body = client.post("/api/sources/crm/reload", headers=AUTH, json={}).json()
    assert [x["status"] for x in body["results"]] == ["skipped", "skipped"]
    assert client.post("/api/queue", headers=AUTH, json={"source": "crm"}).status_code == 400
    client.put("/api/tables/crm__data/hidden", headers=AUTH, json={"hidden": False})
    assert store.hidden() == {"crm__notes", "parts__data"}
