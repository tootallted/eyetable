"""Every data table has a <table>_current view: a history table's current rows, any other table's rows."""

import sqlalchemy as sa
from fastapi.testclient import TestClient

from eyetable import keys
from eyetable.api import create_app
from eyetable.engine import Engine

TOKEN = "test-token-0123456789"
AUTH = {"Authorization": f"Bearer {TOKEN}"}


def view_rows(store, view):
    with store.db.connect() as conn:
        return [dict(r._mapping) for r in conn.execute(sa.text(f"SELECT * FROM {store.qualified(view)}")).all()]


def views(store):
    return set(sa.inspect(store.db).get_view_names())


def test_a_loaded_table_gets_a_view_of_all_its_rows(store):
    engine = Engine(store)
    engine.ingest_upload("parts", "p.csv", b"id,name\n1,bolt\n2,nut\n", create=True)
    assert views(store) == {"parts__data_current"}
    assert [(r["id"], r["name"]) for r in view_rows(store, "parts__data_current")] == [(1, "bolt"), (2, "nut")]
    engine.ingest_upload("parts", "p.csv", b"id,name,colour\n3,washer,red\n", mode="replace")  # a new column
    assert [(r["id"], r["colour"]) for r in view_rows(store, "parts__data_current")] == [(3, "red")]


def test_a_history_table_view_shows_only_current_versions(store):
    engine = Engine(store)
    engine.ingest_upload("parts", "p.csv", b"id,name\n1,bolt\n2,nut\n", create=True)
    keys.choose_key(store, "parts__data", ["id"])
    engine.ingest_upload("parts", "p.csv", b"id,name\n1,bolt\n2,nut\n", mode="replace")
    engine.ingest_upload("parts", "p.csv", b"id,name\n1,BOLT\n3,washer\n", mode="replace")
    assert len(store.read_table("parts__data")) == 4  # bolt (closed), BOLT, nut (missing from the load: kept), washer
    current = view_rows(store, "parts__data_current")
    assert sorted((r["id"], r["name"]) for r in current) == [(1, "BOLT"), (2, "nut"), (3, "washer")]
    assert all(r["ET_isCurrent"] == 1 for r in current)
    keys.clear_key(store, "parts__data")
    engine.ingest_upload("parts", "p.csv", b"id,name\n9,pin\n", mode="replace")  # replaced: no history any more
    assert [(r["id"], r["name"]) for r in view_rows(store, "parts__data_current")] == [(9, "pin")]


def test_a_failed_load_keeps_the_view(store):
    engine = Engine(store)
    engine.ingest_upload("parts", "p.csv", b"id,name\n1,bolt\n", create=True)
    keys.choose_key(store, "parts__data", ["id"])
    r = engine.ingest_upload("parts", "p.csv", b"id,name\n1,a\n1,b\n", mode="replace")  # repeats the key: refused
    assert r.status == "failed"
    assert [(r["id"], r["name"]) for r in view_rows(store, "parts__data_current")] == [(1, "bolt")]


def test_dropping_or_renaming_a_table_takes_its_view(store):
    engine = Engine(store)
    engine.ingest_upload("parts", "p.csv", b"id\n1\n", create=True)
    engine.rename_source("parts", "widgets")
    assert "parts__data_current" not in views(store)  # the old table is archived without a view
    engine.reload_source("widgets")
    assert "widgets__data_current" in views(store)
    store.drop_table("widgets__data")
    assert "widgets__data_current" not in views(store)


def test_startup_makes_views_for_tables_that_have_none(store):
    engine = Engine(store)
    engine.ingest_upload("parts", "p.csv", b"id\n1\n", create=True)
    engine.ingest_upload("orders", "o.csv", b"id\n7\n", create=True)
    store.drop_current_view("parts__data")
    store.drop_current_view("orders__data")
    assert sorted(store.refresh_current_views()) == ["orders__data_current", "parts__data_current"]
    assert sorted(store.refresh_current_views()) == ["orders__data_current", "parts__data_current"]  # again: remade
    assert views(store) == {"orders__data_current", "parts__data_current"}


def test_long_names_and_work_tables(store):
    assert store.current_view("x" * 60) == "x" * 55 + "_current"
    assert store.is_work_table("parts__data__stg") and store.is_work_table("parts__data__incoming")
    assert store.refresh_current_view("parts__data__stg") is None and store.refresh_current_view("nope") is None


def test_the_query_window_lists_and_reads_the_views(store):
    engine = Engine(store)
    engine.ingest_upload("parts", "p.csv", b"id,name\n1,bolt\n", create=True)
    client = TestClient(create_app(engine, TOKEN))
    tables = client.get("/api/schema", headers=AUTH).json()["tables"]
    [view] = [t for t in tables if t["table"] == "parts__data_current"]
    assert view["view_of"] == "parts__data" and "name" in [c["name"] for c in view["columns"]]
    r = client.post("/api/query", headers=AUTH, json={"sql": "SELECT name FROM parts__data_current"})
    assert r.status_code == 200, r.text
    assert r.json()["rows"] == [["bolt"]]
