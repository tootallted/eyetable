"""A database source that loads several chosen tables and views, each into its own table."""

import sqlite3
import time

import pytest
from cryptography.fernet import Fernet
from fastapi.testclient import TestClient

from eyetable import dbconnect
from eyetable.api import create_app
from eyetable.engine import Engine
from eyetable.groups import GroupRunner

TOKEN = "test-token-0123456789"
AUTH = {"Authorization": f"Bearer {TOKEN}"}
KEY = Fernet.generate_key().decode()


@pytest.fixture
def crm(tmp_path):
    path = tmp_path / "crm.db"
    conn = sqlite3.connect(path)
    conn.executescript("""
        CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT);
        CREATE TABLE orders (id INTEGER, customer_id INTEGER, total REAL);
        CREATE TABLE products (id INTEGER, name TEXT);
        CREATE TABLE regions (id INTEGER, name TEXT);
        CREATE VIEW big_orders AS SELECT * FROM orders WHERE total > 50;
        INSERT INTO customers VALUES (1, 'Ada'), (2, 'Grace');
        INSERT INTO orders VALUES (1, 1, 99.5), (2, 2, 10), (3, 1, 75);
        INSERT INTO products VALUES (1, 'Widget');
        INSERT INTO regions VALUES (1, 'West'), (2, 'East'), (3, 'North'), (4, 'South');
    """)
    conn.commit()
    conn.close()
    return f"sqlite:///{path.as_posix()}"


def pick(*names, kind="table"):
    return [{"schema": "main", "name": n, "kind": "view" if n == "big_orders" else kind} for n in names]


@pytest.fixture
def engine(store, crm):
    engine = Engine(store, environ={"EYETABLE_SECRET_KEY": KEY})
    engine.load_db_objects(crm, [{"schema": "main", "name": "customers", "kind": "table", "source": "crm"}])
    return engine


def test_a_source_made_with_one_table_reads_as_before(store, engine):
    config = store.get_source("crm").config
    assert "tables" not in config
    assert dbconnect.db_tables(config) == [{"key": "data", "schema": "main", "table": "customers", "kind": "table", "name": "data"}]
    assert [r.rows for r in engine.reload_source("crm")] == [2]


def test_choosing_more_tables_loads_them_and_keeps_the_first(store, engine):
    done = engine.set_db_tables("crm", pick("customers", "orders", "big_orders"))
    assert done["added"] == ["main.orders", "main.big_orders"] and done["removed"] == []
    assert [(r.dataset_key, r.status, r.rows) for r in done["results"]] == [
        ("main.orders", "succeeded", 3), ("main.big_orders", "succeeded", 2)]
    assert len(store.read_table("crm__data")) == 2  # the first table kept its place
    assert [r["total"] for r in store.read_table("crm__orders")] == [99.5, 10, 75]
    assert len(store.read_table("crm__big_orders")) == 2
    assert {d.key for _, d in store.list_datasets("crm")} == {"data", "main.orders", "main.big_orders"}

    results = engine.reload_source("crm")  # a reload (and so a group run) loads them all
    assert sorted((r.dataset_key, r.rows) for r in results) == [("data", 2), ("main.big_orders", 2), ("main.orders", 3)]
    run = GroupRunner(engine)
    group = run.create("CRM", ["crm"])
    assert store.get_group_run(run.run(group.id, wait=True)).results[0]["rows"] == 7


def test_unchosen_tables_are_taken_out_and_deleted(store, engine):
    engine.set_db_tables("crm", pick("customers", "orders", "products"))
    done = engine.set_db_tables("crm", pick("orders", "regions"))
    assert done["removed"] == ["data", "main.products"] and done["added"] == ["main.regions"]
    assert not store.table_exists("crm__data") and not store.table_exists("crm__products")
    assert len(store.read_table("crm__orders")) == 3 and len(store.read_table("crm__regions")) == 4
    assert [r.dataset_key for r in engine.reload_source("crm")] == ["main.orders", "main.regions"]
    with pytest.raises(ValueError, match="at least one"):
        engine.set_db_tables("crm", [])


def test_a_missing_table_changes_nothing(store, engine):
    before = store.get_source("crm").config
    with pytest.raises(ValueError, match="wasn't found"):
        engine.set_db_tables("crm", pick("customers", "nope"))
    assert store.get_source("crm").config == before and not store.table_exists("crm__nope")


def test_names_stay_unique(store):
    current = [{"key": "data", "schema": "dbo", "table": "people", "kind": "table", "name": "data"}]
    items = dbconnect.choose_tables(current, [
        {"schema": "dbo", "name": "people"}, {"schema": "sales", "name": "people"}, {"schema": "dbo", "name": "Data"},
        {"schema": "hr", "name": "people"}, {"schema": "x", "name": "sales_people"}])
    assert [i["name"] for i in items] == ["data", "people", "dbo_data", "hr_people", "sales_people"]
    again = dbconnect.choose_tables(items, [{"schema": "y", "name": "hr_people"}])
    assert [i["name"] for i in again] == ["hr_people"]  # free again once its owner is gone


def test_settings_with_several_tables(store, engine, crm):
    engine.set_db_tables("crm", pick("customers", "orders"))
    with pytest.raises(ValueError, match="several tables"):
        engine.update_db_source("crm", table="products", test=False)
    engine.update_db_source("crm", connection=crm)  # a new connection keeps the chosen tables
    assert len(dbconnect.db_tables(store.get_source("crm").config)) == 2


def test_endpoints(store, engine):
    web = TestClient(create_app(engine, TOKEN))
    objects = web.get("/api/sources/crm/objects", headers=AUTH).json()["objects"]
    assert {o["name"]: o["chosen"] for o in objects} == {
        "big_orders": False, "customers": True, "orders": False, "products": False, "regions": False}

    job = web.put("/api/sources/crm/tables", headers=AUTH, json={"tables": pick("customers", "orders", "products", "regions")}).json()["job_id"]
    for _ in range(100):
        state = web.get(f"/api/jobs/{job}", headers=AUTH).json()
        if state["status"] != "running":
            break
        time.sleep(0.05)
    assert state["status"] == "succeeded", state["error"]
    assert state["result"]["added"] == ["main.orders", "main.products", "main.regions"] and state["result"]["rows"] == 8

    summary = {s["name"]: s for s in web.get("/api/sources", headers=AUTH).json()}["crm"]
    assert [(t["name"], t["table"], t["rows"]) for t in summary["tables"]] == [
        ("customers", "crm__data", 2), ("orders", "crm__orders", 3), ("products", "crm__products", 1), ("regions", "crm__regions", 4)]
    assert summary["rows"] == 10 and all(t["last_loaded_at"] for t in summary["tables"])

    p = web.get("/api/sources/crm/preview?table=crm__regions", headers=AUTH).json()
    assert len(p["rows"]) == 4
    assert web.get("/api/sources/crm/preview?table=elsewhere", headers=AUTH).status_code == 404
    schema = {s["name"]: s for s in web.get("/api/schema", headers=AUTH).json()["sources"]}["crm"]
    assert schema["table"] == "crm__data" and schema["tables"] == ["crm__data", "crm__orders", "crm__products", "crm__regions"]
    q = web.post("/api/query", headers=AUTH, json={"sql": "SELECT count(*) FROM crm"}).json()
    assert q["rows"] == [[2]]

    assert web.put("/api/sources/crm/tables", headers=AUTH, json={"tables": []}).status_code == 400
    engine.ingest_upload("plain", "p.csv", b"a\n1\n", create=True)
    assert web.get("/api/sources/plain/objects", headers=AUTH).status_code == 400
    assert web.get("/api/sources/crm/objects").status_code == 401
