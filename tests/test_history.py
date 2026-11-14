"""History tables: loads merge by the natural key, keeping every version of each row."""

import datetime as dt
import sqlite3

import pytest
from cryptography.fernet import Fernet
from fastapi.testclient import TestClient

from eyetable import keys
from eyetable.api import create_app
from eyetable.engine import Engine
from eyetable.store import FOREVER

TOKEN = "test-token-0123456789"
AUTH = {"Authorization": f"Bearer {TOKEN}"}
KEY = Fernet.generate_key().decode()


def versions(store, table="crm__data"):
    return [(r["id"], r["name"], r["ET_active"], r["ET_todt"] == FOREVER) for r in store.read_table(table)]


@pytest.fixture
def crm(tmp_path):
    path = tmp_path / "crm.db"
    conn = sqlite3.connect(path)
    conn.executescript("CREATE TABLE accounts (id INTEGER, name TEXT, city TEXT);"
                       "INSERT INTO accounts VALUES (1, 'Ada', 'London'), (2, 'Grace', 'Arlington');")
    conn.commit()
    conn.close()
    return path


def source_sql(path, sql):
    conn = sqlite3.connect(path)
    conn.executescript(sql)
    conn.commit()
    conn.close()


@pytest.fixture
def engine(store, crm):
    engine = Engine(store, environ={"EYETABLE_SECRET_KEY": KEY})
    engine.load_db_objects(f"sqlite:///{crm.as_posix()}", [{"schema": "main", "name": "accounts", "kind": "table", "source": "crm"}])
    return engine


def test_history_needs_a_natural_key(engine, store):
    with pytest.raises(keys.KeyProblem, match="natural key first"):
        keys.choose_history(store, "crm__data", True)
    keys.choose_key(store, "crm__data", ["id"])
    assert keys.choose_history(store, "crm__data", True)["history"] is True
    with pytest.raises(keys.KeyProblem, match="turn history off"):
        keys.clear_key(store, "crm__data")
    with pytest.raises(keys.KeyProblem, match="turn history off"):
        keys.choose_key(store, "crm__data", ["name"])


def test_full_loads_merge_new_and_changed_rows_and_keep_old_versions(engine, store, crm):
    keys.choose_key(store, "crm__data", ["id"])
    keys.choose_history(store, "crm__data", True)

    # turning it on: the next load makes today's rows the first versions (nothing changed yet)
    [r] = engine.reload_source("crm")
    assert r.status == "succeeded", r.error
    assert r.note == "history: 0 new, 0 changed, 2 unchanged"
    assert versions(store) == [(1, "Ada", 1, True), (2, "Grace", 1, True)]
    first = store.read_table("crm__data")[0]
    assert isinstance(first["ET_fromdt"], dt.datetime) and first["ET_todt"] == dt.datetime(9999, 12, 31)

    # a change to Ada, a new account, Grace unchanged
    source_sql(crm, "UPDATE accounts SET city = 'Paris' WHERE id = 1; INSERT INTO accounts VALUES (3, 'Linus', 'Helsinki');")
    [r] = engine.reload_source("crm")
    assert r.note == "history: 1 new, 1 changed, 1 unchanged"
    rows = store.read_table("crm__data")
    assert [(x["id"], x["city"], x["ET_active"]) for x in rows] == [
        (1, "London", 0), (2, "Arlington", 1), (1, "Paris", 1), (3, "Helsinki", 1)]
    old, new = rows[0], rows[2]
    assert old["ET_todt"] == new["ET_fromdt"] and old["ET_todt"] != FOREVER and new["ET_todt"] == FOREVER
    assert old["ET_fromdt"] < old["ET_todt"] or old["ET_fromdt"] == old["ET_todt"]

    # nothing changed: nothing added; a row gone from the source is left as it is
    source_sql(crm, "DELETE FROM accounts WHERE id = 2;")
    [r] = engine.reload_source("crm")
    assert r.note == "history: 0 new, 0 changed, 2 unchanged"
    assert len(store.read_table("crm__data")) == 4

    # the natural key still holds on the current versions, and a scan sees only those
    info = keys.key_info(store, "crm__data")
    assert info["key_check"]["ok"] and info["history"]
    assert keys.scan(store, "crm__data")["rows"] == 3


def test_the_first_load_of_a_history_table_makes_every_row_current(engine, store):
    keys.choose_key(store, "crm__data", ["id"])
    keys.choose_history(store, "crm__data", True)
    store.drop_table("crm__data")  # dropping a table forgets its settings: set them again for a fresh one
    store.save_table_key("crm__data", ["id"])
    store.set_history("crm__data", True)
    [r] = engine.reload_source("crm")
    assert r.note == "history: 2 new, 0 changed, 0 unchanged"
    assert versions(store) == [(1, "Ada", 1, True), (2, "Grace", 1, True)]


def test_a_load_with_repeated_or_missing_keys_changes_nothing(engine, store, crm):
    keys.choose_key(store, "crm__data", ["id"])
    keys.choose_history(store, "crm__data", True)
    engine.reload_source("crm")
    source_sql(crm, "INSERT INTO accounts VALUES (1, 'Ada again', 'Rome'), (NULL, 'nobody', NULL);")
    [r] = engine.reload_source("crm")
    assert r.status == "failed" and "one row per id" in r.error and "1 value more than once" in r.error
    assert versions(store) == [(1, "Ada", 1, True), (2, "Grace", 1, True)]
    assert not store.table_exists("crm__data__incoming")


def test_incremental_loads_merge_too(engine, store, crm):
    source_sql(crm, "ALTER TABLE accounts ADD COLUMN version INTEGER; UPDATE accounts SET version = 1;")
    engine.reload_source("crm")
    keys.choose_key(store, "crm__data", ["id"])
    store.save_increment("crm__data", "version")
    keys.choose_history(store, "crm__data", True)
    engine.reload_source("crm")  # becomes the first versions
    source_sql(crm, "UPDATE accounts SET city = 'Paris', version = 2 WHERE id = 1;")
    [r] = engine.reload_source("crm", incremental=True)
    assert (r.status, r.mode) == ("succeeded", "incremental")
    assert [(x["id"], x["city"], x["ET_active"]) for x in store.read_table("crm__data")] == [
        (1, "London", 0), (2, "Arlington", 1), (1, "Paris", 1)]  # the old version kept, not deleted


def test_turning_history_off_lets_the_next_full_load_replace_the_table(engine, store, crm):
    keys.choose_key(store, "crm__data", ["id"])
    keys.choose_history(store, "crm__data", True)
    engine.reload_source("crm")
    source_sql(crm, "UPDATE accounts SET city = 'Paris' WHERE id = 1;")
    engine.reload_source("crm")
    assert len(store.read_table("crm__data")) == 3
    keys.choose_history(store, "crm__data", False)
    engine.reload_source("crm")
    rows = store.read_table("crm__data")
    assert len(rows) == 2 and "ET_active" not in rows[0]


def test_the_api_and_the_source_flags(engine, store):
    client = TestClient(create_app(engine, TOKEN))
    assert client.put("/api/tables/crm__data/history", headers=AUTH, json={"on": True}).status_code == 400
    client.put("/api/tables/crm__data/key", headers=AUTH, json={"columns": ["id"]})
    r = client.put("/api/tables/crm__data/history", headers=AUTH, json={"on": True})
    assert r.status_code == 200 and r.json()["history"] is True
    assert client.delete("/api/tables/crm__data/key", headers=AUTH).status_code == 400
    [t] = client.get("/api/sources", headers=AUTH).json()[0]["tables"]  # a database source: per table
    assert (t["key"], t["history"], t["incremental"]) == (True, True, None)
    assert client.put("/api/tables/nope__data/history", headers=AUTH, json={"on": True}).status_code == 404
