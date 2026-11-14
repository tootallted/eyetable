"""Renaming a source and its table: the old table is archived with a timestamp; the next full load makes the new one."""

import sqlite3

import pytest
import sqlalchemy as sa
from cryptography.fernet import Fernet
from fastapi.testclient import TestClient

from eyetable import keys, targets
from eyetable.api import create_app
from eyetable.engine import Engine
from eyetable.loadqueue import LoadQueue

TOKEN = "test-token-0123456789"
AUTH = {"Authorization": f"Bearer {TOKEN}"}
KEY = Fernet.generate_key().decode()


def tables(store):
    return sorted(sa.inspect(store.db).get_table_names())


@pytest.fixture
def engine(store):
    engine = Engine(store)
    engine.ingest_upload("ledger", "l.csv", b"id,amount\n1,10\n2,20\n", create=True)
    return engine


def test_renaming_the_table_archives_the_old_one_and_the_next_load_makes_the_new_one(engine, store):
    keys.choose_key(store, "ledger__data", ["id"])
    store.save_increment("ledger__data", "id")
    store.set_hidden("ledger__data", False)
    done = engine.rename("ledger", new_table="gl_entries")
    [change] = done["tables"]
    assert (done["source"], change["from"], change["to"]) == ("ledger", "ledger__data", "gl_entries")
    assert change["archived_as"].startswith("ledger__data_") and len(change["archived_as"]) == len("ledger__data_20261113_161034")
    assert change["archived_as"] in tables(store) and "ledger__data" not in tables(store)
    assert not store.table_exists("gl_entries")  # not until the next load
    assert store.get_source("ledger").config["target"] == "gl_entries"
    assert store.get_table_key("gl_entries")["key"] == ["id"] and store.get_increment("gl_entries") == "id"

    [r] = engine.reload_source("ledger")
    assert (r.status, r.table, r.rows) == ("succeeded", "gl_entries", 2)
    assert [x["amount"] for x in store.read_table("gl_entries")] == [10, 20]
    assert [x["amount"] for x in store.read_table(change["archived_as"])] == [10, 20]  # the old data is kept


def test_renaming_the_source_takes_tables_named_after_it_along(engine, store):
    store.set_source_tags("ledger", ["finance"])
    done = engine.rename("ledger", new_name="general_ledger")
    assert done["renamed_from"] == "ledger" and done["tables"][0]["to"] == "general_ledger__data"
    assert not store.source_exists("ledger") and store.tags_by_source()["general_ledger"] == ["finance"]
    assert "target" not in store.get_source("general_ledger").config  # named after the source, as usual
    assert engine.reload_source("general_ledger")[0].table == "general_ledger__data"
    # a table with a name of its own keeps it when only the source is renamed
    engine.rename("general_ledger", new_table="gl")
    engine.reload_source("general_ledger")
    done = engine.rename("general_ledger", new_name="gl_source")
    assert done["tables"] == [] and targets.table_for(store.get_source("gl_source")) == "gl"


def test_bad_names_are_refused_before_anything_changes(engine, store):
    engine.ingest_upload("other", "o.csv", b"x\n1\n", create=True)
    for new_table, why in [("Bad Name", "can't be a table"), ("Ledger", "lowercase"), ("finance.ledger", "no schemas"),
                           ("other__data", "already the table of other"), ("x__stg", "__stg")]:
        with pytest.raises(ValueError, match=why):
            engine.rename("ledger", new_table=new_table)
    with pytest.raises(ValueError, match="already a source"):
        engine.rename("ledger", new_name="other")
    assert store.table_exists("ledger__data") and "target" not in store.get_source("ledger").config


def test_a_database_source_renames_one_table(store, tmp_path):
    path = tmp_path / "crm.db"
    conn = sqlite3.connect(path)
    conn.executescript("CREATE TABLE accounts (id INTEGER); INSERT INTO accounts VALUES (1);"
                       "CREATE TABLE notes (b TEXT); INSERT INTO notes VALUES ('x');")
    conn.commit()
    conn.close()
    engine = Engine(store, environ={"EYETABLE_SECRET_KEY": KEY})
    engine.load_db_objects(f"sqlite:///{path.as_posix()}", [{"schema": "main", "name": "accounts", "kind": "table", "source": "crm"}])
    engine.set_db_tables("crm", [{"schema": "main", "name": t, "kind": "table"} for t in ("accounts", "notes")])
    done = engine.rename("crm", table="crm__notes", new_table="crm_notes")
    assert [c["to"] for c in done["tables"]] == ["crm_notes"]
    assert targets.tables(store.get_source("crm")) == ["crm__data", "crm_notes"]
    queue = LoadQueue(engine)
    item = queue.wait(queue.add("crm", table="crm_notes")[0]["id"], 30)
    assert item["status"] == "succeeded" and store.count_rows("crm_notes") == 1


def test_the_naming_api(engine, store):
    client = TestClient(create_app(engine, TOKEN))
    r = client.put("/api/sources/ledger/naming", headers=AUTH, json={"name": "gl", "table": "ledger__data", "new_table": "gl_entries"})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["source"] == "gl" and body["summary"]["table"] == "gl_entries" and body["tables"][0]["archived_as"]
    assert client.put("/api/sources/gl/naming", headers=AUTH, json={"new_table": "Nope Nope"}).status_code == 400
    client.post("/api/sources/gl/reload", headers=AUTH, json={})
    assert client.get("/api/tables/gl_entries/keys", headers=AUTH).status_code == 200
    names = [t["table"] for t in client.get("/api/schema", headers=AUTH).json()["tables"]]
    assert "gl_entries" in names and body["tables"][0]["archived_as"] in names  # archived tables stay queryable
    res = client.post("/api/query", headers=AUTH, json={"sql": "SELECT COUNT(*) AS n FROM gl"}).json()
    assert res["rows"] == [[2]]  # the source's name still reaches its (renamed) table
