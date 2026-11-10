"""Incremental loads: the column they go by, and loading only what's new since the last load."""

import datetime as dt
import sqlite3
from decimal import Decimal

import pytest
import sqlalchemy as sa
from cryptography.fernet import Fernet
from fastapi.testclient import TestClient

from eyetable import incremental, keys
from eyetable.api import create_app
from eyetable.connectors.dbtable import DbTableConnector
from eyetable.engine import Engine
from test_gsheets import LINK, fake_fetch

TOKEN = "test-token-0123456789"
AUTH = {"Authorization": f"Bearer {TOKEN}"}
KEY = Fernet.generate_key().decode()


@pytest.fixture
def crm(tmp_path):
    path = tmp_path / "crm.db"
    conn = sqlite3.connect(path)
    conn.executescript("""
        CREATE TABLE accounts (accountid TEXT, name TEXT, version INTEGER, modifiedon TEXT);
        INSERT INTO accounts VALUES ('a1', 'Ada', 1, '2026-11-01T10:00:00Z'), ('a2', 'Grace', 1, '2026-11-02T10:00:00Z');
        CREATE TABLE notes (body TEXT);
        INSERT INTO notes VALUES ('hi');
    """)
    conn.commit()
    conn.close()
    return path


def source_sql(path, sql):
    conn = sqlite3.connect(path)
    conn.executescript(sql)
    conn.commit()
    conn.close()


def engine_with(store, crm, tables=("accounts",)):
    engine = Engine(store, environ={"EYETABLE_SECRET_KEY": KEY})
    url = f"sqlite:///{crm.as_posix()}"
    engine.load_db_objects(url, [{"schema": "main", "name": tables[0], "kind": "table", "source": "crm"}])
    if len(tables) > 1:
        engine.set_db_tables("crm", [{"schema": "main", "name": t, "kind": "table"} for t in tables])
    return engine


def names(store, table="crm__data"):
    return sorted((r["accountid"], r["name"]) for r in store.read_table(table))


def test_ordering_values_from_tables_and_sources():
    assert incremental.ordered("2026-11-01T10:00:00Z") == ("t", dt.datetime(2026, 11, 1, 10, tzinfo=dt.timezone.utc))
    assert incremental.ordered(dt.datetime(2026, 11, 1, 10)) == incremental.ordered("2026-11-01 10:00:00")
    assert incremental.ordered("17") == ("n", Decimal(17)) and incremental.ordered(17.0) == ("n", Decimal("17.0"))
    since = incremental.ordered(dt.datetime(2026, 11, 1, 10))
    assert incremental.is_past("2026-11-01T10:00:01Z", since, False)
    assert not incremental.is_past("2026-11-01T10:00:00Z", since, False)
    assert incremental.is_past("2026-11-01T10:00:00Z", since, True)
    assert not incremental.is_past(None, since, True)
    batches = [[{"Modified On": "2026-11-01"}, {"Modified On": "2026-11-03"}], [{"Modified On": None}]]
    assert list(incremental.newer(batches, "modified_on", "2026-11-02", False)) == [[{"Modified On": "2026-11-03"}]]


def test_the_scan_suggests_incremental_columns_best_first(store):
    store.write("t__data", iter([[
        {"id": 1, "qty": 5, "createdon": dt.datetime(2026, 1, 1), "modifiedon": "2026-11-01T10:00:00Z", "note": "x"},
        {"id": 2, "qty": 5, "createdon": dt.datetime(2026, 1, 2), "modifiedon": "2026-11-02T10:00:00Z", "note": "2026-01-01"},
    ]]), "replace", 1, origin="t")
    found = keys.scan(store, "t__data")["incremental_candidates"]
    assert [c["name"] for c in found] == ["modifiedon", "createdon", "id"]  # qty repeats; note isn't all dates
    assert "new and changed rows" in found[0]["why"] and found[0]["max"] == "2026-11-02T10:00:00Z"
    assert "not changes" in found[1]["why"]


def test_names_that_only_look_like_modified_or_version_columns_are_not_suggested_as_such(store):
    store.write("gl__data", iter([[{"credit": 1.5, "timezoneruleversionnumber": 4, "versionnumber": 10},
                                   {"credit": 2.5, "timezoneruleversionnumber": 4, "versionnumber": 11},
                                   {"credit": 3.5, "timezoneruleversionnumber": 5, "versionnumber": 12}]]), "replace", 1, origin="t")
    found = {c["name"]: c["why"] for c in keys.scan(store, "gl__data")["incremental_candidates"]}
    assert "credit" not in found and "timezoneruleversionnumber" not in found  # not ID-like, and not a row version
    assert "row version" in found["versionnumber"]


def test_choosing_an_incremental_column(store):
    store.write("t__data", iter([[{"id": 1, "meta": {"a": 1}, "flag": True}]]), "replace", 1, origin="t")
    with pytest.raises(keys.KeyProblem, match="eyetable's own"):
        keys.choose_increment(store, "t__data", "ET_timestamp")
    with pytest.raises(keys.KeyProblem, match="date, time or number"):
        keys.choose_increment(store, "t__data", "meta")
    info = keys.choose_increment(store, "t__data", "id")
    assert info["incremental"] == {"column": "id", "kind": "bigint", "latest": 1, "missing": False}
    assert keys.choose_increment(store, "t__data", None)["incremental"] is None


def test_a_database_table_loads_only_new_rows_and_asks_the_database_for_them(store, crm, monkeypatch):
    engine = engine_with(store, crm)
    store.save_increment("crm__data", "modifiedon")
    seen = []
    real_read = DbTableConnector.read

    def spy(self, key, batch_size=1000, since=None):
        seen.append(since)
        return real_read(self, key, batch_size, since)

    monkeypatch.setattr(DbTableConnector, "read", spy)

    # a new account, and a change to Ada; without a natural key the change is added as another row
    source_sql(crm, "INSERT INTO accounts VALUES ('a3', 'Linus', 1, '2026-11-03T10:00:00Z');"
                    "UPDATE accounts SET name = 'Ada L', modifiedon = '2026-11-04T10:00:00Z' WHERE accountid = 'a1';")
    [r] = engine.reload_source("crm", incremental=True)
    assert (r.status, r.mode, r.rows, r.replaced, r.since) == ("succeeded", "incremental", 2, 0, "2026-11-02T10:00:00Z")
    assert seen[-1] == ("modifiedon", "2026-11-02T10:00:00Z", False)
    assert names(store) == [("a1", "Ada"), ("a1", "Ada L"), ("a2", "Grace"), ("a3", "Linus")]
    assert [x["ET_id"] for x in store.read_table("crm__data")] == [1, 2, 3, 4]

    # nothing new: nothing added
    assert engine.reload_source("crm", incremental=True)[0].rows == 0

    # with a natural key, a changed row replaces its older copy, and the boundary row is fetched again (>=)
    engine.reload_source("crm")  # full load: back to one row per account
    keys.choose_key(store, "crm__data", ["accountid"])
    source_sql(crm, "UPDATE accounts SET name = 'Grace H', modifiedon = '2026-11-05T10:00:00Z' WHERE accountid = 'a2';")
    [r] = engine.reload_source("crm", incremental=True)
    assert seen[-1] == ("modifiedon", "2026-11-04T10:00:00Z", True)
    assert (r.rows, r.replaced) == (2, 2)  # Ada L again (the boundary) and Grace H
    assert names(store) == [("a1", "Ada L"), ("a2", "Grace H"), ("a3", "Linus")]


def test_an_empty_table_is_loaded_in_full(store, crm):
    engine = engine_with(store, crm)
    store.save_increment("crm__data", "version")
    with store.db.begin() as conn:
        conn.execute(sa.text("DELETE FROM crm__data"))
    [r] = engine.reload_source("crm", incremental=True)
    assert (r.mode, r.rows) == ("replace", 2)


def test_a_failed_incremental_load_leaves_the_table_as_it_was(store, crm, monkeypatch):
    engine = engine_with(store, crm)
    store.save_increment("crm__data", "version")
    source_sql(crm, "INSERT INTO accounts VALUES ('a3', 'Linus', 2, '2026-11-03'), ('a4', 'Barbara', 3, '2026-11-03');")
    real_read = DbTableConnector.read

    def broken(self, key, batch_size=1000, since=None):
        for batch in real_read(self, key, 1, since):
            yield batch
            raise RuntimeError("the connection dropped")

    monkeypatch.setattr(DbTableConnector, "read", broken)
    [r] = engine.reload_source("crm", incremental=True)
    assert r.status == "failed" and "dropped" in r.error
    assert names(store) == [("a1", "Ada"), ("a2", "Grace")]


def test_only_tables_with_an_incremental_column_load_incrementally(store, crm):
    engine = engine_with(store, crm, tables=("accounts", "notes"))
    with pytest.raises(incremental.IncrementalProblem, match="choose one in Preview Data"):
        engine.reload_source("crm", incremental=True)
    store.save_increment("crm__data", "version")
    source_sql(crm, "INSERT INTO accounts VALUES ('a3', 'Linus', 2, '2026-11-03'); INSERT INTO notes VALUES ('more');")
    results = engine.reload_source("crm", incremental=True)
    assert [(r.table, r.rows) for r in results] == [("crm__data", 1)]  # notes is left alone
    assert len(store.read_table("crm__notes")) == 1
    # one table on its own, either way
    assert [r.table for r in engine.reload_source("crm", table="crm__notes")] == ["crm__notes"]
    assert len(store.read_table("crm__notes")) == 2
    with pytest.raises(ValueError, match="no table"):
        engine.reload_source("crm", table="crm__nope")


def test_a_sheet_keeps_only_the_newer_rows(store):
    engine = Engine(store)
    book = {"Q2 Actuals": [["Account", "Amount", "Updated"], ["Rent", 1250, "2026-04-01"], ["Travel", 90, "2026-04-02"]]}
    engine.ingest_gsheet(LINK, fetch=fake_fetch(book))
    table = "general_ledger_2026_q2_actuals__data"
    store.save_increment(table, "updated")
    keys.choose_key(store, table, ["account"])
    book = {"Q2 Actuals": [["Account", "Amount", "Updated"], ["Rent", 1250, "2026-04-01"], ["Travel", 95, "2026-04-05"],
                           ["Meals", 40, "2026-04-06"]]}
    [r] = engine.reload_source("general_ledger_2026_q2_actuals", sheet_fetch=fake_fetch(book), incremental=True)
    assert (r.status, r.rows, r.replaced) == ("succeeded", 2, 1)  # Travel's update (replacing it) and Meals; Rent is older
    assert sorted((x["account"], x["amount"]) for x in store.read_table(table)) == [("Meals", 40), ("Rent", 1250), ("Travel", 95)]


def test_a_file_source_has_no_incremental_load(store):
    engine = Engine(store)
    engine.ingest_upload("parts", "p.csv", b"id\n1\n", create=True)
    with pytest.raises(incremental.IncrementalProblem, match="Append"):
        engine.reload_source("parts", incremental=True)


def test_the_api_sets_the_column_and_reloads_either_way(store, crm):
    engine = engine_with(store, crm)
    client = TestClient(create_app(engine, TOKEN))
    info = client.get("/api/tables/crm__data/keys", headers=AUTH).json()
    assert [c["name"] for c in info["scan"]["incremental_candidates"]] == ["modifiedon"]  # version holds one value
    assert info["incremental"] is None

    r = client.post("/api/sources/crm/reload", headers=AUTH, json={"incremental": True})
    assert r.status_code == 400 and "incremental column" in r.json()["detail"]
    bad = client.put("/api/tables/crm__data/incremental", headers=AUTH, json={"column": "ET_run_id"})
    assert bad.status_code == 400
    ok = client.put("/api/tables/crm__data/incremental", headers=AUTH, json={"column": "modifiedon"}).json()
    assert ok["incremental"]["column"] == "modifiedon" and ok["incremental"]["latest"] == "2026-11-02T10:00:00Z"
    assert client.get("/api/sources", headers=AUTH).json()[0]["incremental"] == "modifiedon"

    source_sql(crm, "INSERT INTO accounts VALUES ('a3', 'Linus', 1, '2026-11-03T10:00:00Z');")
    body = client.post("/api/sources/crm/reload", headers=AUTH, json={"incremental": True, "table": "crm__data"}).json()
    assert [(x["mode"], x["rows"]) for x in body["results"]] == [("incremental", 1)]
    assert body["source"]["rows"] == 3
    full = client.post("/api/sources/crm/reload", headers=AUTH, json={}).json()
    assert [(x["mode"], x["rows"]) for x in full["results"]] == [("replace", 3)]

    assert client.delete("/api/tables/crm__data/incremental", headers=AUTH).json()["incremental"] is None
    client.post("/api/sources/parts/uploads", headers=AUTH, data={"create": "true"},
                files={"file": ("p.csv", b"id\n1\n", "text/csv")})
    r = client.post("/api/sources/parts/reload", headers=AUTH, json={"incremental": True})
    assert r.status_code == 400 and "Append" in r.json()["detail"]
