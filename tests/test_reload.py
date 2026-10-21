import time

import pytest
from fastapi.testclient import TestClient

from eyetable import gsheets
from eyetable.api import create_app
from eyetable.engine import Engine
from test_gsheets import LEDGER, LINK, fake_fetch

TOKEN = "test-token-0123456789"
AUTH = {"Authorization": f"Bearer {TOKEN}"}


def rows(store, table):
    return [(r["ET_id"], r.get("name") or r.get("account")) for r in store.read_table(table)]


def test_file_reload_replays_replace_and_appends(store):
    engine = Engine(store)
    engine.ingest_upload("parts", "old.csv", b"name\nold\n", create=True)
    engine.ingest_upload("parts", "v1.csv", b"name\nbolt\nnut\n", mode="replace")
    engine.ingest_upload("parts", "v2.csv", b"name\nwasher\n", mode="append")
    before = rows(store, "parts__data")
    loaded_before = store.last_loaded("parts")
    uploads_before = len(store.list_uploads("parts"))

    time.sleep(0.01)
    results = engine.reload_source("parts")
    assert [(r.status, r.rows) for r in results] == [("succeeded", 2), ("succeeded", 1)]
    assert rows(store, "parts__data") == before == [(1, "bolt"), (2, "nut"), (3, "washer")]
    assert store.last_loaded("parts") > loaded_before
    assert len(store.list_uploads("parts")) == uploads_before  # replayed, not stored again


def test_file_reload_needs_a_file(store):
    engine = Engine(store)
    engine.ingest_upload("parts", "v1.csv", b"name\nbolt\n", create=True)
    with store.db.begin() as conn:
        conn.execute(store.uploads.delete())
    with pytest.raises(ValueError, match="no file"):
        engine.reload_source("parts")


def test_sheet_reload_fetches_the_tab_again(store):
    engine = Engine(store)
    engine.ingest_gsheet(LINK, fetch=fake_fetch())
    changed = {**LEDGER, "Q2 Actuals": [["Account", "Amount"], ["Rent", 1250], ["Travel", 300]]}
    [result] = engine.reload_source("general_ledger_2026_q2_actuals", sheet_fetch=fake_fetch(changed))
    assert (result.status, result.rows) == ("succeeded", 2)
    assert rows(store, "general_ledger_2026_q2_actuals__data") == [(1, "Rent"), (2, "Travel")]
    assert len(store.read_table("general_ledger_2026_q1_actuals__data")) == 2  # other tabs untouched


def test_sheet_reload_reports_a_missing_tab(store):
    engine = Engine(store)
    engine.ingest_gsheet(LINK, fetch=fake_fetch())
    renamed = {"Q1 Actuals": LEDGER["Q1 Actuals"], "Q2 (final)": LEDGER["Q2 Actuals"]}
    with pytest.raises(ValueError, match="no longer in"):
        engine.reload_source("general_ledger_2026_q2_actuals", sheet_fetch=fake_fetch(renamed))


def test_database_reload_pulls_again(store, source_db):
    engine = Engine(store)
    engine.add_source("crm", "database", {"url": source_db})
    engine.ingest("crm")
    results = engine.reload_source("crm")
    assert {r.table: r.rows for r in results} == {"crm__customers": 3, "crm__orders": 4, "crm__big_orders": 2}


def test_reload_endpoint(store):
    calls = []

    def fetch(url, google_token=None):
        calls.append(google_token)
        if google_token is None and calls.count(None) > 1:
            raise gsheets.SheetError("this spreadsheet is private", needs_sign_in=True)
        return fake_fetch()(url)

    client = TestClient(create_app(Engine(store), TOKEN, sheet_fetch=fetch))
    client.post("/api/gsheets", headers=AUTH, json={"url": LINK})
    client.post("/api/sources/parts/uploads", headers=AUTH, files={"file": ("p.csv", b"name\nbolt\n", "text/csv")}, data={"create": "true"})

    summary = {s["name"]: s for s in client.get("/api/sources", headers=AUTH).json()}
    first_loaded = summary["parts"]["last_loaded_at"]
    assert first_loaded

    time.sleep(0.01)
    r = client.post("/api/sources/parts/reload", headers=AUTH)
    assert r.status_code == 200, r.text
    assert r.json()["rows"] == 1 and r.json()["source"]["last_loaded_at"] > first_loaded

    r = client.post("/api/sources/general_ledger_2026_q1_actuals/reload", headers=AUTH, json={})
    assert r.status_code == 403 and "private" in r.json()["detail"]  # page then offers Sign in with Google
    r = client.post("/api/sources/general_ledger_2026_q1_actuals/reload", headers=AUTH, json={"google_token": "ya29.x"})
    assert r.status_code == 200 and r.json()["rows"] == 2
    assert calls[-1] == "ya29.x"

    assert client.post("/api/sources/nope/reload", headers=AUTH).status_code == 404
    assert client.post("/api/sources/parts/reload").status_code == 401
