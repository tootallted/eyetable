"""Key scans: which columns, or sets of columns, identify each row, and the natural key a person picks."""

import time

import pytest
from fastapi.testclient import TestClient

from eyetable import keys
from eyetable.api import create_app
from eyetable.engine import Engine

TOKEN = "test-token-0123456789"
AUTH = {"Authorization": f"Bearer {TOKEN}"}


def load(store, table, rows, mode="replace"):
    store.write(table, iter([rows]), mode, 1, origin="test")


def test_a_column_with_no_nulls_and_no_repeats_is_a_primary_key_candidate(store):
    load(store, "people__data", [
        {"name": "Ada", "person_id": 1, "city": "London", "email": "a@x"},
        {"name": "Grace", "person_id": 2, "city": "Arlington", "email": "g@x"},
        {"name": "Ada", "person_id": 3, "city": None, "email": "a2@x"},
    ])
    scan = keys.scan(store, "people__data")
    by = {c["name"]: c for c in scan["columns"]}
    assert [c["name"] for c in scan["columns"]] == ["name", "person_id", "city", "email"]  # no ET_ columns
    assert (by["name"]["distinct"], by["name"]["nulls"], by["name"]["unique"]) == (2, 0, False)
    assert (by["city"]["nulls"], by["city"]["unique"]) == (1, False)
    assert scan["primary_key_candidates"] == ["person_id", "email"]  # the id-like name is suggested first
    assert scan["column_sets"] == []  # not searched when a single column will do


def test_with_no_single_key_it_finds_the_smallest_column_sets(store):
    load(store, "lines__data", [
        {"order_no": "A", "line": 1, "sku": "x", "qty": 1},
        {"order_no": "A", "line": 2, "sku": "y", "qty": 1},
        {"order_no": "B", "line": 1, "sku": "x", "qty": 2},
        {"order_no": "B", "line": 2, "sku": "z", "qty": 2},
    ])
    scan = keys.scan(store, "lines__data")
    assert scan["primary_key_candidates"] == []
    assert ["order_no", "line"] in scan["column_sets"]
    assert ["order_no", "sku"] in scan["column_sets"]
    assert all(len(s) == 2 for s in scan["column_sets"])  # no triples once pairs are found


def test_a_table_with_repeated_rows_has_no_key_at_all(store):
    load(store, "dupes__data", [{"a": 1, "b": "x"}, {"a": 1, "b": "x"}, {"a": 2, "b": "y"}])
    scan = keys.scan(store, "dupes__data")
    assert scan["primary_key_candidates"] == [] and scan["column_sets"] == []
    assert scan["exact_copies"] == 1  # one row is a copy of another


def test_rows_that_differ_only_across_many_columns_have_no_copies(store):
    rows = [{"a": a, "b": b, "c": c, "d": d} for a in (0, 1) for b in (0, 1) for c in (0, 1) for d in (0, 1)]
    load(store, "wide__data", rows)
    scan = keys.scan(store, "wide__data")
    assert scan["column_sets"] == [] and scan["exact_copies"] == 0
    assert keys.choose_key(store, "wide__data", ["a", "b", "c", "d"])["key_check"]["ok"]


def test_choosing_a_key_checks_it_and_every_scan_checks_it_again(store):
    load(store, "parts__data", [{"sku": "x", "qty": 1}, {"sku": "y", "qty": 1}])
    keys.scan_and_save(store, "parts__data")
    with pytest.raises(keys.KeyProblem, match="1 value repeated across 2 rows"):
        keys.choose_key(store, "parts__data", ["qty"])
    with pytest.raises(keys.KeyProblem, match="eyetable's own"):
        keys.choose_key(store, "parts__data", ["ET_id"])
    with pytest.raises(keys.KeyProblem, match="no column nope"):
        keys.choose_key(store, "parts__data", ["nope"])

    info = keys.choose_key(store, "parts__data", ["sku"])
    assert info["key"] == ["sku"] and info["key_check"]["ok"] is True

    # the next load repeats a sku: the key is kept but flagged
    load(store, "parts__data", [{"sku": "x", "qty": 1}, {"sku": "x", "qty": 2}, {"sku": None, "qty": 3}])
    info = keys.scan_and_save(store, "parts__data")
    assert info["key"] == ["sku"]
    check = info["key_check"]
    assert (check["ok"], check["null_rows"], check["duplicate_values"], check["duplicate_rows"]) == (False, 1, 1, 2)
    assert "qty" in info["scan"]["primary_key_candidates"]


def test_a_load_scans_its_table_and_dropping_the_table_forgets_it(store):
    engine = Engine(store)
    engine.ingest_upload("parts", "parts.csv", b"sku,qty\nx,1\ny,1\n", create=True)
    saved = store.get_table_key("parts__data")
    assert saved["scan"]["primary_key_candidates"] == ["sku"]
    store.drop_table("parts__data")
    assert store.get_table_key("parts__data") is None


def test_scans_can_be_turned_off(store):
    Engine(store, scan_keys="off").ingest_upload("parts", "parts.csv", b"sku\nx\n", create=True)
    assert store.get_table_key("parts__data") is None


def test_json_columns_are_compared_as_text(store):
    load(store, "docs__data", [{"meta": {"a": 1}, "n": 1}, {"meta": {"a": 2}, "n": 1}])
    scan = keys.scan(store, "docs__data")
    assert "meta" in scan["primary_key_candidates"]


def test_the_api_shows_scans_runs_them_and_sets_keys(store):
    client = TestClient(create_app(Engine(store), TOKEN))
    r = client.post("/api/sources/parts/uploads", headers=AUTH, data={"create": "true"},
                    files={"file": ("parts.csv", b"sku,qty\nx,1\ny,1\n", "text/csv")})
    assert r.status_code == 200, r.text

    info = client.get("/api/tables/parts__data/keys", headers=AUTH).json()
    assert info["scan"]["primary_key_candidates"] == ["sku"] and info["key"] is None
    assert client.get("/api/tables/nope__data/keys", headers=AUTH).status_code == 404
    assert client.get("/api/tables/parts__data;drop/keys", headers=AUTH).status_code == 404

    job = client.post("/api/tables/parts__data/keys/scan", headers=AUTH).json()["job_id"]
    for _ in range(100):
        state = client.get(f"/api/jobs/{job}", headers=AUTH).json()
        if state["status"] != "running":
            break
        time.sleep(0.05)
    assert state["status"] == "succeeded", state
    assert state["result"]["scan"]["rows"] == 2

    bad = client.put("/api/tables/parts__data/key", headers=AUTH, json={"columns": ["qty"]})
    assert bad.status_code == 400 and "repeated" in bad.json()["detail"]
    good = client.put("/api/tables/parts__data/key", headers=AUTH, json={"columns": ["sku"]}).json()
    assert good["key"] == ["sku"] and good["key_check"]["ok"]
    assert client.delete("/api/tables/parts__data/key", headers=AUTH).json()["key"] is None
