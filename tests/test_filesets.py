"""File sets: a file source reading every file matching a pattern in a server folder, into one table."""

import pytest
from fastapi.testclient import TestClient

from eyetable import filesets
from eyetable.api import create_app
from eyetable.engine import Engine
from eyetable.groups import GroupRunner

TOKEN = "test-token-0123456789"
AUTH = {"Authorization": f"Bearer {TOKEN}"}


@pytest.fixture
def folder(tmp_path):
    d = tmp_path / "ledgers"
    d.mkdir()
    (d / "ledger_2026-01-01.csv").write_text("account,amount\nRent,100\nTravel,20\n")
    (d / "ledger_2026-01-02.csv").write_text("account,amount,memo\nRent,110,late fee\n")  # a column the others lack
    (d / "ledger_2026-01-03.csv").write_text("Account,Amount,cost_center\nMeals,30,CC1\n")  # same names, other case
    (d / "notes.csv").write_text("x\n1\n")  # doesn't match
    (d / "ledger_old.xlsx").write_bytes(b"not text")  # matches the name but isn't a text file
    return d


def rows(store, table):
    return store.read_table(table)


def test_matching_files_and_names(folder):
    assert [f.name for f in filesets.matching_files(str(folder), "ledger_*.csv")] == [
        "ledger_2026-01-01.csv", "ledger_2026-01-02.csv", "ledger_2026-01-03.csv"]
    assert [f.name for f in filesets.matching_files(str(folder), "LEDGER_2026-01-0[12].CSV")] == [
        "ledger_2026-01-01.csv", "ledger_2026-01-02.csv"]
    assert filesets.name_for("ledger_*.csv") == "ledger_aggregated"
    with pytest.raises(filesets.FileSetProblem, match="doesn't exist"):
        filesets.matching_files(str(folder / "nope"), "ledger_*.csv")
    with pytest.raises(filesets.FileSetProblem, match="just a name"):
        filesets.matching_files(str(folder), "sub/ledger_*.csv")
    with pytest.raises(filesets.FileSetProblem, match="only"):
        filesets.matching_files(str(folder), "ledger_*.xlsx")
    with pytest.raises(filesets.FileSetProblem, match="full path"):
        filesets.matching_files("ledgers", "ledger_*.csv")


def test_allowed_folders_limit_where_files_come_from(folder, tmp_path, monkeypatch):
    monkeypatch.setenv("EYETABLE_FILE_FOLDERS", str(tmp_path / "elsewhere"))
    with pytest.raises(filesets.FileSetProblem, match="isn't in a folder eyetable may read"):
        filesets.matching_files(str(folder), "ledger_*.csv")
    monkeypatch.setenv("EYETABLE_FILE_FOLDERS", f"{tmp_path / 'elsewhere'};{tmp_path}")
    assert len(filesets.matching_files(str(folder), "ledger_*.csv")) == 3


def test_an_uploaded_file_becomes_a_set_of_files_in_one_table(store, folder):
    engine = Engine(store)
    engine.ingest_upload("ledger_2026_01_02", "ledger_2026-01-02.csv", (folder / "ledger_2026-01-02.csv").read_bytes(), create=True)
    store.set_source_tags("ledger_2026_01_02", ["finance"])
    group = GroupRunner(engine).create("Nightly", ["ledger_2026_01_02"])

    name, warnings = engine.update_file_source("ledger_2026_01_02", str(folder), "ledger_*.csv", "ledger_aggregated")
    assert (name, warnings) == ("ledger_aggregated", [])
    assert not store.source_exists("ledger_2026_01_02") and not store.table_exists("ledger_2026_01_02__data")
    assert store.table_exists("ledger_aggregated__data")  # the old table, renamed
    assert store.tags_by_source()["ledger_aggregated"] == ["finance"]
    assert store.get_group(group.id).members == ["ledger_aggregated"]

    [r] = engine.reload_source("ledger_aggregated")
    assert (r.status, r.rows, r.note) == ("succeeded", 4, "3 files matching ledger_*.csv")
    got = rows(store, "ledger_aggregated__data")
    cols = list(got[0])
    assert cols[:5] == ["ET_id", "account", "amount", "memo", "cost_center"]  # every column from every file, once
    assert [(x["account"], x["amount"], x["memo"], x["cost_center"]) for x in got] == [
        ("Rent", 100, None, None), ("Travel", 20, None, None), ("Rent", 110, "late fee", None), ("Meals", 30, None, "CC1")]
    assert [x["ET_source"] for x in got] == ["ledger_aggregated - ledger_2026-01-01.csv"] * 2 + [
        "ledger_aggregated - ledger_2026-01-02.csv", "ledger_aggregated - ledger_2026-01-03.csv"]

    # a new file in the folder is picked up by the next load, groups and schedules included
    (folder / "ledger_2026-01-04.csv").write_text("account,amount\nFuel,40\n")
    run = store.get_group_run(GroupRunner(engine).run(group.id, wait=True))
    assert run.status == "succeeded" and run.results[0]["rows"] == 5

    # uploading into it is refused; going back to uploads is allowed
    with pytest.raises(ValueError, match="put the file in that folder"):
        engine.ingest_upload("ledger_aggregated", "x.csv", b"a\n1\n")
    engine.update_file_source("ledger_aggregated", None, "", None)
    assert "pattern" not in store.get_source("ledger_aggregated").config


def test_a_pattern_that_matches_nothing_is_refused_unless_unchecked(store, folder):
    engine = Engine(store)
    engine.ingest_upload("ledger", "l.csv", b"a\n1\n", create=True)
    with pytest.raises(filesets.FileSetProblem, match="no file"):
        engine.update_file_source("ledger", str(folder), "budget_*.csv", None)
    name, warnings = engine.update_file_source("ledger", str(folder), "budget_*.csv", None, test=False)
    assert warnings and "next load fails" in warnings[0]
    with pytest.raises(filesets.FileSetProblem, match="no file"):
        engine.reload_source("ledger")


def test_a_file_that_cant_be_read_names_itself_and_leaves_the_table_as_it_was(store, folder, monkeypatch):
    from eyetable.connectors import file as file_module

    engine = Engine(store)
    engine.ingest_upload("ledger", "l.csv", b"a\n1\n", create=True)
    engine.update_file_source("ledger", str(folder), "ledger_*.csv", None)
    real = file_module.parse_csv

    def picky(content, delimiter=None):
        if b"Meals" in content:
            raise ValueError("unreadable row 2")
        return real(content, delimiter)

    monkeypatch.setattr(file_module, "parse_csv", picky)
    [r] = engine.reload_source("ledger")
    assert r.status == "failed" and "ledger_2026-01-03.csv: unreadable row 2" in r.error
    assert [x["a"] for x in store.read_table("ledger__data")] == [1]  # the earlier load is still there


def test_renaming_checks_the_new_name(store):
    engine = Engine(store)
    engine.ingest_upload("a", "a.csv", b"x\n1\n", create=True)
    engine.ingest_upload("b", "b.csv", b"x\n1\n", create=True)
    with pytest.raises(ValueError, match="already a source"):
        engine.rename_source("a", "b")
    with pytest.raises(ValueError, match="lowercase"):
        engine.rename_source("a", "Bad Name")


def test_the_api(store, folder):
    engine = Engine(store)
    client = TestClient(create_app(engine, TOKEN))
    client.post("/api/sources/ledger_2026_01_02/uploads", headers=AUTH, data={"create": "true"},
                files={"file": ("ledger_2026-01-02.csv", (folder / "ledger_2026-01-02.csv").read_bytes(), "text/csv")})
    settings = client.get("/api/sources/ledger_2026_01_02/settings", headers=AUTH).json()
    assert (settings["type"], settings["filename"], settings["pattern"], settings["name"]) == (
        "file", "ledger_2026-01-02.csv", None, "ledger_2026_01_02")

    match = client.post("/api/files/match", headers=AUTH, json={"folder": str(folder), "pattern": "ledger_*.csv"}).json()
    assert (match["count"], match["suggested_name"], match["files"][0]["name"]) == (3, "ledger_aggregated", "ledger_2026-01-01.csv")
    assert client.post("/api/files/match", headers=AUTH, json={"folder": str(folder / "x"), "pattern": "a.csv"}).status_code == 400

    r = client.put("/api/sources/ledger_2026_01_02/settings", headers=AUTH,
                   json={"folder": str(folder), "pattern": "ledger_*.csv", "name": "ledger_aggregated"})
    assert r.status_code == 200, r.text
    d = r.json()
    assert (d["name"], d["pattern"], d["file_count"]) == ("ledger_aggregated", "ledger_*.csv", 3)
    src = {s["name"]: s for s in client.get("/api/sources", headers=AUTH).json()}["ledger_aggregated"]
    assert src["file_set"] == {"folder": str(folder.resolve()), "pattern": "ledger_*.csv"}
    body = client.post("/api/sources/ledger_aggregated/reload", headers=AUTH, json={}).json()
    assert body["rows"] == 4 and body["source"]["table"] == "ledger_aggregated__data"
