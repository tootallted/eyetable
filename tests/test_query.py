import pytest
from fastapi.testclient import TestClient

from eyetable.api import create_app
from eyetable.engine import Engine
from eyetable.query import QueryError, QueryRunner, clean, first_keyword

TOKEN = "test-token-0123456789"
AUTH = {"Authorization": f"Bearer {TOKEN}"}
PARTS = b"sku,qty,price\nA-1,3,1.5\nB-2,5,2.25\nC-3,8,0.5\n"


@pytest.fixture
def loaded(store):
    engine = Engine(store)
    engine.ingest_upload("parts", "parts.csv", PARTS, create=True)
    engine.ingest_upload("orders", "orders.csv", b"sku,units\nA-1,2\nC-3,1\n", create=True)
    return store


@pytest.fixture
def client(loaded):
    return TestClient(create_app(Engine(loaded), TOKEN))


def ask(client, sql, **extra):
    return client.post("/api/query", headers=AUTH, json={"sql": sql, **extra})


def test_sources_are_queryable_by_name(client):
    r = ask(client, "SELECT sku, qty FROM parts WHERE qty > 3 ORDER BY sku")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["columns"] == ["sku", "qty"]
    assert body["rows"] == [["B-2", 5], ["C-3", 8]]
    assert body["truncated"] is False and body["row_count"] == 2


def test_joins_and_physical_table_names(client):
    sql = """
        -- units ordered per part
        WITH o AS (SELECT sku, units FROM orders)
        SELECT p.sku, o.units, p."ET_id" FROM parts__data p JOIN o ON o.sku = p.sku ORDER BY p.sku;
    """
    assert ask(client, sql).json()["rows"] == [["A-1", 2, 1], ["C-3", 1, 3]]


def test_limit_truncates(client):
    body = ask(client, "SELECT * FROM parts", limit=2).json()
    assert body["row_count"] == 2 and body["truncated"] is True


@pytest.mark.parametrize(
    "sql",
    [
        "DELETE FROM parts__data",
        "DROP TABLE parts__data",
        "UPDATE parts__data SET qty = 0",
        "INSERT INTO parts__data (sku) VALUES ('x')",
        "PRAGMA writable_schema = ON",
        "SELECT 1; DELETE FROM parts__data",
        "",
    ],
)
def test_nothing_can_change_data(client, loaded, sql):
    r = ask(client, sql)
    assert r.status_code == 400
    assert len(loaded.read_table("parts__data")) == 3


def test_errors_come_back_readable(client):
    r = ask(client, "SELECT nope FROM parts")
    assert r.status_code == 400 and "nope" in r.json()["detail"]


def test_query_needs_the_token(client):
    assert client.post("/api/query", json={"sql": "SELECT 1"}).status_code == 401


def test_schema_lists_sources_and_tables(client):
    body = client.get("/api/schema", headers=AUTH).json()
    assert {s["name"]: s.get("table") for s in body["sources"]} == {"orders": "orders__data", "parts": "parts__data"}
    tables = {t["table"]: [c["name"] for c in t["columns"]] for t in body["tables"]}
    assert tables["parts__data"][:4] == ["ET_id", "ET_timestamp", "ET_source", "ET_run_id"]
    assert tables["parts__data"][4:] == ["sku", "qty", "price"]


def test_runner_leaves_no_views_behind(loaded):
    runner = QueryRunner(loaded)
    runner.run("SELECT * FROM parts", aliases={"parts": "parts__data"})
    with pytest.raises(QueryError):
        runner.run("SELECT * FROM parts")  # no alias given: the earlier temp view is gone
    assert len(loaded.read_table("parts__data")) == 3


def test_alias_never_shadows_a_real_table(loaded):
    rows = QueryRunner(loaded).run("SELECT count(*) FROM orders__data", aliases={"orders__data": "parts__data"}).rows
    assert rows == [[2]]


def test_keyword_detection():
    assert first_keyword("  -- hi\n/* x */ (SELECT 1)") == "select"
    assert first_keyword("with x as (select 1) select * from x") == "with"
    assert first_keyword("delete from t") == "delete"
    assert clean("select 1 ;; ") == "select 1"


def test_eyetable_internal_tables_are_off_limits(client):
    tables = {t["table"] for t in client.get("/api/schema", headers=AUTH).json()["tables"]}
    assert tables == {"parts__data", "orders__data"}
    for sql in ("SELECT * FROM uploads", "SELECT content FROM uploads", "SELECT * FROM sources", "SELECT * FROM runs"):
        r = ask(client, sql)
        assert r.status_code == 400 and "prohibited" in r.json()["detail"], sql
