import time

import pytest
from cryptography.fernet import Fernet
from fastapi.testclient import TestClient

from eyetable import dbconnect
from eyetable.api import create_app
from eyetable.credentials import decrypt_secrets
from eyetable.engine import Engine

TOKEN = "test-token-0123456789"
AUTH = {"Authorization": f"Bearer {TOKEN}"}
KEY = Fernet.generate_key().decode()
AZURE = ("Driver={ODBC Driver 18 for SQL Server};Server=tcp:signetic-sql.database.windows.net,1433;"
         "Database=signetic_multitenant;Uid=sql-admin;Pwd={p;a}}ss};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;")


@pytest.fixture
def source_db(tmp_path):
    import sqlite3
    path = tmp_path / "crm.db"
    conn = sqlite3.connect(path)
    conn.executescript("""
        CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT);
        CREATE TABLE orders (id INTEGER, customer_id INTEGER, total REAL);
        CREATE VIEW big_orders AS SELECT * FROM orders WHERE total > 50;
        INSERT INTO customers VALUES (1, 'Ada'), (2, 'Grace');
        INSERT INTO orders VALUES (1, 1, 99.5), (2, 2, 10), (3, 1, 75);
    """)
    conn.commit()
    conn.close()
    return f"sqlite:///{path.as_posix()}"


def test_parse_odbc_handles_braces():
    pairs = dict(dbconnect.parse_odbc(AZURE))
    assert pairs["Driver"] == "ODBC Driver 18 for SQL Server"
    assert pairs["Pwd"] == "p;a}ss"
    assert pairs["Connection Timeout"] == "30"
    assert dbconnect.parse_odbc(dbconnect.build_odbc(list(pairs.items()))) == list(pairs.items())
    with pytest.raises(ValueError, match="closing"):
        dbconnect.parse_odbc("Driver={ODBC;Server=x")


def test_password_is_taken_out_and_encrypted():
    p = dbconnect.prepare(AZURE, KEY)
    assert "p;a}ss" not in str(p.config) and "Pwd" not in p.config["odbc"]
    assert p.config["password"].startswith("eyetable-enc:")
    assert decrypt_secrets(p.config, KEY)["password"] == "p;a}ss"
    assert (p.server, p.database, p.label) == ("signetic-sql.database.windows.net", "signetic_multitenant",
                                                "signetic-sql.database.windows.net/signetic_multitenant")
    url = dbconnect.prepare("postgresql+psycopg://bob:s3cret@db.local:5432/crm", KEY)
    assert "s3cret" not in url.config["url"] and url.label == "db.local/crm"


@pytest.mark.parametrize("bad, message", [
    (AZURE.replace("{p;a}}ss}", "{your_password_here}"), "placeholder"),
    ("Server=x;Database=y;", "no Driver"),
    ("", "paste"),
])
def test_unusable_strings(bad, message):
    with pytest.raises(ValueError, match=message):
        dbconnect.prepare(bad, KEY)


def test_driver_fallback(monkeypatch):
    import pyodbc
    monkeypatch.setattr(pyodbc, "drivers", lambda: ["SQL Server", "ODBC Driver 17 for SQL Server"])
    used, warning = dbconnect.installed_driver("ODBC Driver 18 for SQL Server")
    assert used == "ODBC Driver 17 for SQL Server" and "isn't installed" in warning
    assert dbconnect.installed_driver("ODBC Driver 17 for SQL Server") == ("ODBC Driver 17 for SQL Server", None)
    with pytest.raises(ValueError, match="installed"):
        dbconnect.installed_driver("Oracle in OraClient19Home1")


def test_names():
    assert dbconnect.suggested_name("signetic_multitenant", "dbo", "Customers") == "signetic_multitenant_customers"
    assert dbconnect.suggested_name("crm", "sales", "Order Lines") == "crm_sales_order_lines"
    assert dbconnect.suggested_name("", "main", "x") == "x"


def test_list_and_load_tables_and_views(store, source_db):
    engine = Engine(store, environ={"EYETABLE_SECRET_KEY": KEY})
    prepared = dbconnect.prepare(source_db, KEY)
    db, _ = dbconnect.make_engine(prepared.config, prepared.password)
    assert {(o["name"], o["kind"]) for o in dbconnect.list_objects(db)} == {
        ("customers", "table"), ("orders", "table"), ("big_orders", "view")}
    results = engine.load_db_objects(source_db, [
        {"schema": "main", "name": "customers", "kind": "table", "source": "crm_customers"},
        {"schema": "main", "name": "big_orders", "kind": "view", "source": "crm_big_orders"},
        {"schema": "main", "name": "missing", "kind": "table", "source": "crm_missing"},
    ])
    assert [(r["source"], r["status"], r["rows"]) for r in results] == [
        ("crm_customers", "succeeded", 2), ("crm_big_orders", "succeeded", 2), ("crm_missing", "failed", 0)]
    assert "wasn't found" in results[2]["error"]
    rows = store.read_table("crm_customers__data")
    assert [r["name"] for r in rows] == ["Ada", "Grace"]
    assert rows[0]["ET_source"].endswith("/main.customers")
    assert store.get_source("crm_customers").type == "dbtable"
    assert [(r.status, r.rows) for r in engine.reload_source("crm_big_orders")] == [("succeeded", 2)]


def test_db_api(store, source_db):
    client = TestClient(create_app(Engine(store, environ={"EYETABLE_SECRET_KEY": KEY}), TOKEN))
    listing = client.post("/api/db/inspect", headers=AUTH, json={"connection": source_db}).json()
    by_name = {o["name"]: o for o in listing["objects"]}
    assert by_name["customers"]["kind"] == "table" and by_name["customers"]["loaded_as"] is None
    assert by_name["big_orders"]["kind"] == "view"
    assert not store.list_sources()  # listing saves nothing

    job = client.post("/api/db/load", headers=AUTH, json={"connection": source_db, "items": [
        {"schema": "main", "name": "customers", "kind": "table", "source": "crm_customers"},
        {"schema": "main", "name": "orders", "kind": "table", "source": "crm_orders"},
    ]}).json()["job_id"]
    for _ in range(100):
        state = client.get(f"/api/jobs/{job}", headers=AUTH).json()
        if state["status"] != "running":
            break
        time.sleep(0.05)
    assert state["status"] == "succeeded" and state["result"]["loaded"] == 2 and state["result"]["rows"] == 5

    again = {o["name"]: o for o in client.post("/api/db/inspect", headers=AUTH, json={"connection": source_db}).json()["objects"]}
    assert again["customers"]["loaded_as"] == "crm_customers"
    q = client.post("/api/query", headers=AUTH, json={"sql": "SELECT count(*) FROM crm_orders"}).json()
    assert q["rows"] == [[3]]

    assert client.post("/api/db/inspect", headers=AUTH, json={"connection": "Server=x;"}).status_code == 400
    dup = {"schema": "main", "name": "customers", "kind": "table", "source": "same"}
    assert client.post("/api/db/load", headers=AUTH, json={"connection": source_db, "items": [dup, dup]}).status_code == 400
    assert client.post("/api/db/inspect", json={"connection": source_db}).status_code == 401


def _local_sql_server():
    try:
        import pyodbc
        driver = next(d for d in pyodbc.drivers() if dbconnect._DRIVER_FAMILY.match(d))
        pyodbc.connect(f"Driver={{{driver}}};Server=localhost;Database=tempdb;Trusted_Connection=yes;"
                       "TrustServerCertificate=yes;", timeout=3).close()
        return driver
    except Exception:
        return None


@pytest.mark.skipif(not _local_sql_server(), reason="no local SQL Server reachable with Windows login")
@pytest.mark.parametrize("connection", [
    "Driver={ODBC Driver 18 for SQL Server};Server=localhost;Database=tempdb;Trusted_Connection=yes;TrustServerCertificate=yes;",
    "jdbc:sqlserver://localhost;databaseName=tempdb;integratedSecurity=true;trustServerCertificate=true;loginTimeout=5;",
])
def test_real_sql_server(store, connection):
    import pyodbc
    driver = _local_sql_server()
    setup = pyodbc.connect(f"Driver={{{driver}}};Server=localhost;Database=tempdb;Trusted_Connection=yes;TrustServerCertificate=yes;", autocommit=True)
    c = setup.cursor()
    c.execute("IF SCHEMA_ID('eyetable_pytest') IS NULL EXEC('CREATE SCHEMA eyetable_pytest')")
    c.execute("IF OBJECT_ID('eyetable_pytest.people') IS NOT NULL DROP TABLE eyetable_pytest.people")
    c.execute("CREATE TABLE eyetable_pytest.people (id int, name nvarchar(50), joined datetimeoffset, balance decimal(10,2))")
    c.execute("INSERT INTO eyetable_pytest.people VALUES (1, N'Åsa', '2026-01-02T03:04:05+02:00', 12.50), (2, N'陈', NULL, NULL)")
    try:
        engine = Engine(store, environ={"EYETABLE_SECRET_KEY": KEY})
        [result] = engine.load_db_objects(connection, [{"schema": "eyetable_pytest", "name": "people", "kind": "table", "source": "people"}])
        assert result["status"] == "succeeded" and result["rows"] == 2, result
        assert [r["name"] for r in store.read_table("people__data")] == ["Åsa", "陈"]
        assert [r.rows for r in engine.reload_source("people")] == [2]
    finally:
        c.execute("DROP TABLE IF EXISTS eyetable_pytest.people")
        c.execute("DROP SCHEMA IF EXISTS eyetable_pytest")


# -- JDBC strings and URLs ------------------------------------------------------------

JDBC = ("sqlserver://signetic-sql.database.windows.net:1433;database=signetic_multitenant;user=sql-admin@signetic-sql;"
        "password={p;a}}ss};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;"
        "loginTimeout=30;")


@pytest.fixture
def odbc17(monkeypatch):
    import pyodbc
    monkeypatch.setattr(pyodbc, "drivers", lambda: ["SQL Server", "ODBC Driver 17 for SQL Server"])


@pytest.mark.parametrize("text", [JDBC, "jdbc:" + JDBC, "JDBC:" + JDBC])
def test_jdbc_strings_become_odbc(text, odbc17):
    p = dbconnect.prepare(text, KEY)
    assert dict(dbconnect.parse_odbc(p.config["odbc"])) == {
        "Driver": "ODBC Driver 17 for SQL Server", "Server": "tcp:signetic-sql.database.windows.net,1433",
        "Database": "signetic_multitenant", "Uid": "sql-admin@signetic-sql", "Encrypt": "yes",
        "TrustServerCertificate": "no"}
    assert p.password == "p;a}ss" and "p;a}ss" not in str(p.config)
    assert decrypt_secrets(p.config, KEY)["password"] == "p;a}ss"
    assert p.config["login_timeout"] == 30 and not p.warnings
    assert p.label == "signetic-sql.database.windows.net/signetic_multitenant"
    engine, warnings = dbconnect.make_engine(p.config, p.password)
    assert not warnings  # a driver the string didn't name isn't worth a warning
    engine.dispose()


def test_jdbc_details(odbc17):
    p = dbconnect.prepare(r"sqlserver://db1\sales;databaseName=crm;integratedSecurity=true;sendTimeAsDatetime=false;", KEY)
    pairs = dict(dbconnect.parse_odbc(p.config["odbc"]))
    assert pairs["Server"] == r"db1\sales" and pairs["Database"] == "crm" and pairs["Trusted_Connection"] == "yes"
    assert p.password is None and "password" not in p.config
    assert "sendtimeasdatetime" in p.warnings[0]
    p = dbconnect.prepare("sqlserver://;serverName=db2;portNumber=1500;database=crm;hostNameInCertificate=other.example.com", KEY)
    pairs = dict(dbconnect.parse_odbc(p.config["odbc"]))
    assert pairs["Server"] == "tcp:db2,1500" and pairs["HostNameInCertificate"] == "other.example.com"


@pytest.mark.parametrize("bad, message", [
    (JDBC.replace("{p;a}}ss}", "{your_password_here}"), "placeholder"),
    (JDBC.replace("{p;a}}ss}", "xxxxxxxx"), "placeholder"),
    ("sqlserver://;database=crm;", "no server"),
])
def test_unusable_jdbc_strings(bad, message, odbc17):
    with pytest.raises(ValueError, match=message):
        dbconnect.prepare(bad, KEY)


def test_urls_get_the_installed_driver_and_lose_their_password():
    for text in ("postgresql://bob:s3cret@db.local:5432/crm", "postgres://bob:s3cret@db.local:5432/crm",
                 "jdbc:postgresql://db.local:5432/crm?user=bob&password=s3cret"):
        p = dbconnect.prepare(text, KEY)
        assert p.config["url"] == "postgresql+psycopg://bob@db.local:5432/crm" and p.password == "s3cret"
        engine, _ = dbconnect.make_engine(p.config, p.password)
        assert engine.url.password == "s3cret"
        engine.dispose()


def test_unknown_kinds_of_connection_are_explained(store):
    p = dbconnect.prepare("nosuch://bob:s3cret@db.local/crm", KEY)
    with pytest.raises(ValueError, match="doesn't know nosuch://"):
        dbconnect.make_engine(p.config, p.password)
    client = TestClient(create_app(Engine(store, environ={"EYETABLE_SECRET_KEY": KEY}), TOKEN))
    r = client.post("/api/db/inspect", headers=AUTH, json={"connection": "nosuch://bob:s3cret@db.local/crm"})
    assert r.status_code == 400 and "doesn't know" in r.json()["detail"] and "s3cret" not in r.text


def test_a_crash_still_reaches_the_page(store, monkeypatch):
    """Without CORS headers the browser hides the answer and the page says the API is unreachable."""
    def boom(*_):
        raise RuntimeError("s3cret detail")
    monkeypatch.setattr(dbconnect, "prepare", boom)
    client = TestClient(create_app(Engine(store, environ={"EYETABLE_SECRET_KEY": KEY}), TOKEN))
    r = client.post("/api/db/inspect", headers={**AUTH, "Origin": "https://tootallted.github.io"}, json={"connection": "x"})
    assert r.status_code == 500 and "RuntimeError" in r.json()["detail"] and "s3cret" not in r.text
    assert r.headers["access-control-allow-origin"] == "https://tootallted.github.io"
