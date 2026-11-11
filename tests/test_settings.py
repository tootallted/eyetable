"""Changing how an existing source connects, without making a new one."""

import sqlite3
import urllib.parse

import httpx
import pytest
from cryptography.fernet import Fernet
from fastapi.testclient import TestClient

from eyetable import curl
from eyetable.api import create_app
from eyetable.connectors import build_connector
from eyetable.connectors.curl_source import CurlConnector
from eyetable.credentials import decrypt_secrets
from eyetable.engine import Engine
from test_curl_cert import CLIENT, CLIENT_KEY, key_pem, pem_of
from eyetable import tls

TOKEN = "test-token-0123456789"
AUTH = {"Authorization": f"Bearer {TOKEN}"}
KEY = Fernet.generate_key().decode()


class Api:
    """Accepts one token at a time, like an API whose token was rotated."""

    def __init__(self):
        self.token = "tok_old"

    def __call__(self, request):
        if request.headers.get("authorization") != f"Bearer {self.token}":
            return httpx.Response(401)
        q = dict(urllib.parse.parse_qsl(request.url.query.decode()))
        return httpx.Response(200, json={"value": [{"day": q.get("day", "?"), "host": request.url.host}]})


@pytest.fixture
def api():
    return Api()


def engine_for(store, api):
    def factory(type_, config):
        if type_ == "curl":
            return CurlConnector(config, client=httpx.Client(transport=httpx.MockTransport(api)))
        return build_connector(type_, config)
    return Engine(store, factory, environ={"EYETABLE_SECRET_KEY": KEY})


def http(api):
    return httpx.Client(transport=httpx.MockTransport(api))


COMMAND = "curl 'https://api.test/jobs?day=2026-11-04&api_key=key_1' -H 'Authorization: Bearer tok_old'"


def database(path, rows):
    conn = sqlite3.connect(path)
    conn.executescript("CREATE TABLE people (id INTEGER, name TEXT);")
    conn.executemany("INSERT INTO people VALUES (?, ?)", rows)
    conn.commit()
    conn.close()
    return f"sqlite:///{path.as_posix()}"


# -- curl sources ---------------------------------------------------------------------------

def test_where_each_secret_goes(store, api):
    engine_for(store, api).load_curl(COMMAND, "jobs", client=http(api))
    config = store.get_source("jobs").config
    assert curl.secret_places(config) == [
        {"name": "secret_1", "where": "the api_key query parameter"},
        {"name": "secret_2", "where": "the Authorization header"},
    ]
    basic = curl.prepare("curl -u bob:pw https://api.test/x -d 'token=t1'", KEY)[1]
    assert {p["where"] for p in curl.secret_places(basic)} == {"the -u password", "the request body"}


def test_a_rotated_token(store, api):
    engine = engine_for(store, api)
    engine.load_curl(COMMAND, "jobs", client=http(api))
    api.token = "tok_new"
    [failed] = engine.reload_source("jobs")
    assert failed.status == "failed" and "401 Unauthorized (api.test/jobs)" in failed.error
    assert "key_1" not in failed.error  # the query string, with its key, stays out of the message

    with pytest.raises(ValueError, match="401"):  # a wrong token is checked, and nothing changes
        engine.update_curl_source("jobs", secrets={"secret_2": "Bearer tok_wrong"})
    before = store.get_source("jobs").config
    assert decrypt_secrets(before["variables"]["secret_2"], KEY) == "Bearer tok_old"

    assert engine.update_curl_source("jobs", secrets={"secret_2": " tok_new ", "secret_1": ""}) == []  # just the token
    config = store.get_source("jobs").config
    assert decrypt_secrets(config["variables"]["secret_2"], KEY) == "Bearer tok_new"
    assert decrypt_secrets(config["variables"]["secret_1"], KEY) == "key_1"  # blank kept it
    assert "tok_new" not in str(config)
    assert [r.status for r in engine.reload_source("jobs")] == ["succeeded"]
    with pytest.raises(ValueError, match="no secret_9"):
        engine.update_curl_source("jobs", secrets={"secret_9": "x"})


def test_a_new_command_keeps_the_source_settings(store, api):
    engine = engine_for(store, api)
    engine.load_curl(COMMAND, "jobs", client=http(api), page_size=50, records_field="value")
    src = store.get_source("jobs")
    store.save_source("jobs", "curl", {**src.config, "statements": curl.clean_statements(src.config, {"parts": [
        {"target": "query", "name": "day", "kind": "dates", "from": "2026-11-01", "to": "2026-11-02"}]})})
    before = store.get_source("jobs").config

    engine.update_curl_source("jobs", command=COMMAND.replace("api.test", "api2.test"))
    config = store.get_source("jobs").config
    assert curl.stored_request(config)["url"].startswith("https://api2.test/jobs")
    assert config["records_path"] == before["records_path"] == {"data": "value"}
    assert config.get("pagination") == before.get("pagination")
    assert config["statements"] == before["statements"]
    [result] = engine.reload_source("jobs")
    assert result.rows == 2 and {r["host"] for r in store.read_table("jobs__data")} == {"api2.test"}

    warnings = engine.update_curl_source("jobs", command="curl 'https://api2.test/jobs' -H 'Authorization: Bearer tok_old'")
    assert "no longer fits" in warnings[0] and "statements" not in store.get_source("jobs").config


def test_certificates(store, api):
    engine = engine_for(store, api)
    engine.load_curl(COMMAND, "jobs", client=http(api))
    certificate = tls.client_cert(crt=pem_of(CLIENT), key=key_pem(CLIENT_KEY))
    engine.update_curl_source("jobs", certificate=dict(certificate))
    stored = store.get_source("jobs").config["client_cert"]
    assert stored["host"] == "api.test" and stored["pem"].startswith("eyetable-enc:")
    with pytest.raises(ValueError, match="set for other.test"):
        engine.update_curl_source("jobs", certificate={**certificate, "host": "other.test"})
    warnings = engine.update_curl_source("jobs", command=COMMAND.replace("api.test", "api2.test"))
    assert "won't be sent" in warnings[0]
    engine.update_curl_source("jobs", remove_certificate=True)
    assert "client_cert" not in store.get_source("jobs").config


def test_saving_without_a_check(store, api):
    engine = engine_for(store, api)
    engine.load_curl(COMMAND, "jobs", client=http(api))
    api.token = "nobody"
    warnings = engine.update_curl_source("jobs", secrets={"secret_2": "Bearer later"}, test=False)
    assert "without checking" in warnings[0]
    assert decrypt_secrets(store.get_source("jobs").config["variables"]["secret_2"], KEY) == "Bearer later"


# -- database sources -----------------------------------------------------------------------------

def test_a_moved_database(store, tmp_path):
    engine = Engine(store, environ={"EYETABLE_SECRET_KEY": KEY})
    old = database(tmp_path / "old.db", [(1, "Ada")])
    engine.load_db_objects(old, [{"schema": "main", "name": "people", "kind": "table", "source": "people"}])
    new = database(tmp_path / "new.db", [(1, "Ada"), (2, "Grace")])

    empty = tmp_path / "empty.db"
    sqlite3.connect(empty).close()
    with pytest.raises(ValueError, match="wasn't found"):  # checked before saving
        engine.update_db_source("people", connection=f"sqlite:///{empty.as_posix()}")
    assert store.get_source("people").config["url"].endswith("/old.db")

    engine.update_db_source("people", connection=new)
    assert store.get_source("people").config["url"].endswith("/new.db")
    assert [r.rows for r in engine.reload_source("people")] == [2]
    with pytest.raises(ValueError, match="wasn't found"):
        engine.update_db_source("people", table="nobody")


def test_passwords_are_kept_unless_replaced(store):
    engine = Engine(store, environ={"EYETABLE_SECRET_KEY": KEY})
    store.save_source("crm", "dbtable", {"url": "postgresql+psycopg://bob@db1.local/crm", "password": "x", "schema": "public",
                                         "table": "people", "kind": "table", "label": "db1.local/crm"})
    from eyetable.credentials import encrypt_secret
    store.save_source("crm", "dbtable", {**store.get_source("crm").config, "password": encrypt_secret("old pw", KEY)})

    engine.update_db_source("crm", connection="postgresql://bob@db2.local:5432/crm2", test=False)
    config = store.get_source("crm").config
    assert config["url"] == "postgresql+psycopg://bob@db2.local:5432/crm2" and config["label"] == "db2.local/crm2"
    assert decrypt_secrets(config["password"], KEY) == "old pw" and config["table"] == "people"

    engine.update_db_source("crm", password="new pw", schema="sales", test=False)
    config = store.get_source("crm").config
    assert decrypt_secrets(config["password"], KEY) == "new pw" and config["schema"] == "sales"

    odbc = ("Driver={ODBC Driver 18 for SQL Server};Server=tcp:x.database.windows.net,1433;Database=crm;"
            "Uid=sql-admin;Pwd={from string};Encrypt=yes;")
    engine.update_db_source("crm", connection=odbc, test=False)
    config = store.get_source("crm").config
    assert "Pwd" not in config["odbc"] and decrypt_secrets(config["password"], KEY) == "from string"
    with pytest.raises(ValueError, match="placeholder"):
        engine.update_db_source("crm", connection=odbc.replace("{from string}", "{your_password_here}"), test=False)


# -- endpoints ----------------------------------------------------------------------------------------

def test_endpoints(store, api, tmp_path):
    engine = engine_for(store, api)
    web = TestClient(create_app(engine, TOKEN))
    engine.load_curl(COMMAND, "jobs", client=http(api))
    engine.load_db_objects(database(tmp_path / "crm.db", [(1, "Ada")]),
                           [{"schema": "main", "name": "people", "kind": "table", "source": "people"}])
    engine.ingest_upload("plain", "p.csv", b"a\n1\n", create=True)

    jobs = web.get("/api/sources/jobs/settings", headers=AUTH).json()
    assert jobs["type"] == "curl" and [s["where"] for s in jobs["secrets"]] == ["the api_key query parameter", "the Authorization header"]
    assert [s["scheme"] for s in jobs["secrets"]] == [None, "Bearer"]
    assert jobs["certificate"] is None and "tok_old" not in str(jobs) and "key_1" not in str(jobs)
    people = web.get("/api/sources/people/settings", headers=AUTH).json()
    assert people["format"] == "url" and people["table"] == "people" and people["has_password"] is False
    assert web.get("/api/sources/plain/settings", headers=AUTH).json() == {
        "type": "file", "name": "plain", "folder": None, "pattern": None, "filename": "p.csv"}

    api.token = "tok_new"
    r = web.put("/api/sources/jobs/settings", headers=AUTH, json={"secrets": {"secret_2": "Bearer nope"}})
    assert r.status_code == 400 and "401" in r.json()["detail"] and "nope" not in r.text
    r = web.put("/api/sources/jobs/settings", headers=AUTH, json={"secrets": {"secret_2": "Bearer tok_new"}})
    assert r.status_code == 200 and r.json()["tested"] is True and r.json()["warnings"] == []
    assert web.post("/api/sources/jobs/reload", headers=AUTH, json={}).status_code == 200

    r = web.put("/api/sources/people/settings", headers=AUTH, json={"table": "nobody"})
    assert r.status_code == 400 and "wasn't found" in r.json()["detail"]
    r = web.put("/api/sources/people/settings", headers=AUTH, json={"schema": "main", "table": "people"})
    assert r.status_code == 200 and r.json()["schema"] == "main"
    unchanged = web.put("/api/sources/plain/settings", headers=AUTH, json={})  # a file source: nothing asked, nothing changed
    assert unchanged.status_code == 200 and unchanged.json()["name"] == "plain" and unchanged.json()["pattern"] is None
    assert web.put("/api/sources/plain/settings", headers=AUTH, json={"folder": "C:\nowhere\at\all", "pattern": "a_*.csv"}).status_code == 400
    assert web.put("/api/sources/jobs/settings", json={}).status_code == 401
