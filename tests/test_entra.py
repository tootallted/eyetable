"""Microsoft Entra (MFA) sign-in for database connections, with Microsoft's side stood in for."""

import time

import pytest
from cryptography.fernet import Fernet
from fastapi.testclient import TestClient

from eyetable import dbconnect, entra
from eyetable.api import create_app
from eyetable.engine import Engine

TOKEN = "test-token-0123456789"
AUTH = {"Authorization": f"Bearer {TOKEN}"}
KEY = Fernet.generate_key().decode()
DATAVERSE = ("Driver={ODBC Driver 18 for SQL Server};Server=tcp:signetic-healthhub-prod.crm.dynamics.com,5558;"
             "Authentication=ActiveDirectoryInteractive;UID=ted.clark@healthhub.signetic.com;Encrypt=yes;"
             "TrustServerCertificate=no;Connection Timeout=30;")


class Microsoft:
    """Stands in for Microsoft's sign-in service: who has signed in, and whether their sign-in still works."""

    def __init__(self):
        self.accounts: dict[str, dict] = {}
        self.signs_in_as = "ted.clark@healthhub.signetic.com"
        self.expired = False
        self.flows = 0

    def app(self, client_id, authority, cache):
        ms = self

        class App:
            def get_accounts(self, username=None):
                return [a for u, a in ms.accounts.items() if username is None or u == username.lower()]

            def acquire_token_silent(self, scopes, account):
                if ms.expired:
                    return {"error": "invalid_grant", "error_description": "AADSTS700082: The refresh token has expired.\nTrace ID: x"}
                return {"access_token": f"token-for-{account['username']}-{scopes[0]}"}

            def initiate_device_flow(self, scopes):
                ms.flows += 1
                return {"user_code": "ABCD-1234", "verification_uri": "https://microsoft.com/devicelogin", "expires_in": 900,
                        "message": "To sign in, use a web browser to open https://microsoft.com/devicelogin and enter ABCD-1234."}

            def acquire_token_by_device_flow(self, flow):
                if ms.signs_in_as is None:
                    return {"error": "authorization_declined", "error_description": "The user declined.\nmore"}
                ms.accounts[ms.signs_in_as.lower()] = {"username": ms.signs_in_as}
                return {"access_token": "t", "id_token_claims": {"preferred_username": ms.signs_in_as}}

        return App()


@pytest.fixture
def microsoft(store):
    ms = Microsoft()
    entra.set_provider(entra.EntraTokens(store, KEY, app_factory=ms.app))
    return ms


def wait_for(tokens, flow_id):
    for _ in range(100):
        state = tokens.status(flow_id)
        if state["status"] != "pending":
            return state
        time.sleep(0.02)
    raise AssertionError("the sign-in never finished")


def test_what_a_token_is_for():
    assert entra.scope_for("tcp:signetic-healthhub-prod.crm.dynamics.com,5558") == "https://signetic-healthhub-prod.crm.dynamics.com/.default"
    assert entra.scope_for("org.crm4.dynamics.com") == "https://org.crm4.dynamics.com/.default"
    assert entra.scope_for("tcp:x.database.windows.net,1433") == "https://database.windows.net/.default"
    assert entra.authority_for("Ted@HealthHub.Signetic.com") == "https://login.microsoftonline.com/healthhub.signetic.com"
    assert entra.authority_for(None) == "https://login.microsoftonline.com/organizations"
    packed = entra.token_struct("ab")
    assert packed == b"\x04\x00\x00\x00a\x00b\x00"


def test_an_interactive_string_becomes_a_sign_in(microsoft):
    p = dbconnect.prepare(DATAVERSE, KEY)
    pairs = dict(dbconnect.parse_odbc(p.config["odbc"]))
    assert "Authentication" not in pairs and "UID" not in pairs and pairs["Server"].endswith(",5558")
    assert p.config["entra"] == {"username": "ted.clark@healthhub.signetic.com",
                                 "scope": "https://signetic-healthhub-prod.crm.dynamics.com/.default",
                                 "authority": "https://login.microsoftonline.com/healthhub.signetic.com"}
    assert "password" not in p.config and p.label == "signetic-healthhub-prod.crm.dynamics.com"
    jdbc = dbconnect.prepare("sqlserver://x.database.windows.net:1433;database=db;user=a@b.com;authentication=ActiveDirectoryInteractive", KEY)
    assert jdbc.config["entra"]["scope"] == "https://database.windows.net/.default"
    assert dbconnect.prepare("Driver={ODBC Driver 18 for SQL Server};Server=x;Uid=u;Pwd=p;", KEY).config.get("entra") is None


def test_every_connection_gets_a_fresh_token(microsoft, monkeypatch):
    import pyodbc
    monkeypatch.setattr(pyodbc, "drivers", lambda: ["ODBC Driver 17 for SQL Server"])
    p = dbconnect.prepare(DATAVERSE, KEY)
    engine, _ = dbconnect.make_engine(p.config)
    cparams: dict = {}
    with pytest.raises(entra.EntraSignInNeeded, match="sign in with Microsoft as ted.clark"):
        for listener in engine.dialect.dispatch.do_connect:
            listener(engine.dialect, None, [], cparams)
    microsoft.accounts["ted.clark@healthhub.signetic.com"] = {"username": "ted.clark@healthhub.signetic.com"}
    for listener in engine.dialect.dispatch.do_connect:
        listener(engine.dialect, None, [], cparams)
    token = "token-for-ted.clark@healthhub.signetic.com-https://signetic-healthhub-prod.crm.dynamics.com/.default"
    assert cparams["attrs_before"] == {1256: entra.token_struct(token)}
    microsoft.expired = True
    with pytest.raises(entra.EntraSignInNeeded, match="has expired; sign in again .AADSTS700082"):
        for listener in engine.dialect.dispatch.do_connect:
            listener(engine.dialect, None, [], cparams)


def test_signing_in(store, microsoft):
    tokens = entra.provider()
    request = entra.sign_in_request("ted.clark@healthhub.signetic.com", "tcp:signetic-healthhub-prod.crm.dynamics.com,5558")
    assert not tokens.signed_in(request["username"], request["authority"])
    started = tokens.start(**request)
    assert started["user_code"] == "ABCD-1234" and started["verification_uri"] == "https://microsoft.com/devicelogin"
    state = wait_for(tokens, started["flow_id"])
    assert state["status"] == "done" and state["account"] == "ted.clark@healthhub.signetic.com" and not state["error"]
    assert tokens.signed_in(request["username"], request["authority"])
    assert tokens.token(**request).startswith("token-for-ted.clark")

    microsoft.signs_in_as = "someone.else@healthhub.signetic.com"
    state = wait_for(tokens, tokens.start(**request)["flow_id"])
    assert "but the connection string's user is ted.clark" in state["error"]
    microsoft.signs_in_as = None
    state = wait_for(tokens, tokens.start(**request)["flow_id"])
    assert state == {**state, "status": "failed", "error": "The user declined."}
    with pytest.raises(KeyError):
        tokens.status("nope")


def test_without_a_secret_key_nothing_is_kept(store, microsoft):
    tokens = entra.EntraTokens(store, None, app_factory=microsoft.app)
    with pytest.raises(ValueError, match="EYETABLE_SECRET_KEY"):
        tokens.start(None, "https://database.windows.net/.default", "https://login.microsoftonline.com/organizations")


def test_endpoints(store, microsoft):
    web = TestClient(create_app(Engine(store, environ={"EYETABLE_SECRET_KEY": KEY}), TOKEN))
    entra.provider()._app_factory = microsoft.app  # create_app made a fresh one
    r = web.post("/api/db/inspect", headers=AUTH, json={"connection": DATAVERSE})
    assert r.status_code == 409
    detail = r.json()["detail"]
    assert "sign in with Microsoft as ted.clark" in detail["message"]
    assert detail["entra"]["scope"] == "https://signetic-healthhub-prod.crm.dynamics.com/.default"

    started = web.post("/api/entra/sign-in", headers=AUTH, json=detail["entra"]).json()
    assert started["user_code"] == "ABCD-1234"
    for _ in range(100):
        state = web.get(f"/api/entra/sign-in/{started['flow_id']}", headers=AUTH).json()
        if state["status"] != "pending":
            break
        time.sleep(0.02)
    assert state["status"] == "done"
    assert web.get("/api/entra/sign-in/nope", headers=AUTH).status_code == 404
    bad = {**detail["entra"], "authority": "https://evil.example.com/x"}
    assert web.post("/api/entra/sign-in", headers=AUTH, json=bad).status_code == 400
    assert web.post("/api/entra/sign-in", json=detail["entra"]).status_code == 401
