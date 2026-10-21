import datetime as dt
import time
import urllib.parse

import httpx
import pytest
import sqlalchemy as sa
from cryptography.fernet import Fernet
from fastapi.testclient import TestClient

import eyetable.api as api_module
from eyetable import gsheets
from eyetable.api import create_app
from eyetable.engine import Engine
from eyetable.google_auth import SHEETS_SCOPE, GoogleAuthError, GoogleConnection
from test_gsheets import LINK, fake_fetch

TOKEN = "test-token-0123456789"
AUTH = {"Authorization": f"Bearer {TOKEN}"}
KEY = Fernet.generate_key().decode()
SCOPES = f"openid https://www.googleapis.com/auth/userinfo.email {SHEETS_SCOPE}"


class FakeGoogle:
    """Google's token, revoke and userinfo endpoints."""

    def __init__(self):
        self.calls = []
        self.refresh_issued = True
        self.scope = SCOPES
        self.refresh_fails = False
        self.counter = 0

    def __call__(self, request: httpx.Request) -> httpx.Response:
        form = dict(urllib.parse.parse_qsl(request.content.decode())) if request.content else {}
        self.calls.append((request.url.path, form))
        if request.url.path == "/token":
            assert form["client_secret"] == "shh" and form["client_id"] == "cid"
            if form["grant_type"] == "authorization_code":
                assert form["redirect_uri"] == "postmessage" and form["code"] == "one-time-code"
                body = {"access_token": "at-1", "expires_in": 3599, "scope": self.scope, "token_type": "Bearer"}
                if self.refresh_issued:
                    body["refresh_token"] = "rt-secret"
                return httpx.Response(200, json=body)
            assert form["refresh_token"] == "rt-secret"
            if self.refresh_fails:
                return httpx.Response(400, json={"error": "invalid_grant", "error_description": "Token has been expired or revoked."})
            self.counter += 1
            return httpx.Response(200, json={"access_token": f"at-refreshed-{self.counter}", "expires_in": 3599, "scope": self.scope})
        if request.url.path == "/revoke":
            return httpx.Response(200)
        if request.url.path == "/oauth2/v3/userinfo":
            return httpx.Response(200, json={"email": "ted.clark@adventag.com"})
        return httpx.Response(404)


@pytest.fixture
def google_api():
    return FakeGoogle()


@pytest.fixture
def google(store, google_api):
    return GoogleConnection(store, "cid", "shh", KEY, client=httpx.Client(transport=httpx.MockTransport(google_api)))


def test_connect_stores_the_refresh_token_encrypted(google, store):
    status = google.connect("one-time-code")
    assert status["connected"] and status["email"] == "ted.clark@adventag.com"
    row = store.get_google_connection()
    assert "rt-secret" not in row["refresh_token"]
    assert Fernet(KEY.encode()).decrypt(row["refresh_token"].encode()) == b"rt-secret"
    with store.db.connect() as conn:  # nowhere in the table in plain text
        assert "rt-secret" not in str(conn.execute(sa.select(store.google_connection)).all())


def test_access_tokens_are_cached_then_refreshed(google, google_api):
    google.connect("one-time-code")
    assert google.access_token() == "at-1"  # from the exchange, still fresh
    google._access = ("at-1", dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=1))  # expired
    assert google.access_token() == "at-refreshed-1"
    assert google.access_token() == "at-refreshed-1"  # cached again
    assert sum(1 for path, form in google_api.calls if form.get("grant_type") == "refresh_token") == 1


def test_revoked_authorization_is_reported(google, google_api, store):
    google.connect("one-time-code")
    google._access = None
    google_api.refresh_fails = True
    with pytest.raises(GoogleAuthError, match="expired or revoked"):
        google.access_token()
    status = google.status()
    assert not status["connected"] and status["status"] == "broken"
    with pytest.raises(GoogleAuthError, match="needs reconnecting"):
        google.access_token()


@pytest.mark.parametrize("change, message", [
    ({"refresh_issued": False}, "already authorized"),
    ({"scope": "openid email"}, "read access to Sheets"),
])
def test_connect_refusals(google, google_api, store, change, message):
    for k, v in change.items():
        setattr(google_api, k, v)
    with pytest.raises(GoogleAuthError, match=message):
        google.connect("one-time-code")
    assert store.get_google_connection() is None


def test_disconnect_revokes_and_forgets(google, google_api, store):
    google.connect("one-time-code")
    google.disconnect()
    assert ("/revoke", {"token": "rt-secret"}) in google_api.calls
    assert store.get_google_connection() is None and not google.status()["connected"]


def test_reconnecting_revokes_the_old_token(google, google_api):
    google.connect("one-time-code")
    google.connect("one-time-code")
    assert [c for c in google_api.calls if c[0] == "/revoke"] == [("/revoke", {"token": "rt-secret"})]


def test_not_configured(store):
    google = GoogleConnection(store, "cid", None, None)
    assert not google.configured and google.missing_settings() == ["GOOGLE_CLIENT_SECRET", "EYETABLE_SECRET_KEY"]
    client = TestClient(create_app(Engine(store), TOKEN, google=google))
    assert client.get("/api/google", headers=AUTH).json()["configured"] is False
    r = client.post("/api/google/connect", headers=AUTH, json={"code": "x"})
    assert r.status_code == 503 and "GOOGLE_CLIENT_SECRET" in r.json()["detail"]


def test_scheduled_group_reads_a_private_sheet_with_the_stored_authorization(store, google, monkeypatch):
    seen = []

    def signed_in(url, token, client=None, max_bytes=0):
        seen.append(token)
        return fake_fetch()(url)

    def anonymous(url, client=None, max_bytes=0):
        raise gsheets.SheetError("this spreadsheet is private", needs_sign_in=True)

    monkeypatch.setattr(api_module, "fetch_workbook_signed_in", signed_in)
    monkeypatch.setattr(api_module, "fetch_workbook", anonymous)
    engine = Engine(store)
    app = create_app(engine, TOKEN, google=google)
    client = TestClient(app)

    # signed out and not connected: private sheets are refused, with a sign-in hint
    r = client.post("/api/gsheets", headers=AUTH, json={"url": LINK})
    assert r.status_code == 403

    # connect once through the page's code flow, then everything works without a token
    assert client.post("/api/google/connect", headers=AUTH, json={"code": "one-time-code"}).json()["connected"]
    assert client.post("/api/gsheets", headers=AUTH, json={"url": LINK}).status_code == 200
    groups = app.state.group_runner
    group = groups.create("Ledger", ["general_ledger_2026_q1_actuals"],
                          schedule={"type": "daily", "times": ["06:00"], "days": list(range(7)), "timezone": "UTC"})
    store.update_group(group.id, next_run_at=dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=5))
    [run_id] = groups.tick()
    for _ in range(100):
        run = store.get_group_run(run_id)
        if run.status != "running":
            break
        time.sleep(0.05)
    assert run.status == "succeeded" and run.trigger == "schedule"
    assert seen and all(t == "at-1" for t in seen)  # the stored authorization, no one signed in

    status = client.get("/api/google", headers=AUTH).json()
    assert status["email"] == "ted.clark@adventag.com" and status["last_used_at"]
    assert client.delete("/api/google", headers=AUTH).json()["connected"] is False
