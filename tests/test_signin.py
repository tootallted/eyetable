"""Signing in to the page with Google or Microsoft, and reading OneDrive / SharePoint files as yourself."""

import io
import time

import httpx
import jwt
import openpyxl
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi.testclient import TestClient

from eyetable import msfiles
from eyetable.api import create_app
from eyetable.engine import Engine
from eyetable.gsheets import SheetError
from eyetable.signin import SignIn, SignInError

TOKEN = "test-token-0123456789"
GOOGLE_ID = "123-test.apps.googleusercontent.com"
MS_ID = "11111111-2222-3333-4444-555555555555"
TENANT = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)


def id_token(**claims):
    now = int(time.time())
    body = {"aud": MS_ID, "iss": f"https://login.microsoftonline.com/{TENANT}/v2.0", "tid": TENANT,
            "iat": now, "nbf": now, "exp": now + 3600, "preferred_username": "Ted@Example.com", "name": "Ted"}
    body.update(claims)
    return jwt.encode({k: v for k, v in body.items() if v is not None}, KEY, algorithm="RS256")


def google_api(email="pat@example.com", aud=GOOGLE_ID, verified="true", status=200):
    def answer(request):
        if request.url.params.get("access_token") != "g-token-abcdef":
            return httpx.Response(400, json={"error": "invalid_token"})
        return httpx.Response(status, json={"aud": aud, "azp": aud, "email": email, "email_verified": verified})
    return httpx.Client(transport=httpx.MockTransport(answer))


def signin(**extra):
    options = {"allowed": "ted@example.com, @example.com", "google_client_id": GOOGLE_ID, "ms_client_id": MS_ID,
               "ms_tenant_id": TENANT, "client": google_api()}
    options.update(extra)
    s = SignIn(TOKEN, **options)
    s._signing_key = lambda token: KEY.public_key()  # instead of fetching Microsoft's published keys
    return s


# -- who may sign in ---------------------------------------------------------------

def test_the_allow_list_takes_addresses_and_domains():
    s = SignIn(TOKEN, allowed="ted@example.com; @Contoso.com")
    assert s.is_allowed("TED@example.com") and s.is_allowed("ann@contoso.com")
    assert not s.is_allowed("ann@example.com") and not s.is_allowed("ann@notcontoso.com")


def test_with_no_allow_list_only_the_token_gets_in():
    s = SignIn(TOKEN)
    assert s.options()["enabled"] is False
    with pytest.raises(SignInError, match="allow list") as exc:
        s.sign_in("google", "g-token-abcdef")
    assert exc.value.status == 403


# -- Microsoft ID tokens -----------------------------------------------------------------

def test_a_microsoft_id_token_signs_in_by_its_email():
    session = signin().sign_in("microsoft", id_token())
    assert (session["email"], session["provider"], session["name"]) == ("ted@example.com", "microsoft", "Ted")


@pytest.mark.parametrize("claims, message", [
    ({"aud": "someone-else"}, "Microsoft sign-in not accepted"),
    ({"exp": int(time.time()) - 600}, "expired"),
    ({"tid": "ffffffff-0000-0000-0000-000000000000",
      "iss": "https://login.microsoftonline.com/ffffffff-0000-0000-0000-000000000000/v2.0"}, "different organization"),
    ({"iss": "https://evil.example/v2.0"}, "wasn't issued by Entra ID"),
    ({"preferred_username": None}, "no email-style sign-in name"),
])
def test_bad_microsoft_tokens_are_refused(claims, message):
    with pytest.raises(SignInError, match=message):
        signin().sign_in("microsoft", id_token(**claims))


def test_a_token_signed_by_another_key_is_refused():
    other = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    forged = jwt.encode(jwt.decode(id_token(), options={"verify_signature": False}), other, algorithm="RS256")
    with pytest.raises(SignInError, match="not accepted"):
        signin().sign_in("microsoft", forged)


def test_someone_off_the_list_is_refused():
    with pytest.raises(SignInError, match="isn't allowed") as exc:
        signin(allowed="ted@example.com").sign_in("microsoft", id_token(preferred_username="eve@example.com"))
    assert exc.value.status == 403


# -- Google access tokens ----------------------------------------------------------------

def test_a_google_token_signs_in_by_its_verified_email():
    assert signin().sign_in("google", "g-token-abcdef")["email"] == "pat@example.com"


@pytest.mark.parametrize("client, message", [
    (google_api(aud="another-app"), "different app"),
    (google_api(verified="false"), "no verified email"),
    (google_api(status=400), "didn't accept"),
])
def test_bad_google_tokens_are_refused(client, message):
    with pytest.raises(SignInError, match=message):
        signin(client=client).sign_in("google", "g-token-abcdef")


# -- sessions -----------------------------------------------------------------------------

def test_a_session_names_its_person_until_it_ends():
    s = signin()
    session = s.sign_in("microsoft", id_token())["session"]
    assert s.check_session(session).email == "ted@example.com"
    assert s.check_session(session, now=time.time() + 13 * 3600) is None  # 12 hours by default
    assert s.check_session(session[:-2] + "xx") is None  # tampered with
    assert signin(allowed="pat@example.com").check_session(session) is None  # taken off the list
    other = SignIn("another-api-token-123456", allowed="@example.com")
    assert other.check_session(session) is None  # a new API token ends every session
    assert s.check_session(TOKEN) is None


# -- the API -------------------------------------------------------------------------------

@pytest.fixture
def client(store):
    return TestClient(create_app(Engine(store), TOKEN, signin=signin()))


def test_the_page_signs_in_and_uses_the_session(client):
    options = client.get("/api/signin").json()
    assert options["enabled"] and options["microsoft"] == {"client_id": MS_ID, "tenant_id": TENANT}
    r = client.post("/api/session", json={"provider": "microsoft", "token": id_token()})
    assert r.status_code == 200, r.text
    session = {"Authorization": "Bearer " + r.json()["session"]}
    assert client.get("/api/sources", headers=session).status_code == 200
    assert client.get("/api/session", headers=session).json()["email"] == "ted@example.com"
    assert client.get("/api/session", headers={"Authorization": f"Bearer {TOKEN}"}).json()["provider"] == "token"
    assert client.get("/api/sources", headers={"Authorization": "Bearer ets1.bad.sig"}).status_code == 401
    r = client.post("/api/session", json={"provider": "microsoft", "token": id_token(aud="x")})
    assert r.status_code == 401


def test_loads_started_by_someone_signed_in_say_who(client, store):
    Engine(store).ingest_upload("parts", "parts.csv", b"sku,qty\nA-1,3\n", create=True)
    session = client.post("/api/session", json={"provider": "microsoft", "token": id_token()}).json()["session"]
    item = client.post("/api/queue", headers={"Authorization": f"Bearer {session}"}, json={"source": "parts"}).json()["item"]
    assert item["by"] == "ted@example.com"
    client.app.state.load_queue.wait(item["id"], 30)


# -- OneDrive / SharePoint files ---------------------------------------------------------------

def test_microsoft_links():
    assert msfiles.is_microsoft_url("https://contoso.sharepoint.com/:x:/s/team/EabcDEF?e=x1")
    assert msfiles.is_microsoft_url("https://contoso-my.sharepoint.com/personal/ted/Documents/a.xlsx")
    assert msfiles.is_microsoft_url("https://1drv.ms/x/s!Abc") and msfiles.is_microsoft_url("https://onedrive.live.com/edit?id=1")
    assert not msfiles.is_microsoft_url("https://docs.google.com/spreadsheets/d/abc")
    assert not msfiles.is_microsoft_url("https://sharepoint.com.evil.example/a.xlsx")
    # Microsoft's own example of encoding a sharing link
    assert msfiles.share_id("https://onedrive.live.com/redir?resid=1231244193912!12&authKey=1201919!12921!1") == \
        "u!aHR0cHM6Ly9vbmVkcml2ZS5saXZlLmNvbS9yZWRpcj9yZXNpZD0xMjMxMjQ0MTkzOTEyITEyJmF1dGhLZXk9MTIwMTkxOSExMjkyMSEx"


def workbook_bytes():
    wb = openpyxl.Workbook()
    wb.active.title = "Orders"
    wb.active.append(["sku", "units"])
    wb.active.append(["A-1", 2])
    ws = wb.create_sheet("Prices")
    ws.append(["sku", "price"])
    ws.append(["A-1", 9.5])
    out = io.BytesIO()
    wb.save(out)
    return out.getvalue()


def graph(name="Sales.xlsx", content=None, status=200, folder=False):
    seen = []

    def answer(request):
        seen.append(request)
        if request.headers.get("authorization") != "Bearer ms-graph-token":
            return httpx.Response(401, json={"error": {"message": "bad token"}})
        if status != 200:
            return httpx.Response(status, json={"error": {"message": "Access denied"}})
        if request.url.path.endswith("/content"):
            return httpx.Response(200, content=content if content is not None else workbook_bytes())
        item = {"id": "ITEM1", "name": name, "size": 100, "parentReference": {"driveId": "DRIVE1"}}
        item.update({"folder": {}} if folder else {"file": {}})
        return httpx.Response(200, json=item)
    return httpx.Client(transport=httpx.MockTransport(answer)), seen


LINK = "https://contoso.sharepoint.com/:x:/s/team/EabcDEF?e=x1"


def test_an_excel_file_becomes_one_tab_per_sheet():
    http, seen = graph()
    book = msfiles.fetch_workbook(LINK, "ms-graph-token", client=http)
    assert (book.title, book.provider, book.spreadsheet_id) == ("Sales", "microsoft", "ms:DRIVE1:ITEM1")
    assert [(t.title, t.headers, t.records) for t in book.tabs] == [
        ("Orders", ["sku", "units"], [{"sku": "A-1", "units": 2}]),
        ("Prices", ["sku", "price"], [{"sku": "A-1", "price": 9.5}])]
    assert seen[0].url.path == f"/v1.0/shares/{msfiles.share_id(LINK)}/driveItem"


def test_a_csv_file_is_one_tab():
    http, _ = graph(name="ledger.csv", content="\ufeffday,amount\n2026-11-19,12.5\n".encode())
    [tab] = msfiles.fetch_workbook(LINK, "ms-graph-token", client=http).tabs
    assert tab.title == "ledger" and tab.records == [{"day": "2026-11-19", "amount": "12.5"}]


@pytest.mark.parametrize("token, kwargs, message, sign_in", [
    (None, {}, "sign in with Microsoft", True),
    ("wrong", {}, "sign in with Microsoft again", True),
    ("ms-graph-token", {"status": 403}, "can't open this file", True),
    ("ms-graph-token", {"folder": True}, "is a folder", False),
    ("ms-graph-token", {"name": "deck.pptx"}, "isn't an Excel", False),
])
def test_file_problems_are_explained(token, kwargs, message, sign_in):
    http, _ = graph(**kwargs)
    with pytest.raises(SheetError, match=message) as exc:
        msfiles.fetch_workbook(LINK, token, client=http)
    assert exc.value.needs_sign_in is sign_in


def test_the_api_loads_a_microsoft_file_with_the_callers_token(store, monkeypatch):
    got = []
    real = msfiles.fetch_workbook

    def fetch(url, token, client=None, max_bytes=0):
        got.append(token)
        return real(url, token, client=graph()[0])

    monkeypatch.setattr(msfiles, "fetch_workbook", fetch)
    client = TestClient(create_app(Engine(store), TOKEN))
    auth = {"Authorization": f"Bearer {TOKEN}"}
    r = client.post("/api/gsheets", headers=auth, json={"url": LINK, "microsoft_token": "ms-graph-token"})
    assert r.status_code == 200, r.text
    names = sorted(s.name for s in store.list_sources())
    assert names == ["sales_orders", "sales_prices"]
    src = store.get_source("sales_orders")
    assert src.config["provider"] == "microsoft" and src.config["url"] == LINK
    summary = next(s for s in client.get("/api/sources", headers=auth).json() if s["name"] == "sales_orders")
    assert summary["provider"] == "microsoft" and summary["connection"]["label"] == "Sales"
    r = client.post("/api/sources/sales_orders/reload", headers=auth, json={"microsoft_token": "ms-graph-token"})
    assert r.status_code == 200, r.text
    r = client.post("/api/sources/sales_orders/reload", headers=auth, json={})
    assert r.status_code == 403 and "sign in with Microsoft" in r.json()["detail"]
    assert got == ["ms-graph-token", "ms-graph-token", None]
