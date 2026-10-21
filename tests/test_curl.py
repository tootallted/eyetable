import time
import urllib.parse

import httpx
import pytest
from cryptography.fernet import Fernet
from fastapi.testclient import TestClient

from eyetable.api import create_app
from eyetable.connectors import build_connector
from eyetable.connectors.curl_source import CurlConnector
from eyetable.curl import CurlError, build_collection, infer_paging, mask, parse_curl, plan, suggested_name
from eyetable.engine import Engine

TOKEN = "test-token-0123456789"
AUTH = {"Authorization": f"Bearer {TOKEN}"}
KEY = Fernet.generate_key().decode()
ITEMS = [{"id": i, "name": f"item {i}"} for i in range(1, 6)]  # 5 items


def fake_api(request: httpx.Request) -> httpx.Response:
    """A paginated API: page/per_page, offset/limit, cursor, and next links."""
    q = dict(urllib.parse.parse_qsl(request.url.query.decode()))
    if request.headers.get("authorization") != "Bearer tok_123":
        return httpx.Response(401, json={"error": "unauthorized"})
    path = request.url.path
    if path == "/items":
        page, size = int(q.get("page", 1)), int(q.get("per_page", 10))
        return httpx.Response(200, json={"data": ITEMS[(page - 1) * size: page * size]})
    if path == "/rows":
        offset, limit = int(q.get("offset", 0)), int(q.get("limit", 10))
        return httpx.Response(200, json=ITEMS[offset: offset + limit])
    if path == "/stream":
        start = int(q.get("cursor") or 0)
        chunk = ITEMS[start: start + 2]
        nxt = start + 2 if start + 2 < len(ITEMS) else None
        return httpx.Response(200, json={"results": chunk, "meta": {"next_cursor": str(nxt) if nxt else None}})
    if path == "/linked":
        start = int(q.get("from", 0))
        body = {"value": ITEMS[start: start + 2]}
        if start + 2 < len(ITEMS):
            body["@odata.nextLink"] = f"https://api.test/linked?from={start + 2}"
        return httpx.Response(200, json=body)
    return httpx.Response(404)


def client():
    return httpx.Client(transport=httpx.MockTransport(fake_api))


def engine_for(store):
    def factory(type_, config):
        return CurlConnector(config, client=client()) if type_ == "curl" else build_connector(type_, config)
    return Engine(store, factory, environ={"EYETABLE_SECRET_KEY": KEY})


AUTHED = " -H 'Authorization: Bearer tok_123'"


# -- parsing -------------------------------------------------------------------

def test_parses_the_common_forms():
    bash = parse_curl("curl 'https://api.test/items?page=1' \\\n  -H 'Accept: application/json' \\\n  --compressed")
    assert (bash.method, bash.url, bash.headers) == ("GET", "https://api.test/items?page=1", [("Accept", "application/json")])
    cmd = parse_curl('curl ^"https://api.test/items?page=1^&per_page=2^" ^\n  -H ^"accept: application/json^"')
    assert cmd.url == "https://api.test/items?page=1&per_page=2"
    pwsh = parse_curl("curl.exe -sSL `\n -u ted:pa55 `\n \"https://api.test/items\"")
    assert pwsh.user == ("ted", "pa55") and pwsh.url == "https://api.test/items"
    post = parse_curl("curl -XPOST https://api.test/search --json '{\"q\": 1}'")
    assert post.method == "POST" and post.body == '{"q": 1}' and ("Content-Type", "application/json") in post.headers
    get = parse_curl("curl -G https://api.test/items -d limit=2 --data-urlencode 'q=a b'")
    assert get.method == "GET" and get.url == "https://api.test/items?limit=2&q=a+b" and get.body is None
    form = parse_curl("curl https://api.test/token -d grant_type=x -d client_secret=s3")
    assert form.method == "POST" and form.body == "grant_type=x&client_secret=s3"


@pytest.mark.parametrize("bad, message", [
    ("wget https://x.test", "starts with curl"),
    ("curl -H 'Accept: x'", "no URL"),
    ("curl https://x.test -F file=@a.csv", "-F"),
    ("curl https://x.test -d @body.json", "file"),
    ("curl 'https://x.test", "quotes"),
    ("curl ftp://x.test/a", "http"),
])
def test_parse_errors(bad, message):
    with pytest.raises(CurlError, match=message):
        parse_curl(bad)


def test_secrets_are_pulled_out_and_masked():
    req = parse_curl("curl 'https://api.test/items?page=1&api_key=k1' -H 'Authorization: Bearer t1' "
                     "-H 'X-Request-Id: 7' -b 'sid=s1' -u me:pw")
    collection, secrets = build_collection(req)
    stored = collection["item"][0]["request"]
    assert sorted(secrets.values()) == ["Bearer t1", "k1", "pw", "sid=s1"]
    assert "k1" not in stored["url"] and "{{secret_" in stored["url"]
    assert {h["key"]: h["value"] for h in stored["header"]}["X-Request-Id"] == "7"  # not a secret
    assert "t1" not in str(stored) and "pw" not in str(stored["auth"])
    assert "••••••" in mask(stored["url"], secrets)


def test_names():
    assert suggested_name("https://api.github.com/repos/o/r/issues?page=1") == "github_issues"
    assert suggested_name("https://jsonplaceholder.typicode.com/posts") == "typicode_posts"
    assert suggested_name("https://example.com/") == "example"


# -- paging ---------------------------------------------------------------------

def test_paging_from_the_command():
    spec, text = infer_paging("https://a.test/x?page=3&per_page=50", None, None)
    assert spec["type"] == "page" and (spec["param"], spec["start"], spec["size_param"], spec["size"]) == ("page", 3, "per_page", 50)
    assert "50 per page" in text
    spec, _ = infer_paging("https://a.test/x?_page=1&_limit=10", None, None)
    assert (spec["param"], spec["size_param"]) == ("_page", "_limit")
    spec, text = infer_paging("https://a.test/x?offset=0&limit=200", None, None)
    assert spec["type"] == "offset" and spec["limit"] == 200 and spec["limit_param"] == "limit"
    spec, text = infer_paging("https://a.test/x?offset=0", None, None)
    assert spec is None and "page size" in text


def test_paging_from_the_response():
    spec, _ = infer_paging("https://a.test/x?cursor=", None, {"results": [], "meta": {"next_cursor": "abc"}})
    assert spec == {"type": "cursor", "param": "cursor", "cursor_path": "meta.next_cursor", "max_pages": 1000}
    spec, _ = infer_paging("https://a.test/x", None, {"value": [], "@odata.nextLink": "https://a.test/x?skip=2"})
    assert spec["type"] == "next_link" and spec["next_path"] == "@odata.nextLink"
    response = httpx.Response(200, headers={"link": '<https://a.test/x?page=2>; rel="next"'}, request=httpx.Request("GET", "https://a.test/x"))
    spec, text = infer_paging("https://a.test/x", response, [])
    assert spec["type"] == "next_link" and "Link" in text
    spec, text = infer_paging("https://a.test/x", None, {"data": []})
    assert spec is None and "Only this request" in text


# -- loading --------------------------------------------------------------------

@pytest.mark.parametrize("cmd", [
    "curl 'https://api.test/items?page=1&per_page=2'",
    "curl 'https://api.test/rows?offset=0&limit=2'",
    "curl 'https://api.test/stream?cursor='",
    "curl 'https://api.test/linked'",
])
def test_every_page_is_loaded(store, cmd):
    engine = engine_for(store)
    [result] = engine.load_curl(cmd + AUTHED, "things", client=client())
    assert (result.status, result.rows) == ("succeeded", 5), result.error
    rows = store.read_table("things__data")
    assert [r["id"] for r in rows] == [1, 2, 3, 4, 5] and [r["ET_id"] for r in rows] == [1, 2, 3, 4, 5]
    assert rows[0]["ET_source"].startswith("things - https://api.test/")


def test_only_the_first_page_when_asked(store):
    engine = engine_for(store)
    [result] = engine.load_curl("curl 'https://api.test/items?page=1&per_page=2'" + AUTHED, "things", all_pages=False, client=client())
    assert result.rows == 2


def test_secrets_are_stored_encrypted_and_reload_works(store):
    engine = engine_for(store)
    engine.load_curl("curl 'https://api.test/items?page=1&per_page=2'" + AUTHED, "things", client=client())
    config = store.get_source("things").config
    assert "tok_123" not in str(config)
    assert config["variables"]["secret_1"].startswith("eyetable-enc:")
    [again] = engine.reload_source("things")
    assert again.rows == 5
    # a different key can't read it
    with pytest.raises(ValueError, match="decrypted"):
        Engine(store, environ={"EYETABLE_SECRET_KEY": Fernet.generate_key().decode()}).reload_source("things")


def test_append_and_name_conflicts(store):
    engine = engine_for(store)
    engine.load_curl("curl 'https://api.test/items?page=1&per_page=2'" + AUTHED, "things", client=client())
    engine.load_curl("curl 'https://api.test/items?page=1&per_page=2'" + AUTHED, "things", mode="append", client=client())
    assert len(store.read_table("things__data")) == 10
    engine.ingest_upload("parts", "p.csv", b"a\n1\n", create=True)
    with pytest.raises(ValueError, match="already a file source"):
        engine.load_curl("curl https://api.test/items" + AUTHED, "parts", client=client())


def test_api_errors_are_explained(store):
    with pytest.raises(CurlError, match="401"):
        plan("curl https://api.test/items", KEY, client=client())


# -- endpoints ------------------------------------------------------------------

def test_inspect_and_load_endpoints(store):
    app = create_app(engine_for(store), TOKEN, curl_client=client())
    web = TestClient(app)
    cmd = "curl 'https://api.test/items?page=1&per_page=2'" + AUTHED
    p = web.post("/api/curl/inspect", headers=AUTH, json={"curl": cmd}).json()
    assert p["method"] == "GET" and p["rows"] == 2 and p["secrets"] == 1
    assert p["pagination"]["type"] == "page" and "2 per page" in p["paging_text"]
    assert p["suggested_name"] == "test_items" and [c["name"] for c in p["columns"]] == ["id", "name"]
    assert "tok_123" not in str(p) and p["headers"] == [{"name": "Authorization", "value": "••••••"}]
    assert not store.list_sources()  # checking saves nothing

    job = web.post("/api/curl", headers=AUTH, json={"curl": cmd, "name": "items"}).json()["job_id"]
    for _ in range(100):
        state = web.get(f"/api/jobs/{job}", headers=AUTH).json()
        if state["status"] != "running":
            break
        time.sleep(0.05)
    assert state["status"] == "succeeded", state["error"]
    assert state["result"]["rows"] == 5 and state["result"]["source"]["type"] == "curl"
    summary = {s["name"]: s for s in web.get("/api/sources", headers=AUTH).json()}
    assert summary["items"]["rows"] == 5 and summary["items"]["table"] == "items__data"
    q = web.post("/api/query", headers=AUTH, json={"sql": "SELECT count(*) FROM items"}).json()
    assert q["rows"] == [[5]]

    assert web.post("/api/curl/inspect", headers=AUTH, json={"curl": "curl https://api.test/items"}).status_code == 400
    assert web.post("/api/curl", headers=AUTH, json={"curl": "nope", "name": "x"}).status_code == 400
    assert web.post("/api/curl", headers=AUTH, json={"curl": cmd, "name": "Bad Name"}).status_code == 400
    assert web.post("/api/curl/inspect", json={"curl": cmd}).status_code == 401
