import base64
import json

import httpx
import pytest

from eyetable.connectors.postman import (
    PaginationLimitError,
    PostmanConnector,
    UnresolvedVariableError,
    UnsupportedRequestError,
    extract_records,
    import_postman,
    pagination_settings,
    parse_pagination_option,
    resolve,
)
from eyetable.engine import Engine

USERS = {"data": [{"id": 1, "name": "Ada", "address": {"city": "London"}}, {"id": 2, "name": "Grace"}]}
SALES = {"@odata.context": "x", "value": [{"Region": "West", "Amount": 10.5}]}


def make_connector(collection, environment, sent):
    def handler(request: httpx.Request) -> httpx.Response:
        sent.append(request)
        if request.url.path == "/v2/users":
            return httpx.Response(200, json=USERS)
        if request.url.path == "/v2/users/7":
            return httpx.Response(200, json={"id": 7, "name": "Linus"})
        if request.url.path == "/v2/odata/Sales":
            return httpx.Response(200, json=SALES)
        return httpx.Response(404)

    config = {"collection": collection, "environment": environment}
    return PostmanConnector(config, client=httpx.Client(transport=httpx.MockTransport(handler)))


def test_discover_probes_get_requests_with_resolved_auth(collection, environment):
    sent = []
    with make_connector(collection, environment, sent) as conn:
        found = {d.key: d for d in conn.discover()}

    assert set(found) == {"Users/List users", "Users/Get user", "Users/Create user", "Reports/Sales (OData)"}
    users = found["Users/List users"]
    assert users.name == "users_list_users"
    assert users.shape == "3 x 2"
    assert [c.name for c in users.columns] == ["id", "name", "address__city"]
    assert found["Users/Get user"].row_count == 1

    # POST is cataloged but never sent.
    assert found["Users/Create user"].readable is False
    assert all(r.method == "GET" for r in sent)

    by_path = {r.url.path: r for r in sent}
    users_req = by_path["/v2/users"]  # environment's version=v2 beats the collection's v1
    assert users_req.url.params["page"] == "1"
    assert users_req.headers["Authorization"] == "Bearer tok-123"  # inherited from collection
    assert users_req.headers["Accept"] == "application/json"
    assert "X-Debug" not in users_req.headers  # disabled in Postman
    sales_req = by_path["/v2/odata/Sales"]
    assert sales_req.url.params["api_key"] == "rk-456"  # folder auth overrides collection auth
    assert "Authorization" not in sales_req.headers


def test_read_unwraps_odata_value(collection, environment):
    with make_connector(collection, environment, []) as conn:
        assert list(conn.read("Reports/Sales (OData)")) == [[{"Region": "West", "Amount": 10.5}]]


def test_non_get_requires_opt_in(collection, environment):
    with make_connector(collection, environment, []) as conn:
        with pytest.raises(UnsupportedRequestError):
            conn.fetch("Users/Create user")


def test_missing_variable_is_reported(collection):
    with make_connector(collection, None, []) as conn:
        with pytest.raises(UnresolvedVariableError, match="apiToken"):
            conn.test()


def test_basic_auth_and_overrides(collection, environment):
    collection["auth"] = {
        "type": "basic",
        "basic": [{"key": "username", "value": "{{user}}"}, {"key": "password", "value": "pw"}],
    }
    sent = []
    conn = make_connector(collection, environment, sent)
    conn.variables["user"] = "ted"
    conn.fetch("Users/Get user")
    expected = "Basic " + base64.b64encode(b"ted:pw").decode()
    assert sent[0].headers["Authorization"] == expected


def test_import_moves_secrets_to_env_refs(collection, environment):
    config, secrets = import_postman("shop", collection, environment)
    assert secrets == ["EYETABLE_SHOP_APITOKEN", "EYETABLE_SHOP_REPORTKEY"]
    stored = json.dumps(config)
    assert "tok-123" not in stored and "rk-456" not in stored
    assert "${EYETABLE_SHOP_APITOKEN}" in stored


def test_nested_variables_resolve():
    assert resolve("{{a}}/x", {"a": "{{b}}/y", "b": "z"}) == "z/y/x"


@pytest.mark.parametrize(
    "payload, path, expected",
    [
        ([{"a": 1}], None, [{"a": 1}]),
        ({"results": [{"a": 1}], "count": 1}, None, [{"a": 1}]),
        ({"data": {"items": [{"a": 1}]}}, None, [{"a": 1}]),
        ({"page": 1, "things": [{"a": 1}]}, None, [{"a": 1}]),
        ({"a": 1, "b": {"c": 2}}, None, [{"a": 1, "b__c": 2}]),
        ({"payload": {"orders": [{"a": 1}]}}, "payload.orders", [{"a": 1}]),
        ([1, 2], None, [{"value": 1}, {"value": 2}]),
    ],
)
def test_extract_records(payload, path, expected):
    assert extract_records(payload, path) == expected


# -- pagination -----------------------------------------------------------

ITEMS = [{"id": i} for i in range(1, 6)]
BEARER = {"type": "bearer", "bearer": [{"key": "token", "value": "tok"}]}


def paged_connector(handler, pagination, url="https://api.example.test/items", auth=None):
    collection = {
        "info": {"schema": "https://schema.getpostman.com/json/collection/v2.1.0/collection.json"},
        "item": [{"name": "Items", "request": {"method": "GET", "url": url}}],
    }
    if auth:
        collection["auth"] = auth
    sent = []

    def record(request: httpx.Request) -> httpx.Response:
        sent.append(request)
        return handler(request)

    config = {"collection": collection, "pagination": {"Items": pagination}}
    return PostmanConnector(config, client=httpx.Client(transport=httpx.MockTransport(record))), sent


def page_handler(request: httpx.Request) -> httpx.Response:
    page, size = int(request.url.params["page"]), int(request.url.params.get("per_page", 2))
    return httpx.Response(200, json={"data": ITEMS[(page - 1) * size : page * size]})


def test_page_number_streams_one_batch_per_page():
    conn, sent = paged_connector(page_handler, {"type": "page", "size_param": "per_page", "size": 2})
    batches = conn.read("Items", batch_size=100)
    assert next(batches) == ITEMS[:2]
    assert len(sent) == 1  # later pages are not fetched until the store asks for them
    assert list(batches) == [ITEMS[2:4], ITEMS[4:]]
    assert [(r.url.params["page"], r.url.params["per_page"]) for r in sent] == [("1", "2"), ("2", "2"), ("3", "2")]


def test_page_number_without_size_stops_on_empty_page():
    conn, sent = paged_connector(page_handler, {"type": "page"})
    assert list(conn.read("Items", batch_size=1)) == [[r] for r in ITEMS]  # pages are split to batch_size
    assert [r.url.params["page"] for r in sent] == ["1", "2", "3", "4"]


def test_page_number_custom_param_and_start():
    def handler(request):
        p = int(request.url.params["p"])
        return httpx.Response(200, json=[{"p": p}] if p < 2 else [])

    conn, sent = paged_connector(handler, {"type": "page", "param": "p", "start": 0})
    assert list(conn.read("Items")) == [[{"p": 0}], [{"p": 1}]]


def test_page_number_stops_when_api_ignores_the_param():
    conn, sent = paged_connector(lambda r: httpx.Response(200, json={"data": ITEMS[:2]}), {"type": "page"})
    assert list(conn.read("Items")) == [ITEMS[:2]]
    assert len(sent) == 2


def test_offset_limit_advances_by_rows_received():
    def handler(request):
        offset, limit = int(request.url.params["skip"]), int(request.url.params["take"])
        return httpx.Response(200, json={"results": ITEMS[offset : offset + limit]})

    pagination = {"type": "offset", "offset_param": "skip", "limit_param": "take", "limit": 2}
    conn, sent = paged_connector(handler, pagination)
    assert list(conn.read("Items")) == [ITEMS[:2], ITEMS[2:4], ITEMS[4:]]
    assert [r.url.params["skip"] for r in sent] == ["0", "2", "4"]
    assert all(r.url.params["take"] == "2" for r in sent)


def test_cursor_follows_dotted_path_until_empty():
    pages = {None: (ITEMS[:2], "c2"), "c2": (ITEMS[2:4], "c3"), "c3": (ITEMS[4:], None)}

    def handler(request):
        rows, nxt = pages[request.url.params.get("after")]
        return httpx.Response(200, json={"data": rows, "meta": {"next_cursor": nxt}})

    conn, sent = paged_connector(
        handler,
        {"type": "cursor", "cursor_path": "meta.next_cursor", "param": "after"},
        url="https://api.example.test/items?limit=2",
    )
    assert list(conn.read("Items")) == [ITEMS[:2], ITEMS[2:4], ITEMS[4:]]
    assert [r.url.params.get("after") for r in sent] == [None, "c2", "c3"]
    assert all(r.url.params["limit"] == "2" for r in sent)  # the request's own params are kept


def test_cursor_stops_when_cursor_repeats():
    def handler(request):
        return httpx.Response(200, json={"data": [{"n": len(sent)}], "next": "same"})

    conn, sent = paged_connector(handler, {"type": "cursor", "cursor_path": "next"})
    assert list(conn.read("Items")) == [[{"n": 1}], [{"n": 2}]]
    assert [r.url.params.get("cursor") for r in sent] == [None, "same"]


def test_next_link_from_body_relative_url_keeps_auth():
    def handler(request):
        assert request.headers["Authorization"] == "Bearer tok"
        page = int(request.url.params.get("page", 1))
        nxt = f"/items?page={page + 1}" if page < 3 else None
        return httpx.Response(200, json={"items": [{"page": page}], "links": {"next": nxt}})

    conn, sent = paged_connector(handler, {"type": "next_link", "next_path": "links.next"}, auth=BEARER)
    assert list(conn.read("Items")) == [[{"page": 1}], [{"page": 2}], [{"page": 3}]]
    assert str(sent[1].url) == "https://api.example.test/items?page=2"


def test_next_link_from_link_header_reapplies_query_api_key():
    def handler(request):
        assert request.url.params["api_key"] == "k"
        page = int(request.url.params.get("page", 1))
        headers = {}
        if page < 2:
            headers["Link"] = f'<https://api.example.test/items?page={page + 1}>; rel="next", <x>; rel="last"'
        return httpx.Response(200, json=[{"page": page}], headers=headers)

    auth = {
        "type": "apikey",
        "apikey": [{"key": "key", "value": "api_key"}, {"key": "value", "value": "k"}, {"key": "in", "value": "query"}],
    }
    conn, sent = paged_connector(handler, {"type": "next_link"}, auth=auth)
    assert list(conn.read("Items")) == [[{"page": 1}], [{"page": 2}]]
    assert len(sent) == 2


def test_next_link_to_another_host_drops_credentials():
    def handler(request):
        if request.url.host == "api.example.test":
            return httpx.Response(200, json={"data": [{"a": 1}], "next": "https://cdn.example.test/p2"})
        assert "Authorization" not in request.headers
        return httpx.Response(200, json={"data": [{"a": 2}]})

    conn, sent = paged_connector(handler, {"type": "next_link", "next_path": "next"}, auth=BEARER)
    assert list(conn.read("Items")) == [[{"a": 1}], [{"a": 2}]]
    assert sent[1].headers["host"] == "cdn.example.test"


def test_max_pages_fails_instead_of_truncating():
    pagination = {"type": "page", "size": 2, "size_param": "per_page", "max_pages": 2}
    conn, sent = paged_connector(page_handler, pagination)
    batches = conn.read("Items")
    assert next(batches) == ITEMS[:2]
    assert next(batches) == ITEMS[2:4]
    with pytest.raises(PaginationLimitError, match="max_pages=2"):
        next(batches)
    assert len(sent) == 2  # nothing is sent beyond the limit


def test_max_pages_reached_exactly_is_fine():
    pagination = {"type": "page", "size": 2, "size_param": "per_page", "max_pages": 3}
    conn, sent = paged_connector(page_handler, pagination)
    assert sum(len(b) for b in conn.read("Items")) == 5


def test_discover_probes_only_the_first_page():
    conn, sent = paged_connector(page_handler, {"type": "page", "size": 2, "size_param": "per_page"})
    (info,) = conn.discover()
    assert len(sent) == 1
    assert info.row_count is None  # unknown until ingested
    assert [c.name for c in info.columns] == ["id"]
    assert info.detail["pagination"] == "page"


def test_engine_ingests_every_page(store):
    pagination = {"type": "page", "size": 2, "size_param": "per_page"}
    engine = Engine(store, connector_factory=lambda type_, config: paged_connector(page_handler, pagination)[0])
    engine.add_source("api", "postman", {}, verify=False)
    (result,) = engine.ingest("api", batch_size=1)
    assert result.status == "succeeded" and result.rows == 5
    assert sorted(r["id"] for r in store.read_table(result.table)) == [1, 2, 3, 4, 5]


def test_pagination_for_unknown_request_fails_test():
    conn, _ = paged_connector(page_handler, {"type": "page"})
    conn.pagination["Nope"] = conn.pagination["Items"]
    with pytest.raises(ValueError, match="Nope"):
        conn.test()


@pytest.mark.parametrize(
    "spec, message",
    [
        ({"type": "pages"}, "must be one of"),
        ({"type": "offset"}, "needs limit"),
        ({"type": "cursor"}, "needs cursor_path"),
        ({"type": "page", "size_param": "per_page"}, "also needs size"),
        ({"type": "page", "cursor_path": "x"}, "unknown page pagination option"),
        ({"type": "page", "max_pages": 0}, "at least 1"),
        ({"type": "offset", "limit": "lots"}, "limit must be int"),
    ],
)
def test_pagination_settings_validation(spec, message):
    with pytest.raises(ValueError, match=message):
        pagination_settings(spec)


def test_parse_pagination_option():
    key, settings = parse_pagination_option("Users/List users=page,size_param=per_page,size=100,max_pages=50")
    assert key == "Users/List users"
    assert settings == {
        "type": "page", "param": "page", "start": 1, "size_param": "per_page", "size": 100, "max_pages": 50,
    }
    assert parse_pagination_option("Feed=next_link")[1] == {"type": "next_link", "max_pages": 1000}
    with pytest.raises(ValueError):
        parse_pagination_option("Feed")
    with pytest.raises(ValueError):
        parse_pagination_option("Feed=cursor,cursor_path")


def test_cli_add_postman_stores_pagination(store, tmp_path, monkeypatch):
    from typer.testing import CliRunner

    from eyetable import cli

    path = tmp_path / "collection.json"
    path.write_text(json.dumps({
        "info": {"schema": "https://schema.getpostman.com/json/collection/v2.1.0/collection.json"},
        "item": [{"name": "Items", "request": {"method": "GET", "url": "https://api.example.test/items"}}],
    }))
    monkeypatch.setattr(cli, "_engine", lambda: Engine(store))
    runner = CliRunner()

    args = ["source", "add-postman", "api", str(path), "--paginate", "Items=cursor,cursor_path=meta.next"]
    result = runner.invoke(cli.app, args)
    assert result.exit_code == 0, result.output
    assert store.get_source("api").config["pagination"] == {
        "Items": {"type": "cursor", "param": "cursor", "cursor_path": "meta.next", "max_pages": 1000}
    }

    result = runner.invoke(cli.app, ["source", "add-postman", "api2", str(path), "--paginate", "Items=pages"])
    assert result.exit_code == 1 and "must be one of" in result.output
