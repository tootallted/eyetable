import base64
import json

import httpx
import pytest

from eyetable.connectors.postman import (
    PostmanConnector,
    UnresolvedVariableError,
    UnsupportedRequestError,
    extract_records,
    import_postman,
    resolve,
)

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
