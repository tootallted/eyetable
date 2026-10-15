"""API sources defined by a Postman collection (v2.0 / v2.1 export).

Anything you can run in Postman can be a source: export the collection (and
optionally an environment) and register it. Each request becomes a dataset.

Config:
    {"collection": {...exported collection...},
     "environment": {...exported environment...} | null,
     "variables": {"baseUrl": "..."},          # overrides, highest precedence
     "records_path": {"Users/List users": "data.items"},
     "allow_methods": ["POST"],                 # beyond GET, opt in explicitly
     "probe": true,                             # GET each request during discovery
     "timeout": 30}

Supported: folders, collection/environment/override variables (including
nested and `{{$guid}}`-style dynamic ones), path variables, query params,
headers, raw / urlencoded / formdata (text) / graphql bodies, and bearer,
basic, API key and OAuth2-access-token auth with folder/collection inheritance.
Not supported: pre-request and test scripts (they are JavaScript), file uploads,
and auth types that need a signing flow (AWS, Hawk, digest, NTLM, ...).
"""

import base64
import csv
import datetime as dt
import io
import json
import random
import re
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

import httpx

from eyetable.connectors.base import Connector, Record
from eyetable.credentials import env_ref
from eyetable.identifiers import sanitize
from eyetable.models import ColumnInfo, DatasetInfo
from eyetable.types import infer_columns

_VAR = re.compile(r"\{\{\s*([^{}]+?)\s*\}\}")
_RECORD_KEYS = ("data", "items", "results", "records", "value", "rows", "entries", "objects")
_DYNAMIC = {
    "$guid": lambda: str(uuid.uuid4()),
    "$randomUUID": lambda: str(uuid.uuid4()),
    "$timestamp": lambda: str(int(time.time())),
    "$isoTimestamp": lambda: dt.datetime.now(dt.timezone.utc).isoformat(),
    "$randomInt": lambda: str(random.randint(0, 1000)),
}


class UnresolvedVariableError(ValueError):
    pass


class UnsupportedRequestError(ValueError):
    pass


# -- collection parsing ---------------------------------------------------


@dataclass
class PostmanRequest:
    key: str  # folder path + request name, e.g. "Users/List users"
    name: str
    method: str
    url: Any  # str or Postman url object
    headers: list[dict]
    body: dict | None
    auth: dict | None  # effective auth after folder/collection inheritance


def unwrap_collection(data: dict) -> dict:
    """Accept both a file export and the Postman API's {"collection": {...}} shape."""
    if "info" not in data and isinstance(data.get("collection"), dict):
        data = data["collection"]
    schema = data.get("info", {}).get("schema", "")
    if "item" not in data or ("v2.0" not in schema and "v2.1" not in schema):
        raise ValueError("not a Postman v2.0/v2.1 collection export")
    return data


def unwrap_environment(data: dict) -> dict:
    if "values" not in data and isinstance(data.get("environment"), dict):
        data = data["environment"]
    if not isinstance(data.get("values"), list):
        raise ValueError("not a Postman environment export")
    return data


def _effective_auth(own: dict | None, inherited: dict | None) -> dict | None:
    if own is None or own.get("type") == "inherit":
        return inherited
    return own


def iter_requests(collection: dict) -> Iterator[PostmanRequest]:
    seen: dict[str, int] = {}

    def walk(items: list, path: list[str], inherited: dict | None) -> Iterator[PostmanRequest]:
        for item in items:
            name = item.get("name") or "request"
            if "item" in item:
                yield from walk(item["item"], [*path, name], _effective_auth(item.get("auth"), inherited))
                continue
            request = item.get("request")
            if request is None:
                continue
            if isinstance(request, str):
                request = {"url": request, "method": "GET"}
            key = "/".join([*path, name])
            seen[key] = seen.get(key, 0) + 1
            if seen[key] > 1:
                key = f"{key} ({seen[key]})"
            yield PostmanRequest(
                key=key,
                name=name,
                method=(request.get("method") or "GET").upper(),
                url=request.get("url", ""),
                headers=request.get("header") if isinstance(request.get("header"), list) else [],
                body=request.get("body"),
                auth=_effective_auth(request.get("auth"), inherited),
            )

    yield from walk(collection.get("item", []), [], collection.get("auth"))


def build_variables(collection: dict, environment: dict | None, overrides: dict | None) -> dict[str, str]:
    """Postman precedence, lowest to highest: collection, environment, overrides."""
    values: dict[str, Any] = {}
    for var in collection.get("variable", []) or []:
        if not var.get("disabled") and "key" in var:
            values[var["key"]] = var.get("value")
    for var in (environment or {}).get("values", []):
        if var.get("enabled", True) and "key" in var:
            values[var["key"]] = var.get("value")
    values.update(overrides or {})
    return {k: "" if v is None else str(v) for k, v in values.items()}


def resolve(text: str, variables: dict[str, str]) -> str:
    unresolved: set[str] = set()

    def sub(match: re.Match) -> str:
        name = match.group(1)
        if name in variables:
            return variables[name]
        if name in _DYNAMIC:
            return _DYNAMIC[name]()
        unresolved.add(name)
        return match.group(0)

    for _ in range(10):  # variables may reference other variables
        unresolved.clear()
        new = _VAR.sub(sub, text)
        if new == text:
            break
        text = new
    if unresolved:
        raise UnresolvedVariableError(f"undefined Postman variable(s): {', '.join(sorted(unresolved))}")
    return text


def _pairs(entries: list[dict] | None) -> list[tuple[str, str]]:
    return [
        (e["key"], "" if e.get("value") is None else str(e["value"]))
        for e in entries or []
        if not e.get("disabled") and e.get("key") is not None
    ]


def raw_url(url: Any) -> str:
    if isinstance(url, str):
        return url
    raw = url.get("raw")
    if not raw:
        host = url.get("host", "")
        host = ".".join(host) if isinstance(host, list) else host
        path = url.get("path", "")
        path = "/".join(p if isinstance(p, str) else p.get("value", "") for p in path) if isinstance(path, list) else path
        raw = f"{url['protocol']}://{host}" if url.get("protocol") else host
        if path:
            raw += "/" + path.lstrip("/")
        query = "&".join(f"{k}={v}" for k, v in _pairs(url.get("query")))
        if query:
            raw += "?" + query
    for var in url.get("variable", []) or []:
        if var.get("key"):
            raw = re.sub(rf":{re.escape(var['key'])}(?=[/?#]|$)", str(var.get("value", "")), raw)
    return raw


# -- response handling ----------------------------------------------------


def flatten(record: dict, sep: str = "__", prefix: str = "", depth: int = 3) -> Record:
    """Nested objects become prefixed columns; lists stay as JSON values."""
    out: Record = {}
    for key, value in record.items():
        name = f"{prefix}{sep}{key}" if prefix else str(key)
        if isinstance(value, dict) and depth > 0 and value:
            out.update(flatten(value, sep, name, depth - 1))
        else:
            out[name] = value
    return out


def _find_records(obj: dict) -> Any:
    for key in _RECORD_KEYS:
        if isinstance(obj.get(key), list):
            return obj[key]
    lists = [v for v in obj.values() if isinstance(v, list) and v and all(isinstance(x, dict) for x in v)]
    if len(lists) == 1:
        return lists[0]
    for key in _RECORD_KEYS:  # one level down, e.g. {"data": {"items": [...]}}
        inner = obj.get(key)
        if isinstance(inner, dict):
            found = _find_records(inner)
            if found is not inner:
                return found
    return obj


def extract_records(payload: Any, path: str | None = None) -> list[Record]:
    if path:
        for part in path.split("."):
            payload = payload[int(part)] if isinstance(payload, list) else payload[part]
    elif isinstance(payload, dict):
        payload = _find_records(payload)
    if isinstance(payload, dict):
        return [flatten(payload)]
    if isinstance(payload, list):
        return [flatten(x) if isinstance(x, dict) else {"value": x} for x in payload]
    return [{"value": payload}]


def parse_response(response: httpx.Response) -> Any:
    content_type = response.headers.get("content-type", "")
    if "csv" in content_type:
        return list(csv.DictReader(io.StringIO(response.text)))
    try:
        return response.json()
    except ValueError:
        return response.text


# -- connector ------------------------------------------------------------


class PostmanConnector(Connector):
    type = "postman"

    def __init__(self, config: dict, client: httpx.Client | None = None):
        super().__init__(config)
        self.collection = unwrap_collection(config["collection"])
        environment = config.get("environment")
        self.environment = unwrap_environment(environment) if environment else None
        self.variables = build_variables(self.collection, self.environment, config.get("variables"))
        self.requests = {r.key: r for r in iter_requests(self.collection)}
        self.allowed_methods = {"GET", *(m.upper() for m in config.get("allow_methods", []))}
        self._client = client or httpx.Client(timeout=config.get("timeout", 30), follow_redirects=True)

    def close(self) -> None:
        self._client.close()

    def test(self) -> None:
        if not self.requests:
            raise ValueError("collection contains no requests")
        # Resolving every request surfaces missing variables without sending anything.
        for request in self.requests.values():
            self._build(request)

    def discover(self) -> list[DatasetInfo]:
        probe = self.config.get("probe", True)
        datasets = []
        for request in self.requests.values():
            readable = request.method in self.allowed_methods
            info = DatasetInfo(
                key=request.key,
                name=sanitize(request.key),
                kind="api",
                readable=readable,
                detail={"method": request.method, "url": raw_url(request.url)},
            )
            if probe and readable:
                try:
                    records = self.fetch(request.key)
                    info.row_count = len(records)
                    info.columns = [ColumnInfo(n, k or "unknown") for n, k in infer_columns(records).items()]
                except Exception as exc:  # report it in the catalog; keep discovering
                    info.detail["probe_error"] = f"{type(exc).__name__}: {exc}"
            datasets.append(info)
        return datasets

    def read(self, key: str, batch_size: int = 1000) -> Iterator[list[Record]]:
        records = self.fetch(key)
        for start in range(0, len(records), batch_size):
            yield records[start : start + batch_size]

    def fetch(self, key: str) -> list[Record]:
        request = self.requests.get(key)
        if request is None:
            raise KeyError(f"no request {key!r} in collection")
        if request.method not in self.allowed_methods:
            raise UnsupportedRequestError(
                f"{request.method} {key!r} is not read by default; add it to allow_methods to ingest it"
            )
        response = self._client.send(self._build(request))
        response.raise_for_status()
        path = (self.config.get("records_path") or {}).get(key)
        return extract_records(parse_response(response), path)

    def _build(self, request: PostmanRequest) -> httpx.Request:
        v = self.variables
        url = resolve(raw_url(request.url), v)
        if "://" not in url:
            url = "http://" + url
        headers = {k: resolve(val, v) for k, val in _pairs(request.headers)}
        params: list[tuple[str, str]] = []
        kwargs: dict[str, Any] = {}

        body = request.body or {}
        mode = body.get("mode")
        if mode == "raw" and body.get("raw"):
            kwargs["content"] = resolve(body["raw"], v).encode()
            language = body.get("options", {}).get("raw", {}).get("language")
            if language == "json" and not any(h.lower() == "content-type" for h in headers):
                headers["Content-Type"] = "application/json"
        elif mode == "urlencoded":
            kwargs["data"] = {k: resolve(val, v) for k, val in _pairs(body.get("urlencoded"))}
        elif mode == "formdata":
            fields = body.get("formdata") or []
            if any(f.get("type") == "file" and not f.get("disabled") for f in fields):
                raise UnsupportedRequestError(f"{request.key!r}: file uploads are not supported")
            kwargs["data"] = {k: resolve(val, v) for k, val in _pairs(fields)}
        elif mode == "graphql":
            gql = body.get("graphql") or {}
            variables = gql.get("variables") or "{}"
            if not isinstance(variables, str):
                variables = json.dumps(variables)
            kwargs["content"] = (
                '{"query": %s, "variables": %s}'
                % (json.dumps(resolve(gql.get("query", ""), v)), resolve(variables, v) or "{}")
            ).encode()
            headers.setdefault("Content-Type", "application/json")

        self._apply_auth(request, headers, params)
        built = self._client.build_request(request.method, url, headers=headers, **kwargs)
        if params:
            built.url = built.url.copy_merge_params(params)
        return built

    def _apply_auth(self, request: PostmanRequest, headers: dict, params: list) -> None:
        auth = request.auth
        if not auth or auth.get("type") in (None, "noauth"):
            return
        kind = auth["type"]
        values = {e["key"]: resolve(str(e.get("value", "")), self.variables) for e in auth.get(kind, []) or []}
        if kind == "bearer":
            headers["Authorization"] = f"Bearer {values.get('token', '')}"
        elif kind == "basic":
            pair = f"{values.get('username', '')}:{values.get('password', '')}".encode()
            headers["Authorization"] = "Basic " + base64.b64encode(pair).decode()
        elif kind == "apikey":
            name, value = values.get("key", "X-API-Key"), values.get("value", "")
            if values.get("in") == "query":
                params.append((name, value))
            else:
                headers[name] = value
        elif kind == "oauth2" and values.get("accessToken"):
            token = values["accessToken"]
            if values.get("addTokenTo") == "queryParams":
                params.append(("access_token", token))
            else:
                headers["Authorization"] = f"{values.get('headerPrefix') or 'Bearer'} {token}".strip()
        else:
            raise UnsupportedRequestError(f"{request.key!r}: Postman auth type {kind!r} is not supported")


# -- import ---------------------------------------------------------------


def import_postman(
    source: str,
    collection: dict,
    environment: dict | None = None,
    variables: dict | None = None,
) -> tuple[dict, list[str]]:
    """Build a source config from Postman exports.

    Environment values Postman marks as secret are swapped for `${...}`
    references so they are never written to the core database. Returns the
    config and the environment variable names the caller must set.
    """
    collection = unwrap_collection(collection)
    secret_names: list[str] = []
    if environment is not None:
        environment = unwrap_environment(environment)
        values = []
        for var in environment["values"]:
            var = dict(var)
            if var.get("type") == "secret":
                env_name = f"EYETABLE_{source}_{sanitize(var['key'])}".upper()
                var["value"] = env_ref(env_name)
                secret_names.append(env_name)
            values.append(var)
        environment = {**environment, "values": values}
    config = {"collection": collection, "environment": environment, "variables": variables or {}}
    return config, secret_names
