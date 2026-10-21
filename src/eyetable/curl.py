"""API sources from a pasted curl command.

A curl command (copied from browser dev tools, API docs, or Postman's code
view, in bash, Windows cmd or PowerShell form) becomes a one-request API
source: the request is stored as a Postman v2.1 collection and loaded by
the same connector, so headers, bodies, auth and pagination work the same.

Secrets in the command (Authorization and Cookie headers, API-key-like
headers and query parameters, -u passwords) are pulled out into
{{secret_N}} variables whose values are stored encrypted with
EYETABLE_SECRET_KEY, and are shown masked.

Paging is learned from the command: a page-number parameter (page=1), with
or without a page size (per_page=100), means "every page, that many per
page, starting there"; offset + limit means the same in rows. A cursor
parameter, or a next-page link in the response, is found by looking at the
first response. Paging stops on a short or empty page, a missing cursor or
link, or after 1,000 pages.
"""

import re
import shlex
import urllib.parse
from dataclasses import dataclass, field
from typing import Any

import httpx

from eyetable.connectors.curl_source import CurlConnector
from eyetable.connectors.postman import (
    DEFAULT_MAX_PAGES,
    extract_records,
    pagination_settings,
    parse_response,
)
from eyetable.credentials import encrypt_secret
from eyetable.identifiers import sanitize, source_name
from eyetable.types import infer_columns

REQUEST_NAME = "data"  # one request, so the table is <source>__data
MASK = "••••••"


class CurlError(ValueError):
    pass


@dataclass
class CurlRequest:
    method: str
    url: str
    headers: list[tuple[str, str]] = field(default_factory=list)
    body: str | None = None
    body_is_form: bool = False
    user: tuple[str, str] | None = None
    warnings: list[str] = field(default_factory=list)


# -- parsing ------------------------------------------------------------------

_VALUE_OPTIONS = {
    "-X": "method", "--request": "method",
    "-H": "header", "--header": "header",
    "-d": "data", "--data": "data", "--data-raw": "data", "--data-binary": "data", "--data-ascii": "data",
    "--data-urlencode": "urlencode", "--json": "json",
    "-u": "user", "--user": "user",
    "-b": "cookie", "--cookie": "cookie",
    "-A": "agent", "--user-agent": "agent",
    "-e": "referer", "--referer": "referer",
    "--url": "url", "--oauth2-bearer": "bearer",
    "-F": "form", "--form": "form", "--form-string": "form",
    # options whose value eyetable doesn't need
    "-o": None, "--output": None, "-m": None, "--max-time": None, "--connect-timeout": None, "--retry": None,
    "-w": None, "--write-out": None, "--cacert": None, "--cert": None, "-E": None, "--key": None,
    "-x": None, "--proxy": None, "--resolve": None, "-r": None, "--range": None, "-T": None, "--upload-file": None,
}
_FLAGS = {
    "-s", "--silent", "-S", "--show-error", "-L", "--location", "-k", "--insecure", "-i", "--include", "-v",
    "--verbose", "--compressed", "-G", "--get", "-f", "--fail", "-g", "--globoff", "-N", "--no-buffer",
    "--http1.1", "--http2", "--fail-with-body", "-#", "--progress-bar", "-I", "--head",
}
_SHORT_FLAG_CHARS = set("sSLkivGfgNI#")


def _normalize_shell(text: str) -> str:
    text = text.strip()
    text = re.sub(r"^\$\s+", "", text)  # a pasted shell prompt
    if re.search(r'\^"|\^\r?\n', text):  # Windows cmd form (Chrome's "Copy as cURL (cmd)")
        text = re.sub(r"\^\r?\n", " ", text)
        text = re.sub(r"\^(.)", r"\1", text)
    text = re.sub(r"\\\r?\n", " ", text)  # bash line continuations
    text = re.sub(r"`\r?\n", " ", text)  # PowerShell line continuations
    return text


def parse_curl(text: str) -> CurlRequest:
    try:
        tokens = shlex.split(_normalize_shell(text), posix=True)
    except ValueError as exc:
        raise CurlError(f"the command couldn't be read ({exc}); check its quotes") from None
    if not tokens or tokens[0].lower() not in ("curl", "curl.exe"):
        raise CurlError("paste a command that starts with curl")

    method = None
    url = None
    headers: list[tuple[str, str]] = []
    data: list[str] = []
    json_body = None
    user = None
    get_mode = head = False
    warnings: list[str] = []

    i, args = 0, tokens[1:]
    while i < len(args):
        arg = args[i]
        i += 1
        value = None
        if arg.startswith("--") and "=" in arg:
            arg, value = arg.split("=", 1)
        if arg in _FLAGS:
            get_mode = get_mode or arg in ("-G", "--get")
            head = head or arg in ("-I", "--head")
            continue
        if arg.startswith("-") and not arg.startswith("--") and len(arg) > 2:
            if set(arg[1:]) <= _SHORT_FLAG_CHARS:  # combined flags like -sSL
                get_mode = get_mode or "G" in arg
                continue
            if arg[:2] in _VALUE_OPTIONS:  # value glued on: -XPOST, -H'Accept: x'
                arg, value = arg[:2], arg[2:]
        if arg in _VALUE_OPTIONS:
            if value is None:
                if i >= len(args):
                    raise CurlError(f"{arg} needs a value")
                value = args[i]
                i += 1
            kind = _VALUE_OPTIONS[arg]
            if kind == "method":
                method = value.upper()
            elif kind == "header":
                name, sep, val = value.partition(":")
                if not sep:
                    raise CurlError(f"header {value!r} should look like Name: value")
                if val.strip() or not value.rstrip().endswith(";"):
                    headers.append((name.strip(), val.strip()))
            elif kind == "data":
                if value.startswith("@"):
                    raise CurlError("data read from a file (-d @file) can't be loaded; paste the data itself")
                data.append(value)
            elif kind == "urlencode":
                name, sep, val = value.partition("=")
                data.append(f"{name}={urllib.parse.quote_plus(val)}" if sep else urllib.parse.quote_plus(value))
            elif kind == "json":
                json_body = value
            elif kind == "user":
                name, _, password = value.partition(":")
                user = (name, password)
            elif kind == "cookie":
                if "=" not in value:
                    raise CurlError("cookies read from a file (-b file) can't be used; paste the cookie values")
                headers.append(("Cookie", value))
            elif kind == "agent":
                headers.append(("User-Agent", value))
            elif kind == "referer":
                headers.append(("Referer", value))
            elif kind == "url":
                url = value
            elif kind == "bearer":
                headers.append(("Authorization", f"Bearer {value}"))
            elif kind == "form":
                raise CurlError("multipart form uploads (-F) aren't supported for loading data")
            elif arg in ("-x", "--proxy"):
                warnings.append("the proxy setting (-x) is ignored")
            continue
        if arg.startswith("-") and arg != "-":
            warnings.append(f"ignored option {arg}")
            continue
        if url is None:
            url = arg
        else:
            warnings.append(f"ignored extra argument {arg!r}")

    if not url:
        raise CurlError("the command has no URL")
    if "://" not in url:
        url = "https://" + url
    parsed = urllib.parse.urlsplit(url)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise CurlError(f"{url!r} is not an http(s) URL")
    if head:
        raise CurlError("a HEAD request (-I) returns no data to load")

    body, is_form = None, False
    if json_body is not None:
        body = json_body
        if not any(h.lower() == "content-type" for h, _ in headers):
            headers.append(("Content-Type", "application/json"))
        if not any(h.lower() == "accept" for h, _ in headers):
            headers.append(("Accept", "application/json"))
    elif data:
        body = "&".join(data)
        is_form = True
    if get_mode and body is not None:  # -G: the data goes in the query string
        sep = "&" if parsed.query else ""
        url = urllib.parse.urlunsplit(parsed._replace(query=parsed.query + sep + body))
        body, is_form = None, False
    if is_form and not any(h.lower() == "content-type" for h, _ in headers):
        headers.append(("Content-Type", "application/x-www-form-urlencoded"))
    method = method or ("POST" if body is not None else "GET")
    return CurlRequest(method, url, headers, body, is_form, user, warnings)


# -- secrets ---------------------------------------------------------------------

_SECRET_HEADERS = {"authorization", "proxy-authorization", "cookie"}
_SECRET_WORDS = ("key", "token", "secret", "auth", "session", "signature", "password", "passwd", "credential")
_SECRET_PARAMS = {"api_key", "apikey", "api-key", "key", "token", "access_token", "auth", "signature", "sig",
                  "secret", "password", "client_secret", "code", "session", "sessionid", "id_token"}


def _secret_header(name: str) -> bool:
    lower = name.lower()
    return lower in _SECRET_HEADERS or any(w in lower for w in _SECRET_WORDS)


def _secret_param(name: str) -> bool:
    lower = name.lower()
    return lower in _SECRET_PARAMS or any(w in lower for w in ("token", "secret", "password", "apikey", "api_key"))


class _Vault:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}

    def hide(self, value: str) -> str:
        for name, existing in self.values.items():
            if existing == value:
                return "{{" + name + "}}"
        name = f"secret_{len(self.values) + 1}"
        self.values[name] = value
        return "{{" + name + "}}"


def _split_query(url: str) -> tuple[urllib.parse.SplitResult, list[tuple[str, str]]]:
    parts = urllib.parse.urlsplit(url)
    return parts, urllib.parse.parse_qsl(parts.query, keep_blank_values=True)


def build_collection(req: CurlRequest) -> tuple[dict[str, Any], dict[str, str]]:
    """The request as a one-item Postman collection, with secrets as {{secret_N}}."""
    vault = _Vault()
    parts, query = _split_query(req.url)
    query = [(k, vault.hide(v) if _secret_param(k) and v else v) for k, v in query]
    # keep {{...}} readable in the stored URL
    raw = urllib.parse.urlunsplit(parts._replace(query=urllib.parse.urlencode(query, safe="{}")))
    headers = [{"key": k, "value": vault.hide(v) if _secret_header(k) and v else v} for k, v in req.headers]
    request: dict[str, Any] = {"method": req.method, "header": headers, "url": raw}
    if req.body is not None:
        body = req.body
        if req.body_is_form:
            pairs = urllib.parse.parse_qsl(body, keep_blank_values=True)
            body = "&".join(
                f"{urllib.parse.quote_plus(k)}={vault.hide(v) if _secret_param(k) and v else urllib.parse.quote_plus(v)}"
                for k, v in pairs
            )
        request["body"] = {"mode": "raw", "raw": body}
    if req.user:
        request["auth"] = {"type": "basic", "basic": [
            {"key": "username", "value": req.user[0]},
            {"key": "password", "value": vault.hide(req.user[1]) if req.user[1] else ""},
        ]}
    collection = {
        "info": {"name": "curl", "schema": "https://schema.getpostman.com/json/collection/v2.1.0/collection.json"},
        "item": [{"name": REQUEST_NAME, "request": request}],
    }
    return collection, vault.values


def mask(text: str, secrets: dict[str, str]) -> str:
    return re.sub(r"\{\{secret_\d+\}\}", MASK, text)


# -- paging ---------------------------------------------------------------------

_PAGE = {"page", "pagenumber", "pageno", "pagenum", "pg", "pageindex"}
_SIZE = {"perpage", "pagesize", "limit", "size", "count", "maxresults", "rows", "top", "take", "resultsperpage",
         "pagelimit", "numresults", "itemsperpage"}
_OFFSET = {"offset", "skip", "start", "startindex", "from", "startat"}
_CURSOR = {"cursor", "after", "pagetoken", "nexttoken", "continuation", "continuationtoken", "startingafter",
           "nextpagetoken", "scrollid", "marker"}
_CURSOR_KEYS = ("next_cursor", "nextcursor", "next_page_token", "nextpagetoken", "next_token", "nexttoken",
                "endcursor", "continuation", "continuationtoken", "cursor", "after", "nextmarker", "next_marker")
_NEXT_KEYS = ("@odata.nextlink", "odata.nextlink", "nextlink", "next_page_url", "nextpageurl", "next_url",
              "nexturl", "next", "next_page", "nextpage")


def _canon(name: str) -> str:
    return re.sub(r"[^a-z]", "", name.lower())


def _int(value: str) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _find_key(payload: Any, wanted: tuple[str, ...], accept, depth: int = 3, prefix: str = "") -> str | None:
    """Dotted path of the first key (by preference order) whose value `accept`s."""
    if not isinstance(payload, dict) or depth < 0:
        return None
    lowered = {k.lower(): k for k in payload}
    for name in wanted:
        key = lowered.get(name)
        if key is not None and accept(payload[key]):
            return f"{prefix}{key}"
    for key, value in payload.items():
        if isinstance(value, dict):
            found = _find_key(value, wanted, accept, depth - 1, f"{prefix}{key}.")
            if found:
                return found
    return None


def infer_paging(url: str, response: httpx.Response | None, payload: Any) -> tuple[dict[str, Any] | None, str]:
    """A pagination spec for the connector, and a sentence explaining it."""
    _, query = _split_query(url)
    names = {_canon(k): (k, v) for k, v in query}
    page = next((names[n] for n in names if n in _PAGE), None)
    size = next((names[n] for n in names if n in _SIZE), None)
    offset = next((names[n] for n in names if n in _OFFSET), None)
    cursor = next((names[n] for n in names if n in _CURSOR), None)
    size_value = _int(size[1]) if size else None

    if page and _int(page[1]) is not None:
        spec: dict[str, Any] = {"type": "page", "param": page[0], "start": _int(page[1])}
        text = f"Every page, starting at {page[0]}={page[1]}"
        if size and size_value:
            spec.update(size_param=size[0], size=size_value)
            text += f", {size_value:,} per page ({size[0]}). Stops at the first page with fewer than {size_value:,} rows"
        else:
            text += ". Stops at the first empty page"
        return pagination_settings(spec), text + f" (at most {DEFAULT_MAX_PAGES:,} pages)."
    if offset and _int(offset[1]) is not None and size and size_value:
        spec = {"type": "offset", "offset_param": offset[0], "limit_param": size[0], "limit": size_value,
                "start": _int(offset[1])}
        return pagination_settings(spec), (
            f"Every page, {size_value:,} rows at a time ({size[0]}), starting at {offset[0]}={offset[1]}. "
            f"Stops at the first page with fewer than {size_value:,} rows (at most {DEFAULT_MAX_PAGES:,} pages)."
        )
    if isinstance(payload, dict) and cursor:
        path = _find_key(payload, _CURSOR_KEYS, lambda v: isinstance(v, (str, int)) and v != "")
        if path:
            spec = {"type": "cursor", "param": cursor[0], "cursor_path": path}
            return pagination_settings(spec), (
                f"Every page: each response's {path} is sent back as {cursor[0]} until it runs out "
                f"(at most {DEFAULT_MAX_PAGES:,} pages)."
            )
    if isinstance(payload, dict):
        path = _find_key(payload, _NEXT_KEYS, lambda v: isinstance(v, str) and (v.startswith("http") or v.startswith("/")))
        if path:
            return pagination_settings({"type": "next_link", "next_path": path}), (
                f"Every page, following the response's {path} link until there is none "
                f"(at most {DEFAULT_MAX_PAGES:,} pages)."
            )
    if response is not None and "next" in response.links:
        return pagination_settings({"type": "next_link"}), (
            f"Every page, following the response's Link: rel=\"next\" header (at most {DEFAULT_MAX_PAGES:,} pages)."
        )
    if cursor:
        return None, f"Only this page: {cursor[0]} looks like a cursor, but the response doesn't say where the next one is."
    if offset and not size:
        return None, f"Only this page: {offset[0]} needs a page size (limit=…) in the command to page through."
    return None, "Only this request's response (the command has no page number, offset or cursor)."


def suggested_name(url: str) -> str:
    parts = urllib.parse.urlsplit(url)
    host = parts.hostname or "api"
    labels = [p for p in host.split(".") if p not in ("www", "api", "com", "org", "net", "io", "co", "dev", "app")]
    site = labels[-1] if labels else host.split(".")[0]
    segments = [s for s in parts.path.split("/") if s and not re.fullmatch(r"v\d+|\d+|api|[0-9a-f-]{16,}", s, re.I)]
    tail = segments[-1] if segments else None
    return source_name(f"{site}_{tail}" if tail and sanitize(site) != sanitize(tail) else site)


@dataclass
class CurlPlan:
    method: str
    url: str  # with secrets masked
    headers: list[dict[str, str]]  # values masked where secret
    secrets: int
    pagination: dict[str, Any] | None
    paging_text: str
    columns: list[dict[str, str]]
    rows: int
    sample: list[dict[str, Any]]
    suggested_name: str
    warnings: list[str]


def prepare(text: str, secret_key: str | None) -> tuple[CurlRequest, dict[str, Any], dict[str, str]]:
    """Parse a command into (request, stored config with encrypted secrets, plain secrets)."""
    req = parse_curl(text)
    collection, secrets = build_collection(req)
    config: dict[str, Any] = {
        "collection": collection,
        "variables": {name: encrypt_secret(value, secret_key) for name, value in secrets.items()} if secrets else {},
        "probe": True,
    }
    if req.method != "GET":
        config["allow_methods"] = [req.method]
    return req, config, secrets


def probe(req: CurlRequest, config: dict[str, Any], secrets: dict[str, str],
          client: httpx.Client | None = None) -> tuple[httpx.Response, Any, list[dict[str, Any]]]:
    """Send the request once: (response, parsed body, records)."""
    connector = CurlConnector({**config, "variables": dict(secrets)}, client=client)
    try:
        request = connector.requests[REQUEST_NAME]
        response = connector._send(connector._build(request))
    except httpx.HTTPStatusError as exc:
        raise CurlError(f"the API answered {exc.response.status_code} {exc.response.reason_phrase}") from None
    except httpx.HTTPError as exc:
        raise CurlError(f"couldn't reach the API ({type(exc).__name__}: {exc})") from None
    finally:
        if client is None:
            connector.close()
    payload = parse_response(response)
    return response, payload, extract_records(payload)


def plan(text: str, secret_key: str | None, client: httpx.Client | None = None) -> tuple[CurlPlan, dict[str, Any]]:
    """Check a command: what it requests, how it pages, and what its first page holds."""
    req, config, secrets = prepare(text, secret_key)
    response, payload, records = probe(req, config, secrets, client)
    paging, paging_text = infer_paging(req.url, response, payload)
    if paging:
        config["pagination"] = {REQUEST_NAME: paging}
    warnings = list(req.warnings)
    if req.method != "GET":
        warnings.append(f"This is a {req.method} request; eyetable sends it again every time the source loads.")
    kinds = infer_columns(records)
    stored = config["collection"]["item"][0]["request"]
    result = CurlPlan(
        method=req.method,
        url=mask(stored["url"], secrets),
        headers=[{"name": h["key"], "value": mask(h["value"], secrets)} for h in stored["header"]],
        secrets=len(secrets),
        pagination=paging,
        paging_text=paging_text,
        columns=[{"name": k, "type": v or "text"} for k, v in kinds.items()],
        rows=len(records),
        sample=records[:5],
        suggested_name=suggested_name(req.url),
        warnings=warnings,
    )
    return result, config
