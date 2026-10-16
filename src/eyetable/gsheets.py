"""Google Sheets: one eyetable source per tab of a spreadsheet.

A sheet link is downloaded the way Google's own "Download > Microsoft Excel"
works (an .xlsx export containing every tab), so no Google account or API key
is involved. That means the spreadsheet must be readable by link: shared as
"Anyone with the link can view", or published to the web.

Each tab becomes a source named "<file name>_<tab name>", e.g. the tab
"Q1 Actuals" in "General Ledger 2026" -> general_ledger_2026_q1_actuals.
"""

import datetime as dt
import io
import re
import urllib.parse
from dataclasses import dataclass, field
from typing import Any

import httpx

from eyetable.connectors.file import unique_headers
from eyetable.identifiers import sanitize
from eyetable.identifiers import source_name as make_source_name

_SHEET_URL = re.compile(r"docs\.google\.com/spreadsheets/(?:u/\d+/)?d/(e/)?([A-Za-z0-9_-]{20,})")
_BARE_ID = re.compile(r"[A-Za-z0-9_-]{25,}")
_XLSX = "spreadsheetml"


class SheetError(ValueError):
    pass


@dataclass
class Tab:
    title: str
    index: int
    headers: list[str]
    records: list[dict[str, Any]]
    hidden: bool = False


@dataclass
class Workbook:
    title: str
    spreadsheet_id: str
    published: bool
    url: str
    tabs: list[Tab] = field(default_factory=list)


def parse_sheet_url(url: str) -> tuple[str, bool]:
    """(spreadsheet id, is a published-to-web id) from a Sheets link or a bare id."""
    url = url.strip()
    match = _SHEET_URL.search(url)
    if match:
        return match.group(2), bool(match.group(1))
    if _BARE_ID.fullmatch(url):
        return url, False
    raise SheetError("that doesn't look like a Google Sheets link (https://docs.google.com/spreadsheets/d/...)")


def export_url(spreadsheet_id: str, published: bool) -> str:
    # Built from the id alone, never from the caller's URL, so only Google is ever fetched.
    if published:
        return f"https://docs.google.com/spreadsheets/d/e/{spreadsheet_id}/pub?output=xlsx"
    return f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=xlsx"


def sheet_link(spreadsheet_id: str, published: bool) -> str:
    if published:
        return f"https://docs.google.com/spreadsheets/d/e/{spreadsheet_id}/pubhtml"
    return f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit"


def title_from_disposition(header: str | None) -> str | None:
    """The file name Google puts on the download, without its extension."""
    if not header:
        return None
    star = re.search(r"filename\*\s*=\s*(?:UTF-8|utf-8)''([^;]+)", header)
    if star:
        name = urllib.parse.unquote(star.group(1).strip().strip('"'))
    else:
        plain = re.search(r'filename\s*=\s*"([^"]*)"', header) or re.search(r"filename\s*=\s*([^;]+)", header)
        if not plain:
            return None
        name = plain.group(1).strip()
    name = re.sub(r"\.xlsx$", "", name, flags=re.I).strip()
    return name or None


def source_name(file_title: str, tab_title: str) -> str:
    return make_source_name(f"{sanitize(file_title)}_{sanitize(tab_title)}")


def _cell_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value).strip()


def _normalize_dates(records: list[dict[str, Any]], headers: list[str]) -> None:
    """Sheets dates arrive as midnight datetimes; a column of only those is a date column."""
    for name in headers:
        values = [r[name] for r in records if r[name] is not None]
        if values and all(isinstance(v, dt.datetime) and v.time() == dt.time() and v.tzinfo is None for v in values):
            for r in records:
                if r[name] is not None:
                    r[name] = r[name].date()


def parse_workbook(content: bytes) -> list[Tab]:
    import openpyxl

    try:
        wb = openpyxl.load_workbook(io.BytesIO(content), read_only=True, data_only=True)
    except Exception as exc:
        raise SheetError(f"Google's export could not be read as a spreadsheet ({type(exc).__name__})") from None
    tabs = []
    try:
        for index, ws in enumerate(wb.worksheets):
            rows = [
                list(r)
                for r in ws.iter_rows(values_only=True)
                if any(v is not None and _cell_text(v) != "" for v in r)
            ]
            if not rows:
                tabs.append(Tab(ws.title, index, [], [], ws.sheet_state != "visible"))
                continue
            width = max(max((i + 1 for i, v in enumerate(r) if v is not None and _cell_text(v) != ""), default=0) for r in rows)
            headers = unique_headers([_cell_text(v) for v in rows[0]], width)
            records = []
            for r in rows[1:]:
                record = {}
                for i, name in enumerate(headers):
                    v = r[i] if i < len(r) else None
                    if isinstance(v, str):
                        v = v if v.strip() else None
                    record[name] = v
                records.append(record)
            _normalize_dates(records, headers)
            tabs.append(Tab(ws.title, index, headers, records, ws.sheet_state != "visible"))
    finally:
        wb.close()
    return tabs


def fetch_workbook(url: str, client: httpx.Client | None = None, max_bytes: int = 50 * 1024 * 1024) -> Workbook:
    spreadsheet_id, published = parse_sheet_url(url)
    own_client = client is None
    client = client or httpx.Client(timeout=60, follow_redirects=True)
    try:
        with client.stream("GET", export_url(spreadsheet_id, published)) as response:
            content_type = response.headers.get("content-type", "")
            if response.status_code in (401, 403, 404) or (response.is_success and _XLSX not in content_type):
                raise SheetError(
                    "Google didn't hand over the spreadsheet. Share it as \"Anyone with the link can view\" "
                    "(or File > Share > Publish to web), then try again."
                )
            if not response.is_success:
                raise SheetError(f"Google answered {response.status_code} for that spreadsheet")
            chunks, size = [], 0
            for chunk in response.iter_bytes():
                size += len(chunk)
                if size > max_bytes:
                    raise SheetError(f"the spreadsheet is larger than {max_bytes // (1024 * 1024)} MB")
                chunks.append(chunk)
            title = title_from_disposition(response.headers.get("content-disposition")) or "google_sheet"
    except httpx.HTTPError as exc:
        raise SheetError(f"couldn't reach Google Sheets ({type(exc).__name__})") from None
    finally:
        if own_client:
            client.close()
    return Workbook(title, spreadsheet_id, published, sheet_link(spreadsheet_id, published), parse_workbook(b"".join(chunks)))
