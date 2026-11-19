"""Excel and CSV files in OneDrive or SharePoint, read as the person signed in with Microsoft.

A file's sharing link (or its address in the browser) is turned into a Microsoft
Graph share ID, and the file is downloaded with that person's Graph token
(scope Files.Read.All), so it opens exactly as it would for them. An Excel
workbook becomes one table per tab, like a Google Sheet (see gsheets); a CSV
file is one tab. Nothing about the sign-in is kept.
"""

from __future__ import annotations

import base64
import csv
import io
import re
import urllib.parse

import httpx

from eyetable import gsheets
from eyetable.gsheets import SheetError, Tab, Workbook

GRAPH = "https://graph.microsoft.com/v1.0"
GRAPH_SCOPE = "https://graph.microsoft.com/Files.Read.All"
_HOSTS = re.compile(r"(^|\.)(sharepoint\.com|sharepoint\.us|sharepoint-df\.com|onedrive\.live\.com|1drv\.ms)$", re.I)
EXCEL = (".xlsx", ".xlsm")
TEXT = (".csv", ".txt")


def is_microsoft_url(url: str) -> bool:
    try:
        host = urllib.parse.urlsplit(url.strip()).hostname
    except ValueError:
        return False
    return bool(host and _HOSTS.search(host))


def share_id(url: str) -> str:
    """Graph's ID for a sharing link: "u!" + the link, base64url without padding."""
    return "u!" + base64.urlsafe_b64encode(url.strip().encode()).decode().rstrip("=")


def _graph_problem(response: httpx.Response, what: str) -> SheetError:
    try:
        detail = response.json().get("error", {}).get("message") or ""
    except ValueError:
        detail = ""
    if response.status_code == 401:
        return SheetError("your Microsoft sign-in has expired or doesn't allow reading files; sign in with "
                          "Microsoft again", needs_sign_in=True)
    if response.status_code in (403, 404):
        return SheetError(f"the Microsoft account you're signed in with can't open {what}: check the link and "
                          f"that the file is shared with you{f' ({detail})' if detail else ''}", needs_sign_in=True)
    return SheetError(f"Microsoft answered {response.status_code} for {what}{f': {detail}' if detail else ''}")


def _csv_tab(title: str, content: bytes) -> Tab:
    for encoding in ("utf-8-sig", "cp1252"):
        try:
            text = content.decode(encoding)
            break
        except UnicodeDecodeError:
            continue
    rows = list(csv.reader(io.StringIO(text)))
    return gsheets.tab_from_rows(title, 0, False, rows)


def fetch_workbook(url: str, token: str | None, client: httpx.Client | None = None,
                   max_bytes: int = 50 * 1024 * 1024) -> Workbook:
    """The file at a OneDrive / SharePoint link, as a workbook of tabs."""
    if not token:
        raise SheetError("this file is in OneDrive or SharePoint: sign in with Microsoft to load it as yourself",
                         needs_sign_in=True)
    http = client or httpx.Client(timeout=60, follow_redirects=True)
    headers = {"Authorization": f"Bearer {token}"}
    item_url = f"{GRAPH}/shares/{share_id(url)}/driveItem"
    try:
        meta = http.get(item_url, headers=headers, params={"$select": "id,name,size,file,parentReference"})
        if meta.status_code != 200:
            raise _graph_problem(meta, "this file")
        item = meta.json()
        name = item.get("name") or "file"
        lower = name.lower()
        if "file" not in item:
            raise SheetError(f"{name} is a folder; link to an Excel or CSV file in it")
        if not lower.endswith(EXCEL + TEXT):
            raise SheetError(f"{name} isn't an Excel (.xlsx) or CSV file")
        if (item.get("size") or 0) > max_bytes:
            raise SheetError(f"{name} is {item['size'] / 1024 / 1024:,.0f} MB, over the {max_bytes // 1024 // 1024} MB limit")
        body = http.get(f"{item_url}/content", headers=headers)
        if body.status_code != 200:
            raise _graph_problem(body, name)
    except httpx.HTTPError as exc:
        raise SheetError(f"couldn't reach Microsoft to read the file ({type(exc).__name__})") from None
    finally:
        if client is None:
            http.close()
    title = re.sub(r"\.[^.]+$", "", name)
    if lower.endswith(TEXT):
        tabs = [_csv_tab(title, body.content)]
    else:
        tabs = gsheets.parse_workbook(body.content, what=name)
    drive = (item.get("parentReference") or {}).get("driveId", "")
    return Workbook(title, f"ms:{drive}:{item['id']}", False, url.strip(), tabs, provider="microsoft")
