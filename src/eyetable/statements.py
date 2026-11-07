"""One curl source run as a set of statements, all landing in its one table.

A set takes parts of the stored request and gives each a series of values:

  * dates: every N days between two dates, each of which can be "today" or
    "today-30", so a schedule can keep a rolling window. The text is written
    back in the format it had (2026-11-04, 11/04/2026 or 20261104).
  * numbers: from, to, step (page=1, 2, 3...).
  * list: given values (status eq 'received', then 'paid').

With several parts, every combination is one statement. Each part names what it
changes: a query parameter's text, or text in the path or the body. Text only
matches on its own, so 1 doesn't match inside 2026. Every place the text appears
gets the new value, except a date window: statements covering several days put
the first day at `find` and the last at `end_find`, and when those are the same
text (ge 2026-11-04 and le 2026-11-04) the first place gets the first day and
the others the last.

Stored in a source's config as {"parts": [...], "pause_seconds": 0, "stop_when_empty": false}.
"""

import datetime as dt
import itertools
import math
import re
import urllib.parse
from dataclasses import dataclass
from typing import Any, Iterator

MAX_STATEMENTS = 5000
MAX_PARTS = 5
_DATE_FORMATS = ((r"\d{4}-\d{2}-\d{2}", "%Y-%m-%d"), (r"\d{2}/\d{2}/\d{4}", "%m/%d/%Y"), (r"\d{8}", "%Y%m%d"))
_RELATIVE_DAY = re.compile(r"today(?:\s*([+-])\s*(\d+))?", re.I)
_PIECE = re.compile(r"'([^']+)'|\"([^\"]+)\"|(\d{4}-\d{2}-\d{2}|\d{2}/\d{2}/\d{4})|(?<![\w.:-])(\d+)(?![\w.:-])")
_PAGING_PARAMS = ("param", "size_param", "offset_param", "limit_param")


class StatementError(ValueError):
    pass


@dataclass
class Change:
    target: str  # "query", "path" or "body"
    name: str | None  # the query parameter
    find: str
    value: str
    end_find: str | None = None
    end_value: str | None = None


@dataclass
class Statement:
    label: str
    changes: list[Change]


def date_format(text: str) -> str | None:
    """The strftime format of a date written in one of the known ways."""
    for pattern, fmt in _DATE_FORMATS:
        if re.fullmatch(pattern, text):
            try:
                dt.datetime.strptime(text, fmt)
            except ValueError:
                return None
            return fmt
    return None


def parse_day(text: Any, today: dt.date) -> dt.date:
    text = str(text or "").strip()
    relative = _RELATIVE_DAY.fullmatch(text)
    if relative:
        days = int(relative.group(2) or 0)
        return today + dt.timedelta(days=-days if relative.group(1) == "-" else days)
    try:
        return dt.date.fromisoformat(text)
    except ValueError:
        raise StatementError(f"{text!r} isn't a date; use 2026-11-04, today or today-30") from None


def _matcher(find: str) -> str:
    """`find` as a regex that doesn't match inside a longer number or word."""
    before = r"(?<!\d)" if find[0].isdigit() else r"(?<![A-Za-z])" if find[0].isalpha() else ""
    after = r"(?!\d)" if find[-1].isdigit() else r"(?![A-Za-z])" if find[-1].isalpha() else ""
    return before + re.escape(find) + after


def occurrences(text: str, find: str) -> int:
    return len(re.findall(_matcher(find), text)) if find else 0


def pieces(text: str, limit: int = 12) -> list[dict[str, Any]]:
    """The parts of a value worth making a series of: dates, numbers and quoted text."""
    found: dict[str, dict[str, Any]] = {}
    for match in _PIECE.finditer(text):
        piece = next(g for g in match.groups() if g is not None)
        if "{{" in piece or piece in found:
            continue
        kind = "dates" if date_format(piece) else "numbers" if piece.isdigit() else "list"
        found[piece] = {"text": piece, "kind": kind, "count": occurrences(text, piece)}
    return list(found.values())[:limit]


def encode_query(pairs: list[tuple[str, str]]) -> str:
    # quote, not quote_plus: a space stays %20, which is all that some APIs (OData filters) accept;
    # {{...}} stays readable in a stored URL
    return urllib.parse.urlencode(pairs, safe="{}", quote_via=urllib.parse.quote)


def _substitute(text: str, changes: list[Change]) -> str:
    values: dict[str, list[str]] = {}
    for change in changes:
        values[change.find] = [change.value]
        if change.end_find == change.find:
            values[change.find].append(change.end_value)
        elif change.end_find:
            values[change.end_find] = [change.end_value]
    used = dict.fromkeys(values, 0)

    def swap(match: re.Match) -> str:
        found = match.group(0)
        used[found] += 1
        return values[found][min(used[found], len(values[found])) - 1]

    return re.sub("|".join(_matcher(f) for f in sorted(values, key=len, reverse=True)), swap, text)


def rewrite(url: str, body: dict | None, changes: list[Change]) -> tuple[str, dict | None]:
    """The URL and body of a statement."""
    parts = urllib.parse.urlsplit(url)
    path = [c for c in changes if c.target == "path"]
    if path:
        parts = parts._replace(path=urllib.parse.quote(_substitute(urllib.parse.unquote(parts.path), path), safe="/{}"))
    query = [c for c in changes if c.target == "query"]
    if query:
        pairs = []
        for name, value in urllib.parse.parse_qsl(parts.query, keep_blank_values=True):
            mine = [c for c in query if c.name == name]
            pairs.append((name, _substitute(value, mine) if mine else value))
        parts = parts._replace(query=encode_query(pairs))
    in_body = [c for c in changes if c.target == "body"]
    if in_body and body:
        body = {**body, "raw": _substitute(body.get("raw") or "", in_body)}
    return urllib.parse.urlunsplit(parts), body


def _dates(part: dict[str, Any], today: dt.date) -> list[tuple[str, Change]]:
    first, last = parse_day(part["from"], today), parse_day(part["to"], today)
    if first > last:
        raise StatementError(f"the dates for {part['find']} run from {first} to {last}, so there are none to run")
    days = part.get("days", 1)
    if (last - first).days // days >= MAX_STATEMENTS:
        raise StatementError(f"that's more than {MAX_STATEMENTS:,} statements")
    fmt, end_find = date_format(part["find"]), part.get("end_find")
    end_fmt = date_format(end_find) if end_find else None
    out = []
    while first <= last:
        stop = min(first + dt.timedelta(days=days - 1), last)
        label = first.isoformat() if stop == first else f"{first} to {stop}"
        out.append((label, _change(part, first.strftime(fmt), stop.strftime(end_fmt) if end_fmt else None)))
        first = stop + dt.timedelta(days=1)
    return out


def _change(part: dict[str, Any], value: str, end_value: str | None = None) -> Change:
    return Change(part["target"], part.get("name"), part["find"], value, part.get("end_find") if end_value else None, end_value)


def _series(part: dict[str, Any], today: dt.date) -> list[tuple[str, Change]]:
    if part["kind"] == "dates":
        return _dates(part, today)
    if part["kind"] == "numbers":
        if (part["to"] - part["from"]) // part["step"] >= MAX_STATEMENTS:
            raise StatementError(f"that's more than {MAX_STATEMENTS:,} statements")
        values = [str(n) for n in range(part["from"], part["to"] + 1, part["step"])]
    else:
        values = part["values"]
    return [(value, _change(part, value)) for value in values]


def count(plan: dict[str, Any], today: dt.date | None = None) -> int:
    today = today or dt.date.today()
    return math.prod(len(_series(part, today)) for part in plan["parts"])


def expand(plan: dict[str, Any], today: dt.date | None = None) -> Iterator[Statement]:
    """Every statement of a set, the first part changing slowest."""
    today = today or dt.date.today()
    for combination in itertools.product(*(_series(part, today) for part in plan["parts"])):
        yield Statement(" · ".join(label for label, _ in combination), [change for _, change in combination])


def sets_paging(plan: dict[str, Any], paging: dict[str, Any] | None) -> bool:
    """Whether the set itself changes what automatic paging would, so paging must stay out of its way."""
    names = {paging.get(key) for key in _PAGING_PARAMS} if paging else set()
    return any(part["target"] == "query" and part.get("name") in names for part in plan["parts"])


def _component(part: dict[str, Any], url: str, body: dict | None) -> str:
    parts = urllib.parse.urlsplit(url)
    target = part.get("target")
    if target == "path":
        return urllib.parse.unquote(parts.path)
    if target == "body":
        return (body or {}).get("raw") or ""
    if target != "query":
        raise StatementError("a part changes the query, the path or the body")
    values = [v for k, v in urllib.parse.parse_qsl(parts.query, keep_blank_values=True) if k == part.get("name")]
    if not values:
        raise StatementError(f"the request has no {part.get('name')} parameter")
    return values[0]


def _integer(part: dict[str, Any], key: str) -> int:
    value = part.get(key)
    if isinstance(value, str) and re.fullmatch(r"-?\d+", value.strip()):
        return int(value)
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    raise StatementError(f"{key} must be a whole number")


def _clean_part(part: dict[str, Any], url: str, body: dict | None, today: dt.date) -> dict[str, Any]:
    text = _component(part, url, body)
    find = str(part.get("find") or "").strip() or text
    where = part.get("name") or part.get("target")
    if not find or "{{" in find:
        raise StatementError(f"choose the text to change in {where}")
    if not occurrences(text, find):
        raise StatementError(f"{find!r} isn't in {where}")
    out: dict[str, Any] = {"target": part["target"], "find": find, "kind": part.get("kind")}
    if part["target"] == "query":
        out["name"] = part["name"]
    if out["kind"] == "dates":
        if not date_format(find):
            raise StatementError(f"{find!r} isn't a date written as 2026-11-04, 11/04/2026 or 20261104")
        days = _integer({"days": part.get("days", 1)}, "days")
        if not 1 <= days <= 366:
            raise StatementError("days per statement must be between 1 and 366")
        out.update({"from": str(part.get("from") or "").strip(), "to": str(part.get("to") or "").strip(), "days": days})
        end_find = str(part.get("end_find") or "").strip()
        if end_find:
            if not date_format(end_find):
                raise StatementError(f"{end_find!r} isn't a date written as 2026-11-04, 11/04/2026 or 20261104")
            if not occurrences(text, end_find):
                raise StatementError(f"{end_find!r} isn't in {where}")
            out["end_find"] = end_find
    elif out["kind"] == "numbers":
        out.update({"from": _integer(part, "from"), "to": _integer(part, "to"), "step": _integer({"step": part.get("step", 1)}, "step")})
        if out["step"] < 1:
            raise StatementError("the step must be at least 1")
        if out["from"] > out["to"]:
            raise StatementError(f"the numbers for {find} run from {out['from']} to {out['to']}, so there are none to run")
    elif out["kind"] == "list":
        values = part.get("values")
        values = [str(v).strip() for v in values] if isinstance(values, list) else []
        out["values"] = list(dict.fromkeys(v for v in values if v))
        if not out["values"]:
            raise StatementError(f"give at least one value for {find}")
    else:
        raise StatementError("a part is dates, numbers or a list")
    return out


def normalize(plan: dict[str, Any], url: str, body: dict | None, today: dt.date | None = None) -> dict[str, Any]:
    """A set checked against its request, ready to store."""
    today = today or dt.date.today()
    given = plan.get("parts") or []
    if not given:
        raise StatementError("choose something in the request to change")
    if len(given) > MAX_PARTS:
        raise StatementError(f"a set can change at most {MAX_PARTS} things")
    pause = float(plan.get("pause_seconds") or 0)
    if not 0 <= pause <= 60:
        raise StatementError("the pause must be between 0 and 60 seconds")
    clean = {"parts": [_clean_part(p, url, body, today) for p in given], "pause_seconds": pause,
             "stop_when_empty": bool(plan.get("stop_when_empty"))}
    total = count(clean, today)
    if total > MAX_STATEMENTS:
        raise StatementError(f"that's {total:,} statements; a set can have at most {MAX_STATEMENTS:,}")
    return clean
