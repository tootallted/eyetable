"""Turning arbitrary source names into safe, readable SQL identifiers."""

import hashlib
import re

# Postgres truncates identifiers at 63 bytes; leave room for a staging suffix.
MAX_TABLE_NAME = 50
MAX_COLUMN_NAME = 63

# Source names double as table names and query names, so they stay within
# Postgres's 63-character identifier limit.
MAX_SOURCE_NAME = 63
SOURCE_NAME = re.compile(r"^[a-z][a-z0-9_]{0,62}$")


def sanitize(name: str) -> str:
    s = re.sub(r"[^0-9a-zA-Z]+", "_", name).strip("_").lower()
    if not s:
        s = "unnamed"
    if s[0].isdigit():
        s = f"_{s}"
    return s


def shorten(name: str, limit: int) -> str:
    if len(name) <= limit:
        return name
    digest = hashlib.sha1(name.encode()).hexdigest()[:8]
    return f"{name[: limit - 9]}_{digest}"


def source_name(text: str) -> str:
    """A valid source name made from arbitrary text (a file or tab name, say)."""
    name = sanitize(text)
    if not name[0].isalpha():
        name = f"s_{name.lstrip('_')}"
    return shorten(name, MAX_SOURCE_NAME)


# Tags label sources (#base, #finance) and let a group pick them up by label.
TAG = re.compile(r"^[a-z0-9][a-z0-9_-]{0,39}$")


def normalize_tag(text: str) -> str:
    """'#Base ' -> 'base'; raises ValueError for anything that isn't a valid tag."""
    tag = (text or "").strip().lstrip("#").strip().lower()
    if not TAG.match(tag):
        raise ValueError(
            f"#{tag or '…'} isn't a valid tag: use letters, digits, _ or -, starting with a letter or digit (max 40)"
        )
    return tag


def table_name(source: str, dataset_name: str) -> str:
    return shorten(f"{source}__{sanitize(dataset_name)}", MAX_TABLE_NAME)


def column_name(key: str) -> str:
    return shorten(sanitize(key), MAX_COLUMN_NAME)
