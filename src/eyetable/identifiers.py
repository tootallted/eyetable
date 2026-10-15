"""Turning arbitrary source names into safe, readable SQL identifiers."""

import hashlib
import re

# Postgres truncates identifiers at 63 bytes; leave room for a staging suffix.
MAX_TABLE_NAME = 50
MAX_COLUMN_NAME = 63

SOURCE_NAME = re.compile(r"^[a-z][a-z0-9_]{0,39}$")


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


def table_name(source: str, dataset_name: str) -> str:
    return shorten(f"{source}__{sanitize(dataset_name)}", MAX_TABLE_NAME)


def column_name(key: str) -> str:
    return shorten(sanitize(key), MAX_COLUMN_NAME)
