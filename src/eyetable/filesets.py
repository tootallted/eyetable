"""File sets: a file source that reads every file matching a pattern in a folder on the server.

A file source normally holds files uploaded through the page. Given a folder on
the eyetable server and a file name pattern (`ledger_*.csv`), it instead reads
every matching file there on each load, so Reload, groups and schedules pick up
new files by themselves. All the files load into the source's one table: a
column found in any file gets a column, and rows from files without it are NULL.

Only CSV, TSV and text files are read. EYETABLE_FILE_FOLDERS (folders separated
by ;) limits which folders may be used; with it unset, any folder the eyetable
service can see is allowed.
"""

from __future__ import annotations

import datetime as dt
import fnmatch
import os
import re
from pathlib import Path
from typing import Any

from .identifiers import sanitize

FILE_SUFFIXES = (".csv", ".tsv", ".txt")
WILDCARDS = re.compile(r"[*?\[]")


class FileSetProblem(ValueError):
    """A folder or pattern that can't be used."""


def is_pattern(name: str) -> bool:
    return bool(WILDCARDS.search(name or ""))


def allowed_folders() -> list[Path]:
    raw = os.environ.get("EYETABLE_FILE_FOLDERS", "")
    return [Path(p.strip()).resolve() for p in raw.split(";") if p.strip()]


def check_folder(folder: str) -> Path:
    if not folder or not folder.strip():
        raise FileSetProblem("give the folder on the eyetable server that holds the files")
    path = Path(folder.strip().strip('"')).expanduser()
    if not path.is_absolute():
        raise FileSetProblem(f"{folder} isn't a full path; give it from the drive, like C:\\data\\ledgers")
    path = path.resolve()
    roots = allowed_folders()
    if roots and not any(path == r or r in path.parents for r in roots):
        raise FileSetProblem(f"{path} isn't in a folder eyetable may read (EYETABLE_FILE_FOLDERS: "
                             f"{'; '.join(str(r) for r in roots)})")
    if not path.is_dir():
        raise FileSetProblem(f"the folder {path} doesn't exist on the eyetable server, or the eyetable service can't "
                             "see it (a mapped drive letter isn't visible to a service: use its \\\\server\\share path)")
    return path


def check_pattern(pattern: str) -> str:
    pattern = (pattern or "").strip()
    if not pattern:
        raise FileSetProblem("give a file name, or a pattern such as ledger_*.csv")
    if "/" in pattern or "\\" in pattern:
        raise FileSetProblem("the file name or pattern is just a name, like ledger_*.csv; the folder goes in Folder")
    if not pattern.lower().endswith(FILE_SUFFIXES) and not pattern.endswith("*"):
        raise FileSetProblem(f"only {', '.join(FILE_SUFFIXES)} files can be read; end the pattern with one of them")
    return pattern


def matching_files(folder: str, pattern: str) -> list[Path]:
    """The files in `folder` (not its subfolders) whose names match `pattern`, by name."""
    path, pattern = check_folder(folder), check_pattern(pattern)
    found = [f for f in path.iterdir()
             if f.is_file() and fnmatch.fnmatch(f.name.lower(), pattern.lower()) and f.suffix.lower() in FILE_SUFFIXES]
    return sorted(found, key=lambda f: f.name.lower())


def describe_files(files: list[Path], limit: int = 200) -> list[dict[str, Any]]:
    out = []
    for f in files[:limit]:
        st = f.stat()
        out.append({"name": f.name, "size": st.st_size,
                    "modified": dt.datetime.fromtimestamp(st.st_mtime, dt.timezone.utc)})
    return out


def name_for(pattern: str) -> str:
    """A source name for a pattern: ledger_*.csv -> ledger_aggregated."""
    stem = re.sub(r"\.[A-Za-z0-9*?]+$", "", pattern.strip())
    stem = WILDCARDS.sub(" aggregated ", re.sub(r"\[[^\]]*\]", "*", stem))
    name = re.sub(r"aggregated(_aggregated)+", "aggregated", sanitize(stem))
    if not name[0].isalpha():
        name = "s" + name if name[0] == "_" else "s_" + name
    return name[:64].rstrip("_")
