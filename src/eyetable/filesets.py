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


# -- finding where an uploaded file came from ------------------------------------------
# A browser never tells a page which folder an uploaded file was in, only its name. The
# eyetable server usually runs on the same PC, so it looks for a file with the same name,
# size and contents (SHA-256) in the usual places: Desktop, Downloads, Documents.

SKIP_DIRS = {"appdata", "node_modules", "__pycache__", ".git", ".venv", "venv", "$recycle.bin"}
SEARCH_DEPTH = 4
SEARCH_MAX_DIRS = 20_000


def search_roots() -> list[Path]:
    """Where to look: the allowed folders if EYETABLE_FILE_FOLDERS is set, else the usual user folders."""
    roots = allowed_folders()
    if roots:
        return [r for r in roots if r.is_dir()]
    home = Path.home()
    found = []
    for rel in ("Desktop", "OneDrive/Desktop", "Downloads", "Documents", "OneDrive/Documents", "OneDrive", "."):
        p = (home / rel).resolve()
        if p.is_dir() and p not in found:
            found.append(p)
    return found


def _sha256(path: Path) -> str:
    import hashlib

    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def find_upload(filename: str, size: int, sha256: str, roots: list[Path] | None = None) -> Path | None:
    """The file on this machine an upload came from: same name, size and contents. None if not found."""
    roots = search_roots() if roots is None else roots
    target = filename.lower()
    seen = 0
    for root in roots:
        depth = 1 if root == Path.home().resolve() else SEARCH_DEPTH  # the home folder itself: just its top level
        stack = [(root, 0)]
        while stack:
            folder, level = stack.pop()
            seen += 1
            if seen > SEARCH_MAX_DIRS:
                return None
            try:
                entries = list(os.scandir(folder))
            except OSError:
                continue
            for e in entries:
                try:
                    if e.is_file() and e.name.lower() == target and e.stat().st_size == size and _sha256(Path(e.path)) == sha256:
                        return Path(e.path)
                    if e.is_dir(follow_symlinks=False) and level + 1 < depth and e.name.lower() not in SKIP_DIRS \
                            and not e.name.startswith("."):
                        stack.append((Path(e.path), level + 1))
                except OSError:
                    continue
    return None
