"""Point the published source management page at a new API URL.

Used by run-public.ps1 whenever the Cloudflare tunnel gets a new address.
Only the `var API_BASE = '...';` line changes, and the commit is built from
the committed page (HEAD), so unrelated edits in the working copy are never
swept into the automatic commit. The same line is updated in the working
copy so it stays in step.

    python scripts/publish_api_base.py https://example.trycloudflare.com
"""

import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path

PAGE = "docs/source_management/index.html"
LINE = re.compile(rb"var API_BASE = '[^']*';")


def git(*args: str, input: bytes | None = None, index: str | None = None) -> bytes:
    env = {**os.environ, "GIT_INDEX_FILE": index} if index else None
    result = subprocess.run(["git", *args], input=input, capture_output=True, cwd=REPO, env=env)
    if result.returncode != 0:
        raise SystemExit(f"git {args[0]} failed: {result.stderr.decode(errors='replace').strip()}")
    return result.stdout


def main(url: str) -> None:
    if not re.fullmatch(r"https://[a-z0-9-]+\.trycloudflare\.com", url):
        raise SystemExit(f"refusing to publish unexpected URL {url!r}")
    new_line = f"var API_BASE = '{url}';".encode()

    working = REPO / PAGE
    current = working.read_bytes()
    if LINE.sub(new_line, current, count=1) != current:
        working.write_bytes(LINE.sub(new_line, current, count=1))

    committed = git("show", f"HEAD:{PAGE}")
    if len(LINE.findall(committed)) != 1:
        raise SystemExit("the committed page does not have exactly one API_BASE line")
    updated = LINE.sub(new_line, committed, count=1)
    if updated == committed:
        print("page already points at this URL")
        return
    if git("rev-parse", "--abbrev-ref", "HEAD").strip() != b"main":
        raise SystemExit("not on main; not publishing")
    old_blob = git("rev-parse", f"HEAD:{PAGE}").decode().strip()
    blob = git("hash-object", "-w", "--stdin", input=updated).decode().strip()

    # Build the commit in a private index (HEAD + this one file), so nothing else
    # that happens to be staged in the real index can end up in it.
    with tempfile.TemporaryDirectory() as tmp:
        index = str(Path(tmp) / "index")
        git("read-tree", "HEAD", index=index)
        git("update-index", "--cacheinfo", f"100644,{blob},{PAGE}", index=index)
        tree = git("write-tree", index=index).decode().strip()
    parent = git("rev-parse", "HEAD").decode().strip()
    commit = git("commit-tree", tree, "-p", parent, "-m", f"Point source management page at {url}").decode().strip()
    git("update-ref", "refs/heads/main", commit, parent)

    # Keep the real index in step for this file, unless someone staged their own version of it.
    staged = git("ls-files", "-s", "--", PAGE).decode().split()
    if len(staged) >= 2 and staged[1] == old_blob:
        git("update-index", "--cacheinfo", f"100644,{blob},{PAGE}")

    git("push", "origin", "main")
    print(f"published {url} in {commit[:7]}")


REPO = Path(__file__).resolve().parents[1]

if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit(__doc__)
    main(sys.argv[1])
