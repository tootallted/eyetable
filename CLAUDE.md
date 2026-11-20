# eyetable — notes for Claude and its agents

eyetable captures data from many sources (databases, Postman collections, curl
requests, CSV/Excel files, Google Sheets, OneDrive/SharePoint files) into a core
Postgres database the owner controls, logging every run. README.md is the full
user manual; read the section for the feature you are touching before changing it.

## Layout

- `src/eyetable/` — the Python package (Python 3.11+, SQLAlchemy 2, FastAPI, Typer)
  - `cli.py` — the `eyetable` command (Typer)
  - `api.py` — the web API behind the source management page (`eyetable serve`, port 8765)
  - `engine.py` — discover / ingest orchestration; `store.py` — the core database (`eyetable` meta schema, `eyetable_raw` data schema)
  - `connectors/` — one connector per source kind (`base.py` defines the interface: discover + read)
  - `loadqueue.py`, `schedule.py`, `groups.py`, `incremental.py`, `resume.py` — queued, scheduled, incremental and resumable loads
  - `credentials.py`, `keys.py`, `signin.py`, `entra.py`, `google_auth.py` — secrets and sign-in
- `docs/` — GitHub Pages site. `docs/source_management/index.html` is the whole
  source management page (one large HTML file with inline JS), also served by the API.
- `tests/` — pytest; runs against SQLite and a mocked HTTP transport (no Postgres, no network).
- `scripts/` — Windows service, logon task, Postgres setup, and the public tunnel.

## Commands (PowerShell, from the repo root)

- Run all tests: `.venv\Scripts\python -m pytest -q` (about 3 minutes — run it in the background)
- Run one area: `.venv\Scripts\python -m pytest -q tests/test_curl.py`
- Serve the API and page locally: `.venv\Scripts\eyetable serve` → http://127.0.0.1:8765/source_management/

## Rules

- Every behaviour change comes with a test in `tests/`. Run the targeted tests
  while working and the full suite before committing.
- A user-visible feature also updates README.md, in the same plain-English style.
  Remove items from "Not built yet" when they get built.
- Never read, print or commit `.env`. Source credentials are referenced as
  `${NAME}` and stored secrets are encrypted (`cryptography`); never write a
  secret into the core database, logs, test fixtures or the page.
- Core-storage changes must work on both Postgres (production) and SQLite (tests).
- `replace` loads go through a staging table and swap only on success; keep
  that guarantee in any ingest change.
- Commit messages: one plain-English sentence in the imperative, no prefix
  (e.g. "Add a run webhook so other processes can record runs").

## Watch out

- `scripts/run-public.ps1` may be running on this PC. Whenever the Cloudflare
  tunnel URL changes it rewrites the `var API_BASE = '...';` line in
  `docs/source_management/index.html`, commits that one line to `main`, and
  pushes. Never edit that line by hand, and expect `main` to move under you —
  do feature work on a branch or worktree, not directly on `main`.
- `logs/` holds the API's run logs (ignored by git); useful when debugging the service.
