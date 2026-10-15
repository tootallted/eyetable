# eyetable

Capture your data from any number of sources into storage you own, and keep
complete control over how it is ingested and, ultimately, analyzed.

eyetable is a Python engine on top of open-source Postgres. You register
sources, let the engine walk each source's metadata to find every dataset it
exposes (tables, views, API responses — each one columns × rows), and pull the
ones you want into a core Postgres database where every run is logged.

## How it fits together

```
 sources                     engine (Python)                  core storage (Postgres)
 ───────                     ───────────────                  ──────────────────────
 databases  ── credentials ─► connector.discover() ─► catalog ─► eyetable.sources
 (pg, mssql,                  walk metadata schema             eyetable.datasets
  mysql, ...)                                                  eyetable.runs
                              connector.read()   ─► batches ─► eyetable_raw.<source>__<dataset>
 APIs ── Postman collection ─►
         + environment
```

* **Database sources** connect with supplied credentials through SQLAlchemy, so
  anything it supports works (Postgres, SQL Server, MySQL, SQLite, Oracle, ...).
  Discovery reads the database's own metadata: every non-system schema, every
  table and view, their columns and types, and row counts.
* **API sources** are Postman collections. Anything you can run in Postman can
  be registered as-is: export the collection (and environment), and each request
  becomes a dataset. Folders, variables (collection < environment < overrides),
  path variables, headers, bodies, and bearer / basic / API key / OAuth2 token
  auth with inheritance all carry over. Discovery sends each `GET` once to learn
  its columns and row count; other methods are listed but never sent unless you
  opt in with `--allow-method`.
* **Core storage** is Postgres. Metadata lives in the `eyetable` schema; each
  ingested dataset lands in `eyetable_raw.<source>__<dataset>`, with
  `_eyetable_run_id` and `_eyetable_loaded_at` on every row. Column types are
  inferred and widened as data changes; new fields become new columns.
  `replace` mode loads into a staging table and swaps it in only when the whole
  pull succeeds; `append` adds rows.

## Credentials

Source configs are stored in core storage, so secrets should not be. Any string
in a config can reference an environment variable as `${NAME}`; it is resolved
only when the engine connects. Put the values in your shell or a `.env` file.

* `source add-db --password-env CRM_DB_PASSWORD` stores `${CRM_DB_PASSWORD}`.
* Postman environment values marked **secret** are replaced on import with
  `${EYETABLE_<SOURCE>_<KEY>}`; the command tells you which to set.

## Quickstart (Windows)

1. Get Postgres running, either:
   * `winget install PostgreSQL.PostgreSQL.17`, then create a database and user
     named `eyetable`; or
   * `docker compose up -d` with the included `docker-compose.yml`.
2. Install eyetable:
   ```powershell
   python -m venv .venv
   .venv\Scripts\Activate.ps1
   pip install -e ".[dev]"          # add ,mssql for SQL Server sources
   copy .env.example .env           # then edit EYETABLE_DATABASE_URL
   eyetable init
   ```
3. Register sources, discover, ingest:
   ```powershell
   # a database
   $env:CRM_DB_PASSWORD = "..."
   eyetable source add-db crm --driver postgresql+psycopg --host db.local --database crm `
       --username reader --password-env CRM_DB_PASSWORD
   eyetable discover crm            # every table/view: columns x rows
   eyetable ingest crm              # or --dataset public.customers

   # an API, straight from Postman exports
   eyetable source add-postman shop .\shop.postman_collection.json --environment .\dev.postman_environment.json
   eyetable discover shop
   eyetable ingest shop --dataset "Orders/List orders"

   eyetable runs                    # history of every pull
   ```

For SQL Server: `--driver mssql+pyodbc --query "driver=ODBC Driver 18 for SQL Server"`
(install with `pip install -e ".[mssql]"`).

## Where rows come from in an API response

eyetable looks for the list of records in the JSON: a top-level array, or a
`data` / `items` / `results` / `records` / `value` (OData) / `rows` key, or the
only array of objects in the body. Nested objects become `parent__child`
columns; arrays are kept as JSON. When the guess is wrong, point at the rows:
`--records-path "Orders/List orders=payload.orders"`. CSV responses work too.

## Development

```powershell
.venv\Scripts\python -m pytest
```

Tests run against SQLite and a mocked HTTP transport, so they need no Postgres
server or network.

## Not built yet

* Pagination for API sources (page / offset / cursor / next-link)
* Incremental loads (watermark columns, change tracking)
* Scheduling ingestion runs
* Analysis layer: modeled views on top of `eyetable_raw`, and a UI
* More API auth flows (OAuth2 client credentials / refresh, AWS SigV4), and
  Postman pre-request scripts, which are JavaScript and are not executed
