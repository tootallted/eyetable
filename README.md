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
  ingested dataset lands in `eyetable_raw.<source>__<dataset>`. Column types are
  inferred and widened as data changes; new fields become new columns.
  `replace` mode loads into a staging table and swaps it in only when the whole
  pull succeeds; `append` adds rows.

### Columns on every ingested row

However a table is ingested, every row gets these columns ahead of its data:

| column         | type          | value                                                               |
|----------------|---------------|---------------------------------------------------------------------|
| `ET_id`        | `bigint`      | identity: 1 for a new table's first row, then +1 per row            |
| `ET_timestamp` | `timestamptz` | when the row was written                                            |
| `ET_source`    | `varchar`     | `<source> - <schema.table>` for databases, `<source> - <endpoint URL>` for APIs |
| `ET_run_id`    | `bigint`      | the ingestion run in `eyetable.runs` that wrote the row             |

`replace` builds a new table, so `ET_id` starts again at 1; `append` continues
the sequence. API endpoints are recorded without their query string, which can
carry API keys. The names are mixed case, so quote them in Postgres SQL:
`SELECT "ET_id", "ET_source" FROM eyetable_raw.crm__customers`. A source
column with one of these names is stored with an `_src` suffix (`et_id_src`).

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

## Paginated APIs

By default a request is sent once. To pull every page, give it a pagination
strategy with `--paginate "Request key=type[,option=value,...]"` (repeatable,
one per request):

| type | options (defaults) | next page | stops when |
| --- | --- | --- | --- |
| `page` | `param=page`, `start=1`, `size_param`, `size` | page number + 1 | a page is empty, or shorter than `size` |
| `offset` | `offset_param=offset`, `limit_param=limit`, `limit` (required), `start=0` | offset + rows received | a page is shorter than `limit` |
| `cursor` | `cursor_path` (required), `param=cursor` | cursor read from the response | the cursor is missing, empty or repeats |
| `next_link` | `next_path` | the URL at `next_path`, or the `Link: <...>; rel="next"` header | there is no next URL |

```powershell
eyetable source add-postman shop .\shop.postman_collection.json `
    --paginate "Orders/List orders=page,size_param=per_page,size=100" `
    --paginate "Events/List events=cursor,cursor_path=meta.next_cursor,param=after" `
    --paginate "Customers/List customers=next_link,next_path=links.next"
```

* Paging parameters go in the query string, replacing any value the Postman
  request already has; the request's other params, headers and auth are kept.
  Relative next links are resolved against the current URL, and credentials are
  not sent when a next link points at a different host.
* Set `size` / `limit` no higher than the API's maximum page size, since a
  short page is taken to be the last one.
* Every strategy takes `max_pages` (default 1000). If the API still has more
  after that many pages the run fails rather than loading a truncated table;
  raise `max_pages` for that request. Paging also stops if a page repeats the
  previous one, which happens when an API ignores the parameter.
* Pages stream into core storage as they arrive. Discovery fetches only the
  first page, so a paginated dataset's row count shows as `?` until ingested.

## Development

```powershell
.venv\Scripts\python -m pytest
```

Tests run against SQLite and a mocked HTTP transport, so they need no Postgres
server or network.

## Not built yet

* Incremental loads (watermark columns, change tracking)
* Scheduling ingestion runs
* Analysis layer: modeled views on top of `eyetable_raw`, and a UI
* More API auth flows (OAuth2 client credentials / refresh, AWS SigV4), and
  Postman pre-request scripts, which are JavaScript and are not executed
