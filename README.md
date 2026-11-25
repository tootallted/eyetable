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
* **File sources** are CSV files you drop on the source management page (or
  load with `eyetable upload`). Name the source once; each later version of the
  file either replaces the table's contents or is appended to them. Every
  uploaded file is kept in `eyetable.uploads`.
* **Google Sheets**: paste (or drop) a sheet link and every tab becomes its
  own source named `<file name>_<tab name>`. Loading the link again refreshes
  those tabs, by replacing or appending, and picks up new tabs.
* **Core storage** is Postgres. Metadata lives in the `eyetable` schema; each
  ingested dataset lands in `eyetable_raw.<source>__<dataset>`. Column types are
  inferred and widened as data changes; new fields become new columns.
  `replace` mode loads into a staging table and swaps it in only when the whole
  pull succeeds; `append` adds rows.

### Columns on every ingested row

However a table is ingested, every row gets these columns. `ET_id` comes first,
then the source's own columns, then `ET_timestamp`, `ET_source` and `ET_run_id`
(a column that first turns up partway through a load is still put before them).
Tables made before this order take it on their next replace (a **Reload**); an
append adds new columns at the end until then.

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

## Source management page

`eyetable serve` runs the web API and the source management page:

```powershell
# once: add a long random token to .env as EYETABLE_API_TOKEN
python -c "import secrets; print(secrets.token_urlsafe(32))"

eyetable serve                   # http://127.0.0.1:8765/source_management/
```

The page is also published at
<https://tootallted.github.io/eyetable/source_management/> (GitHub Pages serves
the `docs/` folder). That copy is only the page: it runs in your browser and
calls the API you run, by default at `http://localhost:8765`. Click
**Connection** to point it at another URL. The page asks for the API token
and keeps it for the browser session, or longer if you tick *Remember*.

* **Drop a CSV** to see its columns and first rows before anything is sent.
* **New source:** name it (lowercase letters, digits, `_`) and it is created
  with table `eyetable_raw.<name>__data`.
* **Existing source:** drop the next version of the file and choose **Replace
  contents** (the table is rebuilt from the file; `ET_id` restarts at 1) or
  **Append rows** (`ET_id` continues). A file whose name matches a source is
  matched to it automatically.
* Click a source to see its first rows and the history of every file loaded.

CSV handling: the delimiter (`,` `;` tab `|`) is detected from the header line,
UTF-8 (with or without a BOM) and Windows-1252 are read, and a column becomes a
number, boolean, date or timestamp only when every value in it parses as one.
Values with leading zeros (`007`) stay text. The same load from the command
line: `eyetable upload parts parts.csv --create`, then
`eyetable upload parts parts_v2.csv --mode append`.

Every `/api` route except `/api/health` requires the token, and browsers may
only call the API from the origins in `EYETABLE_CORS_ORIGINS` (default
`https://tootallted.github.io`). Uploads are limited to 50 MB
(`EYETABLE_MAX_UPLOAD_MB`). API docs: `/api/docs`.

### A set of files from a server folder

A file source can read a whole set of files instead of one uploaded file. It
reads every file matching a pattern, such as `ledger_*.csv`, from a folder on
the eyetable server, each time it loads (Reload, groups and schedules). New
files dropped in the folder are picked up by the next load.

**From the Sources panel.** A file source's **Last file** has an edit button
(✎). It opens the full path of the uploaded file, for example
`C:\Users\Ted\Desktop\a_20261105.csv`.

A browser never tells a page which folder an uploaded file came from. The
eyetable server, running on the same PC, finds it by looking for a file with
the same name, size and contents in your Desktop, Downloads and Documents
(including OneDrive's). If `EYETABLE_FILE_FOLDERS` is set, it looks in those
folders instead. If the file can't be found, type the folder in front of the
name.

Change the name to a pattern (`a_*.csv`). Each keystroke shows which files
match. Then **Save and load** reads `a_20261105.csv`, `a_20261106.csv` and
`a_20261107.csv` into the source's one table. A plain file name (no `*`) reads
just that file from the folder on each load.

**From Connection settings.** You can also open the file source in **Preview Data → Connection
settings**:

1. Fill in **Folder on the eyetable server**, for example `C:\data\ledgers`. On
   a service, use `\\server\share\…` rather than a mapped drive letter.
2. Change **File name or pattern** from the uploaded name to a pattern. `*`
   stands for any characters and `?` for one, so `ledger_2026-01-02.csv`
   becomes `ledger_*.csv`.
3. **Source name** suggests a name for the set: `ledger_*.csv` gives
   `ledger_aggregated`, whose table is `eyetable_raw.ledger_aggregated__data`.
   Renaming moves the source's table, runs, tags, groups, keys and settings to
   the new name.
4. Use **Show matching files** to check what will load, then **Save and
   reload**.

How the files load:

* Every matching file loads into the one table, read in name order.
* A column found in any file becomes a column. Rows from files without it get
  NULL there. The same heading in different case (`Amount` / `amount`) is one
  column.
* Each row's `ET_source` names the file it came from.
* A file that can't be read fails the load with its name in the error, and the
  table keeps its previous contents.
* Only the folder itself is read, not its subfolders.
* Only CSV, TSV and text files are read.
* Uploading a file into a set source is refused; the file goes in the folder
  instead.
* **Stop reading the folder** goes back to uploaded files.

By default any folder the eyetable service can see may be used.
`EYETABLE_FILE_FOLDERS` in `.env` (folders separated by `;`) limits it to those
folders and the folders under them.

API: `POST /api/files/match` with `{"folder", "pattern"}` lists the matching
files. `PUT /api/sources/<name>/settings` with `{"folder", "pattern", "name"}`
saves them; send `"pattern": ""` to go back to uploads.

### Google Sheets

Paste a Google Sheets link into **Google Sheet link** (or drag the link onto
the page). **Check tabs** lists every tab with the source it becomes, its
size, and whether it is new or was loaded before; **Load** loads them all.

* Names: the file "General Ledger 2026" with tab "Q1 Actuals" becomes source
  `general_ledger_2026_q1_actuals` (table `…__data`), queryable by that name.
  Names are lowercased, punctuation becomes `_`, and they are capped at 63
  characters; a name starting with a digit gets an `s_` prefix.
* Tabs loaded before are replaced or appended to (your choice); new tabs
  become new sources; empty tabs are skipped. A tab whose name is already used
  by another source is skipped rather than overwritten.
* `ET_source` reads `<source> - <file name> / <tab name>`, and each load keeps a
  CSV snapshot of the tab in `eyetable.uploads`.
* **Private sheets**: click **Sign in with Google** (or the button on the
  "this spreadsheet is private" message) and approve read-only access to
  Sheets in Google's popup. Any sheet your Google account can open then loads.
  The page holds a short-lived (one hour) access token in memory and sends it
  with each sheet request; the API reads the sheet through the Sheets API as
  you and never stores the token. **Sign out** revokes it.
* Signed out, the sheet must be shared as **Anyone with the link can view**
  (or published to the web): eyetable downloads it the way *File → Download →
  Microsoft Excel* does. Either way, formulas come in as their values.
* From the command line: `eyetable gsheet <link> [--mode append]` (link-shared
  sheets only).

Google sign-in needs an OAuth client ID, set as `GOOGLE_CLIENT_ID` in
`docs/source_management/index.html` (currently the "eyetable page" client of
Google Cloud project `eyetable`). It comes from a Google Cloud project in
the adventag.com organization with the Google Sheets API enabled, an
**Internal** OAuth consent screen (adventag.com accounts only, no Google
review), and a *Web application* client whose authorized JavaScript origins
are `https://tootallted.github.io` and `http://localhost:8765`.

### Databases from a connection string

Paste a connection string into **Connection string** and **Connect**:

* ODBC, as SQL Server / Azure SQL give it to you:
  `Driver={ODBC Driver 18 for SQL Server};Server=tcp:x.database.windows.net,1433;Database=db;Uid=user;Pwd={secret};Encrypt=yes;`
* JDBC for SQL Server, as the Azure portal gives it (`jdbc:` in front is optional):
  `jdbc:sqlserver://x.database.windows.net:1433;database=db;user=user@x;password={secret};encrypt=true;trustServerCertificate=false;loginTimeout=30;`
* or a URL: `postgresql://user:secret@host:5432/db`, `mysql+pymysql://…`

eyetable lists every table and view the login can see (schema, name, type and
the database's estimated row count), with a filter and a table/view switch.
Tick the ones to bring in; each gets a source name (`<database>_<table>`, or
`<database>_<schema>_<table>` outside `dbo`/`public`), which you can change.
**Load selected** creates one source per pick and loads them in the
background; one failing doesn't stop the rest. Each is an ordinary
single-table source (type *database table*): queryable by name, with
**Reload** (reads the table or view again), tags and groups. Connecting again
shows what's *loaded as* what; picking those again replaces or appends.

* The password (`Pwd=`/`Password=`, or the URL's) is removed from the stored
  string and kept encrypted with `EYETABLE_SECRET_KEY`. A placeholder such as
  `{your_password_here}` is refused before connecting.
* The ODBC driver must be installed on the PC running eyetable. If the string
  names one that isn't (e.g. *ODBC Driver 18*), the nearest installed version
  of it is used (e.g. 17) and the page says so.
* Azure SQL only accepts connections from IP addresses in its firewall rules;
  if the PC running eyetable isn't allowed, the error says which IP to add
  (Azure portal → the SQL server → Networking).

API: `POST /api/db/inspect` (lists, saves nothing), `POST /api/db/load`
(returns a job id).

### API requests from a curl command

Paste a curl command into **curl command** (copied from your browser's dev
tools with *Copy as cURL* in bash or cmd form, from API docs, Postman's code
view, or PowerShell) and **Check request**. eyetable sends it once and shows
the request (secrets masked), how it will page, the first response's columns
and rows, and a suggested source name. **Load every page** saves the source
and loads it in the background; the result appears when it's done.

* **Paging is learned from the command.** A page number (`page=1`, `_page=`,
  `pageNumber=`…) means every page from there; with a page size (`per_page=`,
  `limit=`, `pageSize=`, `$top=`…) it stops at the first short page.
  `offset=` + `limit=` (or `skip=`/`start=`) does the same in rows. A cursor
  parameter (`cursor=`, `after=`, `pageToken=`…) is followed using the cursor
  the first response returns, and a next-page link in the response
  (`nextLink`, `next_link`, `@odata.nextLink`, `links.next`,
  `_links.next.href`…; absolute, relative or `?query` only) or a
  `Link: rel="next"` header is followed too. At most 1,000 pages. Untick the
  paging box to load only the first response.
* **Paging settings, when guessing isn't enough.** Under the paging line:
  * **API page size**: for an API whose page size is fixed (say 1,000 rows),
    a page with fewer rows is the last, even if it still returns a next link.
    If the first page is full but no link was found, the check step says so.
  * **Next-page link field**: where the next page's link is, picked from the
    link fields of the first response or typed (dotted, e.g. `paging.next`).
  * **Rows field**: where the rows are (e.g. `value`, `data.items`).
  Changing one checks the request again. They're saved with the source, so
  **Reload** and group schedules page the same way.
* **Secrets are encrypted.** Authorization and Cookie headers, key/token-like
  headers and query parameters, and `-u` passwords become `{{secret_N}}`
  values stored encrypted with `EYETABLE_SECRET_KEY`; only the masked form is
  ever shown.
* **JSON, CSV or XML.** An XML response becomes a table too: the repeating
  element is the rows, its child elements and attributes are the columns
  (`<Total currency="USD">12</Total>` gives `total_currency` and `total_value`),
  and nested elements become prefixed columns (`supplier_name`). Namespaces are
  ignored. Numbers, dates and true/false become real column types where every
  value fits; codes with leading zeros stay text. Elements that repeat inside a
  row, like line items, stay together as JSON in one column. If eyetable picks
  the wrong element, or a response can have just one, set **Rows field** (e.g.
  `Invoices.Invoice`); a day with nothing to return then loads no rows.
* **Client certificates.** For an API that requires one, open **Client
  certificate** under the command before **Check request**. The fields match
  Postman's: **Host** and **Port** (blank means the command's host), a **PFX
  file** or **CRT** and **KEY files**, and the **Passphrase**, plus an optional
  **CA certificate file** for an API whose own certificate comes from a private
  authority. The certificate is only sent to that host, and is stored encrypted
  with the source, so **Reload** and schedules use it without the file; the
  passphrase isn't kept. Loading the same source from a new command needs the
  file again.
* Understood: `-X`, `-H`, `-d`/`--data*`, `--data-urlencode`, `--json`, `-G`,
  `-u`, `-b`, `-A`, `-e`, `--url`, `--oauth2-bearer`; flags like `-s -L -k
  --compressed` are ignored. Not loadable: `-F` uploads, `-d @file`, `-I`.
* A curl source is one table, `<name>__data`, queryable by its name, with
  **Reload** (calls the API again, every page) and group membership like any
  other source. Loading the same name again replaces or appends.
* A request that isn't a GET is sent again on every load; the check step says so.

**Running one request as a set of statements.** Click a curl source to see its
request taken apart: method, host, path, headers (secrets masked), each query
parameter and the body. Dates, numbers and quoted text in them are clickable;
click one, or **whole value**, to send the request once per value in a series:

* **Dates**: from and to (a date, `today` or `today-30`, so a schedule keeps a
  rolling window) and days per statement. The text keeps its format
  (`2026-11-04`, `11/04/2026`, `20261104`), and every place it appears changes.
  With several days per statement, the last day goes where you say: another date
  in that value, or the later mentions of the same one.
* **Numbers**: from, to, step (`page=1` to `page=40`). If this is the parameter
  eyetable pages with, automatic paging turns off; **Stop at the first statement
  with no rows** ends the set when the pages run out.
* **List of values**: one per line (`status eq 'received'`, then `'paid'`).

Several parts make every combination, up to 5,000 statements. Each statement
pages as usual, and the page shows what will be sent before you save. All the
rows go into the source's one table on every load, including schedules: a run
replaces the table, so a rolling window keeps only those days (use a fixed
start date and `today` to keep building on them). If a statement fails, the run
stops, says which statement, and the table stays as it was. **Pause** spaces
statements out for APIs with rate limits. Loading the source again from a new
curl command removes its set.

API: `POST /api/curl/inspect`, `POST /api/curl` (returns a job id; both take
optional `page_size`, `next_field`, `records_field` and `all_pages`),
`GET /api/jobs/<id>`; `POST /api/sources/<name>/statements/preview`,
`PUT` and `DELETE /api/sources/<name>/statements`, and
`POST /api/sources/<name>/run` to reload in the background.

### Microsoft Entra sign-in (MFA)

A connection string with `Authentication=ActiveDirectoryInteractive` (ODBC or
JDBC), for Azure SQL, Fabric/Synapse or Dataverse's SQL endpoint
(`*.crm.dynamics.com,5558`), signs in with Microsoft Entra, MFA included:

```
Driver={ODBC Driver 18 for SQL Server};Server=tcp:org.crm.dynamics.com,5558;Authentication=ActiveDirectoryInteractive;UID=you@yourcompany.com;Encrypt=yes;
```

The ODBC driver's own interactive sign-in opens a window on the machine it runs
on, which a service can't, so eyetable uses Microsoft's device code sign-in: on
**Connect** the page shows **Sign in with Microsoft**, then a short code; open
https://microsoft.com/devicelogin in any browser, enter it, sign in as the
`UID` and finish MFA, and the page carries on by itself. eyetable keeps the
sign-in (MSAL's token cache, encrypted with `EYETABLE_SECRET_KEY`) and gets a
fresh access token for every connection, so **Reload** and schedules keep
working until your organization's sign-in policies ask for a new sign-in. Then
loads fail with *the Microsoft sign-in has expired*, and **Connection settings**
has **Sign in with Microsoft** to do it again.

It signs in with the public client Microsoft's Azure SDKs use. If your
organization blocks that, or device code sign-in, register your own public
client app and set `EYETABLE_ENTRA_CLIENT_ID` in `.env`. API:
`POST /api/entra/sign-in`, `GET /api/entra/sign-in/<id>`.

### Renaming a source and its table

Selecting a row in **Sources** shows **Name and table** in Preview Data. There
you can change the source's name, its table's name, or both. The table can be
moved to a schema of its own by writing `schema.table` (`finance.ledger`). A
plain name stays in `eyetable_raw`.

A renamed table isn't renamed in place:

* The current table is **archived**, kept as
  `<schema>_archived.<table>_<yyyymmdd>_<hhmmss>`, for example
  `eyetable_raw_archived.ledger__data_20261113_161034`. It stays in the
  database, unchanged.
* The table's natural key, incremental column and hidden setting move to the
  new name.
* The **next full load** creates the new table. **Save and load** runs one
  straight away; an incremental load of the new, empty table loads in full.
* Tables still named after the source (`<source>__data`) follow a new source
  name, and the box shows the new name as you type. A table with a name of its
  own keeps it.
* A source's groups, tags, history and the query window's source alias follow
  a renamed source.

New schemas, archives included, are created when first needed. On Postgres,
the query window's read-only role is given SELECT on them. The query window
lists tables outside `eyetable_raw` as `schema.table`, archives included.

API: `PUT /api/sources/<name>/naming` with `{"name"?, "table"?, "new_table"?}`.
`table` is the table's current name, needed when a source has several.

### Sources grouped by connection

In **Sources**, sources that come from the same place sit under one line:
database tables from the same server and database, tabs of the same Google
Sheet, and API requests to the same host. The line shows how many sources and
tables it holds, their total rows and the latest load, and is closed until you
click it; which ones are open is remembered in the browser. A source loaded or
chosen elsewhere opens its line. A source with nothing to share a line with
stays a row of its own.

A database connection's line lists its **tables** directly, one row each,
whichever source loads them. A table that a source holds alongside others
says so ("loaded by source …"). Each table row has its own **Reload** and
**Incremental** buttons, and clicking it opens that table in Preview Data.

**Hiding a source ("commenting it out").** Every row has a **Hide** button at its left.
A hidden table drops out of the list, and every load skips it: **Reload**,
the load queue, groups, tag runs and schedules. A group's run lists it as
*skipped (hidden)*, which doesn't count as a failure. Its data stays in
`eyetable_raw` and stays queryable.

Hiding works per table, so one table of a database source can be hidden while
the source's other tables keep loading. When something is hidden, **Show hidden
(N)** at the top of Sources switches the list to the hidden tables, each with
**Unhide**. **Back to sources** returns to the list. API:
`PUT /api/tables/<table>/hidden` with `{"hidden": true}` (or `false`).

**Sorting.** Click a column header to sort by it. Click again to reverse the
order, and a third time to go back to the usual order. Rows and Last loaded
sort high-to-low first. Blank values always sort last, and the browser
remembers the sort.

**Filtering.** Each column has a filter box under its header. A source shows
when every non-empty filter matches part of the column's text. Number columns
also take comparisons such as `>1000`, `<=5` or `=0`. While a filter is on,
every connection line with a match opens, and a count above the table shows
how many sources match, with **Clear filters**. Escape empties a box.

The natural key's column table (in Preview Data) sorts and filters the same
way. Sorting its **Key** column brings ticked columns to the top.

### Several tables in one database source

A database source can load any number of its database's tables and views, each
into its own table. Click the source, and under **Preview Data → Tables and
views** use **Choose tables and views…**: it lists everything the login can see,
with a filter, and ticks what the source loads now. **Save and load** loads the
newly ticked ones straight away; unticked ones are taken out of the source and
their loaded copies deleted (the page asks first, and the database itself is
never touched). Every chosen table must be readable before anything changes.

**Reload**, and any group the source is in, loads all of its tables; one failing
doesn't stop the rest. In **Sources**, the **Table** column names up to three
tables, and counts more; click the source to open a row per table with its
rows, columns and when it last loaded. **Rows** is the total. **First rows** has
a picker for which table to preview.

The table a source was made with keeps its name (`eyetable_raw.<source>__data`),
so queries by the source's name still read it; the others are
`eyetable_raw.<source>__<table>`, listed under the source in the query window.
API: `GET /api/sources/<name>/objects`, `PUT /api/sources/<name>/tables` (a job).

### Changing a source's connection

Click a source and open **Preview Data**: **Connection settings** shows how it
connects, never with its passwords, tokens or keys, and lets you change it in
place, keeping the source's name, table, tags, groups and history:

* **Database table**: edit the connection string (server, database, user) and,
  separately, the password, plus the schema and table. A connection string
  without a password keeps the stored one.
* **API (curl)**: a new value for each stored secret, named by where it goes
  (*the Authorization header*, *the api_key query parameter*); for a `Bearer`
  header, paste just the token. Add, replace or remove the client certificate,
  or paste a whole new curl command: the source keeps its paging, rows field,
  certificate and set of statements (a set that no longer fits is removed).
* Google Sheet and file sources show where they come from; there's nothing to
  connect to change.

With **Check the connection before saving** (on by default), nothing is saved
unless the table can be read or the request works. **Save and reload** loads it
straight away. API: `GET` and `PUT /api/sources/<name>/settings`.

### Reloading a source

Every row in **Sources** has a **Reload** button that loads that source's data
again; **Last loaded** shows when data last finished loading.

* File sources replay their stored files, from the most recent *replace*
  onward (then its appends), so the table comes back exactly as built.
* Google Sheet sources fetch their tab again and replace the table with its
  current contents (a private sheet uses your Google sign-in).
* Database and API sources pull every dataset again and replace the tables.

Each reload is an ingestion run, so rows get a new `ET_timestamp` and
`ET_run_id`, and `ET_id` starts again at 1. API: `POST /api/sources/<name>/reload`.

### Big tables: loading in parts

A database table can be too big to read in one query before the database
gives up on it. Dataverse, for one, stops a `SELECT *` after two minutes and any
other query after five.

**Naming the columns.** eyetable always names the columns it reads, so a query
is never `SELECT *`. On Dataverse, which can't describe its tables, the column
names come from an empty `SELECT TOP 0`.

**Reading in parts.** A big table can also be read as a series of ranges of
one column, with each range its own query. All the parts load into the same
table, so the whole table still arrives in one load. How it works:

* Each part aims at about a million values (rows × columns). That is about
  2,100 rows of a 467-column Dataverse table, or up to 50,000 rows of a narrow
  one.
* The first range is narrow, and each next one is sized from how many rows the
  last one held.
* A part that still times out is halved and tried again.
* Rows with no value in the column are read last.
* Each part is read in full before it's written, so a part tried again never
  writes a row twice.
* An incremental load reads only its newer rows, in parts as well.

**Switching on its own.** If a full read in one query times out, eyetable
switches that table to parts by its best column and loads it again. It keeps
loading that table in parts from then on, and the Load queue says so on the
item.

**The best column.** A creation time is best (`createdon`, `created_at`, …),
because rows never move between parts while the table is read. After that come
an ID that only grows, then other date and time columns. A modified time can
move a row changed during the load into a later part, so that row could be
read twice.

**Choosing it yourself.** **Preview Data → Big tables: loading in parts**
shows how a database table loads. You can pick a column to read it in parts
by, or go back to **In one query**.

API: `GET` and `PUT /api/tables/<table>/split` with `{"column": "createdon"}`;
send `null` to read the table in one query.

### Load queue

On the page, **Reload** and **Incremental** don't hold the page up while a
table loads. The load goes into the **Load queue** panel at the bottom of the
page, and runs in the background on the server. You can carry on with the
page meanwhile, and the source's buttons show **Queued** or **Loading…**. The
confirmation above Sources has a link down to the queue.

The panel stays on the page even when nothing is loading. **Clear finished**
only tidies the live list. **Show earlier runs** lists every load recorded in
`eyetable.runs`, newest first, whatever started it, and these survive service
restarts. You can filter them by:

* part of a source or table name;
* a time range (*From* / *To*, or the quick picks for the last 24 hours, 7
  days or 30 days);
* status.

Each run shows when it started, its table, full or incremental, status, rows
and how long it took, with the error if it failed. API: `GET /api/runs` with
`source`, `since`, `until` (ISO times), `status` and `limit`.

Each queue item shows each of its tables with a progress bar. For a full
load it reads like "15,000 of 30,000 rows (50%)", measured against the table's
row count before the load, with rows per second and about how long is left.
An incremental load counts its new or changed rows instead.

How items run:

* Up to two loads run at once. Two loads of the same source never overlap:
  the second waits its turn. The same reload asked for twice is queued once.
* **Retrying a failed API load.** When a full load of an API source fails
  partway, for example on statement 15 of a 16-statement set or page 15 of
  a paged request, the pages that completed are kept. They wait in its staging
  table (`<table>__stg`) while the live table stays as it was. The failed item
  says how many rows were kept and offers **Retry from statement 15 of 16** (or
  **page 15**). The retry is a new queue item: it reads only what's missing,
  adds it to the kept rows, and swaps in the whole table. A retry that fails
  again keeps its pages too, and can be retried in turn. A failure on the very
  first page keeps nothing. A full load of the source started meanwhile
  replaces the kept rows, so the retry then says to run a full load instead.
  Where to carry on from stays on the server; only its description reaches the
  page. API: `POST /api/queue/<id>/retry`.
* **Cancel** stops a waiting load before it starts. A running load stops at its
  next batch and leaves the table as it was: a full load's half-built copy is
  dropped, and an incremental load's rows are taken back out.
* A finished item turns **green** when everything loaded and **red** when
  anything failed. Every error is listed against its table, with **Technical
  details** (the full traceback). **Dismiss** removes one item, and **Clear
  finished** removes all finished items.

**Queries and log.** Each item has **Show queries and log**, which opens a
scrolling record of what the load is doing, as it does it:

* **SQL source**: every statement sent to the source database, with how long
  it took to run and rows where the driver reports them. The values of range
  and incremental queries are shown too.
* **SQL eyetable**: statements on eyetable's own store, such as creating the
  staging table, inserting batches, adding columns, the swap and the key scan.
  A statement repeated back to back (an insert per batch) is one line with a
  count, total time and total rows.
* **HTTP**: an API source's requests, with status, size and time. Query
  values whose names look like keys or tokens are masked.
* **file**: each file a file set reads, with its rows and columns.
* **step**: the engine's decisions, such as full or incremental, the
  incremental boundary, reading in parts, each part's range and rows, and
  switching to parts after a timeout.

The box follows new lines while it's scrolled to the bottom; scroll up to read
and it stays put. A long statement is cut short until you click it. **Show
bookkeeping** adds eyetable's own record-keeping and SQLAlchemy's table
descriptions, which are hidden by default. **Copy the log** copies it as text.
The log keeps up to 5,000 lines per load. API:
`GET /api/queue/<id>/log?after=<n>` returns the lines from line *n* on.

The queue lives in the API process, so it starts empty when the service
restarts; each load is still recorded in `eyetable.runs`.

**Group, tag and scheduled runs** load their sources through the queue too, one
source after another as before. Each source's item is labeled with what
started it: *Group Nightly · Run now*, *Group Nightly · scheduled* or *Tag
#finance · Run now*. Each item has the same progress, log and errors as any
other. Cancelling one in the queue stops that source, and the group goes on to
the next. The group's own run record still lists every source's result.

The page checks the queue every 10 seconds even when nothing is loading, so a
run started by a schedule appears without a refresh. **Run now** opens the
queue straight away.

API: `POST /api/queue` with `{"source", "table"?, "incremental"?}`, `GET /api/queue`,
`POST /api/queue/<id>/cancel`, `DELETE /api/queue/<id>`, and `DELETE /api/queue`
(clear finished).

### Key candidates and natural keys

After every load, eyetable scans the table it loaded. The scan counts each
source column's blank (null) values and distinct values. eyetable's own ET_
columns are left out: `ET_id` is always unique, but it is only a row number
given at load time, so it can't match a row from one load to the next.

* **Primary key candidates.** These are columns with a value in every row and
  no repeats. If there are several, the most ID-like name is suggested first
  (`id`, `…id`, `…_key`, `…code`, `…number`).
* **No primary key column.** When no single column qualifies, the scan says so.
  It then looks for the smallest sets of two, then three, columns whose values
  are unique together. It searches the most varied columns, for up to a minute.
* **Nothing identifies the rows.** If no set of up to three columns works
  either, the scan counts rows that are **exact copies** of another row. Copies
  usually mean the same records were loaded twice. If there are no copies, it
  takes a larger set of columns to identify a row.

**Preview Data** shows this under **Natural key** for the table being
previewed; a database source with several tables has a Table picker above it.
The **Primary key** stat shows the result in short. To choose the natural key:

1. Tick the column, or columns. **Use …** ticks one of the scan's column sets.
2. Click **Save**. eyetable refuses a key that has a blank or a repeated value,
   and says how many rows are affected.

Every later scan checks the saved key again. If a load breaks the key, the key
is kept and flagged with the blank and repeated counts. **Scan again** runs a
scan on demand.

The natural key is stored per table in `eyetable.table_keys`. Saving one
makes the table keep history (see *History tables*): loads then merge by the
key instead of replacing the table.

API: `GET /api/tables/<table>/keys`, `POST /api/tables/<table>/keys/scan` (a
background job), `PUT /api/tables/<table>/key` with `{"columns": [...]}`, and
`DELETE /api/tables/<table>/key`.

### History tables

**Every table with a natural key keeps history.** This applies to every table
with a key, whenever the key was chosen, so there is nothing to turn on. Saving
a key under **Natural key** in Preview Data starts history from the table's
next load. Tables keyed before this was automatic get it when `eyetable serve`
starts, which lists them. A table whose key no longer holds is left out, and
listed too: with blank or repeated key values, its loads would be refused.

A history table is never replaced. Every load, full or incremental, **merges**
into it by the natural key, which can be one column or several. Three columns
record each row's versions:

| Column | Type | Meaning |
|---|---|---|
| `ET_isCurrent` | integer | 1 for the current version of a key, 0 for older versions |
| `ET_fromdate` | datetime | when this version became current (the load's time on the server) |
| `ET_todate` | datetime | when it stopped being current; `9999-12-31 00:00:00.000` while it is current |

What a load does to each row:

* **A key that isn't there yet** is added: ET_isCurrent 1, ET_fromdate now,
  ET_todate 9999-12-31.
* **A key whose other columns changed** gets a new version, added the same
  way. Its current version is closed: ET_isCurrent 0, ET_todate now (the same
  time as the new version's ET_fromdate).
* **An unchanged row** is left as it is.
* **A row that's missing from the load** is left as it is. Nothing is deleted
  or closed because a row is gone from the source.
* A column first seen in a load is added to the table.

The first load after the key is saved makes the table's rows their first
versions. A load whose rows repeat a key, or have no value in it, is refused
and changes nothing. Each load's queue item says how many rows were new,
changed and unchanged.

Key checks and scans look only at the current versions (`ET_isCurrent = 1`).
To see a key as it was at time `t`, query the table with
`WHERE "ET_fromdate" <= t AND "ET_todate" > t`.

**`<table>_current` views.** Every table of loaded data has a view beside it,
in the same schema, named after it with `_current` added. For example,
`eyetable_raw.parts__data` has `eyetable_raw.parts__data_current`. The view
shows only current data, so anyone who doesn't need history can always query
the `_current` name:

* **For a history table:** `SELECT * FROM <table> WHERE "ET_isCurrent" = 1`.
* **For any other table:** `SELECT * FROM <table>`, since every row is current.

eyetable keeps the views up to date by itself:

* **Around each load:** the view is dropped before the load, because Postgres
  won't swap or widen a table that a view depends on. It's made again
  afterwards, with any new columns, even if the load failed.
* **When a table is dropped or archived:** its view goes with it.
* **At startup:** `eyetable serve` makes any view that's missing, and says how
  many there are.

If a view name would be over Postgres's 63-character limit, the table name is
shortened to fit. If a real table already has the view's name, no view is
made.

The query window lists each view under its table. The query role reads views
the same way it reads tables.

**Tables with views built on them.** Your own views can be built on a table
or on its `_current` view, for example a view saved from the query window.
While any exist, Postgres won't let eyetable drop the `_current` view, swap the
table, or change a column's type. So the table's loads change it in place
instead:

* **The `_current` view is kept.** The table's new columns are added at its
  end.
* **A full load empties the table and refills it** in one transaction, rather
  than swapping in a new table. `ET_id` starts again at 1, and if anything
  fails, the table is left as it was.
* **A new column** in the source is added to the table.
* **A column the source no longer has stays in the table** and is loaded as
  NULL, so the views built on it keep working. The load's note in the Load
  queue and its log carry a warning. It names the column and the views to
  check, for example: *warning: colour is no longer in the source, so it was
  kept in parts__data and loaded as NULL (views depend on parts__data:
  eyetable_views.big); check that the view still gives what you expect*.
* **A column whose values now have another type** keeps its type, and the
  values are cast to it. A value that can't be cast fails the load, and the
  table is left as it was.

History tables already merge in place. They give the same warning when a
column is missing from a load, with or without views on them. A table with no
views on it loads as before: swapped, with its columns following the source.

To change the key, clear it first. **Clear the natural key** asks before it
goes ahead, because clearing it stops history: the next full load replaces the
table, and its old versions go with it. History tables made before these
column names were `ET_active`, `ET_fromdt` and `ET_todt`. They are renamed in
place when `eyetable serve` starts, and their rows stay as they are.

API: saving a key (`PUT /api/tables/<table>/key`) turns history on and
clearing it (`DELETE`) turns it off. `PUT /api/tables/<table>/history` with
`{"on": true}` turns it on again for a keyed table; `{"on": false}` is refused.

**Sources indicators.** A narrow column beside each source or table name
shows how it's set up: **PK** for a natural key, **IC** for an incremental
column, **H** for history kept.

### Incremental loads

A full load reads a table's whole source again and replaces it. An incremental
load adds only the rows that are new since the last load. It goes by the
table's **incremental column**: a column whose values only grow as rows are
added or changed.

**Choosing the column.** The key scan suggests candidates under **Preview
Data → Incremental loads**, best first, each with its latest value:

1. Last-modified times (`modifiedon`, `updated_at`, …). These pick up new
   *and* changed rows.
2. Row version numbers.
3. Creation times, and IDs with no repeats. These pick up new rows, but not
   changes to older ones.
4. Other date and time columns.

Text columns count when they hold ISO dates or times, as JSON from APIs usually
does. Pick a candidate, or any other column from **Another column…**, and click
**Use … for incremental loads**. **Stop incremental loads** turns it off.

**What an incremental load does:**

* It reads the largest value of the column already loaded, and loads only rows
  past it.
* A **database** table is asked for just those rows (`WHERE column > value`).
* An **API** or **Google Sheet** source is fetched in full, because it can't be
  asked for less. Only the newer rows are kept.
* The rows are appended and get a new `ET_run_id`.
* With a **natural key**, each new row replaces the older copy of the same row.
  The boundary value is then fetched again (`>=`), so rows that share the last
  timestamp aren't missed. Without a natural key, a changed row is added again
  as a second copy.
* Rows with no value in the column are never picked up.
* If the table is empty, it is loaded in full.
* A failed incremental load takes its rows back out, so the table is left as it
  was.
* File sources have no incremental load: they get new rows by loading a file
  with **Append**.

**API sources run as a set of day statements** can load incrementally by day,
with no incremental column or natural key:

* Every row a statement set loads has an `ET_statement` column holding its
  statement's label, for example `2026-11-15`. It sits after the source's own
  columns, before `ET_timestamp`.
* On a dates part, set **Incremental loads go back to**, for example
  `today-4`. The part needs **1** day per statement, so the incremental
  statements line up with the full load's.
* A **full load** runs every statement (From to To, say `today-15` to
  `today`) and replaces the whole table, as before.
* An **incremental load** runs only the window: `today-4` to `today` is 5
  statements. The rows go into `<table>__incoming` first. Once every
  statement is in, one transaction deletes the table's rows whose
  `ET_statement` is one of those 5 days and adds the new rows. Rows for
  earlier days are left alone. A day that now has fewer rows loses the
  missing ones.
* If any statement fails, nothing in the table changes.
* A table loaded before its rows were tagged has no `ET_statement` column yet,
  so its first incremental load runs in full and says so. Later incremental
  loads use the window.
* A table that keeps history merges the window's rows by its natural key
  instead; nothing is deleted.
* The source shows **IC**, and its **Incremental** button runs the window.
  Groups and schedules set to incremental use it too.

**Where to start one.** Every source row in **Sources** has **Reload** (full)
and **Incremental**. Incremental loads the source's tables that have an
incremental column and leaves the rest alone. Each table row of an opened
database source has its own pair of buttons, as does **Preview Data** (**Full
load** and **Incremental load**). Groups and schedules still run full loads.

API: `PUT /api/tables/<table>/incremental` with `{"column": "..."}`, and
`DELETE /api/tables/<table>/incremental`. Load with
`POST /api/sources/<name>/reload` and `{"incremental": true, "table": "..."}`;
both fields are optional.

### Tags

Give any source tags such as `#base` or `#finance`: in **Sources**, click
**+ tag** (or the ✎ next to existing tags), type them separated by spaces or
commas, and press Enter. Tags are lowercase letters, digits, `_` and `-`
(`#Base`, `base` and ` base ` all mean `#base`). Click a tag on any row to
show only the sources that have it; from there, **Make a group from #base**
creates a group for it in one click. To reload everything with a tag right
now, click the tag under **Run everything with a tag** in the Groups card. No
group is needed, and the tag's last result shows beside it. Clicking the tag
opens a choice under it:

* **Run incremental** is the default. It has the focus, so Enter runs it. Each
  table with an incremental column loads only its new rows, and every other
  table loads in full. The choice says how many of the tag's tables load
  incrementally.
* **Full load** reloads every table in full.
* **Cancel**, or Esc, closes the choice without running anything.

API: `PUT /api/sources/<name>/tags`, `GET /api/tags`,
`POST /api/tags/<tag>/run` (`{"incremental": true}` for an incremental run).

### Destinations

A **destination** is a database a source's data can be loaded into. Every
source loads into exactly one. The built-in destination, **eyetable**, is
eyetable's own Postgres database (`EYETABLE_DATABASE_URL`, data schema
`eyetable_raw`). Everything has loaded there so far, and it's where a source
goes unless it names another.

The **Destinations** card lists each destination with:

* its kind (SQL Server, Postgres)
* its server and database
* the schema its tables go into
* the sources routed to it

**Add destination** takes a name, a connection string and a schema.
Connection strings come in the same forms as in Load data: ODBC for SQL Server,
JDBC `sqlserver://`, or a URL such as `postgresql://…`.

* **Use the SQL Server on this PC** fills in a connection to the local default
  instance with Windows authentication:

  ```
  Driver={ODBC Driver 17 for SQL Server};Server=localhost;Database=eyetable;Trusted_Connection=yes;Encrypt=no;
  ```

  The EyetableApi service runs as your Windows account, so it signs in to SQL
  Server as you.
* **The schema** defaults to `dbo` on SQL Server and `public` on Postgres.
* **A password** in the string is kept encrypted (`EYETABLE_SECRET_KEY`), like
  database sources' passwords, and never shown again. When you edit a
  destination, leave the connection string empty to keep the stored one.

**Test** connects without changing anything. It reports the server's version,
the database, the login, and whether eyetable may create tables in the schema.
If the database doesn't exist yet on that server, the test says so.

You can rename a destination, and its sources follow. A destination that has
sources routed to it can't be removed, and neither can the built-in one.

The **Destination** column in Sources shows where each source's data goes.
Point at it to see that destination's database. Choosing a source's
destination, and loading into SQL Server, are the next step: for now every
source loads into **eyetable**.

API: `GET/POST /api/destinations`, `PUT/DELETE /api/destinations/<id>`,
`POST /api/destinations/test` (`{"id": …}` or `{"connection": …, "schema": …}`).

### Groups and schedules

A **group** is a named set of sources that load together. In the **Groups**
card: **New group**, name it, tick its tags and/or sources, and optionally
give it a schedule. A tag in a group stands for every source that has it
*when the group runs*, so a source tagged `#base` later is picked up by a
`#base` group automatically (each source still runs once, however it got in;
a tag with no sources is reported in the run). Schedules:

* **Daily at set times**, e.g. 6:00 AM (add more times for several runs a day)
* **Every few hours** between two times, e.g. every hour from 8:00 AM to
  5:00 PM (runs at 8:00, 9:00, … 17:00); every 1, 2, 3, 4, 6, 8 or 12 hours

Either kind runs on the weekdays you pick (quick picks: every day, weekdays,
weekends), in your browser's timezone. The editor shows the schedule in words
and its next three run times as you change it. Untick *Run on this schedule*
to pause a group without losing its schedule.

Each group has two buttons to run it now:

* **Full load** reloads every table in the group in full.
* **Incremental** loads incrementally every table that has an incremental
  column (see *Incremental loads*). Every other table loads in full, including
  file sources and tables without one.

The sources load one after another, in the background, through the Load
queue. The group's row shows progress, then the result for each source,
including whether it actually loaded incrementally or in full. One failing
source doesn't stop the rest; the run is marked *Partly failed* and lists what
went wrong.

A schedule chooses which of the two its runs do: *At the scheduled time,
load: Incremental where set up, full for the rest* or *Everything in full*
(the default). The schedule's description says so, for example "Every day at
6:00 AM, incremental where set up".

Scheduled runs happen inside `eyetable serve` (so on this PC, via the
`EyetableApi` service or the `EyetablePublicApi` logon task), which checks every 30 seconds for groups
that are due. If the PC was off at a scheduled time, the group runs once when
eyetable starts again (a *catch-up* run), not once per missed slot. A group
never runs twice at the same time.

Scheduled runs have nobody signed in to Google, so for private sheets eyetable
keeps its own long-lived Google authorization. At the top of the **Groups**
card, **Connect a Google account** opens Google's popup once (read-only
Sheets access); the API swaps the one-time code it returns for a refresh
token, stores that encrypted (`EYETABLE_SECRET_KEY`) in the `eyetable` schema,
which the query window's role can't read, and mints a fresh one-hour access
token from it whenever a sheet must be read with nobody signed in. The card
then shows *read as you@adventag.com*; **Disconnect** revokes the
authorization at Google and deletes it. If Google stops honoring it (revoked
at myaccount.google.com, or the password changed), the card says so and offers
**Reconnect**.

Which Google identity reads a sheet: yours, while you are signed in on the
page; otherwise the connected account; otherwise none (link-shared sheets
only). This needs three settings in `.env`: `GOOGLE_CLIENT_ID`,
`GOOGLE_CLIENT_SECRET` (from the "eyetable page" OAuth client) and
`EYETABLE_SECRET_KEY`; API: `GET/DELETE /api/google`, `POST /api/google/connect`.

#### Emailing each run's result

In a group's editor, **Email results to** takes one address or several,
separated by commas (semicolons and spaces work too). After every run of that
group finishes, whether from **Full load**, **Incremental** or its schedule,
eyetable emails those addresses. The subject says whether the run succeeded,
partly failed or failed, e.g. *eyetable: Finance morning succeeded*. The email
gives:

* the group's name and the result
* start and end times (in the schedule's timezone, otherwise the server's), and
  the duration
* rows processed and tables populated, summed over the group
* how many sources succeeded, failed or were skipped, and what started the run
* a line for each source, with its tables, rows, full or incremental, and any
  error

Leave the box empty for no email. **Send a test email** checks the setup without
running anything. The group's row shows its addresses. Under the last run,
the row says *Emailed to …*, or why no email was sent (in red), for example
if the SMTP server refused the sign-in. The same note is in
`eyetable.group_runs.email`. Runs of a tag on its own send no email; make a
group from the tag to get one.

The eyetable server sends through an SMTP server you set in `.env`, and
needs a restart afterwards:

```
EYETABLE_SMTP_HOST=smtp.office365.com    # or smtp.gmail.com, or your company's relay
EYETABLE_SMTP_PORT=587                   # 465 for SSL, 25 for a relay without sign-in
EYETABLE_SMTP_USER=reports@example.com   # leave out for a relay that takes no sign-in
EYETABLE_SMTP_PASSWORD=...               # an app password where the provider has them
EYETABLE_SMTP_FROM=reports@example.com   # defaults to the user
EYETABLE_SMTP_SECURITY=starttls          # starttls (default), ssl (default on 465) or none
```

* **Microsoft 365:** the mailbox needs *Authenticated SMTP* turned on (Microsoft
  365 admin center › Users › the user › Mail › Manage email apps). If security
  defaults or MFA block basic sign-in, use an SMTP relay or a connector instead.
* **Google Workspace:** use `smtp.gmail.com` with an app password, made at
  myaccount.google.com › Security › App passwords, after turning on 2-Step
  Verification.

`eyetable serve` prints whether group run emails are on when it starts.
Without `EYETABLE_SMTP_HOST` nothing is sent, and the editor warns you about
that.

API: `GET/POST /api/groups`, `PUT/DELETE /api/groups/<id>` (`notify`: the
addresses), `POST /api/groups/<id>/run` (returns a run id),
`GET /api/group-runs/<id>`, `POST /api/schedule/preview`,
`POST /api/notify/test` (`{"to": "a@x.com, b@y.com"}`).

### Query window

The panel docked at the bottom of the page runs your own SQL over everything
that has been loaded. Every source is a table you can query by its name:

```sql
SELECT p.sku, p.qty, o.units
FROM parts p
JOIN orders o ON o.sku = p.sku
WHERE p."ET_timestamp" > now() - interval '1 day';
```

* The sidebar lists each source with its columns: click a name to insert it,
  ▶ to see its first 100 rows. The physical table (`eyetable_raw.parts__data`)
  works too.
* Drag the table list's right edge to make it wider when long names are cut
  off. Double-click the edge to fit the longest name, or focus it and use the
  arrow keys. The width is remembered on this browser.
* **Run** or **Ctrl+Enter** (⌘↩ on a Mac) runs the query; if text is highlighted, only the
  selection runs. Results show up to 100 / 1,000 / 10,000 rows and can be
  downloaded as CSV. Drag the panel's top edge to resize it, or collapse it.
* Mixed-case columns need double quotes in Postgres: `"ET_id"`, `"ET_source"`.

Queries are read-only by construction, because the API is public: only a
single `SELECT` / `WITH` / `VALUES` / `TABLE` statement runs, through a
server-side cursor inside a read-only transaction that is always rolled back,
with a 30-second timeout (`EYETABLE_QUERY_TIMEOUT_S`). On Postgres it runs as
`eyetable_reader` (`EYETABLE_QUERY_DATABASE_URL`, created by
`scripts/setup-postgres.ps1`), a role that can only `SELECT` from
`eyetable_raw` (and the schemas tables were moved to). Of eyetable's own
`eyetable` schema it can read only the run logs, which hold no credentials:

* `eyetable.runs`: one row per table load, with its source, table, mode,
  status, rows, start and finish times, and error.
* `eyetable.group_runs`: one row per group run, with each member's outcome in
  `results`.

Everything else there (source settings and credentials, sign-ins, uploaded
files) stays out of reach. The server grants this when it starts.

```sql
SELECT id, source, target_table, mode, status, rows_written, started_at, finished_at, error
FROM eyetable.runs
ORDER BY started_at DESC
LIMIT 50;
```

#### Saving queries as views and procedures

Below the editor, a bar saves a query in one step: type a name and press
**Save** (or Enter).

* **Which SQL is saved:** with results showing, the query that produced them.
  Otherwise, the text in the editor.
* **View:** one statement that only reads (`SELECT`, `WITH`, `VALUES`, `TABLE`)
  becomes a **view**. This is the default, and the button says **Save as view**.
* **Procedure:** SQL that a view can't hold becomes a **procedure**, and the
  button says **Save as procedure**. That covers `INSERT`, `UPDATE`, `DELETE`,
  `SELECT … INTO`, `CREATE TABLE`, a cursor, or several statements. Such SQL
  can't be run in the read-only window, but it can be saved. Its body is your
  SQL, inside `BEGIN … END` unless it starts with `DECLARE` or `BEGIN`.
* **Source names:** a source's name (`FROM parts`) is saved as its real table
  (`eyetable_raw.parts__data`), because a saved view can't depend on the
  query window's temporary names.
* **Saving over a name** that's already in use asks before replacing it.

Saved objects are listed at the top of the sidebar under **Views &
procedures**, each marked view, procedure or table. A procedure may create
tables, and those are listed there too.

* **Opening one:** click it to put its `CREATE OR REPLACE` statement in the
  editor. The bar then offers:
  * **Deploy <name>** runs your edited statement. A view whose columns
    changed is dropped and made again.
  * **Run procedure** runs a procedure (`CALL`) and says how long it took.
  * **Drop** removes the object. The statement stays in the editor, so Deploy
    can put it back.
* **Querying a view:** the query window reads views by name, for example
  `SELECT * FROM order_parts`.

They're created in their own schema, `eyetable_views` (`EYETABLE_VIEWS_SCHEMA`),
by a separate role, `eyetable_author` (`EYETABLE_AUTHOR_DATABASE_URL`). That
role owns `eyetable_views` and can read the data schemas, and nothing else. It
can't change loaded tables or see eyetable's own tables (sources,
credentials, runs), so a procedure can only write in `eyetable_views`.

Views are made with `security_invoker = true`: they read with the rights of
whoever queries them, so a view never shows more than that role could read
directly. Deploy accepts only `CREATE [OR REPLACE] VIEW|PROCEDURE` for that
schema.

To set it up:

1. Put `EYETABLE_AUTHOR_DATABASE_URL` in `.env`. It's like
   `EYETABLE_DATABASE_URL`, with the role `eyetable_author` and its own password.
2. Run `scripts/setup-postgres.ps1` as an administrator. It creates the role and
   schema, and the grants.
3. Restart eyetable. `eyetable serve` says `views: saving to eyetable_views`,
   or why saving isn't available. Until then the bar says what's missing.

API: `GET/POST /api/objects`, `GET/DELETE /api/objects/<name>`,
`POST /api/objects/deploy`, `POST /api/objects/<name>/call`,
`POST /api/objects/kind`.

### Run webhook

Processes outside eyetable can record their runs in `eyetable.runs`, so they
show up next to eyetable's own loads in **Show earlier runs** and in queries.
Each call to `/hook` on the API adds one row:

```
http://127.0.0.1:8765/hook?key=<EYETABLE_HOOK_KEY>&source=nightly_export&target_table=erp.orders&rows_written=1200&started_at=2026-11-19T06:02:13-08:00
```

| Parameter | Required | Stored as |
|---|---|---|
| `source` | yes | `webhook - <source>` (up to 54 characters given) |
| `target_table` | yes | as given |
| `mode` | no | as given; blank is NULL |
| `status` | no | `succeeded` when left out. Words for success (success, successful, ok, ...) become `succeeded`; words for failure (fail, failure, error, ...) become `failed`; anything else is kept as given (up to 16 characters) |
| `rows_written` | no | a whole number; 0 when left out |
| `started_at` | no | an ISO time; NULL when left out |
| `finished_at` | no | an ISO time; the time of the call when left out |
| `error` | no | as given; blank is NULL |

* `dataset_key` is `webhook` on these rows. A time without an offset is read
  as the server's local time.
* GET with query parameters, or POST with a JSON object or a form. The answer
  is `201` with the row that was recorded. A bad value gets `400` saying what's
  wrong, and nothing is recorded.
* The call needs the webhook key, `EYETABLE_HOOK_KEY` in `.env`, as `key=...`,
  an `X-Hook-Key` header or a Bearer token. The key can only add runs; it
  opens nothing else. The API token works too. A header keeps the key out of
  logs of the URL.
* Address: `http://127.0.0.1:8765/hook` from this PC, or
  `<tunnel URL>/hook` (the `API_BASE` the page uses) from elsewhere. The quick
  tunnel's URL changes when the service restarts.
* **From a web browser**, use the address that never changes:

  ```
  https://tootallted.github.io/eyetable/hook/?source=nightly_export&target_table=erp.orders&rows_written=1200#key=<EYETABLE_HOOK_KEY>
  ```

  GitHub Pages only serves files, so this page (`docs/hook/index.html`) does the
  call from the browser: it reads the API's current address from the published
  source management page, sends the run to `/hook` with the key in the
  `X-Hook-Key` header, and shows the row that was recorded or what went wrong.
  Put the key after `#`: the part after `#` never leaves the browser, so
  GitHub doesn't see it. `?key=` works too. Each time the address is opened,
  one run is recorded. It needs a browser that runs JavaScript; programs such
  as curl or a scheduler call the API's `/hook` directly.

### Signing in with Google or Microsoft

Instead of typing the API token, people can sign in to the page with a Google
account or a Microsoft (Entra ID) work account, by their email address:

* **Connection** shows **Sign in with Google** and **Sign in with Microsoft**
  once the server has an allow list. The API token still works as before.
* The server checks the sign-in itself. For Google it asks Google about the
  access token: it must be for eyetable's OAuth client and have a verified
  email. For Microsoft it checks the ID token's signature against the
  tenant's published keys, its audience (the app registration), its tenant and
  its expiry. The person is the token's sign-in name (`preferred_username`).
* Only emails on `EYETABLE_ALLOWED_USERS` get in. It takes addresses, and
  `@domain` for everyone at a domain:
  `EYETABLE_ALLOWED_USERS=ted@example.com,@example.com`. With no list, nobody
  can sign in this way.
* A sign-in gets a **session** the page uses in place of the token, for
  `EYETABLE_SESSION_HOURS` (12 by default). It is signed with a key made from
  the API token, so it survives restarts. Changing the API token ends every
  session, and so does taking someone off the list. **Sign out** forgets it in
  this browser; a copy of it kept elsewhere works until it expires.
* The header shows who is signed in, and loads they start show their email
  in the load queue.

**Excel and CSV files in OneDrive or SharePoint** load like Google Sheets:
paste the file's sharing link into the Google Sheet box. Every tab becomes its
own source, and a CSV file is one tab. The file is read as the person signed in
with Microsoft (**Microsoft files** in the header, Microsoft Graph,
`Files.Read.All`), so it opens exactly as it would for them. Nothing about the
sign-in is kept. Reload needs that sign-in too. Scheduled runs and group runs
started without it can't read these files, and say so.

**Setting up Microsoft sign-in** (once, in the Azure portal, by you or your
Entra admin):

1. **Microsoft Entra ID → App registrations → New registration.** Name it
   `eyetable`, choose *Accounts in this organizational directory only*.
2. Under *Redirect URI*, choose **Single-page application (SPA)** and enter
   `https://tootallted.github.io/eyetable/source_management/msal-redirect.html`.
   To sign in on a page served by the API itself, also add
   `http://localhost:8765/source_management/msal-redirect.html` under
   **Authentication → Single-page application**.
3. **API permissions → Add a permission → Microsoft Graph → Delegated:**
   `openid`, `profile`, `email`, `User.Read` and `Files.Read.All`. If your
   organization doesn't let people consent to apps themselves, use **Grant
   admin consent**.
4. From **Overview**, copy the **Application (client) ID** and **Directory
   (tenant) ID** into `.env`, then restart the service:

   ```
   EYETABLE_MS_CLIENT_ID=<Application (client) ID>
   EYETABLE_MS_TENANT_ID=<Directory (tenant) ID>
   EYETABLE_ALLOWED_USERS=you@yourcompany.com
   ```

Neither ID is a secret, and the registration needs no client secret.

API: `GET /api/signin` (what the page may offer), `POST /api/session` with
`{"provider": "google" | "microsoft", "token": "..."}` (a Google access token
or a Microsoft ID token), and `GET /api/session` (who you are).
`/api/gsheets`, `/api/gsheets/inspect`, reloads, the queue and group runs take
`microsoft_token` next to `google_token`.

### Public access from the GitHub Pages page

`scripts/run-public.ps1` makes the API on this PC reachable from the published
page, the same way acomwebsite's transact API is:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\run-public.ps1
```

It runs `eyetable serve` on `127.0.0.1:8765` and a Cloudflare quick tunnel to
it. Quick tunnel URLs change on every restart, so each new URL is written into
`API_BASE` in `docs/source_management/index.html`, committed, and pushed; the
live page follows about a minute later. Either process is restarted if it
exits. Logs go to `logs/` (gitignored).

The tunnel URL is public, so the API protects itself: every call needs the
token, each client IP is limited to 240 requests a minute and 120 uploads an
hour, and an IP that sends 10 wrong tokens in 15 minutes is locked out until
the window passes. Behind the tunnel the client IP comes from Cloudflare's
`CF-Connecting-IP` header, which is trusted only on connections from this
machine. The page works only while this PC and the script are running.

To have it start every time you log in to Windows (hidden, restarted if it
fails), register the scheduled task once:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\install-logon-task.ps1
Start-ScheduledTask -TaskName EyetablePublicApi    # start now without logging out
Stop-ScheduledTask -TaskName EyetablePublicApi     # stop the API and tunnel
```

`-Remove` unregisters it. Only one copy of `run-public.ps1` runs at a time; a
second start just exits.

**As a Windows service** instead, so it starts with Windows before anyone
signs in, shows in Task Manager's Services tab and services.msc as
`EyetableApi` ("eyetable API"), and is restarted if it stops: run once in
PowerShell opened as administrator.

```powershell
powershell -ExecutionPolicy Bypass -File scripts\install-service.ps1
```

It builds `scripts/service/EyetableService.cs` with the C# compiler that comes
with Windows, asks for your Windows password (the service runs as you, so it can
publish the tunnel address with your GitHub sign-in and read `.env`), gives your
account the *Log on as a service* right, and installs the service with delayed
automatic start and restart on failure. It disables the logon task, so the two
never run together. Start, stop or restart it from Task Manager > Services,
services.msc, or `Restart-Service EyetableApi`. `-Uninstall` removes it and
turns the logon task back on. If you change your Windows password, run the
installer again.

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

* Incremental loads on a schedule (groups run full loads), and change tracking
* Scheduling ingestion runs
* Analysis layer: modeled views on top of `eyetable_raw`, and a UI
* More API auth flows (OAuth2 client credentials / refresh, AWS SigV4), and
  Postman pre-request scripts, which are JavaScript and are not executed
