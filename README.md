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

### Sources grouped by connection

In **Sources**, sources that come from the same place sit under one line:
database tables from the same server and database, tabs of the same Google
Sheet, and API requests to the same host. The line shows how many sources and
tables it holds, their total rows and the latest load, and is closed until you
click it; which ones are open is remembered in the browser. A source loaded or
chosen elsewhere opens its line. A source with nothing to share a line with
stays a row of its own.

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

### Tags

Give any source tags such as `#base` or `#finance`: in **Sources**, click
**+ tag** (or the ✎ next to existing tags), type them separated by spaces or
commas, and press Enter. Tags are lowercase letters, digits, `_` and `-`
(`#Base`, `base` and ` base ` all mean `#base`). Click a tag on any row to
show only the sources that have it; from there, **Make a group from #base**
creates a group for it in one click. To reload everything with a tag right
now, click the tag under **Run everything with a tag** in the Groups card; no
group is needed, and its last result shows beside it. API:
`PUT /api/sources/<name>/tags`, `GET /api/tags`, `POST /api/tags/<tag>/run`.

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

**Run now** reloads every source in the group, one after another (each the
same way as its own **Reload** button), in the background: the row shows
progress, then the result per source. One failing source doesn't stop the
rest; the run is marked *Partly failed* and lists what went wrong.

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

API: `GET/POST /api/groups`, `PUT/DELETE /api/groups/<id>`,
`POST /api/groups/<id>/run` (returns a run id), `GET /api/group-runs/<id>`,
`POST /api/schedule/preview`.

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
* **Ctrl+Enter** (⌘↩ on a Mac) runs the query; if text is selected, only the
  selection runs. Results show up to 100 / 1,000 / 10,000 rows and can be
  downloaded as CSV. Drag the panel's top edge to resize it, or collapse it.
* Mixed-case columns need double quotes in Postgres: `"ET_id"`, `"ET_source"`.

Queries are read-only by construction, because the API is public: only a
single `SELECT` / `WITH` / `VALUES` / `TABLE` statement runs, through a
server-side cursor inside a read-only transaction that is always rolled back,
with a 30-second timeout (`EYETABLE_QUERY_TIMEOUT_S`). On Postgres it runs as
`eyetable_reader` (`EYETABLE_QUERY_DATABASE_URL`, created by
`scripts/setup-postgres.ps1`), a role that can only `SELECT` from
`eyetable_raw` and cannot see eyetable's own `eyetable` schema (source configs
and uploaded files).

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

* Incremental loads (watermark columns, change tracking)
* Scheduling ingestion runs
* Analysis layer: modeled views on top of `eyetable_raw`, and a UI
* More API auth flows (OAuth2 client credentials / refresh, AWS SigV4), and
  Postman pre-request scripts, which are JavaScript and are not executed
