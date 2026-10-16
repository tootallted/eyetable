<#
Creates eyetable's roles and database in a local Postgres, using .env:
  * POSTGRES_SUPERUSER_PASSWORD   password of the `postgres` superuser
  * EYETABLE_DATABASE_URL         postgresql+psycopg://<user>:<password>@<host>:<port>/<db>
                                  the role eyetable loads data as (owns the database)
  * EYETABLE_QUERY_DATABASE_URL   optional; the same form for the read-only role the
                                  page's query window runs as. It may only SELECT from
                                  the data schema (EYETABLE_DATA_SCHEMA, default
                                  eyetable_raw), including tables created later, and
                                  every session it opens is read-only.
Safe to re-run: existing roles get their passwords reset to match .env, an
existing database is left alone, and grants are re-applied. Then run
`eyetable init`.

    powershell -ExecutionPolicy Bypass -File scripts\setup-postgres.ps1
#>

param([string]$PgBin = "C:\Program Files\PostgreSQL\17\bin")

$ErrorActionPreference = "Stop"
$RepoDir = Split-Path -Parent $PSScriptRoot
$envFile = Join-Path $RepoDir ".env"

function Get-EnvValue($name, [switch]$Optional) {
    $m = Select-String -Path $envFile -Pattern "^$name=(.*)$" | Select-Object -First 1
    if (-not $m) {
        if ($Optional) { return $null }
        throw "$name is not set in $envFile"
    }
    return $m.Matches[0].Groups[1].Value.Trim()
}

function Split-Url($name, $url) {
    if ($url -notmatch '^postgresql(\+\w+)?://([^:@/]+):([^@/]+)@([^:/]+):(\d+)/(\w+)$') {
        throw "$name must look like postgresql+psycopg://user:password@host:port/db"
    }
    $parts = [pscustomobject]@{ User = $Matches[2]; Password = $Matches[3]; Host = $Matches[4]; Port = $Matches[5]; Db = $Matches[6] }
    if ($parts.User -notmatch '^\w+$' -or $parts.Password -match "'") {
        throw "${name}: the role name must be a plain identifier and the password must not contain quotes"
    }
    return $parts
}

$main = Split-Url "EYETABLE_DATABASE_URL" (Get-EnvValue "EYETABLE_DATABASE_URL")
$readerUrl = Get-EnvValue "EYETABLE_QUERY_DATABASE_URL" -Optional
$reader = if ($readerUrl) { Split-Url "EYETABLE_QUERY_DATABASE_URL" $readerUrl } else { $null }
$schema = Get-EnvValue "EYETABLE_DATA_SCHEMA" -Optional
if (-not $schema) { $schema = "eyetable_raw" }
if ($schema -notmatch '^\w+$') { throw "EYETABLE_DATA_SCHEMA must be a plain identifier" }

$env:PGPASSWORD = Get-EnvValue "POSTGRES_SUPERUSER_PASSWORD"
$psql = Join-Path $PgBin "psql.exe"
$common = @("-h", $main.Host, "-p", $main.Port, "-U", "postgres", "-v", "ON_ERROR_STOP=1", "-q", "-t", "-A")

function Invoke-Sql($db, $sql) {
    $out = & $psql @common -d $db -c $sql
    if ($LASTEXITCODE -ne 0) { throw "psql failed: $sql" }
    return $out
}

function Set-Role($role) {
    if ((Invoke-Sql "postgres" "SELECT 1 FROM pg_roles WHERE rolname = '$($role.User)'") -eq "1") {
        Invoke-Sql "postgres" "ALTER ROLE $($role.User) WITH LOGIN PASSWORD '$($role.Password)'" | Out-Null
        Write-Host "role $($role.User) exists; password set from .env"
    } else {
        Invoke-Sql "postgres" "CREATE ROLE $($role.User) WITH LOGIN PASSWORD '$($role.Password)'" | Out-Null
        Write-Host "created role $($role.User)"
    }
}

try {
    Set-Role $main
    if ((Invoke-Sql "postgres" "SELECT 1 FROM pg_database WHERE datname = '$($main.Db)'") -eq "1") {
        Write-Host "database $($main.Db) exists"
    } else {
        Invoke-Sql "postgres" "CREATE DATABASE $($main.Db) OWNER $($main.User)" | Out-Null
        Write-Host "created database $($main.Db) owned by $($main.User)"
    }

    if ($reader) {
        if ($reader.User -eq $main.User) { throw "EYETABLE_QUERY_DATABASE_URL must use a different role than EYETABLE_DATABASE_URL" }
        Set-Role $reader
        $r = $reader.User
        foreach ($sql in @(
            "CREATE SCHEMA IF NOT EXISTS $schema AUTHORIZATION $($main.User)",
            "GRANT CONNECT ON DATABASE $($main.Db) TO $r",
            "GRANT USAGE ON SCHEMA $schema TO $r",
            "GRANT SELECT ON ALL TABLES IN SCHEMA $schema TO $r",
            "ALTER DEFAULT PRIVILEGES FOR ROLE $($main.User) IN SCHEMA $schema GRANT SELECT ON TABLES TO $r",
            "REVOKE CREATE ON SCHEMA public FROM PUBLIC",
            "ALTER ROLE $r SET default_transaction_read_only = on",
            "ALTER ROLE $r SET statement_timeout = '60s'"
        )) { Invoke-Sql $main.Db $sql | Out-Null }
        Write-Host "role $r may only read $schema (now and for tables created later); its sessions are read-only"
    }
} finally {
    Remove-Item Env:\PGPASSWORD -ErrorAction SilentlyContinue
}
