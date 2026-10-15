<#
Creates the eyetable role and database in a local Postgres, using .env:
  * POSTGRES_SUPERUSER_PASSWORD  password of the `postgres` superuser
  * EYETABLE_DATABASE_URL        postgresql+psycopg://<user>:<password>@<host>:<port>/<db>
Safe to re-run: an existing role gets its password reset to match .env, and
an existing database is left alone. Then run `eyetable init`.

    powershell -ExecutionPolicy Bypass -File scripts\setup-postgres.ps1
#>

param([string]$PgBin = "C:\Program Files\PostgreSQL\17\bin")

$ErrorActionPreference = "Stop"
$RepoDir = Split-Path -Parent $PSScriptRoot
$envFile = Join-Path $RepoDir ".env"

function Get-EnvValue($name) {
    $m = Select-String -Path $envFile -Pattern "^$name=(.*)$" | Select-Object -First 1
    if (-not $m) { throw "$name is not set in $envFile" }
    return $m.Matches[0].Groups[1].Value.Trim()
}

$url = Get-EnvValue "EYETABLE_DATABASE_URL"
if ($url -notmatch '^postgresql(\+\w+)?://([^:@/]+):([^@/]+)@([^:/]+):(\d+)/(\w+)$') {
    throw "EYETABLE_DATABASE_URL must look like postgresql+psycopg://user:password@host:port/db"
}
$user, $password, $hostName, $port, $db = $Matches[2], $Matches[3], $Matches[4], $Matches[5], $Matches[6]
if ($user -notmatch '^\w+$' -or $password -match "'") { throw "role name must be a plain identifier and the password must not contain quotes" }

$env:PGPASSWORD = Get-EnvValue "POSTGRES_SUPERUSER_PASSWORD"
$psql = Join-Path $PgBin "psql.exe"
$common = @("-h", $hostName, "-p", $port, "-U", "postgres", "-v", "ON_ERROR_STOP=1", "-q", "-t", "-A")

try {
    $roleExists = & $psql @common -d postgres -c "SELECT 1 FROM pg_roles WHERE rolname = '$user'"
    if ($roleExists -eq "1") {
        & $psql @common -d postgres -c "ALTER ROLE $user WITH LOGIN PASSWORD '$password'"
        Write-Host "role $user exists; password set from .env"
    } else {
        & $psql @common -d postgres -c "CREATE ROLE $user WITH LOGIN PASSWORD '$password'"
        Write-Host "created role $user"
    }
    $dbExists = & $psql @common -d postgres -c "SELECT 1 FROM pg_database WHERE datname = '$db'"
    if ($dbExists -eq "1") {
        Write-Host "database $db exists"
    } else {
        & $psql @common -d postgres -c "CREATE DATABASE $db OWNER $user"
        Write-Host "created database $db owned by $user"
    }
    if ($LASTEXITCODE -ne 0) { throw "psql failed" }
} finally {
    Remove-Item Env:\PGPASSWORD -ErrorAction SilentlyContinue
}
