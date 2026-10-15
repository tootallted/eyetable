<#
Runs the eyetable API on this PC and makes it reachable from
https://tootallted.github.io/eyetable/source_management/ through a Cloudflare
quick tunnel (the same pattern as acomwebsite's tunnel-watchdog.ps1).

  * `eyetable serve` listens on 127.0.0.1:$Port (never on the network directly).
  * cloudflared opens a quick tunnel to it. Quick tunnel URLs are random and
    change on every restart, so whenever a new one is assigned this script
    writes it into API_BASE in docs/source_management/index.html, commits that
    one file, and pushes; GitHub Pages then serves the page pointing at it.
  * If either process exits, it is restarted.

Anyone on the internet can reach the tunnel URL. Every API call still needs
EYETABLE_API_TOKEN from .env, and the API rate-limits each client and locks
out addresses that keep sending wrong tokens.

Run from anywhere:
    powershell -ExecutionPolicy Bypass -File C:\Users\Ted\eyetable\scripts\run-public.ps1
Stop with Ctrl+C (both child processes are stopped too).
#>

param(
    [int]$Port = 8765,
    [string]$Cloudflared = "C:\Program Files (x86)\cloudflared\cloudflared.exe"
)

$ErrorActionPreference = "Stop"
$RepoDir  = Split-Path -Parent $PSScriptRoot
$PageRel  = "docs/source_management/index.html"
$PageFile = Join-Path $RepoDir $PageRel
$LogDir   = Join-Path $RepoDir "logs"
$Eyetable = Join-Path $RepoDir ".venv\Scripts\eyetable.exe"

New-Item -ItemType Directory -Path $LogDir -Force | Out-Null

function Write-Log($msg) {
    $line = "$(Get-Date -Format o)  $msg"
    Add-Content -Path (Join-Path $LogDir "run-public.log") -Value $line
    Write-Host $line
}

function Start-Api {
    $stamp = Get-Date -Format "yyyyMMdd-HHmmss"
    Write-Log "Starting eyetable serve on 127.0.0.1:$Port"
    # Run from the repo so `.env` (database URL, API token) is picked up.
    Start-Process -FilePath $Eyetable -ArgumentList "serve", "--host", "127.0.0.1", "--port", "$Port" `
        -WorkingDirectory $RepoDir -NoNewWindow -PassThru `
        -RedirectStandardOutput (Join-Path $LogDir "api-$stamp.log") `
        -RedirectStandardError (Join-Path $LogDir "api-$stamp.err.log")
}

function Start-Tunnel {
    $stamp = Get-Date -Format "yyyyMMdd-HHmmss"
    $log = Join-Path $LogDir "tunnel-$stamp.log"
    Write-Log "Starting cloudflared quick tunnel to http://127.0.0.1:$Port"
    $proc = Start-Process -FilePath $Cloudflared -ArgumentList "tunnel", "--url", "http://127.0.0.1:$Port" `
        -NoNewWindow -PassThru -RedirectStandardOutput $log -RedirectStandardError "$log.err"
    $url = $null
    for ($i = 0; $i -lt 30 -and -not $url -and -not $proc.HasExited; $i++) {
        Start-Sleep -Seconds 1
        foreach ($f in @($log, "$log.err")) {
            if (-not $url -and (Test-Path $f)) {
                $m = Select-String -Path $f -Pattern "https://[a-zA-Z0-9\-]+\.trycloudflare\.com" | Select-Object -First 1
                if ($m) { $url = $m.Matches[0].Value }
            }
        }
    }
    if ($url) { Publish-Url $url } else { Write-Log "WARNING: no tunnel URL within 30 s" }
    return $proc
}

function Publish-Url($url) {
    Write-Log "Tunnel URL: $url"
    try {
        $content = [System.IO.File]::ReadAllText($PageFile)
        $updated = $content -replace "var API_BASE = '[^']*';", "var API_BASE = '$url';"
        if ($updated -eq $content) { Write-Log "Page already points at this URL."; return }
        [System.IO.File]::WriteAllText($PageFile, $updated)
        Push-Location $RepoDir
        try {
            git add -- $PageRel | Out-Null
            git commit -m "Point source management page at $url" -- $PageRel | Out-Null
            git push origin main 2>&1 | ForEach-Object { Write-Log "git: $_" }
        } finally { Pop-Location }
        Write-Log "Page updated and pushed; GitHub Pages picks it up in about a minute."
    } catch {
        Write-Log "ERROR publishing the tunnel URL: $($_.Exception.Message)"
    }
}

$api = $null
$tunnel = $null
try {
    while ($true) {
        if (-not $api -or $api.HasExited) {
            if ($api) { Write-Log "eyetable serve exited ($($api.ExitCode)); restarting in 5 s"; Start-Sleep -Seconds 5 }
            $api = Start-Api
            Start-Sleep -Seconds 3
        }
        if (-not $tunnel -or $tunnel.HasExited) {
            if ($tunnel) { Write-Log "cloudflared exited; restarting in 5 s"; Start-Sleep -Seconds 5 }
            $tunnel = Start-Tunnel
        }
        Start-Sleep -Seconds 5
    }
} finally {
    foreach ($p in @($tunnel, $api)) { if ($p -and -not $p.HasExited) { Stop-Process -Id $p.Id -Force } }
    Write-Log "Stopped."
}
