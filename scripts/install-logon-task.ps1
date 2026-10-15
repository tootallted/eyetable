<#
Registers a scheduled task that starts scripts\run-public.ps1 (hidden) every
time you log in to Windows, so the source management page's API comes back
after a reboot. Mirrors the AdventagTunnelWatchdog task: your account, normal
(non-admin) rights, no time limit, runs on battery, restarts if it fails.

    powershell -ExecutionPolicy Bypass -File scripts\install-logon-task.ps1
    powershell -ExecutionPolicy Bypass -File scripts\install-logon-task.ps1 -Remove

Start it now without logging out:   Start-ScheduledTask -TaskName EyetablePublicApi
Stop it:                            Stop-ScheduledTask -TaskName EyetablePublicApi
#>

param(
    [string]$TaskName = "EyetablePublicApi",
    [switch]$Remove
)

$ErrorActionPreference = "Stop"

if ($Remove) {
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
    Write-Host "removed scheduled task $TaskName"
    return
}

$script = Join-Path $PSScriptRoot "run-public.ps1"
$repo = Split-Path -Parent $PSScriptRoot
$user = "$env:USERDOMAIN\$env:USERNAME"

$action = New-ScheduledTaskAction -Execute "powershell.exe" `
    -Argument "-NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File `"$script`"" `
    -WorkingDirectory $repo
$trigger = New-ScheduledTaskTrigger -AtLogOn -User $user
$principal = New-ScheduledTaskPrincipal -UserId $user -LogonType Interactive -RunLevel Limited
$settings = New-ScheduledTaskSettingsSet `
    -ExecutionTimeLimit ([TimeSpan]::Zero) `
    -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
    -MultipleInstances IgnoreNew `
    -RestartCount 999 -RestartInterval (New-TimeSpan -Minutes 1) `
    -StartWhenAvailable

Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Principal $principal `
    -Settings $settings -Description "Runs the eyetable API and Cloudflare tunnel for tootallted.github.io/eyetable/source_management (scripts\run-public.ps1)." `
    -Force | Out-Null
Write-Host "registered scheduled task $TaskName (runs at logon for $user)"
