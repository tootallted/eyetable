<#
Installs the eyetable API as a Windows service, "eyetable API" (EyetableApi),
in place of the logon task. It then shows in Task Manager's Services tab and in
services.msc, starts when Windows starts (before anyone signs in), and is
restarted by Windows if it stops unexpectedly.

The service runs scripts/run-public.ps1, as you: the tunnel address is
published with your GitHub sign-in, and .env is read from the repo. So it asks
for your Windows password once; Windows stores it for the service.

Run in PowerShell as administrator:
    powershell -ExecutionPolicy Bypass -File C:\Users\Ted\eyetable\scripts\install-service.ps1
To remove the service and go back to the logon task:
    powershell -ExecutionPolicy Bypass -File C:\Users\Ted\eyetable\scripts\install-service.ps1 -Uninstall
#>

param([switch]$Uninstall)

$ErrorActionPreference = "Stop"
$ServiceName = "EyetableApi"
$TaskName = "EyetablePublicApi"
$ServiceDir = Join-Path $PSScriptRoot "service"
$Exe = Join-Path $ServiceDir "eyetable-service.exe"
$Csc = Join-Path $env:WINDIR "Microsoft.NET\Framework64\v4.0.30319\csc.exe"

$principal = [Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()
if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    Write-Host "Run this from PowerShell opened with 'Run as administrator'." -ForegroundColor Red
    exit 1
}

function Stop-Runner {
    # The logon task's copy: end it with its API and tunnel, so the port is free.
    Get-CimInstance Win32_Process |
        Where-Object { $_.Name -eq "powershell.exe" -and $_.CommandLine -match "run-public\.ps1" } |
        ForEach-Object { taskkill /T /F /PID $_.ProcessId | Out-Null }
}

function Remove-Service {
    $existing = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
    if ($existing) {
        if ($existing.Status -ne "Stopped") { Stop-Service -Name $ServiceName -Force }
        sc.exe delete $ServiceName | Out-Null
        for ($i = 0; $i -lt 20 -and (Get-Service -Name $ServiceName -ErrorAction SilentlyContinue); $i++) { Start-Sleep -Milliseconds 500 }
    }
}

function Grant-ServiceLogon($account) {
    # Windows only lets an account run a service with the "Log on as a service" right.
    $sid = (New-Object Security.Principal.NTAccount $account).Translate([Security.Principal.SecurityIdentifier]).Value
    $dir = Join-Path $env:TEMP "eyetable-rights"
    New-Item -ItemType Directory -Path $dir -Force | Out-Null
    $inf = Join-Path $dir "rights.inf"
    secedit /export /cfg $inf /areas USER_RIGHTS | Out-Null
    $lines = Get-Content $inf
    $current = $lines | Where-Object { $_ -match "^SeServiceLogonRight\s*=" }
    if ($current -and $current -match [regex]::Escape("*$sid")) { Remove-Item $dir -Recurse -Force; return }
    $lines = if ($current) {
        $lines | ForEach-Object { if ($_ -match "^SeServiceLogonRight\s*=") { "$_,*$sid" } else { $_ } }
    } else {
        $lines | ForEach-Object { $_; if ($_ -eq "[Privilege Rights]") { "SeServiceLogonRight = *$sid" } }
    }
    Set-Content -Path $inf -Value $lines -Encoding Unicode
    secedit /configure /db (Join-Path $dir "rights.sdb") /cfg $inf /areas USER_RIGHTS | Out-Null
    Remove-Item $dir -Recurse -Force
}

if ($Uninstall) {
    Remove-Service
    Enable-ScheduledTask -TaskName $TaskName | Out-Null
    Start-ScheduledTask -TaskName $TaskName
    Write-Host "The eyetable service is removed; the logon task runs the API again." -ForegroundColor Green
    exit 0
}

# 1. Build the service program from scripts\service\EyetableService.cs.
Remove-Service  # a running copy would lock the exe
& $Csc /nologo /target:exe /optimize /reference:System.ServiceProcess.dll "/out:$Exe" (Join-Path $ServiceDir "EyetableService.cs")
if ($LASTEXITCODE -ne 0) { throw "the service program didn't build" }

# 2. Your password, checked before Windows is given it.
Add-Type -AssemblyName System.DirectoryServices.AccountManagement
$account = "$env:COMPUTERNAME\$env:USERNAME"
$machine = New-Object System.DirectoryServices.AccountManagement.PrincipalContext("Machine")
do {
    $credential = Get-Credential -UserName $account -Message "Your Windows password, so the eyetable service can run as you"
    if (-not $credential) { Write-Host "Cancelled; nothing was installed." -ForegroundColor Yellow; exit 1 }
    $ok = $machine.ValidateCredentials($env:USERNAME, $credential.GetNetworkCredential().Password)
    if (-not $ok) { Write-Host "That password didn't work; try again." -ForegroundColor Yellow }
} until ($ok)
Grant-ServiceLogon $account

# 3. The service: starts with Windows (a little after boot, once the network is up),
#    and is restarted if it stops unexpectedly.
New-Service -Name $ServiceName -DisplayName "eyetable API" -BinaryPathName "`"$Exe`"" -Credential $credential `
    -StartupType Automatic -Description "The eyetable API and its Cloudflare tunnel for the source management page (scripts\run-public.ps1)." | Out-Null
sc.exe config $ServiceName start= delayed-auto | Out-Null
sc.exe failure $ServiceName reset= 86400 actions= restart/10000/restart/30000/restart/60000 | Out-Null

# 4. The service replaces the logon task, so they never run at once.
Disable-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue | Out-Null
Stop-Runner
Start-Service -Name $ServiceName
Get-Service -Name $ServiceName | Format-Table -AutoSize Status, Name, DisplayName
Write-Host "Installed. It shows in Task Manager > Services as EyetableApi, and starts with Windows." -ForegroundColor Green
Write-Host "Progress and the tunnel address are in logs\run-public.log as before."
