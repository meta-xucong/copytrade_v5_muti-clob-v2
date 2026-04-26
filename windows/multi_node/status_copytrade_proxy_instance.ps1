param(
    [Parameter(Mandatory = $true)]
    [string]$Instance,

    [string]$InstancesConfig = ""
)

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$windowsDir = Split-Path -Parent $scriptDir
$root = Split-Path -Parent $windowsDir
Set-Location $root

if (-not $InstancesConfig) {
    $InstancesConfig = Join-Path $root "instances\proxy_instances.json"
}
if (-not (Test-Path -LiteralPath $InstancesConfig)) {
    throw "Instances config not found: $InstancesConfig"
}

$payload = Get-Content -LiteralPath $InstancesConfig -Raw -Encoding UTF8 | ConvertFrom-Json
$selected = $payload.instances | Where-Object { $_.name -eq $Instance } | Select-Object -First 1
if (-not $selected) {
    throw "Instance '$Instance' not found in $InstancesConfig"
}

$prefix = [string]$(if ($selected.prefix) { $selected.prefix } else { "persistent_live_$Instance" })
$sessionName = [string]$(if ($selected.session_name) { $selected.session_name } else { "$prefix`_session.json" })
$sessionPath = Join-Path $root ("logs\" + $sessionName)

if (-not (Test-Path -LiteralPath $sessionPath)) {
    Write-Host "Session not found: $sessionPath" -ForegroundColor Yellow
    exit 0
}

$session = Get-Content -LiteralPath $sessionPath -Raw -Encoding UTF8 | ConvertFrom-Json
Write-Host "Instance: $Instance" -ForegroundColor Cyan
Write-Host "Status: $($session.status)"
Write-Host "Desired state: $($session.desired_state)"
Write-Host "Session: $sessionPath"
Write-Host "Supervisor PID: $($session.supervisor_pid)"
Write-Host "Child PID: $($session.child_pid)"
Write-Host "Restarts: $($session.restart_count)"
Write-Host "Stdout: $($session.stdout)"
Write-Host "Stderr: $($session.stderr)"
Write-Host "Updated: $($session.updated_at)"
