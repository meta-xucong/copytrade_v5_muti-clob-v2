param(
    [Parameter(Mandatory = $true)]
    [string]$Instance,

    [string]$InstancesConfig = "",

    [int]$WaitSec = 20
)

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$root = Split-Path -Parent $scriptDir
Set-Location $root

if (-not $InstancesConfig) {
    $InstancesConfig = Join-Path $root "instances\proxy_instances.json"
}
if (-not (Test-Path -LiteralPath $InstancesConfig)) {
    throw "Instances config not found: $InstancesConfig"
}

function Get-PythonCommand {
    $py = Get-Command py -ErrorAction SilentlyContinue
    if ($py) {
        return @("py", "-3")
    }
    $python = Get-Command python -ErrorAction SilentlyContinue
    if ($python) {
        return @("python")
    }
    throw "Python launcher not found. Please install Python or add it to PATH."
}

$payload = Get-Content -LiteralPath $InstancesConfig -Raw -Encoding UTF8 | ConvertFrom-Json
$selected = $payload.instances | Where-Object { $_.name -eq $Instance } | Select-Object -First 1
if (-not $selected) {
    throw "Instance '$Instance' not found in $InstancesConfig"
}

$prefix = [string]$(if ($selected.prefix) { $selected.prefix } else { "persistent_live_$Instance" })
$sessionName = [string]$(if ($selected.session_name) { $selected.session_name } else { "$prefix`_session.json" })
$sessionPath = Join-Path $root ("logs\" + $sessionName)

$pycmd = Get-PythonCommand
$args = @(
    "$root\persistent_copytrade_runner.py",
    "stop",
    "--workdir", $root,
    "--session", $sessionPath,
    "--wait-sec", "$WaitSec"
)

$output = & $pycmd[0] $pycmd[1..($pycmd.Length - 1)] $args
if ($LASTEXITCODE -ne 0) {
    throw "Stop failed with exit code $LASTEXITCODE"
}
Write-Host $output
