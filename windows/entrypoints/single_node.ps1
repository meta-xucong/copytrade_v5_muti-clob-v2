param(
    [ValidateSet("start", "status", "stop")]
    [string]$Action = "status"
)

$ErrorActionPreference = "Stop"

$entryDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$windowsDir = Split-Path -Parent $entryDir
$root = Split-Path -Parent $windowsDir
$singleNodeDir = Join-Path $windowsDir "single_node"
Set-Location $root

$scriptMap = @{
    start = "start_copytrade_live_supervised.ps1"
    status = "status_copytrade_live_supervised.ps1"
    stop = "stop_copytrade_live_supervised.ps1"
}

$target = Join-Path $singleNodeDir $scriptMap[$Action]
if (-not (Test-Path -LiteralPath $target)) {
    throw "Single-node script not found: $target"
}

Write-Host "Single-node copytrade action: $Action" -ForegroundColor Cyan
& $target
if ($null -ne $LASTEXITCODE -and $LASTEXITCODE -ne 0) {
    throw "Single-node action '$Action' failed with exit code $LASTEXITCODE"
}
