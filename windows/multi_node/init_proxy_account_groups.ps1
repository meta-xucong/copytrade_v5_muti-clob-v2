param(
    [int]$Count = 5,

    [string]$AccountsDir = "",

    [switch]$Force
)

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$windowsDir = Split-Path -Parent $scriptDir
$root = Split-Path -Parent $windowsDir
Set-Location $root

function Resolve-ProjectPath {
    param([string]$PathText)
    if (-not $PathText) {
        return [System.IO.Path]::GetFullPath((Join-Path $root "accounts_by_proxy"))
    }
    if ([System.IO.Path]::IsPathRooted($PathText)) {
        return [System.IO.Path]::GetFullPath($PathText)
    }
    return [System.IO.Path]::GetFullPath((Join-Path $root $PathText))
}

if ($Count -lt 1) {
    throw "Count must be at least 1."
}

$targetDir = Resolve-ProjectPath $AccountsDir
New-Item -ItemType Directory -Force -Path $targetDir | Out-Null

for ($i = 1; $i -le $Count; $i++) {
    $fileName = "group-{0:D2}.json" -f $i
    $path = Join-Path $targetDir $fileName
    if ((Test-Path -LiteralPath $path) -and -not $Force) {
        Write-Host "Kept existing: $path" -ForegroundColor Yellow
        continue
    }

    $payload = [ordered]@{
        _comment = "Proxy account group template. File order binds this account group to the selected proxy node order."
        _warning = "Replace placeholders before live mode. Keep private keys safe and do not commit this file."
        accounts = @(
            [ordered]@{
                name = ("test-group-{0:D2}-account-01" -f $i)
                my_address = "0xYOUR_WALLET_ADDRESS"
                private_key = "0xYOUR_PRIVATE_KEY"
                follow_ratio = 0.1
                enabled = $true
                max_notional_per_token = 10000
                max_notional_total = 10000
            }
        )
    }

    $payload | ConvertTo-Json -Depth 20 | Set-Content -LiteralPath $path -Encoding UTF8
    Write-Host "Wrote: $path" -ForegroundColor Green
}

Write-Host "Done. Account groups are sorted by file name when setup_copytrade_proxy_nodes.ps1 binds them to nodes." -ForegroundColor Green
