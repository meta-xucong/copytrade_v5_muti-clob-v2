param(
    [string]$ProfilePath = "",

    [string]$Output = ""
)

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$root = Split-Path -Parent $scriptDir
Set-Location $root

function Get-ActiveClashProfilePath {
    $profilesDir = Join-Path $env:USERPROFILE ".config\clash\profiles"
    $listPath = Join-Path $profilesDir "list.yml"
    if (-not (Test-Path -LiteralPath $listPath)) {
        throw "Clash profile list not found: $listPath"
    }
    $raw = Get-Content -LiteralPath $listPath -Raw -Encoding UTF8
    $indexMatch = [regex]::Match($raw, "(?m)^index:\s*(\d+)\s*$")
    $index = 0
    if ($indexMatch.Success) {
        $index = [int]$indexMatch.Groups[1].Value
    }
    $matches = [regex]::Matches($raw, "(?m)^\s*-\s*time:\s*(.+?)\s*$")
    if ($matches.Count -le $index) {
        throw "Cannot resolve active Clash profile from $listPath"
    }
    $fileName = $matches[$index].Groups[1].Value.Trim().Trim('"').Trim("'")
    return Join-Path $profilesDir $fileName
}

function Unquote-Name {
    param([string]$Text)
    $value = ""
    if ($null -ne $Text) {
        $value = [string]$Text
    }
    $value = $value.Trim()
    if (($value.StartsWith('"') -and $value.EndsWith('"')) -or ($value.StartsWith("'") -and $value.EndsWith("'"))) {
        $value = $value.Substring(1, $value.Length - 2)
    }
    return $value
}

function Get-ClashProxyNames {
    param([string]$Path)
    $lines = Get-Content -LiteralPath $Path -Encoding UTF8
    $inside = $false
    $names = New-Object System.Collections.Generic.List[string]
    foreach ($line in $lines) {
        if ($line -match '^proxies:\s*$') {
            $inside = $true
            continue
        }
        if ($inside -and $line -match '^[A-Za-z][A-Za-z -]*:\s*$') {
            break
        }
        if (-not $inside) {
            continue
        }
        if ($line -match '^\s*-\s*\{name:\s*(?<name>[^,}]+)') {
            $names.Add((Unquote-Name $Matches.name))
            continue
        }
        if ($line -match '^\s*-\s*name:\s*(?<name>.+?)\s*$') {
            $names.Add((Unquote-Name $Matches.name))
            continue
        }
    }
    return $names
}

if (-not $ProfilePath) {
    $ProfilePath = Get-ActiveClashProfilePath
}
if (-not $Output) {
    $Output = Join-Path $root "instances\clash_nodes.current.txt"
}

$ProfilePath = [System.IO.Path]::GetFullPath($ProfilePath)
if (-not (Test-Path -LiteralPath $ProfilePath)) {
    throw "Clash profile not found: $ProfilePath"
}

$nodes = Get-ClashProxyNames -Path $ProfilePath
if ($nodes.Count -eq 0) {
    throw "No proxy nodes found in $ProfilePath"
}

$outDir = Split-Path -Parent $Output
if ($outDir) {
    New-Item -ItemType Directory -Force -Path $outDir | Out-Null
}

$lines = New-Object System.Collections.Generic.List[string]
$lines.Add("# Active Clash profile: $ProfilePath")
$lines.Add("# Copy exact names into instances\clash_listeners.json")
$i = 1
foreach ($node in $nodes) {
    $lines.Add(("{0}. {1}" -f $i, $node))
    $i += 1
}
$lines | Set-Content -LiteralPath $Output -Encoding UTF8

Write-Host "Exported $($nodes.Count) node names." -ForegroundColor Green
Write-Host "Profile: $ProfilePath"
Write-Host "Output: $Output"
