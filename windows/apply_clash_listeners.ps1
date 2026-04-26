param(
    [string]$Mapping = "",

    [string]$ProfilePath = "",

    [switch]$CheckOnly
)

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$root = Split-Path -Parent $scriptDir
Set-Location $root

$beginMarker = "# BEGIN COPYTRADE MANAGED LISTENERS"
$endMarker = "# END COPYTRADE MANAGED LISTENERS"

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

function ConvertTo-YamlSingleQuoted {
    param([string]$Text)
    $value = ""
    if ($null -ne $Text) {
        $value = [string]$Text
    }
    return "'" + ($value -replace "'", "''") + "'"
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

function Get-ReservedPorts {
    param([string]$Raw)
    $ports = New-Object System.Collections.Generic.HashSet[int]
    foreach ($key in @("port", "socks-port", "mixed-port", "redir-port", "tproxy-port")) {
        $m = [regex]::Match($Raw, "(?m)^$([regex]::Escape($key)):\s*(\d+)\s*$")
        if ($m.Success) {
            [void]$ports.Add([int]$m.Groups[1].Value)
        }
    }
    return $ports
}

if (-not $Mapping) {
    $Mapping = Join-Path $root "instances\clash_listeners.json"
}
if (-not (Test-Path -LiteralPath $Mapping)) {
    throw "Mapping file not found: $Mapping. Copy instances\clash_listeners.example.json to instances\clash_listeners.json first."
}

$mappingPayload = Get-Content -LiteralPath $Mapping -Raw -Encoding UTF8 | ConvertFrom-Json
if (-not $ProfilePath -and $mappingPayload.profile_path) {
    $ProfilePath = [string]$mappingPayload.profile_path
}
if (-not $ProfilePath) {
    $ProfilePath = Get-ActiveClashProfilePath
}
$ProfilePath = [System.IO.Path]::GetFullPath($ProfilePath)
if (-not (Test-Path -LiteralPath $ProfilePath)) {
    throw "Clash profile not found: $ProfilePath"
}

$listeners = @($mappingPayload.listeners)
if ($listeners.Count -eq 0) {
    throw "No listeners configured in $Mapping"
}

$raw = Get-Content -LiteralPath $ProfilePath -Raw -Encoding UTF8
$availableNodes = @(Get-ClashProxyNames -Path $ProfilePath)
$nodeSet = New-Object System.Collections.Generic.HashSet[string]
foreach ($node in $availableNodes) {
    [void]$nodeSet.Add($node)
}

$seenPorts = New-Object System.Collections.Generic.HashSet[int]
$reservedPorts = Get-ReservedPorts -Raw $raw
foreach ($listener in $listeners) {
    $port = [int]$listener.port
    $proxy = [string]$listener.proxy
    if ($port -le 0 -or $port -gt 65535) {
        throw "Invalid listener port: $port"
    }
    if (-not $seenPorts.Add($port)) {
        throw "Duplicate listener port in mapping: $port"
    }
    if ($reservedPorts.Contains($port)) {
        throw "Listener port $port conflicts with an existing top-level Clash port."
    }
    if (-not $nodeSet.Contains($proxy)) {
        throw "Proxy node not found in active profile: $proxy"
    }
}

$managedPattern = "(?s)\r?\n?$([regex]::Escape($beginMarker)).*?$([regex]::Escape($endMarker))\r?\n?"
$withoutManaged = [regex]::Replace($raw, $managedPattern, "`r`n")
if ($withoutManaged -match "(?m)^listeners:\s*$") {
    throw "Profile already has an unmanaged top-level listeners block. Merge manually before using this script."
}

$block = New-Object System.Collections.Generic.List[string]
$block.Add("")
$block.Add($beginMarker)
$block.Add("listeners:")
foreach ($listener in $listeners) {
    $name = [string]$listener.name
    if (-not $name) {
        $name = "copytrade-" + [string]$listener.port
    }
    $listen = [string]$listener.listen
    if (-not $listen) {
        $listen = "127.0.0.1"
    }
    $block.Add("  - name: " + (ConvertTo-YamlSingleQuoted $name))
    $block.Add("    type: mixed")
    $block.Add("    listen: " + (ConvertTo-YamlSingleQuoted $listen))
    $block.Add("    port: " + [string][int]$listener.port)
    $block.Add("    proxy: " + (ConvertTo-YamlSingleQuoted ([string]$listener.proxy)))
}
$block.Add($endMarker)
$newRaw = $withoutManaged.TrimEnd() + ($block -join "`r`n") + "`r`n"

$tmp = Join-Path $env:TEMP ("copytrade-clash-config-" + [guid]::NewGuid().ToString("N") + ".yaml")
$newRaw | Set-Content -LiteralPath $tmp -Encoding UTF8
$clashExe = "D:\clash\Clash for Windows\resources\static\files\win\x64\clash-win64.exe"
if (Test-Path -LiteralPath $clashExe) {
    $test = & $clashExe -t -f $tmp 2>&1
    if ($LASTEXITCODE -ne 0) {
        Remove-Item -LiteralPath $tmp -Force -ErrorAction SilentlyContinue
        throw "Clash config validation failed:`n$test"
    }
}
Remove-Item -LiteralPath $tmp -Force -ErrorAction SilentlyContinue

Write-Host "Profile: $ProfilePath" -ForegroundColor Cyan
foreach ($listener in $listeners) {
    Write-Host ("{0}:{1} -> {2}" -f ([string]$listener.listen), ([int]$listener.port), ([string]$listener.proxy))
}

if ($CheckOnly) {
    Write-Host "CheckOnly completed. Profile was not modified." -ForegroundColor Yellow
    exit 0
}

$backup = "$ProfilePath.copytrade-listeners.$((Get-Date).ToString('yyyyMMdd_HHmmss')).bak"
Copy-Item -LiteralPath $ProfilePath -Destination $backup -Force
$newRaw | Set-Content -LiteralPath $ProfilePath -Encoding UTF8
Write-Host "Wrote managed listeners block." -ForegroundColor Green
Write-Host "Backup: $backup"
Write-Host "Reload the profile in Clash for Windows, or restart Clash for Windows, before testing the new ports."
