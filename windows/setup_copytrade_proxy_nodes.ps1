param(
    [string]$Nodes = "",

    [string]$BackupNodes = "",

    [int]$StartPort = 17891,

    [string]$ProfilePath = "",

    [string]$ClashCorePath = "",

    [string]$Mapping = "",

    [string]$AccountsDir = "",

    [string]$AccountFiles = "",

    [string]$ProxyInstances = "",

    [string]$BaseConfig = "copytrade_config.json",

    [ValidateSet("live", "dry")]
    [string]$Mode = "live",

    [int]$Poll = 20,

    [switch]$NoStart,

    [switch]$SkipIpCheck
)

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$root = Split-Path -Parent $scriptDir
Set-Location $root

function Get-ActiveClashProfilePath {
    $candidates = @(
        (Join-Path $env:USERPROFILE ".config\clash\profiles\list.yml")
    )
    foreach ($listPath in $candidates) {
        if (-not (Test-Path -LiteralPath $listPath)) {
            continue
        }
        $profilesDir = Split-Path -Parent $listPath
        $raw = Get-Content -LiteralPath $listPath -Raw -Encoding UTF8
        $indexMatch = [regex]::Match($raw, "(?m)^index:\s*(\d+)\s*$")
        $index = 0
        if ($indexMatch.Success) {
            $index = [int]$indexMatch.Groups[1].Value
        }
        $matches = [regex]::Matches($raw, "(?m)^\s*-\s*time:\s*(.+?)\s*$")
        if ($matches.Count -gt $index) {
            $fileName = $matches[$index].Groups[1].Value.Trim().Trim('"').Trim("'")
            $path = Join-Path $profilesDir $fileName
            if (Test-Path -LiteralPath $path) {
                return $path
            }
        }
    }

    $fallbacks = @(
        (Join-Path $env:USERPROFILE ".config\clash\config.yaml"),
        (Join-Path $env:USERPROFILE ".config\mihomo\config.yaml")
    )
    foreach ($path in $fallbacks) {
        if (Test-Path -LiteralPath $path) {
            return $path
        }
    }
    return ""
}

function Find-ClashCorePath {
    $processes = @(Get-CimInstance Win32_Process | Where-Object {
        $_.Name -in @("clash-win64.exe", "clash.exe", "mihomo.exe", "mihomo-windows-amd64.exe")
    })
    foreach ($proc in $processes) {
        if ($proc.ExecutablePath -and (Test-Path -LiteralPath $proc.ExecutablePath)) {
            return $proc.ExecutablePath
        }
        if ($proc.CommandLine -match '^\s*"([^"]+\.exe)"') {
            $candidate = $Matches[1]
            if (Test-Path -LiteralPath $candidate) {
                return $candidate
            }
        }
        if ($proc.CommandLine -match '^\s*([^\s]+\.exe)') {
            $candidate = $Matches[1]
            if (Test-Path -LiteralPath $candidate) {
                return $candidate
            }
        }
    }

    $common = @(
        "D:\clash\Clash for Windows\resources\static\files\win\x64\clash-win64.exe",
        "$env:LOCALAPPDATA\Programs\Clash for Windows\resources\static\files\win\x64\clash-win64.exe",
        "$env:LOCALAPPDATA\Programs\Clash Verge\resources\mihomo.exe",
        "$env:LOCALAPPDATA\Programs\Clash Verge Rev\resources\mihomo.exe"
    )
    foreach ($candidate in $common) {
        if ($candidate -and (Test-Path -LiteralPath $candidate)) {
            return $candidate
        }
    }
    return ""
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
    return @($names)
}

function Resolve-NodeSelection {
    param(
        [string]$Token,
        [string[]]$AvailableNodes
    )
    $text = ([string]$Token).Trim()
    if (-not $text) {
        return ""
    }
    $indexValue = 0
    if ([int]::TryParse($text, [ref]$indexValue)) {
        if ($indexValue -lt 1 -or $indexValue -gt $AvailableNodes.Count) {
            throw "Node index out of range: $text"
        }
        return $AvailableNodes[$indexValue - 1]
    }

    $exact = @($AvailableNodes | Where-Object { $_ -eq $text })
    if ($exact.Count -eq 1) {
        return $exact[0]
    }

    $contains = @($AvailableNodes | Where-Object { $_ -like "*$text*" })
    if ($contains.Count -eq 1) {
        return $contains[0]
    }
    if ($contains.Count -gt 1) {
        throw "Node selector '$text' matched multiple nodes: $($contains -join '; ')"
    }
    throw "Node selector '$text' did not match any node."
}

function Test-PortListening {
    param([int]$Port)
    $client = New-Object System.Net.Sockets.TcpClient
    try {
        $async = $client.BeginConnect("127.0.0.1", $Port, $null, $null)
        if (-not $async.AsyncWaitHandle.WaitOne(1000, $false)) {
            return $false
        }
        $client.EndConnect($async)
        return $true
    } catch {
        return $false
    } finally {
        $client.Close()
    }
}

function Split-SelectionText {
    param([string]$Text)
    $normalized = ([string]$Text).
        Replace([string][char]0xFF0C, ",").
        Replace([string][char]0xFF1B, ";")
    return @($normalized -split '[,;]' | ForEach-Object { $_.Trim() } | Where-Object { $_ })
}

function Split-BackupGroupText {
    param([string]$Text)
    $normalized = ([string]$Text).
        Replace([string][char]0xFF0C, ",").
        Replace([string][char]0xFF1B, ";").
        Replace("|", "+")
    return @($normalized -split '[,+]' | ForEach-Object { $_.Trim() } | Where-Object { $_ })
}

function Resolve-BackupNodeGroups {
    param(
        [string]$BackupNodesText,
        [int]$ExpectedCount,
        [string[]]$AvailableNodes
    )
    $groups = @()
    if (-not $BackupNodesText) {
        for ($i = 0; $i -lt $ExpectedCount; $i++) {
            $groups += ,@()
        }
        return @($groups)
    }

    $normalized = ([string]$BackupNodesText).Replace([string][char]0xFF1B, ";")
    $rawGroups = @()
    if ($normalized -like "*;*") {
        $rawGroups = @($normalized -split ';')
    } else {
        $rawGroups = @(Split-SelectionText -Text $normalized)
    }
    $rawGroups = @($rawGroups | ForEach-Object { ([string]$_).Trim() })
    if ($rawGroups.Count -ne $ExpectedCount) {
        throw "Backup node group count ($($rawGroups.Count)) must equal selected node count ($ExpectedCount). Use comma for one backup per node, or semicolon between groups."
    }

    foreach ($rawGroup in $rawGroups) {
        $resolved = New-Object System.Collections.Generic.List[string]
        foreach ($token in (Split-BackupGroupText -Text $rawGroup)) {
            $node = Resolve-NodeSelection -Token $token -AvailableNodes $AvailableNodes
            if (-not $resolved.Contains($node)) {
                $resolved.Add($node)
            }
        }
        $groups += ,@($resolved)
    }
    return @($groups)
}

function Resolve-ProjectPath {
    param([string]$PathText)
    if (-not $PathText) {
        return ""
    }
    if ([System.IO.Path]::IsPathRooted($PathText)) {
        return [System.IO.Path]::GetFullPath($PathText)
    }
    return [System.IO.Path]::GetFullPath((Join-Path $root $PathText))
}

function Resolve-AccountFileList {
    param(
        [string]$AccountsDirText,
        [string]$AccountFilesText
    )
    $files = New-Object System.Collections.Generic.List[string]
    if ($AccountFilesText) {
        foreach ($token in (Split-SelectionText -Text $AccountFilesText)) {
            $files.Add((Resolve-ProjectPath $token))
        }
        return @($files)
    }

    $dir = ""
    if ($AccountsDirText) {
        $dir = Resolve-ProjectPath $AccountsDirText
    } else {
        $defaultDir = Join-Path $root "accounts_by_proxy"
        if (Test-Path -LiteralPath $defaultDir) {
            $dir = $defaultDir
        }
    }
    if (-not $dir) {
        return @()
    }
    if (-not (Test-Path -LiteralPath $dir)) {
        throw "Accounts directory not found: $dir"
    }
    $found = Get-ChildItem -LiteralPath $dir -File -Filter "*.json" |
        Where-Object {
            $_.Name -notlike "*.example.json" -and
            $_.Name -notlike "clash_listeners*.json" -and
            $_.Name -notlike "proxy_instances*.json"
        } |
        Sort-Object Name
    foreach ($file in $found) {
        $files.Add($file.FullName)
    }
    return @($files)
}

function Test-AccountFile {
    param([string]$Path)
    if (-not (Test-Path -LiteralPath $Path)) {
        throw "Account file not found: $Path"
    }
    try {
        $payload = Get-Content -LiteralPath $Path -Raw -Encoding UTF8 | ConvertFrom-Json
    } catch {
        throw "Account file is not valid JSON: $Path : $($_.Exception.Message)"
    }
    if (-not ($payload.PSObject.Properties.Name -contains "accounts")) {
        throw "Account file must contain an accounts array: $Path"
    }
    if (@($payload.accounts).Count -eq 0) {
        throw "Account file has no accounts: $Path"
    }
}

if (-not $ProfilePath) {
    $ProfilePath = Get-ActiveClashProfilePath
}
if (-not $ProfilePath -or -not (Test-Path -LiteralPath $ProfilePath)) {
    throw "Could not find an active Clash/Mihomo profile. Pass -ProfilePath <config.yaml>."
}
$ProfilePath = [System.IO.Path]::GetFullPath($ProfilePath)

if (-not $ClashCorePath) {
    $ClashCorePath = Find-ClashCorePath
}
if (-not $ClashCorePath -or -not (Test-Path -LiteralPath $ClashCorePath)) {
    throw "Could not find Clash/Mihomo core. Start Clash first or pass -ClashCorePath <path-to-core.exe>."
}
$ClashCorePath = [System.IO.Path]::GetFullPath($ClashCorePath)

$availableNodes = @(Get-ClashProxyNames -Path $ProfilePath)
if ($availableNodes.Count -eq 0) {
    throw "No proxy nodes found in profile: $ProfilePath"
}

New-Item -ItemType Directory -Force -Path (Join-Path $root "instances") | Out-Null
$nodeListPath = Join-Path $root "instances\clash_nodes.current.txt"
$nodeList = New-Object System.Collections.Generic.List[string]
$nodeList.Add("# Active Clash profile: $ProfilePath")
$nodeList.Add("# Input node indexes or exact names into setup_copytrade_proxy_nodes.ps1")
for ($i = 0; $i -lt $availableNodes.Count; $i++) {
    $nodeList.Add(("{0}. {1}" -f ($i + 1), $availableNodes[$i]))
}
$nodeList | Set-Content -LiteralPath $nodeListPath -Encoding UTF8

Write-Host "Profile: $ProfilePath" -ForegroundColor Cyan
Write-Host "Core: $ClashCorePath" -ForegroundColor Cyan
Write-Host "Available nodes:"
for ($i = 0; $i -lt $availableNodes.Count; $i++) {
    Write-Host ("{0}. {1}" -f ($i + 1), $availableNodes[$i])
}

if (-not $Nodes) {
    $Nodes = Read-Host "Enter node indexes/names, separated by comma. Example: 1,3,5"
}

$tokens = @(Split-SelectionText -Text $Nodes)
if ($tokens.Count -eq 0) {
    throw "No nodes were selected."
}

$selectedNodes = New-Object System.Collections.Generic.List[string]
foreach ($token in $tokens) {
    $node = Resolve-NodeSelection -Token $token -AvailableNodes $availableNodes
    if ($selectedNodes.Contains($node)) {
        throw "Duplicate selected node: $node"
    }
    $selectedNodes.Add($node)
}
$backupNodeGroups = @(Resolve-BackupNodeGroups -BackupNodesText $BackupNodes -ExpectedCount $selectedNodes.Count -AvailableNodes $availableNodes)

if (-not $Mapping) {
    $Mapping = Join-Path $root "instances\clash_listeners.json"
}
if (-not $ProxyInstances) {
    $ProxyInstances = Join-Path $root "instances\proxy_instances.json"
}
$baseConfigPath = Resolve-ProjectPath $BaseConfig
if (-not (Test-Path -LiteralPath $baseConfigPath)) {
    throw "Base copytrade config not found: $baseConfigPath"
}

$listeners = @()
for ($i = 0; $i -lt $selectedNodes.Count; $i++) {
    $port = $StartPort + $i
    if (Test-PortListening -Port $port) {
        Write-Host "Port $port is already listening; the managed core may already be running." -ForegroundColor Yellow
    }
    $listeners += [ordered]@{
        name = ("copytrade-node-{0:D2}" -f ($i + 1))
        listen = "127.0.0.1"
        port = $port
        proxy = $selectedNodes[$i]
        backups = @($backupNodeGroups[$i])
    }
}

$mappingPayload = [ordered]@{
    _comment = "Generated by windows/setup_copytrade_proxy_nodes.ps1. Users choose nodes via -Nodes or interactive input."
    profile_path = $ProfilePath
    clash_core_path = $ClashCorePath
    listeners = $listeners
}
$mappingPayload | ConvertTo-Json -Depth 20 | Set-Content -LiteralPath $Mapping -Encoding UTF8

Write-Host "Wrote mapping: $Mapping" -ForegroundColor Green
foreach ($listener in $listeners) {
    $backupText = ""
    if (@($listener.backups).Count -gt 0) {
        $backupText = " backups=[" + ((@($listener.backups) | ForEach-Object { [string]$_ }) -join "; ") + "]"
    }
    Write-Host ("{0}:{1} -> {2}{3}" -f $listener.listen, $listener.port, $listener.proxy, $backupText)
}

$accountFilesResolved = @(Resolve-AccountFileList -AccountsDirText $AccountsDir -AccountFilesText $AccountFiles)
if ($accountFilesResolved.Count -gt 0) {
    if ($accountFilesResolved.Count -ne $listeners.Count) {
        throw "Account file count ($($accountFilesResolved.Count)) must equal selected node count ($($listeners.Count))."
    }

    $proxyEntries = @()
    for ($i = 0; $i -lt $listeners.Count; $i++) {
        $accountFile = $accountFilesResolved[$i]
        Test-AccountFile -Path $accountFile
        $listener = $listeners[$i]
        $instanceName = [string]$listener.name
        $proxyUrl = "http://127.0.0.1:$($listener.port)"
        $proxyEntries += [ordered]@{
            name = $instanceName
            enabled = $true
            base_config = $baseConfigPath
            accounts_file = $accountFile
            http_proxy = $proxyUrl
            https_proxy = $proxyUrl
            all_proxy = $proxyUrl
            no_proxy = "127.0.0.1,localhost"
            log_dir = (Resolve-ProjectPath ("logs\" + $instanceName))
            prefix = ("persistent_live_" + $instanceName)
            session_name = ("persistent_live_" + $instanceName + "_session.json")
            poll = $Poll
            mode = $Mode
            clash_proxy = [string]$listener.proxy
        }
    }
    $proxyPayload = [ordered]@{
        _comment = "Generated by windows/setup_copytrade_proxy_nodes.ps1. Each entry binds one proxy node to one account JSON."
        instances = $proxyEntries
    }
    $proxyPayload | ConvertTo-Json -Depth 30 | Set-Content -LiteralPath $ProxyInstances -Encoding UTF8
    Write-Host "Wrote copytrade instance mapping: $ProxyInstances" -ForegroundColor Green
    foreach ($entry in $proxyEntries) {
        Write-Host ("{0}: {1} -> {2}" -f $entry.name, $entry.http_proxy, $entry.accounts_file)
    }
} else {
    Write-Host "No account files provided. Skipped copytrade proxy instance mapping." -ForegroundColor Yellow
    Write-Host "Provide -AccountsDir <folder> or -AccountFiles <file1,file2,...> to bind nodes to account JSON files."
}

if (-not $NoStart) {
    & (Join-Path $scriptDir "start_copytrade_clash_cores.ps1") -Mapping $Mapping -ProfilePath $ProfilePath -ClashCorePath $ClashCorePath
    Start-Sleep -Seconds 2
    & (Join-Path $scriptDir "status_copytrade_clash_cores.ps1") -Mapping $Mapping

    if (-not $SkipIpCheck) {
        Write-Host "Exit IP check:" -ForegroundColor Cyan
        foreach ($listener in $listeners) {
            $proxyUrl = "http://127.0.0.1:$($listener.port)"
            try {
                $ip = curl.exe -sS --max-time 20 -x $proxyUrl "https://api.ipify.org?format=json"
                Write-Host ("{0} -> {1}" -f $proxyUrl, $ip)
            } catch {
                Write-Host ("{0} -> IP check failed: {1}" -f $proxyUrl, $_.Exception.Message) -ForegroundColor Yellow
            }
        }
    }
}

Write-Host "Done. Use these proxy URLs in copytrade instances:" -ForegroundColor Green
foreach ($listener in $listeners) {
    Write-Host ("http://127.0.0.1:{0}" -f $listener.port)
}
