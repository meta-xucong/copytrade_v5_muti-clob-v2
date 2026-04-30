param(
    [string]$Mapping = "",

    [string]$ProfilePath = "",

    [string]$ClashCorePath = ""
)

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$windowsDir = Split-Path -Parent $scriptDir
$root = Split-Path -Parent $windowsDir
Set-Location $root

function Get-ActiveClashProfilePath {
    $vergeRoots = @(
        (Join-Path $env:APPDATA "io.github.clash-verge-rev.clash-verge-rev"),
        (Join-Path $env:APPDATA "io.github.clash-verge.clash-verge"),
        (Join-Path $env:APPDATA "clash-verge")
    )
    foreach ($vergeRoot in $vergeRoots) {
        $vergeCandidates = @(
            (Join-Path $vergeRoot "clash-verge.yaml"),
            (Join-Path $vergeRoot "clash-verge-check.yaml"),
            (Join-Path $vergeRoot "config.yaml")
        )
        foreach ($path in $vergeCandidates) {
            if ((Test-Path -LiteralPath $path) -and (@(Get-ClashProxyNames -Path $path)).Count -gt 0) {
                return $path
            }
        }
    }

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

function ConvertTo-SafeName {
    param([string]$Name)
    $safe = [regex]::Replace(([string]$Name).ToLowerInvariant(), "[^a-z0-9_-]+", "-")
    $safe = $safe.Trim("-")
    if (-not $safe) {
        return "listener"
    }
    return $safe
}

function Find-ClashCorePath {
    $preferredProcessNames = @(
        "verge-mihomo.exe",
        "mihomo.exe",
        "mihomo-windows-amd64.exe",
        "clash-win64.exe",
        "clash.exe"
    )
    foreach ($processName in $preferredProcessNames) {
        $processes = @(Get-CimInstance Win32_Process | Where-Object { $_.Name -eq $processName })
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
    }

    $common = @(
        "D:\clash\Clash for Windows\resources\static\files\win\x64\clash-win64.exe",
        "D:\Program Files\Clash Verge\verge-mihomo.exe",
        "D:\Program Files\Clash Verge Rev\verge-mihomo.exe",
        "C:\Program Files\Clash Verge\verge-mihomo.exe",
        "C:\Program Files\Clash Verge Rev\verge-mihomo.exe",
        "$env:LOCALAPPDATA\Programs\Clash for Windows\resources\static\files\win\x64\clash-win64.exe",
        "$env:LOCALAPPDATA\Programs\Clash Verge\verge-mihomo.exe",
        "$env:LOCALAPPDATA\Programs\Clash Verge Rev\verge-mihomo.exe",
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

function Test-PortListening {
    param([int]$Port)
    $client = New-Object System.Net.Sockets.TcpClient
    try {
        $async = $client.BeginConnect("127.0.0.1", $Port, $null, $null)
        if (-not $async.AsyncWaitHandle.WaitOne(800, $false)) {
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

function ConvertTo-YamlSingleQuoted {
    param([string]$Text)
    $value = ""
    if ($null -ne $Text) {
        $value = [string]$Text
    }
    return "'" + ($value -replace "'", "''") + "'"
}

function Get-ListenerBackups {
    param([object]$Listener)
    if (-not ($Listener.PSObject.Properties.Name -contains "backups")) {
        return @()
    }
    return @($Listener.backups | ForEach-Object { ([string]$_).Trim() } | Where-Object { $_ })
}

function Get-ProxyYamlBlock {
    param(
        [string[]]$Lines,
        [string]$ProxyName
    )
    $inside = $false
    for ($i = 0; $i -lt $Lines.Count; $i++) {
        $line = $Lines[$i]
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
            if ((Unquote-Name $Matches.name) -eq $ProxyName) {
                return @($line)
            }
            continue
        }
        if ($line -match '^(?<indent>\s*)-\s*name:\s*(?<name>.+?)\s*$') {
            if ((Unquote-Name $Matches.name) -eq $ProxyName) {
                $block = New-Object System.Collections.Generic.List[string]
                $block.Add($line)
                for ($j = $i + 1; $j -lt $Lines.Count; $j++) {
                    $next = $Lines[$j]
                    if ($next -match '^\s*-\s*(\{|name:)') {
                        break
                    }
                    if ($next -match '^[A-Za-z][A-Za-z -]*:\s*$') {
                        break
                    }
                    $block.Add($next)
                }
                return @($block)
            }
        }
    }
    return @()
}

if (-not $Mapping) {
    $Mapping = Join-Path $root "instances\clash_listeners.json"
}
if (-not (Test-Path -LiteralPath $Mapping)) {
    throw "Mapping file not found: $Mapping"
}

$payload = Get-Content -LiteralPath $Mapping -Raw -Encoding UTF8 | ConvertFrom-Json
if (-not $ProfilePath -and $payload.profile_path) {
    $ProfilePath = [string]$payload.profile_path
}
if (-not $ProfilePath) {
    $ProfilePath = Get-ActiveClashProfilePath
}
$ProfilePath = [System.IO.Path]::GetFullPath($ProfilePath)
if (-not (Test-Path -LiteralPath $ProfilePath)) {
    throw "Clash profile not found: $ProfilePath"
}

$clashExe = $ClashCorePath
if (-not $clashExe) {
    $clashExe = Find-ClashCorePath
}
if (-not (Test-Path -LiteralPath $clashExe)) {
    throw "Clash core not found. Start Clash/Mihomo first or pass -ClashCorePath <path-to-core.exe>."
}

$profileLines = Get-Content -LiteralPath $ProfilePath -Encoding UTF8
$baseDir = Join-Path $root "logs\clash_copytrade\cores"
New-Item -ItemType Directory -Force -Path $baseDir | Out-Null

$index = 0
foreach ($listener in @($payload.listeners)) {
    $index += 1
    $name = [string]$listener.name
    $listen = [string]$listener.listen
    $port = [int]$listener.port
    $proxy = [string]$listener.proxy
    $backups = @(Get-ListenerBackups -Listener $listener)
    if (-not $listen) {
        $listen = "127.0.0.1"
    }
    if ($listen -ne "127.0.0.1") {
        throw "Sidecar Clash cores only support listen=127.0.0.1 for safety: $name"
    }

    $proxyNames = New-Object System.Collections.Generic.List[string]
    $proxyNames.Add($proxy)
    foreach ($backup in $backups) {
        if (-not $proxyNames.Contains($backup)) {
            $proxyNames.Add($backup)
        }
    }

    $proxyBlocks = New-Object System.Collections.Generic.List[string]
    foreach ($proxyName in $proxyNames) {
        $proxyBlock = @(Get-ProxyYamlBlock -Lines $profileLines -ProxyName $proxyName)
        if ($proxyBlock.Count -eq 0) {
            throw "Proxy node not found in profile: $proxyName"
        }
        foreach ($line in $proxyBlock) {
            $proxyBlocks.Add($line)
        }
    }

    $dir = Join-Path $baseDir (ConvertTo-SafeName $name)
    New-Item -ItemType Directory -Force -Path $dir | Out-Null
    $config = Join-Path $dir "config.yaml"
    $controlPort = 17990 + $index
    $yamlLines = @(
        "mixed-port: $port",
        "allow-lan: false",
        "mode: rule",
        "log-level: info",
        "external-controller: 127.0.0.1:$controlPort",
        "ipv6: true",
        "proxies:"
    )
    $yamlLines += @($proxyBlocks)
    $targetProxy = $proxy
    if ($proxyNames.Count -gt 1) {
        $groupName = "$name-fallback"
        $targetProxy = $groupName
        $yamlLines += @(
            "proxy-groups:",
            "  - name: $(ConvertTo-YamlSingleQuoted -Text $groupName)",
            "    type: fallback",
            "    proxies:"
        )
        foreach ($proxyName in $proxyNames) {
            $yamlLines += "      - $(ConvertTo-YamlSingleQuoted -Text $proxyName)"
        }
        $yamlLines += @(
            "    url: 'https://www.gstatic.com/generate_204'",
            "    interval: 60"
        )
    }
    $yamlLines += @(
        "rules:",
        "  - $(ConvertTo-YamlSingleQuoted -Text "MATCH,$targetProxy")"
    )
    $yaml = $yamlLines -join "`r`n"
    ($yaml + "`r`n") | Set-Content -LiteralPath $config -Encoding UTF8

    & $clashExe -t -d $dir -f $config | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "Clash config validation failed for $name"
    }

    $existing = Get-CimInstance Win32_Process |
        Where-Object {
            $_.Name -in @("clash-win64.exe", "clash.exe", "mihomo.exe", "mihomo-windows-amd64.exe", "verge-mihomo.exe") -and
            $_.CommandLine -like "*$config*"
        } |
        Select-Object -First 1
    if ($existing) {
        Write-Host "Already running: $name port=$port pid=$($existing.ProcessId)"
        continue
    }

    if (Test-PortListening -Port $port) {
        throw "Port $port is already listening but not owned by this managed core: $name"
    }

    $stdout = Join-Path $dir "stdout.log"
    $stderr = Join-Path $dir "stderr.log"
    $proc = Start-Process -FilePath $clashExe `
        -ArgumentList @("-d", $dir, "-f", $config) `
        -WindowStyle Hidden `
        -PassThru `
        -RedirectStandardOutput $stdout `
        -RedirectStandardError $stderr
    $backupText = ""
    if ($backups.Count -gt 0) {
        $backupText = " backups=[" + ($backups -join "; ") + "]"
    }
    Write-Host "Started: $name port=$port pid=$($proc.Id) proxy=$proxy$backupText"
}
