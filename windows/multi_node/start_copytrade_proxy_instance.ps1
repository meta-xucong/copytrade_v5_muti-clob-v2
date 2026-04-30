param(
    [Parameter(Mandatory = $true)]
    [string]$Instance,

    [string]$InstancesConfig = "",

    [ValidateSet("live", "dry", "")]
    [string]$Mode = "",

    [switch]$CheckOnly,

    [switch]$SkipProxyPortCheck,

    [switch]$SkipExitIpCheck
)

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$windowsDir = Split-Path -Parent $scriptDir
$root = Split-Path -Parent $windowsDir
Set-Location $root

if (-not $InstancesConfig) {
    $InstancesConfig = Join-Path $root "instances\proxy_instances.json"
}

function Resolve-ProjectPath {
    param([string]$PathText)
    if (-not $PathText) {
        return $null
    }
    if ([System.IO.Path]::IsPathRooted($PathText)) {
        return [System.IO.Path]::GetFullPath($PathText)
    }
    return [System.IO.Path]::GetFullPath((Join-Path $root $PathText))
}

function ConvertTo-HashtableDeep {
    param([object]$Value)
    if ($null -eq $Value) {
        return $null
    }
    if ($Value -is [System.Array]) {
        $items = @()
        foreach ($item in $Value) {
            $items += ,(ConvertTo-HashtableDeep $item)
        }
        return ,$items
    }
    if ($Value -is [System.Management.Automation.PSCustomObject]) {
        $hash = [ordered]@{}
        foreach ($prop in $Value.PSObject.Properties) {
            $hash[$prop.Name] = ConvertTo-HashtableDeep $prop.Value
        }
        return $hash
    }
    return $Value
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

function Test-ProxyEndpoint {
    param([string]$ProxyUrl)
    if (-not $ProxyUrl) {
        throw "Proxy URL is empty."
    }
    $uri = [Uri]$ProxyUrl
    $hostName = $uri.Host
    $port = $uri.Port
    if ($port -le 0) {
        throw "Proxy URL must include a port: $ProxyUrl"
    }
    $client = New-Object System.Net.Sockets.TcpClient
    try {
        $async = $client.BeginConnect($hostName, $port, $null, $null)
        if (-not $async.AsyncWaitHandle.WaitOne(3000, $false)) {
            throw "Proxy port is not reachable within 3s: $hostName`:$port"
        }
        $client.EndConnect($async)
    } finally {
        $client.Close()
    }
}

function Get-ExitIp {
    param([string]$ProxyUrl)
    $target = "https://api.ipify.org?format=json"
    try {
        $resp = Invoke-RestMethod -Uri $target -Proxy $ProxyUrl -TimeoutSec 12
        return [string]$resp.ip
    } catch {
        throw "Exit IP check failed through $ProxyUrl : $($_.Exception.Message)"
    }
}

if (-not (Test-Path -LiteralPath $InstancesConfig)) {
    throw "Instances config not found: $InstancesConfig. Copy instances\proxy_instances.example.json to instances\proxy_instances.json first."
}

$instancesPayload = Get-Content -LiteralPath $InstancesConfig -Raw -Encoding UTF8 | ConvertFrom-Json
$selected = $instancesPayload.instances | Where-Object { $_.name -eq $Instance } | Select-Object -First 1
if (-not $selected) {
    throw "Instance '$Instance' not found in $InstancesConfig"
}
if ($selected.PSObject.Properties.Name -contains "enabled" -and -not [bool]$selected.enabled) {
    throw "Instance '$Instance' is disabled."
}

$baseConfigPath = Resolve-ProjectPath ($(if ($selected.base_config) { [string]$selected.base_config } else { "copytrade_config.json" }))
$accountsPath = Resolve-ProjectPath ([string]$selected.accounts_file)
if (-not (Test-Path -LiteralPath $baseConfigPath)) {
    throw "Base config not found: $baseConfigPath"
}
if (-not (Test-Path -LiteralPath $accountsPath)) {
    throw "Accounts file not found: $accountsPath"
}

$httpProxy = [string]$selected.http_proxy
$httpsProxy = [string]$(if ($selected.https_proxy) { $selected.https_proxy } else { $httpProxy })
$allProxy = [string]$(if ($selected.all_proxy) { $selected.all_proxy } else { $httpProxy })
$noProxy = [string]$(if ($selected.no_proxy) { $selected.no_proxy } else { "127.0.0.1,localhost" })

if (-not $SkipProxyPortCheck) {
    Test-ProxyEndpoint -ProxyUrl $httpProxy
}
$exitIp = ""
if (-not $SkipExitIpCheck) {
    try {
        $exitIp = Get-ExitIp -ProxyUrl $httpProxy
    } catch {
        Write-Warning $_.Exception.Message
        Write-Warning "Continuing because proxy port check passed. Runtime network health will handle transient proxy/API failures."
    }
}

$runtimeDir = Join-Path $root ("logs\instances\" + $Instance)
New-Item -ItemType Directory -Force -Path $runtimeDir | Out-Null

$baseConfig = Get-Content -LiteralPath $baseConfigPath -Raw -Encoding UTF8 | ConvertFrom-Json
$runtimeConfig = ConvertTo-HashtableDeep $baseConfig
$runtimeConfig["accounts_file"] = [System.IO.Path]::GetFullPath($accountsPath)
$logDir = Resolve-ProjectPath ($(if ($selected.log_dir) { [string]$selected.log_dir } else { "logs\$Instance" }))
New-Item -ItemType Directory -Force -Path $logDir | Out-Null
$runtimeConfig["log_dir"] = $logDir
$runtimeConfig["proxy_instance"] = [ordered]@{
    name = $Instance
    http_proxy = $httpProxy
    https_proxy = $httpsProxy
    all_proxy = $allProxy
    exit_ip_checked_at_launch = $exitIp
}

$runtimeConfigPath = Join-Path $runtimeDir ("copytrade_config.$Instance.json")
$runtimeConfig | ConvertTo-Json -Depth 100 | Set-Content -LiteralPath $runtimeConfigPath -Encoding UTF8

$prefix = [string]$(if ($selected.prefix) { $selected.prefix } else { "persistent_live_$Instance" })
$sessionName = [string]$(if ($selected.session_name) { $selected.session_name } else { "$prefix`_session.json" })
$poll = [int]$(if ($selected.poll) { $selected.poll } else { 20 })
$resolvedMode = [string]$(if ($Mode) { $Mode } elseif ($selected.mode) { $selected.mode } else { "live" })

Write-Host "Instance: $Instance" -ForegroundColor Cyan
Write-Host "Runtime config: $runtimeConfigPath"
Write-Host "Accounts: $accountsPath"
Write-Host "Log dir: $logDir"
Write-Host "HTTP_PROXY: $httpProxy"
if ($exitIp) {
    Write-Host "Exit IP: $exitIp" -ForegroundColor Green
}
if ($CheckOnly) {
    Write-Host "CheckOnly completed. Supervisor was not started." -ForegroundColor Yellow
    exit 0
}

$env:HTTP_PROXY = $httpProxy
$env:HTTPS_PROXY = $httpsProxy
$env:ALL_PROXY = $allProxy
$env:http_proxy = $httpProxy
$env:https_proxy = $httpsProxy
$env:all_proxy = $allProxy
$env:NO_PROXY = $noProxy
$env:no_proxy = $noProxy

$pycmd = Get-PythonCommand
$args = @(
    "$root\persistent_copytrade_runner.py",
    "launch",
    "--workdir", $root,
    "--config", $runtimeConfigPath,
    "--mode", $resolvedMode,
    "--poll", "$poll",
    "--prefix", $prefix,
    "--session-name", $sessionName
)

$output = & $pycmd[0] $pycmd[1..($pycmd.Length - 1)] $args
if ($LASTEXITCODE -ne 0) {
    throw "Launch failed with exit code $LASTEXITCODE"
}

try {
    $json = $output | ConvertFrom-Json
    if ($json.already_running) {
        Write-Host "Proxy instance is already running." -ForegroundColor Yellow
        Write-Host "Session: $($json.session)"
        Write-Host "Supervisor PID: $($json.supervisor_pid)"
        Write-Host "Child PID: $($json.child_pid)"
    } else {
        Write-Host "Proxy instance started." -ForegroundColor Green
        Write-Host "Session: $($json.session)"
        Write-Host "Supervisor PID: $($json.supervisor_pid)"
        Write-Host "Stdout log: $($json.stdout)"
        Write-Host "Stderr log: $($json.stderr)"
        Write-Host "Supervisor log: $($json.supervisor_log)"
    }
} catch {
    Write-Host $output
}
