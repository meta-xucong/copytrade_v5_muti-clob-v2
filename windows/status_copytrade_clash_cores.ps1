param(
    [string]$Mapping = ""
)

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$root = Split-Path -Parent $scriptDir
Set-Location $root

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

if (-not $Mapping) {
    $Mapping = Join-Path $root "instances\clash_listeners.json"
}
if (-not (Test-Path -LiteralPath $Mapping)) {
    throw "Mapping file not found: $Mapping"
}

$payload = Get-Content -LiteralPath $Mapping -Raw -Encoding UTF8 | ConvertFrom-Json
$managed = @(Get-CimInstance Win32_Process | Where-Object {
    $_.Name -eq "clash-win64.exe" -and $_.CommandLine -like "*logs\clash_copytrade\cores*"
})

foreach ($listener in @($payload.listeners)) {
    $port = [int]$listener.port
    $name = [string]$listener.name
    $proxy = [string]$listener.proxy
    $backups = @()
    if ($listener.PSObject.Properties.Name -contains "backups") {
        $backups = @($listener.backups | ForEach-Object { ([string]$_).Trim() } | Where-Object { $_ })
    }
    $proc = $managed | Where-Object { $_.CommandLine -like "*$name*" } | Select-Object -First 1
    if (-not $proc) {
        $proc = $managed | Where-Object { $_.CommandLine -like "*$port*" } | Select-Object -First 1
    }
    $listening = Test-PortListening -Port $port
    $pidText = ""
    if ($proc) {
        $pidText = [string]$proc.ProcessId
    }
    $backupText = ""
    if ($backups.Count -gt 0) {
        $backupText = " backups=[" + ($backups -join "; ") + "]"
    }
    Write-Host ("{0} port={1} listening={2} pid={3} proxy={4}{5}" -f $name, $port, $listening, $pidText, $proxy, $backupText)
}
