param(
    [ValidateSet("setup", "check", "start", "status", "stop", "restart")]
    [string]$Action = "status",

    [ValidateSet("dry", "live")]
    [string]$Mode = "dry",

    [string]$Nodes = "",

    [string]$BackupNodes = "",

    [string]$AccountsDir = "accounts_by_proxy",

    [switch]$SkipProxySetup,

    [switch]$SkipExitIpCheck,

    [switch]$AllowDuplicateAccounts
)

$ErrorActionPreference = "Stop"

$entryDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$windowsDir = Split-Path -Parent $entryDir
$root = Split-Path -Parent $windowsDir
$multiNodeDir = Join-Path $windowsDir "multi_node"
Set-Location $root

if (-not $Nodes) {
    $hkPrefix = ([string][char]0x9999) + ([string][char]0x6E2F)
    $Nodes = (@(11, 2, 3, 4, 5) | ForEach-Object { "{0}A{1:D2}" -f $hkPrefix, $_ }) -join ","
}

if (-not $BackupNodes) {
    $hkPrefix = ([string][char]0x9999) + ([string][char]0x6E2F)
    $sharedBackups = (@(6, 7, 8, 9, 10, 11) | ForEach-Object { "{0}A{1:D2}" -f $hkPrefix, $_ }) -join "+"
    $BackupNodes = (@(1..5) | ForEach-Object { $sharedBackups }) -join ";"
}

function Invoke-WindowsScript {
    param(
        [string]$ScriptName,
        [string[]]$ToolArgs = @()
    )
    $scriptPath = Join-Path $multiNodeDir $ScriptName
    if (-not (Test-Path -LiteralPath $scriptPath)) {
        throw "Script not found: $scriptPath"
    }
    $commandArgs = @(
        "-NoProfile",
        "-ExecutionPolicy", "Bypass",
        "-File", $scriptPath
    ) + $ToolArgs
    & powershell @commandArgs
    if ($null -ne $LASTEXITCODE -and $LASTEXITCODE -ne 0) {
        throw "$ScriptName failed with exit code $LASTEXITCODE"
    }
}

function Initialize-AccountGroupsIfMissing {
    $dir = if ([System.IO.Path]::IsPathRooted($AccountsDir)) {
        [System.IO.Path]::GetFullPath($AccountsDir)
    } else {
        [System.IO.Path]::GetFullPath((Join-Path $root $AccountsDir))
    }
    $firstGroup = Join-Path $dir "group-01.json"
    if (-not (Test-Path -LiteralPath $firstGroup)) {
        Write-Host "Account group files are missing; creating default group-01 through group-05 templates." -ForegroundColor Yellow
        Invoke-WindowsScript -ScriptName "init_proxy_account_groups.ps1" -ToolArgs @("-AccountsDir", $dir)
    }
}

function Invoke-ProxySetup {
    Initialize-AccountGroupsIfMissing
    $setupArgs = @(
        "-Nodes", $Nodes,
        "-BackupNodes", $BackupNodes,
        "-AccountsDir", $AccountsDir,
        "-Mode", $Mode
    )
    if ($SkipExitIpCheck) {
        $setupArgs += "-SkipIpCheck"
    }
    Invoke-WindowsScript -ScriptName "setup_copytrade_proxy_nodes.ps1" -ToolArgs $setupArgs
}

function Assert-NoDuplicateLiveAccounts {
    if ($Mode -ne "live" -or $AllowDuplicateAccounts) {
        return
    }
    $instancesPath = Join-Path $root "instances\proxy_instances.json"
    if (-not (Test-Path -LiteralPath $instancesPath)) {
        throw "Proxy instances config not found: $instancesPath. Run setup first."
    }
    $payload = Get-Content -LiteralPath $instancesPath -Raw -Encoding UTF8 | ConvertFrom-Json
    $seen = @{}
    $duplicates = New-Object System.Collections.Generic.List[string]
    foreach ($entry in @($payload.instances)) {
        $accountFile = [string]$entry.accounts_file
        if (-not (Test-Path -LiteralPath $accountFile)) {
            continue
        }
        $accountPayload = Get-Content -LiteralPath $accountFile -Raw -Encoding UTF8 | ConvertFrom-Json
        foreach ($account in @($accountPayload.accounts)) {
            if ($account.PSObject.Properties.Name -contains "enabled" -and -not [bool]$account.enabled) {
                continue
            }
            $identity = ""
            if ($account.PSObject.Properties.Name -contains "my_address") {
                $identity = ([string]$account.my_address).Trim().ToLowerInvariant()
            }
            if (-not $identity -and $account.PSObject.Properties.Name -contains "name") {
                $identity = ([string]$account.name).Trim().ToLowerInvariant()
            }
            if (-not $identity) {
                $identity = [System.IO.Path]::GetFileName($accountFile)
            }
            if ($seen.ContainsKey($identity)) {
                $duplicates.Add("$identity in $($seen[$identity]) and $accountFile")
            } else {
                $seen[$identity] = $accountFile
            }
        }
    }
    if ($duplicates.Count -gt 0) {
        throw "Duplicate live accounts detected. Split accounts across group files before multi-node live, or pass -AllowDuplicateAccounts intentionally. Duplicates: $($duplicates -join '; ')"
    }
}

function Invoke-AllInstances {
    param(
        [ValidateSet("check", "start", "status", "stop")]
        [string]$InstanceAction
    )
    $indexes = 1..5
    if ($InstanceAction -eq "stop") {
        $indexes = 5..1
    }
    foreach ($i in $indexes) {
        $instance = "copytrade-node-{0:D2}" -f $i
        Write-Host ""
        Write-Host "[$InstanceAction] $instance" -ForegroundColor Cyan
        switch ($InstanceAction) {
            "check" {
                $args = @("-Instance", $instance, "-Mode", $Mode, "-CheckOnly")
                if ($SkipExitIpCheck) {
                    $args += "-SkipExitIpCheck"
                }
                Invoke-WindowsScript -ScriptName "start_copytrade_proxy_instance.ps1" -ToolArgs $args
            }
            "start" {
                $args = @("-Instance", $instance, "-Mode", $Mode)
                if ($SkipExitIpCheck) {
                    $args += "-SkipExitIpCheck"
                }
                Invoke-WindowsScript -ScriptName "start_copytrade_proxy_instance.ps1" -ToolArgs $args
            }
            "status" {
                Invoke-WindowsScript -ScriptName "status_copytrade_proxy_instance.ps1" -ToolArgs @("-Instance", $instance)
            }
            "stop" {
                Invoke-WindowsScript -ScriptName "stop_copytrade_proxy_instance.ps1" -ToolArgs @("-Instance", $instance)
            }
        }
    }
}

Write-Host "Multi-node copytrade launcher: action=$Action mode=$Mode" -ForegroundColor Green

switch ($Action) {
    "setup" {
        Invoke-ProxySetup
    }
    "check" {
        if (-not $SkipProxySetup) {
            Invoke-ProxySetup
        }
        Invoke-AllInstances -InstanceAction "check"
    }
    "start" {
        if (-not $SkipProxySetup) {
            Invoke-ProxySetup
        }
        Assert-NoDuplicateLiveAccounts
        Invoke-AllInstances -InstanceAction "start"
    }
    "status" {
        Write-Host ""
        Write-Host "[proxy cores]" -ForegroundColor Cyan
        Invoke-WindowsScript -ScriptName "status_copytrade_clash_cores.ps1"
        Invoke-AllInstances -InstanceAction "status"
    }
    "stop" {
        Invoke-AllInstances -InstanceAction "stop"
    }
    "restart" {
        Invoke-AllInstances -InstanceAction "stop"
        if (-not $SkipProxySetup) {
            Invoke-ProxySetup
        }
        Assert-NoDuplicateLiveAccounts
        Invoke-AllInstances -InstanceAction "start"
    }
}
