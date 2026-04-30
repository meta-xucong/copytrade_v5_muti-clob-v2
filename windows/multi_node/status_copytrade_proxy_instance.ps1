param(
    [Parameter(Mandatory = $true)]
    [string]$Instance,

    [string]$InstancesConfig = ""
)

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$windowsDir = Split-Path -Parent $scriptDir
$root = Split-Path -Parent $windowsDir
Set-Location $root

if (-not $InstancesConfig) {
    $InstancesConfig = Join-Path $root "instances\proxy_instances.json"
}
if (-not (Test-Path -LiteralPath $InstancesConfig)) {
    throw "Instances config not found: $InstancesConfig"
}

$payload = Get-Content -LiteralPath $InstancesConfig -Raw -Encoding UTF8 | ConvertFrom-Json
$selected = $payload.instances | Where-Object { $_.name -eq $Instance } | Select-Object -First 1
if (-not $selected) {
    throw "Instance '$Instance' not found in $InstancesConfig"
}

$prefix = [string]$(if ($selected.prefix) { $selected.prefix } else { "persistent_live_$Instance" })
$sessionName = [string]$(if ($selected.session_name) { $selected.session_name } else { "$prefix`_session.json" })
$sessionPath = Join-Path $root ("logs\" + $sessionName)

if (-not (Test-Path -LiteralPath $sessionPath)) {
    Write-Host "Session not found: $sessionPath" -ForegroundColor Yellow
    exit 0
}

$session = Get-Content -LiteralPath $sessionPath -Raw -Encoding UTF8 | ConvertFrom-Json
Write-Host "Instance: $Instance" -ForegroundColor Cyan
Write-Host "Status: $($session.status)"
Write-Host "Desired state: $($session.desired_state)"
Write-Host "Session: $sessionPath"
Write-Host "Supervisor PID: $($session.supervisor_pid)"
Write-Host "Child PID: $($session.child_pid)"
Write-Host "Restarts: $($session.restart_count)"
Write-Host "Stdout: $($session.stdout)"
Write-Host "Stderr: $($session.stderr)"
Write-Host "Updated: $($session.updated_at)"

function Convert-ToStatePart {
    param([string]$Value)
    $text = ([string]$Value).Trim().ToLowerInvariant()
    if ($text.StartsWith("0x") -and $text.Length -ge 10) {
        return "{0}_{1}" -f $text.Substring(2, 4), $text.Substring($text.Length - 4)
    }
    $clean = [regex]::Replace($text, "[^a-zA-Z0-9_-]+", "_")
    if ($clean.Length -gt 16) {
        $clean = $clean.Substring(0, 16)
    }
    if (-not $clean) {
        return "unknown"
    }
    return $clean
}

function Write-RuntimeHealth {
    $runtimeDir = Join-Path $root ("logs\instances\" + $Instance)
    $runtimeConfig = Join-Path $runtimeDir ("copytrade_config.$Instance.json")
    $baseConfig = if (Test-Path -LiteralPath $runtimeConfig) {
        $runtimeConfig
    } else {
        [string]$selected.base_config
    }
    $accountsFile = [string]$selected.accounts_file
    if (-not (Test-Path -LiteralPath $baseConfig) -or -not (Test-Path -LiteralPath $accountsFile)) {
        return
    }
    try {
        $cfg = Get-Content -LiteralPath $baseConfig -Raw -Encoding UTF8 | ConvertFrom-Json
        $accountsPayload = Get-Content -LiteralPath $accountsFile -Raw -Encoding UTF8 | ConvertFrom-Json
    } catch {
        Write-Host "Runtime health: unavailable ($($_.Exception.Message))" -ForegroundColor Yellow
        return
    }

    $targetAddress = [string]$cfg.target_address
    if (-not $targetAddress -and $cfg.target_addresses) {
        $firstTarget = @($cfg.target_addresses) | Select-Object -First 1
        if ($firstTarget -is [string]) {
            $targetAddress = [string]$firstTarget
        } elseif ($firstTarget -and $firstTarget.PSObject.Properties.Name -contains "address") {
            $targetAddress = [string]$firstTarget.address
        }
    }
    if (-not $targetAddress) {
        return
    }
    $targetPart = Convert-ToStatePart -Value $targetAddress
    $stateDir = Join-Path $runtimeDir "logs\state"
    if (-not (Test-Path -LiteralPath $stateDir)) {
        $stateDir = Join-Path $root "logs\state"
    }

    Write-Host "Runtime health:" -ForegroundColor Cyan
    foreach ($account in @($accountsPayload.accounts)) {
        if ($account.PSObject.Properties.Name -contains "enabled" -and -not [bool]$account.enabled) {
            continue
        }
        $addr = [string]$account.my_address
        if (-not $addr) {
            continue
        }
        $myPart = Convert-ToStatePart -Value $addr
        $statePath = Join-Path $stateDir ("state_{0}_{1}.json" -f $targetPart, $myPart)
        $label = if ($account.PSObject.Properties.Name -contains "name" -and $account.name) {
            [string]$account.name
        } else {
            $addr
        }
        if (-not (Test-Path -LiteralPath $statePath)) {
            Write-Host ("  {0}: state not found ({1})" -f $label, $statePath) -ForegroundColor Yellow
            continue
        }
        try {
            $statePayload = Get-Content -LiteralPath $statePath -Raw -Encoding UTF8 | ConvertFrom-Json
            $health = $statePayload.runtime_health
            if (-not $health) {
                Write-Host ("  {0}: runtime_health not found" -f $label) -ForegroundColor Yellow
                continue
            }
            $lastError = $health.last_error
            $errText = ""
            if ($lastError -and $lastError.component) {
                $errText = " last_error={0}/{1}@{2}" -f $lastError.component, $lastError.kind, $lastError.ts
            }
            Write-Host (
                "  {0}: mode={1} degraded_since={2} last_recovered={3} last_light={4} last_full={5}{6}" -f
                $label,
                $health.mode,
                $health.degraded_since,
                $health.last_recovered_ts,
                $health.last_light_resync_ts,
                $health.last_full_reconcile_ts,
                $errText
            )
        } catch {
            Write-Host ("  {0}: failed to read runtime health ({1})" -f $label, $_.Exception.Message) -ForegroundColor Yellow
        }
    }
}

Write-RuntimeHealth
