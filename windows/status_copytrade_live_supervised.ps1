$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$root = Split-Path -Parent $scriptDir
Set-Location $root

$taskName = "POLY_SMARTMONEY_CopytradeV4PersistentLive"
$sessionPath = Join-Path $root "logs\persistent_live_session.json"

function Invoke-NativeCapture {
    param(
        [string]$FileName,
        [string[]]$Arguments
    )
    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = $FileName
    $psi.Arguments = ($Arguments | ForEach-Object {
        if ($_ -match '\s') { '"' + ($_ -replace '"', '\"') + '"' } else { $_ }
    }) -join ' '
    $psi.UseShellExecute = $false
    $psi.RedirectStandardOutput = $true
    $psi.RedirectStandardError = $true
    $proc = [System.Diagnostics.Process]::Start($psi)
    $stdout = $proc.StandardOutput.ReadToEnd()
    $stderr = $proc.StandardError.ReadToEnd()
    $proc.WaitForExit()
    return @{
        ExitCode = $proc.ExitCode
        Output   = ($stdout + $stderr).Trim()
    }
}

function Test-PidAlive {
    param(
        [int]$ProcessId
    )
    if ($ProcessId -le 0) {
        return $false
    }
    return [bool](Get-Process -Id $ProcessId -ErrorAction SilentlyContinue)
}

$taskInfo = Invoke-NativeCapture -FileName "schtasks.exe" -Arguments @("/Query", "/TN", $taskName, "/FO", "LIST", "/V")
$taskExists = $taskInfo.ExitCode -eq 0
$taskStatus = "missing"
$taskNextRun = ""
$taskLastResult = ""
if ($taskExists) {
    foreach ($line in ($taskInfo.Output -split "`r?`n")) {
        if ($line -match '^Status:\s+(.*)$') {
            $taskStatus = $matches[1].Trim()
        } elseif ($line -match '^Next Run Time:\s+(.*)$') {
            $taskNextRun = $matches[1].Trim()
        } elseif ($line -match '^Last Result:\s+(.*)$') {
            $taskLastResult = $matches[1].Trim()
        }
    }
}

$sessionExists = Test-Path $sessionPath
$session = $null
$supervisorPid = 0
$childPid = 0
$supervisorAlive = $false
$childAlive = $false
if ($sessionExists) {
    $session = Get-Content $sessionPath -Raw | ConvertFrom-Json
    $supervisorPid = [int]($session.supervisor_pid | ForEach-Object { $_ })
    $childPid = [int]($session.child_pid | ForEach-Object { $_ })
    $supervisorAlive = Test-PidAlive -ProcessId $supervisorPid
    $childAlive = Test-PidAlive -ProcessId $childPid
}

Write-Host ""
Write-Host "Persistent Copytrade Status" -ForegroundColor Cyan
Write-Host "Root: $root"
Write-Host ""
Write-Host "Scheduled task:"
Write-Host "  Name: $taskName"
Write-Host "  Exists: $taskExists"
Write-Host "  Status: $taskStatus"
if ($taskNextRun) {
    Write-Host "  Next run: $taskNextRun"
}
if ($taskLastResult) {
    Write-Host "  Last result: $taskLastResult"
}
Write-Host ""
Write-Host "Persistent session:"
Write-Host "  Exists: $sessionExists"
if ($sessionExists -and $session) {
    Write-Host "  Status: $($session.status)"
    Write-Host "  Desired state: $($session.desired_state)"
    Write-Host "  Started at: $($session.started_at)"
    Write-Host "  Updated at: $($session.updated_at)"
    Write-Host "  Restart count: $($session.restart_count)"
    Write-Host "  Supervisor PID: $supervisorPid (alive=$supervisorAlive)"
    Write-Host "  Child PID: $childPid (alive=$childAlive)"
    Write-Host "  Session file: $sessionPath"
    Write-Host "  Stdout log: $($session.stdout)"
    Write-Host "  Stderr log: $($session.stderr)"
    Write-Host "  Supervisor log: $($session.supervisor_log)"
} else {
    Write-Host "  Session file: $sessionPath"
}
Write-Host ""
