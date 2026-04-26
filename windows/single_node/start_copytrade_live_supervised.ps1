$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$windowsDir = Split-Path -Parent $scriptDir
$root = Split-Path -Parent $windowsDir
Set-Location $root

$taskName = "POLY_SMARTMONEY_CopytradeV4PersistentLive"
$taskScript = Join-Path $scriptDir "task_launch_copytrade_live_supervised.ps1"
$action = "powershell.exe -NoProfile -ExecutionPolicy Bypass -File `"$taskScript`""

function Try-RegisterTask {
    param(
        [string[]]$Arguments
    )
    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = "schtasks.exe"
    $psi.Arguments = ($Arguments | ForEach-Object {
        if ($_ -match '\s') { '"' + ($_ -replace '"', '\"') + '"' } else { $_ }
    }) -join ' '
    $psi.UseShellExecute = $false
    $psi.RedirectStandardOutput = $true
    $psi.RedirectStandardError = $true
    $proc = [System.Diagnostics.Process]::Start($psi)
    $stdOut = $proc.StandardOutput.ReadToEnd()
    $stdErr = $proc.StandardError.ReadToEnd()
    $proc.WaitForExit()
    return @{
        ExitCode = $proc.ExitCode
        Output   = (($stdOut + $stdErr) | Out-String)
    }
}

$commonArgs = @("/Create", "/TN", $taskName, "/SC", "ONLOGON", "/DELAY", "0000:30", "/TR", $action, "/F")
$result = Try-RegisterTask -Arguments ($commonArgs + @("/RL", "HIGHEST"))
if ($result.ExitCode -ne 0) {
    $result = Try-RegisterTask -Arguments ($commonArgs + @("/RL", "LIMITED"))
}
$taskRegistered = $result.ExitCode -eq 0

Write-Host ""
if ($taskRegistered) {
    Write-Host "Scheduled task registered." -ForegroundColor Green
    Write-Host "Task name: $taskName"
} else {
    Write-Host "Scheduled task registration skipped." -ForegroundColor Yellow
    Write-Host "Task name: $taskName"
    Write-Host "Reason: $($result.Output.Trim())"
}
Write-Host ""
Write-Host "Launcher target: Polymarket CLOB V2" -ForegroundColor Cyan
Write-Host "Before going live, confirm pUSD collateral is ready and treat pre-cutover open orders as stale." -ForegroundColor Yellow
Write-Host ""

& $taskScript
if ($LASTEXITCODE -ne 0) {
    throw "Launch helper failed with exit code $LASTEXITCODE"
}
