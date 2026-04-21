$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$root = Split-Path -Parent $scriptDir
Set-Location $root
$taskName = "POLY_SMARTMONEY_CopytradeV4PersistentLive"

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

$pycmd = Get-PythonCommand
$args = @(
    "$root\persistent_copytrade_runner.py",
    "stop",
    "--workdir", $root,
    "--session-name", "persistent_live_session.json",
    "--wait-sec", "20"
)
$command = @($pycmd + $args)
$output = & $command[0] $command[1..($command.Length - 1)]
if ($LASTEXITCODE -ne 0) {
    throw "Stop failed with exit code $LASTEXITCODE"
}

try {
    $json = $output | ConvertFrom-Json
    Write-Host ""
    Write-Host "Persistent live supervisor stop requested." -ForegroundColor Yellow
    Write-Host "Session: $($json.session)"
    Write-Host "Supervisor alive after stop: $($json.supervisor_alive)"
    Write-Host "Child alive after stop: $($json.child_alive)"
    Write-Host ""
} catch {
    Write-Host $output
}

$deleteResult = Invoke-NativeCapture -FileName "schtasks.exe" -Arguments @("/Delete", "/TN", $taskName, "/F")
if ($deleteResult.ExitCode -eq 0) {
    Write-Host "Scheduled task removed." -ForegroundColor Yellow
    Write-Host "Task name: $taskName"
} else {
    $deleteText = $deleteResult.Output
    if ($deleteText -match "ERROR:" -or $deleteText -match "cannot find") {
        Write-Host "Scheduled task was already absent." -ForegroundColor Yellow
        Write-Host "Task name: $taskName"
    } else {
        throw "Scheduled task deletion failed: $deleteText"
    }
}
