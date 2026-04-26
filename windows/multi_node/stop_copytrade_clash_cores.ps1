param()

$ErrorActionPreference = "Stop"

$managed = @(Get-CimInstance Win32_Process | Where-Object {
    $_.Name -eq "clash-win64.exe" -and $_.CommandLine -like "*logs\clash_copytrade\cores*"
})

if ($managed.Count -eq 0) {
    Write-Host "No managed copytrade Clash cores are running."
    exit 0
}

foreach ($proc in $managed) {
    Stop-Process -Id $proc.ProcessId -Force
    Write-Host "Stopped managed copytrade Clash core pid=$($proc.ProcessId)"
}

