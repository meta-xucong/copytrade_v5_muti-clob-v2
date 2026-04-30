param()

$ErrorActionPreference = "Stop"

$managed = @(Get-CimInstance Win32_Process | Where-Object {
    $_.Name -in @("clash-win64.exe", "clash.exe", "mihomo.exe", "mihomo-windows-amd64.exe", "verge-mihomo.exe") -and
    $_.CommandLine -like "*logs\clash_copytrade\cores*"
})

if ($managed.Count -eq 0) {
    Write-Host "No managed copytrade Clash cores are running."
    exit 0
}

foreach ($proc in $managed) {
    Stop-Process -Id $proc.ProcessId -Force
    Write-Host "Stopped managed copytrade Clash/Mihomo core pid=$($proc.ProcessId)"
}

