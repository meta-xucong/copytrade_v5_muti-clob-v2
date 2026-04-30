param(
    [ValidateSet("live", "dry-run")]
    [string]$Mode = "live",
    [int]$Minutes = 10,
    [int]$IntervalSeconds = 30,
    [int]$ExpectedNodes = 5,
    [switch]$StopOnCritical = $true
)

$ErrorActionPreference = "Continue"

$start = Get-Date
$end = $start.AddMinutes($Minutes)
$criticalPattern = "Traceback|RuntimeError|CLOB init failed|status=301|pUSD preflight failed|Could not create api key"
$networkPattern = "request error:|WinError 10054|Server disconnected|sync open orders failed|read operation timed out|ConnectTimeout|ReadTimeout|ConnectionResetError|Connection aborted"

function Read-Sessions {
    Get-ChildItem .\logs -File -Filter "persistent_${Mode}_copytrade-node-*_session.json" |
        Sort-Object Name |
        ForEach-Object {
            try {
                $json = Get-Content $_.FullName -Raw | ConvertFrom-Json
                [pscustomobject]@{ Name = $_.BaseName; Json = $json; Path = $_.FullName }
            } catch {
                [pscustomobject]@{ Name = $_.BaseName; Error = $_.Exception.Message; Path = $_.FullName }
            }
        }
}

function Test-LocalPort([int]$Port) {
    $client = New-Object Net.Sockets.TcpClient
    try {
        $async = $client.BeginConnect("127.0.0.1", $Port, $null, $null)
        $ok = $async.AsyncWaitHandle.WaitOne(1000, $false)
        if ($ok) {
            $client.EndConnect($async)
        }
        return $ok
    } catch {
        return $false
    } finally {
        $client.Close()
    }
}

function Stop-Live([string]$Reason) {
    Write-Output "CRITICAL: $Reason"
    if ($StopOnCritical) {
        powershell -NoProfile -ExecutionPolicy Bypass -File .\windows\entrypoints\multi_node_5.ps1 -Action stop -Mode $Mode | Write-Output
    }
    exit 2
}

function Recent-LogLines($StderrPath, [datetime]$Since) {
    if (-not $StderrPath -or -not (Test-Path $StderrPath)) {
        return @()
    }
    $lines = Get-Content $StderrPath -Tail 500 -ErrorAction SilentlyContinue
    $recent = @()
    foreach ($line in $lines) {
        if ($line -match "^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})") {
            try {
                $ts = [datetime]::ParseExact($Matches[1], "yyyy-MM-dd HH:mm:ss", [Globalization.CultureInfo]::InvariantCulture)
                if ($ts -ge $Since) {
                    $recent += $line
                }
            } catch {
            }
        }
    }
    return $recent
}

$checks = 0
while ((Get-Date) -lt $end) {
    $checks += 1
    $sessions = @(Read-Sessions)
    if ($sessions.Count -lt $ExpectedNodes) {
        Stop-Live "session files missing: count=$($sessions.Count), expected=$ExpectedNodes"
    }

    foreach ($session in $sessions) {
        if ($session.Error) {
            Stop-Live "cannot parse session $($session.Name): $($session.Error)"
        }
        if ($session.Json.status -ne "running") {
            Stop-Live "$($session.Name) status=$($session.Json.status)"
        }
        if ([int]$session.Json.restart_count -gt 0) {
            Stop-Live "$($session.Name) restart_count=$($session.Json.restart_count)"
        }
    }

    foreach ($port in 17891..17895) {
        if (-not (Test-LocalPort $port)) {
            Stop-Live "proxy port $port not listening"
        }
    }

    $networkByNode = @{}
    foreach ($session in $sessions) {
        $recent = @(Recent-LogLines ([string]$session.Json.stderr_path) $start)
        $hard = @($recent | Select-String -Pattern $criticalPattern -CaseSensitive:$false)
        if ($hard.Count -gt 0) {
            Stop-Live "hard log error in $($session.Name): $($hard[-1].Line)"
        }

        $network = @($recent | Select-String -Pattern $networkPattern -CaseSensitive:$false)
        if ($network.Count -gt 0) {
            $networkByNode[$session.Name] = $network.Count
        }
    }

    foreach ($key in $networkByNode.Keys) {
        if ([int]$networkByNode[$key] -ge 3) {
            Stop-Live "network errors >=3 in ${key}: $($networkByNode[$key])"
        }
    }
    if ($networkByNode.Keys.Count -ge 2) {
        $detail = ($networkByNode.GetEnumerator() | ForEach-Object { "$($_.Key)=$($_.Value)" } | Sort-Object) -join ", "
        Stop-Live "network errors on >=2 nodes: $detail"
    }

    Start-Sleep -Seconds $IntervalSeconds
}

$sessions = @(Read-Sessions)
Write-Output "OK_WINDOW start=$($start.ToString('yyyy-MM-dd HH:mm:ss')) end=$((Get-Date).ToString('yyyy-MM-dd HH:mm:ss')) checks=$checks"
$sessions |
    ForEach-Object {
        [pscustomobject]@{
            Node = $_.Name
            Status = $_.Json.status
            Restarts = $_.Json.restart_count
            ChildPid = $_.Json.child_pid
            Updated = $_.Json.updated_at
        }
    } |
    Format-Table -AutoSize
