# Clash Proxy Multi-Instance Runbook

## Goal

Run multiple copytrade instances on one Windows machine, where each instance:

- uses its own follower account file
- uses its own Clash/Mihomo local proxy port
- writes isolated logs, runtime config, and supervisor session files
- keeps the core trading code path unchanged

This is the stable process-level proxy plan. It does not attempt per-account proxy routing inside one Python process.

## Network Model

Configure Clash/Mihomo so each local port is pinned to one outbound node:

```text
copytrade-node-01 -> 127.0.0.1:17891 -> primary Clash node A, optional backup node A2
copytrade-node-02 -> 127.0.0.1:17892 -> primary Clash node B, optional backup node B2
copytrade-node-03 -> 127.0.0.1:17893 -> primary Clash node C, optional backup node C2
```

Each copytrade instance receives process-level proxy environment variables:

```powershell
$env:HTTP_PROXY = "http://127.0.0.1:17891"
$env:HTTPS_PROXY = "http://127.0.0.1:17891"
$env:ALL_PROXY = "http://127.0.0.1:17891"
```

The supervised child process inherits these values from the launcher.

## Clash Requirement

Your current Windows machine has Clash for Windows with a Clash 2023.08.17 core. A temporary config smoke test accepted `listeners`, but this core did not bind listener ports reliably in practice. The recommended path is isolated sidecar Clash cores, one per copytrade port.

The older listener-profile approach looked like this, but it is not the default path for this repository:

```yaml
listeners:
  - name: node-a-port
    type: mixed
    listen: 127.0.0.1
    port: 17891
    proxy: "EXACT_NODE_A_NAME"

  - name: node-b-port
    type: mixed
    listen: 127.0.0.1
    port: 17892
    proxy: "EXACT_NODE_B_NAME"
```

Use `127.0.0.1` first. Avoid `0.0.0.0` unless you intentionally want LAN devices to access those proxy ports.

The recommended copytrade listener ports start at `17891` to avoid CFW defaults such as `7890` and `7891`.

## Node Selection

Export the exact node names from the active Clash profile:

```powershell
.\windows\export_clash_nodes.ps1
```

This writes:

```text
instances/clash_nodes.current.txt
```

Copy `instances/clash_listeners.example.json` to `instances/clash_listeners.json`, then fill `proxy` with the exact primary node name and `backups` with optional fallback node names:

```json
{
  "listeners": [
    {
      "name": "copytrade-node-a",
      "listen": "127.0.0.1",
      "port": 17891,
      "proxy": "EXACT_PRIMARY_NODE_A_NAME",
      "backups": ["EXACT_BACKUP_NODE_A_NAME"]
    }
  ]
}
```

The legacy listener apply helper still exists for experiments:

```powershell
.\windows\apply_clash_listeners.ps1 -CheckOnly
.\windows\apply_clash_listeners.ps1
```

The apply script backs up the active profile before writing.

Use isolated sidecar cores for normal operation. This leaves the main Clash for Windows VPN on `7890` untouched:

```powershell
.\windows\start_copytrade_clash_cores.ps1
.\windows\status_copytrade_clash_cores.ps1
.\windows\stop_copytrade_clash_cores.ps1
```

For a fresh machine, prefer the standardized one-command setup:

```powershell
.\windows\setup_copytrade_proxy_nodes.ps1
```

It lists nodes and asks the user which ones to use.

You can specify one backup per selected node:

```powershell
.\windows\setup_copytrade_proxy_nodes.ps1 `
  -Nodes "香港A01,香港A02,香港A03,香港A04,香港A05" `
  -BackupNodes "香港A06,香港A07,香港A08,香港A09,香港A10" `
  -AccountsDir ".\accounts_by_proxy"
```

## File Layout

Recommended files:

```text
accounts_by_proxy/group-01.json
accounts_by_proxy/group-02.json
instances/proxy_instances.json
logs/instances/copytrade-node-01/
logs/instances/copytrade-node-02/
logs/copytrade-node-01/
logs/copytrade-node-02/
```

`instances/proxy_instances.json` maps instance names to account files and proxy ports. Start from `instances/proxy_instances.example.json`.

## Start

From the project root:

```powershell
.\windows\start_copytrade_proxy_instance.ps1 -Instance copytrade-node-01
.\windows\start_copytrade_proxy_instance.ps1 -Instance copytrade-node-02
```

For dry-run:

```powershell
.\windows\start_copytrade_proxy_instance.ps1 -Instance copytrade-node-01 -Mode dry
```

For a preflight-only check without launching:

```powershell
.\windows\start_copytrade_proxy_instance.ps1 -Instance copytrade-node-01 -CheckOnly
```

For script-only validation before Clash listeners are configured:

```powershell
.\windows\start_copytrade_proxy_instance.ps1 -Instance copytrade-node-01 -InstancesConfig .\instances\proxy_instances.example.json -CheckOnly -SkipProxyPortCheck -SkipExitIpCheck
```

## Stop And Status

```powershell
.\windows\status_copytrade_proxy_instance.ps1 -Instance copytrade-node-01
.\windows\stop_copytrade_proxy_instance.ps1 -Instance copytrade-node-01
```

The scripts read the same `instances/proxy_instances.json` file by default.

## Validation

Before starting the supervisor, the launcher checks:

- instance exists in `instances/proxy_instances.json`
- account file exists
- base config exists
- proxy host/port is reachable
- optional outbound IP through the proxy

The IP check is informational. A failure blocks launch by default because a dead proxy would silently collapse the isolation model. Use `-SkipExitIpCheck` only when the public IP service is unavailable but the local proxy port is known-good.

The proxy port check also blocks launch by default. Use `-SkipProxyPortCheck` only for script validation or when another monitor has already confirmed the local listener.

## Operational Rules

- Do not put the same account in two live instances at the same time.
- Keep each instance on a stable proxy port.
- Give each instance a unique `prefix` and `session_name`.
- Keep generated runtime configs under `logs/instances/<name>/`.
- Review the printed proxy metadata before starting live mode.

## Rollback

Stop all proxy instances:

```powershell
.\windows\stop_copytrade_proxy_instance.ps1 -Instance copytrade-node-01
.\windows\stop_copytrade_proxy_instance.ps1 -Instance copytrade-node-02
```

Then use the original launcher:

```powershell
.\windows\start_copytrade_live_supervised.ps1
```
