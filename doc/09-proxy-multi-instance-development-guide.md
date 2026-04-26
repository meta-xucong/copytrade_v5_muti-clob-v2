# Proxy Multi-Instance Development Guide

## Scope

This guide covers the stable process-level proxy implementation. It intentionally avoids changes to:

- CLOB order construction
- signer behavior
- per-account trade execution
- WebSocket parsing
- market resolution logic

The launcher generates an instance-specific runtime config and starts the existing persistent supervisor with proxy environment variables.

## Design

The source of truth is `instances/proxy_instances.json`.

Each instance entry declares:

```json
{
  "name": "copytrade-node-01",
  "enabled": true,
  "base_config": "copytrade_config.json",
  "accounts_file": "accounts_by_proxy/group-01.json",
  "http_proxy": "http://127.0.0.1:17891",
  "https_proxy": "http://127.0.0.1:17891",
  "all_proxy": "http://127.0.0.1:17891",
  "log_dir": "logs/copytrade-node-01",
  "prefix": "persistent_live_copytrade-node-01",
  "session_name": "persistent_live_copytrade-node-01_session.json"
}
```

The launcher:

1. loads the base copytrade config
2. overwrites `accounts_file`
3. overwrites `log_dir` with an absolute instance log path
4. writes `logs/instances/<name>/copytrade_config.<name>.json`
5. sets process proxy environment variables
6. calls `persistent_copytrade_runner.py launch`

## Why Runtime Configs

Runtime configs avoid editing `copytrade_config.json` for each instance. This matters because the active strategy config is large and frequently tuned.

Runtime configs are generated artifacts. They belong under `logs/instances/<name>/` and should not be treated as source configuration.

## Script Responsibilities

`windows/start_copytrade_proxy_instance.ps1`

- validates an instance
- checks proxy connectivity
- writes runtime config
- starts the supervisor

`windows/status_copytrade_proxy_instance.ps1`

- resolves the instance session file
- prints session status and relevant PIDs

`windows/stop_copytrade_proxy_instance.ps1`

- resolves the instance session file
- delegates shutdown to `persistent_copytrade_runner.py stop`

## Testing Strategy

Use focused checks first:

```powershell
py -3 -m py_compile persistent_copytrade_runner.py copytrade_run.py
```

Then validate script parsing and config generation:

```powershell
.\windows\multi_node\start_copytrade_proxy_instance.ps1 -Instance copytrade-node-01 -InstancesConfig .\instances\proxy_instances.example.json -CheckOnly -SkipExitIpCheck
```

If Clash listener ports are not configured yet, include `-SkipProxyPortCheck`.

Live/dry runtime validation should be done with a small account group first.

## Acceptance Criteria

- each instance can generate an isolated runtime config
- each instance has a unique session file
- each instance has a unique log directory
- process-level proxy env vars are visible in the generated launch flow
- status/stop scripts target only the requested instance
- no core trading behavior changes are required
