# Copytrade Entrypoints

This folder contains human-facing launchers. The parent `windows` folder still
contains the lower-level implementation scripts.

## Single Node

Legacy single-node launcher:

```powershell
.\single_node.ps1 -Action start
.\single_node.ps1 -Action status
.\single_node.ps1 -Action stop
```

Double-click helpers:

```text
single_node_start.bat
single_node_status.bat
single_node_stop.bat
```

## Five Proxy Nodes

Five-node launcher aligned to:

```text
copytrade-node-01 -> 127.0.0.1:17891
copytrade-node-02 -> 127.0.0.1:17892
copytrade-node-03 -> 127.0.0.1:17893
copytrade-node-04 -> 127.0.0.1:17894
copytrade-node-05 -> 127.0.0.1:17895
```

Setup or refresh proxy mappings:

```powershell
.\multi_node_5.ps1 -Action setup
```

Preflight all five without starting supervisors:

```powershell
.\multi_node_5.ps1 -Action check -Mode dry
```

Start all five in dry mode:

```powershell
.\multi_node_5.ps1 -Action start -Mode dry
```

Start all five in live mode:

```powershell
.\multi_node_5.ps1 -Action start -Mode live
```

Status and stop:

```powershell
.\multi_node_5.ps1 -Action status
.\multi_node_5.ps1 -Action stop
```

Double-click helpers:

```text
multi_node_5_start_dry.bat
multi_node_5_start_live.bat
multi_node_5_status.bat
multi_node_5_stop.bat
```

Live mode blocks duplicate accounts across `accounts_by_proxy/group-*.json` by
default. Use `-AllowDuplicateAccounts` only for an intentional short test.
