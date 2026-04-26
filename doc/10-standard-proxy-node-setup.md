# Standard Proxy Node Setup

This is the portable one-command flow for a Windows machine that has Clash for Windows, Clash Verge, or Mihomo running with a local profile.

## Interactive Setup

Start Clash first, then run:

```powershell
.\windows\setup_copytrade_proxy_nodes.ps1
```

The script will:

1. find the active Clash/Mihomo profile
2. find the running Clash/Mihomo core executable
3. list available proxy nodes
4. ask the user to input node indexes or node names
5. write `instances/clash_listeners.json`
6. start one isolated copytrade-only Clash core per selected node
7. optionally bind one account JSON file to each selected node
8. test each local proxy port with `api.ipify.org`

## Bind Nodes To Account JSON Files

Create the default five generic account group files:

```powershell
.\windows\init_proxy_account_groups.ps1
```

Prepare one account JSON per selected proxy node. Put them in a folder and name them so alphabetical order is the intended matching order:

```text
accounts_by_proxy/
  group-01.json
  group-02.json
  group-03.json
  group-04.json
  group-05.json
```

Run:

```powershell
.\windows\setup_copytrade_proxy_nodes.ps1 -Nodes "1,3,5,7,9" -AccountsDir ".\accounts_by_proxy"
```

Add one backup node per selected node:

```powershell
.\windows\setup_copytrade_proxy_nodes.ps1 `
  -Nodes "1,3,5,7,9" `
  -BackupNodes "11,13,15,17,19" `
  -AccountsDir ".\accounts_by_proxy"
```

Backup nodes are matched by order. The first backup belongs to the first selected node, the second backup belongs to the second selected node, and so on. Use semicolons if one selected node has more than one backup:

```powershell
.\windows\setup_copytrade_proxy_nodes.ps1 `
  -Nodes "1,3" `
  -BackupNodes "11+13;15+17" `
  -AccountsDir ".\accounts_by_proxy"
```

Matching is order-based:

```text
first selected node  -> first JSON file by name
second selected node -> second JSON file by name
third selected node  -> third JSON file by name
```

The script writes:

```text
instances/clash_listeners.json
instances/proxy_instances.json
```

`instances/proxy_instances.json` is what `start_copytrade_proxy_instance.ps1` uses.

You can also pass account files explicitly:

```powershell
.\windows\setup_copytrade_proxy_nodes.ps1 `
  -Nodes "1,3,5" `
  -AccountFiles ".\accounts_by_proxy\group-01.json,.\accounts_by_proxy\group-02.json,.\accounts_by_proxy\group-03.json"
```

The number of selected nodes must equal the number of account JSON files.

## Non-Interactive Setup

Use node indexes:

```powershell
.\windows\setup_copytrade_proxy_nodes.ps1 -Nodes "1,3,5,7,9"
```

Use unique node keywords:

```powershell
.\windows\setup_copytrade_proxy_nodes.ps1 -Nodes "A01,A02,A03"
```

Default ports start at `17891`, so five selected nodes become:

```text
127.0.0.1:17891
127.0.0.1:17892
127.0.0.1:17893
127.0.0.1:17894
127.0.0.1:17895
```

## Manual Overrides

If automatic discovery fails:

```powershell
.\windows\setup_copytrade_proxy_nodes.ps1 `
  -ProfilePath "C:\path\to\config.yaml" `
  -ClashCorePath "C:\path\to\mihomo.exe"
```

To only generate mappings without starting cores:

```powershell
.\windows\setup_copytrade_proxy_nodes.ps1 -Nodes "1,3,5" -AccountsDir ".\accounts_by_proxy" -NoStart
```

## Manage Cores

```powershell
.\windows\status_copytrade_clash_cores.ps1
.\windows\stop_copytrade_clash_cores.ps1
.\windows\start_copytrade_clash_cores.ps1
```

These sidecar cores do not change the user's main Clash GUI selected node.

## Start Copytrade Instances

After node/account binding is generated:

```powershell
.\windows\start_copytrade_proxy_instance.ps1 -Instance copytrade-node-01 -Mode dry
.\windows\start_copytrade_proxy_instance.ps1 -Instance copytrade-node-02 -Mode dry
```

Use the instance names printed in `instances/proxy_instances.json`.
