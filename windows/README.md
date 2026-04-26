# Windows Launchers

Use `entrypoints/` for day-to-day start, status, and stop commands.

```text
entrypoints/single_node.ps1       legacy single-node launcher
entrypoints/multi_node_5.ps1      five-instance multi-proxy launcher
single_node/                      legacy single-account implementation scripts
multi_node/                       five-node proxy implementation scripts
```

The lower-level implementation tools are grouped under `single_node/` and
`multi_node/`.

Common commands:

```powershell
cd D:\AI\copytrade_v5\POLY_SMARTMONEY\copytrade_v5_muti\windows\entrypoints

.\single_node.ps1 -Action status

.\multi_node_5.ps1 -Action check -Mode dry
.\multi_node_5.ps1 -Action start -Mode dry
.\multi_node_5.ps1 -Action status
.\multi_node_5.ps1 -Action stop
```

Do not start `multi_node_5.ps1 -Action start -Mode live` until every
`accounts_by_proxy/group-*.json` file contains a distinct live account group.
