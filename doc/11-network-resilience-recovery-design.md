# V5 网络韧性与断线恢复优化开发文档

## 1. 背景

V5 当前已经具备一部分“网络不稳定时不盲目交易”的能力：目标仓位、目标成交、我的仓位、我的成交、远端挂单同步都有 `ok / incomplete / unreliable` 口径；主循环遇到不可靠数据时会跳过本轮或冻结买入；启动阶段也有 `hemostasis` 止血恢复逻辑，用来扫描目标卖出并补卖我方残余仓位。

本次要做的不是把这些能力推翻重写，而是把它们收口成一套明确的运行时网络状态机：

- 主线路、备用线路、CLOB、Data API、WS 任一短暂异常时，进程不主动退出。
- 网络恢复后自动重连、自动校准状态。
- 校准分两层：轻量 resync 每次恢复都做；全量卖出/止血恢复最多 30 分钟执行一次。
- 外部监控不再因为普通网络错误停机，只对真正 fatal 或长期无法确认交易状态的情况报警或进入安全模式。

## 2. 本地代码现状结论

本地代码里实际可复用的核心逻辑很多，主要集中在：

- `copytrade_run.py`
- `ct_exec.py`
- `ct_clob_gateway.py`
- `ct_data.py`
- `ct_state.py`
- `persistent_copytrade_runner.py`
- `smartmoney_query/poly_martmoney_query/api_client.py`

本次检索没有在 `.py / .json / .ps1 / .md` 里发现实际可调用的 WebSocket client 或 reconnect loop，只有 `doc/09-proxy-multi-instance-development-guide.md` 提到 WebSocket parsing。因此，如果 V5 当前运行版已经切到 WS，那 WS 模块要么尚未同步到本地仓库，要么在外部依赖里；本开发文档按“需要新增 WS 接入层或把已有 WS 模块接入统一 health 接口”设计。

## 3. 目标

1. 短暂网络异常不杀进程，不触发 supervisor 重启，不外部 stop。
2. 网络异常期间不继续做不可靠的新买入。
3. 已有仓位的卖出优先级高于新买入，但必须在订单状态可确认时执行。
4. 网络恢复后自动执行轻量状态同步。
5. 网络恢复后自动执行一次受 30 分钟节流保护的全量卖出恢复。
6. WS 断线后必须用 REST/Data API 做回放修复，避免漏掉断线期间的成交信号。
7. 所有恢复行为可观测，可从日志和 state 文件确认当前模式、最后错误、最后恢复时间、最后全量自检时间。

## 4. 非目标

- 不自动 wrap、approve 或做链上资金迁移。
- 不绕过 pUSD preflight、私钥、API credential、SDK 导入失败等 fatal/preflight 类错误。
- 不修改跟单策略的资金比例、目标列表、账号分配、代理节点分配。
- 不把普通网络恢复逻辑交给 Windows supervisor 通过重启完成。
- 不在 WS 事件流不可靠时完全信任 WS，REST/Data API 仍作为恢复校准面。

## 5. 可直接复用的现有机制

### 5.1 Data API 请求回退

文件：

- `smartmoney_query/poly_martmoney_query/api_client.py`
- `ct_data.py`

可复用点：

- `_request_with_backoff()` 已有指数回退、抖动、`Retry-After` 处理。
- `_fetch_positions_norm_http()` 对 429、5xx、timeout、connection error 有重试。
- `fetch_positions_norm()` 返回 `(positions, info)`，其中 `info.ok` 和 `info.incomplete` 已经可以作为健康判断输入。

使用方式：

- 不需要重写请求回退。
- 新增 runtime health 时，直接把 `info.ok == false`、`info.incomplete == true`、`error_msg` 归入 `component=data_api_positions`。
- 失败时继续复用现有“跳过本轮”的策略。

### 5.2 多目标地址聚合

文件：

- `copytrade_run.py`

可复用点：

- `_fetch_all_target_positions()` 对每个目标地址独立 try/catch，单个目标失败不会导致整个进程退出。
- `_fetch_all_target_actions()` 同样对每个目标地址独立容错，并返回 `ok / incomplete / latest_ms`。

使用方式：

- 作为恢复后的 target resync 入口。
- 断线期间如果部分目标失败，把 health 标记为 degraded，把 action replay window 打开，不推进 cursor。

### 5.3 Action replay 与 seen-id 去重

文件：

- `copytrade_run.py`
- `ct_state.py`

可复用点：

- `actions_unreliable_until`
- `actions_replay_from_ms`
- `target_actions_cursor_ms`
- `target_trades_cursor_ms`
- `seen_action_ids`
- `seen_trade_ids`

现有逻辑已经做到：

- Action 不可靠时保留 cursor。
- 设置 replay window。
- replay 时保留 seen ids，避免重复消费历史 action。
- API lag 过高时自动打开短窗口 replay。

使用方式：

- WS 重连后必须设置 `actions_replay_from_ms = now_ms - recovery_actions_replay_window_sec * 1000`。
- 不要清空 `seen_action_ids / seen_trade_ids`。
- 恢复完成且 REST replay 成功后再推进 cursor。

### 5.4 我的成交不可靠时冻结买入

文件：

- `copytrade_run.py`
- `ct_state.py`

可复用点：

- `my_trades_unreliable_until`
- 当前下单过滤阶段已有 `if my_trades_unreliable and side == "BUY": blocked_reasons.add("my_trades_unreliable")`

使用方式：

- 网络 degraded 时，不需要新增一套买入拦截，只要把统一 health 的 `buy_paused_until` 或 `network_degraded` 映射到同一个买入过滤区域。
- 原有 `my_trades_unreliable` 继续保留，作为更细粒度的买入冻结原因。

### 5.5 仓位不完整时跳过本轮

文件：

- `copytrade_run.py`

可复用点：

- `target_info.ok == false` 或 `target_info.incomplete == true` 时 `[SAFE] target positions incomplete; skipping this loop`
- `my_info.ok == false` 或 `my_info.incomplete == true` 时 `[SAFE] my positions incomplete; skipping this loop`

使用方式：

- 继续作为 degraded/safe mode 下的主要保护。
- 新增 health 后，这类 skip loop 同时记录 `component=target_positions` 或 `component=my_positions` 的失败。

### 5.6 远端挂单同步的账本优先策略

文件：

- `copytrade_run.py`
- `ct_clob_gateway.py`
- `ct_exec.py`

可复用点：

- `fetch_open_orders_norm_v2()` 返回 `(orders, ok, err)`。
- 主循环合并远端可见挂单时，不会因为远端短暂不可见就删除本地 managed orders。
- `order_visibility_grace_sec`
- `order_ts_by_id`
- `managed_order_ids`
- `remote_order_snapshot_ts`

使用方式：

- 网络恢复后的轻量 resync 第一优先级就是调用 `fetch_open_orders_norm()`。
- 如果失败，进程继续存活，但进入 `safe_mode/order_state_unknown`，冻结新买入。
- 如果成功，复用现有 ledger-first merge，而不是重建 open orders。

### 5.7 启动止血恢复

文件：

- `copytrade_run.py`

可复用点：

- `_collect_target_sell_token_ids()`
- `_run_hemostasis_recovery_for_account()`
- `_run_hemostasis_recovery_startup()`
- `must_exit_tokens`
- `last_nonzero_my_shares`
- `open_orders`
- `topic_state`
- `_estimate_recovery_shares_from_state()`

现有逻辑已经能：

- 扫描目标近期 SELL token。
- 把相关 token 标成 `must_exit`。
- 拉我的仓位和远端挂单。
- 取消相关卖单并重挂/市价卖出。
- 多轮确认残余仓位。

使用方式：

- 不建议重跑完整 boot 流程，因为 boot 会涉及 baseline/cursor 语义，可能改变启动基线。
- 应把 hemostasis 抽成通用入口：`run_hemostasis_recovery(reason="startup|network_recovered")`。
- `network_recovered` 入口加 30 分钟节流。

### 5.8 must_exit 与在线卖出恢复

文件：

- `copytrade_run.py`

可复用点：

- `must_exit_tokens`
- `last_target_sell_action_ts_by_token`
- `online_sell_recover_window_sec`
- `online_sell_recover_min_shares`
- topic `EXITING / EXIT_RECOVER`

使用方式：

- 网络恢复后，如果 replay 发现 SELL，直接复用 `must_exit` 和现有退出路径。
- 全量 hemostasis 只作为兜底，不替代正常实时 sell flow。

### 5.9 missing freeze 与 topic 风控

文件：

- `copytrade_run.py`

可复用点：

- `missing_data_freeze`
- `target_missing_streak`
- `missing_timeout_sec`
- `missing_to_zero_rounds`
- `topic_risk_l2_freeze_sec`

使用方式：

- 网络 degraded 时，不要把 target missing 误判成目标清仓。
- 继续用 freeze 机制阻止不可靠数据驱动的买入或误卖。

### 5.10 supervisor 只负责进程崩溃

文件：

- `persistent_copytrade_runner.py`

可复用点：

- 子进程退出时 supervisor 自动重启。
- stop flag 和 desired_state 已经清晰区分“人为请求停止”和“进程异常退出”。

使用方式：

- 网络异常不应该通过 supervisor restart 解决。
- supervisor 继续只管 crash、spawn failure、手动 stop。
- 外部监控规则需要改成读取 runtime health，而不是看到网络 ERROR 就 stop。

## 6. 当前缺口

### 6.1 缺统一 runtime health

当前代码的健康信息散落在：

- `actions_unreliable_until`
- `my_trades_unreliable_until`
- `target_info.ok`
- `my_info.ok`
- `fetch_open_orders_norm(... ok, err)`
- 日志里的 warning/error

缺一个统一结构表达：

- 当前是 running、degraded、reconnecting、resyncing、safe_mode 还是 fatal。
- 哪个组件异常。
- 最近一次异常时间。
- 最近一次恢复时间。
- 是否需要轻量 resync。
- 是否需要全量 reconcile。
- 买入是否应暂停。

### 6.2 缺错误分类收口

`doc/03-technical-design.md` 已经定义四类错误：

- `transient`
- `preflight`
- `market_state`
- `fatal`

但实际代码里分类仍然分散。比如：

- `fetch_open_orders_norm_v2()` 只返回错误字符串。
- `get_orderbook_v2()` 捕获异常后返回空盘口。
- `apply_actions()` 对下单失败多数只记 warning。
- CLOB 初始化失败、pUSD preflight、RuntimeError 等没有和普通网络异常统一分层。

需要新增一个集中分类器。

### 6.3 缺恢复边缘触发器

当前有启动恢复，但缺：

- 从 degraded/reconnecting 回到 running 的边缘检测。
- 恢复后自动轻量 resync。
- 恢复后自动全量 hemostasis，且最多 30 分钟一次。

### 6.4 缺 WS 连接状态接入

本地仓库没有实际 WS 连接模块。需要新增或接入以下信息：

- `ws_connected`
- `last_ws_msg_ts`
- `last_ws_error`
- `ws_reconnect_count`
- `ws_gap_start_ts`
- `ws_gap_end_ts`

WS 恢复后不能直接认为事件完整，必须触发 REST replay。

### 6.5 外部监控停机规则需要降级

之前停机是因为外部 heartbeat 规则看到 10 分钟内多个节点网络 ERROR 后执行 `multi_node_5.ps1 -Action stop`。新策略下，普通网络异常应该变成：

- 进程内部 degraded。
- 外部监控报告 degraded 状态。
- 超过阈值仍无法确认订单状态时报警。
- 只有 fatal/preflight/program crash 才 stop。

## 7. 推荐总体架构

### 7.1 新增 `ct_runtime_health.py`

职责：

- 统一错误分类。
- 维护运行时状态。
- 提供是否暂停买入、是否需要 resync、是否 fatal 的判断。

建议接口：

```python
def classify_error(exc_or_msg: object) -> str:
    """return: transient | preflight | market_state | fatal"""

def ensure_runtime_health(state: dict) -> dict:
    """Initialize and normalize state['runtime_health']."""

def record_component_success(state: dict, component: str, now_ts: int) -> None:
    """Mark one component healthy and detect recovery edge."""

def record_component_failure(
    state: dict,
    component: str,
    kind: str,
    message: str,
    now_ts: int,
) -> None:
    """Record failure and move mode to degraded/safe_mode/fatal."""

def should_pause_buys(state: dict, now_ts: int) -> tuple[bool, str]:
    """Return whether BUY actions should be blocked."""

def consume_resync_request(state: dict) -> tuple[bool, bool]:
    """Return (need_light_resync, need_full_reconcile)."""
```

错误分类建议：

| 类型 | 示例 | 行为 |
| --- | --- | --- |
| `transient` | `WinError 10054`, timeout, connection reset, `Server disconnected`, 408, 429, 5xx, engine restarting 425 | degraded/reconnecting，不停机 |
| `market_state` | market closed, no orderbook, no best bid, min order not satisfied, FAK no match | 不停机，按市场状态跳过/hold |
| `preflight` | pUSD 不足、allowance 不足、API credential 创建失败、配置缺失 | 启动期阻断或 live 安全停新交易 |
| `fatal` | Traceback、RuntimeError、状态文件损坏、私钥无效、代码 bug | 停机诊断 |

### 7.2 扩展 `ct_state.DEFAULT_STATE`

新增：

```json
"runtime_health": {
  "mode": "running",
  "degraded_since": 0,
  "last_recovered_ts": 0,
  "last_light_resync_ts": 0,
  "last_full_reconcile_ts": 0,
  "buy_paused_until": 0,
  "needs_light_resync": false,
  "needs_full_reconcile": false,
  "components": {},
  "last_error": {
    "component": "",
    "kind": "",
    "message": "",
    "ts": 0
  },
  "order_state_unknown_since": 0,
  "ws_gap_start_ts": 0,
  "ws_last_msg_ts": 0,
  "ws_reconnect_count": 0
}
```

兼容原则：

- 老 state 缺字段时自动补默认值。
- 不迁移、不清空现有 cursor 和 seen ids。
- 不用 runtime health 替代已有 `actions_unreliable_until` 等字段，而是把它们作为组件输入。

### 7.3 新增 `ct_recovery.py`

职责：

- 网络恢复后的轻量状态同步。
- 受节流控制的全量止血恢复。

建议接口：

```python
def run_light_resync_after_reconnect(
    cfg: dict,
    data_client: object,
    account_contexts: list,
    logger: logging.Logger,
) -> dict:
    """Fetch remote open orders, my positions, my trades cursor, and invalidate target cache."""

def run_full_reconcile_after_reconnect(
    cfg: dict,
    data_client: object,
    account_contexts: list,
    target_addresses: list[str],
    logger: logging.Logger,
    dry_run: bool,
) -> dict:
    """Reuse hemostasis sell recovery, throttled by runtime_health.last_full_reconcile_ts."""
```

实现原则：

- 轻量 resync 每次网络恢复都做。
- 全量 reconcile 最多每 1800 秒做一次。
- 全量 reconcile 只做卖出/止血恢复，不重新执行 boot baseline。
- 全量 reconcile 期间冻结新买入。

## 8. 主循环改造设计

### 8.1 初始化 health

在 `copytrade_run.py` 加载每个账号 state 后：

```python
ensure_runtime_health(acct_ctx.state)
```

在 `_apply_cfg_settings()` 中读取新增配置。

### 8.2 包装关键组件

需要记录 health 的组件：

- `data_api_target_positions`
- `data_api_target_actions`
- `data_api_my_positions`
- `data_api_my_trades`
- `clob_open_orders`
- `clob_orderbook`
- `clob_place_order`
- `ws_market`
- `ws_user`

示例：

```python
remote_orders, ok, err = fetch_open_orders_norm(clob_read_client, api_timeout_sec)
if ok:
    record_component_success(state, "clob_open_orders", now_ts)
else:
    kind = classify_error(err)
    record_component_failure(state, "clob_open_orders", kind, str(err), now_ts)
```

### 8.3 degraded 模式行为

进入 degraded 的触发：

- 任一关键网络组件 transient 失败。
- WS heartbeat 超时。
- Data API 返回 incomplete。
- CLOB open orders 同步失败。

degraded 下：

- 不退出进程。
- 不推进不可靠 cursor。
- 设置 action replay window。
- 暂停新 BUY。
- 如果订单状态不可确认，SELL 也只允许在安全条件满足时执行；否则进入 `safe_mode/order_state_unknown`。
- 继续周期性尝试恢复。

### 8.4 恢复边缘

当 degraded/reconnecting 后，关键组件连续成功一次或达到配置的成功条件：

1. mode 变为 `resyncing`。
2. 执行轻量 resync。
3. 设置 `actions_replay_from_ms`。
4. 如果距离上次全量 reconcile >= 1800 秒，执行 hemostasis recovery。
5. 成功后 mode 变回 `running`。

建议恢复条件：

- `clob_open_orders` 成功。
- `data_api_my_positions` 成功。
- `data_api_target_actions` 成功。
- 如果启用 WS，则 WS 已 connected 且最近 heartbeat 正常。

### 8.5 fatal 条件

这些情况仍应停机或至少进入不可交易状态：

- `RuntimeError` 或未捕获 Traceback。
- CLOB client 初始化失败。
- 私钥、funder、proxy wallet 配置错误。
- pUSD preflight failed。
- API credential 无法创建或签名失败。
- 下单请求结果长期 unknown，且无法通过 open orders / trades / positions 确认。
- state 文件无法保存或持续损坏。

注意：普通 `WinError 10054`、timeout、`Server disconnected` 不属于 fatal。

## 9. WS 接入设计

### 9.1 WS 只做实时加速，不做唯一事实源

WS 的优点是实时性高，但断线期间天然可能漏事件。因此设计口径：

- WS 正常时用于实时触发。
- WS 断线时进入 degraded。
- WS 重连后必须用 REST/Data API replay 修复缺口。

### 9.2 建议新增 WS adapter

如果当前 WS 代码尚未同步，建议新增：

- `ct_ws.py`

职责：

- 建立连接。
- heartbeat/ping-pong。
- 自动重连。
- 指数回退。
- 记录 gap 起止时间。
- 将 WS 事件写入内存队列，不直接推进最终 cursor。

建议接口：

```python
class WsRuntime:
    def start(self) -> None: ...
    def stop(self) -> None: ...
    def poll_events(self, max_events: int = 1000) -> list[dict]: ...
    def health_snapshot(self) -> dict: ...
```

### 9.3 WS 重连后的补偿

WS 从 disconnected 到 connected：

```python
gap_start_ms = state["runtime_health"].get("ws_gap_start_ts", 0) * 1000
replay_from_ms = min(gap_start_ms, now_ms - ws_replay_window_sec * 1000)
state["actions_replay_from_ms"] = max(0, replay_from_ms)
state["runtime_health"]["needs_light_resync"] = True
state["runtime_health"]["needs_full_reconcile"] = True
```

其中全量 reconcile 仍受 30 分钟节流。

## 10. 配置项建议

新增配置：

```json
{
  "network_soft_fail_enabled": true,
  "network_degraded_pause_buys": true,
  "network_reconnect_backoff_initial_sec": 3,
  "network_reconnect_backoff_max_sec": 60,
  "network_recover_success_rounds": 1,
  "network_light_resync_on_recover": true,
  "network_full_reconcile_on_recover": true,
  "network_full_reconcile_min_interval_sec": 1800,
  "network_order_unknown_safe_mode_sec": 180,
  "network_order_unknown_fatal_sec": 900,
  "recovery_actions_replay_window_sec": 1800,
  "ws_enabled": false,
  "ws_heartbeat_timeout_sec": 30,
  "ws_replay_window_sec": 1800,
  "ws_reconnect_backoff_max_sec": 60
}
```

和现有配置关系：

- `hemostasis_recovery_enabled` 继续控制全量止血恢复是否允许。
- `hemostasis_recovery_window_sec` 继续作为扫描目标 SELL 的窗口。
- `actions_replay_window_sec` 如果为 0，建议 recovery 使用 `recovery_actions_replay_window_sec` 单独控制。
- `api_timeout_sec` 保持当前 30 秒。

## 11. 恢复流程

### 11.1 网络失败阶段

1. 组件调用失败。
2. `classify_error()` 判定为 transient。
3. `runtime_health.mode = degraded`。
4. 记录 last_error。
5. 设置 `buy_paused_until`。
6. 设置 `actions_replay_from_ms`。
7. 本轮不推进 cursor。
8. 保存 state。
9. 继续下一轮尝试。

### 11.2 网络恢复阶段

1. 关键组件调用成功。
2. `runtime_health.mode = resyncing`。
3. 轻量 resync：
   - 同步远端 open orders。
   - 合并本地 managed orders。
   - 拉取我的仓位。
   - 拉取我的成交。
   - 刷新 target actions replay window。
4. 全量 reconcile 判断：
   - `now_ts - last_full_reconcile_ts >= 1800`
   - `hemostasis_recovery_enabled == true`
5. 若满足，调用 hemostasis recovery。
6. 清理 degraded 标记。
7. `runtime_health.mode = running`。
8. 保存 state。

### 11.3 长时间网络不可用

主备线路都不可用时：

- 进程不退出。
- mode 保持 `reconnecting` 或 `safe_mode`。
- 不做新 BUY。
- 不推进 cursor。
- 周期性重试。
- 外部监控只报告“等待网络恢复”。

但如果存在“下单请求已经发出，返回结果未知”的情况：

- 进入 `order_state_unknown`。
- 立即暂停所有新交易。
- 持续用 open orders、my trades、my positions 校验。
- 超过 `network_order_unknown_fatal_sec` 仍无法确认时，报警并建议人工介入。

## 12. 代码改造清单

### 12.1 新增文件

- `ct_runtime_health.py`
  - 错误分类。
  - runtime health 状态机。
  - buy pause 判断。
  - resync request 消费。

- `ct_recovery.py`
  - 轻量 resync。
  - recovery 版 hemostasis wrapper。
  - 30 分钟节流。

- `ct_ws.py`
  - 如果 WS 模块尚不存在，则新增。
  - 如果 WS 模块已存在但未同步到仓库，则把它接入 `ct_runtime_health.py`。

### 12.2 修改文件

- `ct_state.py`
  - 增加 `runtime_health` 默认字段。
  - 增加兼容修复。

- `copytrade_run.py`
  - 初始化 health。
  - 在 Data API、CLOB、WS 关键调用处记录 success/failure。
  - degraded 下暂停 BUY。
  - 恢复边缘触发 light resync/full reconcile。
  - 把 `_run_hemostasis_recovery_startup()` 拆成可复用 wrapper。

- `ct_exec.py`
  - 下单、取消、盘口错误接入统一分类。
  - 对 unknown order result 增加状态记录。

- `ct_clob_gateway.py`
  - CLOB read/write 错误返回分类信息。
  - `fetch_open_orders_norm_v2()` 保留 `(orders, ok, err)`，并可扩展返回 `err_kind`。

- `persistent_copytrade_runner.py`
  - 不处理普通网络异常。
  - 可选：把 child 最后 health 快照写入 session JSON，方便外部 status 读取。

- `windows/entrypoints/multi_node_5.ps1`
  - `status` 输出 runtime health 摘要。
  - 外部 stop 条件从“网络 ERROR 频率”改为“fatal 或长期 order_state_unknown”。

### 12.3 测试文件

新增：

- `tests/test_runtime_health.py`
- `tests/test_network_recovery_resync.py`
- `tests/test_recovery_hemostasis_throttle.py`
- `tests/test_ws_reconnect_replay.py`
- `tests/test_order_unknown_safe_mode.py`

复用/回归：

- `sandbox_restart_recovery_smoke.py`
- `tests/test_must_exit_recovery.py`
- `tests/test_clob_gateway_reads.py`
- `tests/test_clob_gateway_writes.py`
- `tests/test_pusd_preflight.py`
- `tests/test_fixes_56.py`

## 13. 测试方案

### 13.1 单元测试

覆盖：

- `WinError 10054` 分类为 transient。
- timeout / connection reset / 429 / 5xx 分类为 transient。
- pUSD / allowance / auth / credential 分类为 preflight。
- market closed / no orderbook / FAK no match 分类为 market_state。
- RuntimeError / unexpected traceback 分类为 fatal。
- degraded -> resyncing -> running 状态迁移。
- full reconcile 30 分钟节流。

### 13.2 集成测试

场景：

1. `fetch_open_orders_norm()` 连续失败两轮后恢复：
   - 进程不退出。
   - BUY 暂停。
   - 恢复后执行 light resync。

2. target actions API 失败后恢复：
   - cursor 不推进。
   - `actions_replay_from_ms` 被设置。
   - 恢复后 seen ids 去重生效，不重复买入。

3. WS 断开 5 分钟后重连：
   - mode 进入 degraded。
   - 重连后 REST replay 覆盖 gap。
   - SELL 信号能进入 must_exit。

4. 网络恢复 30 分钟内多次抖动：
   - light resync 多次执行。
   - full reconcile 只执行一次。

5. 已发送订单但响应未知：
   - 进入 order_state_unknown。
   - 不继续新交易。
   - open orders 或 trades 确认后恢复。

### 13.3 纸面演练

用日志校验这些关键行：

- `[HEALTH] mode=degraded component=... kind=transient`
- `[HEALTH] recovered component=...`
- `[RECOVERY] light_resync begin/end`
- `[RECOVERY] full_reconcile skipped reason=throttled`
- `[HEMOSTASIS_SUMMARY] ...`
- `[HEALTH] mode=running`

## 14. 上线步骤

### Phase 1: 只加观测，不改交易行为

- 新增 `ct_runtime_health.py`。
- 接入 Data API 和 CLOB open orders 的 success/failure 记录。
- state 写入 `runtime_health`。
- 外部 status 能看到 mode。

验收：

- 网络 ERROR 出现时，日志显示 degraded。
- 现有交易行为不变。

### Phase 2: degraded 暂停新 BUY

- 把 runtime health 接入现有买入过滤区。
- degraded 时只冻结 BUY，不影响已有 safe skip。

验收：

- 模拟网络失败时，BUY 被 hold，SELL 逻辑不被新买入干扰。

### Phase 3: 恢复后 light resync

- 新增 `ct_recovery.py` 的 light resync。
- 恢复边缘自动触发。

验收：

- CLOB/Data 恢复后能自动同步 open orders、my positions、my trades。

### Phase 4: 恢复后 full reconcile

- 抽出 hemostasis recovery wrapper。
- 增加 `last_full_reconcile_ts` 节流。

验收：

- 30 分钟内多次断线只全量恢复一次。
- 有目标 SELL 的 token 能进入 must_exit/补卖。

### Phase 5: WS 接入

- 新增或接入 WS adapter。
- WS reconnect 触发 REST replay。

验收：

- WS 断线不退出。
- 重连后不漏断线期间的 SELL。

### Phase 6: 外部监控规则调整

- heartbeat 不再因为普通网络 ERROR 执行 stop。
- status 输出 degraded/reconnecting/safe_mode。
- 只有 fatal/preflight/order_state_unknown 超时才停机或要求人工介入。

## 15. 验收标准

上线后满足：

- 主备网络同时断开时，V5 进程保持存活。
- 网络恢复后自动回到 running。
- 恢复后 light resync 必执行。
- full reconcile 最多 30 分钟一次。
- 普通 `WinError 10054`、timeout、`Server disconnected` 不触发停机。
- pUSD preflight failed、CLOB init failed、RuntimeError 仍触发停机或不可交易状态。
- replay 不重复买入。
- 断线期间目标 SELL 能通过 replay/must_exit/hemostasis 补上。
- 外部 `multi_node_5 status` 能看到每个节点 runtime health。

## 16. 开发注意事项

- 不要在恢复时重置 `bootstrapped`。
- 不要清空 `seen_action_ids`、`seen_trade_ids`。
- 不要把完整 startup boot 当成断线恢复逻辑。
- 不要因为 open orders 一次失败删除本地 open_orders。
- 不要在 Data API incomplete 时把 target missing 当成目标清仓。
- 不要让 WS 成为唯一事实源。
- 任何“订单是否已成交/已挂出”不确定时，宁可暂停新交易，也不要继续叠加订单。

## 17. 最终推荐方案

采用“进程常驻 + 状态降级 + 恢复校准”的方案：

1. 短暂网络错误只进入 degraded，不停机。
2. degraded 期间暂停新买入，不推进不可靠 cursor。
3. 网络恢复后先 light resync。
4. 再按 30 分钟节流运行 hemostasis full reconcile。
5. WS 负责实时，REST/Data API 负责断线补偿。
6. 外部监控从“错误即 stop”调整为“读 health 后分级处置”。

这个方案能最大化复用现有代码，改动集中在 health/recovery 两个新模块和主循环接入点上，同时避免网络抖动导致程序频繁停机，也降低断线期间漏卖、重复买入、订单状态不明继续交易的风险。
