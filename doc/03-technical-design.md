# 03 技术设计

## 1. 设计目标
- 将 Polymarket 交易接入细节从业务逻辑中剥离。
- 以最小改动保持现有跟单策略和状态机行为不变。
- 消除 V1 fee 注入逻辑和 SDK 直连耦合。
- 为后续联调、cutover 和长期维护提供稳定边界。

## 2. 目标架构

### 2.1 模块划分
后续代码实施时新增或调整以下模块边界：

- `ct_clob_gateway.py`
  唯一允许直接依赖 `py_clob_client_v2` 的模块。
- `ct_exec.py`
  保留业务级执行器角色，只负责动作编排、重试策略、风控协同、错误归类。
- `copytrade_run.py`
  只负责主循环、配置、状态、调度，不直接使用 SDK 类型。
- `ct_resolver.py`
  保留市场可交易状态与 `token_id` 解析职责，不承担 V2 费用参数读取。
- `ct_state.py`
  保留状态文件读写；新增 V2 缓存默认字段时统一在此处理兼容。

### 2.2 新增模块职责
`ct_clob_gateway.py` 统一暴露以下接口：

- `init_client(account_cfg, runtime_cfg) -> gateway_client`
  初始化 V2 客户端并完成 API creds 绑定。
- `get_orderbook(client, token_id, timeout) -> normalized_book`
  返回统一格式的 `best_bid/best_ask/...`。
- `get_open_orders(client, timeout) -> normalized_orders`
  返回统一格式的挂单列表。
- `place_limit_order(client, *, token_id, side, price, size, allow_partial, builder_code, timeout) -> result`
  下 maker 单。
- `place_market_order(client, *, token_id, side, amount, price, order_type, user_usdc_balance, builder_code, timeout) -> result`
  下 taker 单。
- `cancel_order(client, order_id, timeout) -> result`
  撤单。
- `get_market_info(client, condition_id, timeout) -> normalized_market_info`
  查询 V2 `getClobMarketInfo()` 并标准化返回。
- `preflight_collateral(client, runtime_cfg, account_state) -> check_result`
  做 pUSD 前置检查或错误归类支撑。

业务代码禁止绕过网关直接导入 V2 SDK 类型。

## 3. 统一数据契约

### 3.1 业务动作结构
现有 `actions` 数据结构继续保留，不因为迁移改写：
- `type`
- `token_id`
- `side`
- `price`
- `size`
- `_taker`
- 其余业务辅助字段

原因：
- 可以最大限度减少策略层和风控层改动。
- 让迁移集中在接入层而不是策略层。

### 3.2 订单结果结构
网关返回的统一结果格式固定为：
- `order_id`
- `response`
- `status`
- `error_code`
- `error_message`

若上游 SDK 未提供某字段，网关负责归一化。

### 3.3 市场信息缓存结构
在状态文件中新增 `market_info_cache`，键为 `condition_id`。

缓存值固定为：
- `ts`
- `condition_id`
- `info`
- `token_map`
- `min_tick_size`
- `min_order_size`
- `fee_rate`
- `fee_exponent`
- `taker_only_fee`
- `rfq_enabled`

用途分离：
- `market_status_cache`
  只管市场是否可交易、是否关闭、是否启用 order book。
- `market_info_cache`
  只管 V2 CLOB 参数。

## 4. 配置设计

### 4.1 保留项
以下现有配置默认保留：
- `poly_host`
- `poly_chain_id`
- `poly_signature`
- `api_timeout_sec`
- `allow_partial`
- `taker_order_type`
- `maker_only`
- 各类风控、节流、轮询、退出策略参数

### 4.2 新增项
实施阶段新增以下配置：
- `poly_sdk_version`
  固定值 `v2`，仅用于显式标记当前接入代际。
- `poly_builder_code`
  可选，默认空。
- `market_info_cache_ttl_sec`
  默认 `300` 秒。
- `require_pusd_ready`
  默认 `true`。
- `cutover_force_remote_refresh`
  默认 `true`，用于切换后强制刷新远端挂单与市场缓存。

### 4.3 删除项
后续代码实施中删除以下配置及隐式行为：
- 基于 `fee_rate_bps` 的任何配置或临时参数
- 任何为 V1 fee-rate 手动计算服务的逻辑开关

## 5. 订单与费用设计

### 5.1 Limit Order
`ct_exec.py` 只计算业务意图：
- 是否下单
- 下单方向
- 价格
- 数量
- maker/taker 选择

网关负责把业务意图映射到 V2 limit order API。

### 5.2 Market Order
对市价买单采用以下策略：
- 默认不强依赖 `userUSDCBalance`。
- 若网关能稳定获取可用余额，则填入。
- 若余额获取失败，不阻塞下单流程，但要记录 warning。

这是本轮明确的实现决策，避免执行者自行决定。

### 5.3 Fee 处理
设计口径固定如下：
- 业务层不再解析 `/fee-rate`。
- 不向订单参数手工写入 `fee_rate_bps`。
- `getClobMarketInfo()` 只用于风控、日志和诊断，不作为“签名前置计算”的输入。

## 6. pUSD 前置校验设计

### 6.1 本轮不做自动 wrap
不在主程序内加入：
- `wrap()`
- `approve()`
- 任何主动链上资金迁移流程

### 6.2 程序侧必须具备的能力
- 将“余额不足 / allowance 不足”错误归类为“优先检查 pUSD 准备状态”。
- 启动前允许执行一次非阻塞预检查：
  若检测能力不足，只输出提示，不中断程序。
- cutover 与运行手册中明确要求人工确认 pUSD 可用余额。

## 7. Builder 设计
- 默认不启用 builder。
- 若 `poly_builder_code` 为空，网关不传 `builderCode`。
- 若未来启用，只允许在网关层传 `builderCode`。
- 不允许重新引入 `POLY_BUILDER_API_KEY`、`POLY_BUILDER_SECRET`、`POLY_BUILDER_PASSPHRASE`、`POLY_BUILDER_SIGNATURE`。

## 8. 状态兼容设计

### 8.1 保持兼容读取
旧状态文件必须仍可被 `ct_state.py` 读取。

### 8.2 新增默认字段
未来在 `DEFAULT_STATE` 中新增：
- `market_info_cache`
- `cutover_rebuild_pending`
- `remote_order_snapshot_ts`

### 8.3 不保留 V1 fee 缓存
`_FEE_RATE_CACHE` 属于 V1 逻辑，实施时删除，不做迁移保留。

## 9. 运行器与脚本设计

### 9.1 运行器
`bounded_copytrade_runner.py` 和 `persistent_copytrade_runner.py` 后续必须增加：
- 启动前依赖检查
- 对 `py_clob_client_v2` 是否可导入的检查
- 对 `copytrade_config.json` 中新配置项的兼容检查

### 9.2 Windows 脚本
`windows/*.ps1|*.bat` 后续必须同步更新：
- 新环境准备说明
- 启动前自检提示
- cutover 日人工操作提醒

## 10. 错误处理设计
错误分类统一为四类：
- `transient`
  临时网络、服务抖动、引擎重启、超时。
- `preflight`
  配置缺失、SDK 不可导入、pUSD 未准备、builder code 非法。
- `market_state`
  市场关闭、盘口不可用、最小下单量不满足。
- `fatal`
  不可恢复的程序错误。

分类决策权统一在网关和执行层，不允许散落在主循环各处。

## 11. 实施后的文件级变化清单

### 11.1 预计新增
- `ct_clob_gateway.py`
- `tests/test_clob_gateway_v2.py`
- `tests/test_market_info_cache.py`
- `tests/test_pusd_preflight.py`
- `tests/test_cutover_rebuild.py`

### 11.2 预计改造
- `copytrade_run.py`
- `ct_exec.py`
- `ct_state.py`
- `bounded_copytrade_runner.py`
- `persistent_copytrade_runner.py`
- `windows/*.ps1|*.bat`

### 11.3 预计删改
- 删除 V1 fee-rate 查询逻辑
- 重写 `tests/test_fee_rate_bps.py`

## 12. 设计结论
技术方案采用“网关收口、业务动作不变、状态平滑兼容、资金准备不自动化”的迁移路径。这样可以把协议差异控制在有限模块内，同时保持策略层和风控层稳定，适合正式项目按阶段推进。
