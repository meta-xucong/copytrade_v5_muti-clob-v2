# 01 需求与基线

## 1. 文档目标
定义本项目迁移到 Polymarket CLOB V2 的业务目标、现状边界、代码基线和非目标，作为后续设计与实施的输入。

## 2. 业务目标
- 保持现有跟单策略的核心行为不变：
  目标地址动作识别、仓位同步、风控限额、退出逻辑、多账户轮转、状态持久化。
- 将交易接入层从 CLOB V1 平稳迁移到 CLOB V2。
- 在迁移过程中遵循正式工程流程，而不是一次性大改：
  先文档，后模块化改造，再预发布联调，最后 cutover。
- 将未来改造范围控制在“必要的协议迁移 + 可验证的工程收口”内，不把额外资金管理功能混入同一轮。

## 3. 明确不在本轮实现的内容
- 不直接修改业务代码。
- 不把 USDC.e 到 pUSD 的自动 `wrap()` 集成到主跟单程序。
- 不恢复或兼容旧 Builder HMAC 方案。
- 不设计“一套代码同时长期支持 V1 和 V2”的双栈运行方案。
- 不引入一次性大规模重写，也不更换现有策略逻辑。

## 4. 当前代码基线

### 4.1 代码入口与职责
- `copytrade_run.py`
  主循环、配置加载、账号初始化、目标地址动作拉取、状态调度、客户端初始化。
- `ct_exec.py`
  下单、撤单、挂单查询、盘口读取、订单执行与重试、费用处理。
- `ct_data.py`
  数据 API 拉取，包含目标账户持仓、交易、动作采集。
- `ct_resolver.py`
  Gamma/CLOB 市场元数据查询、市场可交易状态判断、`token_id` 解析。
- `ct_state.py`
  状态文件默认结构、读写兼容。
- `bounded_copytrade_runner.py`
  有界时长运行器。
- `persistent_copytrade_runner.py`
  常驻监督运行器。
- `windows/*.ps1|*.bat`
  Windows 下的启动、停止、状态查看脚本。

### 4.2 已确认的事实
- 当前实现显式依赖 V1 Python SDK：`py_clob_client`。
- 当前本地环境未安装 `py_clob_client_v2`。
- 当前目录不是 Git 仓库根目录；`git status` 无法使用。
- 当前仓库测试基线为 `53 passed in 5.16s`。

### 4.3 当前 V1 耦合点
- `copytrade_run.py`
  使用 `py_clob_client.client.ClobClient` 初始化客户端，并调用 `create_or_derive_api_creds()`。
- `ct_exec.py`
  直接从 `py_clob_client.clob_types` 导入 `OrderArgs`、`MarketOrderArgs`、`OpenOrderParams`。
- `ct_exec.py`
  存在 `_resolve_order_fee_rate_bps()`，通过 `GET /fee-rate` 查询费用并缓存。
- `ct_exec.py`
  在 `place_order()`、`place_market_order()` 中向 V1 `OrderArgs` / `MarketOrderArgs` 传 `fee_rate_bps`。
- 测试文件 `tests/test_fee_rate_bps.py`
  明确验证了 V1 的 `fee_rate_bps` 传递路径。

## 5. 官方 V2 迁移要求摘要

### 5.1 协议与 SDK
- SDK 从 `py-clob-client` 迁移到 `py-clob-client-v2`。
- V2 的订单签名结构移除了 `nonce`、`feeRateBps`、`taker`，新增 `timestamp`、`metadata`、`builder`。
- API 认证的 L1/L2 逻辑保持不变。

### 5.2 费用模型
- 费用不再嵌入签名订单。
- 平台 fee 由协议在撮合时处理。
- 市场参数查询改为 `getClobMarketInfo()`。
- 市价买单可选传 `userUSDCBalance`，用于 fee-aware fill 计算。

### 5.3 Builder
- 旧 `POLY_BUILDER_*` 头和 `builder-signing-sdk` 被移除。
- 未来若启用，仅通过 `builderCode` 注入。

### 5.4 抵押资产
- 抵押资产从 USDC.e 迁移到 pUSD。
- API-only 交易者需要自行完成 USDC.e 到 pUSD 的准备流程。

### 5.5 Cutover 行为
- 官方迁移页正文当前显示 cutover 时间为 `April 28, 2026 (~11:00 UTC)`。
- 迁移期间约有 1 小时停机窗口。
- 所有 open orders 会被清空，迁移完成后必须重建。

## 6. 本项目的迁移目标范围

### 6.1 必须完成
- 客户端初始化迁移到 V2。
- 下单、撤单、查单、盘口读取统一收口到 V2 交易适配层。
- 删除手动 fee-rate 处理逻辑。
- 将市场最小下单量、tick size、费用参数等查询口径对齐到 V2。
- 设计并记录 pUSD 前置校验流程。
- 更新运行器与 Windows 启动脚本的迁移注意事项。

### 6.2 本轮只出文档，不落代码
- 文档需要达到“后续执行者可以直接按文档实施”的程度。
- 每个阶段都要有验收标准和退出条件。

## 7. 约束条件
- 当前策略逻辑复杂，不能用一次性重写替代逐步迁移。
- 需要保留现有状态文件兼容性，至少在迁移期允许旧状态被读取。
- 正式 cutover 后不能回滚到 V1 继续运行，因为官方不再兼容 V1 SDK。
- 由于当前目录缺少 Git 元信息，实施阶段必须补充版本化约束。

## 8. 风险基线
- 高风险：V1 订单参数和 fee 逻辑失效，导致无法下单。
- 高风险：cutover 后旧 open orders 被清空，若程序未准备好重建逻辑，会出现空窗。
- 中风险：pUSD 未准备完成，出现 `balance / allowance` 类错误。
- 中风险：市场元数据字段变化导致风控、最小下单量或 tick size 判断失真。
- 中风险：运行器和启动脚本未同步更新，导致主程序虽完成迁移但运维入口仍失败。

## 9. 基线结论
本项目不是“简单替换一个 SDK 包名”，而是需要同步重构交易适配层、费用逻辑、市场参数获取口径、资产准备流程提示和 cutover 运行手册。后续执行必须以“适配层收口 + 文档先行 + 阶段化验证”为核心原则。
