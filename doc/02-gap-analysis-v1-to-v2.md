# 02 V1 到 V2 差异分析

## 1. 目的
本文件用于把“官方要求”和“当前代码现状”逐项对齐，明确每一处差距、影响模块和迁移优先级。

## 2. 差异总表

| 主题 | 当前实现 | V2 要求 | 影响等级 | 结论 |
| --- | --- | --- | --- | --- |
| SDK 包 | `py_clob_client` | `py-clob-client-v2` | 高 | 必须替换 |
| 客户端初始化 | V1 `ClobClient(host, chain_id, key, creds, signature_type, funder, ...)` 风格 | V2 SDK 接口 | 高 | 必须收口到统一适配层 |
| 下单参数 | `fee_rate_bps` 由本地填充 | `feeRateBps` 不再由用户传入 | 高 | 必须移除 |
| 市价单参数 | 未传 `userUSDCBalance` | 可选支持 `userUSDCBalance` | 中 | 设计为可扩展能力 |
| 手动 fee 查询 | `GET /fee-rate` + 本地缓存 | 优先使用 `getClobMarketInfo()` / SDK 自动处理 | 高 | 必须删除旧逻辑 |
| Builder | 当前未见启用 | 未来若启用仅 `builderCode` | 中 | 保持关闭，方案中预留 |
| 抵押资产 | 现有代码未显式管理 pUSD | 需要以 pUSD 作为交易抵押资产 | 高 | 需加入前置校验与运行手册 |
| 订单重建 | 当前未针对 cutover 单独设计 | open orders 将在 cutover 被清空 | 高 | 必须加入 runbook |
| 市场元数据 | 依赖 Gamma/CLOB 现有字段缓存 | 需要补充 V2 市场信息口径 | 高 | 必须设计新数据来源 |
| 启动脚本 | 基于当前 V1 环境假设 | 需检查 V2 依赖与切换前置项 | 中 | 必须补文档和实施任务 |

## 3. 逐项差异分析

### 3.1 SDK 与客户端初始化
当前现状：
- `copytrade_run.py` 直接从 `py_clob_client.client` 导入 `ClobClient`。
- 运行逻辑直接在主流程中创建客户端并设置 API creds。

V2 要求：
- 切换到 `py-clob-client-v2`。
- 主业务代码不应继续直接依赖 SDK 的构造细节。

结论：
- 未来必须新增统一交易网关模块，例如 `ct_clob_gateway.py`。
- 所有客户端初始化、认证、下单、撤单、查单、盘口、市场参数查询都通过该网关暴露。
- `copytrade_run.py` 只依赖本地网关，不直接依赖 Polymarket SDK。

### 3.2 下单接口与订单结构
当前现状：
- `ct_exec.py` 使用 V1 `OrderArgs` / `MarketOrderArgs`。
- `place_order()` 和 `place_market_order()` 显式向订单参数注入 `fee_rate_bps`。
- 业务层对 V1 参数结构有感知。

V2 要求：
- 移除 `feeRateBps`、`nonce`、`taker` 的用户传入。
- 新增 `builderCode` 可选字段。
- SDK 负责 V2 订单结构处理。

结论：
- `ct_exec.py` 中所有直接构造 SDK 订单参数的逻辑必须未来迁移到网关内部。
- 上层业务动作仍保留统一动作格式：
  `type/token_id/side/price/size/_taker/...`
- 业务层不关心 V2 原始订单字段。

### 3.3 费用模型
当前现状：
- `ct_exec.py` 有 `_resolve_order_fee_rate_bps()`。
- 会请求 `https://clob.polymarket.com/fee-rate`。
- `tests/test_fee_rate_bps.py` 明确验证这条链路。

V2 要求：
- 费用由协议在撮合时处理。
- 集成侧不再手工注入 `feeRateBps`。
- 应改用 `getClobMarketInfo()` 获取市场参数。

结论：
- `_resolve_order_fee_rate_bps()` 及对应缓存将被删除。
- `tests/test_fee_rate_bps.py` 将在实施阶段被重写为“市场参数适配 / SDK 自动 fee 行为”的测试。
- 所有依赖“本地先取 fee 再签单”的逻辑都视为 V1 历史逻辑。

### 3.4 市场元数据与缓存
当前现状：
- `market_status_cache` 中被使用的字段包括：
  `feesEnabled`、`orderMinSize`、`orderPriceMinTickSize`、`acceptingOrders`、`enableOrderBook`。
- `ct_resolver.py` 主要从 Gamma / sampling-markets 拉市场状态。
- `ct_exec.py` 用缓存字段参与 tick size 与最小下单量判断。

V2 要求：
- `getClobMarketInfo()` 成为 CLOB 级市场参数的正式来源，返回 `mts`、`mos`、`fd`、`t`、`rfqe` 等。
- 费用与市场交易参数不应继续只依赖旧缓存字段。

结论：
- 后续设计中需要把“市场状态缓存”和“市场交易参数缓存”拆开。
- `market_status_cache` 保留用于 tradeable/closed/open-order-book 判断。
- 新增 `market_info_cache`，用于 V2 `getClobMarketInfo()` 的结果缓存。

### 3.5 pUSD 与余额前置
当前现状：
- 代码中没有显式的 pUSD / onramp / wrap 流程。
- 只有通用的余额不足 / allowance 不足错误处理。

V2 要求：
- 交易抵押资产为 pUSD。
- API-only 交易者要自行完成资金准备。

结论：
- 本轮不把自动链上 `wrap()` 加入主程序。
- 未来代码只增加“前置校验”和“错误提示增强”：
  发现余额/allowance 问题时优先提示检查 pUSD 资金准备。
- 运行手册中必须单列资金准备步骤。

### 3.6 Builder 能力
当前现状：
- 未发现 `POLY_BUILDER_*` 或 builder HMAC 实装。
- 当前系统可以视为“未启用 builder”。

V2 要求：
- 旧 builder HMAC 方案废弃。
- 若启用，则通过 `builderCode`。

结论：
- 当前迁移按“默认关闭 builder”设计。
- 技术设计里只预留可选配置项：
  `poly_builder_code` 或 `POLY_BUILDER_CODE`。
- 未启用时默认不传 builder 字段。

### 3.7 Cutover 与订单簿清空
当前现状：
- 现有逻辑有 open order 管理、adopt existing orders、state 持久化。
- 但没有针对“官方统一清空 open orders”的专项 runbook。

V2 要求：
- cutover 后 open orders 不迁移，必须重新下。

结论：
- 需要在 runbook 中规定：
  cutover 前暂停程序；
  cutover 后主动刷新远端挂单视图；
  清理本地挂单影子状态；
  再恢复主循环。

### 3.8 启动器与运维入口
当前现状：
- 有 `bounded_copytrade_runner.py`、`persistent_copytrade_runner.py` 和 `windows` 脚本。
- 脚本目前不校验 V2 依赖是否具备。

V2 要求：
- 运维入口必须与新依赖和 cutover 流程一致。

结论：
- 运维入口也属于迁移范围。
- 实施阶段必须增加依赖检查、环境检查和切换日操作说明。

## 4. 未来需删除或重构的 V1 专项内容
- `ct_exec.py::_resolve_order_fee_rate_bps()`
- `ct_exec.py` 中所有 `fee_rate_bps` 参数透传
- `tests/test_fee_rate_bps.py` 的 V1 断言模型
- `copytrade_run.py` 中直接依赖 V1 SDK 构造与 API creds 派生细节的代码

## 5. 未来需新增的能力
- 统一 CLOB V2 交易网关
- V2 市场信息缓存
- pUSD 前置校验与错误提示
- cutover 清单与状态清理步骤
- 运行器/脚本的依赖自检

## 6. 差异分析结论
真正的迁移边界不是“替换 SDK”，而是：
- 交易接入层重构
- 费用链路删除与替换
- 市场参数缓存口径升级
- 资产准备流程显式化
- cutover 操作制度化

只要这五项没有同时完成，程序就不能算完成了 CLOB V2 迁移。
