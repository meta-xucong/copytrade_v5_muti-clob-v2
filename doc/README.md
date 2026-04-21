# CLOB V2 迁移文档索引

## 文档目的
本目录用于沉淀 `copytrade_v5_muti` 从 Polymarket CLOB V1 迁移到 CLOB V2 的正式工程文档。

本轮只产出规划与协作规范，不直接修改业务代码。目标是先把需求、差异、技术设计、实施顺序、测试方案和 cutover 操作手册写清楚，再进入分阶段落地。

## 当前状态
- 状态：规划文档已建立，等待后续按阶段实施。
- 范围：仅覆盖 `D:\AI\copytrade_v5\POLY_SMARTMONEY\copytrade_v5_muti`。
- 代码基线：现有测试为 `53 passed`。
- 现状限制：当前目录不是 Git 仓库根目录，正式实施前需要明确版本管理与回滚方式。

## 建议阅读顺序
1. [01-requirements-baseline.md](./01-requirements-baseline.md)
2. [02-gap-analysis-v1-to-v2.md](./02-gap-analysis-v1-to-v2.md)
3. [03-technical-design.md](./03-technical-design.md)
4. [04-implementation-roadmap.md](./04-implementation-roadmap.md)
5. [05-test-plan-and-cutover-runbook.md](./05-test-plan-and-cutover-runbook.md)
6. [agents.md](./agents.md)

## 各文档用途
- `01-requirements-baseline.md`
  记录业务目标、现状基线、代码耦合面、已确认事实、非目标。
- `02-gap-analysis-v1-to-v2.md`
  对照官方 V2 迁移要求和本项目当前实现，明确差距、影响面和优先级。
- `03-technical-design.md`
  定义未来代码改造的目标架构、模块职责、接口边界、缓存与配置策略。
- `04-implementation-roadmap.md`
  将迁移拆成正式工程阶段，定义里程碑、交付物、入口条件、退出条件。
- `05-test-plan-and-cutover-runbook.md`
  规定测试矩阵、联调步骤、预发布验证、正式切换当天的执行手册和异常处置。
- `agents.md`
  约束未来 AI 或人工协作者的工作方式，避免偏离方案。

## 关键结论速览
- 当前程序显式依赖 V1 Python SDK：`py_clob_client`。
- `copytrade_run.py` 负责主流程与客户端初始化，`ct_exec.py` 负责下单/撤单/挂单查询，`ct_resolver.py` 负责市场元数据解析，`ct_state.py` 负责状态持久化。
- 当前交易链包含 V1 特有逻辑：`/fee-rate` 查询、`fee_rate_bps` 注入、V1 `ClobClient` 初始化方式。
- 本轮方案不集成自动 `wrap()`，只定义“人工准备 pUSD + 程序前置校验”的流程。
- Builder 能力默认关闭；若未来启用，只允许使用 V2 `builderCode`，不恢复旧 HMAC 方案。

## 外部参考
- Polymarket 官方迁移文档：<https://docs.polymarket.com/v2-migration>
- Polymarket Contracts：<https://docs.polymarket.com/resources/contracts>
- Polymarket USD（pUSD）：<https://docs.polymarket.com/concepts/pusd>
- Polymarket Python CLOB Client V2：<https://github.com/Polymarket/py-clob-client-v2>

## 日期核验说明
在本次调研中，搜索摘要曾出现 `April 22, 2026`，但官方迁移页正文当前显示 cutover 时间为 `April 28, 2026 (~11:00 UTC)`。本目录所有文档均以后者为准，并要求后续执行前再次核验官方正文与状态页。
