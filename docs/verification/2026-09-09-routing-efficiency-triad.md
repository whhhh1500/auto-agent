# 路由效率三臂实测（第 41 波）

状态：有限开发证据，未通过稳定路由或全局效率验收。本记录只描述一次匹配三臂实验，不能据此排名全局最优策略。

## 范围、证据与计量边界

`TestLiveRoutingEfficiencyTriad` 使用真实 Responses 端点、受保护的只读合成工具和相同的八项不透明 fixture。每个请求模型只运行 **1 个 triad**；每个 triad 串行运行 `sequential_react`、`parallel_react` 与 `ptc` 三臂。三臂共用任务、数据、四工具菜单、10 轮和 11 次工具调用上限；唯一臂差异是持久 profile 中的路由指令。测试源码为 `examples/programmatic/live_routing_efficiency_test.go`。

私有、不可提交的审计证据位于 `.codex-v46-audit/programmatic-20260909/optimization-wave-41/`：

- 安全汇总：`safe-summary.json`，SHA-256 `6f4f4c0d75db74a9b105e676d2273462a1cac3ef8911c5f7e0ffa1adaa587216`。
- 冻结测试二进制：`routing-efficiency.test.exe`，SHA-256 `09d210e7bdd1b2d36ca830b3c7375a3250211ce73ef1cb0384fd50e677e6785e`。
- 逐模型日志：`terra.log`、`luna.log`；汇总 schema 为 `harness.programmatic.routing-efficiency-summary/v1`，协议为 `responses`。

汇总明确记录 `triads_per_model: 1` 和 `retry_count: 0`。逻辑合同失败会保留证据并继续后续已计划臂；HTTP、传输或 Responses 协议失败会停止该模型余下工作，也不会重试。本波没有把失败样本重跑到通过。

表中 token 是 provider 报告的 usage，并经过该样本的本地 invocation/ledger 对账；它不是网关后端身份、独立账单或计费金额的证明。请求模型名只是发往兼容网关的配置名，不能独立认证实际服务的模型身份。

## 结果

| 请求模型 | 臂 | 运行状态 / 验收 | 模型轮次 | 已报告输入 / 输出 token | 事实 |
| --- | --- | --- | ---: | ---: | --- |
| `gpt-5.6-terra` | `sequential_react` | limited / FAIL | 4 | 21,061 / 1,213 | 强制顺序指令未被遵循：先走了 catalog/execute，后才 inventory。 |
| `gpt-5.6-terra` | `parallel_react` | completed / FAIL | 10 | 47,857 / 785 | 强制批量指令未被遵循：详情被逐项提交，耗尽十轮合同。 |
| `gpt-5.6-terra` | `ptc` | completed / PASS | 3 | 14,994 / 745 | catalog、一次 execute、九个程序子工具调用和最终答案都满足合同。 |
| `gpt-5.6-luna` | `sequential_react` | completed / FAIL | 3 | 13,884 / 722 | 顺序臂实际把独立详情批量提交，因不符合顺序合同失败。 |
| `gpt-5.6-luna` | `parallel_react` | completed / PASS | 3 | 13,874 / 723 | inventory 后的一个回复提交全部八个 detail，满足合同。 |
| `gpt-5.6-luna` | `ptc` | failed / FAIL | 2 | unknown | 首轮忽略 PTC 指令而调用 inventory；第二轮 HTTP 429。首轮仅报告 4,140 / 92 token，第二轮没有完整 usage，故 triad 总量未知，不能写为 0 或 4,232。 |

测试的验收不把 `Runtime` 的 `completed` 当作成功：它还要求每项工具效果恰好一次、正确且持久的最终答案、完整 usage 对账，以及该臂所要求的调用形状。Terra 的两个受强制直接路由臂均失败、PTC 通过；Luna 的顺序臂因实际批量失败、并行臂通过、PTC 因首轮不遵从和后续 429 失败。

所有真实 Requests 都观察到四工具菜单、存在的 instructions 以及 `parallel_tool_calls: true`。这只说明该提示片段和请求 flag 已到达该次协议请求；它不保证模型遵守指令，也不等于宿主获得稳定、可自动发布的路由。模型同一回复可提交多个调用也不改变宿主逐项执行权限、预算、审批和 journal 保护的语义。

本波没有未参与调优的任务、受限重复样本、跨网关身份认证或完整的 PTC 失败用量。因此，不能由 Terra 的 PTC 或 Luna 的 parallel 成功宣称任一策略在任务分布、模型或网关间全局最优，也不能将两个请求模型作能力排序。

## 后续状态

第 42 波已经实现共享 resolver、同一 Run 首动作锁定以及真实投影菜单验收。结果与原始假阴性边界见[强制执行路由真实模型验收](2026-09-09-enforced-routing-live.md)。离线候选、留出评测、canary 和人工提升仍是后续控制面工作。

## 第 41 波提出的架构方向

下一步应把真实观测与线上执行分离：

1. 宿主先以确定性 eligibility 规则筛掉不具备前提的路线，例如依赖形状、已知能力、风险、预算和授权边界。
2. 在同一 Run 中，模型的第一个合格动作只用于在候选中作一次选择；随后由宿主自动锁定该路线，避免每轮提示或 flag 重新争夺路由。
3. 新候选只在离线数据与评测中产生和验证，再经 canary 收集证据，最后由人工审阅并提升到发布控制面。

该闭环不允许线上根据单次模型输出自行改写策略、写入长期经验或自动晋级。失败、429、usage 不完整或缺少留出验证的记录应保持候选或未知状态，不能成为自动路由规则。
