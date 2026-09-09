# Agent 执行与编排重构：目标与验收台账

状态：本轮约定范围已完成。CodePTC 运行器依用户要求延后；真实模型选路质量与线上性能未评测，不宣称全局最优。

## 用户要求与边界

让 AI 根据当前任务和可用能力选择执行逻辑，打通 Agent 循环、编排、上下文以及工具/能力合同。当前 ReAct 风格循环继续可用；PTC 要提供真实的程序化工具调用；CodePTC 的代码工作区运行器明确延后，但必须保留路由和工具/能力暴露规范。

“最优”在本次工程中按可验证目标评估：简单任务不强制增加分类模型往返，复杂批量任务可以用程序处理工具结果，决策与执行证据一致，能力权限不能扩大，恢复不重复未知副作用，公开合同保持小而可扩展。没有对等评测时不宣称全局最优或性能领先。

## 决策与执行方向

模型的直接工具调用和程序执行请求应尽量复用同一个已有授权、用量与持久证据保护的模型回合。不得在 server 执行器解析阶段额外请求一个未纳入 model admission / outcome 记录的分类模型。

当前执行方式不是同一维度：ReAct 是反馈循环，PTC 是程序化动作，Workflow / Graph / Subagent 是任务组织能力。自动选择应允许模型在同一任务中按结果选择直接动作或程序动作，并调用宿主已注册的编排能力；不能把 Graph 的单节点包装伪装成自动多 Agent 规划。

Session、ToolJournal 和既有运行事实继续作为执行证据来源。新增描述、选择记录或任务视图只能明确引用这些事实，避免另建互相矛盾的执行状态。必要的新持久化事实须在实现前定义事务与恢复语义。

## 必须完成的验收项

| 要求 | 完成证据 | 当前状态 |
| --- | --- | --- |
| AI 自动选择直接执行或 PTC | 默认 general 的实际工具选择集与 program 能力接线；离线协议模型实际读取 catalog/bindings 后执行，简单/批量对照 | 协议已验证；真实模型选路质量未测 |
| PTC 真正执行程序 | `pkg/execution/programmatic`、`examples/programmatic` 实际 for/if/data/call；最终值与直接路径一致 | 已实现并验证 |
| 明确工具/能力程序化规范 | app/programmatic 投影与 corebridge；版本/Source/ProviderRevision/Schema 的 binding digest，公开 DTO 与拒绝测试 | 已实现并验证 |
| CodePTC 延后但保留接入能力 | runexecutor 预留 codeptc@1，app/programmatic.CodePTCToolAccess 使用共享 Descriptor；默认拒绝、显式选择无回退及未来工厂测试 | 已完成预留；运行器按要求延后 |
| 上下文支持执行策略 | 精简常驻说明、catalog 按需返回完整语法；默认 assembler/摘要器内部结果去重及滚动边界测试，真实 server 最终模型上下文配对 | 已实现并验证 |
| 统一授权和副作用保护 | accepted invocation + Invoker 唯一桥接；snapshot 过滤、精确 binding、稳定 child identity、root/child 共用预算测试 | 已实现并验证 |
| 可取消、有界资源使用 | VM 步数/全局循环/值预算/深度/取消、DAG 返回展开与数值边界；91,060 次有界 fuzz | 已实现并验证；分配预算不等于进程 RSS 隔离 |
| 暂停、恢复和未知结果处理 | SQLite/PG 的 Runtime 恢复及完整 native queued HTTP 审批→重建→同 run 续跑；unknown child 拒绝；既有 Native A/B/worker/FastRouter 真实硬杀兼容回归 | 已验证限定场景；不承诺任意 PTC 崩溃点重放 |
| 现有执行与编排可组合 | PTC→显式 workflow、进程内 subagent 的 guarded 身份测试；Graph 保留独立 executor 维度，不注册伪程序目标 | 已验证有限组合；不宣称 durable subagent/Graph 程序恢复 |
| 可审计评测 | 1/8 命中项对照：相同结果，累计请求 JSON、模型回合、journal 次数；本机三次 microbenchmark，详见验收记录 | 已完成离线协议测量；无厂商质量/费用结论 |
| 完整交付 | 接线、共享合同/预留文档、API 清单、示例、普通/race/PG/覆盖率/静态/凭据门禁，首次失败与修复均记录 | 已完成 |

## 已收敛的实现决策

1. PTC 语言与资源隔离：不能把解释器的步数限制等同于内存限制，必须覆盖巨大分配、递归与嵌套数据。
2. 程序审批恢复：只有可证明确定性的重建或真实检查点才可继续；工具调用身份须绑定程序及调用内容，不能只用执行序号。
3. 能力投影：公开工具 Schema 与精确版本可复用；不得向模型或程序泄漏 Manifest 的凭据和执行配置。
4. 默认接线与策略：避免新增未审计模型请求，也不能只增加 Prompt 而没有真实的 PTC 执行路径。

上述事项已由代码审核与测试收敛：受限 IR、有界值复制与严格 JSON；确定性子调用身份及 SQL 审批重建；仅公开目录投影；默认服务实际接线。按上表明确保留的边界，不实现通用 CodePTC 或任意崩溃点的 PTC 检查点。

本轮命令、测量及证据边界统一记录在 [程序化执行验收](../../verification/2026-09-09-programmatic-execution.md)。
