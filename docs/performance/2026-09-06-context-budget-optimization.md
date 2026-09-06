# 上下文工具预算与资源优化：实测记录

日期：2026-09-06；优化前源码基线 `d1e0bb2`，优化后为与本记录一同提交的代码。整体目标仍是更可靠、易扩展、占用更低、token 更省的 Agent 基础设施；这份记录只证明本次具体改动和实测范围，不是整体目标完成或跨框架性能领先声明。

## 本次改动解决了什么

1. **工具预算缺失**：原先 Assembler 完成后才读取 `Schemas()`，因此预算和 trace 用量漏掉工具声明。现在每个模型步骤只读取一次最终工具集合，作为 `ModelContext.Tools` 供估算器使用，同一集合发送给模型。工具成本占用必需的 `LayerTools` 预算；可选旧历史为它让出空间，工具或当前消息放不下时在请求模型前报错。
2. **重复校验成本**：`Assemble` 已验证消息，内置估算器又重复验证参数并执行 JSON 序列化。现在仅对已验证消息、确切的内置 estimator 类型走私有快速路径。公开方法和自定义扩展仍维持原校验/调用合同，避免通过嵌入内置类型绕过外部覆盖方法。
3. **估算扩展接口没有接到默认入口**：新增应用层 `ContextEstimator`，一次覆盖消息、提示片段与工具 Schema；`Config.Estimator` 可替换默认估算。没有把厂商 tokenizer、数据库或协议 SDK 引入内核。
4. **OTel 指标静默丢失**：内核已发送的 `harness.model.context.input_bytes`、`input_tokens`、`dropped_groups` 不在 OTel 固定指标表中。新增 SDK reader 回归先复现缺失，再补齐注册，并核对累计值。真实审计夹具也从 noop metrics 改为 SDK ManualReader。

设计边界与未完成的整体工作见[实施说明](../implementation/2026-09-06-context-cost-and-budget.md)。

## 本机性能前后对照

Windows amd64、i7-12700K、Go 1.25.13。单个 benchmark 顺序执行；每项前后各 5 个样本，`-benchtime=400ms -count=5 -benchmem -p 1`。两个版本使用相同新夹具文件，未让基准触发外部模型或数据库。此处 120 条普通历史和 40 组工具交互均未附带单独的工具声明，用于隔离“历史估算重复校验”的优化收益。

| 夹具 / 指标 | 优化前中位数 | 优化后中位数 | 变化 |
| --- | ---: | ---: | ---: |
| 普通历史耗时 | 141,001 ns/op | 139,332 ns/op | 约 -1.2%，视为基本相当 |
| 普通历史分配 | 43,432 B/op；172 allocs/op | 43,432 B/op；172 allocs/op | 不变 |
| 工具历史耗时 | 355,977 ns/op | 310,165 ns/op | **-12.87%** |
| 工具历史分配字节 | 128,367 B/op | 112,990 B/op | **-11.98%** |
| 工具历史分配次数 | 2,203 allocs/op | 1,763 allocs/op | **-19.97%** |

`B/op` 是每操作累计分配，不是进程 RSS。没有独占机器，没有证明长时间驻留内存下降，也没有比较其他框架。新增的工具集合快照/成本计算本身有成本；这张表没有将它混入旧夹具后宣称所有请求统一提速。

完整原始样本：

```text
# before: d1e0bb2 + same benchmark fixture
BenchmarkAssemblerDefaultHistory-20  3247  141001 ns/op   43433 B/op   172 allocs/op
BenchmarkAssemblerDefaultHistory-20  3481  140229 ns/op   43432 B/op   172 allocs/op
BenchmarkAssemblerDefaultHistory-20  3382  140222 ns/op   43432 B/op   172 allocs/op
BenchmarkAssemblerDefaultHistory-20  3324  143202 ns/op   43432 B/op   172 allocs/op
BenchmarkAssemblerDefaultHistory-20  3476  145349 ns/op   43433 B/op   172 allocs/op
BenchmarkAssemblerToolHistory-20     1353  357999 ns/op  128379 B/op  2203 allocs/op
BenchmarkAssemblerToolHistory-20     1360  355644 ns/op  128363 B/op  2203 allocs/op
BenchmarkAssemblerToolHistory-20     1353  355696 ns/op  128369 B/op  2203 allocs/op
BenchmarkAssemblerToolHistory-20     1348  362074 ns/op  128367 B/op  2203 allocs/op
BenchmarkAssemblerToolHistory-20     1335  355977 ns/op  128363 B/op  2203 allocs/op
# after
BenchmarkAssemblerDefaultHistory-20  3261  138610 ns/op   43435 B/op   172 allocs/op
BenchmarkAssemblerDefaultHistory-20  3516  141105 ns/op   43432 B/op   172 allocs/op
BenchmarkAssemblerDefaultHistory-20  3319  139332 ns/op   43432 B/op   172 allocs/op
BenchmarkAssemblerDefaultHistory-20  3516  139394 ns/op   43432 B/op   172 allocs/op
BenchmarkAssemblerDefaultHistory-20  3522  136823 ns/op   43432 B/op   172 allocs/op
BenchmarkAssemblerToolHistory-20     1550  308269 ns/op  112990 B/op  1763 allocs/op
BenchmarkAssemblerToolHistory-20     1568  309430 ns/op  112993 B/op  1763 allocs/op
BenchmarkAssemblerToolHistory-20     1576  311107 ns/op  112991 B/op  1763 allocs/op
BenchmarkAssemblerToolHistory-20     1563  310165 ns/op  112986 B/op  1763 allocs/op
BenchmarkAssemblerToolHistory-20     1554  312691 ns/op  112989 B/op  1763 allocs/op
```

## Gemini 实际输入 token 对照

使用授权的现有端点和 Key、`gemini-3.8-flash`。A/B 两次请求的模型、工具集合、Profile 指令、预先存入 SQL 的历史和最终问题相同；历史里有一个随机 marker，最终问题不提供答案，必须保留相应旧消息才能回答。

夹具主动设置 **16,000 context window / 2,048 output reserve / 1,024 safety margin**。这是测试应用预算，不是 Gemini 的真实模型容量声明。8 个工具声明都保持可见；本任务不需要调用工具。旧行为组只在调用 Assembler 前清空 `ModelContext.Tools`，重现原来的预算漏算；发送给模型的工具集合仍是相同的 8 个。

| 项目 | 旧行为：工具漏算 | 修复：计入工具预算 |
| --- | ---: | ---: |
| 发给模型的历史消息 | 43 | 20 |
| 实际发送的工具数 | 8 | 8 |
| 上游报告 input tokens | **3,910** | **2,501** |
| 上游报告 output tokens | 215 | 178 |
| 当前 Run 的工具执行 | 0 | 0 |
| 持久聊天历史事件数 | 53 | 53 |
| 最终回答 | 相同随机 marker | 相同随机 marker |

该固定任务 input tokens 减少 **1,409（36.04%）**，没有删除持久聊天记录，也没有把应执行的工具藏起来。它证明本夹具在保持所需事实和结果的情况下减少了实际输入；只有一组真实对照，不能当作所有任务平均节省率或复杂长任务质量保证。output tokens 由模型生成，不将其差值归因为确定性优化。

旧 trace 的估算 `input_tokens=10,804`，修复后为 `12,923`，尽管实际上游输入更少。原因是旧估算没有工具成本，两者估算范围不一致；不能把估算值升高误判成真实消耗升高。修复后导出的 `dropped_groups=23` 与消息数量变化一致。估算 token、模型 reported usage、账单金额是三个不同概念。

本次另对 Workflow 和父子 Agent 做了真实回归：Workflow 返回 17，父子 Agent 返回子工具中的随机值，工具副作用/终态/SQL 委派关联及父子 trace 均通过。三类用例合计 **8 次串行模型请求**、峰值在途 1、自动重试 0、上游 usage **7,351 input / 1,255 output tokens**，用例总时间 23.65 秒。与前次 29 次真实验收分开计数。

### 可审核的事件、trace 和指标

| 样本 | 证据 | 事件 / span |
| --- | --- | ---: |
| A/B 旧行为组 | [run_73f81725…](../verification/evidence/2026-09-06-context-budget/run_73f81725e0b023a85d0313db8c4b23ea.json) | 53 / 3 |
| A/B 修复组 | [run_b3f521a2…](../verification/evidence/2026-09-06-context-budget/run_b3f521a20d291f72098bb69b77013aaa.json) | 53 / 3 |
| Workflow | [run_a6ee565c…](../verification/evidence/2026-09-06-context-budget/run_a6ee565cfc10050b921f64dd7019da0f.json) | 17 / 7 |
| 父 Agent | [run_9d467602…](../verification/evidence/2026-09-06-context-budget/run_9d46760288bcb75b69f46aff2ca4d614.json) | 14 / 5 |
| 子 Agent | [run_f96aba84…](../verification/evidence/2026-09-06-context-budget/run_f96aba84d01c1a0b1e0e78c34ca451cf.json) | 14 / 4 |

审计文件是脱敏合成业务样本。`context_metrics_process_cumulative` 是该测试服务实例内 reader 收到的累积值，不是按 Run 过滤的指标；父子 Agent 共用实例，因此两份文件包含相同累计值。Run 级关联看 span 属性，避免把 Run ID 作为高基数 metrics 标签。

HTTP 历史通过小页读取后与 SQL 逐项比对；Workflow 重建服务和连接池后历史不变。OTel 使用真正 SDK reader 和本地 exporter，尚未连接远端监控平台。

## 扩展接口及兼容边界

源码入口：[ModelContext](../../pkg/core/model_context.go)、[Assembler / ContextEstimator](../../pkg/app/contextassembly/assembler.go)、[工具成本](../../pkg/app/contextassembly/tool_cost.go)。

```go
type ContextEstimator interface {
    Estimate(core.ChatMessage) (Cost, error)
    EstimateFragment(Layer, string) (Cost, error)
    EstimateTools([]core.ToolSchema) (Cost, error)
}
```

- 通过 `NewAssembler(Config{Estimator: implementation})` 安装；nil 使用公开的 `ConservativeEstimator`。扩展实现可以嵌入该类型，覆盖需要改变的估算方法；回归验证了覆盖方法不会被内置快速路径绕过。
- `ModelContext.Tools` 只供预算输入使用。Runtime 提供私有副本，插件改动该副本或返回另一集合，都不会修改本步实际发送的工具。旧自定义 assembler 不要求新增返回字段；自行忽略工具的旧实现仍需由集成方升级预算逻辑。
- 原有低层 `BudgetEstimator` / `TokenEstimator` 仍可使用。默认 Assembler 的替换需要同时定义工具成本，避免只换消息 tokenizer 却继续漏算工具。共享 estimator 必须支持并发调用；本次真实模型运行仍是串行。
- 内置工具估算包括 name、description、parameters 的中立 JSON 字节数，另留每工具 64 和整组 32 的 framing 余量；它是可替换的保守应用估算，**不承诺任意协议的精确 token 上限**。厂商协议包装、tokenizer 与扩展字段不同，应由相应实现提供成本模型。
- 工具成本不允许负值、零成本的非空工具集合或越界值；panic 被转换为上下文错误。默认工具估算拒绝重复名称、非法描述、无效 JSON 值/循环和支持子集中的非法 Schema。
- 没有引入全局缓存，没有改变 capability 执行权限、工具集合、持久事件格式或 SQL schema。`Config` 新字段建议使用具名字段初始化。

## 测试与复跑

离线验证包括：预算挤占可选旧历史而保留当前问题；当前必需消息放不下则失败；显式 tools 层预算；custom estimator 生效/隔离；内置快速路径成本一致；工具 Schema 超限通过 HTTP/SQL 留下 `model_context_failed`，**模型调用数为 0**；OTel 指标 reader 累计值正确；HTTP 历史、Workflow 和父子审计回归。

本机命令输出在忽略目录 `.tmp-context-budget-20260906/`：`01-before-bench.txt`、`02-regression-with-metric-failure.txt`、`04-after-bench.txt`、`06-offline-audited-budget.txt`、`07-live-budget-audit.jsonl`。早期测试编译错误和一个测试对名称空格的过强假设已修正；这些离线失败没有触发真实模型请求。

```powershell
# D 盘缓存和临时目录，与前次记录相同；不加载任何模型凭据。
go test -p 1 -run '^$' -bench 'BenchmarkAssembler(DefaultHistory|ToolHistory)$' `
  -benchmem -benchtime=400ms -count=5 ./pkg/app/contextassembly

# 准备隔离 PostgreSQL；不消耗外部模型额度。
go test -p 1 -parallel 1 -count=1 ./pkg/server -run '^TestPostgresSerial'

# 仅在已安全注入模型/数据库环境并设置 HARNESS_ACCEPTANCE_LIVE_SERIAL=1 后执行。
# 有真实模型费用，预计 8 个顺序请求；证据目录通过 HARNESS_ACCEPTANCE_EVIDENCE_DIR 设置。
go test -p 1 -parallel 1 -json -count=1 -timeout=900s ./pkg/server `
  -run '^TestLiveModelSerialModuleAcceptance$/(protected_workflow|durable_subagent|tool_budget_history)$'
```

全仓串行 Go 测试（含真实 PostgreSQL、关闭 live 开关）、格式、build、vet、staticcheck v0.7.0 均通过；上下文、内核、OTel 及新增 PostgreSQL 串行场景的定向 race 检查通过。原始输出为 `08-all-tests.txt` 至 `12-affected-race.txt`。本次没有将较低的单次分配量等同于已经全面优于其他 Agent 框架。

OpenAPI 102 个操作、本地文档链接及内核公共表面预算检查通过：34 个生产文件、8,631 非空物理行、904 项公共表面计数，比原评估只增加 `ModelContext.Tools` 一个字段；估算实现与新扩展接口均位于应用层。本轮独立 PostgreSQL 测试实例已停止，证据保留。
