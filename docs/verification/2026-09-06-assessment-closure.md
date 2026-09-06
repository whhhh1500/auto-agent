# 评估整改与集成验收

日期：2026-09-06。范围：`harness-core` 本地源码、Windows Basic、真实 PostgreSQL、
HTTP 审批恢复、Graph 示例，以及用户指定的 `gemini-3.8-flash` 模型。
后续同日补充了真实 PostgreSQL 上的 queued worker 进程硬终止恢复验收；该补验的
默认路径使用确定性离线模型夹具，并另以 `gemini-3.8-flash` 串行执行两个显式 live
故障点，共 2 次模型请求，替换进程 0 次模型请求，无自动重试。
这是[初始项目评估](2026-09-06-project-assessment.md)的后续记录。

## 已交付变更

1. PostgreSQL CI 和本地 smoke 共用 `scripts/test-postgres`，运行全仓
   `^TestPostgres`，覆盖之前遗漏的七个 SQL 适配器包。缺 DSN、跳过、零测试、
   失败或截断输出均使门禁失败；CI 保存 JSONL 工件。
2. 架构、README 和 SQL 验证矩阵与当前实现对齐。Windows 明确只支持 current-user
   Basic；新增 pre-GA/实验能力清单，标明 E2B 尚无内置适配器。
3. `examples/graph-review` 提供 draft/review/finalize 三节点 SQL 示例，验证重新打开
   数据库、重建 Workflow 后恢复、稳定审批 attempt、已完成节点不重复和未知结果关闭。
   Reviews 由应用注入，示例测试使用独立的审批服务替身；SQL 审批持久化另由服务测试覆盖。
4. 增加真实 TCP HTTP、SQL Session/Run Queue/Approval/Tool Journal 的集成测试：
   原服务暂停并关闭，替换实例从相同数据库恢复同一个 run；重复审批不重做工具副作用。
5. 真实模型验收发现并修复两个兼容性缺口：恢复 Session 后首次 Append 错误地把空历史
   投影标记为有效；Chat Completions 丢弃工具调用的 `extra_content`。
6. queued worker 在非幂等工具副作用已提交、Session 的 tool call 尚未 durable 时被硬杀，
   替换 worker 会重新调用模型和工具。现为同步/queued 两条服务路径建立 per-run
   pre-tool durable checkpoint，并用两个独立 OS 子进程验证异常退出后的 fail-closed 恢复。

7. 同日后续增加 queued worker 的 SQL generation fence：Server 在启动时拒绝无法证明
   `Sessions + RunQueue + SessionLeaser` 共用同一 atomic SQL domain 的组合；worker 用
   `instance + compact Run ID digest + generation + nonce` 的唯一 Session lease holder，显式 repair 和同一
   fenced `WriteBehind` 覆盖后续 model/tool/checkpoint/background/terminal Session 写入。
   SQLite 定向和 race 验证覆盖旧 generation、queue lease 过期、cancel、background flush、
   predecessor repair、approval pause/resume 和 successor 完成。`ErrSessionWriteFenceLost`
   只停止/abort stale worker，不会写 `store_error` terminal 或结算旧 claim。该补验没有运行
   真实 PostgreSQL：未配置 DSN 时 PostgreSQL 用例按仓库约定跳过，因此 SQLite 结果不应被
   表述为 PostgreSQL 锁竞争或事务行为的实测证据。

恢复逻辑现在先使非空 Session 的投影缓存失效。工具调用新增可选、最多 64 KiB 的
`continuation` 字符串，由外层协议适配器解释，原样随对应 assistant 工具调用持久化和
返回；上下文与请求预算包含该字段。未知协议状态被拒绝，不作为工具参数或授权信息。
核心没有引入模型厂商依赖，SQL schema 无需升级，旧事件仍有效。旧记录中已经丢失的
签名不能凭空恢复。Google 的
[thought-signature 文档](https://ai.google.dev/gemini-api/docs/generate-content/thought-signatures)
说明了 Gemini 对工具调用续接签名的要求及 OpenAI 兼容格式中的位置。

## 环境与本地证据

- Windows amd64，Go 1.25.13，PostgreSQL 17.6。
- PostgreSQL 使用本任务独立初始化的测试集群，仅绑定 `127.0.0.1:55436`；用例使用
  随机、独占 schema 并清理自己创建的 schema。未接触业务数据库。
  验收完成后已按该集群的精确 data 路径正常停止，`pg_ctl` 退出码 0；保留被忽略的诊断目录。
- `GOCACHE`、`GOTMPDIR`、`TEMP`、`TMP` 均显式设到 D 盘。
- 原始日志位于仓库内被 Git 忽略的 `.tmp-assessment-closure-20260906/`。
  进程硬终止的修复前/后日志与脱敏 JSON 位于同样被忽略的
  `.tmp-process-crash-20260906/`；它们是本机复核材料，不是已发布的仓库工件。
  凭据从用户指定的仓库外 `.env` 读取，仅注入测试进程，不复制到仓库或日志。
- 本地 Git `main` 已初始化，无远程地址；初始快照 `6e562ac`，整改设计 `14b95ea`。

## 验收结果

| 检查 | 结果与范围 |
| --- | --- |
| PostgreSQL 全仓门禁 | 57 个顶层测试、21 个子测试、10 个包；零跳过、零失败 |
| 全仓 `go test -p 4 -json -count=1 -timeout 600s ./...`，配置测试 PG | 80 个包通过；1,616 个顶层测试通过，含子测试 2,381 条通过记录；10 条显式跳过；零失败 |
| 构建、vet、格式检查 | `go build -p 4 ./...`、`go vet -p 4 ./...`、`scripts/verify-gofmt.ps1` 通过 |
| Race | Windows 上全部相关包通过，包含 core、context assembly、model execution 与协议适配器、server、Graph 示例、PostgreSQL runner；配置真实测试 PG，非全仓 Linux race |
| Staticcheck | `GOTOOLCHAIN=go1.25.13 go run honnef.co/go/tools/cmd/staticcheck@v0.7.0 ./...` 退出码 0 |
| OpenAPI | 核对 102 个已注册 `/v1` 操作，通过 |
| 核心架构预算 | 34 个生产文件、8,624 个非空物理行、公共表面计数 903；原预算不变，检查通过 |
| Windows Basic 原生 Medium 验收 | 带 `sandboxacceptance` 标签的入口通过，约 5.17 秒；5 个 native 用例覆盖启动、关闭失败清理、连续会话、超时、取消 |
| Graph 示例 | SQLite 和 PostgreSQL 重建恢复测试通过；草稿执行一次、review 暂停/恢复两次进入、finalize 一次；中断结果不重跑 |
| 真实 Gemini 审批恢复 | 通过；同一 run、2 次模型调用、1 次工具效果、1 个终态事件 |
| queued worker 进程硬终止恢复 | 离线夹具与串行 Gemini 各 2 个故障点通过；每点 1 次业务效果、1 条 journal、替换进程 0 次模型调用，Run 以 `run_interrupted` 失败收束 |

全仓普通测试中的跳过不计为成功验收。它们包括平台权限/原生 helper 条件、S3 smoke
和默认关闭的计费模型用例；PostgreSQL 专项无跳过。真实模型和 Windows Medium 入口
单独显式执行。远程 GitHub CI、PostgreSQL 16、Linux 原生、Docker、S3 和长期生产负载
不在本次实测证据内。

Race 原始记录为 `race.log`，固定工具链的 Staticcheck 记录为 `staticcheck-pinned.log`，
Windows 原生记录为 `windows-basic.log`。首次未固定工具链的 Staticcheck 自动选择了
Go 1.26.8 并通过；最终另外固定 Go 1.25.13，与 CI 约定一致后通过，不混用两者的环境证据。
提交前检查了 34 个变更文件，所用真实 LLM key 精确匹配为零；凭据文件、测试日志和数据库
目录都被 Git 排除。该检查不等同于完整安全审计。

<a id="worker-process-crash-recovery"></a>

## Worker 进程硬终止恢复

### 问题时序与修复机制

修复前的时序是：模型流结束并在内存 Session 追加 `assistant/message` 与 `tool/call` →
SQL journal 开始 → 非幂等工具提交业务副作用 → worker 进程在下一次 write-behind flush 前被
硬终止。PostgreSQL 中 Session version 仍为 0，替换 worker 看不到原 call identity，因而
重新调用模型、生成新 call ID 并再次执行工具。故障注入分别卡在业务效果已经提交但 journal
仍为 `started`，以及 journal 已为 `completed` 且带结果之后；修复前两点都发生重复执行。

修复保持公共 `Runtime` 和 `RunExecutor` 合同不变：

1. 新增可重复的同步 `WriteBehind.Checkpoint`；writer 打开时，之后的 `MarkDirty` 仍会按原
   write-behind 窗口批量保存，也可再次 checkpoint。`Flush` 会排空当前待写前缀并终结关闭；
   此后的 `MarkDirty` / `Checkpoint` 不再持久化新事件，也不会重新开启后台调度。
2. 同步 HTTP Run 与 queued worker 都在 executor 解析前创建 writer，并为该 Run 浅复制
   `Runtime`。共享 Runtime 没有被改写。
3. per-run Runtime 的 journal 使用私有 wrapper；模型产生的 `tool/call` 已追加后，wrapper
   在调用底层 `BeginToolInvocation` 前执行 checkpoint。checkpoint 失败时不进入 journal
   Begin，也不允许工具 provider 执行，并立即取消该 Run，阻止第二次模型调用。
4. executor registry 通过 `runexecutor.Dependencies{Runtime: runtime}` 收到这个 per-run
   Runtime，因此使用所注入 Runtime 受保护工具路径的自定义 executor 同样经过 checkpoint。
   自定义 executor 若绕过该 Runtime、直接执行副作用，或在 `RunTurn` / `ResumeTurn` 返回后
   异步继续使用 Runtime 或 `emit`，则不在 executor 合同和这一保证内。
5. FastRouter 命中后先构造 call，并在 Agent 的 guarded Runtime 内追加 `EvToolCall`，再进入
   guarded `Execute`、journal Begin 和工具副作用。外部 `ToolRuntime` 仍可按原公共
   `Dispatch` 合同使用；新增行为没有扩大公共接口。

[checkpoint 单元与同步/queued 集成测试](../../pkg/server/server_tool_checkpoint_test.go)分别
检查 checkpoint 失败不进入底层 Begin、tool call 在 Begin 前已 durable、共享 Runtime 未被改写；
[write-behind 回归](../../pkg/storage/durability_test.go)分别检查 Checkpoint 会等待已有后台写、
继续排空并恢复后续调度，重复调用不重写已 durable 版本；也检查 Flush 后的 MarkDirty / Checkpoint
都不会再持久化或重新开启调度。
`06-targeted.log` 记录这些用例和 executor registry 回归通过，但没有将“收到 Runtime”扩大为
“任意自定义 executor 都必然使用受保护调用路径”。

持久 checkpoint 注入失败也有同步/queued 离线回归。两条路径都是 store write attempts 1、
inner journal Begin 0、工具执行 0、模型调用 1。wrapper 在保存原始 checkpoint 错误后仍主动
取消内部 Run，因此不会进入工具或第二次模型调用；该内部取消不是最终对外错误合同。
同步 SSE 会抑制由此产生的 `tool_cancelled` 与 cancelled `run/end`，最终只发送结构化
`store/error`，其中 `code=store_error`、`status=failed`，并保留原始持久化错误。同步路径的
durable Session 保持 version 0 / 0 events，RunControl 为 `failed/store_error`；queued 路径
同样以 `failed/store_error` 收束并向 worker 返回可匹配原始 checkpoint 错误的失败，durable
Session 也没有部分历史。`store/error` 是当前 SSE 响应，不是 durable Session 事件。

[同步/queued FastRouter checkpoint 测试](../../pkg/server/server_tool_checkpoint_test.go)检查
server writer 在 journal Begin 前已经持久化 `EvToolCall`，且模型调用为 0；
[core 顺序测试](../../pkg/core/runtime_test.go)覆盖 append → journal Begin → effect，
[审批回归](../../pkg/core/fastrouter_test.go)检查暂停与恢复复用同一个 call。它们都是进程内、
确定性的顺序测试，没有注入独立进程崩溃，也没有运行 FastRouter 专属 Gemini live 场景，
因此不能借用下方普通模型工具路径的 crash 证据。

这是一条按工具副作用设置的 durable 边界：每个使用 journal 的受保护工具在副作用前增加
一次同步持久追加。模型流的普通 chunk 继续批量 write-behind，没有改成逐 chunk 同步写。
当前尚无该新增交互的独立延迟/吞吐 benchmark；后续需要在真实 PostgreSQL 延迟下测量，
不能用下方两个单次 live 样本代替分布测量。

### 修复前后数据

修复前记录来自本机忽略目录中的 `04-red.log` 及其 `red-evidence` JSON。修复后的当前离线
强化证据是下表链接的 `run_3bdc…` / `run_7c40…`；它们使用确定性离线模型夹具、真实
PostgreSQL 和真实 OS 子进程，不含 Gemini 请求。该表只比较离线夹具，不混入后面的 live PID
或模型耗时。

| 状态 / 故障点 | Run | 首进程 PID → 替换 PID | durable version | 业务效果 | 模型调用（首进程 + 替换） | journal | Run 终态 |
| --- | --- | --- | ---: | ---: | ---: | --- | --- |
| 修复前 / `effect_committed` | `run_a8b1…bcceb` | 18300 → 32112 | 0 | 1 → 2 | 1 + 2 | 2 rows | `completed` |
| 修复前 / `journal_completed` | `run_0a7f…bc27` | 29748 → 31896 | 0 | 1 → 2 | 1 + 2 | 2 rows | `completed` |
| 修复后 / `effect_committed` | [run_3bdc…0494](evidence/2026-09-06-process-crash-recovery/run_3bdcd9f13bbea22f927b52f9be510494_process_crash.json) | 29872 → 30644 | 5 | 1 → 1 | 1 + 0 | 1 row，`started`，无结果；恢复后不变 | `failed / run_interrupted` |
| 修复后 / `journal_completed` | [run_7c40…f0e8](evidence/2026-09-06-process-crash-recovery/run_7c40bd65b7955a8275dd9b2d2781f0e8_process_crash.json) | 31724 → 19328 | 5 | 1 → 1 | 1 + 0 | 1 row，`completed`，有结果；恢复后不变 | `failed / run_interrupted` |

修复后两个故障点的 durable 前缀都精确为 5 条：`run/start`、`user/message`、
`step/start`、`assistant/message`、`tool/call`。恢复只追加同一 call ID 的
`tool/result(code=tool_outcome_unknown, repaired=true)`、`step/end`、
`run/error(code=run_interrupted)`、`run/end(status=failed)`，最终共 9 条；HTTP 分页历史
与 PostgreSQL 原始 event chunks 逐项一致。首进程 queue row 均为
`worker_id=process-crash, attempt=1, generation=1`；过期 claim 恢复为 1 成功、0 失败，
替换进程收束后 queue row 为 0。

两份离线强化证据还显式记录 `effect_observed_durable_version=5`，证明业务效果提交时已经能
观察到同一 tool call 的 durable 前缀；`http_history_matches_sql=true` 则记录 HTTP 历史与
原始 SQL event chunks 的一致性。测试仅在 durable 语义、业务效果、journal、queue、历史、
PID/异常退出和 trace 断言全部通过，并完成敏感值检查之后才发布 JSON；失败路径不会留下看似
合格的本次发布证据。

与上述离线表分开，显式 Gemini live 层串行跑通两个故障点，总测试耗时 8.29 秒，
2 次模型请求、峰值在途 1、
替换进程 0 次模型请求、无自动重试。每点仍为 version 5、最终 9 条事件 / HTTP 3 页、
业务效果 1 → 1、1 条 journal 和 `failed / run_interrupted`：

| 故障点 | 脱敏证据 | 首进程 PID → 替换 PID | 模型 span | 报告 tokens（input / output） |
| --- | --- | --- | ---: | ---: |
| `effect_committed` | [run_4d8e…e1b](evidence/2026-09-06-process-crash-recovery/run_4d8ee6bc062d612f4158152040608e1b_process_crash.json) | 32224 → 31100 | 4,603 ms | 89 / 149 |
| `journal_completed` | [run_c78f…266](evidence/2026-09-06-process-crash-recovery/run_c78f2a2646cfd5725f9ae5766dc69266_process_crash.json) | 5100 → 19896 | 2,569 ms | 89 / 135 |

这里的耗时是各自唯一 model span；8.29 秒是包含进程启动、故障注入、PostgreSQL 检查、
claim 恢复和替换进程收束的测试总时间。每个故障点只执行一次，不能据此给出 checkpoint
p50/p95、稳定吞吐或模型延迟结论。

验收不是只看测试退出码。每个故障点联合检查 durable chat history、Run/queue 状态、
journal 状态、业务效果计数、trace 以及实际进程身份和退出：checkpoint PID 必须等于父测试
启动的 PID；首进程必须非正常非零退出（本次 Windows exit code 1）；替换 PID 必须不同且
正常结束。`effect_committed` 时 checkpoint 位于仍 active 的 tool span，硬杀使该 span
不可能正常 end/export，证据不会伪造完整 span；已结束的 model span 与 active Run trace
关联。`journal_completed` 时 tool span 已正常结束并与同一 Run trace 关联。两个路径的替换
进程都只有 queue claim span，没有 model/tool span，和 0 次替换模型调用一致。

### 证据分层与保留边界

[进程硬终止测试](../../pkg/server/server_process_crash_test.go)同时承载两层入口：默认
`TestRunWorkerProcessCrashRecovery` 是 offline fixture 层，使用真实 PostgreSQL、真实
`Process.Kill`、独立恢复进程与本地 OTel exporter，模型响应和业务工具内容是确定性夹具；
显式的 `TestLiveRunWorkerProcessCrashRecovery` 才是 Gemini live 层，它要求
`HARNESS_ACCEPTANCE_LIVE_SERIAL=1`。本轮 live 层固定 `gemini-3.8-flash`、严格串行、无自动
重试，并生成上方 `run_4d8e…` / `run_c78f…` 两份公开脱敏证据。离线层生成
`run_3bdc…` / `run_7c40…`，用于确定性回归和 durable/HTTP 一致性强化；live 层只补充真实
provider 经过同一持久化/恢复路径的单样本证据。两层的 PID、模型耗时和 token 不能交叉引用，
也不能合并为性能基准。

`journal_completed` 当前仍按安全边界 fail closed：即使 journal 已有完成结果，替换进程也
写入未知工具结果并将 Run 收束为 `run_interrupted`，不会自动把已完成结果送回模型继续运行。
这是避免不确定恢复再次触发外部效果的保守边界，也是后续可改进项；“完成结果自动续跑”
尚未实现、没有真实验收，不能从本轮通过中推导出来。

## 真实模型会话

使用用户指定的 OpenAI 兼容连接和精确模型名 `gemini-3.8-flash`。测试输入为接受
`doc-acceptance` 文档，唯一工具是需要审批的本地测试工具。工具仅记录一次内存计数
并返回固定文档结果，不发布外部内容。

流程：HTTP 创建 Session → 异步提交 → 真实模型工具调用 → SQL 等待审批 → 关闭原服务
和连接池 → 独立服务/连接池读取 SQL → HTTP 批准 → 同一 run 恢复 → 工具执行 → 真实
模型最终回复 → 终态落库。重复审批后再次领取队列，证明没有额外工具效果。

成功日志：`live-gemini-fixed.jsonl`，用例耗时 4.82 秒。

- Session：`sess_537cc0f234ce6f4dd71e8d194bd407e6`
- Run：`run_7a9f99aa09d7938cfe5df0ad89895cc5`
- 模型调用：2 次；报告输入 278 tokens、输出 275 tokens。
- 最后一段 usage：输入 160、输出 84；总量来自包裹实际模型适配器的观察器，未把最后
  一段 usage 冒充整个会话用量，也未推算价格。
- 最终回复：`The document doc-acceptance has been successfully approved and accepted.`

修复前的两次真实诊断会话在恢复后 HTTP 400 失败，记录分别为
`live-gemini-first.jsonl` 和 `live-gemini-diagnostic.jsonl`。它们不计为通过；修复后只执行
上述一次成功会话。确定性 HTTP 回归用例会检查原工具调用中完整的续接字段，且核心测试
覆盖恢复后先 Append、后投影的 cold/warm 与默认 compactor 路径。

复现时先从秘密管理器注入 `HARNESS_LLM_BASE_URL`、`HARNESS_LLM_API_KEY` 和一次性
测试数据库 `HARNESS_TEST_PG_DSN`，然后显式启用计费模型用例：

```powershell
$env:HARNESS_LLM_MODEL = 'gemini-3.8-flash'
$env:HARNESS_LLM_MAX_TOKENS = '2048'
$env:HARNESS_ACCEPTANCE_LIVE_MODEL = '1'
go test -count=1 -v -timeout 300s ./pkg/server -run '^TestLiveModelHTTPApprovalResumesOnReplacementInstance$'
Remove-Item Env:HARNESS_ACCEPTANCE_LIVE_MODEL
```

默认全仓与 PostgreSQL CI 不自动调用外部模型。

## 有界并发基线

独立执行 `TestPostgresHTTPConcurrentApprovalWorkload`，8 个并发客户端、2 个独立服务
实例与连接池、共 4 个 worker，2,048 次完整的创建/提交/审批/恢复/完成操作。每个会话
需要两次本地 HTTP 模型请求；本地模型固定输出，用于测量编排与数据库成本。

| 指标 | 实测 |
| --- | ---: |
| 成功 / 失败 | 2,048 / 0 |
| 工具效果数 | 2,048 |
| 测量窗口 | 16.078 秒 |
| 完整会话吞吐 | 127.38 次/秒 |
| p50 | 50.000 ms |
| p95 | 168.000 ms |
| p99 | 198.440 ms |

延迟从 HTTP 创建 Session 开始，到 SQL run 为 completed；包含客户端轮询与审批等待。
分位数使用 nearest-rank，吞吐仅用测量窗口。日志：`postgres-workload.jsonl`。

```powershell
# HARNESS_TEST_PG_DSN 已指向独立测试库。
$env:HARNESS_ACCEPTANCE_CLIENTS = '8'
$env:HARNESS_ACCEPTANCE_RUNS = '2048'
go test -count=1 -v -timeout 300s ./pkg/server -run '^TestPostgresHTTPConcurrentApprovalWorkload$'
Remove-Item Env:HARNESS_ACCEPTANCE_CLIENTS, Env:HARNESS_ACCEPTANCE_RUNS
```

两个服务实例运行在同一个 Go 测试进程中；恢复测试执行有序关闭与重建，不是操作系统
强杀或跨机器故障演练。16 秒本地窗口不代表真实模型吞吐、长期稳定性或生产 SLA。
认证使用注入的固定测试 principal，未覆盖登录、公网代理或完整多租户混合负载。

## Windows 与 E2B 的边界

Windows 实现保持 Basic：普通 Medium 调用者、受限子进程、Job 生命周期清理、每进程
单活动 Session、Host 网络，无网络隔离；不增加专用账号、WFP 或提权。
`WRITE_RESTRICTED` 存在 Everyone/logon 可写例外，不能描述为宿主机文件系统完全隔离。
更完整的既有边界与历史证据见 [Windows Basic 记录](2026-09-06-windows-basic-acceptance.md)。

当前没有内置 E2B SDK/API client，也没有向外提供 E2B 兼容服务端接口。可用的扩展入口
是 `pkg/execution/sandbox` 的 Provider、Session 和精确版本 Registry；接入 E2B 应在
独立适配器实现生命周期、命令、产物和真实 assurance 映射。本次未实现或实测 E2B。
