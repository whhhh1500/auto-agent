# Agent 模块离线微基准与已有性能证据

日期：2026-09-06。源码：本地 Git `5beed15`。本次为[模块评估](../agent-module-assessment.md)补测已有微基准，不修改运行代码。[框架对比](../agent-framework-comparison.md)未运行其他框架，因此本文没有跨框架性能排名。

## 本次测量方法

- Windows amd64，Go 1.25.13，12th Gen Intel Core i7-12700K。
- Go benchmark 名称的 `-20` 为该次 GOMAXPROCS 标记，不表示 20 个客户端或 20 个并行 Agent。
- 6 个包，9 个场景（工具检索含两个子场景），每项 3 个样本；`-benchtime 200ms -count 3`。表中取三个样本的中位数及最小—最大范围，不把三个样本当作充分统计置信区间。
- 所选 benchmark 没有使用 `RunParallel` 对 Agent 并发施压；`-p 1` 限制包执行并行，不改变单个 benchmark 的内部逻辑。
- 未调用外部 LLM、PostgreSQL、S3 或云沙箱。`-run '^$'` 不执行普通测试，不能把本次结果当作全仓回归通过。
- 本机未独占给 benchmark，可能受其他进程、频率、调度与 GC 影响；未设置生产 CPU/内存限额。
- Go cache、构建临时目录与 TEMP/TMP 显式放在 D 盘。原始命令输出保留于 Git 忽略目录，下面另收录可公开的全部 27 条 benchmark 样本，避免文档只引用本机未提交文件。

## 中位数与范围

`B/op` 是每操作累计分配字节，不是进程 RSS、长期驻留内存或单个对象大小；分配次数同理。不同夹具不可直接横向比较绝对大小。

| 场景 | 时间中位数 | 三次时间范围 | B/op 中位数 | allocs/op | 夹具含义 |
| --- | ---: | ---: | ---: | ---: | --- |
| Session 热缓存投影 | 101.119 µs | 99.780–104.416 µs | 250,885 | 641 | 2,048 事件，预热投影后重复派生消息 |
| 全量投影后近期压缩 | 410.453 µs | 404.140–412.521 µs | 1,017,094 | 2,562 | 8,192 事件，完整投影后保留最多 128 消息 |
| 融合近期压缩 | 23.376 µs | 23.236–23.951 µs | 16,960 | 41 | 同样 8,192 事件与近期压缩配置，走融合路径 |
| 模型目录 Resolve | 187.8 ns | 184.5–190.1 ns | 40 | 2 | 内存目录解析；不含 SQL/网络/模型 |
| 模型设置热缓存 Load | 38.98 ns | 38.71–39.00 ns | 16 | 1 | 已预热缓存读取；不含初次加载与配置更新 |
| Scheduler 注册 1,000 活动项 | 375.569 µs | 374.606–381.826 µs | 415,038 | 6,953 | 每操作创建调度器、注册整批 1,000 项、关闭；手动时钟 |
| 工具搜索 indexed | 203.015 µs | 197.820–203.060 µs | 155,580 | 521 | 500 工具，查询 render image，TopK 8 |
| 工具搜索 legacy | 986.475 µs | 974.444–1,037.864 µs | 1,483,979 | 11,051 | 同目录与查询，旧快照/分词路径 |
| 默认历史 Assembler | 152.197 µs | 151.269–152.970 µs | 43,435 | 172 | 120 条长文本 user 消息，32,768 上下文 / 4,096 输出预算 |

夹具源码：[Session](../../pkg/core/session_context_benchmark_test.go)、[modelcontrol](../../pkg/app/modelcontrol/modelcontrol_test.go)、[modelsettings cache](../../pkg/app/modelsettings/cache_test.go)、[Scheduler](../../pkg/app/runliveness/scheduler_test.go)、[toollib](../../pkg/extensions/toollib)、[Assembler](../../pkg/app/contextassembly/assembler_test.go)。

Session 夹具交替追加 user/assistant 事件，并在部分 assistant 事件携带工具调用。压缩配置为 `MaxMessages=128`、`MaxToolResultChars=512`，不代表真实客户的所有历史分布。Assembler 的长文本为重复字符串，也不是语义任务集。工具目录夹具使用高度相关的共同词项，不能据此推断长尾工具召回或 ANN 能力。

Assembler 夹具不含最终独立工具 Schema 的 token 成本；当前 `ModelContext` 也不接收该集合，实际模型调用在后续附加工具声明。完整请求预算缺口见 [M14](../agent-module-assessment.md#m14)，不能用本基准证明任意工具目录都满足模型上下文窗口。

## 能支持的结论

- 同一长历史夹具下，融合路径的时间中位数相对先全投影再压缩约改善 **17.56 倍**，分配字节约减少 **59.97 倍**。它证明本仓库局部优化有效，不表示整个 Agent 提速 17.56 倍，也不表示任意状态的复杂度都降为常数。
- 同一 500 工具夹具下，索引路径比旧路径时间约改善 **4.86 倍**，分配字节约减少 **9.54 倍**。尚未测搜索准确率与万级目录。
- 已缓存的目录和模型设置读取在该夹具内很轻；当前优先排查完整历史复制、模型/工具 I/O、数据库和大产物，比单看纳秒级配置读取更合理。这是基于当前测量的优化优先级判断。
- Scheduler 的数值按整批 1,000 注册计算，既不是每个 Run 的真实存活开销，也不证明支持 1,000 并行推理。实际心跳回调、SQL 续租和超时负载未包含在此夹具中。
- 微基准测局部机器成本，不测任务完成质量、模型 TTFT、真实账单、长期内存泄漏或故障恢复概率。

## 本次复现命令

在源码基线对应的仓库根目录运行；新提交的结果需记录新 commit，不能覆盖后继续沿用旧日期。以下不启用真实模型验收。

```powershell
$env:GOCACHE = 'D:\cc\auto_agent\.codex-gocache'
$env:GOTMPDIR = 'D:\cc\auto_agent\.codex-gotmp'
$env:TEMP = 'D:\cc\auto_agent\.codex-tmp'
$env:TMP = $env:TEMP
$env:GOTOOLCHAIN = 'go1.25.13'

New-Item -ItemType Directory -Force -Path $env:GOCACHE, $env:GOTMPDIR, $env:TEMP | Out-Null
New-Item -ItemType Directory -Force -Path '.tmp-module-review-20260906' | Out-Null

git rev-parse HEAD
go version
$bench = '^(BenchmarkSessionDeriveMessagesCached|BenchmarkSessionDeriveThenRecentCompactLongHistory|BenchmarkSessionFusedRecentCompactLongHistory|BenchmarkResolve|BenchmarkCachedRepositoryLoad|BenchmarkSchedulerRegister1000Active|BenchmarkCatalogSearchIndexedVsLegacy|BenchmarkAssemblerDefaultHistory)$'
go test -p 1 -run '^$' -bench $bench -benchmem -benchtime 200ms -count 3 ./pkg/core ./pkg/app/modelcontrol ./pkg/app/modelsettings ./pkg/app/runliveness ./pkg/extensions/toollib ./pkg/app/contextassembly |
    Tee-Object -FilePath '.tmp-module-review-20260906/microbench.txt'
if ($LASTEXITCODE -ne 0) { throw "Benchmark failed with exit code $LASTEXITCODE" }
```

## 本次全部原始样本

以下为命令输出的 benchmark 行，统一单位 ns/op、B/op、allocs/op；所有六个包最终均 PASS，命令退出码 0。

```text
BenchmarkSessionDeriveMessagesCached-20                   2295      99780 ns/op    250884 B/op     641 allocs/op
BenchmarkSessionDeriveMessagesCached-20                   2336     101119 ns/op    250887 B/op     641 allocs/op
BenchmarkSessionDeriveMessagesCached-20                   2282     104416 ns/op    250885 B/op     641 allocs/op
BenchmarkSessionDeriveThenRecentCompactLongHistory-20      577     404140 ns/op   1017095 B/op    2562 allocs/op
BenchmarkSessionDeriveThenRecentCompactLongHistory-20      559     412521 ns/op   1017092 B/op    2562 allocs/op
BenchmarkSessionDeriveThenRecentCompactLongHistory-20      573     410453 ns/op   1017094 B/op    2562 allocs/op
BenchmarkSessionFusedRecentCompactLongHistory-20         10000      23236 ns/op     16960 B/op      41 allocs/op
BenchmarkSessionFusedRecentCompactLongHistory-20          9835      23951 ns/op     16960 B/op      41 allocs/op
BenchmarkSessionFusedRecentCompactLongHistory-20         10000      23376 ns/op     16960 B/op      41 allocs/op
BenchmarkResolve-20                                   1246389        184.5 ns/op      40 B/op       2 allocs/op
BenchmarkResolve-20                                   1284660        187.8 ns/op      40 B/op       2 allocs/op
BenchmarkResolve-20                                   1273516        190.1 ns/op      40 B/op       2 allocs/op
BenchmarkCachedRepositoryLoad-20                      6139802         39.00 ns/op     16 B/op       1 allocs/op
BenchmarkCachedRepositoryLoad-20                      5778466         38.98 ns/op     16 B/op       1 allocs/op
BenchmarkCachedRepositoryLoad-20                      6148611         38.71 ns/op     16 B/op       1 allocs/op
BenchmarkSchedulerRegister1000Active-20                   615     375569 ns/op    415073 B/op    6953 allocs/op
BenchmarkSchedulerRegister1000Active-20                   652     381826 ns/op    415035 B/op    6953 allocs/op
BenchmarkSchedulerRegister1000Active-20                   634     374606 ns/op    415038 B/op    6953 allocs/op
BenchmarkCatalogSearchIndexedVsLegacy/indexed-20         1167     203060 ns/op    155581 B/op     521 allocs/op
BenchmarkCatalogSearchIndexedVsLegacy/indexed-20         1240     197820 ns/op    155576 B/op     521 allocs/op
BenchmarkCatalogSearchIndexedVsLegacy/indexed-20         1224     203015 ns/op    155580 B/op     521 allocs/op
BenchmarkCatalogSearchIndexedVsLegacy/legacy-snapshot-and-tokenize-20 247 986475 ns/op 1483979 B/op 11051 allocs/op
BenchmarkCatalogSearchIndexedVsLegacy/legacy-snapshot-and-tokenize-20 247 1037864 ns/op 1483979 B/op 11051 allocs/op
BenchmarkCatalogSearchIndexedVsLegacy/legacy-snapshot-and-tokenize-20 235 974444 ns/op 1483978 B/op 11051 allocs/op
BenchmarkAssemblerDefaultHistory-20                     1521     152197 ns/op     43438 B/op     172 allocs/op
BenchmarkAssemblerDefaultHistory-20                     1569     151269 ns/op     43435 B/op     172 allocs/op
BenchmarkAssemblerDefaultHistory-20                     1610     152970 ns/op     43432 B/op     172 allocs/op
```

## 已有集成与真实会话记录：本次未重跑

以下数据引用同日已提交的[整改集成验收](../verification/2026-09-06-assessment-closure.md)，不是新微基准的结果。

| 既有项目 | 已记录结果 | 能证明 / 不能证明 |
| --- | --- | --- |
| PostgreSQL HTTP 审批并发 | 8 客户端、2 服务对象与独立 SQL pool、4 worker、2,048 完整会话，0 失败；16.078 秒，127.38 会话/秒；p50 50 ms、p95 168 ms、p99 198.440 ms | 本地固定输出 HTTP 模型，每会话两次请求；覆盖部分编排/SQL 成本。两个服务对象在同一个 Go 测试进程，不是跨 OS 进程、跨机器或长期 SLA |
| 真实 gemini-3.8-flash | 一次成功审批恢复会话，2 模型请求、1 工具效果、1 终态；用例约 4.82 秒，输入 278 / 输出 275 tokens | 用户指定兼容连接；包含有序服务关闭/重建与自动审批，不是 native Gemini SDK、人工等待样本或重复统计分布 |
| Windows Basic Medium | 5 个原生用例约 5.17 秒 | 包含约 5 秒超时用例；不是启动耗时，也不证明网络隔离或多 Session 并行 |
| 全仓与专项质量门禁 | 80 包通过；1,616 顶层测试、含子测试 2,381 通过记录；10 条明确条件跳过。PG 专项 10 包、57 顶层/21 子测试、零跳过 | 正确性与条件环境证据；普通全仓的跳过不计为相应功能已验证，远程 CI/Linux/S3 等不能据此宣称本次验收完成 |

真实模型修复前的两次失败诊断也保留在原验收记录中，不能只展示成功后宣称全流程始终无失败。真实模型用例需要单独显式启用，本次文档工作没有重试这些请求。

## 历史 P5 / 轻载记录：不能当作当前完整服务基准

[perf-p0.md](perf-p0.md)保留 2026-09-04 的历史 P5 工具/夹具结果，源码与负载不同于本次微基准及完整 SQL 审批会话：

| 历史负载 | 已记录数据 | 解释 |
| --- | --- | --- |
| prepared typical，16 KiB，500 并发 | p95 18.81 ms，RSS peak 约 0.53 GiB | 合成的 production-shaped Agent 夹具；不是公网真实模型服务 |
| prepared max，1 MiB，500 并发 | p95 812.03 ms，RSS peak 约 6.99 GiB | 包含输入驻留、复制、GC 与 allocator；极端大输入并发成本明显 |
| fresh-child /healthz，500 并发 | 一次全成功，另一次 222 成功 / 278 失败 | 冷启动连接突发有波动，不能只取全成功一次宣布稳定 |
| keepalive 500 worker、5 秒 measured | 测量窗口 0 错；单列 warmup 有 6,052 transport 失败 | warmup 失败保留，短暂热窗口不能代替长期 soak |
| 20 秒 idle / 单客户端轻载 | Windows working set peak 约 19.69 / 21.95 MiB | 只是特定历史轻载观察，不能外推高并发内存 |

本项目可以说“已有局部优化和有界集成基线”，目前不能说“500 并发生产稳定”“整个框架只占 20 MB”或“比 Python Agent 框架快若干倍”。

## 待补的性能维度

| 优先级 | 试验 | 主要输出 |
| --- | --- | --- |
| 高 | 独立服务进程 + 真实 PG，多租户、长稳态、进程强杀与租约丢失 | 成功/重复/未知副作用，恢复时间，p99，数据库负载 |
| 高 | 实际模型任务集，固定版本与输入 | 任务成功率、TTFT、完成时间、总 tokens、工具次数、成本 |
| 高 | 完整 System/历史/工具 Schema 的请求预算 | 与目标模型 token 计数对照、超窗拒绝与裁剪行为 |
| 高（启用 WASM 前） | 显式内存与执行终止配置的有界隔离验收 | 内存增长上限、计算循环取消、重复执行资源回收；当前实现缺口见 M27 |
| 高 | 长历史、大工具结果、大 Artifact、慢 SSE 消费者 | 分配、RSS、GC、背压、断连后的观察恢复 |
| 按需求 | 原生 Linux / E2B / Windows 高频会话 | 冷/热启动、并发限制、取消/清理、文件吞吐和实际 assurance |
| 按需求 | RAG 数据规模与多语言任务 | Recall@K、MRR/nDCG、答案忠实度、授权过滤、检索延迟 |
| 选型前 | 相同持久化与保障条件下的 Eino/LangGraph/厂商 SDK 对照 | 端到端质量、速度、资源和总开发运维成本 |

这些是下一轮验证建议，本次未执行；后续结果应另记环境、源码版本与实际测量范围。
