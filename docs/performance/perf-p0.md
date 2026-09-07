# Perf-P0 本地基线

`cmd/perf-p0` 是一个确定性的、本地无外网的性能基线工具。它覆盖：

- 真实 HTTP `/healthz` 控制面请求；
- legacy Session event projection 与 `RecentTurnsCompactor`（用于底层成本归因）；
- 当前 `Agent` + `RecentTurnsCompactor` fused production fast path，配合确定性的本地 LLM/tool runtime；
- 本地 `toollib.Catalog` 搜索；
- 可选的控制面 soak 请求。

默认并发矩阵是 `10,100,500`。每个数字都是同时启动并执行操作的 goroutine 数量，工具不会加入 admission、队列、backpressure、rate limit 或 429。这样 500 的结果代表真实 500 并发，而不是被准入机制隐藏后的排队结果。

`legacy_session_projection_compaction` 是 `DeriveMessages()` 后再 `Compact()` 的诊断 workload；它不代表当前默认 Agent 执行路径。`agent_production_recent_compaction` 通过实际 `Agent.RunTurn()`，使用 `RecentTurnsCompactor` 的 fused projection，以及确定性的本地模型和工具 runtime，作为当前默认路径的主要参考。

## 输入场景

| 场景 | payload | projection messages | tools |
|---|---:|---:|---:|
| small | 1 KiB | 8 | 32 |
| typical | 16 KiB | 32 | 128 |
| max | 1 MiB | 4 | 512 |

`max` 使用当前工具参数的 1 MiB 合法上限（`core.MaxToolArgumentBytes`）。Session event 的独立硬上限是 16 MiB；默认基线不把 16 MiB 复制 500 份，以免用一次不可执行的分配掩盖其它热点。可以修改工具输入场景后另行测量该上限。

## 运行

只跑默认矩阵并写入被 `.gitignore` 忽略的 `data/`：

```text
go run ./cmd/perf-p0 -out data/perf-p0-baseline
```

命令行默认先构建一次自身可执行文件，再通过 `-perf-child` 为每个 case 启动独立子进程；因此 CLI 报告的 `process_isolated=true`，每个 case 的 RSS/heap 不继承前一个 case。`perfp0.Run` 是同进程库接口，保留给库测试和归因使用，不应与 CLI 的 P5 结果混称。

小型快速回归：

```text
go run ./cmd/perf-p0 -concurrency 10 -scenarios small -out data/perf-p0-small
```

增加 30 秒控制面 soak：

```text
go run ./cmd/perf-p0 -soak 30s -soak-workers 100 -out data/perf-p0-soak
```

每个独立 case 和 soak 都会先执行一次 `runtime.GC()`，再启动 sampler 和 case 计时，以尽量隔离前序 workload 的 Go live-heap；这次 GC 不计入 case latency。不会调用 `debug.FreeOSMemory`，所以 RSS 不会被人为伪造。采样器默认每 1ms 读取一次 case 运行中的 Go heap/goroutine，并在 Windows 上读取当前进程 working set 与 kernel/user CPU time；可用 `-sample-interval 5ms` 调低开销。采样器只在 case 期间存在，不参与请求准入。更低间隔会增加 `runtime.ReadMemStats`、系统调用和一个采样 goroutine 的开销；高频瞬时峰仍可能落在采样间隔之间，因此报告使用 `max_seen` 而不是声称绝对 peak。

输出两个文件：

- `<prefix>.json`：机器可读的 case、吞吐、p50/p95/p99、Go heap/alloc/goroutine/GC 数据；其中 `heap_alloc_max_seen_bytes` 和 `rss_peak_bytes` 是采样期间观测到的最大值，不冒充绝对峰值；
- `<prefix>.md`：不包含原始 payload 或全部样本的简洁表格。

## 指标边界

当前基线记录 Go runtime 的 heap、累计分配、malloc/free、采样期间 goroutine 最大值、`PauseTotalNs` 增量和 GC 次数。Markdown 中的 `Ops` 和 `Attempt ops/s` 都统计完成的调用尝试，包括返回错误的尝试；`Errors` 单列，只有 `Errors == 0` 时才可将吞吐用于成功路径比较。Windows 构建记录 working set before/after/peak、进程 kernel/user/total CPU time 增量及由 wall time 推导的进程 CPU 百分比；该百分比是聚合 CPU 时间，多核进程可能超过 100%。非 Windows 构建明确为 `unsupported`。`perfp0.Run` 这个库接口的 case 在同一进程内执行：每个 case 虽在计时前执行一次 `runtime.GC()`，但不会调用 `debug.FreeOSMemory`，所以其 RSS 会受到前序 case 和 Go arena 保留影响，不能表述为隔离 case 的常驻内存。CLI 默认使用 P5 的 `RunIsolated`，每个 case 在独立 child 中采样；这消除了跨 case 污染，但 child 内的 allocator/arena 保留和采样间隔仍然存在。短 case 也会受 Windows 计时分辨率影响。不会用 heap 数字冒充 RSS，也不会把暂未测量的 provisional 目标标为通过。

## 历史 P4：同进程 GC 隔离两轮结果

下表是 production-shaped `Agent` workload 在 500 并发、两轮各自先 GC 的范围，不是单次数字：

| workload | payload | heap peak | p95 | errors |
|---|---:|---:|---:|---:|
| `agent_production_recent_compaction` | typical 16 KiB | 161–198 MiB | 162.6–200.1 ms | 两轮均为 0 |
| `agent_production_recent_compaction` | max 1 MiB | 1.54–1.76 GiB | 2.83–2.94 s | 两轮均为 0 |

max/500 case 的 RSS peak 在两轮间为 1.84–3.53 GiB。该结果是 P4 历史同进程基线，RSS 会受到前序 case、Go arena 和 allocator 保留影响；它不是当前 CLI 的隔离 case 结论，也不是每个执行的长期常驻量。

500 并发 HTTP `/healthz` 也跑了两轮：一轮 0 错，另一轮有 200 transport errors。不能据此宣称 500 HTTP 已稳定通过（not evidence that 500 HTTP is stably passing）；这是 P4 同进程冷启动/连接突发的历史诊断，不能与 P5 的独立 child 结果混淆。P5 fresh-child 的 500 冷启动 case 中 500 次操作有 222 成功、278 失败，失败统一归类为 `cold_start_transport`，所以该 case 也不是通过。P5 报告会将 fresh-child 的连接突发错误归类为 `cold_start_transport`，可选 soak 则归类为 `steady_keepalive_transport`；错误样本只保留有界、脱敏后的分类，不隐藏错误。production Agent workload 的上述两轮则均为 0 错。所有结果仍代表真实并发，不通过 admission、队列、backpressure、rate limit 或 429 来改变负载。

此前的 384 MiB/500 并发数字只是 provisional target，已不适用于 max 场景：500 * 1 MiB 在未计 context、headers、maps、request/response copies、运行时开销和 GC/allocator 保留前，就已有 500 MiB 原始信息下界。本切片不以此替换为新 gate。

## 真实临时 SQLite 服务短测

独立临时 `cmd/server` Windows exe、dev/bootstrap SQLite/data 和独立端口的 `/healthz` 短测结果如下。构建进程在测量前已退出，统计 PID 仅为 server exe；临时数据和日志均在测量后删除。

| 场景 | 窗口 | Working Set max | Private Memory max | normalized CPU | 结果 |
|---|---:|---:|---:|---:|---|
| idle | 20 s | 19.69 MiB | 19.04 MiB | 0% | goroutine 指标无公开端点，标为 unsupported |
| 单客户端 `/healthz` 轻载 | 20 s | 21.95 MiB | 21.11 MiB | 0.0039% | 96 成功、0 失败、client p95 1.778 ms |

这是短时 idle/轻载观察，不是长期 soak，也不是完整 500 HTTP 稳定性证明。

## 16 MiB streaming 结果

16 MiB streaming benchmark 的 Go heap 分配约为 File 60 KiB/op、S3 约 168 KiB/op；payload 不会线性进入 Go heap。这个结果不消除对象后端为完整性/替换语义使用的受限临时存储，也不等同于所有 session/cold-layout 路径已经流式化。

## Perf-P5：按 case 进程隔离的 prepared RunTurn

P4 的同进程顺序矩阵会受到 Go arena 和前序 case RSS 保留影响。P5 将命令行默认路径改为：先构建一次 `cmd/perf-p0`，再为每个 case 启动同一可执行文件的 `-perf-child`；父进程通过有界 JSON stdin/stdout 协议合并结果。`perfp0.Run` 仍保留同进程库测试能力。报告 `version=3`，并显式写入 `process_isolated=true`。

prepared production case 的 Session、Agent、TurnInput 全部在 sampler 和计时前创建；`runtime.total_alloc_delta` 与 process RSS/heap 的 measured 区间只覆盖并发 `RunTurn`。`fixture` 字段单独记录 setup 后 heap/RSS baseline 和 retained/delta。RSS 下降是合法的有符号 delta，不再被标成 unsupported；只有采样失败才会标记 unsupported。

产物：`data/perf-p5-2026-09-04.json`、`data/perf-p5-2026-09-04.md`，以及 `data/perf-p5-typical500-{cpu,mem}.pprof`、`data/perf-p5-max500-{cpu,mem}.pprof`。P5 全矩阵覆盖 small/typical/max × 10/100/500；prepared production case 均为 0 errors。最新一次 fresh-child control health 500 并发为 500/500 成功，但此前同一命令的一次独立运行是 222 成功、278 失败，失败统一归类为 `cold_start_transport`。两次结果共同说明冷启动连接突发存在运行时波动，不能把单次 0 错宣称为稳定性通过；失败证据没有以重试、准入、429 或限流隐藏。

独立子进程中的重点结果如下（Windows working set，按单个 case 观察）：

| workload | concurrency | p95 | fixture heap retained | fixture RSS delta | RunTurn heap max | RunTurn alloc delta | RSS peak |
|---|---:|---:|---:|---:|---:|---:|---:|
| control health | 10 | 2.18 ms | 0 MiB | 0 MiB | 1.10 MiB | 0.43 MiB | 11.24 MiB |
| prepared typical 16 KiB | 500 | 18.81 ms | 295.56 MiB | 397.52 MiB | 530.51 MiB | 0.23 GiB | 0.53 GiB |
| prepared max 1 MiB | 500 | 812.03 ms | 2.95 GiB | 3.87 GiB | 6.90 GiB | 9.05 GiB | 6.99 GiB |

这些数字区分了常态与极端：小请求/控制面仍是几十 MiB 级常驻基线的候选范围，但本次 500 并发 16 KiB 达到约 0.53 GiB RSS，500 并发 1 MiB 达到约 6.99 GiB RSS。极端值包含输入驻留、消息/工具结构、复制、GC 和 allocator 行为，不能用几百 MiB 常态目标覆盖；也不能通过 admission、排队或 429 把它误报为系统本身的低内存消耗。

P5 另做了 500 worker、5 秒的两阶段稳态 keepalive 对照（独立 child 内复用同一个 HTTP transport，非冷启动连接突发）。固定 250ms warmup 单独记录 14,176 成功、6,052 `warmup_transport` 失败；随后 measured 5.002 秒窗口完成 732,919 次操作，732,919 成功、0 失败。warmup 的失败没有被隐藏或并入 measured，measured 的 0 错也不构成长期稳定性证明；该结果只说明本次 warmed window 未观察到错误。完整产物为 `data/perf-p5-control-soak-500.json` 与 `.md`。
