# Pre-tool durable checkpoint 性能量化

日期：2026-09-06。测量基线 Git HEAD：`a1bcede`（`fix(runtime): checkpoint tool calls before side effects`）。测量时工作树还包含另一项未提交的显式 session repair 工作；它不修改本报告直接测量的 `WriteBehind`、checkpoint wrapper 或 Agent 工具路径。性能采集与审查只修改两份 benchmark/measurement 文件与本文档，生产代码不在本报告变更范围内。

## 结论摘要

- `WriteBehind.Checkpoint` 在没有 pending event 时中位数为 **20.31 ns/op，0 B/op，0 allocs/op**。
- 单个 pending prefix 使用 `MemorySessionStore` 时中位数为 **5.228 µs/op，3,533 B/op，43 allocs/op**；使用本地文件 SQLite 时为 **345.617 µs/op，8,675 B/op，183 allocs/op**。
- background flush 在途交接场景中位数为 **3.735 µs/op，392 B/op，5 allocs/op**。这是 scheduler-sensitive 的合成 in-memory 协调/排空成本，不是数据库延迟或稳定 tail 指标。
- 完整本地工具回合的 Go benchmark 中，启用 durable checkpoint 后：
  - Memory：`67.033 → 101.869 µs/op`，增加 **34.836 µs / 51.97%**；`51,863 → 67,503 B/op`；`584 → 788 allocs/op`。
  - 文件 SQLite：`426.318 → 687.770 µs/op`，增加 **261.452 µs / 61.33%**；`44,459 → 52,208 B/op`；`482 → 668 allocs/op`。
- 500 个批次、每批 32 个独立操作的归一化分布中，durable checkpoint 相对 direct journal：
  - Memory：p50 增加 **31.222 µs**，mean 增加 **20.935 µs**，单 worker measured-path 吞吐由 **14,451.60** 降至 **11,094.84 ops/s**（-23.23%）。
  - 文件 SQLite：p50/p95/p99 分别增加 **272.810 / 286.219 / 324.335 µs**，mean 增加 **193.403 µs**，吞吐由 **3,285.57** 降至 **2,008.98 ops/s**（-38.85%）。
- no-op checkpoint wrapper 与 direct journal 的 allocs/op 相同；SQLite batch mean 和吞吐分别只相差 1.487 µs 与 -0.49%。Memory batch p99/mean 受到明显调度离群值影响，因此不把单轮 p99 或 no-op 差异解释成 wrapper 固有成本。
- 本机没有配置 `HARNESS_TEST_PG_DSN`。真实 PostgreSQL **未测**，本文不使用 Memory 或 SQLite 数字代替 PostgreSQL 结论。

## 环境与边界

| 项目 | 值 |
| --- | --- |
| OS / arch | Windows / amd64 |
| Go | go1.25.13 |
| CPU | 12th Gen Intel Core i7-12700K |
| GOMAXPROCS | 20 |
| benchmark | `-benchtime=300ms -count=5 -benchmem` |
| latency measurement | 每个 backend/mode 500 批，每批 32 个独立操作，共 16,000 次操作 |
| cache / temp | `GOCACHE=D:\cc\auto_agent\.codex-gocache`；`GOTMPDIR=D:\cc\auto_agent\.codex-gotmp`；`TEMP/TMP=D:\cc\auto_agent\.codex-test-tmp` |

没有调用真实 LLM、HTTP 模型、S3、云沙箱或其他外部服务，没有读取 credentials。模型夹具只在当前进程内按固定顺序返回一次 tool call 和一次最终文本。

本机没有独占给 benchmark，结果可能受到调度、CPU 频率、文件系统缓存、GC 和其他进程影响。所有吞吐均为单 worker、只覆盖明确 measured path 的计算值，不是服务容量或生产 SLA。

## 测量方法

### WriteBehind 微基准

源码：[write_behind_benchmark_test.go](../../pkg/storage/write_behind_benchmark_test.go)。

- `noop/memory`：空 Session 已经持久化，重复调用同一个 writer 的 `Checkpoint`。
- `single_pending_prefix/memory`：每次迭代在计时区外新建 Session/Memory store 并追加一个事件，只计同步 `Checkpoint`。`MemorySessionStore` 没有实现 `SessionAppender`，因此这里覆盖其 snapshot clone + `Save` fallback。
- `single_pending_prefix/sqlite_file`：使用 `modernc.org/sqlite` 和 D 盘临时文件。schema 初始化、旧 Session 清理、Session 创建和事件追加都在计时区外；只计一个 event chunk 的 `Checkpoint` transaction。
- `background_flush_contention/in_memory_appender`：第一个 prefix 已经进入并阻塞在合成 appender，随后追加第二个事件；计时区内启动 `Checkpoint`、释放在途 appender并等待剩余 prefix 排空。没有注入固定 sleep 或伪造 I/O 延迟，但 goroutine 调度点无法在不修改生产实现的前提下完全固定，因此它只用于观察协调量级，不作为稳定 tail latency。

Go benchmark 报告的 `ns/op` 是其自适应重复计时结果；`B/op` 和 `allocs/op` 只统计计时区。表中时间取 5 次结果的中位数，范围保留最小值和最大值。

### 受保护工具路径

源码：[server_tool_checkpoint_benchmark_test.go](../../pkg/server/server_tool_checkpoint_benchmark_test.go)。每个 measured operation 包含：

1. `Agent.RunTurn` 的两次进程内固定模型调用。
2. assistant/tool-call 事件追加、core guarded tool funnel、ToolJournal begin/complete。
3. 一个无 I/O 的本地确定性工具副作用。
4. tool result、最终 assistant/run end 事件追加。
5. `WriteBehind.Flush`，确保 direct baseline 与 checkpoint arm 都包含最终持久化。

三个 mode：

- `direct_journal`：guard 直接调用确定性内存 journal；只在回合末 Flush。
- `noop_wrapper`：安装与生产 checkpoint journal 同形的 wrapper，但 checkpoint callback 直接成功返回；用于隔离包装、间接调用和 failure bookkeeping。
- `durable_checkpoint`：通过 `runtimeWithToolCheckpoint` 安装真实 `writer.Checkpoint`；工具副作用前持久化当时的 session prefix，回合末再 Flush 剩余事件。

Session store 分为 `MemorySessionStore` 和本地文件 SQLite。每个 mode 使用独立 backend；SQLite 因而使用独立临时数据库文件，配置 PostgreSQL 时每个 mode 使用独立随机 schema。SQL reset 在单个事务中按 `event_chunks → sessions` 顺序完成。schema 初始化、reset、Session 创建、Agent/writer/model/tool 构造都在计时区外。journal 是确定性内存夹具，因此这里量化的是 **pre-tool session checkpoint**，不包含 SQL Tool Journal transaction。

每个 mode 在计时前执行一次完整 preflight：验证两次模型调用、一次工具调用、completed 状态、`SavedVersion == Session.Version`，并从 store 重新加载后逐事件比较。正式 benchmark 每次迭代结束后只做无 I/O 状态验证，避免计时外深拷贝加载制造额外 GC 压力并污染后续迭代；执行结果保存在 operation 对象中并在计时后读取，工具与持久化副作用也都被观察，因此不能被编译器安全消除。

### p50/p95/p99 与吞吐

Windows 本机对亚毫秒 `time.Now` 的逐操作读数出现大量相同刻度，初次逐操作方案会产生错误的 `p50=0s`，该结果已废弃。正式 measurement 在计时前预建 32 个互相独立的 Session/Agent，然后对整批计时，并以批次总时长除以 32 得到 batch-normalized per-operation 样本：

- 每个 backend/mode 500 个归一化样本，即 16,000 次成功操作。
- p50/p95/p99 使用排序后的 nearest-rank。
- mean 和吞吐使用全部 measured batch 时间；吞吐为 `16,000 / measured_seconds`。
- setup/reset 不在 latency 或 throughput 分母内；没有使用 `go test` 包总耗时。
- 每个样本轮换第一个 mode，避免 direct/no-op/durable 长时间整块顺序运行造成的系统负载漂移偏置；每个 mode 仍使用自己的 backend。
- 每批计时结束后逐个执行与 preflight 相同的完整持久化验证。验证本身不计时，但和任何计时外 setup 一样，仍可能间接影响后续 GC、文件缓存和调度，这属于本地微基准局限。

这种方法解决了本机计时粒度问题，但分位数是“批均值的分布”，会平滑批内单请求尖峰。因此本文明确称为 **batch-normalized p50/p95/p99**，不能当作真实单请求 tail latency。

## WriteBehind Checkpoint 结果

| 场景 | 时间中位数 | 5 次范围 | B/op | allocs/op | 中位数等价吞吐 |
| --- | ---: | ---: | ---: | ---: | ---: |
| no-op / Memory | 20.31 ns | 19.85–20.87 ns | 0 | 0 | 49.24 M ops/s |
| 1 pending / Memory | 5.228 µs | 4.863–9.893 µs | 3,533 | 43 | 191.28 k ops/s |
| 1 pending / SQLite file | 345.617 µs | 339.702–350.033 µs | 8,675 | 183 | 2.89 k ops/s |
| background handoff / synthetic appender | 3.735 µs | 3.651–3.818 µs | 392 | 5 | 267.74 k ops/s |

五次时间原始值：

```text
noop/memory: 20.04, 19.85, 20.31, 20.81, 20.87 ns/op
single_pending_prefix/memory: 5060, 4863, 5228, 9893, 9874 ns/op
single_pending_prefix/sqlite_file: 339702, 341097, 345617, 349415, 350033 ns/op
background_flush_contention/in_memory_appender: 3690, 3651, 3735, 3798, 3818 ns/op
```

## 完整工具回合 benchmark

| Backend | Mode | 时间中位数 | 5 次范围 | B/op | allocs/op |
| --- | --- | ---: | ---: | ---: | ---: |
| Memory | direct journal | 67.033 µs | 65.056–67.745 µs | 51,863 | 584 |
| Memory | no-op wrapper | 66.606 µs | 66.372–71.163 µs | 51,866 | 584 |
| Memory | durable checkpoint | 101.869 µs | 91.279–162.800 µs | 67,503 | 788 |
| SQLite file | direct journal | 426.318 µs | 414.708–444.855 µs | 44,459 | 482 |
| SQLite file | no-op wrapper | 437.823 µs | 419.952–454.513 µs | 44,458 | 482 |
| SQLite file | durable checkpoint | 687.770 µs | 685.697–702.946 µs | 52,208 | 668 |

五次时间原始值：

```text
memory/direct_journal: 67724, 65056, 67033, 67745, 66429 ns/op
memory/noop_wrapper: 66387, 66606, 66372, 67340, 71163 ns/op
memory/durable_checkpoint: 91279, 101869, 91814, 119583, 162800 ns/op
sqlite_file/direct_journal: 430822, 426318, 444855, 414708, 419956 ns/op
sqlite_file/noop_wrapper: 419952, 437823, 449333, 454513, 437421 ns/op
sqlite_file/durable_checkpoint: 686607, 685697, 690603, 702946, 687770 ns/op
```

Memory 与 SQLite 的 no-op wrapper 和 direct 时间范围都重合，allocs/op 也不变。Memory durable 的 5 次结果出现明显非平稳漂移，因此 benchmark 中位数只作为该次运行的描述值；下面轮换 mode 的 batch measurement 更适合比较持续运行时的中心趋势。不能把 no-op 的某次快慢解释成 wrapper 固有改善或回退。

durable checkpoint 相对 direct 的 benchmark 中位数增量：

| Backend | 时间增量 | 相对增量 | B/op 增量 | allocs/op 增量 |
| --- | ---: | ---: | ---: | ---: |
| Memory | +34.836 µs | +51.97% | +15,640 | +204 |
| SQLite file | +261.452 µs | +61.33% | +7,749 | +186 |

## Batch-normalized 分布与吞吐

| Backend | Mode | 操作数 | p50 | p95 | p99 | Mean | 单 worker measured-path throughput |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Memory | direct journal | 16,000 | 62.509 µs | 93.906 µs | 176.559 µs | 69.196 µs | 14,451.60 ops/s |
| Memory | no-op wrapper | 16,000 | 62.512 µs | 93.934 µs | 354.046 µs | 74.258 µs | 13,466.49 ops/s |
| Memory | durable checkpoint | 16,000 | 93.731 µs | 106.546 µs | 129.706 µs | 90.131 µs | 11,094.84 ops/s |
| SQLite file | direct journal | 16,000 | 218.768 µs | 656.259 µs | 740.621 µs | 304.361 µs | 3,285.57 ops/s |
| SQLite file | no-op wrapper | 16,000 | 229.818 µs | 665.087 µs | 735.996 µs | 305.848 µs | 3,269.60 ops/s |
| SQLite file | durable checkpoint | 16,000 | 491.578 µs | 942.478 µs | 1.064956 ms | 497.764 µs | 2,008.98 ops/s |

`noop_wrapper` 的 SQLite p99 低于 direct 是样本波动，不能解释为性能提升。它的 mean 和 throughput 分别只相差 1.487 µs 与 -0.49%。Memory 的 no-op p99 出现 354.046 µs 离群，durable p99 反而低于 direct/no-op；这说明本机 batch p99 仍受调度尖峰支配，不能单独用于判断 checkpoint 的 tail 因果效应。

## PostgreSQL 状态

measurement 和 benchmark 都支持在 `HARNESS_TEST_PG_DSN` 已配置时调用 `internal/testdb.Postgres`：每个 mode 创建独立随机 schema，所有 pool 关闭后只删除自己拥有的 schema。当前进程没有该环境变量；benchmark 明确显示 `SKIP`，measurement 输出为：

```text
CHECKPOINT_PERF backend=postgres status=not_measured reason=HARNESS_TEST_PG_DSN_not_configured
```

因此本文没有 PostgreSQL 样本数、p50/p95/p99、吞吐或 allocs/op。SQLite 是本地文件 demo/self-host backend，不能外推 PostgreSQL 的网络、WAL、fsync、连接池、锁竞争或事务延迟。

## 可复跑命令

以下命令均从仓库根目录运行：

```powershell
$env:GOCACHE='D:\cc\auto_agent\.codex-gocache'
$env:GOTMPDIR='D:\cc\auto_agent\.codex-gotmp'
$env:TEMP='D:\cc\auto_agent\.codex-test-tmp'
$env:TMP='D:\cc\auto_agent\.codex-test-tmp'

go test ./pkg/storage -run '^$' `
  -bench '^BenchmarkWriteBehindCheckpoint$' `
  -benchtime=300ms -count=5 -benchmem

go test ./pkg/server -run '^$' `
  -bench '^BenchmarkProtectedToolCheckpoint/(memory|sqlite_file)/' `
  -benchtime=300ms -count=5 -benchmem

$env:HARNESS_RUN_CHECKPOINT_PERF='1'
$env:HARNESS_CHECKPOINT_PERF_SAMPLES='500'
$env:HARNESS_CHECKPOINT_PERF_BATCH='32'
go test ./pkg/server `
  -run '^TestPreToolCheckpointLatencyMeasurement$' `
  -count=1 -v
```

若本机已经按项目约定配置 `HARNESS_TEST_PG_DSN`，同一个 measurement 命令会额外运行隔离 PostgreSQL case；benchmark 可用：

```powershell
go test ./pkg/server -run '^$' `
  -bench '^BenchmarkProtectedToolCheckpoint/postgres/' `
  -benchtime=300ms -count=5 -benchmem
```

本次原始输出保存在 Git 工作树外：

```text
D:\cc\auto_agent\.codex-test-tmp\checkpoint-storage-bench-review-2026-09-06.txt
D:\cc\auto_agent\.codex-test-tmp\checkpoint-tool-bench-review-2026-09-06.txt
D:\cc\auto_agent\.codex-test-tmp\checkpoint-latency-review-2026-09-06.txt
```

## 验证状态

以下检查通过：

- 两组正式 benchmark 与 opt-in latency measurement。
- `go test -race ./pkg/storage -run '^$' -bench '^BenchmarkWriteBehindCheckpoint$' -benchtime=100x -count=1`。
- `go test -race ./pkg/server -run '^$' -bench '^BenchmarkProtectedToolCheckpoint/(memory|sqlite_file)/' -benchtime=50x -count=1`。
- 未配置 DSN 时 PostgreSQL benchmark 明确 `SKIP`，measurement 明确记录 `status=not_measured`；没有执行真实 PostgreSQL，也没有用其他 backend 代替。
- WriteBehind checkpoint/flush/background append 定向正确性测试。
- server tool checkpoint 顺序、失败取消和原始错误保留定向测试。
- `go test ./pkg/storage ./pkg/server -count=1`。
- `go vet ./pkg/storage ./pkg/server`、新增 Go 文件 `gofmt -d`、`git diff --check` 和本文本地链接检查。

## 局限与后续

- 分位数是 batch-normalized per-operation，不是单请求 tail；若要正式单请求 p99，应在 Linux/生产目标 OS 上使用足够精度的 monotonic clock 或外部负载发生器。
- 单 worker、无并发 Session、无数据库连接池竞争、无多进程 worker；不能推导高并发吞吐或 scale-out SLA。
- setup/reset、Session 创建和 schema migration 不在 measured path；吞吐不是端到端 API 吞吐。
- 计时外 setup/验证虽然不进入 duration 和 allocation 统计，仍可能通过 GC、OS page cache、SQLite 文件系统状态和调度间接影响下一批；mode 轮换和独立 backend 只能降低、不能消除该影响。
- 固定模型没有网络、tokenization、provider serialization、流式等待或重试；结果只量化框架局部成本。
- journal 是内存夹具；真实 SQL Tool Journal 会有独立 transaction 成本。本文只回答 pre-tool session checkpoint 的增量。
- SQLite 使用本机文件系统与当前默认 driver/schema 设置；冷盘、不同同步策略、杀进程 durability 和长期 WAL 行为未测。
- synthetic contention 只量化 WriteBehind 协调与第二个 prefix 排空，不代表慢数据库在途时的等待分布。
- 未固定 CPU affinity、频率或进程优先级，也没有长期 soak；5 次范围是观测范围，不是统计置信区间。Memory pending 与 durable benchmark 均观察到非平稳漂移，绝对值不应脱离原始范围引用。
- 真实 PostgreSQL 是剩余的主要证据缺口。具备本地 testdb 后应至少保留 500 个 batch-normalized 样本，并另做并发单请求测量，分别报告数据库和完整工具回合的分布。
