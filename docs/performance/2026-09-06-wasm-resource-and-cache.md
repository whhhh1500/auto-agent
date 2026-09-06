# WASM 资源限制、编译缓存与真实会话验收

日期：2026-09-06。修复前基线 `4ec3307`；修复后为与本记录一同提交的代码。范围是 M27 外层 WASM 执行器，不改变 `pkg/core` 公共接口，不改变 Windows Basic Sandbox，不声称已有 E2B 兼容接口。本轮有新的资源边界与重复执行收益，但没有证明整体 Agent 已优于其他框架。

## 发现与修复

旧实现使用 wazero 默认 runtime。独立测试子进程实际执行无限循环时，100 ms 的 deadline 和 cancel 都不能终止计算，两个子进程均在父测试的 4 秒硬超时下被结束；初始内存 2049 页的模块也被允许执行。旧源码关于“微秒、很小占用”的注释没有本项目实测依据，已移除。

现在 `WazeroExecutor{}` 默认设定 2048 页（128 MiB）线性内存和 30 秒时限，强制开启运行中取消检查。调用方更短的 context deadline 优先；已取消的调用在文件读取前返回。模块文件必须为普通文件，仍受 32 MiB 大小、argv 和 1 MiB stdout 上限约束；runtime 清理不依赖已经取消的 context。默认取消检查关闭及页数语义已核对锁定依赖的 [wazero v1.12.0 RuntimeConfig 文档](https://pkg.go.dev/github.com/tetratelabs/wazero@v1.12.0#RuntimeConfig)与本机源码。

新增可选 `CompilationCache` 由宿主管理。最初仅接入缓存时，基准显示热调用仍约 524 ms，几乎无收益。检查锁定依赖的 `InstantiateWithConfig` 源码后发现，该便捷方法会把编译代码的关闭绑定到实例退出；Go WASI 的 `proc_exit` 会清除缓存中的编译结果。最终改为显式 `CompileModule` / `InstantiateModule`，由 runtime（无缓存）或宿主 cache（共享缓存）管理编译代码生命周期，每次调用仍有独立实例、内存、argv 和输出。该生命周期发现来自源码和本地 A/B 测量，首轮未生效样本也保留于下文。

## 接入与扩展合同

配置全部在外层 [WazeroExecutor](../../pkg/execution/wazero.go)，core 的 `Executor` / `ExecutionSpec` 无变更。既有 `WazeroExecutor{}` 调用继续可用，但现在具有默认资源上限；超过默认要求的已注册模块应由宿主明确调整配置。

```go
cache := wazero.NewCompilationCache()
defer cache.Close(context.Background()) // 停止使用这些执行器后关闭

executor := execution.WazeroExecutor{
    MemoryLimitPages: 1024,            // 64 MiB; zero = 128 MiB
    Timeout:          5 * time.Second, // zero = 30 seconds
    CompilationCache: cache,          // nil = no shared compilation cache
}
// registry.RegisterExecutor(scope, manifest, executor)
// manifest.Execution.Runtime = "wazero"; Entrypoint 指向已注册本地 WASI 模块。
```

非零页数必须不超过 65536，负时限返回错误，零值不能用于关闭限制。取消/超时错误可通过 `errors.Is(err, context.Canceled/DeadlineExceeded)` 判断。资源配置进入 `ArtifactRevision()`，默认和显式等价配置得到相同标识；策略变化改变标识。缓存配置不改变 guest 语义，不计入标识。

缓存不是全局单例，不会自动安装到默认服务器；集成方需要在组合根明确注册。宿主拥有缓存模块集合和关闭时机，宜为有限、可信的模块目录或版本批次共享，模块更新后轮换/关闭旧缓存；wazero 的此接口不提供项目自有 LRU 配额。缓存常驻的编译代码有成本，B/op 不包含这部分保留量。相同路径文件内容变化会重新编译；收紧策略时，初始内存校验和运行中的 `memory.grow` 限制都仍生效。

128 MiB 是 **guest 线性内存上限**，不是整个 Go 进程 RSS 上限；编译代码、解析结构、表和 Go heap 不包含在内。时限从调用开始计算，但 guest 循环取消不是对任意文件读取或编译阶段的操作系统硬抢占。模块路径由注册方提供；现有 WASI 未挂载宿主文件系统、继承宿主环境或额外注入网络 host functions。需要整个进程的硬隔离与资源配额时，仍应使用独立受限执行环境。默认资源策略是可配置的工程选择，不是所有 WASI 程序的容量承诺。

## 有界回归结果

[执行器测试](../../pkg/execution/wazero_test.go)使用实际 WASM 字节码，不使用假 runtime；Go WASI 测试使用仓库 [internal/calc](../../internal/calc/main.go) 编译结果。

| 验收项 | 实际结果 |
| --- | --- |
| 初始内存超过默认 2048 页 | 旧版通过，新版拒绝 |
| 无限循环响应 caller deadline / cancel | 旧版均由父进程 4 s 硬超时终止；新版子进程各约 0.11 s 正常结束 |
| 无 caller deadline、执行器自设 100 ms | 约 0.11 s 正常结束；返回 DeadlineExceeded |
| 共享缓存下重复取消、取消后正常调用 | 两次循环约 0.21 s，下一次有限调用成功 |
| memory.grow 在自定义 2 页以内/以外 | 成功返回旧页数；超限返回 -1 |
| 缓存后收紧内存策略 | 超限初始内存被拒绝；原本可增长到第 3 页的代码在新策略下不能访问第 3 页 |
| 缓存状态隔离、同路径模块替换 | 三次调用计数都从 0 开始；替换成 trap 的模块实际触发 trap |
| WASI stdout、输出超限、trap、非法二进制、失败后调用 | 正确输出 / 明确失败 / 后续可继续调用 |
| Go WASI argv / stdout | `[137,-29,8] → 116`、`[1,2] → 3`、无参数 `→ 0` |

这些短循环验证取消路径，不是对所有恶意二进制的安全证明。测试父进程有额外硬超时，防止取消回归留下无限运行 guest；子进程临时目录由父测试持有并清理。

## 完整 Run 性能实测

环境：Windows/amd64、Intel i7-12700K、Go 1.25.13、wazero 1.12.0、GOMAXPROCS=20。串行运行；测量期间无本轮 PostgreSQL 和 LLM 工作。模块 `internal/calc` 编译时使用 `GOOS=wasip1 GOARCH=wasm go build -p 1 -trimpath`，大小 **2,500,210 B**，SHA-256 **`f966fcb3d129dd01e9240262b81c98e00bb1b73de86d0ea5eeedaf59ed26dffd`**。Go 构建缓存与临时目录在 D 盘。

每种模式 5 个样本，每样本 3 次完整 `Run`，覆盖读取、解析、编译/查询缓存、WASI 设置、guest 执行和 runtime 清理。模块构建不计时；热缓存预热不计时；冷缓存模式把 cache 创建与关闭计入。所有调用检查输出 `116`。

| 最终实现，5 样本中位数 | 不共享缓存 | 新建冷缓存 | 已预热缓存 |
| --- | ---: | ---: | ---: |
| ns/op | 528,274,700 | 530,021,800 | 19,795,900 |
| B/op（累计分配） | 65,464,152 | 65,463,933 | 32,596,741 |
| allocs/op | 228,114 | 228,113 | 187,141 |

此模块热缓存相对不共享缓存：耗时 **降低 96.25%（26.69 倍）**，每次累计分配字节 **降低 50.21%**，分配次数 **降低 17.96%**。冷启动几乎没有收益。热缓存仍需解析和分配 guest 内存，32.60 MB/op 的剩余分配值得继续优化。这里没有测稳态 RSS、缓存保留字节、多租户负载或其他框架，不能把这些比例外推成整体服务的内存/速度收益，也没有本轮 token 节省对照。

最终原始样本，列为 `ns/op | B/op | allocs/op`：

| 模式 | 样本 1 | 样本 2 | 样本 3 | 样本 4 | 样本 5 |
| --- | --- | --- | --- | --- | --- |
| uncached | 522572800 / 65464152 / 228114 | 530550400 / 65467453 / 228118 | 536448333 / 65464120 / 228113 | 525432000 / 65464104 / 228112 | 528274700 / 65465736 / 228115 |
| cache_cold | 526606167 / 65463933 / 228114 | 530021800 / 65463896 / 228113 | 533804200 / 65463746 / 228113 | 529198633 / 65464034 / 228113 | 545124633 / 65464296 / 228115 |
| cache_warm | 21707100 / 32596805 / 187142 | 19572300 / 32596586 / 187141 | 19617267 / 32597274 / 187142 | 19795900 / 32596666 / 187141 | 20795167 / 32596741 / 187141 |

首轮接入缓存但仍用 `InstantiateWithConfig` 的样本，同一口径；此实现已被替换：

| 模式 | 样本 1 | 样本 2 | 样本 3 | 样本 4 | 样本 5 |
| --- | --- | --- | --- | --- | --- |
| uncached | 522551567 / 65466157 / 228117 | 527042533 / 65464013 / 228113 | 528663033 / 65465874 / 228115 | 533281333 / 65463613 / 228111 | 528318533 / 65463885 / 228112 |
| cache_cold | 535548167 / 65464200 / 228114 | 532293233 / 65464045 / 228113 | 522689400 / 65464226 / 228114 | 538337833 / 65464200 / 228114 | 524520767 / 65464077 / 228114 |
| cache_warm | 522374533 / 65423293 / 227996 | 528372100 / 65423234 / 227995 | 525708567 / 65423522 / 227996 | 519659167 / 65423106 / 227995 | 523590267 / 65423378 / 227995 |

## 真实 Gemini、trace 与聊天历史

2026-09-06 14:14:10–14:14:15（Asia/Shanghai），使用用户指定端点和 `gemini-3.8-flash`，`HARNESS_LLM_MAX_TOKENS=2048`。只运行新增 `wasm_computation` 场景，**2 次真实请求、最大在途 1、自动重试 0**，用例耗时 **5.59 s**。两次分别上报 **139/198**、**180/60** 输入/输出 token，合计 **319 输入、258 输出**。

模型按要求调用 `serial.wasm_sum`，参数 `[137,-29,8]`，实际 WASI stdout 和模型最终回复均为 `116`。不是只判断模型能否自己算出数值。该调用为缓存首次使用，工具 span 为 **531 ms**，不能拿热缓存基准替代此真实会话的冷执行耗时。

- Run：`run_5b0ffa9a7a32467273df29fca362d91c`；Session：`sess_b7e87d81b10b5f556bc13b26dc993f38`。
- Run trace：`312479448e1f00b6adde1bcae2a47b99`，2 个模型 span、1 个工具 span，均关联该 Run 的根 span；另有独立 queue claim span，共 5 个 span。
- SQL 持久事件 13 条，HTTP 历史分页 5 页，逐条一致；工具调用/结果配对、唯一终态通过。重建 HTTP 服务与 SQL 连接池后历史完全相同，没有额外请求模型。这是同一 Go 测试进程内重建实例，不是强杀生产进程的恢复测试。
- SDK Reader 上下文累计值：1804 input bytes、1382 estimated input tokens、0 dropped groups。它们是本地保守估算的累计量，**不是上游实际 319 输入 token**。model span 第二次耗时 1946 ms 包含串行节流等待，上游适配器实测 1483 ms。
- [真实会话事件、trace、指标 JSON](../verification/evidence/2026-09-06-wasm-resource/run_5b0ffa9a7a32467273df29fca362d91c.json)。模型 continuation 已脱敏，无 key/endpoint 明文。

另外，用脚本模型、同一个真实 Go WASI 模块和 PostgreSQL 验证成功与拒绝两个路径，**没有 LLM 计费请求**。成功路径 [run_2092af…](../verification/evidence/2026-09-06-wasm-resource/run_2092afaf28f29d22749f25fb91a67a91.json) 返回 116 并完成重建后历史审核；资源拒绝路径 [run_daf3ee…](../verification/evidence/2026-09-06-wasm-resource/run_daf3ee12d5e8b1b909d803dd91e3451a.json) 把该 Go 模块 36 页初始内存限制为 1 页，实际编译失败，SQL 工具结果 `OK=false`，工具 span 为 `Error`、`tool.outcome=error`。两个路径各 12 条持久事件和 5 个 span。拒绝不是成功执行；Run 完成仅表示模型收到并报告了工具失败。

## 复跑与质量检查

普通测试自动构建仓库计算模块；指定 `HARNESS_TEST_WASM_CALC` 可复用预先编译的相同文件。`HARNESS_TEST_PG_DSN` 启用独立 schema 的数据库验收。LLM 测试只接受显式开关，模型/key 通过环境注入，不能写入命令行或文档。

```text
go test -p 1 -parallel 1 -count=1 -timeout=90s ./pkg/execution ./pkg/server -run "^Test(Wazero|PostgresSerialWASM)"
go test -p 1 -run "^$" -bench "^BenchmarkWazeroGoWASI$" -benchmem -benchtime=3x -count=5 -timeout=300s ./pkg/execution
# 仅显式启用 HARNESS_ACCEPTANCE_LIVE_SERIAL=1 后执行以下真实请求：
go test -p 1 -parallel 1 -json -count=1 -timeout=300s ./pkg/server -run "^TestLiveModelSerialModuleAcceptance$/wasm_computation$"
```

最终检查已通过：

- 全仓 `go test -p 1 -parallel 1 -count=1 -timeout=600s ./...`，开启本机 PostgreSQL、关闭 LLM 开关，未提供预编译模块路径，从测试自动构建 Go WASI 的路径也执行成功；server 21.240 s、storage 30.207 s。
- 全仓 `go build -p 1 ./...`、`go vet -p 1 ./...`、`staticcheck@v0.7.0 ./...`、gofmt。Staticcheck 首次指出 nil context 负向测试的 SA1012，已添加仅针对该测试行的说明并重跑通过；未关闭全局规则。
- `go test -race -p 1 -parallel 1 -count=1 -timeout=180s ./pkg/execution ./pkg/server -run "^Test(Wazero|PostgresSerialWASM)"`：execution 12.111 s、server 9.240 s。
- OpenAPI 102 个操作，core 公共 API / 体积 gate：34 个生产文件、8631 非空物理行、904 公共表面计数，与本轮前相同。
- `git diff --check`，改动 Markdown 本地链接检查，审计导出与暂存区的 key/endpoint 明文检查。

未在此轮重跑全仓 race、容器、漏洞扫描或 Windows Basic Sandbox 原生验收，不能把上述本机检查写成全部 CI job 已执行。

本机完整日志保留于忽略目录 `.tmp-wasm-resource-20260906`：01 为旧版失败，03/07/09 为资源与集成回归，06 为缓存未生效基准，08 为最终基准，10 为真实模型 JSONL，11–20 为质量检查。公开证据为上方三个已脱敏 JSON 和全部基准样本。

下一步仍包括工具按需披露的总 token/正确率实测、长历史与长任务恢复、缓存常驻 RSS 和跨框架公平任务集对照；本记录不替代这些工作。[设计与边界](../implementation/2026-09-06-wasm-resource-control.md)，[完整模块评估](../agent-module-assessment.md#m27)。
