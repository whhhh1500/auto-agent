# 程序化执行验收

状态：本轮约定范围验收完成。源码基线为本地 `ad90ac0` 加本次程序化执行工作树。所有模型均为确定性离线夹具，没有调用外部模型、发送通知或使用生产凭据。

随后按用户要求开展的外部模型测试单独记录在[真实模型验收](2026-09-09-programmatic-live-models.md)，包括真实请求发现的协议兼容问题与修复；本页保留此前离线与恢复验收的原始证据范围。

## 已执行证据

- `go test -count=1 ./internal/modulecheck ./pkg/app/runexecutor`：通过。包目录与依赖约束一致，Core 公开 API 与生产行数仍在原有固定预算内；`codeptc@1` 默认未注册，显式解析不回退。
- `go test -count=1 ./pkg/core ./pkg/app/contextassembly`：通过。真实 Runtime 提供惰性、经过 Profile/权限/过滤器筛选的快照目录；嵌套结果在父结果完成后不重复进入上下文与摘要；跨审批的完整程序组不会被滚动摘要截断。
- `go test -race ./pkg/execution/programmatic`：VM agent 执行通过。另有源大小、重复 JSON 键、深度、循环/工具预算、取消、原样错误传播、大整数比较和共享引用膨胀回归。
- `go test -run=^$ -fuzz=FuzzCompileAndRunBounded -fuzztime=5s -parallel=2 ./pkg/execution/programmatic`：本机完成 91,060 次 fuzz 执行，通过。成功解释的返回值继续经过宿主 JSON 序列化，并核对有界结果；不是长期 fuzz 或隔离证明。
- 真实 PostgreSQL 17.6、回环端口 55441：`go test -count=1 -timeout=180s -run=Test(NativeQueuedCompletedTool.*ProcessCrashRecovery|RunWorkerProcessCrashRecovery|FastRouterProcessCrashRecovery)$ ./pkg/server` 通过，测试包耗时 4.496 秒。覆盖既有 Native A/B、普通 worker 与 FastRouter 的实际子进程硬杀回归；这是既有恢复边界的兼容证据，不是新 PTC 任意崩溃点恢复证明。

本机工具链为 Go 1.25.13，Windows/amd64。原始本轮 PG 日志位于仓库外 `.codex-v46-audit/programmatic-20260909`；该路径不是公开仓库依赖。

## 本轮明确保留的边界

CodePTC 仅预留路由与未来宿主接口。PTC 执行受限 IR，不提供任意语言、文件、网络、进程、并行或异常捕获能力。程序内部效果不是一个跨工具事务；未知工具结果或未知模型 outcome 仍不得盲目重放。

离线模型夹具可以证明指令、执行路径、上下文和调用次数。它不能证明真实模型的语法成功率、模式选择质量、任务正确率、费用或线上延迟。

## 直接工具与 PTC 协议对照

执行 `go test -count=1 -v -run=TestRunWithActiveRowsShowsSimpleAndBatchShapes ./examples/programmatic`。两条路径都使用真实 Runtime、受保护工具 journal 和默认 ContextAssembler；最终模型上下文不包含无配对的嵌套工具结果。直接路径由固定模型读取列表及每个 detail 结果，再决定下一调用；程序路径执行实际 for/if 指令。

| 命中项 | 路径 | 模型回合 | 业务工具调用 | journal 调用 | 累计消息 JSON 字节 | 累计请求 JSON 字节 |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 直接 | 3 | 2 | 2 | 996 | 2,307 |
| 1 | PTC | 3 | 2 | 4 | 7,410 | 11,541 |
| 8 | 直接 | 10 | 9 | 9 | 14,653 | 19,023 |
| 8 | PTC | 3 | 9 | 11 | 7,705 | 11,836 |

两组最终名称列表分别完全一致。PTC 额外的 journal 项来自目录和父程序调用。请求字节是每次 `json.Marshal(core.GenerateOptions)` 的长度之和，包含 System、消息与工具 Schema；不是厂商 token，也不包含不可序列化的 admission 元数据。直接夹具只注册业务工具，PTC 夹具另外注册程序目录/执行能力；这是两种协议配置对照，不能当成同一 Profile 的真实模型选路实验。

完整语法随 `program.catalog` 返回，`program.execute` 的常驻描述只保留简短用法。小任务中目录和程序开销明显；本批量夹具中减少模型往返才开始占优。N=8 的三次本地微基准中，直接路径约 735–876 µs/op、557 KB/op，PTC 约 1.111–1.120 ms/op、701 KB/op；这是本机解释和保护链路成本，不能推导有网络和模型推理时的总延迟。对应运行方式及限制见 [示例](../../examples/programmatic/README.md)。

## SQL 实例替换与失败边界

`go test -count=1 ./pkg/adapter/programmatic/toolcapability -run 'TestProgram(Resume|Nested)'` 已在 SQLite 与真实 PostgreSQL 隔离 schema 上通过（PG 环境变量同上，包耗时 3.124 秒）。涵盖新 Runtime 从 SQL Session 恢复、已完成子效果不重放、绑定更换/撤销后待执行效果为零、未知 child journal 阻止后续效果、父子共享工具次数预算。

这组 Runtime 级测试使用 SQL 存储和已决定的 DurableApprover 接缝；它不等于 server 外部审批 API 验收。真实 SQLApprovalStore 决定审批还需要 native run_control 状态，完整 server 审批闭环另行验证，不把缺少该状态的直接 Runtime 夹具写成端到端通过。


完整 server 闭环另由 `TestNativeQueuedProgramApprovalResumesReplacementWithoutReplayingChild` 与 `TestNativeQueuedProgramUnknownChildFailsClosedAfterReplacement` 在 SQLite/PG 上通过（2.004 秒）：真实 HTTP 审批、SQL run_control、关闭并重建连接池/stores/Server、同一 run 恢复；两个效果各一次，重复批准不再 claim，未知 child 不执行。最终模型输入使用默认 assembler 并验证父调用配对与嵌套结果省略。

## 最终门禁记录

- 全仓 `go test -count=1 -timeout=600s ./...` 通过；最终修改的注册 helper 又完成定向普通/race 验证，测试从空注册表提供能力，不预先挂载隐藏的依赖层。
- 严格 PostgreSQL 门禁 `go run ./scripts/test-postgres -log ...` 通过：109 tests、105 subtests、10 packages，零跳过。
- `go build ./...`、`go vet ./...` 通过；staticcheck 首次发现一处简化表达式和一个未使用的测试 helper，修复后全仓通过。
- 覆盖率门禁全部通过：Core 79.1%、evaluation 73.8%、execution 69.0%、server 67.9%、storage 72.0%、provider/openai 73.6%、workflow 57.5%。新 VM 覆盖率 65.2%、toolcapability 78.0%；覆盖率不是恢复语义证明。
- `govulncheck@v1.7.0` 报告可达符号漏洞为 0；依赖模块层面另有 3 个不可达条目，不能把本结果写成整个依赖树绝对无漏洞。
- Gitleaks v8.28.0 对本次 45 个公开变更文件快照扫描，零泄漏；未扫描仓库外凭据目录或将它复制进仓库。
- Core 固定预算核验：34 个生产文件、8,820 非空行、910 项公开 API，未扩上限。
- 全仓 race 首次在既有 `TestAsyncWorkerUsesCanaryRuntimeSelection` 出现 5.04 秒 claim/session lease 过期；独立 race 连续三次通过（0.66/0.65/0.65 秒），定位到该功能测试使用与其断言无关的 300ms 租约窗口；仅该测试改用 5 秒真实租约，生产逻辑与过期测试不变，server 整包 race 复跑通过（362.718 秒）。存储整包 race 另在累计 600 秒触发包级超时（当时当前测试运行 11 秒、当前子测试 0 秒），没有数据竞争报告；按实际枚举的 447 个顶层测试入口，轮转分成 112/112/112/111 四组并行复验，选择集合覆盖全部入口；测试原有平台/PG 条件仍适用，PG 另有零跳过门禁。四组 race 全部通过，耗时分别为 237.208、220.632、227.514、228.641 秒。全仓 race 首轮非零退出记录保留；失败包完成针对性修复/分组全量复验，未重复运行已经通过且未变更的其他包。

专用 PostgreSQL 55441 已正常停止，数据与审计日志保留在仓库外；本轮没有提交或推送。
