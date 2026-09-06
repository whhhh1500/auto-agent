# Harness Core 项目评估

日期：2026-09-06。范围：当前本地源码快照；按开源 Agent 基础框架定位评估。

## 结论

项目已具备较完整的 Agent 运行与管理基础设施，适合继续作为开源 pre-GA 框架和受控业务试点使用。核心运行语义、持久化防重、审批恢复、权限约束和扩展边界是主要优势。现有证据还不足以承诺稳定公共 API、完整生产环境互操作性或大规模业务负载表现。

当前最需要优先处理的是 PostgreSQL 持续验证覆盖、对外能力边界和文档一致性。后续功能应继续通过适配器和扩展加入，控制核心公共接口增长。

## 本次直接验证

环境为 Windows amd64、Go 1.25.13。所有本次 Go 命令的 GOCACHE、GOTMPDIR、TEMP 和 TMP 均设置到 D 盘。本次未启动业务服务或调用真实模型，也未使用生产数据库和外部账号凭据。

| 检查 | 结果 |
| --- | --- |
| `go test -p 4 -json -count=1 -timeout 600s ./...` | 退出码 0；78 个有测试的包通过；1,553 个顶层测试通过，含子测试共 2,288 条通过记录；74 条跳过，0 条失败 |
| `go build -p 4 ./...` | 通过 |
| `go vet -p 4 ./...` | 通过 |
| `go run ./scripts/verify-openapi openapi/harness-core-v1.yaml pkg/server pkg/console/static/index.html` | 通过，核对 102 个已注册 `/v1` 操作 |
| `./scripts/verify-gofmt.ps1` | 通过 |
| 架构约束测试 | 核心依赖边界和 API/规模预算检查通过 |

74 条跳过记录中，65 条是名称以 TestPostgres 开头的测试，其中 12 条位于 SQL 适配器包。其余包含 POSIX 权限检查、Windows 原生启动用例及辅助目标、S3 smoke。Windows 正向启动用例在当前 elevated 进程中按既定条件跳过；本次未将历史 Medium 验收记录计入新通过项。

本次没有执行 Linux/race、Staticcheck、govulncheck、Docker 容器、真实 PostgreSQL/S3、真实模型调用或长期压力验证。CI 文件包含其中若干检查，但配置存在不代表本快照已经执行通过。

测试 JSONL 和标准错误保存在本机 D 盘的 `.codex-tmp` 目录，文件名前缀为 `harness-assessment-20260906-tests`；运行日志不纳入 Git。

## 架构与完成度

| 方面 | 判断与依据 |
| --- | --- |
| 核心与适配器边界 | `pkg/core` 的标准库依赖约束有自动检查；模型、SQL、执行器和传输置于外层，符合基础框架定位 |
| 运行可靠性 | Session 事件、不可变组合快照、审批恢复、工具调用日志、Queue/Session 租约及过期执行者隔离均有实现和测试 |
| 权限与副作用 | 主运行入口检查 Session 所有权；工具统一经过参数、Hook、预算、审批和持久调用日志；嵌套工具也有受保护入口 |
| 扩展能力 | Capability/Profile/Plugin、HTTP/MCP/WASM、Runner、Memory/RAG、Evaluation、Graph 与模型协议适配器已形成较广覆盖 |
| HTTP 集成 | 有 OpenAPI 与 Console 路由一致性检查，当前覆盖 102 个 `/v1` 操作；README 明确 API 处于 pre-GA |
| 生产交付 | 有非 root 容器、production PostgreSQL 要求、就绪检查和 CI 配置；本次未复跑真实生产部署链路 |

源码规模按 `pkg/cmd/internal/examples/scripts` 中的物理行统计，包含空行、注释和开发验收程序：587 个 Go 文件，其中 260 个 `_test.go`；非测试文件 83,884 行，测试文件 70,504 行。规模反映维护成本，不代表覆盖率。

核心预算测试实际报告：34 个生产 Go 文件、8,615 个非空物理行、902 个公共表面计数项。902 的口径包含顶层导出名、导出方法、字段与接口方法，并不是 902 个接口。已有预算保护值得保留，但首个稳定版本前仍应按真实集成场景收敛对外入口。

## 发现与优先级

### P1：PostgreSQL CI 漏跑 SQL 适配器测试

[ci.yml](../../.github/workflows/ci.yml) 的 PostgreSQL 作业只执行 `./pkg/storage -run '^TestPostgres'`。`pkg/adapter/sql` 下 effectjournal、compositionstore、fencejournal、artifactmigration、graphcheckpoint、graphsegment 和 notificationtarget 的 PostgreSQL 用例未包含在该命令中。其他测试作业未设置 HARNESS_TEST_PG_DSN，因此这些用例会跳过。

本次测试日志实际观察到这 7 个适配器包的 12 条 PostgreSQL 跳过记录。SQLSchemaVersion 当前为 41，适配器包含 CAS、租约、历史检查点和迁移语义，SQLite 通过无法替代 PostgreSQL 实测。

建议扩大 PostgreSQL 作业的包范围，覆盖所有依赖该测试 DSN 的包，同时沿用“不得跳过”的门禁。验收应展示适配器用例在一次真实 PostgreSQL CI 中执行通过。这是持续验证缺口，不是本次发现了数据库运行错误。

### P2：架构文档对 Windows 沙箱的描述已过时

[architecture.md](../architecture.md) 的 Outer packages 和 Integration boundaries 段仍写 Windows local sandbox 不可用；[README](../../README.md) 与 [2026-09-06 验收记录](2026-09-06-windows-basic-acceptance.md) 则描述已经实现的 current-user Basic provider。

代码报告 NetworkHost 和 NetworkIsolation=false，并有单会话准入实现。建议统一当前能力说明：普通 Medium 调用者、restricted child、Job 生命周期清理、每进程一个活动 Session、没有网络隔离。保留 Windows 正向验收作为独立检查，普通 elevated 单测通过不能替代它。

### P2：Graph 应继续以有限的实验能力对外说明

[Graph 服务适配器](../../pkg/adapter/runexecutor/graph/graph.go) 的 NewDefaultDefinition 只有一个 `core-turn` 节点，并明确当前 registration 只接受这一内置定义。底层 Graph 合同与检查点机制已经存在，但默认服务器接入不等于任意多节点业务流程已经交付。

README 当前对此有准确限定，应继续保持。若真实产品需要多节点 Graph，应先在独立 adapter registration 中交付一个可恢复的实际业务样例，再扩大对外支持范围。这是能力范围，不是实现缺陷。

### P2：性能证据不能外推为生产业务容量

[性能说明](../performance/perf-p0.md) 使用本地确定性模型、工具和 `/healthz` workload。已有历史记录披露 500 并发冷启动连接突发的波动，并区分输入驻留、运行区间和 GC；这种证据口径是合理的。

这些数字不包含真实模型网络时延、生产 PostgreSQL、多实例队列及完整租户业务混合负载。本次未复跑压力测试，不判定当前性能回归。建议围绕一个目标业务定义负载、持续窗口和验收指标后，再补齐端到端基线。

### P2：稳定接口前需要控制维护面

核心已设置公共表面和体积预算，但整个服务拥有较多管理入口、可选基础设施配置和多套执行路径。框架应优先明确推荐集成入口、稳定与实验包清单、最小运行样例，以及公开 API 的兼容策略。

不建议为“更通用”而继续向核心增加业务字段。优先使用已有 adapter/extension 边界，让真实调用方验证扩展方式是否足够简单。

## 建议顺序

1. 补齐 PostgreSQL 适配器持续验证，建立一份不含跳过项的生产数据库证据。
2. 同步 Windows 与 Graph 的当前支持边界，整理稳定/实验 API 清单。
3. 选一个真实业务，验证模型调用、工具审批、进程重启恢复和最终结果的完整链路。
4. 基于该业务做 PostgreSQL、多实例与持续负载验收，再确定生产容量和版本承诺。

## 本地版本控制

评估开始时目录没有 Git 元数据。根据用户后续要求，在项目根目录初始化本地 `main` 分支并创建初始快照。增加缓存、凭据和临时产物的忽略规则，并固定文本换行策略；保留原有源码和 Markdown 有意使用的行末换行空格。

初始快照仅纳入源码、测试、文档、契约、部署与维护文件，以及本报告。真实 `.env`、本地凭据、数据库、日志、缓存和编译产物均排除。常见密钥格式扫描的唯一命中来自明确标注为开发用途的 Compose 示例密码；这项扫描不等于完整安全审计。
