# 评估整改与集成验收

日期：2026-09-06。范围：`harness-core` 本地源码、Windows Basic、真实 PostgreSQL、
HTTP 审批恢复、Graph 示例，以及用户指定的 `gemini-3.8-flash` 模型。
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

全仓普通测试中的跳过不计为成功验收。它们包括平台权限/原生 helper 条件、S3 smoke
和默认关闭的计费模型用例；PostgreSQL 专项无跳过。真实模型和 Windows Medium 入口
单独显式执行。远程 GitHub CI、PostgreSQL 16、Linux 原生、Docker、S3 和长期生产负载
不在本次实测证据内。

Race 原始记录为 `race.log`，固定工具链的 Staticcheck 记录为 `staticcheck-pinned.log`，
Windows 原生记录为 `windows-basic.log`。首次未固定工具链的 Staticcheck 自动选择了
Go 1.26.8 并通过；最终另外固定 Go 1.25.13，与 CI 约定一致后通过，不混用两者的环境证据。
提交前检查了 34 个变更文件，所用真实 LLM key 精确匹配为零；凭据文件、测试日志和数据库
目录都被 Git 排除。该检查不等同于完整安全审计。

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
