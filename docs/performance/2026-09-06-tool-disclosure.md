# 工具按需披露：真实 token、恢复与延迟对照

日期：2026-09-06；修复前基线 `e498950`，修复后为本记录一同提交的实现。此记录验证 M24 内置披露协议；不代表整体 Agent 目标完成，也不是跨框架排名。

## 两个已复现的问题

1. **描述成功，下一次请求仍未声明工具。** 旧 `describe` 返回完整 Schema，但 `Schemas()` 始终只有 hot tools 和目录入口。原单测直接调用隐藏 ID，无法证明实际模型协议能接续。新增严格脚本模型先复现失败：`described tool missing from actual model request`；修复后，再用真实 Gemini 走 search → describe → 业务工具 → 最终回复，声明序列为 **1、1、2、2**。
2. **宿主目录被截走。** 宿主注册 `harness.tool.library` 时，旧包装仍注入同名入口并把请求交给内置目录。现已保留宿主快照/分发，不生成重复声明或拦截宿主实现；有先失败后通过的回归，以及实际调用返回宿主结果的检查。

改动位于 [disclose.go](../../pkg/core/disclose.go) 和 [agent_model_call.go](../../pkg/core/agent_model_call.go)。**没有新增公共 core 接口**，默认 `DiscloseTools=false` 保持不变。

## 最终行为与扩展边界

每次模型调用根据当前投影/机械压缩后的消息恢复选择：成功 describe 的工具，或已有配对结果的业务工具，最多保留最近 8 个；hot tools 和目录入口继续提供。随后现有 ContextAssembler 为这份最终工具集合计算预算。选择无全局可变状态，新建 Agent、服务实例和连接池后可从保留的历史重新计算。

声明始终读取**当前授权快照**，不采用历史结果里的 `input_schema`。失败/歧义描述、结果与请求 ID 不一致、孤立结果，以及已不在当前可见集合中的工具不能通过 describe 加入声明；其他业务结果也不能冒充目录响应来暴露另一个工具。已有业务调用结果只恢复它自身当前可见的 Schema，执行仍要经过原有权限、Hook、审批和预算检查。历史里的工具结果若已被截断、发现过程已被摘要或删除出当前投影，可以重新发现；这不是无限期的工具选择记忆。

回归实际执行了权限撤销后的解析、声明检查与调用拒绝，以及新 Schema 注册后的恢复；也覆盖了 legacy ToolCall、Schema 防御复制、最近 8 个选择、重复使用不重复声明、旧快照移除能力与不同历史间无选择泄漏。

需要区分两个扩展位置：

- `core.Runtime.DiscloseTools` / `AgentOptions.DiscloseTools` 是内置关键词目录与固定 8 个近期工具选择的开关，未新增每 Profile 持久设置或可插入搜索器字段。它只包装 `CapabilitySnapshot`。
- `pkg/extensions/toollib.Catalog.SetSearcher` 可替换 **该 Catalog** 的搜索实现，MCP 目录使用此扩展；它不会自动替换 core 内置目录。若宿主要使用另一套完整发现/选择策略，应在外层实现 `ToolRuntime` 并交给 `AgentOptions.Tools`，以当前已过滤快照为授权来源。保留宿主同名目录仅保证分发不被劫持，不会自动替宿主提供按需声明策略。

没有将这些不同能力笼统写成“已有统一可插拔工具检索”。设计见[实施说明](../implementation/2026-09-06-tool-disclosure.md)。

## 真实受控任务

2026-09-06 **14:39:01–14:40:05 Asia/Shanghai**，用户端点、`gemini-3.8-flash`、最大输出设置 2048。实际 **20 次模型请求、最大在途 1、自动重试 0**，总计上游报告 **19,399 输入 / 3,221 输出 token**。两个大小各一个固定顺序 A/B 样本，没有为改善指标重复请求。

4 与 24 个工具使用同一套只读业务目录：库存、账单、客服、物流等不同查询，每个有真实职责说明和同形 record_id 输入，没有循环重复文字填充 Schema。每个规模的两个 arm 使用相同目录、Profile 指令、任务、两个记录 ID 和随机结果 marker；只切换 `DiscloseTools`。具体夹具见 [server_live_disclosure_test.go](../../pkg/server/server_live_disclosure_test.go)。业务工具是合成数据夹具，模型请求、HTTP、PostgreSQL 和 OTel 都是真实路径，不是连接生产库存系统。

每个 arm 两轮：第一轮查 `ORION-42`，重建 HTTP 服务实例与数据库连接池，核对完整历史一致，再查 `LYRA-17`。模型必须实际执行正确业务工具，不能只自行猜答案；每轮恰好一次成功业务调用，两次独立 marker 都精确匹配。披露模式第一轮执行 search、describe、业务调用共 3 次工具，第二轮直接执行业务调用，无重复发现。

## 总成本与实际代价

以下都是**两轮整段会话累计**，包括发现步骤和恢复后的请求，而非只比较首个请求大小。总 token = 输入 + 输出，是数量统计，不是不同输入/输出单价加权后的账单。

| 目录 | 模式 | 模型请求 | 工具调用（含发现） | 输入 token | 输出 token | 总 token | 实测耗时 |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 4 工具 | 全量声明 | 4 | 2 | 2,373 | 599 | 2,972 | 13,428 ms |
| 4 工具 | 按需披露 | 6 | 4 | 3,562 | 900 | 4,462 | 15,960 ms |
| 24 工具 | 全量声明 | 4 | 2 | 9,829 | 636 | 10,465 | 12,544 ms |
| 24 工具 | 按需披露 | 6 | 4 | 3,635 | 1,086 | 4,721 | 19,733 ms |

- **24 工具：输入减少 63.02%，总量减少 54.89%，耗时增加 57.31%。** 发现降低重复携带目录的成本，但增加模型往返与输出。
- **4 工具：输入增加 50.11%，总量增加 50.13%，耗时增加 18.86%。** 首次发现及其留存历史的额外成本超过缩小 Schema 的收益。
- 两组业务答案均为 4/4 成功（每 arm 两轮），业务效果均为每 arm 2 次；披露组多出的 2 次工具为只读 search / describe。

耗时从 arm 第一轮前计到第二轮完成，包含 HTTP、SQL、实例重建，以及验收器强制的模型请求间隔至少 1 秒。上游延迟/输出具有随机性；固定顺序单样本、合成同形 Schema 和未测模型缓存折扣限制了外推范围。**不能据此宣布 24 个是通用开启阈值、所有大目录更快，或小目录永远不能受益。** 当前保持默认全量声明，集成方对大目录低命中比例场景显式试用，并按整个任务的 token、正确率与延迟选择；没有按首请求变小就自动打开。

## 逐请求证据

格式为 `输入/输出 token`，从左到右是顺序请求；前述规模内的两轮任务相同。全量模式前两个属于第一轮，后两个属于第二轮；披露模式前四个属于第一轮，后两个属于重建后的第二轮。

| 目录/模式 | 请求序列 | 每次声明的工具数 |
| --- | --- | --- |
| 4 / 全量 | 499/170、568/163、620/138、686/128 | 4、4、4、4 |
| 4 / 披露 | 234/215、353/108、626/149、695/124、841/162、813/142 | 1、1、2、2、2、2 |
| 24 / 全量 | 2361/224、2431/173、2484/209、2553/30 | 24、24、24、24 |
| 24 / 披露 | 234/184、358/46、631/257、701/165、754/214、957/220 | 1、1、2、2、2、2 |

两份结构化比较文件包含每次请求的实际工具名、消息数、token 及全部 Run ID：[4 工具对照](../verification/evidence/2026-09-06-tool-disclosure/disclosure_4_comparison.json)、[24 工具对照](../verification/evidence/2026-09-06-tool-disclosure/disclosure_24_comparison.json)。对照输出没有硬编码“披露必须更省”的断言，因此小目录反例完整保留。

## Trace、历史与恢复审核

每轮结束都比对 HTTP 全部分页历史与 SQL 事件，核对模型步骤、工具调用/结果配对、Run/Model/Tool span 父子关联及唯一终态。两轮之间重建 HTTP 服务与 SQL pool，历史逐条相同，且恢复后声明继续包含目标工具；未重复执行发现工具。这里是同一个 Go 进程中的实例重建，不是 kill-and-recover 验收。

| 目录/模式/轮次 | 审计 Run 文件 | 当时会话累计事件 | 当前 Run 的 span |
| --- | --- | ---: | ---: |
| 4 / 全量 / 1 | [run_c1b8b6…](../verification/evidence/2026-09-06-tool-disclosure/run_c1b8b6e53323abf729ade5cdefe79ad3.json) | 14 | 5 |
| 4 / 全量 / 2 | [run_f1b539…](../verification/evidence/2026-09-06-tool-disclosure/run_f1b539a4cbbc08bd8c18c4d79f978032.json) | 28 | 5 |
| 4 / 披露 / 1 | [run_079289…](../verification/evidence/2026-09-06-tool-disclosure/run_0792895159440500e1abc77fb644e75e.json) | 24 | 9 |
| 4 / 披露 / 2 | [run_734eaf…](../verification/evidence/2026-09-06-tool-disclosure/run_734eafe66b2f20b73ce75dd296fffb53.json) | 38 | 5 |
| 24 / 全量 / 1 | [run_d7784f…](../verification/evidence/2026-09-06-tool-disclosure/run_d7784fdda3d103aad2c13b14ec003ce3.json) | 14 | 5 |
| 24 / 全量 / 2 | [run_14b69b…](../verification/evidence/2026-09-06-tool-disclosure/run_14b69b2bfc316c5306705d164160c1ae.json) | 29 | 5 |
| 24 / 披露 / 1 | [run_5d3f0e…](../verification/evidence/2026-09-06-tool-disclosure/run_5d3f0ec8301727568850307aa3ea54d8.json) | 24 | 9 |
| 24 / 披露 / 2 | [run_4fee61…](../verification/evidence/2026-09-06-tool-disclosure/run_4fee6171396dabd3a66f8e4db815158d.json) | 38 | 5 |

8 个 Run、4 个 Session，最终共有 133 条不重复持久事件。第二轮文件包含第一轮历史，不能把两份文件的事件数直接相加。每个 Run 导出还包含独立 queue claim span；5 个 span 是 claim + run + 2 model + 1 tool，9 个是 claim + run + 4 model + 3 tool。读取了真实 SDK ManualReader 上下文指标，但这些是当前服务实例内的保守估算累计值，不等于上游 tokenizer；中途实例重建会重置此类累计值。公开审计已脱敏 continuation，并检查无 key/endpoint 明文。

## 本地选择成本

Windows/amd64、i7-12700K、Go 1.25.13、GOMAXPROCS=20，5 样本、300 ms benchtime；无数据库/LLM 并行工作。500 个非 hot 工具加 1 个 hot 工具，120 条消息包括 40 个完整业务调用/结果组，索引构造不计时。新选择流程最后输出 8 个近期工具、1 个 hot 工具和 1 个库入口。

| 路径 | ns/op 原始 5 样本 | B/op | allocs/op |
| --- | --- | ---: | ---: |
| 从历史恢复选择并克隆必要 Schema | 10387、9873、10335、9863、9990 | 13,120 | 94 |
| 仅返回 hot 与入口，不恢复选择 | 1418、1409、1478、1418、1403 | 3,616 | 27 |
| 旧克隆全目录再过滤 hot 的参考夹具 | 369702、385903、378069、392662、372222 | 589,635（第 4 样本 589,636） | 4,014 |

新增恢复功能的中位耗时 **9.990 µs**。这些路径工作量不同：不能称新功能比原本仅返回入口更快；它增加了正确声明/恢复的开销，同时仍只复制有界的近期 Schema。没有测整个服务 RSS、长期保留量、万级目录或跨语言检索准确率。

## 复跑与质量检查

普通 PostgreSQL 回归使用严格脚本模型，不计费；真实测试必须显式启用 `HARNESS_ACCEPTANCE_LIVE_SERIAL=1`，设置模型/key/endpoint 与 PostgreSQL DSN，最多 1 请求在途、不自动重试。密钥只通过环境注入，不能放进命令或证据。

```text
go test -p 1 -parallel 1 -count=1 ./pkg/core -run "Disclos"
go test -p 1 -parallel 1 -count=1 -timeout=120s ./pkg/server -run "^TestPostgresSerialToolDisclosure$"
go test -p 1 -run "^$" -bench "^BenchmarkDisclosedSchemas(FromHistory|HotOnlyVsFullSnapshot)$" -benchmem -benchtime=300ms -count=5 ./pkg/core
# 仅显式启用真实模型后运行，两个子用例合计 20 个实际请求：
go test -p 1 -parallel 1 -json -count=1 -timeout=600s ./pkg/server -run "^TestLiveModelSerialModuleAcceptance$/(tool_disclosure_small|tool_disclosure_large)$"
```

最终通过的本机检查：

- 全仓 `go test -p 1 -parallel 1 -count=1 -timeout=600s ./...`，启用 PostgreSQL、关闭 LLM：server 42.842 s、storage 37.208 s；新增权限撤销/版本更新与宿主分发测试另作定向复核通过。
- 全仓 `go build -p 1 ./...`、`go vet -p 1 ./...`、`staticcheck@v0.7.0 ./...`，以及 gofmt。
- `go test -race -p 1 -parallel 1 -count=1 -timeout=180s ./pkg/core ./pkg/server -run "Disclos|CompositionRevision|ProfilePermissionFiltering"`：core 1.043 s、server 20.538 s；包括新 PostgreSQL 两轮披露恢复与已有审批组合标识回归。
- OpenAPI：102 个操作；core：34 个生产文件、8706 非空物理行、904 公共表面计数。生产私有代码相比上一提交增加 75 行，公共表面不变，门禁阈值未放宽。
- 改动 Markdown 本地链接、`git diff --check`，审计导出和暂存区无用户 key/endpoint 明文。

本轮没有重跑全仓 race、容器、漏洞扫描或 Windows Basic 原生验收，不能称为全部 CI job 已执行。

本机忽略目录 `.tmp-tool-disclosure-20260906` 保留旧版失败、回归日志、完整 benchmark、离线/真实 SQL 与 trace 文件；公开证据为上方 10 个 JSON。01/02 为协议和宿主目录修复前后；03 的既有断言只保存最后一次请求，随后增强为分别检查首请求的隐藏与实际调用后的声明，04 通过；05 基准；06 离线对照；07 真实 JSONL；08–18 为质量检查。Go 缓存和临时目录在 D 盘。

整体目标仍需长历史/长任务、真正进程重启与副作用恢复、厂商协议成本模型、RSS 和跨框架同等任务集；本轮没有以局部 token 收益替代这些未完成项。
