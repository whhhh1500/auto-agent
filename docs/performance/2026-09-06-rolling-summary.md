# 连续摘要：历史正确性、真实 token 与恢复证据

日期：2026-09-06。修复前基线 `9e90990`，修复后实现随本记录一同提交。对应 M15，设计见[实施说明](../implementation/2026-09-06-rolling-summary.md)。这里验证默认本地提取式摘要，不代表可选 LLM 摘要或整个 Agent 目标完成。

## 已复现并修复

1. **连续摘要替换范围错误。** 投影把摘要放在旧历史的位置，但 `SourceSeq` 是稍后追加的摘要事件序号。旧实现取投影首尾序号作为范围，导致旧尾部残留、来源范围丢失，默认 120/60 配置最终报 `rolling summarizer result exceeds MaxMessages`，且错误前已经追加了摘要事件。新增 6/2、8/7、120/60 连续摘要回归先失败，再通过。
2. **旧摘要超过 1 KiB 时整份事实消失。** 一份约 1.5 KiB、仍远低于 12 KiB 总上限的旧摘要，在下一轮被普通消息限制转换为一个省略/hash 标记。新实现使用剩余总摘要预算保存完整旧摘要，三轮回归保留指定事实。超出总预算仍明确省略，不截半段文本冒充完整记录。
3. **不同轮次复用 tool call ID 时配对失效。** 旧提取器保留已完成调用的 ID 索引，下一次同 ID 调用被忽略，结果标为 unpaired。现在配对完成后删除待配对索引，回归核对两次不同工具名与各自结果。该问题由代码检查定位并加回归，未单独进行真实工具复用调用。

生产改动仅在 [summarizer.go](../../pkg/app/contextassembly/summarizer.go) 与 [extractive_summarizer.go](../../pkg/app/contextassembly/extractive_summarizer.go)。归档范围覆盖原始 provenance 和旧摘要事件，预先检查不会遮住保留尾部，以真实 user turn 为切点；`KeepTail` 是上限，必要时少保留旧尾部。无安全切点在摘要调用/写日志前失败；摘要器返回时 context 已取消也不再追加事件。既有原始日志不删除，core 事件格式、公共接口与门禁阈值均未增加。

## 真实任务与对照边界

2026-09-06 **15:07:48–15:08:11 Asia/Shanghai**，用户端点、`gemini-3.8-flash`，最大输出设置 2048。总计 **6 次模型请求，最大在途 1，自动重试 0**；上游报告累计 **11,247 输入 / 1,779 输出 token**。没有为改善数字重复计费请求。

夹具见 [server_live_summary_test.go](../../pkg/server/server_live_summary_test.go)：

- 两个 arm 使用相同的三份随机仓库验证代码、相同 Profile 指令、同一合成历史和同一组三个问题。每轮恰好一次模型请求、无工具调用，依次问 ALPHA、BETA、GAMMA；回答必须精确等于对应代码。
- 首轮前直接向 SQL 会话写入 64 轮合成对话，后两轮各新增 32 轮；共 128 轮种子，**不是 128 轮真实模型交谈**。三个事实位于第一次归档前缀最后四条 user 消息中，符合 extractive 的明确选择规则。此测试证明被选中事实的连续保留，不证明任意远古事实都能召回。
- 真实回答不重复其他仓库代码，下一次所问代码从未出现在先前回答中。启用摘要时，断言待问代码在实际组装请求中只出现一次，且只来自 summary 消息，不能从未归档尾部抄出答案。
- A 为保留完整历史；B 启用默认阈值 `MaxMessages=120 / KeepTail=60` 的本地 extractive 摘要。**两组均关闭机械 RecentTurnsCompactor 的窗口裁剪**，让对照只改变摘要；最终 ContextAssembler 在两组均启用。不是声称默认服务关闭摘要后仍会发送全部历史。
- 各 arm 三轮之间替换 HTTP 服务实例和 SQL pool 两次，比对重建前后完整事件与派生投影。没有杀死 Go 进程，未验证崩溃期间的恢复。

## 整段 token 和代价

| 三轮累计 | 模型请求 | 输入 token | 输出 token | 总 token | 实测耗时 | 正确答案 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 完整历史 | 3 | 7,520 | 889 | 8,409 | 10,769 ms | 3/3 |
| 本地连续摘要 | 3 | 3,727 | 890 | 4,617 | 12,017 ms | 3/3 |

本组输入减少 **50.44%**，输入+输出总量减少 **45.09%**，耗时增加 **11.59%**。没有额外摘要模型调用。耗时包含 SQL/HTTP、历史分页检查、服务重建和验收器强制请求间隔至少 1 秒，且受上游波动影响。固定顺序 A/B 各一个样本，不是统计均值、生产成本承诺或跨框架排名；token 数量也不是按输入/输出单价加权的账单。

| 请求顺序 | 完整历史 输入/输出 | 摘要 输入/输出 | 完整/摘要实际消息数 | 摘要范围 | 摘要文本 UTF-8 字节 |
| --- | --- | --- | --- | --- | ---: |
| ALPHA | 1701/249 | 1112/348 | 129 / 60 | 1–70 | 1,940 |
| BETA | 2409/323 | 1240/169 | 195 / 60 | 1–146 | 2,480 |
| GAMMA | 3410/317 | 1375/373 | 261 / 60 | 1–222 | 3,020 |

第 2/3 轮旧摘要均大于 1 KiB，仍携带未问过的事实，来源范围从原始起点持续扩展。所有请求、实际组装消息、摘要内容和范围在[结构化对照证据](../verification/evidence/2026-09-06-rolling-summary/rolling_summary_comparison.json)中，便于独立审核。

## Trace、聊天历史与结果共同验收

每个 Run 对齐一个真实 model call span、一个 agent step、唯一终态和精确回答；此外导出独立 queue claim span 与 run span，共 3 个 span。归档行为通过持久 `context/summary` 事件、其范围和实际模型消息审计，**没有把本地摘要冒充成额外 model span**。

| 模式/轮次 | 审计 Run 文件 | 当时会话累计事件 | 当前 Run span |
| --- | --- | ---: | ---: |
| 完整 / 1 | [run_54ad04…](../verification/evidence/2026-09-06-rolling-summary/run_54ad04e504a91fa9fa6a31d3d8a0f797.json) | 139 | 3 |
| 完整 / 2 | [run_5cc868…](../verification/evidence/2026-09-06-rolling-summary/run_5cc868c10851306dff15747bc14c3272.json) | 214 | 3 |
| 完整 / 3 | [run_5d8e8d…](../verification/evidence/2026-09-06-rolling-summary/run_5d8e8d49869beb8fcf860c39c6f65560.json) | 290 | 3 |
| 摘要 / 1 | [run_622c97…](../verification/evidence/2026-09-06-rolling-summary/run_622c97df532933a98ea9063fe65c3cc2.json) | 140 | 3 |
| 摘要 / 2 | [run_78ff64…](../verification/evidence/2026-09-06-rolling-summary/run_78ff64cd5f59e5444c1fcfae995230fe.json) | 216 | 3 |
| 摘要 / 3 | [run_bed6d0…](../verification/evidence/2026-09-06-rolling-summary/run_bed6d0c021c092bfe0e1b123cf821a5b.json) | 292 | 3 |

6 个 Run、2 个 Session 最终共有 **582 条不重复事件**，包括每个 Session 的合成种子；不能将所有中间文件的累计事件相加。HTTP 按每页 3 条读取并与 SQL 每条事件、序号、version 比较。四次服务/连接池替换后，事件和投影均相同；摘要组共三个摘要事件，原始消息仍存在于 HTTP/SQL 历史。

证据还包括真实 OTel SDK ManualReader 的上下文成本指标。这是服务实例内的保守估算累计值，实例替换会重置，不等于上游精确 tokenizer 或进程 RSS。七个公开 JSON 已检查没有用户 key/endpoint 明文，协议 continuation 已脱敏。

## 本地开销

Windows/amd64、i7-12700K、Go 1.25.13、GOMAXPROCS=20，5 样本、每样本 300 ms。基准 fixture 先完成一次摘要，再追加对话，形成 121 条投影消息、189 条原始事件。它独立于上面的真实对照，不含 SQL、HTTP 或模型调用。

| 路径 | ns/op 的 5 样本 | 中位耗时 | B/op | allocs/op |
| --- | --- | ---: | ---: | ---: |
| 计算封闭归档范围 | 588.8、594.3、589.5、594.3、578.2 | 0.5895 µs | 1,024 | 1 |
| 生成所选前缀的本地摘要 | 1105、1058、1050、1042、1063 | 1.058 µs | 1,505 | 9 |
| 从事件恢复 + 投影 + 追加摘要 + 重新投影 | 194024、191558、193634、190797、195922 | 193.634 µs | 225,117–225,209 | 2,495 |

选择算法为两次线性扫描，辅助空间与当前消息数成正比；无需增加全局状态。完整恢复路径远比单独生成摘要成本高，不能把 1 µs 的提取时间当作整个会话恢复时间。B/op 是每次累计分配字节，未测存活堆/RSS，也没有将新正确行为与旧错误行为比较后宣称整体更快。

## 扩展方式及尚未解决的边界

继续通过 app 层 `ContextSummarizer` / `ContextSummarizerFunc` 提供不同语义或结构化提取策略，使用 `Runtime.Summarizer` 安装 `RollingSummarizer`；无须修改 core 事件内核。该 Go 扩展需要集成方编译接入，不是模型自动安装或任意用户脚本入口。

默认 extractive 只选最近 4 条 user 目标、已有摘要和工具结果，忽略普通 assistant 叙述；普通消息/工具结果分别受 1,024/2,048 字节限制，超限整项省略并留 fingerprint。摘要自身最多 12 KiB；旧摘要嵌套仍会增长，本次从 1,940 增至 3,020 字节。达到剩余总预算后仍可能整份省略；较大的旧摘要也可能挤掉后续记录。因此不能承诺无限轮或任意事实不丢失，长期结构化记忆/语义压缩仍是独立工作。

**可选 `LlmSummarizer` 的明确缺口：** 当前 transcript 只记录角色和 Content，没有完整带入工具名、参数及调用 ID；其 summary-only stream 没有把 Usage 返回给 agent run 汇总，也没有单独集成摘要调用的 telemetry。这些来自代码检查，尚未修复或真实验收；本轮所有 token 数字只适用于没有额外摘要模型请求的默认 extractive 路径。将来启用 LLM 摘要时，必须把该调用纳入总费用/trace，再评估收益。

## 复跑和检查

所有 Go 缓存、编译临时目录与 TEMP 均在 D 盘。离线 PostgreSQL 模型为脚本夹具，不调用端点。真实测试需要显式设置 `HARNESS_ACCEPTANCE_LIVE_SERIAL=1`、模型/URL/key 和 PostgreSQL DSN；凭据只经环境传入。

```text
go test ./pkg/app/contextassembly -run "RollingSummarizer|ExtractiveSummarizer" -count=1
go test ./pkg/server -run "^TestPostgresSerialRollingSummary$" -count=1 -timeout=120s
go test ./pkg/app/contextassembly -run "^$" -bench "^BenchmarkRollingSummary$" -benchmem -benchtime=300ms -count=5
# 仅显式启用真实模型时，六次串行请求：
go test ./pkg/server -run "^TestLiveModelSerialModuleAcceptance$/^rolling_summary$" -count=1 -json -timeout=240s
```

全仓 PostgreSQL-enabled / LLM-disabled `go test ./... -count=1 -timeout=600s` 通过（server 46.450 s、storage 34.980 s）；全仓 build、vet、本机 staticcheck 0.8.1 与 CI 指定 staticcheck 0.7.0 均通过。`go test -race ./pkg/app/contextassembly ./pkg/server -run "RollingSummary|RollingSummarizer|ExtractiveSummarizer" -count=1 -timeout=120s` 通过，分别 1.055 s / 9.028 s，包含离线 SQL 三轮恢复。OpenAPI 验证 102 个操作，281 个本地 Markdown 链接有效，格式和 diff 检查通过。core 未改动，预算门禁仍为 34 个生产文件、8706 非空物理行、904 公共表面计数。本次没有运行全部 race、容器、原生 Windows Basic 验收或整个 CI 矩阵。

忽略目录 `.tmp-rolling-summary-20260906` 保留：01 原版失败，02 修复后单测，03 离线 SQL 对照，04 真实 JSONL，05 五样本基准，06–14 质量检查；离线和真实证据分别保存在各自子目录。首次 OpenAPI 命令使用错误路径而失败，随后按当前 CI 路径重跑；这不计为产品失败，也不触发模型重试。
