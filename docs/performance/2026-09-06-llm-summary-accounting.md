# LLM 摘要：完整用量、失败审核与真实成本

日期：2026-09-06；前一提交 `9ec4353`。本记录对应 M09 / M15 / M43，补齐上一轮明确列出的 LLM 摘要缺口。设计见[实施说明](../implementation/2026-09-06-llm-summary-accounting.md)。本轮不改变默认 extractive 策略，也不代表所有长任务或跨框架目标完成。

## 查出的实际问题与改动

新增回归先复现三项失败：成功摘要没有 usage 事件；摘要 transcript 缺少工具 ID/名字/参数；普通模型已报告用量后返回错误，Run 没有入账。修复后都通过。

- **共享流消费返回用量。** 新增 `core.ConsumeModelStreamUsage`，复用唯一协议校验器；返回第一份通过数值校验的 usage，即使后续流失败。重复或非法报告不覆盖它。`nil` 表示没有收到有效报告，不表示免费；明确报告 0 是非 nil 的零值。旧 `ConsumeModelStream` 保留，现有调用方无需迁移。
- **摘要用量进入既有账本。** app 的可选 `MeteredContextSummarizer` 接收 `SummaryRequest`，返回 `SummaryResult{Text, Usage}`。`RollingSummarizer` 将有效 usage 作为独立的 `run/usage` 增量追加/发送，包括失败和取消路径。SQL RunStat 已对同 Run 的此类事件求和，无须新事件格式、数据库迁移或可变全局计数器。普通 agent 模型失败路径也先记录已报告 usage，再结束 Run。
- **实际身份与 trace。** Rolling 绑定当前 principal、scope、session、Run、step，覆盖错误的静态运行身份；Provider/Model 由摘要配置选择。输入/预算预检之后才进入 Gate。可选注入 `LlmSummarizer.Telemetry`，span 使用 `model.purpose=context_summary`、`model.invoked` 和 `model.usage.reported` 等属性，继承 Run 的 trace。Gate 拒绝有尝试 span，但不增加实际模型调用计数；预检拒绝不会启动模型 span。配置通过现有 Go 组合方式接入，不是默认服务开关。
- **保留工具含义。** 摘要输入为有界 JSONL，记录角色、来源序号、工具调用 ID/名字/参数、结果 Call ID 和 summary provenance，排除厂商 continuation。大 Content 在序列化前省略；参数先限制字节、节点（2048）和深度（32），未知自定义 marshaler 不执行、循环/超限参数明确标 `args_omitted`。JSON 转义按最多六倍膨胀保守预留，可能比精确序列化更早省略。本地 extractive 同样补上有界工具参数，否则同一个 lookup 的多个结果无法可靠区分查询对象。
- **限制拒绝路径的复制。** core 在消费者接受 chunk 后才复制文本到聚合 buffer。摘要器拒绝一个超大 chunk 时，不再先复制整块文本。返回的 usage 也独立于聚合结构，避免保存 usage 指针时保留无用文本/工具 buffer。

主要源码：[llm_summarizer.go](../../pkg/app/contextassembly/llm_summarizer.go)、[summary_accounting.go](../../pkg/app/contextassembly/summary_accounting.go)、[summary_transcript.go](../../pkg/app/contextassembly/summary_transcript.go)、[extractive_summarizer.go](../../pkg/app/contextassembly/extractive_summarizer.go)、[llm.go](../../pkg/core/llm.go)、[agent.go](../../pkg/core/agent.go)。LLM 策略从原 summarizer 文件拆出，滚动归档与模型传输分别维护。

集成方显式安装时，可使用下面的组合；`summaryAdapter`、`gate`、`recorder` 和模型选择由宿主提供，Run 身份由 Rolling 在每次调用时填写，不能固定成某个用户或旧 Run。示例的字节预算是部署选择，与 provider 的输出 token 设置分别控制。

```go
runtime.Summarizer = &contextassembly.RollingSummarizer{
    MaxMessages: 120, KeepTail: 60,
    Summarizer: contextassembly.LlmSummarizer{
        Adapter: summaryAdapter, ModelCallGate: gate, Telemetry: recorder,
        RequestIdentity: core.ModelCallRequest{Provider: providerName, Model: modelName},
        MaxInputMessages: 256, MaxInputBytes: 32 << 10, MaxOutputBytes: 2048,
    },
}
```

## 串行真实任务

2026-09-06 **15:39:19–15:40:04 Asia/Shanghai**，用户端点、`gemini-3.8-flash`，最大输出设置 2048。**9 次真实模型请求，最大在途 1，自动重试 0**，累计上游报告 **12,190 输入 / 5,223 输出 token**；未为成本数字重跑请求。

两组都使用 120/60 阈值，关闭机械窗口裁剪，启用最终 ContextAssembler。A 使用本地 extractive，B 使用 LLM 摘要。每组初始 64 轮历史，后两次各新增 32 轮；这些是直接写入 SQL 的**合成历史**。三个随机 proof 只出现在 `warehouse.lookup` 的结果里，ALPHA/BETA/GAMMA 的对应关系来自工具参数，Call ID 使用无仓库含义的编号。工具调用本身是种子事件，**本轮没有实际执行仓库工具**；真实调用验证的是模型读取工具来源历史、生成摘要和回答的链路。

每组分别问三个仓库的代码，回答必须精确匹配；上一轮回答不得包含下一仓库代码。当前待问代码只来自一条 summary 消息，不从未归档尾部获得。三轮之间各替换两次 HTTP 服务实例和 SQL pool，事件与投影逐条一致。这是同进程实例重建，未模拟进程被杀死。

## 包含摘要调用的总成本

| 三轮累计 | 普通模型调用 | 摘要模型调用 | 输入 token | 输出 token | 总 token | 实测耗时 | 正确答案 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 本地 extractive | 3 | 0 | 3,367 | 1,056 | 4,423 | 10,047 ms | 3/3 |
| LLM 摘要 | 3 | 3 | 8,823 | 4,167 | 12,990 | 34,177 ms | 3/3 |

LLM 组总 token **增加 193.69%**、耗时**增加 240.17%**。若只看该组的三个普通回答请求，会看到输入 2,844，比本地组少 15.53%；普通请求总量 4,036，也少 8.75%。但三个摘要请求另外消耗 **5,979 输入 + 2,975 输出 = 8,954 token**，计入后收益消失。这正是本轮修复漏账和审核总成本的必要性。

| 模式 | 顺序请求的 输入/输出 token | 实际消息数 | 三次摘要 UTF-8 字节 |
| --- | --- | --- | --- |
| 本地 extractive | 992/367、1120/389、1255/300 | 60、60、60 | 872、1412、1952 |
| LLM 摘要 | **1944/857**、936/393、**1997/1085**、950/409、**2038/1033**、958/390 | 1、60、1、60、1、60 | 802、789、719 |

粗体为摘要调用；其一条 user 消息是整个有界 JSONL transcript，并非只有一条原始历史。LLM 摘要虽较短，输出计量仍按上游报告全量记录，不能根据可见摘要字符数自行扣减。结果见[对照 JSON](../verification/evidence/2026-09-06-llm-summary/llm_summary_comparison.json)。它保存每个调用的 token/消息数，以及最终普通模型请求的组装消息和摘要；没有保存摘要请求的完整原始 HTTP 包。摘要输入的 JSONL/工具参数规则另由离线捕获回归验证，不能把重建的输入说成实际 HTTP 抓包。

这是固定顺序、各一次的合成任务样本。耗时包含 SQL/HTTP、分页审核、服务重建及验收器的至少一秒请求间隔；有上游波动，没有评估缓存折扣、不同输入/输出单价或真实语义任务平均成功率。当前继续默认本地提取；不能据此认为 LLM 摘要在所有更长任务里都不合算。

## Trace、历史、用量与 SQL 四方核对

普通请求仍对应 agent step；摘要 span 独立计数并核对 `context/summary` 事件数，不能把“模型 span 数必须等于 step 数”原断言直接套到有摘要的 Run。每个模型 span 都属于同一 Run trace，父 span 指向该 Run。每轮再对齐上游报告、该 Run 的 `run/usage` 事件之和与 SQL RunStat。

| 模式/轮次 | Run 审计 | 用量/SQL 审计 | 累计事件 | Run span 数 |
| --- | --- | --- | ---: | ---: |
| 本地 / 1 | [run_abc2c0…](../verification/evidence/2026-09-06-llm-summary/run_abc2c0083d2c88a8df5bf0f129853f8b.json) | [usage](../verification/evidence/2026-09-06-llm-summary/run_abc2c0083d2c88a8df5bf0f129853f8b_usage.json) | 149 | 3 |
| 本地 / 2 | [run_e910fb…](../verification/evidence/2026-09-06-llm-summary/run_e910fb666eaa2578e6a4188dd7287f88.json) | [usage](../verification/evidence/2026-09-06-llm-summary/run_e910fb666eaa2578e6a4188dd7287f88_usage.json) | 225 | 3 |
| 本地 / 3 | [run_e1c4d8…](../verification/evidence/2026-09-06-llm-summary/run_e1c4d8f8afa9f5097e19439590e0f53a.json) | [usage](../verification/evidence/2026-09-06-llm-summary/run_e1c4d8f8afa9f5097e19439590e0f53a_usage.json) | 301 | 3 |
| LLM / 1 | [run_91e2a5…](../verification/evidence/2026-09-06-llm-summary/run_91e2a5532c137ad6fb37ec8d69b45681.json) | [usage](../verification/evidence/2026-09-06-llm-summary/run_91e2a5532c137ad6fb37ec8d69b45681_usage.json) | 150 | 4 |
| LLM / 2 | [run_530445…](../verification/evidence/2026-09-06-llm-summary/run_530445d21fa823f6149f48739a6c122e.json) | [usage](../verification/evidence/2026-09-06-llm-summary/run_530445d21fa823f6149f48739a6c122e_usage.json) | 228 | 4 |
| LLM / 3 | [run_33c393…](../verification/evidence/2026-09-06-llm-summary/run_33c393c9fca6b1f26921e5cfff16bb08.json) | [usage](../verification/evidence/2026-09-06-llm-summary/run_33c393c9fca6b1f26921e5cfff16bb08_usage.json) | 305 | 4 |

3 个 span 是 queue claim、Run、普通 model call；LLM 组额外一个摘要 model call。6 个 Run、2 个 Session 最终 **606 条不重复事件**，包括合成种子；中间审计包含累计历史，不能相加。原始事件仍可通过 HTTP 读取，与 SQL 每条事件/序号/version 一致。OTel ManualReader 的上下文指标是实例内保守估算累计值，包含摘要请求，实例重建会重置；它不等于上游 tokenizer。

**失败路径另用脚本模型注入，不消耗用户额度。** 模型报告 23 输入/5 输出后返回错误：Run failed，恰好一个 error 摘要 span，未调用普通模型，未写入替换摘要，已报告用量保留在 HTTP/SQL 历史和 RunStat，错误正文脱敏。见[失败审计](../verification/evidence/2026-09-06-llm-summary/run_2b75578d2311256f85b566b21beb8671.json)和[失败用量](../verification/evidence/2026-09-06-llm-summary/run_2b75578d2311256f85b566b21beb8671_usage.json)。其他离线回归覆盖 Gate 拒绝、输入预检、负预算、当前身份覆盖静态身份、取消、重复/负值/超限 usage、无报告与明确 0、超大参数图及 continuation 隔离。

## 被拒绝大块文本的本地开销

Windows/amd64、i7-12700K、Go 1.25.13、GOMAXPROCS=20；5 样本，每样本 300 ms。预建一个 2 MiB 文本 chunk，消费者立即拒绝。参考代码只将 `text.WriteString` 移回回调前，其他路径相同；跑完后恢复最终源码并核验。无模型/SQL 调用参与此基准。

| 拒绝前复制策略 | ns/op 五样本 | 中位 ns/op | B/op | allocs/op |
| --- | --- | ---: | ---: | ---: |
| 原先先复制再回调 | 146499、140701、145737、136509、137054 | 140,701 | 2,097,534–2,097,546 | 11 |
| 先回调，接受后复制 | 327.6、300.2、291.5、283.3、296.2 | 296.2 | 376 | 10 |

这证明**拒绝路径**省掉了约 2 MiB 聚合副本。来源 chunk 本身已存在，不计构造成本；未测服务 RSS、网络接收、正常成功请求吞吐或模型费用，不能把此比例用作整个 Agent 加速比。有效响应仍需要聚合，摘要器自身也有有界文本 builder。

## 复跑与边界

```text
go test ./pkg/core ./pkg/app/contextassembly -run "Usage|Summary|Summarizer" -count=1
go test ./pkg/server -run "^TestPostgres(SerialLLMSummaryAccounting|SummaryFailureKeepsUsageAndTrace)$" -count=1 -timeout=120s
go test ./pkg/core -run "^$" -bench "^BenchmarkRejectedModelChunk$" -benchmem -benchtime=300ms -count=5
# 仅显式配置凭据、SQL DSN 并启用 HARNESS_ACCEPTANCE_LIVE_SERIAL=1 时：
go test ./pkg/server -run "^TestLiveModelSerialModuleAcceptance$/^llm_summary$" -count=1 -json -timeout=300s
```

全仓 PostgreSQL-enabled / LLM-disabled 测试通过：server 55.668 s、storage 33.632 s；build、vet、CI 指定 staticcheck 0.7.0 通过。最终 `go test -race ./pkg/core ./pkg/app/contextassembly ./pkg/server -run "Model|Stream|Usage|Summar|Extractive|AgentLoop|AgentRunTurn" -count=1 -timeout=180s` 通过，分别 1.055 / 1.081 / 23.461 s，包含 SQL 对照、失败和最后补充的负预算回归；随后再次核验最终 build 及受影响包 vet/staticcheck。OpenAPI 验证 102 个操作。core 仍为 34 个生产文件，8721 非空物理行、公共表面 905，新增一个用量消费函数，未放宽门禁。没有运行完整 race/容器/原生 Windows Basic 验收矩阵。Go 缓存及临时目录均在 D 盘。

默认仍有 12 KiB 摘要总上限和抽取选择限制；LLM 仍可能错误压缩、遗漏事实或在有限预算下失败。本轮只验证三个工具来源事实、三次摘要及同进程恢复。服务实际部署需要显式注入摘要 Gate/Telemetry；调用兼容的 `Summarize` 会丢弃返回用量，独立使用者应选择 `SummarizeWithUsage`。目前记录的是上游已报告的计量，未报告费用、异常退出尚未保存的事件、账单核销、并发预算预留、真正进程退出恢复与公平跨框架比较仍不能由这些结果证明。

本地忽略目录 `.tmp-llm-summary-20260906` 保留 01 原版失败、02/03/05 离线回归、04 SQL/trace 成功与失败夹具、06 真实 JSONL、07/08 复制基准、09 以后质量检查；公开证据包含 13 份真实 JSON 和 2 份离线失败 JSON，明确区分。

收尾核验：309 个本地 Markdown 链接有效，Go 格式与 diff 检查通过；15 份 JSON 已做凭据扫描并与原始导出校验 hash。仅停止本次拥有的 PostgreSQL fixture，确认 55436 端口监听消失；没有清理历史证据或触碰其他仓库。
