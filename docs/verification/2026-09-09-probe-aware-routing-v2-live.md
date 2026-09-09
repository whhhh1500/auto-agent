# Probe-aware v2 实时路由验收（第 44–52 波）

状态：第 52 波在一个开发 fixture 上为两个**请求模型**各取得一组无重试、双 arm 的完整通过结果。它支持此 fixture 中的 `auto_probe_once` 选 PTC 后，比其 `direct_only` control 报告更少 token 和更小最终输入上下文；它不证明后端模型身份、账单、统计稳定性、全局最优或该差异的唯一因果来源，也不触发线上默认路由或候选自动晋级。

## 冻结范围与证据

本记录对应 `TestLiveProbeRouteV2Acceptance`。第 52 波的私有、不可提交证据位于 `D:\cc\auto_agent\.codex-v46-audit\programmatic-20260909\optimization-wave-52\`：

- 冻结二进制：`programmatic-v2-live.test.exe`，SHA-256 `020257425785741dff557b20775329873ae6f0eeada4fdeeb112a40c69b0af16`。
- 构建时 HEAD：`ad90ac0095ca0a5047437405ad37b1ba44669f56`；manifest 同时记录工作树为 dirty，故该 SHA 与 HEAD 是证据身份，不是干净提交的可复现发布物。
- evidence schema：`harness.programmatic.live-probe-route-v2-evidence/v3`；route implementation revision 是 `programmatic-route-projection/v2-probe-once-host-projected-ptc-choice-capacity-admission`。
- 每个请求模型各有一个 `auto_probe_once` 与一个 `direct_only` 开发样本，均无自动重试。真实请求使用 Responses 协议、`stream=true`、`store=false`；记录只保留安全的结构、计量、上下文和效果谓词，不保存 prompt、回答、参数、工具内容、PTC 源码或凭据。

`gpt-5.6-terra` 和 `gpt-5.6-luna` 是传给兼容网关的配置名，不能认证该网关实际使用的后端身份。下表的 usage 是 provider 报告、并由该样本的 adapter call、session usage ledger 与唯一 ledger invocation ID 对账的用量；它不是独立账单、计费金额或 HTTP 请求以外重试的证明。

两 arm 都从一次中性的 `fixture.inventory` probe 开始。auto 的固定 envelope 是 probe、host-projected Direct/PTC choice、无工具 final，共三轮；direct control 可至十轮，在保留两项直接工具菜单的条件下分批或逐项读取，最后一轮不得调用工具。两 arm 的工具预算都是十。PTC 选择只接受候选的 `selection` 与安全的顶层 `projection`，宿主生成并校验 canonical PTC source；resolver 与普通 sequential executor 仍在路径中。

## 第 52 波结果

| 请求模型 | arm / 实际路线 | 模型轮次 | 输入 / 输出 / 合计 reported tokens | 节省相对同模型 direct | 最终输入 tokens | 完成 journal | 验收 |
| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| `gpt-5.6-terra` | auto / PTC | 3 | 12,293 / 234 / 12,527 | 4,701 / 17,228 = **27.3%** | 4,237 | 10 | PASS |
| `gpt-5.6-terra` | direct | 3 | 16,645 / 583 / 17,228 | baseline | 36,831 | 9 | PASS |
| `gpt-5.6-luna` | auto / PTC | 3 | 12,293 / 240 / 12,533 | 50,340 / 62,873 = **80.1%** | 4,237 | 10 | PASS |
| `gpt-5.6-luna` | direct | 10 | 62,263 / 610 / 62,873 | baseline | 36,779 | 9 | PASS |

百分比分别按 `(17,228 - 12,527) / 17,228 = 27.2877…%` 与 `(62,873 - 12,533) / 62,873 = 80.0674…%` 计算，再显示到一位小数。auto 的 10 条 journal 是一条 probe、一次 host-projected PTC execute 与八个程序子调用；direct 的 9 条是 probe 加八个 detail。它们不是等价的顶层菜单或相同 envelope：auto 的 final menu 为空，direct 在 final 仍有两项直接工具。因此，这是被验收的端到端路线对照，不能把 token 差异归因于某一个 prompt、菜单、模型能力或 PTC 本身。

四条 v3 record 都通过以下同一份 fixture 合同：八个 candidate 各被选择和执行一次（`8/8`, `exact_once=true`, `coverage_status=complete`，零 duplicate）；完整 provider usage；usage protocol 一致；ledger usage matched；唯一 ledger invocation ID；三或十次 context assembly 均无失败和无 dropped group；所有可观察 wire body 有效 JSON、`stream=true`、`store=false`，有工具时投影工具数与 `parallel_tool_calls=true` 相符。实际传给 assembler 的 limits 均为 `128000/4096`。这些 predicate 属于冻结 fixture record 及其受控的 Session、Tool Journal、usage ledger、wire/context 验收，不由 OTel span 单独证明。运行时 route receipt 只是 content-free 的观测投影，不替代 durable Session/Journal，也不构成 `CoverageContract` 或 release 输入。这些 predicate 只说明这个 fixture 的结构、效果和局部对账通过，不能证明其他任务上的语义覆盖。

## 第 44–51 波：失败、修复与可比性边界

第 52 波不是连续成功的挑选结果，私有目录完整保留先前波次：

| 波次 | 观察结果 | 处理后仍应保留的边界 |
| --- | --- | --- |
| 44–46 | v2 fixture 和协议收敛期间有 runtime/acceptance failures；第 46 波两模型的 direct control 通过，但 auto 均失败。 | 早期 record 缺少后续 v2/v3 的容量、coverage 与完整 context 字段，不能拿来补写成第 52 波强合同的通过。 |
| 47 | Terra auto 在 final request 因 fixture 未装配 production context assembler 而收到 HTTP 400；最终轮 usage 未报告，ledger 不匹配。 | 这不是 PTC 成本为零或答案/账本成功；失败 usage 必须保持未知/不完整。 |
| 48 | 修正 fixture assembler 组合后，Terra auto PTC 通过；direct 仍发生 runtime failure。该历史 recording wrapper 走 32,768/4,096 fallback。 | 32K 是 wrapper 未透传 adapter limits 时的压力窗口，不能说成 provider 原生窗口，也不能与后续 128K 对照直接合并。 |
| 49 | 在透传上下文额度后，Terra auto PTC 与 direct 都通过；Luna auto PTC 通过，但 direct 在限制内未完成。 | Luna direct 的失败不构成 auto 的有效成本基线；它也不支持模型间排序。 |
| 50 | Terra 的 auto PTC 三轮（12,269/252）相对十轮 direct（62,177/643）显示很大局部收益。 | auto record 仍错误地记录 32,768/4,096、direct 为 128,000/4,096，limits 不同，不能把该差异当作公平成本结论。 |
| 51 | limits 均为 128,000/4,096；Terra direct 十轮、8/8 通过，但 auto 锁定 direct 后只实际 detail 1/8，答案不匹配，因而 acceptance failure。 | Runtime `completed`、完整 usage 和正常 wire 都不能替代 exact coverage/答案合同。 |
| 52 | 增加 choice guidance、完整 adapter-limit forwarding、effect 前 capacity admission，以及 v3 exact coverage 后，Terra 与 Luna 各自 auto/direct 四臂均通过。 | 这是每模型一份无重试开发样本，不证明稳定收益、留出表现或全局最优。 |

Wave 47 的 HTTP 400、Wave 48 的 32K fallback、Wave 49 的 Luna direct limit、Wave 50 的 limit 不一致和 Wave 51 的 `1/8` partial direct 都是结论边界的一部分。第 52 波修复的是测试和宿主的可观察合同，不是对旧 record 的重跑、重算或删除。

## 当前结论与明确不成立的推论

在这个八项、独立、只读的开发任务上，probe 后的 host-projected PTC choice 在两个请求模型样本中均完成并通过精确覆盖；它的 reported total token 和 final input token 都低于对应 direct control。该结果足以作为后续受控评估的候选证据，不足以改变默认产品路线。

下列结论不成立：

1. 请求模型名不能证明网关后端身份；provider usage 不是账单，也没有价格、缓存或因果归因。
2. 每模型只有一个无重试开发样本，未证明统计稳定性、跨模型排名、任务分布效果、最小成本或全局最优。
3. auto 与 direct 的可见菜单、轮次上限及 final 菜单不同；runner JSON 是脱离内容的 receipt，能支持本地结构谓词，不能独立重放或证明模型内容质量。
4. v3 不把 Summarizer 的 limits 链统一；已有 summary/rollover 路径仍须单独验证它接收到的预算来源和容量行为。
5. probe-first 是条件式策略：如果 Memory/RAG 在 probe 前先被调用，当前首合格动作锁会把 Run 锁到 Direct，不能把这份 inventory-only fixture 外推为记忆或检索任务的自动路由。
6. 同一 Run 的并发 `ContinueTurn` 仍需要 durable run lease/fence；本波串行、独立 session 的成功不证明并发恢复或重复执行安全。

## 下一步的发布控制面

当前工作树已为 terminal `auto_probe_once` 增加 content-free 的只读 route-evidence trace 投影，并让可选 efficiency gate 接受冻结 v2 route metadata。该投影只重建 Session、Journal、usage ledger 与 summary archive 中可证明的结构和计量事实；它在 run/queue control 落成终态后有界执行，允许因进程失败而缺失或重复，不是 SQL 审计权威。efficiency gate 会绑定不同的 candidate/baseline run、baseline 引用和除五个 route 字段外完全相同的 composition metadata；它仍依赖评测侧质量/ledger/context 证据，不能替代可信的任务级 `CoverageContract`。

后续仍应定义可信的 `CoverageContract`，让任务、候选、外部效果、质量和成本口径都能版本化并由宿主验证。候选路径只能沿着 sealed evidence → held-out gate → canary → manual promotion 前进；开发 evidence 不可自行晋级。

自进化只能离线提出候选并接受上述门禁，不能在线修改权限、执行计划或长期记忆。任何 future route decision 仍须保留既有 Runtime、Journal、审批、预算和恢复保护；没有可信容量或 coverage 证据时应保持 unknown/fail closed，而不是从历史样本补出成本或成功。
