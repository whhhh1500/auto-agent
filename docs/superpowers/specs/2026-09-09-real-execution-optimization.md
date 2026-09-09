# 真实执行优化与证据闭环

状态：进行中。目标涵盖真实选路、token、模型回合、任务质量，以及它们与会话、上下文、记忆、可观测性和候选策略改进的联系。本计划不是完成声明；此前五个场景通过不构成整体目标通过。

## 优化目标

先保证任务结果、权限、审批、未知结果非重放和预算边界，再比较完成同一任务所需的总资源。比较至少包含任务模型、摘要/反思模型、失败与修复尝试的输入/输出 tokens、模型请求数、工具调用数和实际延迟。报告用量缺失必须标明未知，不能当成零成本。节流等待独立于 provider 耗时记录；不凭原始 token 数推导价格或缓存收益。

不能通过固定强制 PTC、降低答案要求、给出预期答案、放宽失败预算，或只保留成功样本来满足目标。不存在由六个样例证明的全局最优；完成判断必须基于可复现、覆盖约定场景的候选对照和明确剩余证据边界。

## 当前证据与下一实验

下一实验采用[记忆与权威来源合同](2026-09-09-memory-authority-experiment.md)：自动组允许跳过记忆，独立 read-both 诊断组证明实际冲突暴露；较小工具菜单的直接组只作可行性 control。权威无记录使用成功的 `available:false`，不与 backend error 混淆。正确猜中值但未读取权威、读取冲突后采用旧值、权威无记录时采用旧值都不得通过。

第三十三波[真实权威对照](../../verification/2026-09-09-memory-authority-live.md)的普通 8 组通过、两个 read-both diagnostic 失败，20 次调用报告 79,593 tokens。失败组最终答案正确且两工具结果均配对，但历史值未被召回结果观察到，故不能计为处理过冲突。下一 v2 只补封闭检索诊断，不改变提示或指定查询词。

后续 Wave38–40 的 exact lookup 证据也在同一[权威对照记录](../../verification/2026-09-09-memory-authority-live.md#第三十八至四十波可选-exact-lookup)中。Wave38 的 0/6 是独立只读工具被串行拆为两轮、未留下 final 的调度失败，不是 exact/found/authority/conflict/journal/usage 谓词失败；Wave39 唯一预定的 fixture/prompt treatment 是“同一 response、独立只读”的并行提示，live 每格会重建随机 opaque values，token 差异只是非配对单样本观察，不能作因果估计；该波为 Terra 3/3、Luna 2/3。Wave40 以结构化 exact JSON 参数覆盖 mixed-case 后为 Terra 1/1、Luna 1/1，合计 2/2。它支持将独立只读调用显式暴露为可并行，并将 canonical key 作为结构化参数传递；不支持把 lookup 写成默认菜单、替代 recall、宣称最低 token，或让候选自动在线发布。

最新第三十二波的[记忆有效性实测](../../verification/2026-09-09-memory-validity-live.md)六臂均通过，12 次真实 adapter 调用共报告 47,315 tokens，逐次计量与上下文隔离均核验。它使用测试私有过滤器，不代表生产 TTL 或最低成本。第三十一波的[源码可用性对照](../../verification/2026-09-09-repository-availability-and-strict-gate.md)仍保留失败边界：完整源码两模型通过，缺失 compiler 源码两模型仍将优先级字段填为非 null 并误报完整。两轮直接读取和完整计量不代替质量通过。

第二十九波已完成[真实仓库取证与逐次用量对账](../../verification/2026-09-09-repository-grounding-and-invocation-accounting.md)：两个模型都自主批量读取四个冻结源码文件，两轮完成，数值/符号/答案和每 invocation usage 均通过。四次调用共报告 33,603 token。它补充需要模型阅读和解释源码的真实项目开发场景，不是未见留出集或最低成本证明。下一重点仍为冻结的独立任务分布、可信预算信息与候选成本门禁。

此前第二十八波见[真实模型复验](../../verification/2026-09-09-live-model-recheck.md)：冻结二进制下六项均通过，18 次调用共报告 133,627 token；Terra 自动使用直接列表加 PTC 详情，Luna 自动使用直接路径，均三轮。另行核对最终上下文限额、完整结果和零丢弃，并补强下一版本测试的相应断言。此次成功不覆盖此前失败。此前的[选路信息与 adapter 额度](../../verification/2026-09-09-routing-information-and-adapter-limits.md)继续保留：旧计数包装器未透传 adapter limits，32K 结果属于回退窗口压力证据；128,000/4,096 是独立透传的编译额度。模型仍看不到本次可用预算，下一阶段须冻结协议、补可信决策预算信息，并转向未参与调优的任务分布。

第四十一波以每个请求模型一个、无重试的匹配 triad 检验强制 `sequential_react`、`parallel_react` 与 `ptc`。Terra 的两个强制直接路由臂不遵从、PTC 通过；Luna 的顺序臂实际批量而合同失败、并行通过，PTC 首轮忽略指令后第二轮 HTTP 429，完整 usage 因而未知。请求模型名不认证网关后端，token 仅为 provider 报告；提示与 `parallel_tool_calls` flag 到达请求也不等于宿主有稳定路由。详见[路由效率三臂实测](../../verification/2026-09-09-routing-efficiency-triad.md)。该单 triad 开发证据不能排名全局最优。

第 44–52 波的 probe-aware v2 受控对照现已形成独立记录。第 52 波使用 evidence v3 和冻结 binary `020257425785741dff557b20775329873ae6f0eeada4fdeeb112a40c69b0af16`，在 `128000/4096` 实际 assembler limits 下，每个请求模型只运行一个无重试 auto/direct 开发样本。Terra 的 auto PTC 3 轮为 12,293/234=12,527，direct 3 轮为 16,645/583=17,228，少 4,701（27.3%）；Luna 的 auto PTC 3 轮为 12,293/240=12,533，direct 10 轮为 62,263/610=62,873，少 50,340（80.1%）。双方四臂均为 8/8 exact once、coverage complete，并通过 usage/ledger/wire/context 谓词；auto journal 为 10、direct 为 9，最终 input tokens 是 4,237 对 Terra 36,831/Luna 36,779。Wave 47 fixture 缺 assembler 的 HTTP 400、Wave 48 wrapper 32K、Wave 49 Luna direct limit、Wave 50 auto limits 错配和 Wave 51 Terra auto 只完成 1/8 均未删除或重写。该对照的 menu/envelope 不同，runner JSON 只是不含内容 receipt；每模型一个开发样本、请求模型名和 reported usage 都不能证明后端身份、账单、因果、稳定性或全局最优。本段 predicate 来自冻结 fixture record 的 Session、Journal、usage ledger 与 wire/context 对账，不由 OTel route receipt 单独证明。详见[Probe-aware v2 实时路由验收](../../verification/2026-09-09-probe-aware-routing-v2-live.md)。

此前第二十三波的[大结果实测](../../verification/2026-09-09-large-tool-results.md)仍保留：v3 首请求不含结果大小，不能据此证明按大小选路。v4 只增加 manifest 派生的真实输出上界，不改变数据、窗口、预算或成功标准；额度透传和语法说明修订均用独立冻结版本验证。

早期基线见 `docs/verification/2026-09-09-programmatic-live-models.md`：当时两模型自主批量均逐项直接调用十轮；Terra 指定 PTC 三轮完成，Luna 指定 PTC 尚未完成。这个对照没有覆盖“一个模型响应提交多个独立工具调用”，不能将低效的逐项直接调用当成最优直接基线。

后续实测发现兼容网关在省略 `parallel_tool_calls` 时返回 false。显式启用后的第二波六项对照只有 Terra 批量直接及带输出合同 PTC 通过；Luna 仍有遵循及程序错误。完整结果见 [执行优化记录](../../verification/2026-09-09-execution-optimization.md)。因此，早期十轮结果不能单纯解释为模型自主偏好，也不能继续作为 PTC 优于最佳直接调用的依据。持久评测效率投影和逐 case JSON 已补充，真实稳定性与跨上下文验证仍在进行。

后续批量直接、记忆与摘要对照已完成若干开发波次，见 [记忆与上下文优化记录](../../verification/2026-09-09-memory-context-optimization.md)。渐进展示候选让两模型自主 n8 都三轮完成，但指定 PTC 仍失败，故尚未接入默认生产路径。多个模型 tool calls 不等于并行产生副作用，实际工具继续走既有保护执行器。

已补固定、无数据泄露的 PTC 编译/运行诊断、绑定计数与 guard 错误身份传播。第十二波 Terra 通过，Luna 编译和绑定均正确但出现 `runtime_for_not_list`；需要针对结果包装合同继续验证，不能反复重试直到偶然通过。诊断不表示先前工具没有执行，修改程序后不可盲目重跑有副作用的前缀。

## 验证矩阵

| 需求 | 必须比较或核验 | 当前缺口 |
| --- | --- | --- |
| 选路正确 | 简单直接、可批量直接、确定性依赖链/PTC、需模型解释的新证据、工具不可用 | 简单/固定批量及四文件源码取证已测；源码缺失已实测失败，独立依赖分布及可靠缺证处理未通过 |
| token 效率 | 同样正确结果下，自动/最佳直接基线/PTC；失败与摘要成本计入 | 已加批量直接对照及 reported usage 投影；尚无跨任务最低成本证明 |
| 回合与延迟 | 固定预算、实际请求与步骤区分、请求耗时与节流等待区分 | 新波次已分开记录；渐进展示候选让 Luna 自主批量三轮，但 PTC 未通过，未生产整合 |
| 稳定性 | 冻结开发集与未参与调优的留出集，两个模型，受限重复样本 | 当前小样本不能说明分布或稳定收益 |
| 会话与上下文 | 新会话/长历史/摘要后/恢复后结果一致；目录与绑定仍可用；嵌套结果不重复灌入模型 | 短历史、近12KiB摘要三次滚动、SQLite已完成会话重开已实测；模型窗口饱和、未知结果/PG恢复及更长分布未完成 |
| 记忆 | 有无相关记忆、无关/过期/错误记忆对正确率与成本的影响；作用域隔离 | 指定 lookup、跨作用域及测试私有过期/后端故障已实测通过；生产持久/错误记忆与权威冲突/同任务成本对照未覆盖 |
| 可观测性 | 按 run/case/版本保留失败、usage、步骤、工具效果和上下文证据；未知字段不补零 | 已加持久评测投影及 terminal v2 content-free trace receipt；trace 是有界 best effort，外部效果与最终 context assembly 仍为 unknown，旧波次不回填 |
| 候选改进闭环 | 观测 → 带来源候选 → 开发集验证 → 冻结留出集 → 既有发布门禁 | 没有自进化控制器，先复用证据与门禁，不自动写长期记忆 |

## 实验与版本纪律

host envelope 与 bindings 字段说明的受控对照已经执行，仍有跨模型失败。下一比较固定大详情字段：自动、批量直接、PTC 三臂使用相同四工具菜单、typed 输出合同、任务正文、MaxSteps=10 和 MaxToolCalls=11。两 forced 臂只增加策略标签。保留 4KiB 字段压力场景；另有独立 2KiB 对照用于区分数据处理收益与窗口容量不足，不替换压力场景。

默认 Core 窗口/输出预算是 32,768/4,096，Assembler 安全余量 1,024 且按 UTF-8 字节数估算 token 上界，因此 8×4KiB 正文已超过 27,648 的本地输入预算。不得暗调窗口或估算器使某一路径通过。记录每轮组装的丢弃组、实际模型消息中 detail 结果数量及实际 fixture 效果；比较 provider 报告 token，不能用上下文字节数冒充模型用量。

该实验已推进到 v3 条件化 catalog 指令，见[大结果实测](../../verification/2026-09-09-large-tool-results.md)。v1 的工具许可歧义与 v2 的无条件 catalog 冲突均保留为失败开发基线，生产 general profile 同步修正为选择 PTC 后才取 bindings。出站安全观测核对 instructions 字节数，不能由模型不遵从推断网关丢弃指令。

历史 v3 direct ToolSchema 未包含 MaxOutputBytes，两个大小的首请求正文哈希相同；v4 及后续私有候选已将 manifest 派生输出上界加入 description，相关实测见第二十四波起记录。生产通用 ToolSchema 没有因此获得预算字段。执行前容量 preflight 仍未实现：它须处理未知上界、结果组合、既有上下文和执行副作用，不能简单用“八项乘上限”全局拒绝，也不能冒充模型自主决策。现有逐工具 hook 不具备原子批次准入语义。

1. 已经看过并用于调优的 inventory 样本属于开发样本，不能再当作留出验收。
2. 后续任务集改变数据规模、无关项、结果大小与依赖形状。输入、工具数据、期望结果和候选策略分别版本化；期望结果不进入模型提示。
3. 每轮实验限制实际调用数量和输出预算，串行执行，遵守端点限流。连接错误与任务失败分别记录；不自动无限重试。
4. 结构化结果包含请求模型/协议、源码或组合版本、case/prompt 指纹、每轮用量及工具选择、最终状态和错误分类。输出文件不可覆盖既有实验。
5. 开发对照允许强制某一策略以建立成本基线；自动选路验收必须不给出该样本的最佳模式标签。
6. 策略收益以正确性门槛下的成本与延迟对照表达。不把一次更低 token 的失败回答当成优化。

## 可复用架构与边界

当前架构审核补充：现有 summary live fixture 每臂只有一次归档，下一长会话样本需跨真实三个 RunTurn 验证 OLD 被显式 NEW 更新后仍能正确回答 current，并逐 RunID 对账。多次 replace 的历史覆盖范围可以重叠，不能错误要求所有历史 summary event 范围互不相交；应核验最终有效投影、归档推进和上下文与持久摘要一致。使用测试小消息阈值须明确区别于生产 120/60；本地 assembler 声明的小 token 窗口也不是厂商原生窗口证明。预算饱和与事实更新应分别记录，未知用量不能补零。

本地公开 Summarize 最小探针已复现：11,950/12,000-byte prior summary 与一条短新更新输入时，旧摘要被保留，新更新被遗漏且函数无错误；11,800-byte prior 能同时保留。下一步骤先用固定高体积历史完成真实 rollover 基线，再针对这一容量分配缺陷修复；不得改小输入或移动更新位置让样本通过。此探针本身不等于真实模型回答失败证据。

该步骤已完成开发对照：第十六波两个真实模型均丢失 NEW，第十七波在相同历史和提示下均正确使用 NEW，详情见[重复滚动验证](../../verification/2026-09-09-summary-rollover-and-evidence.md)。修复只预留近期用户空间，旧摘要仍可能被明确遗漏；不能推广为无限历史保留。

第十九波两个模型均通过 SQLite 关闭重开后继续回答及再次 SQL 保存/加载，见[PTC 与恢复实测](../../verification/2026-09-09-ptc-stability-and-session-recovery.md)。该样本不覆盖硬杀、未知请求 outcome 或当前授权刷新。下一 PTC 效益对照需增加大中间结果而非只比较八项小记录，并保留自动/直接/PTC 同任务基线。

可选 release efficiency gate 现在读取每 case 的 ExecutionEvidence、逐 invocation ledger 和 context assembly 计量，并接受冻结 v1 route 或 v2 `auto_probe_once` 候选。它要求不同 candidate/baseline run、精确 baseline 引用、同一 holdout cohort、同一非 route composition、完整质量与用量证据；缺失或漂移均 inconclusive。该门禁约束的是 provider-reported token 与已记录的调用/上下文/工具事件，仍不能把 StepsStarted 冒充请求已发送、把 reported usage 冒充账单，或证明任务级候选 coverage 和外部副作用完整。

普通 live 观测已开始补逐 invocation 对账：adapter 入口次数独立于节流前取消；通过同 run/step 的 StepStart 序号对应 model usage；数值互换但总和相同必须失败。缺失报告、冲突报告和普通记录之外的 summary 用量都不能计为完整。旧记录不回填这一更强证据。新仓库取证任务使用冻结的四个源码文件、只读枚举路径工具和独立 AST oracle；这属于真实仓库集成开发样本，不能因为换了数据就冒称未见留出集。

预算信息候选仍在设计阶段。冻结 RunStart composition 能证明初始 MaxSteps/MaxToolCalls，assembler request 能证明本地窗口和输出预留；提示必须在组装预算计算之前加入，不能由 adapter 在已组装的请求上追加字节。不能泛化 `MaxToolCalls - distinct EvToolCall` 为精确剩余额度：EvToolCall 包含嵌套调用，拒绝和恢复路径与 guard 的计数语义也不同。没有状态证明时只能展示初始 cap、已观察事件数量或 unknown；不得把这些值冒充批次预留、provider 余额或原生窗口认证。此边界未完成前不接入生产动态预算提示。

`pkg/evaluation` 已隔离 case Session 并冻结 dataset/artifact revision，`pkg/server/server_evaluation.go` 已有 candidate 对 baseline 的 release gate。当前 optional `efficiency-gate/v1` 在质量与能力兼容性通过后作为附加 release 条件运行；未请求或 disabled 时为 `not_requested`，不改变既有质量 gate。它不读取 OTel route receipt，也不自动触发 canary、promotion 或策略演化。步骤开始不证明网络请求已发送；只有 usage 报告也不能证明所有请求都有完整计量。

观测规则命中是候选来源，不是任务真值。错误回答、自述成功、训练样本命中和未完成运行不能直接写为长期经验。记忆优化应区分任务事实与策略经验，保留来源、作用域、版本、失效和撤销能力；首先验证效果，再考虑接入既有发布控制面。

后续路由架构应由宿主先做确定性 eligibility，再将同一 Run 的首个合格模型动作自动锁定为路线；候选只可离线生成、评测与 canary 收证，人工审阅后才可提升。线上不得依据单次输出自动改写路由或晋级策略。

PTC、直接调用、workflow/subagent/graph 是不同层面的选择，不能通过工具模式实验宣称多 Agent 编排也达到最优。CodePTC 仍按已有决定保留路由/接口，不启用运行器。

## 完成审计

第三十一波以 v2 合同独立重跑，完整源码两模型通过，compiler 缺失两模型仍失败：数值为 null，配置优先级仍非 null，状态误报完整；引用均通过。8 次真实调用报告 61,998 tokens、逐调用对账通过。模型自述完整不能直接成为长期经验或发布依据。下一步实际失效过滤、后端故障和 limit 语义遵循[记忆有效性实验合同](2026-09-09-memory-validity-experiment.md)，当前生产尚无 TTL。

第三十波证据缺失对照四项仅一项通过，8 次真实 adapter 调用报告 61,561 tokens，逐次用量均对账；状态/字段/引用失败与合同歧义均保留，见[证据缺失与严格门禁](../../verification/2026-09-09-repository-availability-and-strict-gate.md)。已增加可选 `require_all_cases`，严格 release 对照冻结 dataset，避免平均阈值和交集比较掩盖失败或缺项。该门禁只约束既定 case 质量，尚不包含完整成本证明或自动策略进化。修订实验合同必须另起版本，不能覆盖失败结果。

上述每一行都需要与其范围匹配的当前证据。新单测、文档、一个通过样例或一般性绿灯不能代替真实选路/成本/跨会话验收。保留失败及未测项目；未满足的项目继续处于进行中，不用缩小目标来关闭目标。
