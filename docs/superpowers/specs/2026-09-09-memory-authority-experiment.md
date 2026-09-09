# 记忆与当前权威来源对照（进行中）

前一实验只证明失效记录在工具返回前被过滤，未覆盖仍可读取但内容错误的记忆。本实验把“知道当前事实”和“历史记忆线索”分开，测当前事实的正确率、选路、实际调用和总报告用量。

## 合同

只读 `authority.current` 返回固定 entity 的当前 code 与版本；它是本任务当前事实的权威来源。生产 `memory.recall` 返回同 entity 的历史线索，其中 code 可匹配或冲突。无当前权威记录时返回成功且封闭的 `{available:false}`，不含 code/version；这测数据不可用，不重复上一轮 backend-error。所有值在实验前冻结为不透明随机值，不进入问题文本。该权威来源为测试私有 capability，经正常 accepted invocation/Runtime 执行，不代表已接入生产数据源。

自动组使用 memory+authority 菜单，同一问题询问当前 code；直接基线只暴露 authority，问题文本不变。固定三步、两次工具预算，允许直接调用、同轮批量或顺序查询。自动模式可以跳过记忆，这属于合法路线；没有实际读到冲突记忆时不能声称验证了冲突处理。

开发对照拟包含 automatic_match、automatic_conflict、authority_unavailable、direct_only_conflict。另设 read_both_conflict 诊断组，明确要求核对两个来源，以保证模型实际面对矛盾证据。它的问题版本不同，不能纳入自动组与直接基线的同问题成本比较。各臂使用独立 Session，每模型共享冻结事实；顺序和样本数公开，不反复运行直到通过。

当前 code 正确且确实读取成功权威结果才算成功；不可用组必须实际收到无当前记录结果并回答 UNKNOWN。诊断冲突组还必须观察到两个真实来源的不同值进入模型上下文。不允许通过隐藏错误记忆、给出预期值或静默修正答案得到通过。

## 证据与边界

逐 arm 记录实际 memory/authority 调用与结果、两来源是否被模型看到、冲突暴露是否适用、最终与 durable 答案、peer memory 不变、无跨 scope 泄漏、adapter invocation 与 usage 账本匹配。未知报告保持 unknown；失败和有意的诊断额外调用也计成本。失败原因与指标保持封闭谓词，不落随机值、正文或凭据。

同任务自动与直接基线只有都正确时才讨论成本差值。直接组能力菜单更小，因此是可行性 control，不是同条件因果节省证明。诊断组通过不证明自动选择会主动核对；自动跳过记忆通过不证明抗污染。两者分别报告，避免把检索数量多当作可靠或把少调用当作正确。

离线先用实际工具结果驱动 probe，反例覆盖错误记忆作答、权威无记录后旧值作答、缺权威结果却猜中值、非法 entity、跨 scope。通过后再冻结 live 入口，按用户授权模型进行有限实测。生产记忆写入、TTL、候选自动发布、Core API 均不因本实验扩张。

## 已实现的离线基础

新增 `examples/programmatic/live_memory_authority_shared_test.go` 和 `live_memory_authority_offline_test.go`。标准 memory.recall 与测试私有 authority.current 均经真实 Runtime；权威工具具有 accepted invocation、read permission、entity enum、512-byte 上限及输出 schema，oracle 另验证精确 available 两分支。

五种开发配置已有离线闭环。read-both 另覆盖同轮两工具和 memory→authority→answer 三轮正例；验收按实际工具名/唯一调用 ID 配对，允许预算内 2–3 次 adapter 调用，不依赖 probe 的固定 ID。自动模式未读记忆时只证明权威取证正确，诊断模式必须实际将两份冲突结果带入续轮上下文。

拒绝反例已执行：普通错误答案、猜中当前值却未读权威、冲突后仍采用历史值、权威无记录后采用历史值、非法实体参数。无记录组还要求当前值在全部观察请求、所有工具结果及最终答案中缺席；peer 按完整 Entry 快照核验未改。逐 invocation usage 与 Session 账本独立对账。

root 最终执行 `go test -race ./examples/programmatic -run '^TestMemoryAuthority' -count=1` 通过（1.065s），`go vet ./examples/programmatic` 通过。vet 初次发现反例表复制带 Mutex 的 probe，已改为指针并复验。离线使用确定性 adapter 从实际工具结果作答，不能作为真实模型表现或 token benchmark；尚未新增 live 入口、安全证据 schema 或真实请求。

## 第三十三波真实结果

live 入口、安全 schema 和汇总器已接入。两模型共 10 组、20 次实际 adapter 调用，报告 79,593 tokens；8 组通过，两个 read-both diagnostic 失败。普通组都正确使用权威结果或在无记录时回答 UNKNOWN；所有组均两轮。两个 diagnostic 虽调用两工具并答对，但历史值未被 memory result 观察到，因此不能声称冲突已暴露或解决。完整结果见[真实对照报告](../../verification/2026-09-09-memory-authority-live.md)。

下一 diagnostic v2 保持相同提示、数据与工具，只新增 query 精确分类、召回条目数及历史/current 结果观察谓词，各模型重跑一次以定位失败层。不得先指定查询词再把通过归为模型自主冲突处理。

## 第三十四波 v2 diagnostic 事实

冻结二进制 SHA-256 为 `1181f228457c0554ee4c2a5f454ccfdf09f7caada6a28bde0641a272f2caff95`。两模型各执行一次相同的 read-both diagnostic，均为 2 rounds / 2 actual adapter calls。Terra 报告 7,991 输入、174 输出 tokens；Luna 报告 7,974 输入、157 输出 tokens。合计输入 15,965、输出 331，共 16,296 tokens、4 calls；逐 invocation usage 对账完整。

两条 v2 记录的 `memory_query_observed=true`，但 exact 和 TrimSpace 后 exact 均为 false；memory result envelope 可解析且 entry count 为 0，历史值未观察到。两者的 authority current 值均观察到，两个工具结果均与续轮 context 精确配对，最终与 durable 答案均正确。两条记录仍为 `acceptance_passed=false`，因为 `conflict_exposed=false` 且 `conflict_exposed_and_resolved=false`。

因此当前事实支持的诊断是：参数选择与字面 substring 检索语义不对齐，而不是 context 丢失、authority backend failure 或回答质量失败。原 query、随机值、工具正文和模型正文不进入记录。

后续只可提出不含具体 entity 的通用 canonical-identifier 描述候选。它必须先经冻结小样本真实闸门，证明任意实体的可命中 query、权威优先和完整成本，才值得进入 held-out 评估或讨论生产合同变更。

## 第三十五波 v3 小闸门事实

通用 canonical-identifier 描述候选在同一已知实体 diagnostic 上经过一次冻结小样本真实闸门。二进制 SHA-256 为 `4a00004e7c979ab9c3dd079ab88dbcc0ea887e778cf1c1c874c117736bd882af`。Terra 报告 8,123 输入、189 输出 tokens；Luna 报告 8,114 输入、167 输出 tokens。两者均为 2 rounds / 2 actual calls；合计 16,237 输入、356 输出、16,593 tokens、4 calls，逐 invocation usage 对账完整。

两条记录均为 query exact 与 TrimSpace-exact true、entries parsed true/count 1、historical/current observed true、两个 context pair true、conflict exposed/resolved true、final/durable answers true，最终 2/2 通过。相对 wave34，Terra 增加 132 输入与 15 输出 tokens，Luna 增加 140 输入与 10 输出 tokens，合计增加 272 输入与 25 输出 tokens。

这是同实体的已知失败修复证据，只说明该通用描述候选在此小闸门中恢复了可命中 query 与权威优先。它不证明跨实体稳定性、任意 entity 的稳定路由或生产成本收益。下一步固定为 3 个 held-out candidate-only entities；在其执行前不得基于本轮结果更改描述、实体、提示或以重试筛选通过样本。

## 第三十六波 held-out candidate-only 事实

冻结二进制 SHA-256 为 `9a9aa7d89034fbb3a16cde8ceeaedccb45c70e55506c9e5d3e267e0fc721e60f`。Terra 的 heldout1/2/3 分别报告 8,143/187、8,157/215、8,144/189 输入/输出 tokens，三项均通过，小计 24,444/591。Luna 的 heldout1/2/3 分别报告 8,084/186、8,088/180、8,146/189，其中前两项失败、第三项通过，小计 24,318/555。总计 48,762 输入、1,146 输出、49,908 tokens、12 actual calls；所有样本均为 2 rounds / 2 calls，invocation 与 usage 对账完整。

Luna heldout1/2 的 query exact 与 TrimSpace-exact 均为 false，entries parsed true/count 0，historical observed、conflict exposed 和 resolved 均为 false；authority、两个 context pairs、final/durable answers、scope 与 journal 均为 true。Luna heldout3 与 Terra 三个样本的完整 v3 predicates 均为 true。

因此 candidate-only held-out 不支持相对 token 收益结论。4/6 仅为部分可行性，候选未通过跨实体 gate。三项实体均为小写，大小写检索语义尚未测试。下一步仅对 Luna 两个失败形状增加闭合诊断，不重跑样本到绿。

## 第三十七波 Luna shape diagnostic 事实

冻结二进制 SHA-256 为 `b141cb5f1e836fc30feeb967713c652f78483ed8b78261572f21e0ebedbf9548`。仅 Luna heldout1 与 heldout2 各执行一次新的独立 diagnostic：分别为 8,141/170 与 8,152/166 输入/输出 tokens，合计 16,293/336、16,629 tokens、4 calls；两项均为 2 rounds / 2 calls。

两条记录均 `diagnostic_complete=true`，且实际 shape exact、query exact、entries count 1、historical/current observed、两个 context pairs、conflict exposed/resolved、final/durable answers 与 usage predicates 均为 true。

这些是 wave36 后的第二次独立调用，不覆盖 wave36 相同两项的失败，也未观察到旧失败 raw shape。结论只能是单次间不稳定，不能确认 separator 改写或任何单一描述变化为恢复原因。下一步评估可选 `memory.lookup` 精确键能力并检查采样控制；当前边界未暴露 temperature/seed，这只是设计观察，不能归因于 provider。

## 第三十八至四十波 exact lookup 事实与后续合同

`memory.lookup` 作为独立的显式 opt-in exact-key capability 接入；它不替换 recall。Wave38（`0ee82a…`）的 V4 `then` 提示让两模型 × 三 case 的 exact/found/authority/conflict/journal/usage 谓词全为 true，但两个独立只读工具被串行分两轮，2 轮上限内没有 final，故 0/6。报告 47,568 输入、1,041 输出、48,609 tokens。Wave39（`267f3e…`）唯一预定的 fixture/prompt treatment 是提示同一 response 调用独立只读工具；live 每格会重建随机 opaque values，token 差异只是非配对单样本观察，不能作因果估计。Terra 3/3、Luna 2/3，所有样本 2 rounds / 2 calls，报告 48,504/1,217/49,721 tokens。Luna mixed-case 未 exact/found，却仍有正确 authority/final/durable，保留为失败。Wave40（`8d10f9…`）仅覆盖 mixed-case，在 V5 后追加 exact JSON 参数字面量；Terra 1/1、Luna 1/1，合计 2/2 强门通过，2 rounds / 2 calls，报告 16,227/346/16,573 tokens。

外层 `optimization-wave-38/39/40/summary.json` 各自记录封闭汇总；不写 nonce、凭据、key、提示或正文。请求模型名不独立证明后端身份，usage 为 provider 报告，每个 model/case 均为单次样本。Wave36 recall 的 4/6、49,908 tokens 与 Wave39 lookup 的 5/6、49,721 tokens 因菜单、提示和 case3 大小写不同，不能作因果或全局最低 token 结论；共同 h1/h2 只显示 Terra 16,702→16,518 且都通过，Luna 16,538（两失败）→16,568（两通过）。

后续候选必须让路由器/上下文把独立只读工具标为可并行，并把 canonical key 作为结构化参数原样渲染；不得只靠自然语言复制。任何自进化流程只能离线利用这些记录选择候选，再经既有冻结评测和发布门禁晋级；不得在线自改记忆、提示或路由。
