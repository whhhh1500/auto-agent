# 错误记忆与当前权威来源真实对照

第三十三波使用用户指定本地配置第 6–9 行，通过 Responses 发出真实请求。HEAD 为 `ad90ac0`，冻结测试二进制 SHA-256 为 `9845cae94b715b542b92940f408c942519784f8fdefc7f682408d8ac9a95a25d`。请求模型名称不认证网关后端身份；凭据、随机事实、提示、工具正文和模型正文均不进入安全记录。

## 实验合同

每个模型在 Runtime 前生成一组不透明 current/historical/peer 值。普通四组共享同一 prompt、pacer 与 current/peer 基础事实；automatic_match 仅将本组可见 historical 映射为 current。三个自动组暴露相同 memory.recall + authority.current 菜单；direct-only control 只暴露 authority，工具 schema 更小；read-both diagnostic 使用另一提示，明确要求核对两个来源，不参与自动路线成本胜负。

authority.current 是测试私有、只读、accepted-invocation capability。可用时返回封闭的 current entity/version/code；无记录时成功返回 `{available:false}`，区别于上一波 backend error。memory.recall 使用生产标准 capability 与同 scope SliceStore。所有组经正常 profile、Session、journal 和 assembler；MaxSteps=3、MaxToolCalls=2，允许同轮批量或三轮顺序。缺权威证据即使猜中也不能通过；权威无记录时历史值不能升级为 current。

## 结果

| 请求模型 | 场景 | 路线 | 验收 | adapter 调用 | 已报告输入/输出 |
| --- | --- | --- | --- | ---: | ---: |
| gpt-5.6-terra | automatic_match | authority only | PASS | 2 | 7,851 / 120 |
| gpt-5.6-terra | automatic_conflict | memory + authority | PASS | 2 | 7,982 / 192 |
| gpt-5.6-terra | authority_unavailable | authority only | PASS | 2 | 7,872 / 120 |
| gpt-5.6-terra | direct_only_conflict | authority only | PASS | 2 | 7,528 / 108 |
| gpt-5.6-terra | read_both_conflict | memory + authority | FAIL | 2 | 7,980 / 189 |
| gpt-5.6-luna | automatic_match | authority only | PASS | 2 | 7,873 / 107 |
| gpt-5.6-luna | automatic_conflict | authority only | PASS | 2 | 7,850 / 84 |
| gpt-5.6-luna | authority_unavailable | authority only | PASS | 2 | 7,875 / 123 |
| gpt-5.6-luna | direct_only_conflict | authority only | PASS | 2 | 7,524 / 82 |
| gpt-5.6-luna | read_both_conflict | memory + authority | FAIL | 2 | 7,975 / 158 |

合计 20 次实际 adapter 调用，输入 78,310、输出 1,283，共 **79,593 tokens**。10 条记录逐 invocation 对账均通过，无未知用量或跳过记录。两模型 runner 因各自 diagnostic 失败而退出 1。所有组恰好两轮，没有摘要模型调用。

普通组全部得到正确 current/UNKNOWN，权威调用和封闭结果均进入续轮上下文。direct-only 的输入小计较同模型自动组低，但菜单、schema、实际工具结果和数据条件不同，只是较小能力集 control，不能作为同条件因果节省或全局最小 token 证明。

Terra automatic_conflict 调用了两个工具，Luna automatic_conflict 只调用权威来源。安全 evidence 进一步表明 Terra 的 memory result 成功并与调用配对进入上下文，但 `conflict_exposed=false`；不能由“调用了两个工具”推出看到了旧值。两个 read-both diagnostic 也都实际调用两工具、收到成功结果并完成双方 context pairing，最终与 durable answer 采用正确 current，但验收失败，因为历史值未在 memory result 中出现。它们不算已处理冲突。

现有 v1 sidecar 未保存 query 或结果正文，无法从旧记录判断模型用了哪个查询词，也无法区分空 entries 与其他不含目标值的成功结果。下一 v2 只增加封闭诊断：query 是否精确等于实体标识、entries 是否可解析及数量、历史/current 值是否分别被结果观察到。保持原提示、数据和工具不变，各模型仅重跑一次 diagnostic；不通过指定查询词来制造绿灯。

## 对路由、记忆与候选改进的含义

本轮证明两个模型能在两轮内优先使用权威来源，并在权威无记录时回答 UNKNOWN。它没有证明自动路线处理了错误记忆：Luna 跳过 memory 是正确且更简单的路线；Terra 调用 memory 但没有暴露旧值。read-both 的失败揭示了更基础的检索可观测性缺口。

候选经验必须区分 `authority_only_correct`、`memory_called`、`conflict_exposed` 与 `conflict_exposed_and_resolved`。只有最后一项才可作为抗错误记忆证据。模型自述、工具调用出现或最终碰巧正确都不能直接进入长期策略记忆或触发发布。

这是指定实体的开发集成实验，使用测试私有权威来源与内存 Store。它不覆盖生产数据源、SQL、错误记忆版本治理、自由检索词分布、持久恢复或最低成本稳定性。

安全原始记录存于外层 `programmatic-20260909/optimization-wave-33`。本轮测试前 authority 定向 race、programmatic vet、七项 Python 汇总测试及证据脱敏测试通过。全仓 gate、PG 和硬杀未在本轮执行。

## 第三十四波 v2 diagnostic

v2 保持 read-both diagnostic 的提示、工具、数据和 oracle 不变，只增加封闭的 query 与结果形状观测。冻结测试二进制 SHA-256 为 `1181f228457c0554ee4c2a5f454ccfdf09f7caada6a28bde0641a272f2caff95`。原 query、随机值、工具正文和模型正文均未写入记录。

| 请求模型 | 轮次 / 工具调用 | 输入 / 输出 tokens | query observed | query exact / trim-exact | entries parsed / count | historical observed | authority current / 两个 context pair | answer / durable | 验收 |
| --- | ---: | ---: | --- | --- | --- | --- | --- | --- | --- |
| gpt-5.6-terra | 2 / 2 | 7,991 / 174 | true | false / false | true / 0 | false | true / true | true / true | FAIL |
| gpt-5.6-luna | 2 / 2 | 7,974 / 157 | true | false / false | true / 0 | false | true / true | true / true | FAIL |

两样本共 4 次实际 adapter 调用，输入 15,965、输出 331，共 **16,296 tokens**。两条记录均逐 invocation 对账完整；`conflict_exposed=false` 与 `conflict_exposed_and_resolved=false`，因此 `acceptance_passed=false`。

失败层已定位为模型参数选择与 SliceStore 的字面 substring 检索语义不对齐：模型发出了 memory query，但该 query 既不等于 entity，也不经 TrimSpace 后等于 entity，导致成功 envelope 中为零 entries。这不是 context 丢失、authority backend failure、usage 缺失或最终回答错误。authority current 和两个工具结果都精确进入续轮上下文，最终与 durable 答案仍正确采用 current 值。

下一步仅评估不含具体 entity 的通用 canonical-identifier 描述候选。先以冻结的小样本真实闸门验证 query 命中、权威优先和完整成本，再决定是否进入 held-out 评估；不得保存原 query 或事实值，也不得通过指定查询词重试到绿。

## 第三十五波 v3 生产小闸门

v3 仅评估通用 canonical-identifier 描述候选；二进制 SHA-256 为 `4a00004e7c979ab9c3dd079ab88dbcc0ea887e778cf1c1c874c117736bd882af`。两模型各执行一次冻结的 read-both diagnostic，均为 2 rounds / 2 actual adapter calls。

| 请求模型 | 输入 / 输出 tokens | query exact / trim-exact | entries parsed / count | historical / current observed | 两个 context pair | conflict / resolved | answer / durable / usage | 验收 |
| --- | ---: | --- | --- | --- | --- | --- | --- | --- |
| gpt-5.6-terra | 8,123 / 189 | true / true | true / 1 | true / true | true | true / true | true / true / true | PASS |
| gpt-5.6-luna | 8,114 / 167 | true / true | true / 1 | true / true | true | true / true | true / true / true | PASS |

两样本共 4 calls，输入 16,237、输出 356，共 **16,593 tokens**；2/2 通过。相对 wave34，Terra 增加 132 输入与 15 输出 tokens；Luna 增加 140 输入与 10 输出 tokens；合计增加 272 输入与 25 输出 tokens。所有逐 invocation usage 对账完整。

该同实体小样本只支持“通用描述候选修复了 wave34 已知的参数选择与字面检索不对齐”这一结论。它不证明跨实体稳定性、自由查询词分布、普遍成本收益或生产合同已可发布。下一步是 3 个冻结 held-out candidate-only entities；不得根据本轮结果再次改变描述、实体或提示后重试。

## 第三十六波 held-out candidate-only

冻结 held-out 二进制 SHA-256 为 `9a9aa7d89034fbb3a16cde8ceeaedccb45c70e55506c9e5d3e267e0fc721e60f`。所有 6 个样本均为 2 rounds / 2 actual calls，逐 invocation 与 usage 对账完整。

| 请求模型 | held-out entity | 输入 / 输出 tokens | 验收 |
| --- | --- | ---: | --- |
| gpt-5.6-terra | heldout1 | 8,143 / 187 | PASS |
| gpt-5.6-terra | heldout2 | 8,157 / 215 | PASS |
| gpt-5.6-terra | heldout3 | 8,144 / 189 | PASS |
| gpt-5.6-luna | heldout1 | 8,084 / 186 | FAIL |
| gpt-5.6-luna | heldout2 | 8,088 / 180 | FAIL |
| gpt-5.6-luna | heldout3 | 8,146 / 189 | PASS |

Terra 小计输入 24,444、输出 591 tokens；Luna 小计输入 24,318、输出 555 tokens。总计输入 48,762、输出 1,146，共 **49,908 tokens**、12 calls。Terra 三个 held-out entities 全部通过；Luna 的 heldout1 与 heldout2 query exact/trim-exact 为 false，entries parsed true/count 0，historical/conflict/resolved 均为 false，但 authority、两个 context pair、最终/durable answer、scope 与 journal 均为 true。Luna heldout3 与 Terra 三个样本的完整 v3 predicates 均为 true。

candidate-only held-out 不用于证明相对 token 收益；4/6 只证明部分可行性，候选未通过跨实体 gate。三项 held-out entity 均为小写，尚未覆盖大小写检索语义。下一步仅为 Luna 两个失败形状增加闭合诊断，不重跑样本以筛选通过结果。

## 第三十七波 Luna shape diagnostic

冻结二进制 SHA-256 为 `b141cb5f1e836fc30feeb967713c652f78483ed8b78261572f21e0ebedbf9548`。仅对 Luna heldout1 与 heldout2 各做一次新的独立 diagnostic 调用：heldout1 报告 8,141 输入、170 输出 tokens；heldout2 报告 8,152 输入、166 输出 tokens。合计输入 16,293、输出 336，共 **16,629 tokens**、4 calls；两项均为 2 rounds / 2 calls。

两次 diagnostic 的 `diagnostic_complete=true`。实际 shape exact、query exact、entry count 1、historical/current observed、两个 context pair、conflict exposed/resolved、final/durable answer 与 usage predicates 均为 true。

这两项是 wave36 之后的第二次独立调用，不能覆盖或改写 wave36 同两项的失败记录；本轮也没有观察到旧失败的 raw shape。因此证据只支持单次间行为不稳定，不能确认是 separator 改写或任何单一描述细节导致恢复。下一步评估可选的 `memory.lookup` 精确键能力并检查采样控制；当前接口边界没有 temperature/seed，能作为设计观察，但不能当作 provider 根因。

## 第三十八至四十波：可选 exact lookup

三波均使用显式 opt-in 的 `memory.lookup`，不替代既有 `memory.recall`。所有安全记录只保留封闭谓词、版本、轮次和 provider 报告用量；外层证据分别为 `optimization-wave-38/summary.json`、`optimization-wave-39/summary.json`、`optimization-wave-40/summary.json`，不记录 nonce、凭据、canonical key、提示或工具/模型正文。请求模型名不独立认证网关后端身份，用量也是 provider 报告；每个 model/case 仅一个样本。

| 波次 | 冻结二进制 SHA-256 | 唯一变更与结果 | 轮次 / 调用 | 已报告输入 / 输出 / 总 tokens |
| --- | --- | --- | --- | ---: |
| 38（v4） | `0ee82a…` | V4 提示含 `then`。两模型 × 三 case 的 exact/found/authority/conflict/journal/usage 谓词均为 true；但两个独立只读工具被串行拆为两轮，2 轮上限内没有 final，故 0/6 通过。 | 每样本 2 / 2 | 47,568 / 1,041 / **48,609** |
| 39（v5） | `267f3e…` | 唯一预定的 fixture/prompt treatment 是提示同一 response 调用独立只读工具；live 每格会重建随机 opaque values，token 差异只是非配对单样本观察，不能作因果估计。Terra 3/3，Luna 2/3；所有样本均为 2 轮 / 2 calls。Luna mixed-case case 的 exact=false、found=false，但 authority/final/durable 仍正确。 | 每样本 2 / 2 | 48,504 / 1,217 / **49,721** |
| 40（v6） | `8d10f9…` | 仅测 mixed-case；在 V5 后追加 exact JSON 参数字面量。Terra 1/1、Luna 1/1，合计 2/2 强门通过。 | 每样本 2 / 2 | 16,227 / 346 / **16,573** |

Wave36 的 recall held-out 为 4/6、49,908 tokens；Wave39 的 lookup 为 5/6、49,721 tokens。但二者的菜单、提示和 case3 大小写条件不同，不能作严格因果比较，也不能称 lookup 为全局最小 token。共同 h1/h2 仅显示：Terra recall 16,702→lookup 16,518，二者均通过；Luna recall 16,538（两项失败）→lookup 16,568（两项通过）。

可执行结论仅限合同层：路由器/上下文应把独立只读工具明确标为可并行；canonical key 必须作为结构化参数原样渲染，不能只依赖自然语言复制。`memory.lookup` 保持显式 opt-in，并不取代相关性 `memory.recall`。这些证据最多作为离线候选晋级输入；任何自进化机制不得据此在线改写记忆、提示或路由策略。
