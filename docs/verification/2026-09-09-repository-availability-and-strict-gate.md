# 证据缺失实测与逐 case 发布门禁

第三十波继续使用用户指定本地文件第 6–9 行的配置，通过 Responses 发出真实请求。凭据不写入仓库或报告。请求模型名称不认证网关后端身份。本轮四个开发样本仅一个通过；失败记录和已报告用量全部保留，不作为独立留出集或最低成本证明。

## 冻结合同

基于 HEAD `ad90ac0` 的未提交工作区，测试二进制 SHA-256 为 `efc8d9f1ea03d611337e97b8b1f1863d88c8c7cac2bb8b13c9009d0c1e6fa33c`。入口为 `TestLiveRepositoryAvailabilityAcceptance`，fixture 为 `repository_availability_v1`，记录 schema 为 `harness.programmatic.live-repository-availability/v1`。

每个模型依次运行完整四文件、缺少 compiler 文件两个 arm。两臂提示相同、Runtime 独立，读取工具的枚举和内存内容均按可用文件构造；缺失文件的源码不进入模型。完整源码 bundle 与[第二十九波](2026-09-09-repository-grounding-and-invocation-accounting.md)相同。工具菜单保留直接读取及 program catalog/execute。模型须读取每个可用文件一次，给出 fallback 与 compiler 限额及引用；缺失 compiler 证据时三个 compiler 字段应为 null，状态为 `insufficient_evidence`。

每臂 MaxSteps=6、MaxToolCalls=8、输出 cap=4,096；assembler 使用 adapter 编译配置 128,000/4,096，并独立验证组装成功。这不是 provider 原生容量认证。每模型跨两臂共享 15 秒请求起始间隔；不自动重试。

## 实测

| 请求模型 | 可用证据 | 答案验收 | adapter 调用 | 已报告输入 | 已报告输出 |
| --- | --- | --- | ---: | ---: | ---: |
| gpt-5.6-terra | 完整 | PASS | 2 | 16,075 | 634 |
| gpt-5.6-terra | compiler 缺失 | FAIL | 2 | 13,050 | 676 |
| gpt-5.6-luna | 完整 | FAIL | 2 | 16,079 | 1,015 |
| gpt-5.6-luna | compiler 缺失 | FAIL | 2 | 13,057 | 975 |

合计 8 次 adapter 调用，输入 58,261、输出 3,300，共 **61,561 tokens**。四条记录逐 invocation 对账均通过，无未知 usage 样本；这是已报告用量，不是独立账单。汇总器接受四条记录，无无效或未知 schema 被跳过。两个模型进程及总 runner 均退出 1，不能由 Runtime completed 判定答案合格。

四组都首轮直接读取、第二轮回答，未选择 PTC。两轮结束支持本样本的轮次可控性，不能证明路由最优。缺失组输入更少但答案验收失败，不能计为效率收益。

失败谓词分开记录：

- Terra 缺失组：状态和 compiler 字段检查失败，fallback 与引用检查通过。这证明至少一个应为 null 的 compiler 字段非 null，但没有保存其原始值。
- Luna 完整组：compiler 字段和符号引用检查失败，状态、fallback 与路径检查通过。
- Luna 缺失组：三个 compiler 字段全部为 null，状态与符号引用检查失败；不能将此概括为编造 compiler 数值。

原始模型答案未持久化，现有谓词不能恢复具体错误值。合同复核另发现两项可能影响归因的歧义：`configured_output_overrides_default` 未明确指非零配置覆盖默认值；提示允许 unqualified source identifier，而额外引用检查只接受函数声明和 ValueSpec 名称，可能拒绝合法局部变量、类型或字段。现有结果保留原合同结论，不能认定所有失败都来自模型。修订应使用新 fixture 版本并增加封闭诊断字段，在新一轮同时重跑全部对照；不得回改旧结果或反复运行直到通过。

## 第三十一波：明确合同后的独立重跑

`repository_availability_v2` 明确非零配置相对默认值的源码优先级、全部字段有证据才为 complete、每个可用路径必须有引用。提示不包含变量名、预期数值或正确布尔值。required 定义/函数集不变；额外引用接受当前文件 AST 中的精确未限定 identifier，新增离线反例保证局部变量可用、虚构与限定名拒绝、必需引用不能省略。

冻结二进制 SHA-256 为 `1b0b737066c6c4ddb4ba60f54a86f6bd731cb40a583c816004f533e35cf46186`。第 31 波单独保存，未覆盖或重试第 30 波。schema 保持 v1 的兼容结构，并新增字段级安全诊断；fixture variant 和二进制哈希区分语义版本。

| 请求模型 | 可用证据 | 答案验收 | adapter 调用 | 已报告输入 | 已报告输出 |
| --- | --- | --- | ---: | ---: | ---: |
| gpt-5.6-terra | 完整 | PASS | 2 | 16,218 | 705 |
| gpt-5.6-terra | compiler 缺失 | FAIL | 2 | 13,199 | 751 |
| gpt-5.6-luna | 完整 | PASS | 2 | 16,220 | 824 |
| gpt-5.6-luna | compiler 缺失 | FAIL | 2 | 13,201 | 880 |

合计 8 次 adapter 调用，输入 58,838、输出 3,160，共 **61,998 tokens**；四条记录逐调用对账均通过、无未知用量或跳过记录。四组仍为直接读取后回答。完整组两模型全部答案/引用检查通过；缺失组两模型均将 compiler window/output 留 null，但配置优先级字段非 null，状态为 recognized-but-inconsistent（在该臂即 complete）。required refs、额外 identifiers、fallback、路径、三方答案一致性全部通过。因此这两项失败不再能归因于局部变量引用被拒；不保存原始布尔值，不能说具体回答 true 还是 false。

本轮整体及两模型进程退出 1。v2 同时修改了问题表述和额外引用校验，且每组仅一个样本，不能把 Luna 完整组转绿全部归因于某一个修订或声称统计稳定。缺失组应继续作为质量失败，不能因两轮结束或输入更少成为成功经验。后续候选评测必须独立检验事实证据完整性，不能将模型自述 complete 当成发布或长期记忆写入的授权。

v2 定向普通/race、programmatic vet 和五项 Python 汇总测试通过。证据存于外层 `programmatic-20260909/optimization-wave-31`。实际过滤失效记忆、后端错误与数量限制的下一实验按[独立合同](../superpowers/specs/2026-09-09-memory-validity-experiment.md)推进，目前不据此宣称生产支持 TTL。

## 发布门禁实现

`evaluation.GatePolicy.RequireAllCases` 和 HTTP `require_all_cases` 为可选字段，默认 false，保留既有 dataset 平均阈值行为。启用后检查完整、唯一且合法的 case ID、TotalCases/PassedCases 一致性、有限分值，并要求每个 CaseResult.Passed 为 true。该规则不改变自定义 evaluator 的 assertion 权重语义。

严格回归比较要求 baseline completed 且 case 集完整，与 candidate case ID 集相同。严格 release 还对照冻结 dataset 的实际 case ID 集；不能用交集比较或伪造计数掩盖缺项。独立 HTTP 测试证明平均阈值为 0.5 时旧行为兼容；严格模式有失败 case 拒绝发布并保持 live profile，全部通过才允许发布，缺失 baseline case 也被拒绝。

直接 Go API 的 NaN/Inf case、assertion、gate 和 comparison 分值被拒绝。标准 JSON 传输本已不接受这些数值，这不是远程 NaN JSON 漏洞的证明。成本 gate 尚未实现：生产 ExecutionEvidence 的 usage 小计和 step 计数不能证明全部 provider/summary 调用已计量，不据此自动发布策略。

## 验证边界

programmatic 包普通测试、race、vet 和五项 Python 汇总测试通过。evaluation/server 完整普通包测试由 Terra 子 agent 执行通过；root 另行执行有限分值、完整 case、严格 HTTP 发布与 baseline 缺失四项定向 race 测试通过。完整 server race 被子 agent 中止，不记为通过。本轮未运行全仓 gate、PG 或硬杀验收。

安全结构证据在外层审计目录 `programmatic-20260909/optimization-wave-30`。后续仍需修订合同后对照、独立留出任务、记忆失效/权威冲突，以及完整成本发布门禁；当前结果不满足全局最优或长期稳定的完成条件。
