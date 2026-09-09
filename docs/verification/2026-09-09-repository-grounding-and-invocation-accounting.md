# 仓库取证与逐次模型用量对账

第二十九波从固定库存开发样本转向当前仓库的源码取证任务。用户指定的本地第 6–9 行模型配置保持不变，使用 Responses 真实请求。本任务涉及此前已调查的问题，明确属于仓库集成开发实验，不计为未见过的留出集。

## 冻结与任务

HEAD 为 `ad90ac0`，包含工作区未提交实现。冻结测试二进制 SHA-256：`296f8fd83f0e718c72645368a17ad3acc4d0a98becec44d7e146c5970c78f9b5`。

在 Runtime 启动前将下列四个当前源码文件读入内存，共 25,041 bytes，冻结 bundle SHA-256 为 `1cc065c7bc78db3c29c387bb1b7cbdc0f6771ff948c143efe9699a7ec8b951a6`：

- `pkg/core/model_context.go`
- `pkg/core/agent_model_call.go`
- `pkg/adapter/modelexecution/corebridge/adapter.go`
- `pkg/adapter/modelruntime/compiler.go`

`repo.source.read` 仅接受固定枚举路径，声明 `PermRead`，经 Runtime accepted invocation 执行；运行中只读取冻结内存，不接受任意文件路径。它返回完整源码、路径和内容哈希，最大输出声明根据真实编码 JSON 计算。菜单同时包含 `program.catalog` 与 `program.execute`，不指定直接或 PTC 策略。此 profile 未额外添加 system 策略文本，与此前库存 profile 不属于同一提示配置。

任务要求读取四文件各一次，输出 fallback window/output、compiler default window/output、配置输出值是否优先，以及默认值定义和传递链的文件/符号证据。提示不包含预期数值或符号名称。oracle 从冻结源码 AST 提取数值并检查对应符号及配置覆盖分支；每个模型提交的引用也必须存在于对应源码。

固定上限为 MaxSteps=6、MaxToolCalls=8、输出 cap=4,096。adapter 可选 limits 被透传，独立 recorder 捕获 Runtime 送到 assembler 的实际 request，并在成功验收时要求匹配。128,000/4,096 是本地编译配置，不能认证网关后端模型身份或原生容量。

## 真实结果

| 请求模型 | 实际路径 | 验收 | adapter 调用 | 实际源码读取 | 输入/输出 token | provider 调用累计耗时 | 节流等待 |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| gpt-5.6-terra | 首轮提交四个直接读取，第二轮回答 | PASS | 2 | 4 | 16,056 / 647 | 15,504 ms | 7,233 ms |
| gpt-5.6-luna | 首轮提交四个直接读取，第二轮回答 | PASS | 2 | 4 | 16,056 / 844 | 19,227 ms | 7,015 ms |

两模型进程和 runner 均退出 0。每模型四文件各实际读取一次、所有工具结果 OK；没有调用 program catalog/execute。五项答案值、必需符号引用和额外引用存在性均通过，模型最后一轮文本、TurnResult.Answer 与 Session 最终 assistant 文本一致（忽略首尾空白）。两个实际 assembler request 最终记录均为 128,000/4,096，组装成功。

合计四次 adapter 调用，已报告输入 32,112、输出 1,491，共 **33,603 token**。每次调用均有独立 usage 对应；汇总器接受两记录，无未知/无效记录被跳过，两个样本均通过新的 invocation 对账。没有新增摘要调用。provider 耗时是 inner adapter 的实测调用区间，区别于 pacing 等待；并不是独立网络链路的服务端计时或费用账单。

这表明两模型在这个需要阅读源码解释证据的任务上，都能使用可批量的直接调用，两轮完成。它没有建立最低 token 的基线对照，不证明全局最优、长期稳定，或其他仓库任务也会选对路线。

## 对账修订与失败证据

此前普通 live 测试只比较整次运行的 usage 总和；两个调用的用量互换仍可能总和相等。新包装器将内存中的 `(RunID, Step)` 与 Session 的 StepStart 序号对应，回推 Core 的 model invocation ID，逐项匹配输入/输出用量。JSON 只保存计数和谓词，不额外公开逐次身份或正文。

- 实际进入 inner adapter 才计入 `actual_adapter_calls`；pacing 前取消单独记录。
- 首个有界 usage 保留为已知小计；重复、无效或 finish 后的报告使协议一致性失败。此观察器不声称重现 Core 的全部 parser 语义。
- 缺失 usage 不通过已有 `0/0` 账本事件补成零成本；失败前已报告 usage 继续保留。
- 普通记录发现同 run 的 summary usage 时，对账不得声明完整，避免漏算摘要成本。专用摘要实验保留独立总成本协议。
- Cleanup 独立生成对账谓词；任务正确性失败不会抹去已知用量，也不会由计量通过反推答案正确。

离线反例证明：两个 step 用量互换但总额相同会被拒绝；缺失/冲突报告不会计为完整；pacing 取消不进入 adapter；真实 Runtime 中 adapter 报 usage 后失败，Session 仍落下可逐次匹配的失败调用用量。

汇总器将旧记录的逐次证据标为 unavailable，不回填历史。重新读取第二十八波六记录时，旧小计仍为 130,002 / 3,625，但六个样本的 invocation verification 均为 unavailable。这不否定此前汇总对账，只明确它比新证据弱。

## 验证与下一边界

本轮 `go test ./examples/programmatic -count=1`、该包 race、vet，以及五项 Python 汇总测试均通过。Terra 子 agent 的独立只读复核未发现使本次有限验收结论失真的问题。凭据扫描快照覆盖 92 个变更/未跟踪源码文档文件、1,640,804 bytes，SHA-256 为 `42a76ebe8ca984b4e927de5a67fd0245bb0a4bd75c746fd6318b4a5a605c666a`，扫描无泄漏；这是新增本报告之前的明确快照。

原始安全结构证据与日志保存在外层审计目录 `programmatic-20260909/optimization-wave-29`。本轮未执行 PG、硬杀恢复或全仓门禁。预算动态提示、未见留出任务、污染/过期记忆对照及成本发布门禁仍未完成。尤其不能把 `MaxToolCalls - EvToolCall 数量` 泛化成精确工具余额：嵌套、拒绝及恢复路径的计数语义必须单独证明。完整目标继续按[验证矩阵](../superpowers/specs/2026-09-09-real-execution-optimization.md)推进。
