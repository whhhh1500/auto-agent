# 真实模型执行优化：批量协议与输出合同

状态：进行中，未通过整体优化验收。本文延续 [首轮真实模型记录](2026-09-09-programmatic-live-models.md)，保留其中的成功和失败；首轮的逐项直接调用不是最优直接调用基线。完整要求见 [验证矩阵](../superpowers/specs/2026-09-09-real-execution-optimization.md)。

后续波次见 [记忆与上下文优化](2026-09-09-memory-context-optimization.md)：补充查询说明后两模型记忆对照通过；默认本地摘要的早期事实丢失已复现并修复；工具菜单对照已取得真实 wire 证据。本文保留此前波次状态，不回填历史失败。

## 条件与可复现性

2026-09-09 使用用户指定私有配置范围中的两个模型名：`gpt-5.6-terra`、`gpt-5.6-luna`，通过兼容 Responses 的真实端点运行实际 Runtime、受保护工具入口和上下文装配器。模型名是请求配置，不能证明代理服务背后的模型身份。工具为只读合成 inventory/detail；期望答案与程序源码不提供给模型。这仍是开发样本，不是生产业务或冻结留出集。

每波保留独立日志和不可覆盖的逐 case JSON，包含失败、usage、请求数、请求耗时、节流等待、程序摘要和固定诊断。私有审计目录为 `.codex-v46-audit/programmatic-20260909/optimization-wave-1` 与 `optimization-wave-2`，位于仓库之外，不含公开提交所需的凭据。第二波开始时的 Go 源码快照摘要为 `33c61bf961fa6d660e928a6b19ba6029cb5b40067651d3ce495b92f26e1a415f`；它是启动时路径与文件内容摘要，不是可重建二进制的完整证明。请求最小起始间隔为 15 秒；下表 provider 时间剔除了该等待，不等同生产端到端延迟。

## 协议缺口

第一波，两模型的批量直接调用都在 3 轮预算耗尽时失败，两模型指定 PTC 也失败。随后对 Terra 做最小真实协议对照：相同提示、工具与 `tool_choice=required`，显式 `parallel_tool_calls=true` 时服务返回该值为 true、一次产生 8 个 function call；省略时返回 false、只产生 1 个。该探针只证明回复结构，不证明任务或工具效果。

Responses adapter 已在存在工具时显式发送 `parallel_tool_calls=true`，对应实现 revision 为 `openai-responses-v3-batched-tool-calls`。Core 仍逐项执行并检查权限、预算、审批和 journal。允许一个回复包含多个调用不意味着本地并行执行或免除保护。

## 第二波真实结果

批量直接场景要求 inventory 后在同一个回复提交全部 8 个 detail 调用，最多 3 轮。PTC 场景最多 6 轮；有输出合同变体与原 PTC 使用相同提示、数据和预算，只增加工具 `OutputSchema`。成功要求真实 fixture 调用、每项恰好一次、最终答案与 Session 一致，不能只看 Runtime 的 completed 状态。

| 模型 | 条件 | 验收 | 请求轮数 | 输入 token | 输出 token | provider 请求合计 |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| Terra | 批量直接 | 通过 | 3 | 13,426 | 553 | 14.682 秒 |
| Terra | PTC，无输出合同 | 失败 | 3 | 14,498 | 858 | 22.180 秒 |
| Terra | PTC，有输出合同 | 通过 | 3 | 14,584 | 585 | 15.031 秒 |
| Luna | 批量直接 | 失败 | 3 | 12,796 | 273 | 10.943 秒 |
| Luna | PTC，无输出合同 | 失败 | 3 | 14,510 | 1,130 | 25.761 秒 |
| Luna | PTC，有输出合同 | 失败 | 4 | 20,454 | 1,716 | 36.721 秒 |

两个模型的整组 Go 测试退出码均为 1。六项中仅两项通过，不能报告整组绿灯。token 为该 case 所有已报告轮次之和，包括失败和模型修正请求，不是计费金额。

Terra 批量直接实际执行 inventory 一次、detail 八次，第二轮回复包含八个调用。无合同 PTC 编译与绑定均正确，但报 `runtime_get_not_object`，inventory 执行一次、detail 未执行；有合同变体本次完成。后者比成功的批量直接仍多消耗 token，不能由这组任务主张 PTC 更省。合同变体只有一次样本，不证明稳定因果收益。

Luna 批量直接仍逐项产生调用，未在 3 轮内完成；显式协议允许不保证模型遵循。无合同 PTC 因 `program_bindings_mismatch` 在子工具执行前拒绝。有合同变体先绑定错误，再修正绑定，但程序仍报 `runtime_for_not_list`，detail 未执行。不能把“模型生成了可编译代码”当作程序正确。

## 第三波：自主选路复验

同一固定测试二进制（SHA-256 `9fbf2912784536bc1010f258a6f0b13deb0f24510e5f328685eae5c6b0a22cda`）对两模型运行原 `batch_n8_auto` 提示，不给出直接/PTC 模式标签。证据保留在独立 `optimization-wave-3`。

| 请求模型 | 结果及自主动作 | 模型轮数 | 输入 token | 输出 token | provider 请求合计 | 节流等待 |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Terra | 通过；inventory 后一个回复提交 8 个 detail | 3 | 13,374 | 590 | 16.495 秒 | 16.014 秒 |
| Luna | 通过；inventory 后逐项 detail | 10 | 46,928 | 631 | 29.375 秒 | 108.409 秒 |

两者均实际读取八项且每项一次。Terra 此次无需额外路由模型就自行选择批量直接调用；Luna 正确完成任务，但回合与输入成本明显较高。不能因为后者任务正确就宣称效率达标，也不能把人为节流等待归咎于模型推理速度。

另对 Luna 请求做最小协议探针：显式 true、单个合法名称工具、要求同回复八个调用，服务返回 true 且产生八个调用（输入 3,624、输出 132 token）。因此，“Luna 请求绝不支持批量”也不符合证据；失败出现在更完整的 Runtime 提示/工具组合条件下，仍需隔离哪些条件影响选择。探针没有执行工具，不替代业务验收。

## 第四、五波：真实记忆与上下文对照

固定测试二进制 SHA-256 为 `03e7ab62eca52ac1fa61bfd0be495b6dfb0085a5886587dc378008369a0b48ce`。正式 `memory.recall` capability、进程内 `SliceStore`、受保护 Runtime 与标准 context assembler 组成两臂：相关事实含未进入提示的随机值；无关臂仅存另一项目事实。任务明确要求一次 recall，故这验证检索遵循和上下文注入，不能说明自主记忆选路。

| 波次 | 请求模型 | 场景 | 验收 | 轮数 | 输入/输出 token |
| --- | --- | --- | --- | ---: | ---: |
| 4 | Terra | 相关记忆 | 通过 | 2 | 7,671 / 100 |
| 4 | Terra | 无匹配记忆 | 通过 | 2 | 7,607 / 108 |
| 4 | Luna | 相关记忆 | 网关 HTTP 502，后续臂停止 | 1 | 未报告，未知 |
| 5 | Luna | 相关记忆，独立补验 | 两轮 HTTP 成功但最终答案不匹配，后续臂停止 | 2 | 7,618 / 101 |

Terra 通过项核验随机值不在首轮 system/messages；Session 中唯一 recall 的 CallID 与 Content 逐字出现在最终模型上下文；返回答案与持久 assistant 消息一致；每轮 stream usage 与 Session usage 总账相等。无关臂验证指定查询为空，不能推广为所有无关/错误记忆都能被正确过滤。

Luna 的 502 不作为模型能力失败，且该请求实际计费未知。独立补验的任务失败则必须保留，不能由后续重试覆盖。该版 JSON 未保存 recall 条数和答案谓词，日志首先在最终答案处失败，现有证据不足以区分检索未命中与模型误用结果；不能补写一个未经观察的根因。

补验之后已为未来记录增加可选 `memory` 计数/布尔诊断：召回是否存在、条数是否解析、上下文是否精确配对、是否观察首轮、随机值检查是否适用，以及答案/持久答案/usage 是否匹配。离线测试确认 Runtime 完成但验收失败时仍会保存这些谓词，且不含记忆原文。新增字段未经下一波真实调用验证，旧记录没有回填。

没有测试 SQL 记忆持久化、跨用户污染、错误/过期事实、长历史摘要或记忆的成本收益。随机值只通过这次真实存储和模型路径验证，不把生成答案自动写回长期记忆。

## 第三十八至四十波：exact lookup 的调度与参数合同

后续 memory-authority 记录补充了显式 opt-in `memory.lookup`，详见[权威对照](2026-09-09-memory-authority-live.md#第三十八至四十波可选-exact-lookup)。Wave38 的 V4 `then` 提示在两模型 × 三 case 都得到 exact/found/authority/conflict/journal/usage=true，却因两个独立只读工具串行为两轮、2 轮上限没有 final 而 0/6；报告 48,609 tokens。Wave39 唯一预定的 fixture/prompt treatment 是“同一 response、独立只读”的并行提示；live 每格会重建随机 opaque values，token 差异只是非配对单样本观察，不能作因果估计。该波为 Terra 3/3、Luna 2/3、49,721 tokens；Luna mixed-case 仍保留 exact/found=false 的失败。Wave40 仅针对 mixed-case，在 V5 后追加 exact JSON 参数字面量，Terra 1/1、Luna 1/1，合计 2/2 强门通过、16,573 tokens。

这说明路由器/上下文应把独立只读工具标为可并行，并将 canonical key 作为结构化参数原样传递；不证明 lookup 能替代 recall、具有全局最低 token 或可在线自动演化。Wave36 recall 的 4/6、49,908 tokens 与 Wave39 lookup 的 5/6、49,721 tokens 不具同一菜单、提示和 case3 大小写条件，不能作因果比较。请求模型名不认证网关后端，usage 为 provider 报告，每个 model/case 仅单次；外层 `optimization-wave-38/39/40/summary.json` 不进入仓库且未复制 nonce、凭据、key 或正文。

## 已补齐的证据接口与边界

- PTC 固定诊断区分变量、对象读取、索引、循环等失败，不输出原始数据；host guard 错误保持原样传播。错误不保证先前子工具无副作用，不能盲目重放。
- `evaluation.CaseResult` 可选 `ExecutionEvidence` 从同一 Run 的 Session 事件投影 usage、步骤和工具事件，包含摘要 usage；通过既有 JSON 存储兼容旧记录。步骤不等于网络请求，工具结果事件不等于真实外部效果，观察到 usage 不证明计量完整。
- 真实 harness 将验收成功与 Runtime 完成分开，保留失败、程序诊断、调用数量，以及 provider 时间与节流等待。数据可用于候选评估，不自动写入长期记忆或触发策略发布。

定向 plain/race、vet、evaluation JSON/SQLite 往返、Responses/bridge 多调用、Core 审批恢复与预算测试通过。它们验证相应实现边界，不代替上表失败的模型验收。

## 未完成项

协议修正后已完成上述一个开发任务的自主选路复验，不能代表其他任务。真实记忆仅 Terra 两臂通过，Luna 未通过。长上下文/摘要、跨会话、不同任务形状与冻结留出集均不能由此推断通过。还未证明最低 token、跨模型稳定性或自动自进化收益。CodePTC 继续仅保留接口与路由。
