# 大工具结果、选路与上下文容量实测

本记录是开发实验，使用用户指定配置中的两个请求模型 `gpt-5.6-terra` / `gpt-5.6-luna`，通过 Responses 协议运行真实 Runtime。模型名称是请求配置，不是网关后端身份认证。只使用合成只读工具，不发送本地业务数据。CodePTC 未启用。

## 对照设计

同一八项 inventory 任务、四工具菜单、typed 输出合同、MaxSteps=10、MaxToolCalls=11、模型输出上限 4,096。详情额外包含精确定长的 2 KiB 或 4 KiB 无关 ASCII 记录，包含变化的序号与校验值；JSON 编码后的总输出上限由实际内容计算。auto 可自行直接调用、PTC 或混合；另外两臂只增加强制批量直接或强制 PTC 的策略指令。

验收要求正确 FINAL、inventory 一次、八个不同详情各一次、工具结果成功、模型报告用量与 durable ledger 总额相符。forced PTC 还要求叶子工具确由 execute 的受保护子调用执行；正确答案但违反强制策略仍是失败。用量总额匹配不等于独立验证了每个远端请求的账单，失败成本也保留。

默认上下文输入预算为 32,768 - 4,096 - 1,024 = 27,648 token，当前保守估算器按 UTF-8 字节数作为 token 上界。最新用户之后的 assistant 工具调用及对应结果属于必需原子组。八个 4 KiB 结果会使该组超预算，在下一次模型请求前失败；不能将其视为完成任务的低成本路径。2 KiB 离线直接调用对照确认 inventory 与八个详情共九个结果都保留，零丢弃。4 KiB 离线确认 ErrBudgetExceeded，错误时 slots/dropped-groups 的 observed 标志为 false，零值不代表成功观测。

## 第二十波：初版提示基线

冻结二进制 SHA-256：`3087a879d80ec8d257ecfae997adb3fea5b9a3ebdcc624742acd7c822fc057c4`。

变体 `large_detail_v1` 的公共提示使用“只用 fixture 工具”，存在将 program 包装工具误排除的歧义。此波保留为开发基线，不用它证明自动选路的最佳性。强制策略在本地 adapter 输入中确有不同 system 字节数；不能仅从模型违背策略或相同 reported input token 推断网关忽略指令。

本波只执行 2 KiB，共六个完整记录：

| 请求模型 | 策略标签 | 验收 | 模型调用 | 已报告输入/输出 token |
| --- | --- | --- | ---: | ---: |
| Terra | auto | 通过，批量直接 | 3 | 18,574 / 614 |
| Terra | batched direct | 通过 | 3 | 18,572 / 590 |
| Terra | forced PTC | 失败，实际直接调用 | 3 | 18,574 / 589 |
| Luna | auto | 通过，批量直接 | 3 | 18,650 / 574 |
| Luna | batched direct | 通过 | 3 | 18,601 / 555 |
| Luna | forced PTC | 失败，实际多轮直接调用 | 10 | 69,840 / 640 |

六例均完成八项只读效果及最终答案，最终模型上下文保留九个顶层工具结果，零丢弃；两个 forced PTC 都没有执行 program.execute，因此模型测试进程均退出 1。Luna 的十轮成本保留，不能把正确答案等同于路由验收通过。此波没有实际 HTTP instructions 观测，不回填后续字段。

后续变体 `large_detail_v2_prompt_scope` 明确允许当前暴露的 program.catalog/program.execute，不提供最佳路由、程序或答案。所有其他数据和预算保持相同。新增出站请求结构观测只记录 instructions 是否存在及字节数，不记录正文，用于核对本地 system 是否进入实际 HTTP 请求；它仍不能证明远端模型遵循指令。

## 第二十一波：明确工具许可与实际出站指令

冻结二进制 SHA-256：`ad041a7c4a145fa3a89d2aef87fe7e76d8314ec41bc70d9066798e7f9890253b`，变体 `large_detail_v2_prompt_scope`，仅执行 2 KiB。

| 请求模型 | 策略标签 | 验收 | 模型调用 | 已报告输入/输出 token |
| --- | --- | --- | ---: | ---: |
| Terra | auto | 失败，程序修正后预算耗尽，详情 7/8 | 4 | 21,074 / 1,247 |
| Terra | batched direct | 失败，先尝试无效 PTC 后直接完成 | 5 | 32,282 / 1,278 |
| Terra | forced PTC | 失败，实际混合路径 | 3 | 15,465 / 668 |
| Luna | auto | 通过，直接列表＋PTC 详情 | 3 | 15,580 / 917 |
| Luna | batched direct | 失败，实际混合路径 | 3 | 15,454 / 817 |
| Luna | forced PTC | 失败，运行错误后预算耗尽，详情 7/8 | 4 | 21,722 / 2,042 |

六例共 22 次 adapter 调用与出站请求结构记录逐一匹配，每次 instructions 存在且字节数等于本地 system。没有依据把选路失败归因于本地漏发 system，也不能据此确认网关实际执行了它。两个测试进程均退出 1。失败的两个 limited run 均已实际调用 inventory 一次和详情七次；程序失败不等于之前没有效果。

Terra auto 的第一次编译失败观测到对象 literal，第二次编译与 bindings 均通过但耗尽工具预算。Luna forced PTC 第一次编译和 binding 正确，运行出现 `runtime_get_missing_key`，后续尝试仍未完成。Luna auto 的成功混合路径合法，验证不强迫 auto 全部走 PTC；同样路径在 forced PTC 臂违反其控制条件，不能标为强制策略通过。

审核还确认了共同指令冲突：forced-direct 禁令之后，公共 fragment 仍含无条件 “First obtain ... program.catalog”。profile 按 fragment ID 排序，这条共同指令位于强制策略之后。下一变体 `large_detail_v3_conditional_catalog` 将其改为仅选择 PTC 时才取 catalog；普通旧 live 基线保留原文以免静默改版。生产 general profile 的同一句同步条件化，未改变工具权限或执行接口。v2 不能作为可靠的最佳直接成本基线。

## 第二十二波：条件化后的直接基线

冻结二进制 SHA-256：`0067cd3672d2d4b61c8c99aa21eaa8d41747c667f7e70ab7a86f2b1528370cc4`，变体 `large_detail_v3_conditional_catalog`。先只运行 2 KiB 强制批量直接，以检验控制指令是否可用，不在失败基线上继续扩大实验。

| 请求模型 | 验收 | 模型调用 | 已报告输入/输出 token |
| --- | --- | ---: | ---: |
| Terra | 通过 | 3 | 18,647 / 600 |
| Luna | 通过 | 3 | 18,672 / 647 |

两例均 inventory 一次、八个详情同一模型响应提交、无 program 调用、答案正确，最终上下文九个顶层结果完整且零丢弃。六次出站 instructions 与本地 system 字节数逐一相等；两个测试进程退出 0。这是单样本修复验证，不是统计因果或稳定成功率证明。

## 第二十三波：同一冻结二进制的其余三臂对照

与第二十二波使用完全相同的二进制和 v3，不再修改提示、数据、预算或估算器。补齐 2 KiB 的 auto/PTC 和 4 KiB 的全部三臂，每模型每臂只执行一次。

| 请求模型 | 大小与策略 | 验收 | 模型调用 | 已报告输入/输出 token |
| --- | --- | --- | ---: | ---: |
| Terra | 2 KiB auto | 通过，批量直接 | 3 | 18,626 / 612 |
| Terra | 2 KiB forced PTC | 失败，catalog 后仍直接调用 | 3 | 20,563 / 742 |
| Luna | 2 KiB auto | 通过，完整 PTC | 3 | 15,116 / 611 |
| Luna | 2 KiB forced PTC | 通过 | 3 | 15,050 / 626 |
| Terra | 4 KiB auto | 通过，完整 PTC | 3 | 15,163 / 615 |
| Terra | 4 KiB batched direct | 失败，实际尝试 PTC 后预算耗尽 | 4 | 21,126 / 1,162 |
| Terra | 4 KiB forced PTC | 失败，编译错误后直接调用，组装超预算 | 4 | 21,136 / 1,189 |
| Luna | 4 KiB auto | 失败，直接调用后组装超预算 | 2 | 8,675 / 575 |
| Luna | 4 KiB batched direct | 失败，组装超预算 | 2 | 8,662 / 550 |
| Luna | 4 KiB forced PTC | 失败，实际直接调用后组装超预算 | 2 | 8,642 / 514 |

本波十例共 29 次 adapter 调用，出站结构记录数量逐项相符，instructions 均存在且字节数匹配。两个测试进程均退出 1；没有因预期压力失败就把任务状态涂成通过。

Luna 2 KiB auto 的成功 PTC 总量为 15,727 token，相对同二进制直接对照的 19,319 少约 18.6%；强制 PTC 的 15,676 少约 18.9%。三条成功路径都是三轮，不能据此声称轮数降低。Terra 2 KiB auto 选择直接，19,238 token 与强制直接的 19,247 接近。不同模型、不同大小及失败样本不能混成最低成本排名。

四个 `final_context_budget_exceeded=true` 样本（Terra 4 KiB forced PTC 与 Luna 三个 4 KiB 臂）都已实际执行 inventory 一次和八个详情，但未发送最终回答请求。失败时 slots/dropped-groups 的 observed 标志为 false，不能解释成零丢弃或成功组装。Terra 4 KiB batched-direct 标签样本实际走了 PTC，只有七个详情效果且触及工具预算，不能称其验证了八项直接结果的容量边界。

PTC 成功样本最终上下文仅保留 catalog 与 execute 两个顶层结果，八个受保护子结果未重复灌入模型。强制策略仍会被模型违反，程序仍有编译/运行错误；条件化指令修复消除了明确冲突，但没有提供确定性的模式保证。

## 本轮本地验证

第二十至二十三波累计 24 个真实开发样本、82 次 adapter 调用，已报告输入 474,466、输出 18,974，共 493,440 token，包含所有失败样本；82 条 round 均有 usage 报告。该总量是本轮验证成本，不是单任务成本，也不等于远端账单认证。按二进制、模型、case 分组的汇总分别保留，未将不同版本合并成成功率。

examples/programmatic 与 cmd/server 的普通测试、race、vet，以及 modulecheck、contextassembly/programmatic 定向测试和 Python 汇总器测试通过。真实测试的失败状态与上述表格一致。验证范围不包含本轮 PostgreSQL、硬杀或全仓新一轮门禁。

变更源码与文档快照经 gitleaks 扫描未发现凭据；配置文件与私有原始审计目录不在公开仓库中。

## 证据边界

初始直接工具 schema 只有 name、description、parameters，不含 Manifest.MaxOutputBytes；2 KiB/4 KiB 的非零 padding 使用相同 description。上界只在 catalog 返回后可见。第二十三波 Terra 两个 auto 首请求均为 3,765 bytes，且已观测正文 SHA-256 相同。因此首轮不同选路不能归因为模型已知道结果大小；4 KiB 失败也不能全归为模型忽略了已知容量。

后续自动选路对照应先以非路由性描述暴露真实输出上界，从同一 manifest 派生，避免要求模型凭不可见信息决策。第二十四波已实现私有 fixture 的该对照，见[选路信息与 adapter 额度](2026-09-09-routing-information-and-adapter-limits.md)。生产通用元数据展示与执行前容量保护仍未实现，不能把 catalog 已有字段误写为直接工具初始 schema 已具备的能力。

后续审核确认，旧 liveModel 包装器没有透传 inner adapter 的 ModelContextLimits，因此本记录的 32,768 是 Core 回退窗口，不是生产 Responses 编译 plan 的默认 128,000。这里的压力结果保留原作用域；独立 adapter 额度对照另见上述后续记录。

这是已参与设计和调优的八项任务，非留出集；每模型每臂单样本不能证明稳定成功率或全局最低 token。4 KiB 是本地上下文预算压力，不是厂商原生上下文窗口测试。当前批次组装失败不自动重做已完成工具，但本实验没有证明硬杀、新 run 或远端未知 outcome 的去重。

历史 PTC 失败、近容量摘要修复及 SQLite 干净恢复结果仍保留在[PTC 与恢复](2026-09-09-ptc-stability-and-session-recovery.md)和[滚动摘要](2026-09-09-summary-rollover-and-evidence.md)中。候选策略尚不能直接进入自动发布或长期记忆；仍需留出任务、污染/过期记忆、持久恢复及资源门禁验收。
