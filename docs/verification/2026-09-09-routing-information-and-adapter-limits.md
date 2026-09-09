# 选路信息、语法说明与 adapter 额度对照

本记录承接[大工具结果实测](2026-09-09-large-tool-results.md)，仍使用用户指定的 Terra/Luna 请求配置、Responses 协议及合成只读工具。请求模型名称与编译额度不是网关后端身份或原生模型窗口的认证。不同版本和失败样本分别保留。

## 第二十四波：首次模型调用可见输出上界

私有 v4 fixture 从实际 `Manifest.MaxOutputBytes` 派生 detail description 的 `Maximum output N bytes.`，不提供路由建议。旧 v3 description 保留。任务、八项数据、工具菜单、typed contract、MaxSteps=10、MaxToolCalls=11、模型输出上限 4,096 和 32,768 回退窗口均不变。离线实际 Runtime 首次 GenerateOptions.Tools 证实大小描述可见且与真实 JSON 上界相等。

冻结二进制 SHA-256：`c3ec5cc93ff0ddf93253b03560fdeefb0735ad52a591863ea46c26642c971193`。本波只运行两模型、两大小的 auto，每项一次，未重复整套强制策略。

| 请求模型 | 大小 | 实际路径与验收 | 调用 | 已报告输入/输出 token |
| --- | --- | --- | ---: | ---: |
| Terra | 2 KiB | 完整 PTC，通过 | 3 | 15,068 / 559 |
| Terra | 4 KiB | 完整 PTC，通过 | 3 | 15,053 / 564 |
| Luna | 2 KiB | 批量直接，通过 | 3 | 18,673 / 549 |
| Luna | 4 KiB | 批量直接，组装超预算失败 | 2 | 8,716 / 583 |

四例实际 inventory 各一次、详情各八次；成功 PTC 最终只有两个顶层结果。Luna 4 KiB 已执行工具，最终回答请求未发送，不能称其更高效。11 个 adapter 调用都有匹配的出站结构记录，instructions 存在且字节数与本地 system 一致。测试退出码 Terra=0、Luna=1；新私有 runner 也会返回整体失败状态，不再吞掉子测试退出码。

结果大小变为可见，仍不足以保证稳定选路。与 v3 对比，每模型每项只有单个开发样本，不能由路线变化作统计因果结论，也不能将提示变化解释成确定性的执行策略。

进一步只读审核确认，v4/v5 的 model system/user 没有本次输入窗口、输出预留、剩余步骤或工具预算；额度报告只供 Assembler 使用。模型已知单次结果上界，仍未知该 run 的实际可用预算。因此这些失败不能写成“模型忽视了已知预算”。后续完整决策上下文须分别提供可信预算信息与 Runtime 最终保护，不能只增加输出大小描述就宣称信息已经齐备。

## 独立语法说明修订

后续二进制使用 `programmatic-extension/v6-container-expression-guidance`。LanguageGuide 明确表达式对象需要 op，literal.value 只接受 scalar，数组/对象分别使用 list/items、map/entries，成员仍为表达式。指令集、Compile 行为与诊断类别未改。已有数组 literal 拒绝测试补齐对象情形，另以实际 caller 验证 map/get 构造工具参数，catalog 测试验证新说明原样可见。

该变更只针对常见容器表达方式误用，不会保证变量存在、数据形状正确或重试无重复效果。其真实对照与第二十四波的旧 guide 分开，不能把同时发生的额度变化归给语法说明。

## 第二十五波：新 guide 与保留的小窗口基线

冻结二进制 SHA-256：`7a618574fc60d44f5893ee28dc796ba430022745232a9db72789323b9f273587`，使用 v6 guide；仍是 v4 大小可见 fixture 与旧额度包装。新增结构字段直接确认六例传给 assembler 的额度均为 32,768/4,096。除两大小 auto 外，补 4 KiB 强制直接，为额度对照保留同策略基线。

| 请求模型 | 场景 | 验收 | 调用 | 已报告输入/输出 token |
| --- | --- | --- | ---: | ---: |
| Terra | 2 KiB auto | PTC 通过 | 3 | 15,139 / 532 |
| Terra | 4 KiB auto | 直接调用后组装超预算 | 2 | 8,639 / 531 |
| Terra | 4 KiB batched direct | 实际 PTC 运行错误后预算耗尽 | 4 | 21,397 / 1,155 |
| Luna | 2 KiB auto | 批量直接通过 | 3 | 18,729 / 581 |
| Luna | 4 KiB auto | 混合路径运行错误后预算耗尽 | 4 | 22,395 / 1,953 |
| Luna | 4 KiB batched direct | 实际混合路径，策略失败 | 3 | 15,728 / 754 |

两测试进程均退出 1，19 次 adapter 调用都有完整 round usage 和匹配的出站结构记录。已生成的 PTC 均编译通过，但 Terra 出现 runtime_get_not_object、Luna 出现 runtime_variable_missing，因此不能声称 guide 解决了 PTC 可靠性。Terra 的失败样本实际重复执行列表两次、仅完成六个详情；Luna 的失败 auto 实际列表一次、详情七次。该只读 fixture 允许检测重复，但没有因此提供跨新程序尝试的语义去重保证。

## 新发现：压力窗口与实际 adapter 配置必须区分

旧 liveModel 计数包装器只实现 LlmAdapter，没有转发可选的 ModelContextLimits 方法，Core 因而使用 32,768 / 4,096 回退值。上述以及历史 4 KiB 失败是有效的本地小窗口压力证据，不能直接代表生产 Responses 编译配置。

corebridge.Adapter 会返回 frozen ProviderPlan 的额度；当前 modelruntime compiler 的默认 context 是 128,000，输出额度由配置 MaxTokens 决定，本次配置为 4,096。这些是本地编译值，未经 provider 原生窗口认证。独立对照需要明确透传 adapter 额度，记录组装请求中实际 window/output 与来源；旧小窗口测试必须继续保留，不可静默改成大窗口后声称修复了历史失败。

## 第二十六波：同一二进制透传 adapter 额度

使用与第二十五波相同的冻结二进制、v6 guide、输出大小披露和任务预算；私有 v5 wrapper 只转发 inner adapter 的可选 ModelContextLimits。四例实际 assembler request 均记录 128,000/4,096，未修改 tokenizer 或安全余量。

| 请求模型 | 4 KiB 策略 | 验收 | 调用 | 已报告输入/输出 token |
| --- | --- | --- | ---: | ---: |
| Terra | auto | 完整 PTC，通过 | 3 | 15,163 / 614 |
| Terra | batched direct | 实际 PTC，策略失败 | 3 | 15,168 / 593 |
| Luna | auto | 直接列表＋PTC 详情，通过 | 3 | 15,573 / 720 |
| Luna | batched direct | 批量直接，通过 | 3 | 23,426 / 596 |

四例均实际列表一次、详情八次；两种 PTC 成功路径只向模型保留两个或三个顶层结果，Luna 真正直接路径保留九个结果，均零丢弃。12 次 adapter 调用与出站结构记录相符；测试退出码 Terra=1、Luna=0。

Luna 的 4 KiB 真实直接路径在编译额度下能完成，修正了将 32K 压力结果推广到该配置的风险。自动混合路径总 token 为 16,293，对照直接的 24,022 少约 32.2%，同为三轮；但两者均携带四工具菜单。尚需直接模式仅展示两工具的更严格成本基线，不能据此宣称最优模式已经确定。

## 第二十七波：独立两工具直接模式

冻结二进制 SHA-256：`a3762463c32a2c930a1f0298a065d276a4672baa3e387ff859f328b59099d753`，变体 `large_detail_v6_direct_only_adapter_limits`。保留相同 4 KiB 数据、输出上界、128,000/4,096 adapter 额度、10/11 执行预算；仅注册 inventory/detail，不加入 program 选择说明。批量直接指令也不再提及不存在的 program 工具。该独立配置同时改变菜单与相关说明，不是四工具菜单内的单变量因果实验，也没有计入自动系统预先选择该执行模式的额外成本。

| 请求模型 | 验收 | 调用 | 已报告输入/输出 token | 合计 |
| --- | --- | ---: | ---: | ---: |
| Terra | 批量直接通过 | 3 | 22,117 / 541 | 22,658 |
| Luna | 批量直接通过 | 3 | 22,123 / 538 | 22,661 |

首个 GenerateOptions 工具列表与全部六次实际 HTTP 请求均核验为两工具。两例列表一次、详情八次、答案正确，最终九个结果完整且零丢弃，测试进程均退出 0。

第二十六波 Terra 自动完整 PTC 的 15,777 token 比此直接模式样本少约 30.4%；Luna 自动混合路径的 16,293 比此直接模式样本少约 28.1%。轮数仍都是三轮。这个更严格基线支持当前大结果成功样本的局部收益，但不同配置每项只有一个开发样本，不能证明稳定收益或整个任务分布上的最优选择。

## 本轮验证与下一证据需求

第二十四至二十七波共 16 个真实样本、48 次 adapter 调用，已报告输入 273,107、输出 11,363，合计 284,470 token，包含所有失败；48 条 round 均报告 usage。独立汇总保留版本、case 与失败，不以运行 completed 代替策略验收。

examples、VM 与 toolcapability 的离线/race、vet、modulecheck 及 Python 汇总器验证通过。本轮没有重新执行 PostgreSQL、硬杀或全仓门禁。生产语法说明是最小澄清，输出上界展示与额度透传对照仍属测试配置；尚未自动部署策略、写入长期记忆或放宽安全预算。

下一阶段须冻结当前协议，在未参与调优的任务上验证包含可信预算信息的决策、不同菜单配置的端到端成本，以及 PTC 新尝试对已完成工具前缀的处理。先补齐 per-invocation 计量和实际工具效果，再考虑候选策略的发布门禁，不能把失败程序自述或单样本收益写成长期经验。

## 容量保护的架构边界

OnBeforeTool 在 approval/provider 前，但它是逐 call hook。首次 hook 发生前 assistant 批次已持久化且当前 call 已计预算；只拒绝第一个 sibling 不能阻止后续 effect，对全部 sibling 拒绝又会产生多个结果并消耗预算。它不是原子批次 admission，也不会自动切换为 PTC。

MaxOutputBytes 是上界。最坏结果组超预算意味着风险，不能证明真实结果必然装不下；反之，完整必需组连同 system、schemas、IDs 和 framing 的可信最坏上界仍能容纳，才可给出充分的 safe-fit 证明。宽默认上界、缺失 manifest 和 partial/unknown effects 需要独立分类。

下一最小生产候选宜先做只读风险观测，从同一 live session 的 durable assistant 批次和 frozen composition manifests 推导，记录额度来源、上下界证据、实际输出与组装结局。真正的完整批次预检接缝需独立架构审议，不能用逐 call hook 冒充。现有 Config.Estimator 可接入版本化 tokenizer，但有限 provider usage 样本只能校验，不能据此自动缩小安全余量。
