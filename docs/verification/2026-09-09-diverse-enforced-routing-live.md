# 多任务强制路由真实模型验收（第 43 波）

状态：数据平面路由和计量证据完整，但当前 `auto_first_action` 仍表现出明显 direct 偏置，不能晋级为默认自动路由。两个请求模型分别只启动一次冻结测试二进制，每个 arm 只尝试一次；15 秒请求间隔下没有出现 HTTP 或协议错误。

## 测试范围与证据

`TestLiveDiverseEnforcedRouteMatrix` 通过共享 `internal/executionroute.Resolve` 与默认 `runexecutor` 运行两个任务的 `auto_first_action`、`direct_only`、`ptc_only` 三臂：

- `n1_simple`：inventory 后只读取一个 active detail。direct 是预期的低开销路线。
- `filter_loop_large_detail`：inventory 后读取八个 active detail，每项带 2 KiB 无关字段；PTC 在受保护解释器中完成同样的 inventory、过滤、循环和八次 detail，只把计数返回模型。该任务用于检验 PTC 是否真正减少上下文，而不是只改变调用名称。

任务 prompt 不出现 `program.catalog`、`program.execute`、PTC、direct 或 route mode。每个 arm 的模型上限为 10 轮，工具调用上限为 11；相同任务在两个请求模型和三个 arm 中使用相同 fixture 字节。协议为 Responses，`stream=true`、`store=false`，相邻 adapter 调用开始时间至少间隔 15 秒。

测试配置仍只从本机 `D:\cc\llm.txt` 第 6–9 行读取：第 6 行端点、第 7 行凭证、第 8/9 行请求模型。端点、凭证、prompt、模型正文、工具参数/结果和生成程序均未进入证据或仓库。

私有证据位于 `.codex-v46-audit/programmatic-20260909/optimization-wave-43/`：

- 冻结二进制 `programmatic-diverse-enforced-route.test.exe`，SHA-256 `e45834f23bd87ec2ca7367d937f84768abe579ea543dafcef0115fe1c59ef5bf`。
- 安全汇总 `safe-summary.json`，SHA-256 `aed5982e3f66866d26a42026a6b80dd9e2210164f0cb05148d54a10fcd3407c0`。
- 12 份原始安全 JSON 都有完整 provider-reported usage、三轮 adapter/session ledger 对账、实际工具菜单、wire 结构、上下文聚合、执行边界和 no-retry 标记。

请求模型名不能独立认证网关实际使用的后端模型；usage 也不是独立账单证明。本波每个任务/模型只有一个样本，数值不能解释为长期均值。

## 结果

| 请求模型 | 任务 | arm | 路径 | 验收 | 输入 / 输出 / 总 token | 顶层 / 子调用 | 最终上下文 token |
| --- | --- | --- | --- | --- | ---: | ---: | ---: |
| `gpt-5.6-terra` | n1 | auto | direct | PASS | 11,647 / 179 / 11,826 | 2 / 0 | 2,970 |
| `gpt-5.6-terra` | n1 | direct | direct | PASS | 11,571 / 156 / 11,727 | 2 / 0 | 2,957 |
| `gpt-5.6-terra` | n1 | PTC | catalog + execute | FAIL | 13,557 / 592 / 14,149 | 2 / 1 | 7,000 |
| `gpt-5.6-terra` | large filter | auto | direct | PASS | 17,297 / 520 / 17,817 | 9 / 0 | 20,987 |
| `gpt-5.6-terra` | large filter | direct | direct | PASS | 17,223 / 547 / 17,770 | 9 / 0 | 20,981 |
| `gpt-5.6-terra` | large filter | PTC | catalog + execute | FAIL | 13,683 / 559 / 14,242 | 2 / 1 | 7,473 |
| `gpt-5.6-luna` | n1 | auto | direct | FAIL | 11,603 / 149 / 11,752 | 2 / 0 | 2,858 |
| `gpt-5.6-luna` | n1 | direct | direct | FAIL | 11,553 / 127 / 11,680 | 2 / 0 | 2,858 |
| `gpt-5.6-luna` | n1 | PTC | catalog + execute | FAIL | 13,493 / 493 / 13,986 | 2 / 2 | 6,760 |
| `gpt-5.6-luna` | large filter | auto | direct | PASS | 17,338 / 558 / 17,896 | 9 / 0 | 21,134 |
| `gpt-5.6-luna` | large filter | direct | direct | FAIL | 17,235 / 508 / 17,743 | 9 / 0 | 21,019 |
| `gpt-5.6-luna` | large filter | PTC | catalog + execute | PASS | 13,547 / 465 / 14,012 | 2 / 9 | 6,974 |

十二个 arm 都完成三轮且 usage 完整，总计报告 174,600 token：Terra 87,531，Luna 87,069。该总数包含失败样本的已报告 usage；失败成本不会被丢弃，也不会被当成效率收益。

### 失败分类

Terra 的两个 PTC arm 都只产生一个程序子调用和一个失败子结果，未满足任务要求的 inventory + active detail 效果合同。它们的 token 较低也不能成为效率胜利，因为质量 gate 必须先通过。

Luna 的 n1 三臂和 large direct 都完成了预期工具效果，但没有满足字节级最终答案合同。安全证据刻意不保存答案正文；即使输出 token 数暗示可能只是格式差异，也没有足够证据事后改判，必须保留 FAIL。Luna large auto 与 PTC 同时通过，因此可作本波唯一的 PTC/direct-route 成功对照。

### 成本与上下文

- Terra n1：auto 选择 direct，较 direct 多 99 token（`0.84%`）；两者都通过。PTC 更贵且质量失败。
- Terra large：auto 选择 direct，较 direct 多 47 token（`0.26%`）；两者都通过。PTC 虽少 3,528 token，但程序效果失败，不能进入效率 gate。
- Luna large：auto 选择 direct 并通过；PTC 也通过。PTC 比 auto 少 3,884 token（`21.70%`），最终上下文从 21,134 降至 6,974 token（`67.00%`），工具消息从 9 条降至 2 条。

## 架构结论

`auto_first_action` 的执行稳定性成立：四个 auto arm 都被宿主锁定为 direct，实际 wire 菜单、Session 事件与冻结 route 一致，没有混路、隐藏工具绕过或额外模型轮次。它的选择质量还不够：Luna large 已证明 PTC 在同质量下明显更省，但 auto 仍在第一步调用 inventory 后立即锁定 direct。

这暴露出当前“任何第一个直接工具都决定整条路线”的结构限制。inventory 一类发现工具会在模型知道 fan-out 和实际数据压力前锁死 direct。下一候选不应增加一个独立分类模型请求；应把只读发现工具声明为中性 probe，保持 direct/PTC 两条路线仍可选，并用冻结的输出上限、fan-out、上下文预算和已观测 probe 规模生成结构化 route decision。该候选仍需避免重复副作用、限制 probe 数量，并在同一 Run 持久化锁定依据。

在该候选通过 n1、large filter、审批、撤销、恢复和留出集前，general 继续使用 `direct_only`。效率 gate 已接到 release/canary：质量与能力兼容先行；启用时必须有匹配 baseline、不同且可验证的冻结 route identity，以及逐 case composition/artifact revision；failed 或 inconclusive 都阻止 stage，且不会自动 promote。
