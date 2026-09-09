# 强制执行路由真实模型验收（第 42 波）

状态：共享执行路由在一次新鲜会话的真实 Responses 实验中得到有限验证；尚未达到自动发布或“全局最优”结论。Terra 的三个 arm 都完成任务，Luna 只完成 auto arm，随后在 direct arm 的最终请求收到 HTTP 429，PTC arm 因而未运行。两个请求模型都只启动一次冻结测试二进制，未重试。

## 测试边界

`TestLiveEnforcedRouteTriad` 不再依赖 profile 提示词让模型自行遵守路由。每个 arm 都先调用共享的 `internal/executionroute.Resolve` 和默认 `runexecutor` registry，再由模型面对宿主投影后的实际工具菜单。测试使用：

- `auto_first_action`：首轮只暴露 catalog 和直接工具；模型第一次合格动作后由宿主锁定 direct 或 catalog/execute 路径。
- `direct_only`：只投影两个直接 fixture 工具。
- `ptc_only`：先只投影 `program.catalog`，严格完成 catalog 后只投影 `program.execute`，执行结果后不再投影工具。

每个请求模型内的三个 arm 共享同一个随机八项不透明数据集、同一用户任务、同一基础 profile、四项能力全集、10 轮模型调用上限和 11 次工具调用上限。每个 arm 使用独立 Session 和 journal。两个模型由两个独立进程运行，因此它们各自的数据集不同，不能用本波结果做模型间排名。

测试配置取自本机 `D:\cc\llm.txt` 第 6–9 行：第 6 行作为端点，第 7 行只注入进程环境，第 8、9 行作为请求模型名。凭证和端点没有写入证据、日志或仓库。协议固定为 Responses，`stream=true`、`store=false`，相邻 adapter 调用开始时间至少间隔 5 秒。

冻结产物与私有证据位于 `.codex-v46-audit/programmatic-20260909/optimization-wave-42/`：

- 冻结测试二进制 `programmatic-enforced-route.test.exe`，SHA-256 `650f07eedf7a6dfedb37d7f6fb7544bc9f289717c2e30294372afa5915aeb156`。
- 修正后的安全汇总 `safe-summary.json`，schema v2，SHA-256 `921b378de1f95d944071b72926ce908fec3c3caafeb08c3e863b46846959cf37`。首次派生汇总把空工具数组误计为一项，已原样保留为 `safe-summary-v1-invalid.json`，SHA-256 `79be16bf36fc881312a1aca29a2e5ab37e5eff50529d988317760fd4a3cf5a33`；v2 明确记录 supersedes 关系和更正原因。
- 原始 JSON 只包含哈希、冻结路由元数据、轮次、provider 报告的 usage、工具名/数量、HTTP 状态、延迟和上下文/ledger/执行聚合；不包含 prompt、模型正文、工具参数/结果、生成程序或凭证。

请求模型名只证明发送给兼容网关的配置名，不能独立认证网关实际使用的后端模型。表中的 token 是 provider 响应报告并由本地 invocation/session ledger 对账的 usage，不是独立账单或计费证明。

## 结果

| 请求模型 | arm | Runtime / 原始测试 / 修正审定 | 实际路径 | 模型轮次 | 输入 / 输出 / 总 token | 顶层 / 程序子调用 |
| --- | --- | --- | --- | ---: | ---: | ---: |
| `gpt-5.6-terra` | `auto_first_action` | completed / FAIL / PASS | direct | 3 | 12,618 / 748 / 13,366 | 9 / 0 |
| `gpt-5.6-terra` | `direct_only` | completed / FAIL / PASS | direct | 3 | 12,622 / 703 / 13,325 | 9 / 0 |
| `gpt-5.6-terra` | `ptc_only` | completed / FAIL / PASS | catalog + execute | 3 | 13,679 / 768 / 14,447 | 2 / 9 |
| `gpt-5.6-luna` | `auto_first_action` | completed / FAIL / PASS | direct | 3 | 12,633 / 730 / 13,363 | 9 / 0 |
| `gpt-5.6-luna` | `direct_only` | failed / FAIL / FAIL | direct，最终请求 429 | 3 | unknown；前两轮已知 7,704 / 531 | 9 / 0 |
| `gpt-5.6-luna` | `ptc_only` | 未运行 | unknown | 0 | unknown | unknown |

Luna direct 的第三轮没有 usage，完整总量必须保持 unknown；前两轮的已知小计不能冒充完整成本。HTTP 429 按合同停止该模型剩余 arm，所以 PTC 缺失也不能当作零成本或失败的路由选择。

Terra 是本波唯一完整三臂比较。三个 arm 都是三轮，`direct_only` 的 13,325 token 最低。auto 正确锁定 direct，较 direct 多 41 token，约 `0.31%`；PTC 较 direct 多 1,122 token，约 `8.42%`。PTC 把模型可见的顶层工具边界从 9 次降为 2 次，但仍在受保护执行器内部运行 9 个子调用，在这个独立 fan-out 任务上没有减少模型轮次或 token。因此，这一任务应走 direct，auto 的选择方向正确，但单样本没有证明它总能得到最小成本。

## 原始红灯与事后审定

冻结二进制中的 wire 断言错误地要求每个 Responses 请求都出现顶层 `instructions`，并要求零工具的最终轮也携带 `parallel_tool_calls=true`。真实记录显示：

- auto/direct 的每轮 wire `tool_count` 与实际投影工具数完全一致，但系统上下文由请求 `input` 承载，顶层 `instructions` 不存在。
- PTC 最终轮实际投影工具数和 wire `tool_count` 都是 0，编码器相应省略 `parallel_tool_calls`。
- 所有完成样本在到达该断言前，已经通过任务答案、工具效果恰好一次、预算、usage/ledger、上下文和执行路径检查。

所以冻结测试的原始 `acceptance_passed` 保持 `false`，原始证据没有修改。事后 PASS 使用更正后的纯结构规则重新审定：body 可观察且为有效 JSON、`stream=true`、`store=false`、wire 工具数精确等于当轮投影工具数；只有工具数大于 0 时才要求 `parallel_tool_calls=true`。修复只改验收器并增加离线回归，没有再次请求模型。

## 对路由、自进化和可观测性的结论

本波证明了新鲜运行的数据平面可以强制执行三种路由，隐藏工具不会仅凭模型生成的名称绕过投影，auto 的首个合格动作可以在同一 Run 内锁定路径。它没有覆盖进程恢复、授权变化、审批、动态能力变化或多种任务形态。catalog 后恢复的 composition/provider revision 绑定已由离线 race 测试覆盖，但还没有真实模型恢复验收。

效率 gate 现在要求 baseline/candidate 的冻结 route identity 非空且不同，并与 composition revision 和逐 case artifact revision 一致；默认至少需要 3 个 paired case、其中 2 个改善。候选仍需通过多任务、留出数据、失败/审批/恢复场景和 canary，再由人工提升。第 42 波是开发样本，不会自动改写 general profile；当前默认继续保持 `direct_only`。

后续第 43 波增加 n1 与大结果过滤/循环任务，确认当前 auto 对 direct 有稳定偏置，并找到一个同质量下 PTC 节省约 21.70% token 的样本。见[多任务强制路由真实模型验收](2026-09-09-diverse-enforced-routing-live.md)。
