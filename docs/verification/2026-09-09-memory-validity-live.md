# 记忆有效性与后端故障真实验收

第三十二波通过用户指定本地配置第 6–9 行发出 Responses 真实请求。HEAD 为 `ad90ac0`，冻结的是包含未提交实现的测试二进制，SHA-256 为 `a12a84ab9cb6b6f01c5599d5e19a7c23cced8fe6171c81d92312d1932fc554f4`。请求模型名称不能认证网关后端身份。

## 合同与实际路径

入口 `TestLiveMemoryValidityAcceptance`，fixture `memory_validity_v1`。每个模型内三臂共享同一组不透明随机事实、lookup 和 prompt，安全记录的 prompt hash 也相同；两模型各生成自己的随机组，因此不构成跨模型逐字相同输入的严格性能比较。每臂新建 Runtime/Session，固定按 fresh、expired、backend_error 顺序运行一次，不重试失败。

任务要求一次 `memory.recall`、limit=1；有一条记录就回答其中 code，否则回答 UNKNOWN。问题不含目标值。仅暴露 recall，使用 read principal、生产标准 capability、accepted invocation、journal 和正常 assembler。MaxSteps=2、MaxToolCalls=1，输出上限 4,096，每模型共用 15 秒请求起始间隔。该 observer 没有新增 adapter limits 透传，沿用既有 memory fixture 的 Core fallback，不用这个实验认证 provider 原生窗口。

测试私有 Store 装饰器按 `(scope, entry ID)` 有效期账本过滤，固定有效期时钟；先从有界 SliceStore 取全部匹配项，再过滤并应用调用者原始 limit。expiry 等于 now 及缺少有效期均不可用。底层记录仍保留；同 ID 的 peer scope 条目参与隔离验证。为保证排序反例成立，第一条有效记录后间隔 20ms 写入不可用项，并独立检查原始首项确实不可用。详见[实验合同与离线基础](../superpowers/specs/2026-09-09-memory-validity-experiment.md)。

## 真实结果

| 请求模型 | 场景 | 验收 | 实际 adapter 调用 | 实际 recall | 已报告输入 | 已报告输出 |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| gpt-5.6-terra | fresh | PASS | 2 | 1 | 7,857 | 191 |
| gpt-5.6-terra | expired | PASS | 2 | 1 | 7,736 | 116 |
| gpt-5.6-terra | backend_error | PASS | 2 | 1 | 7,715 | 158 |
| gpt-5.6-luna | fresh | PASS | 2 | 1 | 7,792 | 123 |
| gpt-5.6-luna | expired | PASS | 2 | 1 | 7,703 | 81 |
| gpt-5.6-luna | backend_error | PASS | 2 | 1 | 7,727 | 116 |

合计 **12 次 adapter 调用、6 次 recall，输入 46,530、输出 785，共 47,315 tokens**。六条记录逐 invocation 对账均通过，没有未知用量样本或被跳过记录。两模型进程和 runner 均退出 0；没有新增摘要模型调用。这是 adapter 观察与已报告用量，不能代替独立网络计数或账单。

fresh 返回一条有效同 scope 记录，最终与 durable answer 均为其值；expired 返回成功且可解析的空 entries，最终 UNKNOWN；backend_error 返回失败且代码为 `memory_recall_failed`，最终同样 UNKNOWN。后者不是检索成功或自动恢复，只是按本任务合同诚实结束且没有再次检索。

所有组均有两次组装、一次完成 journal、精确 durable call/result 与下一请求上下文配对。失效、缺有效期、peer、无关值在工具结果、全轮模型上下文和最终答案中均缺席；expired/backend_error 的目标值也缺席。原始记录保留、peer 快照未改变、原始首项不可用的断言全部通过。fresh 的 target-absence 检查不适用，记录中有独立 applicability 字段，不能把其 false 解读为泄漏失败。

## 观测与后续使用

新 schema `harness.programmatic.live-memory-validity/v1` 保留普通 `invocation_evidence`，附加封闭计数/谓词；不保存提示、随机事实、工具正文或模型正文。汇总器保留质量失败的已报告成本，并把缺失调用 usage 标为 unknown。离线反例验证末次 usage 缺失和错误答案不能假绿；redaction 测试核验落盘记录不含不透明值。

这与候选改进的联系是证据分类：工具失败、成功空结果、正确未知回答是不同事实。不能把故障组 PASS 写成“检索成功”，不能把源码任务中的自述 complete 当长期经验。质量和完整资源计量要独立满足，已报告小计不能单独触发发布。

本轮是指定 lookup 的开发集成实验，使用内存 Store 和测试私有有效期过滤。未覆盖生产 TTL、SQL expiry、自由检索词选择、仍可读但错误的记忆与独立权威来源冲突、长期会话或恢复。不同组最终需要的信息不同，不能用 UNKNOWN 更短回答宣称节省成本；没有同任务无记忆/直接证据基线，也不能宣称最低 token。

## 验证

programmatic 整包普通测试、memory-validity 定向 race、programmatic vet、六项 Python 汇总测试通过。两位 Terra 子 agent 分别实现和只读复核，真实请求由 root 执行。凭据扫描快照为 103 个变更/未跟踪源码文档文件、1,810,772 bytes，SHA-256 `87219bc488197935d372be2f56816d79ceec108dd4742d563c9ce188c880d41c`，未发现泄漏；该快照在新增本报告之前。未运行全仓 gate、PG 或硬杀。

安全原始记录保存在外层审计目录 `programmatic-20260909/optimization-wave-32`。整体优化目标仍需独立任务分布、权威冲突、完整成本发布门禁及更广稳定性证据，不能由六组通过关闭。
