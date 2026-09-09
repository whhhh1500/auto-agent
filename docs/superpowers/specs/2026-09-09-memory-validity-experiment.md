# 记忆有效性实验合同（开发真实验收已完成）

目标是区分可用记忆、实际失效过滤和后端故障对回答质量、调用次数及成本的影响。不能靠给记录添加 `expired` 标签来模拟已实现的有效期；当前 MemoryEntry/SliceStore 没有生产有效期语义。本合同仅规划测试私有 store 装饰器，不宣称生产功能已接入。

## 固定条件

- 三臂 fresh、expired、backend_error 使用相同问题、能力菜单和 Runtime 预算，事实值为不进入提示的随机不透明值。
- 经生产 `memory.recall`、accepted invocation、Session、assembler 调用；不由模型包装器伪造工具结果。
- 固定测试时钟，私有有效期账本按 `(scope.String(), Entry.ID)` 索引；到期时刻等于 now 即失效。缺少有效期的实验记录按不可用处理，不能默认为永不过期。
- 基础 SliceStore 内同时保留当前 scope 与 peer scope 的同查询不同值记录；有效期过滤不删除或修改它们。
- 先在 SliceStore 中按实际 scope/query/tags 召回全部有界记录，再过滤有效期，最后应用调用者 limit。当前 SliceStore 的 limit=0 支持此过程。不能先裁剪 limit 再过滤，否则排序靠前的过期项会掩盖靠后的有效项；该实现仅适用于这个有界 fixture，不是任意远程 Store 的分页方案。

## 质量与观测

fresh 返回当前不透明值；expired 返回空条目并回答 UNKNOWN；backend_error 经真实 capability 错误映射返回失败结果并回答 UNKNOWN。用户提示可规定未知时的输出合同，但不包含 arm 名称、预期值或过期账本。三臂均只允许一次 recall、两次实际 adapter 调用，禁止把重试或失败回答当节省。

独立验证源 store 调用、工具事件与结果、最终模型上下文中的精确 call/result 对、TurnResult 与 durable 最终答案。失效值、peer 值和无关值不得出现在工具结果、模型可见上下文或最终答案；底层保留的记录不受此禁令，因为它们用于验证未被修改。

逐调用使用现有 invocation 对账；缺失报告标为 unknown，失败前的已报告用量保留。只保存哈希、计数和封闭谓词，不能持久化随机秘密值、凭据或模型正文。证据需要独立 schema 和汇总器支持，不能误并入旧 memory-scope 的总额核对协议。

离线先覆盖到期边界、scope 同 ID 冲突、limit 前过滤、缺失有效期、后端错误、peer 不变和错误答案被拒绝，再冻结二进制跑两模型共六臂。此为开发集成实验，未实现之前不得标为真实验收完成。

污染记忆另需独立权威来源的对照。模型仅看到一个仍有效但错误的值时，没有证据判断它错误；不能将无法猜中隐藏真值认定为路由缺陷。未来该实验应明确权威版本、冲突处理和撤销，质量与完整成本门禁通过前不把结果写成长期策略经验。

## 当前实现与证据

测试私有 `live_memory_validity_shared_test.go` 与 `live_memory_validity_offline_test.go` 已建立三臂 Runtime 闭环。确定性 adapter 从实际召回工具结果生成答案；不把隐藏目标值直接交给答题模型。验证一次 scoped recall、完成 journal、两次 adapter invocation、精确 call/result 上下文配对、失效/缺有效期/peer/无关值不进入模型可见请求，以及原始记录保留。过期臂和故障臂还拒绝目标值出现在任何模型请求与输出。

缺失末次 usage 的反例保留先前报告的小计但拒绝完整对账；错误最终答案即使 Runtime completed 也被拒绝。原始调用 limit 在改为内部 0 之前仍作 Store 合同校验。

最初定向执行因 SliceStore 写入时间戳相同而失败：无法证明不可用项排在有效项之前。fixture 在第一条有效记录后增加 20ms 间隔，仍独立断言实际原始排序，不以插入顺序推断。有效期判断使用固定时钟；这段间隔仅用于 SliceStore 自行生成的 CreatedAt。修订后 root 执行 `go test -race ./examples/programmatic -run '^TestMemoryValidity' -count=1` 通过（1.254s）。这不是 TTL 生产验收，也未调用真实 provider。

第三十二波已补 live 入口、安全 schema/汇总器并冻结二进制完成两模型六臂，全部通过，12 次真实 adapter 调用共报告 47,315 tokens。每模型三臂共享 opaque fixture/prompt；逐次用量及状态/上下文断言通过。见[真实验收记录](../../verification/2026-09-09-memory-validity-live.md)。此结果仅完成本开发合同，不代表生产 TTL、SQL expiry、权威冲突或最低成本已验证。
