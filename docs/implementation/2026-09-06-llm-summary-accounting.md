# LLM 摘要的输入、用量与审核

目标：可选 LLM 摘要纳入实际 Run 身份、trace 和持久 token 统计，失败时仍记录模型已报告的有效用量；比较整个任务成本，不能只展示压缩后的请求。

## 设计

- core 增加一个返回已验证 Usage 的流消费函数，继续复用唯一协议校验器；旧接口保留。有效 usage 可以先于流错误返回，重复/非法 usage 不重复计费。普通 agent 模型错误路径同样保留已校验的 usage。
- app 增加可选 `MeteredContextSummarizer`，接收消息和实际 Run 身份，返回摘要文本、usage 和错误。已有 `ContextSummarizer` 的本地策略照常工作。`RollingSummarizer` 将已报告的摘要用量作为独立 `run/usage` 增量追加并发送，现有 SQL RunStat 汇总所有该 Run 的增量；成功和失败都进入账本。
- `LlmSummarizer` 按实际会话/Run/step 构造 Gate 身份，在输入检查后才授权和调用。Telemetry 从宿主显式注入，以 `model.purpose=context_summary` 标记模型 span/指标，继承 Run trace。预检/Gate 拒绝不计为真实模型调用，错误字符串不暴露上游响应。
- transcript 保留每条消息的角色/来源序号、工具 ID/名字/参数、结果配对 ID 与摘要 provenance；不发送厂商 continuation。按完整消息记录写入有界 JSONL，超预算显式省略；app 在序列化前限制 JSON-native 参数图的字节、节点和深度，拒绝调用任意自定义 marshaler。对超长 Content 先替换省略标记，不先复制整个文本。本地 extractive 同样保留有界工具参数，避免丢失“结果属于哪次查询”的含义。
- 不启用默认 LLM 摘要，不新增 core 事件格式、全局可变用量缓存或自动重试。模型摘要仍受有限输入/输出预算与事实丢失风险约束。

## 验收

先离线覆盖：finish 上 usage、有效 usage 后错误/取消、重复/负值 usage、tool metadata/continuation 隔离、超限预检、Gate 身份、失败不追加摘要但保留费用、SQL RunStat 与历史 usage 求和一致、trace purpose 和父子关联。再以同模型同历史做本地提取/LLM 摘要任务对照，串行真实请求，包含摘要调用的全量输入/输出/总 token；恢复后继续验证未被上一轮回答泄露的事实。保留失败与负收益，不为数字重跑计费请求。

## 实施与实测

以上已实施。9 次真实请求、峰值在途 1、零自动重试，两组各三次正确回答。LLM 普通请求更小，但计入摘要后总 token 4,423→12,990、耗时 10.05→34.18 秒，默认仍用本地提取。脚本模型注入失败的用量、trace、终态与 SQL 统计一致；2 MiB 被拒绝 chunk 的聚合副本已消除。15 份证据、有限预算和恢复边界、基准与最终门禁见[实测记录](../performance/2026-09-06-llm-summary-accounting.md)。整体目标仍需真正进程退出恢复与公平跨框架任务集，未因本切片通过而标完成。
