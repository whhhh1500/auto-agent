# WASM 资源控制与编译复用

本轮针对模块评估 M27 已确认的执行中取消、线性内存缺口，并实测重复执行的编译开销。范围为 `pkg/execution` 外层适配器；core 的 Executor、ExecutionSpec、工具协议不增加字段。

## 设计

- 保留每次调用独立 runtime / WASI / 内存。共享 runtime 需要额外实例命名、并发关闭和租户隔离管理，本轮不采用。
- `WazeroExecutor` 增加 `MemoryLimitPages`、`Timeout`、`CompilationCache`。零值分别采用 2048 页（128 MiB）、30 秒、不共享编译缓存。非零页数必须不超过 WASM32 上限 65536；负时限返回错误。配置在读取文件前校验。
- 强制开启 `WithCloseOnContextDone(true)`，以调用 context 和适配器时限的较早者中断 guest 计算。错误保留 `errors.Is` 对 canceled/deadline 的识别。关闭 runtime 使用独立于取消的 context。
- 编译缓存可选，由宿主创建、共享与关闭；执行器不关闭外部缓存。不保存模块实例、内存、argv 或输出。缓存为 wazero 已有接口，不新增核心抽象或全局缓存；宿主应限制加载的模块集合并管理缓存生命周期。
- ArtifactRevision 包含规范化后的资源配置，资源策略变化会改变组合标识；缓存不会改变 guest 语义，不计入标识。
- 仍限制模块文件 32 MiB、stdout 1 MiB 和 argv；只开放现有 WASI，无宿主文件挂载、继承环境或网络接口。该适配器面向已注册的本地普通模块文件，不是整个宿主进程的硬资源隔离。文件读取和编译阶段不承诺可在任意 deadline 精确抢占；线性内存上限不包含编译器、表、Go heap 和所有其他宿主开销。

## 验收

1. 用独立测试子进程复现 guest 无限循环不响应 deadline / cancel；父进程有额外硬超时，失败不能留下失控线程。
2. 使用实际 WASM 字节码验证初始内存拒绝、memory.grow 上界、trap、WASI 输出、输出超限和取消后下一次调用；验证共享缓存不共享内存且不能绕过更严格的资源限制。
3. 对仓库 `internal/calc` 用 Go WASI 编译，验证真实 argv / stdout；对相同模块分别测量冷执行、首次缓存执行、热缓存执行，报告完整执行而非只计编译函数；不将 B/op 写成 RSS。
4. 串行 Gemini 3.8 Flash 发起 WASM 工具调用，检查算术结果、真实工具结果、SQL 历史、HTTP 分页历史与 OTel trace 对应关系。测试使用现有 opt-in 开关，普通检查不得调用 LLM。
5. 更新 M27、架构文档、证据报告，通过相关单测/race 与仓库质量检查，本地提交。此切片完成不代表已证明整体优于所有主流框架。

## 本切片结果

上述执行器资源、缓存、真实会话验收已完成，详见[实测记录与证据](../performance/2026-09-06-wasm-resource-and-cache.md)。实测额外发现 `InstantiateWithConfig` 在 WASI 退出时清除共享编译结果的问题，改为显式编译/实例化后才取得热调用收益；资源配置、core 零增量和独立实例的设计保持不变。整体目标继续按模块评估中的未验证项推进。
