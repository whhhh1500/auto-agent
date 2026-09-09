# CodePTC 预留接入合同

CodePTC 延后实现。本次仅预留执行器路由标识与宿主侧的能力访问接口，不提供代码运行器、沙箱集成、程序恢复或 AI 模式选择器。PTC 也不能因这些预留接口而被视为已实现。

## 模式路由

预留执行器引用为 `codeptc@1`，复用 `pkg/app/runexecutor` 的 `Registration`、`Factory`、`RunExecutor` 和精确版本解析。默认服务不注册此引用；显式解析它返回 `ErrExecutorNotFound`，不回退到 sequential。

未来实现须由宿主显式提供真实工厂和 implementation revision 后注册。仅设置 Profile 的 `harness.executor.id=codeptc`、`harness.executor.version=1` 不会使该模式可执行。现有执行器身份记录、恢复时的版本检查继续适用，但不自动证明新运行器具有安全恢复能力。

未来 AI 选择器必须从实际可用的执行器及其适用条件中选择。文档里的预留模式不是可用能力目录，不能通过注册一个假执行器来占位。

## 能力与工具边界

CodePTC 必须使用框架明确暴露的能力接口。普通模型工具暴露不自动等于允许代码调用，代码运行权限也不等于业务工具权限。

共用的程序化能力声明为 `CapabilityManifest.Metadata["harness.programmatic.exposure"] = "1"`。键和值由 `pkg/app/programmatic` 的 `ExposureKey` 和 `ExposureVersion` 定义，避免 PTC 与未来 CodePTC 维护两套能力清单。该声明仅允许宿主进行程序化目录投影，不授予权限，也不启用 CodePTC。未来 CodePTC 宿主仍须验证运行器适配条件和当前授权。

预留的宿主接口为 `programmatic.CodePTCToolAccess`：

```go
ListTools(context.Context) ([]programmatic.Descriptor, error)
CallTool(context.Context, string, map[string]any) (core.CapabilityResult, error)
```

共享描述符 `programmatic.Descriptor` 包含公开 `ToolSchema`、`CapabilityVersion`、精确 `BindingDigest`、`OutputSchema` 和 `MaxOutputBytes`。绑定摘要覆盖宿主提供的能力合同、来源和 ProviderRevision；描述符不输出这些内部配置。接口由可信宿主针对当前运行建立；未来代码运行器使用受限协议访问它，不能允许代码自行替换宿主实现或调用身份。共享目录与受保护工具桥可以复用，但此处不提供 CodePTC 运行器或其进程通信协议。

未来宿主先筛选显式允许程序调用、且当前身份有权访问的能力，再生成本次运行的非敏感工具接口。代码不能接收完整 CapabilityManifest 中的执行配置、凭据引用或内部端点，也不能自行传入租户、会话、运行和调用身份。

调用桥接必须绑定可信运行身份、能力版本和稳定调用身份；每次实际执行都重新检查适用的授权、参数、预算及审批要求，并关联调用日志。代码不能直接持有底层 Provider 或用原始执行接口绕过保护链路。

## 后续实现验收条件

- 能力声明、按权限生成接口和实际调用三者一致；未暴露或被撤销的能力不能执行。
- 工具参数、输出边界、取消、预算及错误脱敏得到验证。
- 程序暂停等待审批后，恢复不会重复已完成的副作用；未知结果禁止盲目重放。
- 进程硬杀、结果丢失、取消竞争及能力版本变化有明确行为和验收证据。
- 沙箱文件、网络和进程权限不能无意中绕过工具规范的治理范围。

当前预留接口不满足上述实现验收条件，也不为代码执行提供任何权限。
