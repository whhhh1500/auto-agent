# 通知平台扩展

通知层不维护固定的平台名单。宿主选择并注册平台实现，管理员通过现有目标管理接口动态配置接收目标，Agent 使用相同的 `notify.channels`、`notify.targets`、`notify.send` 工具发现并调用它们。

## 两个不同的扩展入口

- **平台实现**：开发者提供 `notification.Channel`，以 `ChannelRef{ID, Version}` 注册。实现可以使用自己的 SDK、内部通知服务，或通过 `notifybridge` 使用 `nikoksr/notify` 的 `Notifier`。不需要修改 Core 或 SQL schema。
- **接收目标**：管理员调用 `GET/POST/PUT/DELETE /v1/admin/notification-targets`，按租户新增、更新、停用或删除目标。每个目标引用一个已注册的平台版本，其配置结构由该平台定义。

平台注册是宿主组装时的代码注入；目标配置是在运行时生效的持久化数据。默认二进制保留 Webhook 实现。仅在目标 JSON 中写入一个新平台名称不会安装 SDK，也不会加载任意 Go 代码。需要独立部署的平台，可以由外部通知服务实现，再通过现有 Webhook 通道连接。

## 平台合同

`Channel` 只需要实现两个方法：

```go
Descriptor() notification.Descriptor
Deliver(context.Context, notification.Delivery) (notification.Receipt, error)
```

`Descriptor` 仅包含公开渠道标识和能力。`Delivery` 的租户、会话、运行和调用身份由受保护工具路径提供，目标是 opaque `TargetRef`，不是模型可直接指定的 URL 或 token。平台按当前租户、目标和精确渠道版本解析私有配置。

平台还可提供 `ConfigurationValidator`，在保存配置时验证其结构。验证过程不应发送消息或发起认证请求；错误不得包含凭据。默认 SQL 存储加密配置，列表只返回元数据，更新使用 revision 防止覆盖他人的修改。

## 使用 notify

宿主使用 `pkg/adapter/notification/runtime` 集中声明平台：

```go
assembly, err := notificationruntime.New([]notificationruntime.Registration{
    {Ref: ref, Validator: validator, Build: buildChannel},
    // 添加更多 Registration；无需修改渠道枚举或数据库结构。
})
```

`Ref` 是平台和版本，`Validator` 是可选配置校验器，`Build` 接收公共目标 Service 并返回该版本的 `Channel`。将 `assembly.Refs()` 传给目标仓库；用相同引用和 `assembly.Validators()` 构造 Service，再调用 `assembly.BuildRegistry(service)`。将这个 Service 作为 `coretool.NewWithDirectory` 的目录，使所有已注册渠道的启用目标都能被发现。

默认 server 使用相同的组装入口。管理列表的 `channels` 字段提供已注册的 ID 和版本，Console 据此显示候选项，配置编辑器不规定平台字段。

Console 默认展示 `notify v1.6.0` 平台目录，数据独立放在 [notification-platforms.js](../pkg/console/static/js/notification-platforms.js)。目录覆盖官方 [service 源码目录](https://github.com/nikoksr/notify/tree/v1.6.0/service) 的 33 个服务包，并将同一 `line` 包中的 LINE Notify 单列，共 34 条。包含 README 表格漏列、源码已经提供的 Mattermost。目录只提供平台名称和建议的本项目渠道引用；“同名渠道已注册”根据本服务的 ID/版本计算，不等于已配置目标或真实投递验证。

其中 32 条作为可选接入项；WhatsApp 按 [v1.6.0 README](https://github.com/nikoksr/notify/blob/v1.6.0/README.md#supported-services) 标注为上游不支持，LINE Notify 按[官方停服公告](https://notify-bot.line.me/)标注为已于 2025-03-31 停服。这两项仍可查看，但不能从目录选用。其他条目表示上游包含实现，不保证厂商服务在所有账户、地区或系统上均可用。

目录数据不会限制 `Registration` 或目标 API 的渠道名称；后来添加的平台仍可使用自定义 ID/版本，管理端会从服务端发现它。维护者可以增加目录条目而不修改发送或权限逻辑。

可运行的组合验证见 [notification-extension](../examples/notification-extension/extension_test.go)：两个自定义平台共享真实 SQLite/AES 目标存储，验证多租户隔离、配置更新、停用和删除；发送由本地捕获服务实现，不连接外部平台。

可选桥接包为 `pkg/adapter/notification/notifybridge`，锁定 `github.com/nikoksr/notify v1.6.0`。它接受渠道引用、配置解析器和工厂：

```go
channel, err := notifybridge.New(ref, targets,
    func(ctx context.Context, config []byte) (notify.Notifier, error) {
        // 解析当前目标的配置，构造调用方选择的服务。
        // 返回的服务只能发送到该目标允许的接收人。
        return newConfiguredService(ctx, config)
    })
```

这里的 `newConfiguredService` 是使用方的实现，不是框架内置函数。工厂可以返回任意满足 `notify.Notifier` 的具体服务；框架没有平台枚举或平台 switch。按需导入服务包，不自动引入所有平台 SDK。

桥接每次投递解析当前目标配置，并调用工厂返回的 `Send`。不使用全局 `notify.UseServices`，不跨租户广播，不缓存租户凭据。标题固定为 `auto-agent`，正文为 `Delivery.Text`。`notify.Notifier` 没有 format/metadata 参数，桥接不自动转换这些字段；固定格式可由目标服务处理，需要逐条消息格式或元数据映射的宿主可以直接实现 `Channel`。

工厂必须返回可实际发送的服务，不能返回空的或禁用的广播器冒充成功。服务及其 SDK 必须处理 context、超时和连接关闭；桥接不能强制中断忽略 context 的第三方实现。桥接会清理其拥有的配置字节，但 Go 字符串、SDK 内部复制及平台凭据的生命周期仍由实现负责。

## 投递语义

一次工具调用选择一个渠道版本和一个目标。`notify.send` 继续要求审批，且不声明幂等。桥接不自动重试；网络错误可能表示平台已经收到消息，不能因此安全地重发。

服务返回成功时桥接只记录 `accepted`，不声称接收人已阅读或最终收到。回执标识使用本地 idempotency key，不是平台提供的送达凭证。错误和 panic 对外返回固定错误，不能透出服务响应正文或配置。

去重、限流、模板、业务路由和后台任务不属于桥接职责。需要这些能力的宿主可以在平台实现或外部通知服务中提供，并明确其失败和重试语义。
