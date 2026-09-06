# Harness Core 与主流 Agent 框架：实现差异与选型

核验日期：2026-09-06。本地源码基线：`5beed15`。这是[44 个 Agent 模块评估](agent-module-assessment.md)的对照资料。外部能力依据本次访问的官方文档，未安装并运行七套框架；没有统一硬件、模型和任务的横向性能/质量测试。下文“更适合”是基于能力与本项目目标的工程判断，不是官方排名。

## 先给出选型判断

| 主要目标 | 优先候选 | 对本项目的判断与理由 |
| --- | --- | --- |
| 继续建设自己的 Go 多租户 Agent 服务，保留现有审批、审计、配置和恢复合同 | Harness Core | 现有工程已经围绕这些约束贯通；直接迁移会重做大量服务集成。值得继续，但应聚焦治理与适配，不必自研所有外围生态 |
| 从零快速做普通 Agent 应用、需要很多现成模型和工具 | LangChain / OpenAI Agents SDK / 对应厂商 ADK | 通常比补齐本项目连接器更省开发工作；具体候选取决于厂商与语言 |
| 复杂分支、循环、并行和可恢复图流程 | LangGraph；Go 项目重点比较 Eino、ADK 2.x | 本项目 Graph 当前服务适配较窄，不是通用图编辑/运行平台；成熟图 API 是更充分的起点 |
| Go 组件组合、RAG、流式图与 Agent 开发 | Eino | 同语言下最值得直接做原型对照的候选；当前项目的服务治理可以与它的组件服务协作 |
| OpenAI 原生能力、MCP 多传输、快速使用已有 E2B 客户端集成 | OpenAI Agents SDK | 本项目需要额外适配；官方 SDK 的现成功能更贴近这个目标，沙箱供应商仍需单独配置 |
| Gemini/Google 生态和相应模型能力 | Google ADK | 本项目只验证了兼容连接，未内置原生 Gemini 协议；原生生态候选更直接 |
| .NET / Microsoft 企业应用与对应托管栈 | Microsoft Agent Framework | 语言、身份和平台整合通常更契合；其 Go 支持仍须按预览范围审查 |
| Python 中按角色、任务和流程快速组织协作 | CrewAI | 高层角色/任务表达更直接；不能据此推导复杂任务成功率更高 |
| 文档接入、向量检索和 RAG 是主要工作量 | LlamaIndex；Go 也比较 Eino | 当前关键词 Index 适合小范围起步，完整 RAG 生态应优先复用 |
| 找出“最快”“最便宜”“最可靠” | 暂不能定胜负 | [已有数据](performance/2026-09-06-module-benchmarks.md)全部是本地基准/验收，缺少同口径对照与长期生产证据 |

这些候选不是必须互斥。可让 Harness Core 管 session、权限、审批和持久调用，以 HTTP 或 Private Runner 调用由 Eino/LlamaIndex 等实现的专业服务；代价是多一层部署、网络和追踪关联，需要保持副作用幂等与权限边界。

## 比较对象与版本范围

| 对象 | 本次参考范围 | 比较层次 |
| --- | --- | --- |
| Harness Core | `5beed15`，Go 1.25.13，公共 API pre-GA | 内核 + 可选扩展 + 自托管参考服务 + Console |
| LangChain / LangGraph | 当前官方 Python OSS 文档；Deep Agents 只作同生态高层补充 | 高层 Agent API 与图执行/持久化分开看；Agent Server/LangSmith 另列服务/平台面 |
| OpenAI Agents SDK | 当前官方 Python/TypeScript 指南 | SDK、模型 API、沙箱供应商与托管工具分开看 |
| Microsoft Agent Framework | 当前官方总览，包含 C#、Python 与 Go public preview | Agent、workflow、harness、hosting；不假设语言功能对等 |
| Google ADK | 当前 ADK 2.0 与 graph 文档；graph 标注 Python/TypeScript/Go v2.0.0 | 当前 graph/dynamic 与传统 workflow 模板分开看 |
| CrewAI | 官方页面重定向到 `v1.15.20` 文档 | Agents/Crew 与 Flows 的状态持久化分开看 |
| Eino | 当前 CloudWeGo 官方 overview、ADK HITL 与 checkpoint 文档 | Go 组件/compose 与高层 ADK 分开看 |
| LlamaIndex | 当前 Python framework Agent 与 VectorStoreIndex 文档 | AgentWorkflow 与数据/RAG 工具链分开看 |

除明确写出的文档版本外，不将页面内容冒充已锁定的库 release。采用前应锁定版本并跑实际功能验收。此处没有依据 GitHub star 数、营销材料或未经复现的厂商速度图做排名。

<a id="c1"></a>

## C1 — LangChain / LangGraph

LangChain 的高层 Agent API 建在 LangGraph 上，提供模型、工具和 middleware 组合；同生态 Deep Agents 进一步组织压缩、文件系统与子 Agent 等能力。这意味着“有工具循环和摘要”不是本项目独有的方向。[官方 overview](https://docs.langchain.com/oss/python/langchain/overview)

LangGraph 将状态恢复作为核心能力：线程状态可通过 checkpointer 保存，跨线程数据可通过 Store 管理；checkpoint 的 superstep、任务写入和不同 durability 模式涉及不同的延迟/恢复取舍。当前文档也包含增量 channel 路径，不能把它概括成“总是全量复制状态”。[Persistence](https://docs.langchain.com/oss/python/langgraph/persistence)、[Checkpointers](https://docs.langchain.com/oss/python/langgraph/checkpointers)

人工介入使用 interrupt / resume。恢复时包含 interrupt 的节点会重新进入，所以节点前置副作用仍需幂等设计；这与 Harness 的稳定工具调用日志侧重点不同。[Interrupts](https://docs.langchain.com/oss/python/langgraph/interrupts)

| 维度 | 与本项目的实质区别 |
| --- | --- |
| 编排单位 | LangGraph 面向可组合状态图；本项目默认是受保护顺序循环，Graph 是可选且当前服务适配有限 |
| 治理 | 本项目把多层 scope、principal、工具审批和 SQL run/control 集成在参考服务中；使用 LangGraph 仍须结合宿主授权及相应服务能力 |
| 恢复 | 两者都有持久化；checkpoint、事件日志与调用 journal 是不同颗粒度，不能直接以“有/无恢复”二分 |
| 生态 | 开发者现成组件选择通常更有利于 LangChain；本项目更强调自己的合同与组合证据 |
| 性能 | LangGraph 的并行与持久模式可影响吞吐，本项目的顺序/SQL 路径也有成本；缺同任务实测，不排名 |

**工程判断：** 一般复杂图应用优先试 LangGraph；已有 Go 服务治理需求且图很窄，继续用 Harness 有合理性。LangSmith 的观察/评估与 Agent Server 的托管/服务功能应计入相应产品比较，不能把 OSS SDK 当作整个生态上限。相关平台入口见[官方 overview](https://docs.langchain.com/oss/python/langchain/overview)与[checkpointer 部署说明](https://docs.langchain.com/oss/python/langgraph/checkpointers)。

<a id="c2"></a>

## C2 — OpenAI Agents SDK

官方 SDK 使用 Agent、运行循环、工具与 handoff 等对象组织应用，提供 Python/TypeScript 路径和模型 provider 扩展。其角色是应用 SDK；模型 API、托管工具和部署环境是另外的层。[Agents SDK 指南](https://developers.openai.com/api/docs/guides/agents)

当前审批机制可将运行中断状态序列化后再恢复，并非只支持进程内回调。开发者仍需要保管运行状态、决策和外部授权。[Guardrails and approvals](https://developers.openai.com/api/docs/guides/agents/guardrails-approvals)

MCP 指南覆盖 stdio、Streamable HTTP 和 hosted MCP 路径，并包含 tracing / observability 集成。本项目当前 MCP 主要是 stdio 客户端，其差距应明确写出。[Integrations and observability](https://developers.openai.com/api/docs/guides/agents/integrations-observability)

**E2B 结论：** 当前 Sandbox Agents 官方 provider 表明确列出 `E2BSandboxClient`，也列出 Docker 等不同运行环境。它说明已有客户端接入路径，不表示用户无需创建供应商配置，也不意味着这些 provider 提供完全一样的隔离和生命周期保证。[Sandbox Agents](https://developers.openai.com/api/docs/guides/agents/sandboxes)

| 维度 | 与本项目的实质区别 |
| --- | --- |
| 厂商功能 | SDK 更直接跟随 OpenAI Agent/工具接口；Harness 通过 Provider/Protocol 维持统一运行合同，适配范围较少 |
| 审批与状态 | 都支持暂停/恢复；Harness 已组合 SQL 审批、租户授权、队列与工具 journal，SDK 应结合其宿主持久化设计 |
| 沙箱 | 当前 SDK 已有 E2B client 路径；Harness 只有自有 Provider/Session 接口与本机实现，必须另写 E2B adapter |
| 多 Agent | SDK 的 handoff/Agent 工具与 Harness 父子持久委派含义不完全相同；不能只按 Agent 数量比较 |
| 性能 | 没有相同模型、流模式、工具、持久后端与并发对照；不能宣称 Go 框架必然更快 |

**工程判断：** 快速使用 OpenAI 原生工具或已有云沙箱集成，SDK 更直接；需要延续自有 Go 租户、审批和调用证据时，Harness 更贴近现有工程。不能把 OpenAI SDK 与 ChatGPT、Codex 或供应商云服务的完整产品能力混为一谈。

<a id="c3"></a>

## C3 — Microsoft Agent Framework

官方将其定位为 AutoGen 和 Semantic Kernel 的直接后继，组织 Agent、Harness Agent、workflow 与 hosting 等能力。当前文档包含 C#、Python 和 **Go public preview**；Go 尚未覆盖 declarative agents、RAG、CodeAct、functional workflows 等列明项目。因此“Microsoft 只有 .NET/Python”“Go 功能已全部对齐”都不准确。[官方 overview](https://learn.microsoft.com/en-us/agent-framework/overview/)

| 维度 | 与本项目的实质区别 |
| --- | --- |
| 选择范围 | 应把当前 Agent Framework 作为新评估主体，而不是仅凭旧 AutoGen 群聊示例或旧 Semantic Kernel plugin 印象判断 |
| 应用集成 | Microsoft 生态和相应语言应用是其明显的适配方向；本项目是自主管理的 Go 服务基础设施 |
| 服务比较 | 其 hosting 与 workflow 层比“裸 SDK 函数”更接近本项目服务组合；需要按所选语言和部署后端确认具体能力 |
| 可扩展性 | 两者都可通过模型/工具/执行边界扩展；本项目已有控制面也带来更多自维护责任 |
| 性能与成熟度 | 本次未运行其 Go 预览实现；不能从 C# 或 Python 的文档/案例外推 Go 的功能和可靠性 |

**工程判断：** Microsoft 企业栈优先评估该框架；纯 Go 项目可纳入候选，但要逐项验证预览缺口。现有 Harness 服务如果没有相应生态需求，没有仅因“大厂框架”而整体重写的技术依据。

<a id="c4"></a>

## C4 — Google ADK

当前 ADK 文档已经包含图式工作流：代码、工具、LLM、人类输入等节点可以用显式边组织，支持分支和状态管理；页面标注 Python、TypeScript、Go v2.0.0。不能仍把它概括为只有 Sequential/Parallel/Loop 三种固定模板。[Graph workflows](https://adk.dev/graphs/)

传统 workflow 模板仍有参考价值，但文档对 ADK 2 的 graph/dynamic 路径另有说明，采用时应按目标语言和版本选择 API。[Workflow agents](https://adk.dev/agents/workflow-agents/)、[ADK 2.0](https://adk.dev/2.0/)

ADK 也有独立 ArtifactService 与上下文保存/载入接口，因此“产物管理”并非本项目独有；本项目自己的 S3 配置与迁移日志是另一层服务治理。[Artifacts](https://adk.dev/artifacts/)

| 维度 | 与本项目的实质区别 |
| --- | --- |
| Gemini | ADK 属于原生生态候选；Harness 当前真实模型证据来自兼容连接，未内置原生 Gemini Protocol |
| 编排 | ADK 当前 graph API 比 Harness 服务中的单节点包装表达范围更广；具体所需节点、恢复和授权仍要做原型 |
| 语言 | ADK 已有 Go 图路径；不能把 Go 语言当成本项目相对它的独占优势 |
| 产物 | 都可管理产物，但具体后端、保留、迁移、租户权限与恢复合同要分别核验 |
| 质量/性能 | 原生生态更省适配不等于任务必然更准或更快，本次无横向实测 |

**工程判断：** Gemini 专属功能或 Google 生态优先时先试 ADK；需保留既有自有治理服务时可做外部 adapter。若只通过相同兼容网关运行文本工具会话，则要以实际缺失能力判断迁移价值。

<a id="c5"></a>

## C5 — CrewAI

CrewAI 以角色、目标、任务与工具组织 Agent，并提供委派、迭代限制和上下文管理配置。其代码执行可选择 Docker safe 模式；不能笼统说它没有隔离执行选项。[Agents](https://docs.crewai.com/v1.15.20/en/concepts/agents)

Flows 通过 start/listen/router 等结构组织状态，支持 `@persist`，默认持久后端为 SQLite，提供基于状态身份的恢复路径。因此“CrewAI 完全没有持久化”也是过时或不完整的比较。状态恢复不自动证明任意外部副作用恰好执行一次。[Flows](https://docs.crewai.com/v1.15.20/en/concepts/flows)

| 维度 | 与本项目的实质区别 |
| --- | --- |
| 抽象重心 | CrewAI 更便于表达角色与任务协作；Harness 强调 Capability 合同、层级权限和持久调用证据 |
| 编排 | Crew/Agent 与 Flow 是不同层；应拿具体流程对照 Harness Workflow/Graph，而不是只比较角色 Prompt |
| 安全边界 | Docker 代码执行与 Harness Windows Basic 是不同执行环境；角色和审批都不能替代沙箱保障核验 |
| 成本 | 委派、规划和更多迭代可能放大 token 与工具次数，需以任务完成成本评估 |
| 运维 | Harness 已有自己的多租户服务管理；采用 CrewAI 后可由宿主/对应平台补足，不是不可实现 |

**工程判断：** Python 团队快速验证角色协作时 CrewAI 更直接；固定受审批业务链路可继续使用 Harness。高层抽象越方便，越需要用真实轨迹证明工具调用次数、恢复和质量符合要求。

<a id="c6"></a>

## C6 — Eino

Eino 是最值得与本项目直接比较的 Go 候选之一：提供 ChatModel、Tool、Retriever、Embedding 等组件抽象及 Chain/Graph/Workflow 组合，重视类型、流式处理与 callbacks，上层另有 ADK Agent 能力。[官方 overview](https://www.cloudwego.io/docs/eino/overview/)

其图编排已有 checkpoint/interrupt 与状态恢复设计，包含嵌套/并行场景；中断后节点可能重新进入，仍需正确处理先前副作用。ADK 层也有 HITL 接口，不能说它只会内存函数组合。[Checkpoint and interrupt](https://www.cloudwego.io/docs/eino/core_modules/chain_and_graph_orchestration/checkpoint_interrupt/)、[ADK HITL](https://www.cloudwego.io/docs/eino/core_modules/eino_adk/agent_hitl/)

| 维度 | 与本项目的实质区别 |
| --- | --- |
| 通用组件 | Eino 面向模型/检索/工具与编排组件开发；Harness 目前提供的模型协议和检索后端较少 |
| 核心设计 | Eino 的 typed compose 与 Harness 的版本化 Capability/Scope 是不同重心，两者都可以保持 Go 接口扩展 |
| 图与恢复 | Eino 的通用图和中断 API 是成熟度评估重点；Harness 更强调自己 SQL 段租约、审批和调用 journal 的组合 |
| 服务治理 | Harness 当前仓库包含账号、配置、队列、发布和 Console；Eino 组件库不应单独承担整个 SaaS 比较口径 |
| 性能 | 同为 Go 不能默认性能相同，也不能默认 Harness 更快；组件流、状态复制、持久模式和具体任务才是关键 |

**工程判断：** 从零写 Go Agent/RAG/复杂流程，优先用 Eino 做原型；保留 Harness 的已有治理投资也合理。可以把专业编排作为远程能力接入，或在明确合同后做本地 adapter；不建议为了统一名字直接合并双方运行状态机。

<a id="c7"></a>

## C7 — LlamaIndex

LlamaIndex 不只是一个检索库，当前 Python 文档包含 FunctionAgent、ReActAgent、CodeActAgent 和多 Agent 的 AgentWorkflow。它可以参与完整 Agent 应用，对比时不应把 Agent 能力遗漏。[Agents](https://developers.llamaindex.ai/python/framework/module_guides/deploying/agents/)

VectorStoreIndex 文档涵盖文档/节点索引、ingestion pipeline 与向量存储接入。这是与本项目现有关键词 Index 最直接的功能差异；具体 connector 质量与规模仍应按实际后端验证。[VectorStoreIndex](https://developers.llamaindex.ai/python/framework/module_guides/indexing/vector_store_index/)

| 维度 | 与本项目的实质区别 |
| --- | --- |
| 知识链路 | LlamaIndex 的数据/索引组合比当前 Harness RAG 更完整；Harness 的 Index 主要提供受 scope 约束的注入边界 |
| Agent | 双方都有 Agent/工具执行，但 LlamaIndex 的数据工具与 AgentWorkflow 面向不同开发入口 |
| 治理 | Harness 的租户、审批、任务恢复可继续作为外部服务面；调用外部 RAG 服务时必须传递并核验授权范围 |
| 性能/质量 | ANN 延迟、召回与答案质量由索引、embedding、重排和数据分布共同决定，不能用框架名代替评测 |

**工程判断：** 如果主要开发工作是接文档、切块、索引和检索，优先复用它或相应 Go 生态；当前 Harness 不值得为了保持“全自研”而重复实现所有连接器。

## 按模块族归纳差异

这里的“优先候选”指减少特定场景的实现工作，不是每项性能分数。详细本地实现、扩展接口和证据见对应 M 模块。

| 模块族 | Harness 当前定位 | 外部候选更值得比较的部分 | 判断 |
| --- | --- | --- | --- |
| 循环、Profile、工具合同（M01–M06） | 受 scope 管理的组合与顺序循环 | LangChain、OpenAI、Eino 的开发入口 | 多租户治理选现有主线；快速接生态选成熟组件 |
| ModuleHost（M07） | 可选生命周期与效果日志 | 不直接等价于 handoff/工作流插件 | 有受治理动态部署需求才启用 |
| 模型/上下文（M08–M15） | 控制/执行分层，显式预算，有限协议 | 厂商 SDK 原生功能、Deep Agents 等上下文策略 | 兼容会话可沿用，专属功能先看原生生态 |
| 准入/审批/journal（M16–M18） | SQL 审批与调用证据组合 | LangGraph、OpenAI、Eino 暂停恢复 | 都有能力；精确比较副作用和授权合同 |
| 队列/多 Agent/图（M19–M23） | 服务队列、受控子委派、窄图适配 | 通用图、并行、HITL 与托管执行 | 复杂编排优先对照 LangGraph/Eino/ADK |
| 发现/远程工具（M24–M26） | 索引、stdio MCP、受约束 HTTP | 现成工具库与 MCP 多传输 | 当前边界够用则沿用，否则补 adapter |
| 代码/沙箱/Runner（M27–M31） | WASM、本机 provider、私有协议 | OpenAI Sandbox Agents 等现成云接入 | E2B 接入速度外部 SDK 有优势；保障另核验 |
| Memory/RAG（M32–M33） | 可扩展合同、关键词与 SQL | 向量、ingestion、数据连接器 | 知识链路优先复用成熟生态 |
| 存储/身份/设置（M34–M37） | 自托管服务管理 | SDK 宿主或托管平台的对应服务 | 比较完整部署方案，不能只比 SDK |
| 通知/评估/发布（M38–M40） | 审批通知与自有 release gate | SaaS connectors、实验与观测平台 | 现有治理可用，第三方工具按需接入 |
| API/Console/Telemetry（M41–M43） | 自有服务面、OTel | Agent Server/hosting/追踪产品 | 接入与运维需求决定，不做混层排名 |
| 质量与性能（M44） | 有当前正确性与局部性能证据 | 同任务、同环境试验 | 暂无全局优胜结论 |

## 如果要真正回答“哪个性能更好”

建议先选 Harness、一个同语言候选 Eino、一个图候选 LangGraph，按实际业务再加厂商 SDK。以下是后续评测设计，**本次未执行**：

1. 固定模型版本、endpoint、参数、Prompt、工具 Schema、检索内容和终止条件；模型存在随机性时重复任务并保留全部失败。
2. 同时测纯编排（确定性本地模型）和真实模型；前者看框架成本，后者看任务完成时间、成功率、tokens、费用及工具次数。
3. 持久方案保持可比：不能一边每工具同步写 PG，一边仅写内存，再用 QPS 排名；记录同步/异步/checkpoint 频率。
4. 固定 CPU/内存限制，独立服务进程与数据库，记录冷/热状态；报告 p50/p95/p99、错误、队列等待、GC、RSS 和长稳态。
5. 对审批等待、重启、强杀、工具已执行但结果未落库、租约丢失与重复回调做故障试验；单独验证权限与副作用次数。
6. 按任务成功且合同满足时的总开发/运维成本选型；即使局部微基准更快，也不能抵消连接器缺失或质量下降。

**对当前项目的投入建议：** 保留已有 Go 服务治理与小接口方向。[工具 Schema 预算漏算](performance/2026-09-06-context-budget-optimization.md)已修复并有真实 token 对照，接下来继续补[WASM 资源/终止配置](agent-module-assessment.md#m27)、更精确的协议成本模型和外层集成验证。复杂编排与 RAG 通过原型决定复用或自研，避免用一个总分掩盖各模块的不同成熟度。
