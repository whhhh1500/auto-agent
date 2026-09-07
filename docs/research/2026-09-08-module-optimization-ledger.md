# Module optimization ledger — 2026-09-08

## Scope and baseline

This is a research ledger, not an implementation plan or a performance claim.
It records the current source tree at `b752bbe9f38ae1ef69ee5e23824032271a5c3053`
(`feat(server): schedule native model evidence retention`). The worktree was
clean when the inventory was made. `go list ./...` on Go 1.25.13 found 90
current build targets. Platform-tagged sandbox helpers and Windows acceptance
commands are recorded below even when they are not targets on the host OS.

The Core boundary remains fixed at 34 production files, 8,817 non-blank lines,
and 910 public API items. This ledger does not authorize a Core or public API
change. All proposed checks use synthetic adapters, local SQL/SQLite fixtures,
or platform acceptance fixtures; none require a billable model provider.

Status meanings:

- **reviewed** — current code and a directly relevant primary source were read.
- **pending** — indexed, but no module-specific source comparison has been
  made in this first pass.
- **not adopted** — a source-informed idea was considered and deliberately
  excluded from the current architecture.

`docs/agent-module-assessment.md` is the stable M01–M44 module index. Package
paths may appear in more than one row where a boundary is intentionally shared.

## Module index

| ID | Current packages and code anchor | Current responsibility | Existing design / test entry | Research status |
| --- | --- | --- | --- | --- |
| M01 | `pkg/core`, `pkg/runtime`; `pkg/core/runtime.go` | Agent loop and runtime event transition contract | Assessment M01; `go test ./pkg/core ./pkg/runtime` | pending |
| M02 | `pkg/core`; `scope*.go`, `principal*.go` | Scope, principal and ownership | Assessment M02; `go test ./pkg/core` | pending |
| M03 | `pkg/core`; `capability*.go` | Capability registry, manifests and immutable snapshots | Assessment M03; `go test ./pkg/core` | pending |
| M04 | `pkg/core`; `profile*.go` | Profile and prompt layering | Assessment M04; `go test ./pkg/core` | pending |
| M05 | `pkg/core`, `pkg/adapter/coreplugin` | PluginHost compatibility boundary | Assessment M05; `go test ./pkg/core ./pkg/adapter/coreplugin` | pending |
| M06 | `pkg/app/capabilityruntime`, `pkg/runtime` | Dynamic capability factory declarations | Assessment M06; `go test ./pkg/app/capabilityruntime ./pkg/runtime` | pending |
| M07 | `pkg/storage/{sql_completed_tool_result_fence,sql_native_queued_tool_effect_witness,tool_journal,write_behind}.go`, `pkg/adapter/sql/{effectjournal,fencejournal}` | Module lifecycle, effects and fences | Assessment M07; `go test ./pkg/core ./pkg/storage ./pkg/adapter/sql/effectjournal ./pkg/adapter/sql/fencejournal` | reviewed; generic retry not adopted |
| M08 | `pkg/app/{modelcatalog,modelcontrol}`, `pkg/adapter/modelruntime` | Catalog, plans, plugin selection and compatibility | Assessment M08; `go test ./pkg/app/modelcatalog ./pkg/app/modelcontrol ./pkg/adapter/modelruntime` | pending |
| M09 | `pkg/app/modelexecution/{contract,stream,registry}.go`; `pkg/adapter/modelexecution/{openai,anthropic,corebridge}` | Provider/protocol execution and normalized streaming | Assessment M09; `go test ./pkg/app/modelexecution ./pkg/adapter/modelexecution/...` | reviewed |
| M10 | `pkg/app/{modelsettings,secretview}`, `pkg/adapter/modelsettings`, `pkg/adapter/sql/settings` | Model settings, credential material and cache resolution | Assessment M10; `go test ./pkg/app/modelsettings ./pkg/app/secretview ./pkg/adapter/modelsettings ./pkg/adapter/sql/settings` | pending |
| M11 | `pkg/server/server_native_model_checkpoint.go`, `pkg/storage/sql_native_queued_model_*.go` | Model call gate, durable admission and outcome evidence | Assessment M11; `go test ./pkg/server ./pkg/storage` | reviewed |
| M12 | `pkg/runtime`, `pkg/control` | Fast routing and release-aware selection | Assessment M12; `go test ./pkg/runtime ./pkg/control` | pending |
| M13 | `pkg/core/session*.go`, `pkg/storage/sql_session*.go` | Session events and message projection | Assessment M13; `go test ./pkg/core ./pkg/storage` | pending |
| M14 | `pkg/app/contextassembly`; `assembler.go`, `context_budget.go` | Context assembly, budgets and recent compression | Assessment M14; `go test ./pkg/app/contextassembly`; implementation/performance context-budget docs | reviewed; estimator candidate is contract-gated |
| M15 | `pkg/core/context_summary.go`, `pkg/app/contextassembly/{extractive_summarizer,summary_transcript,summary_accounting}.go`, `pkg/server/server_summary_accounting*.go` | Summary accounting and long context policy | Assessment M15; `go test ./pkg/app/contextassembly ./pkg/server` | reviewed; retain current safeguards |
| M16 | `pkg/core/policy*.go`, `pkg/server/server_tool_checkpoint.go` | Policy, hooks, schemas, limits and pre-tool protection | Assessment M16; `go test ./pkg/core ./pkg/server` | reviewed |
| M17 | `pkg/storage/sql_approval*.go`, `pkg/server/server_run_worker.go`, `pkg/adapter/graphapproval` | Human approval, durable pause and resume | Assessment M17; `go test ./pkg/storage ./pkg/server ./pkg/adapter/graphapproval` | pending |
| M18 | `pkg/core/tool_invocation*.go`, `pkg/storage/sql_tool*.go` | Tool journal and unknown-result handling | Assessment M18; `go test ./pkg/core ./pkg/storage` | reviewed |
| M19 | `pkg/storage/{run_control,sql_run_control}.go`, `pkg/server/server_run_worker.go`, `pkg/app/runliveness` | Async queue, leases, workers and liveness | Assessment M19; `go test ./pkg/storage ./pkg/server ./pkg/app/runliveness` | reviewed; jitter and fairness changes not adopted |
| M20 | `pkg/extensions/subagent`, `pkg/server/dynamic_capability*.go` | Subagent and durable delegation | Assessment M20; `go test ./pkg/extensions/subagent ./pkg/server` | pending |
| M21 | `pkg/extensions/workflow` | Deterministic workflow capability | Assessment M21; `go test ./pkg/extensions/workflow` | pending |
| M22 | `pkg/app/runexecutor`, `pkg/adapter/runexecutor/graph`, `pkg/execution/graph` | Executor selection and graph execution | Assessment M22; `go test ./pkg/app/runexecutor ./pkg/adapter/runexecutor/graph ./pkg/execution/graph` | pending |
| M23 | `pkg/extensions/graph`, `pkg/adapter/{memory,sql}/graphcheckpoint`, `pkg/adapter/sql/graphsegment` | Graph checkpoints, history and segment authorization | Assessment M23; `go test ./pkg/extensions/graph ./pkg/adapter/memory/graphcheckpoint ./pkg/adapter/sql/graphcheckpoint ./pkg/adapter/sql/graphsegment` | pending |
| M24 | `pkg/extensions/toollib`, `pkg/execution/mcp_*.go` | Tool library, search and progressive disclosure | Assessment M24; `go test ./pkg/extensions/toollib ./pkg/execution` | reviewed |
| M25 | `pkg/execution/mcp_*.go`, `docs/mcp.md` | MCP process contract, payload limits and observation | Assessment M25; `go test ./pkg/execution` | reviewed |
| M26 | `pkg/execution/http*.go` | HTTP capability execution and SSRF boundary | Assessment M26; `go test ./pkg/execution` | pending |
| M27 | `pkg/execution/{wazero*,wasm*}.go`, `internal/testwasm` | WASM execution and limits | Assessment M27; `go test ./pkg/execution`; wasm-resource implementation/verification docs | reviewed |
| M28 | `pkg/execution/sandbox`, `pkg/adapter/sandboxexec`, `pkg/adapter/httpapi/sandbox` | Sandbox contracts, registry and admission adapter | Assessment M28; `go test ./pkg/execution/sandbox ./pkg/adapter/sandboxexec ./pkg/adapter/httpapi/sandbox` | reviewed |
| M29 | `pkg/execution/sandbox/windows_*`, `internal/sandbox*`, `cmd/windows-*` | Windows current-user Basic isolation | Assessment M29; platform acceptance commands and Windows-tag tests | reviewed |
| M30 | `pkg/execution/{bwrap_linux,bwrap_other}.go` | Linux local execution and E2B gap | Assessment M30; `go test ./pkg/execution` | pending |
| M31 | `pkg/extensions/runner`, `pkg/adapter/httpapi/runner`, `docs/runner.md` | Private runner and remote worker protocol | Assessment M31; `go test ./pkg/extensions/runner ./pkg/adapter/httpapi/runner` | pending |
| M32 | `pkg/extensions/memory/memory.go`, `pkg/storage/sql_memory_rag.go` | Long-term memory | Assessment M32; `go test ./pkg/extensions/memory ./pkg/storage` | reviewed; retain deterministic bounded recall |
| M33 | `pkg/extensions/rag/rag.go`, `pkg/storage/sql_memory_rag.go` | RAG retrieval | Assessment M33; `go test ./pkg/extensions/rag ./pkg/storage` | reviewed; offline quality measurement pending |
| M34 | `pkg/storage/sql_native_queued_model_retention.go`, `pkg/storage/sql_schema.go`, `pkg/adapter/sql/sqlkit` | Session persistence, SQL and event queries | Assessment M34; `go test ./pkg/storage ./pkg/adapter/sql/sqlkit` | reviewed; retention sub-batching/index change not adopted |
| M35 | `pkg/app/artifactmigration`, `pkg/adapter/{sql,storage}/artifactmigration`, `pkg/storage` | Artifacts, object streaming and migration | Assessment M35; `go test ./pkg/app/artifactmigration ./pkg/adapter/sql/artifactmigration ./pkg/adapter/storage/artifactmigration ./pkg/storage` | pending |
| M36 | `pkg/app/identity`, `pkg/adapter/httpapi/{auth,accountadmin}`, `pkg/server` | Identity, accounts, auth and admin authorization | Assessment M36; `go test ./pkg/app/identity ./pkg/adapter/httpapi/auth ./pkg/adapter/httpapi/accountadmin ./pkg/server` | pending |
| M37 | `pkg/app/{settings,storageconfig}`, `pkg/adapter/httpapi/{settings,storage}`, `pkg/adapter/sql/settings`, `pkg/adapter/storageconfig` | Settings, storage config and secret view | Assessment M37; `go test ./pkg/app/settings ./pkg/app/storageconfig ./pkg/adapter/httpapi/settings ./pkg/adapter/httpapi/storage ./pkg/adapter/sql/settings ./pkg/adapter/storageconfig` | pending |
| M38 | `pkg/app/notification`, `pkg/adapter/notification/{coretool,webhook,webhook/targetresolver}`, `pkg/adapter/{httpapi,sql}/notificationtarget` | Notification channels and targets | Assessment M38; `go test ./pkg/app/notification ./pkg/adapter/notification/... ./pkg/adapter/httpapi/notificationtarget ./pkg/adapter/sql/notificationtarget` | pending |
| M39 | `pkg/evaluation` | Evaluation datasets and regression gate | Assessment M39; `go test ./pkg/evaluation` | pending |
| M40 | `pkg/control`, `pkg/server/server_telemetry*.go` | Profile release, canary and rollback | Assessment M40; `go test ./pkg/control ./pkg/server` | pending |
| M41 | `pkg/server`, `pkg/adapter/httpapi/{jsonbody,modelsettings,runner,sandbox}`, `openapi/harness-core-v1.yaml` | HTTP API, JSON contract and SSE | Assessment M41; `go test ./pkg/server ./pkg/adapter/httpapi/...`; `go run ./scripts/verify-openapi ...` | pending |
| M42 | `pkg/console`, `pkg/console/static` | Embedded console | Assessment M42; `go test ./pkg/console` | pending |
| M43 | `pkg/telemetry/otel`, `pkg/logging`, `pkg/buildinfo` | Telemetry, logging and build information | Assessment M43; `go test ./pkg/telemetry/otel ./pkg/logging ./pkg/buildinfo` | pending |
| M44 | `internal/modulecheck`, `internal/perfp0`, `cmd/perf-p0`, `scripts/{test-postgres,verify-openapi}` | Tests, architecture guard and performance tooling | Assessment M44; `go test ./internal/modulecheck ./internal/perfp0 ./scripts/...` | pending |

Additional current build targets not naturally owned by a single M row are
`cmd/{demo,perf-p0,server,starter-server}`, `examples/{crypto,graph-review,runner-worker,starter}`, `internal/{calc,testdb,testwasm}`, `pkg/integration`,
`pkg/buildinfo`, `pkg/logging`, and `scripts`; they are covered by the entry,
operations, testing, and corresponding feature rows above. This prevents the
M01–M44 index from silently excluding the 90-target build graph.

## Sources read in this pass

| Source | Date / access | Applied to | Constraint carried into this ledger |
| --- | --- | --- | --- |
| [tau2-bench](https://arxiv.org/html/2506.07982v1), sections 3.2–3.3 | 2025-06; accessed 2026-09-08 | M09, M16, M18, M24–M31, M39 | Model a deterministic task as initialization, allowed solution actions, and final-state assertions. Do not import its domains, simulator, dataset, or reported scores. |
| [AgentDojo](https://arxiv.org/html/2406.13352v3) | 2024-06; accessed 2026-09-08 | M16, M18, M24–M29 | Security evaluation must record both benign task completion and adversarial effect prevention; deny-all is not a safety improvement. Do not import its framework or dataset. |
| [Anthropic: Building effective agents](https://www.anthropic.com/engineering/building-effective-agents) | 2024-12; accessed 2026-09-08 | M09, M20–M25 | Prefer simple composable workflows, observable environment feedback, clear tool interfaces, and stopping conditions; add complexity only with measured benefit. |
| [Anthropic: Writing effective tools for agents](https://www.anthropic.com/engineering/writing-tools-for-agents) | 2025; accessed 2026-09-08 | M09, M16, M24–M25 | Tool descriptions and schemas are an agent-computer interface that need scenario-based evaluation, not only transport correctness. |
| [OpenAI: A practical guide to building agents](https://openai.com/business/guides-and-resources/a-practical-guide-to-building-ai-agents/) | 2025; accessed 2026-09-08 | M16–M18, M24–M31 | Classify tool risk by reversibility, permissions and financial impact; require human intervention for sensitive actions. This supports existing approvals and policy gates, rather than replacing them. |
| [LongRAG](https://arxiv.org/abs/2406.15319) | 2024-06; accessed 2026-09-08 | M14–M15, M33 | Long-context retrieval/chunking results are background evidence only. They do not establish that this repository should change its provider-independent context or summary policy. |
| [A-MEM](https://arxiv.org/abs/2502.12110) | submitted 2025-02; NeurIPS 2025 version accessed 2026-09-08 | M32 | Dynamic graph memory is a research comparison, not a reason to replace the bounded, scope-exact memory contract without a measured harness defect. |
| [RAGChecker](https://arxiv.org/abs/2408.08067) | submitted 2024-08; accessed 2026-09-08 | M33, M39 | Diagnostic retrieval metrics motivate an offline labeled corpus before any retrieval implementation change. |
| [RankRAG](https://arxiv.org/abs/2407.02485) | submitted 2024-07; accessed 2026-09-08 | M33 | Learned ranking performance does not justify adding a model, billing path, or new authority surface to deterministic scoped retrieval. |
| [Temporal Workflow Execution](https://docs.temporal.io/workflow-execution) | current documentation, copyright 2026; accessed 2026-09-08 | M07, M11, M18 | Event-history-checked replay is a useful recovery principle; it does not authorize provider/tool replay outside this repository's exact evidence contracts. |
| [AWS Well-Architected REL05-BP03](https://docs.aws.amazon.com/wellarchitected/2023-04-10/framework/rel_mitigate_interaction_failure_limit_retries.html) | updated 2023-07; accessed 2026-09-08 | M18–M19 | Retry only operations known to be retryable and idempotent. This is a foundation, not 2024–2026 frontier research. |
| [AWS: Exponential Backoff and Jitter](https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/) | published 2015-03, updated 2023-05; accessed 2026-09-08 | M19 | Jitter is a comparison baseline only. It is foundational material, not frontier research, and the local default-parameter experiment rejects adoption. |
| [Aequitas admission control](https://research.google/pubs/aequitas-admission-control-for-latency-critical-rpcs-in-datacenters/) | SIGCOMM 2022; accessed 2026-09-08 | M19 | Fairness requires scheduling state and goals outside the present queue contract. It is a comparison, not a proposal to claim tenant fairness. |
| [PostgreSQL: Routine Vacuuming](https://www.postgresql.org/docs/current/routine-vacuuming.html) | current PostgreSQL 18 documentation; accessed 2026-09-08 | M34 | `DELETE` creates MVCC dead rows and fixed maintenance schedules have I/O trade-offs. This is current official documentation, not a research paper. |
| [Anthropic: Demystifying evals for AI agents](https://www.anthropic.com/engineering/demystifying-evals-for-ai-agents) | 2026-01-09; accessed 2026-09-08 | M33, M39, M44 | Separate diagnostic capability-quality measurements from near-perfect regression checks; both may examine final environment state and transcripts, with a code grader chosen only where its limits fit the assertion. M39/M44 remain pending until their code is reviewed. |

## Reviewed-module source mapping and candidate status

This section is deliberately more specific than the module table: every
`reviewed` row names the source, current code evidence, and the implementation
status. A reviewed source comparison is not an approval to change code.

| Module | Current code evidence | Source correspondence | Candidate and status | Explicit boundary |
| --- | --- | --- | --- | --- |
| M09 model adapters | `pkg/app/modelexecution/stream.go` rejects post-finish events, duplicate usage, conflicting tool fragments, oversized arguments and invalid finish/tool combinations; `registry.go` applies it before emitting normalized events. | tau2-bench's init/solution/assertion pattern and Anthropic's tool-interface guidance support scenario-level contract tests. | R-01, **pending design review**: synthetic cross-adapter normalized-stream scenarios. | No provider call, no new adapter framework, and no production behavior change. |
| M11 durable model outcomes | `pkg/server/server_native_model_checkpoint.go` and `pkg/storage/sql_native_queued_model_*.go` fence admission and persist outcome evidence. | tau2-bench supports final durable-state assertions; it is a test-design input only. | **pending measurement**: include durable admission/outcome/retry state in existing deterministic recovery cases if an uncovered sequence is identified. | Do not import the benchmark, change recovery protocol, or consume Core/API budget. |
| M14 context assembly | `assembler.go:40` injects `ContextEstimator`; `assembler.go:68` combines context window, max output and safety margin; `context_budget.go:171,178,358` keeps tool call/result groups atomic and fails a required-group budget closed. `ConservativeEstimator` at `assembler.go:199` intentionally uses an upper-bound UTF-8-byte model. | LongRAG is only background. It does not prove a provider-independent tokenizer estimate or a change to the current framing contract. | **pending provider-contract research**: an app-layer estimator for one named protocol may be evaluated only after that provider's serialized-input/framing contract establishes a conservative upper bound. | Golden CJK/emoji/tool-schema tests alone cannot prove `estimate >= actual` for every provider protocol or billing rule. Keep the byte upper bound as default; no Core tokenizer dependency. |
| M15 summary and long context | `pkg/core/context_summary.go` exposes only the `RunSummarizer` contract; `extractive_summarizer.go:71` is one-pass and bounded; `summary_transcript.go:30` bounds LLM transcript input. Tool call/result pairing and summary accounting have focused tests. | LongRAG reports long-context retrieval/chunk choices, not a correctness proof for summary replacement. | **not adopted**: no provider-summary switch, expanded transcript, or altered tool-pair rule is proposed. Current safeguards remain the reviewed choice. | Preserve bounded, deterministic extractive fallback and usage accounting. |
| M16 policy and pre-tool protection | `pkg/core/policy*.go` resolves capability/policy; `pkg/server/server_tool_checkpoint.go` records effect admission before non-approval effects. | AgentDojo requires utility and attack resistance to be evaluated together; OpenAI's guide supports proportionate approval for sensitive actions. | R-02, **pending design review**: paired benign/adversarial deterministic tool-output contracts. | Do not add an LLM classifier or make the policy path depend on billable inference. |
| M18 tool journal | `pkg/core/tool_invocation*.go` and `pkg/storage/sql_tool*.go` retain invocation state and unknown-result evidence. | tau2-bench supports final-state assertions; AgentDojo supports paired safety/utility cases. | R-02, **pending design review**: journal effects become the durable assertion surface for local injection fixtures. | No fake success result and no change to effect/fence semantics. |
| M24 tool library | `pkg/extensions/toollib` supplies discovery and progressive disclosure over the capability surface. | Anthropic's tool-interface guidance and AgentDojo's evaluation shape. | R-02, **pending design review**: safe/benign fixture pairs using the existing discovery surface. | Do not import AgentDojo tool domains/framework or modify tool-library runtime behavior. |
| M25 MCP | `pkg/execution/mcp_*.go` bounds MCP payload and process behavior. | Anthropic's tool-interface guidance and AgentDojo's paired-evaluation shape. | R-02, **pending design review**: optional local MCP payload fixture under existing limits. | Preserve MCP limits and process contracts; no new MCP framework. |
| M27 WASM | `pkg/execution/{wazero*,wasm*}.go` and `internal/testwasm` enforce local execution/resource behavior. | AgentDojo motivates adversarial tool-output cases, not a new WASM execution policy. | R-02 has optional synthetic capability cases, **pending design review**. | No resource-limit relaxation or model-derived execution decision. |
| M28 sandbox contracts | `pkg/execution/sandbox`, `pkg/adapter/sandboxexec`, and `pkg/adapter/httpapi/sandbox` enforce assurance, network and resource declarations. | AgentDojo's test shape supports checking enforcement without equating it with model robustness. | R-02 has optional synthetic network-denial and approval-required cases, **pending design review**. | No assurance downgrade, host-network equivalence claim, or generic fallback. |
| M29 Windows Basic sandbox | `pkg/execution/sandbox/windows_*`, `internal/sandbox*` and `cmd/windows-*` contain platform-specific current-user isolation evidence. | AgentDojo does not establish a Windows isolation claim; it only motivates adversarial input coverage. | **not adopted**: no general sandbox integration follows from this source comparison. | Platform acceptance remains the only basis for a Windows assurance claim. |
| M32 long-term memory | `pkg/extensions/memory/memory.go:30` caps scope entries at 1024 and `:184` performs exact-scope recall; `pkg/storage/sql_memory_rag.go:222-257` uses lower-case substring/tags and stable recency/id ordering. | A-MEM's dynamic memory graph is a useful contrast, but it changes the repository's bounded deterministic scope/upsert contract. | **not adopted**: do not add graph memory absent a measured recall defect and explicit new contract. | Keep exact scope, bounded entries and deterministic ordering; do not claim semantic relevance ranking exists today. |
| M33 RAG retrieval | `pkg/extensions/rag/rag.go:113-122` tokenizes deterministically; `:228` exposes the reference ranker. `pkg/storage/sql_memory_rag.go:381-481` applies scope prefixes/default top-K and stable unique-overlap ranking; parity is covered in `pkg/storage/infra_test.go:309,343`. Test-only baseline is `pkg/storage/sql_rag_quality_baseline_test.go` with `pkg/storage/testdata/rag_quality_baseline.json`. | RAGChecker motivates diagnostics; RankRAG demonstrates learned ranking, which is unsuitable without a measured gap and authority/cost review. Anthropic's 2026 evaluation guidance distinguishes this diagnostic quality corpus from an isolated near-perfect regression gate. | **small fixed regression baseline; diagnostic measurements, not a production-quality threshold or improvement**. SQLite and a real PostgreSQL rerun of `TestPostgresSQLRagQualityBaseline` both passed (native exit 0) with micro Recall@5 `5/7`, micro precision@5 `5/10`, macro MRR@5 `0.500`, and expected misses `semantic-billing-date`, `cjk-receipt`. Recall is relevant returned / all qrels, precision is relevant returned / all returned top-five chunks, and macro MRR assigns zero when no relevant result is retrieved. The hand-authored qrels exercise inheritance/sibling isolation, any-tag behavior, a long lexical distractor, semantic miss, and CJK punctuation miss; they are not representative of production traffic. The first PostgreSQL attempt found missing `rag_documents` schema setup in the test harness; the harness initialization was corrected and the real rerun passed. This was not a production RAG defect. A private deterministic lexical second stage remains **pending** and needs a separately measured acceptance criterion. | Do not add an LLM reranker, dense index, fusion framework, scope/tag leakage or cross-database nondeterminism. |
| M07 lifecycle/effects/fences | `sql_completed_tool_result_fence.go`, `sql_native_queued_tool_effect_witness.go:81` and `tool_journal.go` lock and revalidate queue/session/lease generation. `write_behind.go:17-31,185-234` retains the first append failure terminally. | Temporal replay checks commands against history, but v43/v44 plus journal and fenced append form this repository's stricter per-effect proof. | **not adopted**: no generic `WriteBehind` retry. It could obscure an atomic v45/v46 prefix/outcome failure. | No schema/Core/API change; failure stays terminal rather than becoming an implicit provider/tool retry. |
| M11 durable model outcomes, recovery detail | `sql_native_queued_model_invocation.go:66-72` records v45 admission; `sql_native_queued_model_outcome.go:45-65` writes atomic v46 plus suffix; `:316-411` derives a digest from admission or a legal persisted chunk boundary, never an arbitrary intra-chunk suffix. | Temporal supports history-bound recovery but does not justify model provider replay. | **not adopted**: generic auto-resume/retry; unknown v45 remains fail-closed. | Keep sequential, current-authority re-admission and exact identity proof. |
| M18 tool journal, recovery detail | `tool_journal.go` and `sql_native_queued_completed_tool_recovery.go:29-152` perform A atomic delivery or B exact sidecar readback, each with current epoch/fence and v44/journal proof. | Temporal history and AWS's retry-only-when-idempotent principle. | **not adopted**: recovery for partial streams, multiple pending tools, or generic V3-only history. | Unknown results must never provoke an external tool/provider replay. |
| M19 queue, leases and liveness | `run_control.go:159-190` implements FIFO plus PostgreSQL `SKIP LOCKED`; `:487-549` increments attempt/generation; `:602-636` fences retry. `server_run_worker.go:232-293` starts empty polling at 250 ms and backs idle polling off to 1 s; `:631-678` uses fixed exponential retry. Defaults are `DefaultRunMaxAttempts=3` at `run_control.go:24`. | AWS jitter is foundational comparison material; Aequitas shows that fairness requires explicit scheduling state and policy. Neither is a claim of current frontier evidence. | **rejected after local experiment**: equal jitter. The fixed-seed, aligned-poll model at the default 250 ms interval leaves 1,000 workers in one observed bucket on attempt 1 for both fixed and equal jitter; on attempt 2, fixed/equal maximum buckets are 1,000/995. With default max attempts 3, only attempts 1–2 retry. **not adopted**: cross-tenant fairness, which requires durable virtual-time/deficit/weight state absent from the contract. | The model uses seed `20260908`, integer-ms due times and common poll phase; it excludes SQL contention, claim service, leases, provider calls and throughput. A lower simulated mean scheduled delay is not a throughput improvement. The real empty-idle poll can back off to 1 s, which this 250 ms aligned upper-bound check does not model. Raw script/artifact remain outside the repository. |
| M34 SQL/session retention | `sql_native_queued_model_retention.go:14-82` bounds candidates to 8192 and uses one transaction; `:85-181` locks/rebuilds exact history, validates v45/v46 plus digest, and deletes only that pair. `server_native_queued_retention.go:21-46` enables it only on a sealed Native-static worker loop. v43/v44/journal and v45 without v46 remain intact. | PostgreSQL vacuuming documentation explains the MVCC cost after deletion, without prescribing this application transaction shape. | **not adopted this round**: retention sub-batching or an additional created-at index. Both alter partial-progress/error semantics or schema, beyond scope. | Preserve bounded loop and proof-based pruning; no generic retention behavior change. |

## First-pass, source-informed candidates

### R-01 — adapter contract scenarios, test-only

**Current evidence.** `pkg/app/modelexecution/stream.go` already rejects
post-finish events, duplicate usage, conflicting tool fragments, over-limit
arguments, and invalid finish/tool combinations. `registry.go` applies that
validator to every protocol event. The OpenAI, Anthropic and core bridge
adapters each have their own decoder tests, but there is no named cross-adapter
scenario table that expresses the same tool trajectory and expected normalized
event/final state for every built-in protocol.

**Candidate.** Add a package-level test scenario table, not a new runtime
abstraction: seed a synthetic provider transcript, execute each built-in
protocol through the existing registry, and assert normalized text/tool-call
ordering, exact usage handling, terminal reason, and the durable session effect
after a local executor consumes it. Structure each scenario as `init`,
`solution`, `assert`, following the useful *test construction* idea from
tau2-bench. Include malformed-stream negatives beside valid transcript cases.

**Why it is verifiable.** It runs with `httptest`/synthetic providers and local
fixtures, has no provider billing, and can be accepted only if all existing
adapters preserve their individual tests plus the shared outcomes.

**Status.** **pending design review**; no code change proposed in this ledger.

### R-02 — paired tool-output security contracts, test-only first

**Current evidence.** `pkg/core` resolves capabilities and policies;
`pkg/server/server_tool_checkpoint.go` records an effect admission before a
non-approval tool effect; `pkg/core`/`pkg/storage` preserve invocation state;
`pkg/execution/mcp_*.go` and `pkg/extensions/toollib` expose external tool
content and progressive disclosure; `pkg/execution/sandbox` and
`pkg/adapter/sandboxexec` enforce explicit assurance, network and resource
contracts. These are strong enforcement seams, but the current suite has no
small, shared, named corpus of untrusted tool-result prompt-injection cases
whose success criterion includes both a useful benign operation and the absence
of an unauthorized irreversible effect.

**Candidate.** Add a local deterministic fixture corpus at the server/execution
test boundary: initialize a benign requested action and an untrusted tool
payload, permit an authorized safe/read action, then assert final durable state
and journal effects. Pair each injection case with a benign counterpart using
the same capability surface. Include approval-required and sandbox network
denial cases. This mirrors AgentDojo's paired utility/security measurement
without importing its business domains or benchmark score.

**Why it is verifiable.** Local capabilities count effects; session/journal
records prove no hidden replay; sandbox probes are synthetic. A candidate fails
if it blocks the benign counterpart, so it cannot improve a headline security
number by becoming deny-all.

**Limit of the test.** A scripted local model can only establish that the
deterministic capability, approval, journal and sandbox enforcement seams make
the expected decision for a supplied payload. It cannot measure a model's
ability to recognize or resist prompt injection. Any future model-behaviour
claim needs a separately designed, provider-aware evaluation and must not be
derived from this deterministic gate.

**Status.** **pending design review**; no production policy, classifier, taint
model, or Core API change is proposed.

## Explicit non-adoptions in this pass

1. Do not import tau2-bench or AgentDojo as a production dependency, business
domain, task dataset, or score target. Their useful contribution here is a
test-design discipline, not an architectural dependency.
2. Do not add an LLM-based prompt-injection classifier into the tool path. It
would add latency, nondeterminism and a billing dependency at an authorization
boundary; existing deterministic policy, approval, capability and sandbox
contracts remain the enforcement point.
3. Do not weaken sandbox assurance to claim isolation when a provider reports
only process/shared-kernel or host networking. The current explicit assurance
and requested-network contract is the correct fail-closed boundary; new
providers need platform-specific evidence, not a generic fallback.
4. Do not create a cross-provider runtime adapter framework before a failing
shared scenario demonstrates an incompatibility. The current normalized
`modelexecution` contract and small built-in adapters preserve the open-source
public surface better.

## Next review sequence

Continue source comparison one M row at a time, preserve this ledger's
`pending` labels until code and a source are actually reviewed, and add a dated
measurement before changing a reviewed status to verified. Any future
implementation proposal must name its package owner, test command, expected
effect/failure boundary, and whether it consumes a Core/API budget.
