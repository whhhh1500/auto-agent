package extensions_test

import (
	"context"
	"encoding/json"
	"fmt"
	. "github.com/cc-auto-agent/harness-core/pkg/core"
	"github.com/cc-auto-agent/harness-core/pkg/extensions/memory"
	"github.com/cc-auto-agent/harness-core/pkg/extensions/rag"
	"github.com/cc-auto-agent/harness-core/pkg/extensions/workflow"
	"strings"
	"testing"
)

func testScopes() (ScopePath, ScopePath, ScopePath, ScopePath) {
	global := MustScopePath(ScopeRef{Kind: ScopeGlobal, ID: "global"})
	product, _ := global.Child(ScopeRef{Kind: ScopeProduct, ID: "product"})
	tenant, _ := product.Child(ScopeRef{Kind: ScopeTenant, ID: "tenant-a"})
	user, _ := tenant.Child(ScopeRef{Kind: ScopeUser, ID: "user-a"})
	return global, product, tenant, user
}

func testPrincipal(user ScopePath) Principal {
	return Principal{SubjectID: "user-a", TenantID: "tenant-a", Scope: user,
		Grants: NewPermissionSet(PermRead, PermWrite)}
}

func mustUserScope(t *testing.T) ScopePath {
	t.Helper()
	_, _, _, user := testScopes()
	return user
}

type staticTool struct {
	manifest CapabilityManifest
	content  string
}

func (t staticTool) Manifest() CapabilityManifest { return t.manifest }
func (t staticTool) Execute(context.Context, CapabilityRequest) (CapabilityResult, error) {
	return CapabilityResult{Content: t.content, OK: true}, nil
}

type directInvoker map[string]ToolProvider

func (i directInvoker) InvokeTool(ctx context.Context, call ToolCall) (CapabilityResult, error) {
	provider, ok := i[call.Name]
	if !ok {
		return CapabilityResult{Content: "capability not found", OK: false}, nil
	}
	return provider.Execute(ctx, CapabilityRequest{CallID: call.ID, Args: call.Args})
}

func toolManifest(id, version string) CapabilityManifest {
	return CapabilityManifest{ID: id, Version: version, Name: id, Kind: KindTool,
		Contract: "harness.tool/v1", RequiredPermissions: []Permission{PermRead}, Tool: &ToolExposure{}}
}

func objectSchema(properties map[string]any) map[string]any {
	return map[string]any{"type": "object", "properties": properties}
}

func jsonResult(value any) (CapabilityResult, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return CapabilityResult{}, err
	}
	return CapabilityResult{Content: string(encoded), OK: true}, nil
}

// --- Workflow ---

func TestWorkflowRunsStepsWithReferences(t *testing.T) {
	discover := staticTool{manifest: toolManifest("crypto.token.discover", "1.0.0"), content: `{"tokens":[{"symbol":"NOVA"}]}`}
	analyze := &echoCapability{manifest: toolManifest("crypto.token.analyze", "1.0.0")}

	flow, err := workflow.NewCapability("crypto.flow.research", workflow.Definition{
		Description: "Discover new tokens then analyze the first one.",
		InputSchema: objectSchema(map[string]any{}),
		Steps: []workflow.Step{
			{Name: "discover", CapabilityID: discover.Manifest().ID},
			{Name: "analyze", CapabilityID: analyze.Manifest().ID, Args: map[string]any{
				"symbol": "$ref:discover.data.tokens.0.symbol",
			}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	principal := testPrincipal(mustUserScope(t))
	result, err := flow.Execute(context.Background(), CapabilityRequest{
		CallID: "wf-1",
		Args:   map[string]any{},
		Context: CapabilityContext{Principal: principal, Scope: principal.Scope, Invoker: directInvoker{
			discover.Manifest().ID: discover, analyze.Manifest().ID: analyze,
		}},
	})
	if err != nil || !result.OK {
		t.Fatalf("workflow failed: %#v %v", result, err)
	}
	if !strings.Contains(result.Content, "NOVA") {
		t.Fatalf("analysis did not see the referenced symbol: %q", result.Content)
	}
	steps := result.Metadata["workflow.steps"].([]map[string]any)
	if len(steps) != 2 || steps[0]["name"] != "discover" || steps[1]["name"] != "analyze" {
		t.Fatalf("unexpected step outcomes: %#v", steps)
	}
}

type echoCapability struct{ manifest CapabilityManifest }

func (e *echoCapability) Manifest() CapabilityManifest { return e.manifest }

func (e *echoCapability) Execute(_ context.Context, request CapabilityRequest) (CapabilityResult, error) {
	return jsonResult(request.Args)
}

func TestWorkflowFailsFastAndReportsMissingRef(t *testing.T) {
	failing := &failingCapability{manifest: toolManifest("x.fail", "1.0.0")}
	flow, err := workflow.NewCapability("x.flow", workflow.Definition{
		Steps: []workflow.Step{
			{Name: "boom", CapabilityID: failing.Manifest().ID},
			{Name: "never", CapabilityID: failing.Manifest().ID},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	principal := testPrincipal(mustUserScope(t))
	result, _ := flow.Execute(context.Background(), CapabilityRequest{
		CallID: "wf-2", Context: CapabilityContext{
			Principal: principal, Scope: principal.Scope,
			Invoker: directInvoker{failing.Manifest().ID: failing},
		},
	})
	if result.OK || result.Metadata["workflow.failed_step"] != "boom" {
		t.Fatalf("workflow must fail fast on the failing step: %#v", result)
	}

	broken, err := workflow.NewCapability("x.flow2", workflow.Definition{
		Steps: []workflow.Step{{Name: "a", CapabilityID: failing.Manifest().ID, Args: map[string]any{
			"symbol": "$ref:input.missing.key",
		}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	result, _ = broken.Execute(context.Background(), CapabilityRequest{
		CallID: "wf-3", Args: map[string]any{}, Context: CapabilityContext{
			Principal: principal, Scope: principal.Scope,
			Invoker: directInvoker{failing.Manifest().ID: failing},
		},
	})
	if result.OK || result.Metadata["code"] != "workflow_ref_missing" {
		t.Fatalf("missing reference must deny with a stable code: %#v", result)
	}
}

func TestWorkflowRejectsBadDefinitions(t *testing.T) {
	if _, err := workflow.NewCapability("no.steps", workflow.Definition{}); err == nil {
		t.Fatal("empty workflow must be rejected")
	}
	if _, err := workflow.NewCapability("dup.names", workflow.Definition{Steps: []workflow.Step{
		{Name: "a", CapabilityID: "x.tool"}, {Name: "a", CapabilityID: "x.tool"},
	}}); err == nil {
		t.Fatal("duplicate step names must be rejected")
	}
}

type failingCapability struct{ manifest CapabilityManifest }

func (f *failingCapability) Manifest() CapabilityManifest { return f.manifest }

func (f *failingCapability) Execute(context.Context, CapabilityRequest) (CapabilityResult, error) {
	return CapabilityResult{Content: "boom", OK: false}, nil
}

// --- Memory ---

func TestMemoryRememberRecallForgetWithScopeIsolation(t *testing.T) {
	store := memory.NewSliceStore()
	memory, err := memory.NewCapability("user.memory", store)
	if err != nil {
		t.Fatal(err)
	}
	_, _, tenant, user := testScopes()
	principal := testPrincipal(user)
	// A different subject in the same tenant: memory must not leak.
	bobScope, err := tenant.Child(ScopeRef{Kind: ScopeUser, ID: "bob"})
	if err != nil {
		t.Fatal(err)
	}
	otherPrincipal := Principal{
		SubjectID: "bob", TenantID: "tenant-a", Scope: bobScope,
		Grants: principal.Grants,
	}

	call := func(p Principal, args map[string]any) CapabilityResult {
		result, _ := memory.Execute(context.Background(), CapabilityRequest{
			CallID: "m", Args: args,
			Context: CapabilityContext{Principal: p, Scope: p.Scope},
		})
		return result
	}

	if result := call(principal, map[string]any{
		"action": "remember", "key": "watchlist", "content": "Alice watches SOL and NOVA", "tags": []any{"portfolio"},
	}); !result.OK {
		t.Fatalf("remember failed: %#v", result)
	}

	recalled := call(principal, map[string]any{"action": "recall", "query": "sol"})
	if !recalled.OK || !strings.Contains(recalled.Content, "watchlist") {
		t.Fatalf("recall failed: %#v", recalled)
	}

	// Same tenant, different user: no memory leaks across subjects.
	if result := call(otherPrincipal, map[string]any{"action": "recall", "query": "sol"}); strings.Contains(result.Content, "watchlist") {
		t.Fatalf("memory leaked across users: %#v", result)
	}

	// Tag filter and replacement semantics: same key replaces the old entry.
	call(principal, map[string]any{"action": "remember", "key": "watchlist", "content": "Alice now watches ETH only", "tags": []any{"portfolio"}})
	tagged := call(principal, map[string]any{"action": "recall", "tags": []any{"portfolio"}})
	if !strings.Contains(tagged.Content, "ETH only") || strings.Contains(tagged.Content, "NOVA") {
		t.Fatalf("replacement by key failed: %s", tagged.Content)
	}

	// Forget by id.
	var parsed struct {
		Entries []MemoryEntry `json:"entries"`
	}
	if err := json.Unmarshal([]byte(tagged.Content), &parsed); err != nil {
		t.Fatal(err)
	}
	if len(parsed.Entries) != 1 {
		t.Fatalf("expected exactly one entry, got %#v", parsed.Entries)
	}
	if result := call(principal, map[string]any{"action": "forget", "id": parsed.Entries[0].ID}); !result.OK {
		t.Fatalf("forget failed: %#v", result)
	}
	if result := call(principal, map[string]any{"action": "recall", "query": ""}); strings.Contains(result.Content, "ETH only") {
		t.Fatalf("entry survived forget: %s", result.Content)
	}

	if result := call(principal, map[string]any{"action": "explode"}); result.OK || result.Metadata["code"] != CodeInvalidArgs {
		t.Fatalf("unknown action must be denied: %#v", result)
	}
}

func TestMemoryStoreReturnsIndependentTagSlices(t *testing.T) {
	store := memory.NewSliceStore()
	_, _, _, user := testScopes()
	entry, err := store.Remember(context.Background(), user, memory.Entry{Key: "k", Content: "v", Tags: []string{"original"}})
	if err != nil {
		t.Fatal(err)
	}
	entry.Tags[0] = "mutated"
	recalled, err := store.Recall(context.Background(), user, "", nil, 10)
	if err != nil || len(recalled) != 1 || recalled[0].Tags[0] != "original" {
		t.Fatalf("memory tags share mutable storage: %#v %v", recalled, err)
	}
}

func TestMemoryStoreValidatesBoundedRecallContract(t *testing.T) {
	store := memory.NewSliceStore()
	_, _, _, user := testScopes()
	ctx := context.Background()
	exactTag := strings.Repeat("界", memory.MaxTagRunes)

	if _, err := store.Remember(ctx, user, memory.Entry{Key: "unicode", Content: "valid", Tags: []string{exactTag}}); err != nil {
		t.Fatalf("exact Unicode tag boundary was rejected: %v", err)
	}
	if _, err := store.Remember(ctx, user, memory.Entry{
		Key: "too-long-tag", Content: "invalid", Tags: []string{strings.Repeat("界", memory.MaxTagRunes+1)},
	}); err == nil {
		t.Fatal("overlong tag was accepted")
	}
	duplicateTags := make([]string, memory.MaxRecallTags+1)
	if _, err := store.Remember(ctx, user, memory.Entry{Key: "too-many-tags", Content: "invalid", Tags: duplicateTags}); err == nil {
		t.Fatal("duplicate tags bypassed the remember tag count limit")
	}
	if _, err := store.Remember(ctx, user, memory.Entry{Key: "empty-tag", Content: "valid", Tags: []string{""}}); err != nil {
		t.Fatalf("empty tag was rejected: %v", err)
	}

	if _, err := store.Recall(ctx, user, strings.Repeat("界", memory.MaxQueryRunes), []string{exactTag}, memory.MaxRecallLimit); err != nil {
		t.Fatalf("exact Unicode recall boundaries were rejected: %v", err)
	}
	if _, err := store.Recall(ctx, user, strings.Repeat("界", memory.MaxQueryRunes+1), nil, 0); err == nil {
		t.Fatal("overlong recall query was accepted")
	}
	if _, err := store.Recall(ctx, user, "", duplicateTags, 0); err == nil {
		t.Fatal("duplicate tags bypassed the recall tag count limit")
	}
	if _, err := store.Recall(ctx, user, "", []string{strings.Repeat("界", memory.MaxTagRunes+1)}, 0); err == nil {
		t.Fatal("overlong recall tag was accepted")
	}
	if _, err := store.Recall(ctx, user, "", nil, -1); err == nil {
		t.Fatal("negative recall limit was accepted")
	}
	if _, err := store.Recall(ctx, user, "", nil, memory.MaxRecallLimit+1); err == nil {
		t.Fatal("recall limit above the maximum was accepted")
	}

	unlimited, err := store.Recall(ctx, user, "", nil, 0)
	if err != nil || len(unlimited) != 2 {
		t.Fatalf("limit 0 must retain unlimited store semantics: entries=%#v err=%v", unlimited, err)
	}
	limited, err := store.Recall(ctx, user, "", nil, 1)
	if err != nil || len(limited) != 1 {
		t.Fatalf("positive recall limit was not applied: entries=%#v err=%v", limited, err)
	}
	emptyTagged, err := store.Recall(ctx, user, "", []string{""}, 0)
	if err != nil || len(emptyTagged) != 1 || emptyTagged[0].Key != "empty-tag" {
		t.Fatalf("empty tag was not preserved as a valid filter: entries=%#v err=%v", emptyTagged, err)
	}
}

type memoryRecallProbeStore struct {
	memory.Store
	recallCalls int
}

func (s *memoryRecallProbeStore) Recall(ctx context.Context, scope ScopePath, query string, tags []string, limit int) ([]memory.Entry, error) {
	s.recallCalls++
	return s.Store.Recall(ctx, scope, query, tags, limit)
}

type memoryToolCallModel struct{ call ToolCall }

func (memoryToolCallModel) Provider() string { return "memory-validation-test" }

func (m memoryToolCallModel) Stream(_ context.Context, options GenerateOptions, emit func(StreamChunk)) error {
	if options.Messages[len(options.Messages)-1].Role == RoleTool {
		emit(StreamChunk{Kind: StreamKindAssistant, Text: "done"})
		emit(StreamChunk{Kind: StreamKindFinish, FinishKind: FinishStop})
		return nil
	}
	call := m.call
	emit(StreamChunk{Kind: StreamKindAssistant, ToolCall: &call})
	emit(StreamChunk{Kind: StreamKindFinish, FinishKind: FinishToolCalls})
	return nil
}

func invokeMemoryRecallTool(t *testing.T, store memory.Store, args map[string]any) CapabilityResult {
	t.Helper()
	_, _, _, user := testScopes()
	principal := testPrincipal(user)
	capability, err := memory.NewCapability("user.memory.validation", store)
	if err != nil {
		t.Fatal(err)
	}
	registry := NewCapabilityRegistry()
	if err := registry.Register(user, capability); err != nil {
		t.Fatal(err)
	}
	sessionScope, err := user.Child(ScopeRef{Kind: ScopeSession, ID: "memory-validation"})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := (CapabilityResolver{Registry: registry}).Resolve(principal, sessionScope)
	if err != nil {
		t.Fatal(err)
	}
	session, err := NewSession(SessionOptions{
		ID: "memory-validation", ProfileID: "memory.validation", Principal: principal, Scope: sessionScope,
	})
	if err != nil {
		t.Fatal(err)
	}
	call := ToolCall{ID: "call-memory-validation", Name: capability.Manifest().ID, Args: args}
	agent, err := NewAgent(AgentOptions{LLM: memoryToolCallModel{call: call}, Tools: snapshot, Session: session})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := agent.RunTurn(context.Background(), TurnInput{RunID: "run-memory-validation", Text: "recall memory"}); err != nil {
		t.Fatal(err)
	}
	result, exists, err := session.ToolResult("run-memory-validation", call.ID)
	if err != nil || !exists {
		t.Fatalf("memory tool result was not recorded: exists=%t err=%v", exists, err)
	}
	return result
}

func TestMemoryToolSchemaBoundsRecallBeforeProvider(t *testing.T) {
	cases := []struct {
		name      string
		args      map[string]any
		wantOK    bool
		wantCalls int
	}{
		{
			name: "missing action",
			args: map[string]any{"query": "sol"},
		},
		{
			name: "oversized query",
			args: map[string]any{"action": "recall", "query": strings.Repeat("界", memory.MaxQueryRunes+1)},
		},
		{
			name: "too many duplicate tags",
			args: map[string]any{"action": "recall", "tags": make([]any, memory.MaxRecallTags+1)},
		},
		{
			name: "oversized tag item",
			args: map[string]any{"action": "recall", "tags": []any{strings.Repeat("界", memory.MaxTagRunes+1)}},
		},
		{
			name: "exact Unicode rune boundaries",
			args: map[string]any{
				"action": "recall",
				"query":  strings.Repeat("界", memory.MaxQueryRunes),
				"tags":   []any{strings.Repeat("界", memory.MaxTagRunes)},
			},
			wantOK: true, wantCalls: 1,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := &memoryRecallProbeStore{Store: memory.NewSliceStore()}
			result := invokeMemoryRecallTool(t, store, tc.args)
			if result.OK != tc.wantOK || store.recallCalls != tc.wantCalls {
				t.Fatalf("unexpected validation result=%#v recallCalls=%d", result, store.recallCalls)
			}
			if !tc.wantOK && result.Metadata["code"] != CodeInvalidArgs {
				t.Fatalf("schema denial must use invalid args before provider execution: %#v", result)
			}
		})
	}

	capability, err := memory.NewCapability("user.memory.revision", memory.NewSliceStore())
	if err != nil {
		t.Fatal(err)
	}
	if revision := capability.ArtifactRevision(); revision != "memory-capability/v3" {
		t.Fatalf("unexpected memory artifact revision %q", revision)
	}
}

// --- RAG ---

type ragSearchProbe struct {
	searches int
}

func (*ragSearchProbe) Ingest(context.Context, ScopePath, rag.Document) error { return nil }

func (p *ragSearchProbe) Search(context.Context, ScopePath, rag.Query) ([]rag.Chunk, error) {
	p.searches++
	return nil, nil
}

type ragToolCallAdapter struct {
	call ToolCall
	sent bool
}

func (*ragToolCallAdapter) Provider() string { return "rag-tool-call-test" }

func (a *ragToolCallAdapter) Stream(_ context.Context, _ GenerateOptions, emit func(StreamChunk)) error {
	if !a.sent {
		a.sent = true
		call := a.call
		emit(StreamChunk{Kind: StreamKindAssistant, ToolCall: &call, ToolCalls: []ToolCall{call}})
		emit(StreamChunk{Kind: StreamKindFinish, FinishKind: FinishToolCalls})
		return nil
	}
	emit(StreamChunk{Kind: StreamKindAssistant, Text: "done"})
	emit(StreamChunk{Kind: StreamKindFinish, FinishKind: FinishStop})
	return nil
}

func runRAGToolCall(t *testing.T, sessionID string, args map[string]any) int {
	t.Helper()
	_, _, _, user := testScopes()
	principal := testPrincipal(user)
	sessionScope, err := user.Child(ScopeRef{Kind: ScopeSession, ID: sessionID})
	if err != nil {
		t.Fatal(err)
	}
	session, err := NewSession(SessionOptions{
		ID: sessionID, ProfileID: "test.profile", Principal: principal, Scope: sessionScope,
	})
	if err != nil {
		t.Fatal(err)
	}
	probe := &ragSearchProbe{}
	capability, err := rag.NewSearchCapability("crypto.kb.search", probe)
	if err != nil {
		t.Fatal(err)
	}
	registry := NewCapabilityRegistry()
	if err := registry.Register(user, capability); err != nil {
		t.Fatal(err)
	}
	snapshot, err := (CapabilityResolver{Registry: registry}).Resolve(principal, sessionScope)
	if err != nil {
		t.Fatal(err)
	}
	adapter := &ragToolCallAdapter{call: ToolCall{ID: "rag-call", Name: capability.Manifest().ID, Args: args}}
	agent, err := NewAgent(AgentOptions{LLM: adapter, Tools: snapshot, Session: session, MaxSteps: 2})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := agent.RunTurn(context.Background(), TurnInput{RunID: "rag-run", Text: "search"}); err != nil {
		t.Fatal(err)
	}
	return probe.searches
}

func TestRAGToolSchemaRejectsOutOfBoundsArgsBeforeIndex(t *testing.T) {
	if searches := runRAGToolCall(t, "rag-schema-exact", map[string]any{
		"query": strings.Repeat("界", rag.MaxQueryRunes),
		"top_k": float64(rag.MaxSearchTopK),
		"tags":  []any{strings.Repeat("界", rag.MaxTagRunes)},
	}); searches != 1 {
		t.Fatalf("exact-boundary tool arguments reached index %d times, want 1", searches)
	}

	tooManyTags := make([]any, rag.MaxQueryTags+1)
	for i := range tooManyTags {
		tooManyTags[i] = "duplicate"
	}
	uniqueTokens := make([]string, rag.MaxQueryTokens+1)
	for i := range uniqueTokens {
		uniqueTokens[i] = fmt.Sprintf("token-%d", i)
	}
	for name, args := range map[string]map[string]any{
		"missing query":           {},
		"empty query":             {"query": ""},
		"oversized unicode query": {"query": strings.Repeat("界", rag.MaxQueryRunes+1)},
		"too many duplicate tags": {"query": "match", "tags": tooManyTags},
		"oversized unicode tag":   {"query": "match", "tags": []any{strings.Repeat("界", rag.MaxTagRunes+1)}},
		"too many unique tokens":  {"query": strings.Join(uniqueTokens, " ")},
		"top k below minimum":     {"query": "match", "top_k": float64(0)},
		"top k above maximum":     {"query": "match", "top_k": float64(rag.MaxSearchTopK + 1)},
	} {
		t.Run(name, func(t *testing.T) {
			if searches := runRAGToolCall(t, "rag-schema-"+strings.ReplaceAll(name, " ", "-"), args); searches != 0 {
				t.Fatalf("out-of-bounds tool arguments reached index %d times", searches)
			}
		})
	}
}

func TestRAGSearchRanksAndRespectsScopeVisibility(t *testing.T) {
	index := rag.NewKeywordIndex()
	rag, err := rag.NewSearchCapability("crypto.kb.search", index)
	if err != nil {
		t.Fatal(err)
	}
	global, product, tenant, user := testScopes()
	principal := testPrincipal(user)
	ctx := context.Background()

	// Tenant knowledge: visible to the tenant's users.
	if err := index.Ingest(ctx, tenant, RagDocument{
		ID: "sol-whitepaper", Source: "docs/sol.md",
		Content: "Solana is a high throughput blockchain optimized for parallel execution.",
		Tags:    []string{"research"},
	}); err != nil {
		t.Fatal(err)
	}
	// Product knowledge: also visible.
	if err := index.Ingest(ctx, product, RagDocument{
		ID: "product-risk", Source: "policy.md",
		Content: "Product policy: always flag unverified token contracts as high risk.",
	}); err != nil {
		t.Fatal(err)
	}
	// Another tenant's knowledge: never visible.
	otherTenant, _ := product.Child(ScopeRef{Kind: ScopeTenant, ID: "otherco"})
	if err := index.Ingest(ctx, otherTenant, RagDocument{
		ID: "secret", Content: "Solana internal trading desk notes for otherco only.",
	}); err != nil {
		t.Fatal(err)
	}
	// Global knowledge: visible from everywhere.
	if err := index.Ingest(ctx, global, RagDocument{
		ID: "glossary", Content: "Blockchain glossary: throughput means transactions per second.",
	}); err != nil {
		t.Fatal(err)
	}

	search := func(args map[string]any) CapabilityResult {
		result, _ := rag.Execute(ctx, CapabilityRequest{
			CallID: "r", Args: args,
			Context: CapabilityContext{Principal: principal, Scope: user},
		})
		return result
	}

	result := search(map[string]any{"query": "solana throughput", "top_k": float64(5)})
	if !result.OK {
		t.Fatalf("search failed: %#v", result)
	}
	if !strings.Contains(result.Content, "sol-whitepaper") || !strings.Contains(result.Content, "glossary") {
		t.Fatalf("expected tenant and global hits: %s", result.Content)
	}
	if strings.Contains(result.Content, "secret") {
		t.Fatalf("another tenant's documents leaked: %s", result.Content)
	}

	// Ranking: the more specific document scores first.
	if strings.Index(result.Content, "sol-whitepaper") > strings.Index(result.Content, "glossary") {
		t.Fatalf("ranking is wrong, specific doc must come first: %s", result.Content)
	}

	// Tag filter.
	tagged := search(map[string]any{"query": "solana", "tags": []any{"research"}})
	if !strings.Contains(tagged.Content, "sol-whitepaper") || strings.Contains(tagged.Content, "glossary") {
		t.Fatalf("tag filter broken: %s", tagged.Content)
	}

	// Empty query is refused with a stable code.
	if result := search(map[string]any{"query": "   "}); result.OK || result.Metadata["code"] != "rag_search_failed" {
		t.Fatalf("empty query must fail cleanly: %#v", result)
	}
}

func TestRAGIndexFreezesDocumentTags(t *testing.T) {
	index := rag.NewKeywordIndex()
	_, _, tenant, user := testScopes()
	document := rag.Document{ID: "doc", Content: "solana research", Tags: []string{"research"}}
	if err := index.Ingest(context.Background(), tenant, document); err != nil {
		t.Fatal(err)
	}
	document.Tags[0] = "mutated"
	results, err := index.Search(context.Background(), user, rag.Query{Query: "solana", Tags: []string{"research"}})
	if err != nil || len(results) != 1 {
		t.Fatalf("rag document tags were not frozen: %#v %v", results, err)
	}
}
