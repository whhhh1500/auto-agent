package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	programtools "github.com/whhhh1500/auto-agent/pkg/adapter/programmatic/toolcapability"
	appcontextassembly "github.com/whhhh1500/auto-agent/pkg/app/contextassembly"
	access "github.com/whhhh1500/auto-agent/pkg/app/programmatic"
	core "github.com/whhhh1500/auto-agent/pkg/core"
)

const (
	repositoryFixtureProfileID = "example.repository-limits"
	repositorySourceToolID     = "repo.source.read"
	repositoryEvidenceSchema   = "harness.programmatic.live-repository/v1"
	repositoryFixtureVariant   = "repository_limits_v1"
	repositoryMaxSourceBytes   = 16 << 10
)

var repositorySourcePaths = []string{
	"pkg/core/model_context.go",
	"pkg/core/agent_model_call.go",
	"pkg/adapter/modelexecution/corebridge/adapter.go",
	"pkg/adapter/modelruntime/compiler.go",
}

type repositorySource struct {
	Path, SHA256, Content string
}

type repositoryBundle struct {
	Sources map[string]repositorySource
	SHA256  string
}

// TestLiveRepositoryLimitsAcceptance is a development-only repository
// integration task. Its four source files are frozen into memory before the
// Runtime starts; the capability never reads arbitrary files or disk.
func TestLiveRepositoryLimitsAcceptance(t *testing.T) {
	if os.Getenv("HARNESS_PROGRAMMATIC_LIVE") != "1" && os.Getenv("HARNESS_ACCEPTANCE_LIVE_PROGRAMMATIC") != "1" {
		t.Skip("set HARNESS_PROGRAMMATIC_LIVE=1 to make intentional live model requests")
	}
	if liveProtocol() != "responses" {
		t.Skip("set HARNESS_PROGRAMMATIC_PROTOCOL=responses for repository integration evidence")
	}
	bundle, err := loadRepositoryBundle()
	if err != nil {
		t.Fatal(err)
	}
	oracle, err := repositoryLimitsOracle(bundle)
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := newLiveAdapter(context.Background())
	if err != nil {
		t.Fatal("live model configuration is incomplete or invalid")
	}
	limits, ok := liveAdapterLimits(adapter)
	if !ok {
		t.Fatal("live adapter did not report valid context limits")
	}
	modelID := strings.TrimSpace(os.Getenv("HARNESS_LLM_MODEL"))
	if modelID == "" {
		t.Fatal("live model configuration is incomplete or invalid")
	}
	interval, err := liveMinRequestInterval(os.Getenv("HARNESS_PROGRAMMATIC_MIN_REQUEST_INTERVAL"))
	if err != nil {
		t.Fatal("live request interval is invalid")
	}
	caseDef := liveCase{name: "repository_limits_auto", maxModelRounds: 6, maxToolCalls: 8, outputContracts: true, fixtureVariant: repositoryFixtureVariant, prompt: repositoryLimitsPrompt()}
	inner := newLiveModel(t, adapter, caseDef.name, caseDef.maxModelRounds, &liveRequestPacer{interval: interval})
	model := &repositoryLiveModel{liveModel: inner}
	fixture, err := newRepositoryFixture(model, bundle)
	if err != nil {
		t.Fatal(err)
	}
	if err := configureRepositoryProfile(fixture, adapter.Provider(), modelID, caseDef.maxModelRounds, caseDef.maxToolCalls); err != nil {
		t.Fatal(err)
	}
	installRepositoryContextRecorder(t, fixture)
	started := time.Now()
	runID := "live-repository-limits"
	var result core.TurnResult
	var events []core.SessionEvent
	acceptancePassed := false
	t.Cleanup(func() {
		base := finalizedLiveEvidence(caseDef, result, modelID, liveProtocol(), liveSourceRevision(), caseDef.prompt, events, inner, time.Since(started), acceptancePassed)
		base.Schema = repositoryEvidenceSchema
		base.FixtureVariant = repositoryFixtureVariant
		record := liveRepositoryEvidence{liveEvidenceRecord: base, Repository: repositoryEvidenceFromFixture(fixture, bundle, oracle, limits, result, model)}
		if err := writeLiveRepositoryEvidence(os.Getenv("HARNESS_PROGRAMMATIC_EVIDENCE_PATH"), record); err != nil {
			t.Error("could not write live repository experiment evidence")
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	result, err = fixture.runtime.RunTurn(ctx, fixture.principal, fixture.session, core.TurnInput{RunID: runID, Text: caseDef.prompt}, nil)
	events = fixture.session.Events()
	if err != nil || result.Status != core.RunCompleted {
		t.Fatalf("repository integration did not complete (status=%s, rounds=%d)", result.Status, inner.rounds())
	}
	assertRepositoryAcceptance(t, fixture, bundle, oracle, limits, result, model)
	acceptancePassed = true
}

func repositoryLimitsPrompt() string {
	return `Use only the currently exposed read-only tools. This is a repository integration task, not a general claim about a provider. Read every available source file exactly once. Return exactly one JSON object and no Markdown with these keys: fallback_window, fallback_output, compiler_window, compiler_output, configured_output_overrides_default, evidence. evidence must be an array of objects containing path and an unqualified source identifier in symbol. Include the identifiers that define the fallback/default values and the functions or methods that carry the limits through the source chain. Do not present compiler defaults as provider-authenticated native limits.`
}

type repositoryAnswer struct {
	FallbackWindow                   int                        `json:"fallback_window"`
	FallbackOutput                   int                        `json:"fallback_output"`
	CompilerWindow                   int                        `json:"compiler_window"`
	CompilerOutput                   int                        `json:"compiler_output"`
	ConfiguredOutputOverridesDefault bool                       `json:"configured_output_overrides_default"`
	Evidence                         []repositoryAnswerEvidence `json:"evidence"`
}

type repositoryAnswerEvidence struct {
	Path   string `json:"path"`
	Symbol string `json:"symbol"`
}

type repositoryLimits struct {
	FallbackWindow, FallbackOutput, CompilerWindow, CompilerOutput int
	ConfiguredOutputOverridesDefault                               bool
	RequiredSymbols                                                map[string]map[string]bool
}

type repositorySourceTool struct {
	manifest core.CapabilityManifest
	bundle   repositoryBundle
	mu       sync.Mutex
	calls    map[string]int
	failures int
}

func newRepositorySourceTool(bundle repositoryBundle) (*repositorySourceTool, error) {
	paths := repositoryAvailablePaths(bundle)
	manifest := core.CapabilityManifest{
		ID: repositorySourceToolID, Version: "1.0.0", Name: "Frozen repository source reader",
		Description: "Read one complete UTF-8 source file from the fixed repository allowlist.", Kind: core.KindKnowledge, Idempotent: true,
		RequiredPermissions: []core.Permission{core.PermRead},
		Metadata:            map[string]string{access.ExposureKey: access.ExposureVersion},
		Tool:                &core.ToolExposure{Parameters: map[string]any{"type": "object", "required": []any{"path"}, "additionalProperties": false, "properties": map[string]any{"path": map[string]any{"type": "string", "enum": stringsToAny(paths)}}}},
		OutputSchema:        map[string]any{"type": "object", "required": []any{"path", "sha256", "content"}, "additionalProperties": false, "properties": map[string]any{"path": map[string]any{"type": "string"}, "sha256": map[string]any{"type": "string"}, "content": map[string]any{"type": "string"}}},
	}
	maximum := 0
	for _, source := range bundle.Sources {
		content, err := repositorySourceResult(source)
		if err != nil {
			return nil, err
		}
		if len(content) > maximum {
			maximum = len(content)
		}
	}
	manifest.MaxOutputBytes = maximum
	return &repositorySourceTool{manifest: manifest, bundle: bundle, calls: map[string]int{}}, nil
}

func repositoryAvailablePaths(bundle repositoryBundle) []string {
	paths := make([]string, 0, len(bundle.Sources))
	for _, path := range repositorySourcePaths {
		if _, ok := bundle.Sources[path]; ok {
			paths = append(paths, path)
		}
	}
	return paths
}

func stringsToAny(values []string) []any {
	out := make([]any, len(values))
	for i := range values {
		out[i] = values[i]
	}
	return out
}
func (t *repositorySourceTool) Manifest() core.CapabilityManifest { return t.manifest }
func (t *repositorySourceTool) Execute(_ context.Context, request core.CapabilityRequest) (core.CapabilityResult, error) {
	if err := core.RequireAcceptedInvocation(request); err != nil {
		return core.CapabilityResult{}, err
	}
	path, _ := request.Args["path"].(string)
	content, err := t.read(path)
	if err != nil {
		return core.CapabilityResult{}, err
	}
	return core.CapabilityResult{Content: content, OK: true}, nil
}
func (t *repositorySourceTool) read(path string) (string, error) {
	source, ok := t.bundle.Sources[path]
	if !ok {
		t.mu.Lock()
		t.failures++
		t.mu.Unlock()
		return "", fmt.Errorf("repository source path is not allowlisted")
	}
	content, err := repositorySourceResult(source)
	if err != nil {
		return "", err
	}
	t.mu.Lock()
	t.calls[path]++
	t.mu.Unlock()
	return content, nil
}
func (t *repositorySourceTool) effect() (map[string]int, int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := map[string]int{}
	for k, v := range t.calls {
		out[k] = v
	}
	return out, t.failures
}

func repositorySourceResult(source repositorySource) (string, error) {
	encoded, err := json.Marshal(map[string]string{"path": source.Path, "sha256": source.SHA256, "content": source.Content})
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

type repositoryFixture struct {
	runtime   *core.Runtime
	principal core.Principal
	session   *core.Session
	source    *repositorySourceTool
	context   *repositoryContextRecorder
}

func newRepositoryFixture(model core.LlmAdapter, bundle repositoryBundle) (*repositoryFixture, error) {
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	product, err := global.Child(core.ScopeRef{Kind: core.ScopeProduct, ID: "repository-integration"})
	if err != nil {
		return nil, err
	}
	user, err := product.Child(core.ScopeRef{Kind: core.ScopeUser, ID: "repository-reader"})
	if err != nil {
		return nil, err
	}
	principal := core.Principal{TenantID: "repository-tenant", SubjectID: "repository-subject", Scope: user, Grants: core.NewPermissionSet(core.PermRead)}
	scope, err := user.Child(core.ScopeRef{Kind: core.ScopeSession, ID: "repository-session"})
	if err != nil {
		return nil, err
	}
	session, err := core.NewSession(core.SessionOptions{ID: "repository-session", ProfileID: repositoryFixtureProfileID, Principal: principal, Scope: scope})
	if err != nil {
		return nil, err
	}
	source, err := newRepositorySourceTool(bundle)
	if err != nil {
		return nil, err
	}
	catalog, err := programtools.NewCatalogCapability(programtools.CatalogID)
	if err != nil {
		return nil, err
	}
	execute, err := programtools.NewExecuteCapability(programtools.ExecuteID)
	if err != nil {
		return nil, err
	}
	registry := core.NewCapabilityRegistry()
	for _, capability := range []core.Capability{catalog, execute, source} {
		if err := registry.Register(product, capability); err != nil {
			return nil, err
		}
	}
	profiles := core.NewAgentProfileRegistry()
	name := "Repository limits integration"
	selection := core.ModelSelection{Provider: model.Provider(), Model: "repository-limits"}
	steps, calls := 6, 8
	if err := profiles.Bind(core.AgentProfileLayer{Scope: product, ProfileID: repositoryFixtureProfileID, Name: &name, Model: &selection, AddCapabilities: []string{programtools.CatalogID, programtools.ExecuteID, repositorySourceToolID}, MaxSteps: &steps, MaxToolCalls: &calls}); err != nil {
		return nil, err
	}
	assembler, err := appcontextassembly.NewAssembler(appcontextassembly.Config{})
	if err != nil {
		return nil, err
	}
	runtime := &core.Runtime{Capabilities: registry, Profiles: profiles, ToolJournal: newMemoryJournal(), Models: core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) { return model, nil }), ContextAssembler: assembler.AssembleModelContext}
	return &repositoryFixture{runtime: runtime, principal: principal, session: session, source: source}, nil
}

func configureRepositoryProfile(f *repositoryFixture, provider, modelID string, maxSteps, maxToolCalls int) error {
	name := "Live repository limits integration"
	selection := core.ModelSelection{Provider: provider, Model: modelID}
	return f.runtime.Profiles.Bind(core.AgentProfileLayer{Scope: f.session.Scope(), ProfileID: repositoryFixtureProfileID, Name: &name, Model: &selection, MaxSteps: &maxSteps, MaxToolCalls: &maxToolCalls})
}

type repositoryLiveModel struct {
	*liveModel
	mu   sync.Mutex
	text strings.Builder
}

func (m *repositoryLiveModel) Stream(ctx context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	m.mu.Lock()
	m.text.Reset()
	m.mu.Unlock()
	return m.liveModel.Stream(ctx, options, func(chunk core.StreamChunk) {
		if chunk.Kind == core.StreamKindAssistant && chunk.Text != "" {
			m.mu.Lock()
			m.text.WriteString(chunk.Text)
			m.mu.Unlock()
		}
		emit(chunk)
	})
}
func (m *repositoryLiveModel) answer() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.text.String()
}

type repositoryEvidenceSource struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
	Bytes  int    `json:"bytes"`
}
type repositoryEffectsEvidence struct {
	Observed            bool `json:"observed"`
	SourceReadCalls     int  `json:"source_read_calls"`
	UniquePaths         int  `json:"unique_paths"`
	AllReadsOK          bool `json:"all_reads_ok"`
	EachAllowlistedOnce bool `json:"each_allowlisted_once"`
	UnexpectedPaths     bool `json:"unexpected_paths"`
}
type repositoryOracleEvidence struct {
	Parsed                        bool `json:"parsed"`
	FallbackWindowMatch           bool `json:"fallback_window_match"`
	FallbackOutputMatch           bool `json:"fallback_output_match"`
	CompilerWindowMatch           bool `json:"compiler_window_match"`
	CompilerOutputMatch           bool `json:"compiler_output_match"`
	ConfiguredOutputOverrideMatch bool `json:"configured_output_override_match"`
	RequiredSymbolRefsMatch       bool `json:"required_symbol_refs_match"`
}
type repositoryAnswerEvidenceResult struct {
	Observed                bool `json:"observed"`
	ValidJSON               bool `json:"valid_json"`
	KeysExact               bool `json:"keys_exact"`
	ModelEqualsDurable      bool `json:"model_equals_durable"`
	ResultEqualsDurable     bool `json:"result_equals_durable"`
	NumericValuesMatch      bool `json:"numeric_values_match"`
	EvidencePathsMatch      bool `json:"evidence_paths_match"`
	EvidenceSymbolRefsMatch bool `json:"evidence_symbol_refs_match"`
	Passed                  bool `json:"passed"`
}
type repositoryEvidence struct {
	TaskKind            string                         `json:"task_kind"`
	FixtureVariant      string                         `json:"fixture_variant"`
	SourceBundleSHA256  string                         `json:"source_bundle_sha256"`
	Sources             []repositoryEvidenceSource     `json:"sources"`
	LimitsObserved      bool                           `json:"limits_observed"`
	AssemblySucceeded   bool                           `json:"assembly_succeeded"`
	ContextWindowTokens int                            `json:"context_window_tokens"`
	MaxOutputTokens     int                            `json:"max_output_tokens"`
	LimitsSource        string                         `json:"limits_source"`
	Effects             repositoryEffectsEvidence      `json:"effects"`
	Oracle              repositoryOracleEvidence       `json:"oracle"`
	Answer              repositoryAnswerEvidenceResult `json:"answer_quality"`
}
type liveRepositoryEvidence struct {
	liveEvidenceRecord
	Repository repositoryEvidence `json:"repository"`
}

type repositoryContextRecorder struct {
	mu       sync.Mutex
	observed bool
	success  bool
	window   int
	output   int
}

func (r *repositoryContextRecorder) observe(request, _ core.ModelContext, err error) {
	r.mu.Lock()
	r.observed, r.success = true, err == nil
	r.window, r.output = request.ContextWindowTokens, request.MaxOutputTokens
	r.mu.Unlock()
}

func (r *repositoryContextRecorder) snapshot() (bool, bool, int, int) {
	if r == nil {
		return false, false, 0, 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.observed, r.success, r.window, r.output
}

func installRepositoryContextRecorder(t *testing.T, fixture *repositoryFixture) {
	t.Helper()
	assembler, err := appcontextassembly.NewAssembler(appcontextassembly.Config{})
	if err != nil {
		t.Fatal(err)
	}
	recorder := &repositoryContextRecorder{}
	fixture.context = recorder
	fixture.runtime.ContextAssembler = func(ctx context.Context, request core.ModelContext) (core.ModelContext, error) {
		assembled, err := assembler.AssembleModelContext(ctx, request)
		recorder.observe(request, assembled, err)
		return assembled, err
	}
}

func repositoryEvidenceFromFixture(f *repositoryFixture, b repositoryBundle, o repositoryLimits, limits liveAdapterContextLimits, result core.TurnResult, model *repositoryLiveModel) repositoryEvidence {
	observed, assembled, window, output := f.context.snapshot()
	e := repositoryEvidence{TaskKind: "repo_integration_development", FixtureVariant: repositoryFixtureVariant, SourceBundleSHA256: b.SHA256, LimitsObserved: observed, AssemblySucceeded: assembled, ContextWindowTokens: window, MaxOutputTokens: output, LimitsSource: "adapter_model_context_limits"}
	for _, p := range repositorySourcePaths {
		s := b.Sources[p]
		e.Sources = append(e.Sources, repositoryEvidenceSource{Path: p, SHA256: s.SHA256, Bytes: len(s.Content)})
	}
	counts, failures := f.source.effect()
	e.Effects = repositoryEffectsEvidence{Observed: true, UniquePaths: len(counts), AllReadsOK: failures == 0, EachAllowlistedOnce: true}
	for _, p := range repositorySourcePaths {
		e.Effects.SourceReadCalls += counts[p]
		if counts[p] != 1 {
			e.Effects.EachAllowlistedOnce = false
		}
	}
	for p := range counts {
		if _, ok := b.Sources[p]; !ok {
			e.Effects.UnexpectedPaths = true
		}
	}
	answer, valid, keys := parseRepositoryAnswer(result.Answer)
	durable := f.session.LastAssistantText(result.RunID)
	e.Answer = repositoryAnswerEvidenceResult{Observed: result.RunID != "", ValidJSON: valid, KeysExact: keys, ModelEqualsDurable: strings.TrimSpace(model.answer()) == strings.TrimSpace(durable)}
	e.Answer.ResultEqualsDurable = strings.TrimSpace(result.Answer) == strings.TrimSpace(durable)
	if valid {
		fallbackWindowMatch := answer.FallbackWindow == o.FallbackWindow
		fallbackOutputMatch := answer.FallbackOutput == o.FallbackOutput
		compilerWindowMatch := answer.CompilerWindow == o.CompilerWindow
		compilerOutputMatch := answer.CompilerOutput == o.CompilerOutput
		configuredOutputMatch := answer.ConfiguredOutputOverridesDefault == o.ConfiguredOutputOverridesDefault
		e.Answer.NumericValuesMatch = fallbackWindowMatch && fallbackOutputMatch && compilerWindowMatch && compilerOutputMatch && configuredOutputMatch
		e.Answer.EvidencePathsMatch = repositoryEvidencePaths(answer.Evidence)
		e.Answer.EvidenceSymbolRefsMatch = repositoryEvidenceSymbols(answer.Evidence, o.RequiredSymbols)
		for _, reference := range answer.Evidence {
			if !repositorySymbolExists(b.Sources[reference.Path].Content, reference.Symbol) {
				e.Answer.EvidenceSymbolRefsMatch = false
			}
		}
		e.Oracle = repositoryOracleEvidence{Parsed: true, FallbackWindowMatch: fallbackWindowMatch, FallbackOutputMatch: fallbackOutputMatch, CompilerWindowMatch: compilerWindowMatch, CompilerOutputMatch: compilerOutputMatch, ConfiguredOutputOverrideMatch: configuredOutputMatch, RequiredSymbolRefsMatch: e.Answer.EvidenceSymbolRefsMatch}
	}
	e.Answer.Passed = e.Answer.ValidJSON && e.Answer.KeysExact && e.Answer.ModelEqualsDurable && e.Answer.ResultEqualsDurable && e.Answer.NumericValuesMatch && e.Answer.EvidencePathsMatch && e.Answer.EvidenceSymbolRefsMatch
	return e
}

func parseRepositoryAnswer(text string) (repositoryAnswer, bool, bool) {
	var raw map[string]json.RawMessage
	if json.Unmarshal([]byte(text), &raw) != nil {
		return repositoryAnswer{}, false, false
	}
	want := map[string]bool{"fallback_window": true, "fallback_output": true, "compiler_window": true, "compiler_output": true, "configured_output_overrides_default": true, "evidence": true}
	if len(raw) != len(want) {
		return repositoryAnswer{}, false, false
	}
	for k := range want {
		if _, ok := raw[k]; !ok {
			return repositoryAnswer{}, false, false
		}
	}
	var out repositoryAnswer
	if json.Unmarshal([]byte(text), &out) != nil {
		return repositoryAnswer{}, false, false
	}
	return out, true, true
}
func repositoryEvidencePaths(values []repositoryAnswerEvidence) bool {
	seen := map[string]bool{}
	for _, v := range values {
		seen[v.Path] = true
	}
	return len(seen) == len(repositorySourcePaths) && func() bool {
		for _, p := range repositorySourcePaths {
			if !seen[p] {
				return false
			}
		}
		return true
	}()
}
func repositoryEvidenceSymbols(values []repositoryAnswerEvidence, required map[string]map[string]bool) bool {
	seen := map[string]map[string]bool{}
	for _, v := range values {
		if seen[v.Path] == nil {
			seen[v.Path] = map[string]bool{}
		}
		seen[v.Path][v.Symbol] = true
	}
	for path, symbols := range required {
		for symbol := range symbols {
			if !seen[path][symbol] {
				return false
			}
		}
	}
	return true
}

func assertRepositoryAcceptance(t *testing.T, f *repositoryFixture, b repositoryBundle, o repositoryLimits, limits liveAdapterContextLimits, result core.TurnResult, model *repositoryLiveModel) {
	t.Helper()
	if model.rounds() > 6 {
		t.Fatal("repository model exceeded round budget")
	}
	e := repositoryEvidenceFromFixture(f, b, o, limits, result, model)
	if !e.LimitsObserved || !e.AssemblySucceeded || e.ContextWindowTokens != limits.contextWindowTokens || e.MaxOutputTokens != limits.maxOutputTokens {
		t.Fatalf("assembler did not receive adapter limits: evidence=%#v want=%#v", e, limits)
	}
	if !e.Effects.AllReadsOK || !e.Effects.EachAllowlistedOnce || e.Effects.UnexpectedPaths || e.Effects.SourceReadCalls != 4 {
		t.Fatalf("repository effects=%#v", e.Effects)
	}
	if !e.Answer.Passed || !e.Oracle.RequiredSymbolRefsMatch {
		t.Fatalf("repository answer predicates=%#v oracle=%#v", e.Answer, e.Oracle)
	}
	_, results, _, _ := liveSessionEvidence(f.session.Events())
	for _, toolResult := range results {
		if !toolResult.OK {
			t.Fatalf("repository tool result %q was not OK", toolResult.CallID)
		}
	}
	inv := liveInvocationEvidenceFromRounds(model.evidenceRounds(), f.session.Events())
	if inv == nil || !inv.AdapterCallsObserved || !inv.ReportedUsageComplete || !inv.UsageProtocolConsistent || !inv.LedgerUsageMatched || !inv.UniqueLedgerInvocationIDs {
		t.Fatalf("repository invocation evidence=%#v", inv)
	}
}

func loadRepositoryBundle() (repositoryBundle, error) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return repositoryBundle{}, fmt.Errorf("locate repository fixture")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
	sources := map[string]repositorySource{}
	hash := sha256.New()
	for _, path := range repositorySourcePaths {
		content, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(path)))
		if err != nil {
			return repositoryBundle{}, err
		}
		if len(content) > repositoryMaxSourceBytes || !utf8.Valid(content) {
			return repositoryBundle{}, fmt.Errorf("repository source bundle member is invalid")
		}
		sum := sha256.Sum256(content)
		source := repositorySource{Path: path, SHA256: hex.EncodeToString(sum[:]), Content: string(content)}
		sources[path] = source
		hash.Write([]byte(path))
		hash.Write([]byte{0})
		hash.Write(content)
		hash.Write([]byte{0})
	}
	return repositoryBundle{Sources: sources, SHA256: hex.EncodeToString(hash.Sum(nil))}, nil
}
func repositoryLimitsOracle(bundle repositoryBundle) (repositoryLimits, error) {
	model := bundle.Sources[repositorySourcePaths[0]].Content
	compiler := bundle.Sources[repositorySourcePaths[3]].Content
	fallbackWindow, err := repositoryConst(model, "conservativeContextWindowTokens")
	if err != nil {
		return repositoryLimits{}, err
	}
	fallbackOutput, err := repositoryConst(model, "conservativeMaxOutputTokens")
	if err != nil {
		return repositoryLimits{}, err
	}
	compilerWindow, err := repositoryConst(compiler, "defaultContextWindow")
	if err != nil {
		return repositoryLimits{}, err
	}
	compilerOutput, err := repositoryConst(compiler, "defaultMaxOutput")
	if err != nil {
		return repositoryLimits{}, err
	}
	if !repositoryCompilerUsesConfiguredOutput(compiler) {
		return repositoryLimits{}, fmt.Errorf("compiler configured max output priority changed")
	}
	required := map[string]map[string]bool{
		repositorySourcePaths[0]: {"conservativeContextWindowTokens": true, "conservativeMaxOutputTokens": true, "modelContextLimits": true},
		repositorySourcePaths[1]: {"callModel": true},
		repositorySourcePaths[2]: {"ModelContextLimits": true},
		repositorySourcePaths[3]: {"defaultContextWindow": true, "defaultMaxOutput": true, "compile": true},
	}
	for path, symbols := range required {
		for symbol := range symbols {
			if !repositorySymbolExists(bundle.Sources[path].Content, symbol) {
				return repositoryLimits{}, fmt.Errorf("required repository symbol %s is missing", symbol)
			}
		}
	}
	return repositoryLimits{FallbackWindow: fallbackWindow, FallbackOutput: fallbackOutput, CompilerWindow: compilerWindow, CompilerOutput: compilerOutput, ConfiguredOutputOverridesDefault: true, RequiredSymbols: required}, nil
}

func repositorySymbolExists(source, wanted string) bool {
	file, err := parser.ParseFile(token.NewFileSet(), "frozen.go", source, 0)
	if err != nil {
		return false
	}
	found := false
	ast.Inspect(file, func(node ast.Node) bool {
		switch value := node.(type) {
		case *ast.FuncDecl:
			found = found || value.Name.Name == wanted
		case *ast.ValueSpec:
			for _, name := range value.Names {
				found = found || name.Name == wanted
			}
		}
		return !found
	})
	return found
}

func repositoryCompilerUsesConfiguredOutput(source string) bool {
	file, err := parser.ParseFile(token.NewFileSet(), "frozen.go", source, 0)
	if err != nil {
		return false
	}
	initialized, fallback := false, false
	ast.Inspect(file, func(node ast.Node) bool {
		function, ok := node.(*ast.FuncDecl)
		if !ok || function.Name.Name != "compile" {
			return true
		}
		ast.Inspect(function.Body, func(child ast.Node) bool {
			switch value := child.(type) {
			case *ast.AssignStmt:
				if len(value.Lhs) == 1 && len(value.Rhs) == 1 && identifierName(value.Lhs[0]) == "maxOutput" {
					if selection, ok := value.Rhs[0].(*ast.SelectorExpr); ok && identifierName(selection.X) == "configuration" && selection.Sel.Name == "MaxTokens" {
						initialized = true
					}
				}
			case *ast.IfStmt:
				binary, ok := value.Cond.(*ast.BinaryExpr)
				if !ok || binary.Op != token.EQL || identifierName(binary.X) != "maxOutput" || integerLiteral(binary.Y) != 0 {
					return true
				}
				for _, statement := range value.Body.List {
					assignment, ok := statement.(*ast.AssignStmt)
					if ok && len(assignment.Lhs) == 1 && len(assignment.Rhs) == 1 && identifierName(assignment.Lhs[0]) == "maxOutput" && identifierName(assignment.Rhs[0]) == "defaultMaxOutput" {
						fallback = true
					}
				}
			}
			return true
		})
		return false
	})
	return initialized && fallback
}

func identifierName(expression ast.Expr) string {
	if value, ok := expression.(*ast.Ident); ok {
		return value.Name
	}
	return ""
}
func integerLiteral(expression ast.Expr) int {
	if value, ok := expression.(*ast.BasicLit); ok {
		parsed, _ := strconv.Atoi(value.Value)
		return parsed
	}
	return -1
}
func repositoryConst(source, name string) (int, error) {
	file, err := parser.ParseFile(token.NewFileSet(), "frozen.go", source, 0)
	if err != nil {
		return 0, err
	}
	var found string
	ast.Inspect(file, func(node ast.Node) bool {
		decl, ok := node.(*ast.ValueSpec)
		if !ok {
			return true
		}
		for i, n := range decl.Names {
			if n.Name == name && i < len(decl.Values) {
				if lit, ok := decl.Values[i].(*ast.BasicLit); ok {
					found = lit.Value
				}
			}
		}
		return true
	})
	if found == "" {
		return 0, fmt.Errorf("constant %s missing", name)
	}
	return strconv.Atoi(strings.ReplaceAll(found, "_", ""))
}
func writeLiveRepositoryEvidence(directory string, record liveRepositoryEvidence) error {
	if strings.TrimSpace(directory) == "" {
		return nil
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return errors.New("live evidence directory is unavailable")
	}
	payload, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return errors.New("live repository evidence encoding failed")
	}
	identity := sha256.Sum256([]byte(record.RunID + "\x00" + record.CaseID + "\x00" + record.Repository.SourceBundleSHA256 + "\x00" + time.Now().UTC().Format(time.RFC3339Nano)))
	return writeLiveEvidenceFile(filepath.Join(directory, "programmatic-live-repository-"+hex.EncodeToString(identity[:12])+".json"), append(payload, '\n'))
}
