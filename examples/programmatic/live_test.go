package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	executionopenai "github.com/whhhh1500/auto-agent/pkg/adapter/modelexecution/openai"
	"github.com/whhhh1500/auto-agent/pkg/adapter/modelruntime"
	programtools "github.com/whhhh1500/auto-agent/pkg/adapter/programmatic/toolcapability"
	"github.com/whhhh1500/auto-agent/pkg/app/modelsettings"
	core "github.com/whhhh1500/auto-agent/pkg/core"
	ptc "github.com/whhhh1500/auto-agent/pkg/execution/programmatic"
	provideropenai "github.com/whhhh1500/auto-agent/pkg/provider/openai"
)

// TestLiveProgrammaticSelection is an opt-in protocol acceptance test. It
// sends prompts to the configured model, but its only tools are the in-memory,
// read-only fixture capabilities from main.go. It is deliberately serial so a
// small, visible model-call budget applies independently to every case.
//
// Enable it only for an intentional live run:
//
//	HARNESS_PROGRAMMATIC_LIVE=1 go test ./examples/programmatic -run TestLiveProgrammaticSelection -v
//
// NewOpenAIAdapterFromEnv owns HARNESS_LLM_* parsing; this test neither reads
// nor logs the API key or any upstream response body.
func TestLiveProgrammaticSelection(t *testing.T) {
	if os.Getenv("HARNESS_PROGRAMMATIC_LIVE") != "1" && os.Getenv("HARNESS_ACCEPTANCE_LIVE_PROGRAMMATIC") != "1" {
		t.Skip("set HARNESS_PROGRAMMATIC_LIVE=1 to make intentional live model requests")
	}

	adapter, err := newLiveAdapter(context.Background())
	if err != nil {
		t.Fatal("live model configuration is incomplete or invalid")
	}
	modelID := strings.TrimSpace(os.Getenv("HARNESS_LLM_MODEL"))
	if modelID == "" {
		t.Fatal("live model configuration is incomplete or invalid")
	}
	minInterval, err := liveMinRequestInterval(os.Getenv("HARNESS_PROGRAMMATIC_MIN_REQUEST_INTERVAL"))
	if err != nil {
		t.Fatal("live request interval is invalid")
	}
	pacer := &liveRequestPacer{interval: minInterval}

	tests := []liveCase{
		{
			name:                 "batch_n8_batched_direct",
			activeRows:           8,
			maxModelRounds:       3,
			maxToolCalls:         9,
			requireBatchedDirect: true,
			prompt:               liveBatchedDirectPrompt(8),
		},
		{
			name:           "simple_n1_auto",
			activeRows:     1,
			maxModelRounds: 6,
			maxToolCalls:   4,
			requireProgram: false,
			prompt:         livePrompt(1, false),
		},
		{
			name:       "batch_n8_auto",
			activeRows: 8,
			// A direct strategy can legitimately obtain inventory and issue all
			// eight detail calls in one model response. If it instead emits one
			// detail call per turn, allow ten rounds so this test measures that
			// model decision rather than calling a valid direct run a budget bug.
			maxModelRounds: 10,
			maxToolCalls:   11, // catalog + execute + inventory + eight details
			requireProgram: false,
			prompt:         livePrompt(8, false),
		},
		{
			name:           "batch_n8_forced_ptc",
			activeRows:     8,
			maxModelRounds: 6,
			maxToolCalls:   11,
			requireProgram: true,
			prompt:         livePrompt(8, true),
		},
		{
			name:            "batch_n8_ptc_with_output_contract",
			activeRows:      8,
			maxModelRounds:  6,
			maxToolCalls:    11,
			requireProgram:  true,
			outputContracts: true,
			prompt:          livePrompt(8, true),
		},
	}

	for _, test := range tests {
		stopAfterTransportOrProtocolFailure := false
		passed := t.Run(test.name, func(t *testing.T) {
			// Do not use t.Parallel: each real request remains attributable to
			// one case and there is no retrying adapter in this path.
			model := newLiveModel(t, adapter, test.name, test.maxModelRounds, pacer)
			runID := "live-" + test.name
			caseStarted := time.Now()
			var result core.TurnResult
			var events []core.SessionEvent
			var runElapsed time.Duration
			var fixture *fixture
			t.Cleanup(func() {
				cleanupEvents := events
				if fixture != nil && fixture.session != nil {
					cleanupEvents = fixture.session.Events()
				}
				record := finalizedLiveEvidence(test, result, modelID, liveProtocol(), liveSourceRevision(), test.prompt, cleanupEvents, model, runElapsed, !t.Failed())
				record.FixtureEffects = liveFixtureEffectEvidenceFromFixture(fixture)
				if record.RunID == "" {
					record.RunID = runID
				}
				if writeErr := writeLiveEvidence(os.Getenv("HARNESS_PROGRAMMATIC_EVIDENCE_PATH"), record); writeErr != nil {
					t.Error("could not write live experiment evidence")
				}
			})
			fixture, err = newFixtureWithOutputContracts(model, true, test.activeRows, test.outputContracts)
			if err != nil {
				t.Fatal("could not construct live programmatic fixture")
			}
			if err := configureLiveProfile(fixture, adapter.Provider(), modelID, test.maxModelRounds, test.maxToolCalls); err != nil {
				t.Fatal("could not configure live programmatic fixture profile")
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			result, err = fixture.runtime.RunTurn(ctx, fixture.principal, fixture.session, core.TurnInput{
				RunID: runID,
				Text:  test.prompt,
			}, nil)
			runElapsed = time.Since(caseStarted)
			events = fixture.session.Events()
			diagnoseLiveProgramExecutions(t, events)
			if err != nil || result.Status != core.RunCompleted {
				stopAfterTransportOrProtocolFailure = model.stopSubsequentCases()
				t.Fatalf("live case did not complete (status=%s, model_rounds=%d)", result.Status, model.rounds())
			}
			assertLiveEvidence(t, fixture, test, model, result)
		})
		if !passed && stopAfterTransportOrProtocolFailure {
			t.Log("live model HTTP, transport, or responses protocol failure: remaining cases skipped")
			break
		}
	}
}

// newLiveAdapter performs an explicit test-only protocol selection. It never
// retries or falls back: chat retains the production compatibility adapter,
// while responses uses the production model-runtime compiler and Responses
// protocol directly. This function is reached only after the opt-in guard.
func newLiveAdapter(ctx context.Context) (core.LlmAdapter, error) {
	switch strings.TrimSpace(os.Getenv("HARNESS_PROGRAMMATIC_PROTOCOL")) {
	case "", "chat":
		return provideropenai.NewOpenAIAdapterFromEnv()
	case "responses":
		return newLiveResponsesAdapter(ctx)
	default:
		return nil, errors.New("live model protocol is invalid")
	}
}

func newLiveResponsesAdapter(ctx context.Context) (core.LlmAdapter, error) {
	baseURL := firstEnvironmentToken(os.Getenv("HARNESS_LLM_BASE_URL"))
	if baseURL == "" {
		baseURL = "https://api.openai.com/v1"
	}
	modelID := strings.TrimSpace(os.Getenv("HARNESS_LLM_MODEL"))
	key := os.Getenv("HARNESS_LLM_API_KEY")
	if modelID == "" || key == "" {
		return nil, errors.New("live model configuration is incomplete")
	}
	maxTokens, err := liveMaxTokens(os.Getenv("HARNESS_LLM_MAX_TOKENS"))
	if err != nil {
		return nil, err
	}
	plugins, err := modelruntime.NewBuiltinPluginRegistry()
	if err != nil {
		return nil, err
	}
	compiler, err := modelruntime.NewCompiler(modelruntime.CompilerOptions{Plugins: plugins})
	if err != nil {
		return nil, err
	}
	configuration := modelsettings.StoredConfiguration{
		BaseURL: baseURL, APIKey: key, Model: modelID,
		Provider: modelsettings.ProviderOpenAI, Protocol: modelsettings.ProtocolOpenAIResponses,
		MaxTokens: maxTokens, AllowedModels: splitLiveModels(os.Getenv("HARNESS_LLM_ALLOWED_MODELS")),
		Source: modelsettings.ConfigSourceLegacyFallback,
	}
	return compiler.Resolve(ctx, configuration, core.ModelSelection{Provider: string(modelsettings.ProviderOpenAI), Model: modelID})
}

func firstEnvironmentToken(value string) string {
	fields := strings.Fields(value)
	if len(fields) == 0 {
		return ""
	}
	return fields[0]
}

func liveMaxTokens(value string) (int, error) {
	if strings.TrimSpace(value) == "" {
		return 0, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return 0, errors.New("live model token budget is invalid")
	}
	return parsed, nil
}

// liveMinRequestInterval is an explicit, bounded client-side pacing control
// for acceptance runs. Zero preserves the default no-delay behavior.
func liveMinRequestInterval(value string) (time.Duration, error) {
	if strings.TrimSpace(value) == "" {
		return 0, nil
	}
	seconds, err := strconv.Atoi(value)
	if err != nil || seconds < 0 || seconds > 60 {
		return 0, errors.New("live request interval is invalid")
	}
	return time.Duration(seconds) * time.Second, nil
}

func splitLiveModels(value string) []string {
	var models []string
	for _, item := range strings.Split(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			models = append(models, item)
		}
	}
	return models
}

func TestLiveMaxTokens(t *testing.T) {
	for _, test := range []struct {
		value string
		want  int
		valid bool
	}{
		{"", 0, true},
		{"4096", 4096, true},
		{"0", 0, false},
		{"-1", 0, false},
		{"not-a-number", 0, false},
	} {
		got, err := liveMaxTokens(test.value)
		if (err == nil) != test.valid || got != test.want {
			t.Fatalf("liveMaxTokens(%q) = (%d, valid=%t), want (%d, valid=%t)", test.value, got, err == nil, test.want, test.valid)
		}
	}
}

func TestLiveMinRequestInterval(t *testing.T) {
	for _, test := range []struct {
		value string
		want  time.Duration
		valid bool
	}{
		{"", 0, true},
		{"0", 0, true},
		{"5", 5 * time.Second, true},
		{"60", 60 * time.Second, true},
		{"-1", 0, false},
		{"61", 0, false},
		{"bad", 0, false},
	} {
		got, err := liveMinRequestInterval(test.value)
		if (err == nil) != test.valid || got != test.want {
			t.Fatalf("liveMinRequestInterval(%q) = (%s, valid=%t), want (%s, valid=%t)", test.value, got, err == nil, test.want, test.valid)
		}
	}
}

func TestLiveRequestPacerHonorsCanceledContext(t *testing.T) {
	pacer := &liveRequestPacer{interval: time.Second, lastStart: time.Now()}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := pacer.Wait(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal("request pacer did not stop promptly for a canceled context")
	}
}

func TestLiveAdapterUsesExplicitResponsesProtocol(t *testing.T) {
	t.Setenv("HARNESS_PROGRAMMATIC_PROTOCOL", "responses")
	t.Setenv("HARNESS_LLM_BASE_URL", "https://example.test/v1 note-ignored")
	t.Setenv("HARNESS_LLM_API_KEY", "test-key")
	t.Setenv("HARNESS_LLM_MODEL", "test-model")
	t.Setenv("HARNESS_LLM_MAX_TOKENS", "4096")
	adapter, err := newLiveAdapter(context.Background())
	if err != nil || adapter.Provider() != string(modelsettings.ProviderOpenAI) {
		t.Fatal("explicit responses protocol did not compile its model-runtime adapter")
	}
	t.Setenv("HARNESS_PROGRAMMATIC_PROTOCOL", "not-a-protocol")
	if _, err := newLiveAdapter(context.Background()); err == nil {
		t.Fatal("unknown live protocol was not rejected without fallback")
	}
}

func TestProgramChildCallID(t *testing.T) {
	parent := "model-execute/allowed-top-level-slash"
	valid := parent + "/" + strings.Repeat("a", 64)
	for _, child := range []string{
		valid,
		"other/" + strings.Repeat("a", 64),
		parent + "/" + strings.Repeat("A", 64),
		parent + "/" + strings.Repeat("a", 63),
	} {
		got := isProgramChildCallID(parent, child)
		if got != (child == valid) {
			t.Fatalf("child identity validation mismatch")
		}
	}
}

func TestLiveResponsesErrorCodesAreRedactedAndStable(t *testing.T) {
	for _, test := range []struct {
		err  error
		code string
	}{
		{errors.New("openai responses stream did not start with created"), "stream_missing_created"},
		{fmt.Errorf("openai responses SSE event: %w", errors.New("decoder detail")), "sse_event_invalid"},
		{errors.New("openai responses unlisted local error"), "unknown_responses_protocol"},
	} {
		if class := liveErrorClass(test.err); class != "responses_protocol" {
			t.Fatalf("response protocol class = %q", class)
		}
		if got := liveErrorCode(test.err, "responses_protocol"); got != test.code {
			t.Fatalf("response protocol code = %q, want %q", got, test.code)
		}
	}
}

type liveCase struct {
	name                 string
	activeRows           int
	maxModelRounds       int
	maxToolCalls         int
	requireProgram       bool
	requireBatchedDirect bool
	outputContracts      bool
	fixtureVariant       string
	detailPaddingBytes   int
	strategyLabel        string
	prompt               string
}

func livePrompt(activeRows int, forceProgram bool) string {
	strategy := ""
	if forceProgram {
		strategy = "You must call program.catalog first, then call program.execute exactly once with a program you write from the catalog language. Do not call fixture tools directly from the model."
	}
	return fmt.Sprintf(`Use only the exposed read-only fixture tools. There are %d active inventory rows. Obtain every active item's name exactly once, ignore the inactive row, and do not request any tool outside this fixture. %s After the tools complete, reply with exactly one line and no Markdown in this format: FINAL: <active item names in inventory order, separated by comma and one space>.`, activeRows, strategy)
}

func liveBatchedDirectPrompt(activeRows int) string {
	return fmt.Sprintf(`Use only the exposed read-only fixture tools. There are %d active inventory rows. First call fixture.inventory. In your next model response, submit one independent fixture.detail call for every active inventory id, all in that single response. Do not call program.catalog or program.execute. After the tools complete, reply with exactly one line and no Markdown in this format: FINAL: <active item names in inventory order, separated by comma and one space>.`, activeRows)
}

// configureLiveProfile makes the persisted run composition name the same real
// model the adapter is configured to call. The adapter intentionally owns the
// wire-model configuration, so the test reads only the non-secret model name
// from its documented environment contract.
func configureLiveProfile(f *fixture, provider, modelID string, maxSteps, maxToolCalls int) error {
	return configureLiveProfileOptions(f, provider, modelID, maxSteps, maxToolCalls, liveProfileOptions{})
}

// configureLiveProfileWithStrategy leaves the normal profile intact. The two
// large-result control arms may add one fixed, test-only strategy directive;
// the task text, tool menu, output contracts, and all runtime budgets remain
// identical across the arms.
func configureLiveProfileWithStrategy(f *fixture, provider, modelID string, maxSteps, maxToolCalls int, strategy string) error {
	return configureLiveProfileOptions(f, provider, modelID, maxSteps, maxToolCalls, liveProfileOptions{strategy: strategy})
}

// configureLiveLargeDetailProfile is private to the large-result experiment.
// It deliberately changes only the catalog guidance's scope: catalog is
// mandatory when the model has chosen program.execute, not for every route.
// The ordinary live fixture keeps its historical instruction verbatim.
func configureLiveLargeDetailProfile(f *fixture, provider, modelID string, maxSteps, maxToolCalls int, strategy string) error {
	return configureLiveProfileOptions(f, provider, modelID, maxSteps, maxToolCalls, liveProfileOptions{strategy: strategy, conditionalProgramCatalog: true})
}

// configureLiveLargeDetailDirectOnlyProfile keeps the large-detail direct
// control's model, tool, and turn budgets aligned with the programmatic
// experiment without adding a program-selection instruction when neither
// program capability is exposed.
func configureLiveLargeDetailDirectOnlyProfile(f *fixture, provider, modelID string, maxSteps, maxToolCalls int, strategy string) error {
	return configureLiveProfileOptions(f, provider, modelID, maxSteps, maxToolCalls, liveProfileOptions{strategy: strategy, omitProgramSelection: true})
}

type liveProfileOptions struct {
	strategy                  string
	conditionalProgramCatalog bool
	omitProgramSelection      bool
}

func configureLiveProfileOptions(f *fixture, provider, modelID string, maxSteps, maxToolCalls int, options liveProfileOptions) error {
	selection := core.ModelSelection{Provider: provider, Model: modelID}
	fragments := make([]core.PromptFragment, 0, 2)
	if !options.omitProgramSelection {
		fragments = append(fragments, core.PromptFragment{
			ID: "live-programmatic-selection", Section: core.PromptInstructions,
			Content: liveProgramSelectionInstruction(options.conditionalProgramCatalog),
		})
	}
	if options.strategy != "" {
		instruction, ok := liveStrategyInstruction(options.strategy)
		if !ok {
			return errors.New("live strategy label is invalid")
		}
		fragments = append(fragments, core.PromptFragment{ID: "live-large-result-strategy", Section: core.PromptInstructions, Content: instruction})
	}
	return f.runtime.Profiles.Bind(core.AgentProfileLayer{
		Scope: f.session.Scope(), ProfileID: fixtureProfileID,
		Model: &selection, MaxSteps: &maxSteps, MaxToolCalls: &maxToolCalls, PutFragments: fragments,
	})
}

func liveProgramSelectionInstruction(conditionalProgramCatalog bool) string {
	if conditionalProgramCatalog {
		return "Choose among the tools currently exposed: call an ordinary tool directly for a simple action or when the next decision requires your interpretation; use program.execute for deterministic, bounded loops, branches and data processing. When choosing program.execute, first obtain program bindings from program.catalog and follow the program tool description. If program.execute is not exposed, use the available direct tools. Treat returned tool outcomes as evidence before deciding the next step."
	}
	// Keep this established ordinary-live baseline text byte-for-byte stable.
	return "Choose among the tools currently exposed: call an ordinary tool directly for a simple action or when the next decision requires your interpretation; use program.execute for deterministic, bounded loops, branches and data processing. First obtain program bindings from program.catalog and follow the program tool description. If program.execute is not exposed, use the available direct tools. Treat returned tool outcomes as evidence before deciding the next step."
}

func liveStrategyInstruction(strategy string) (string, bool) {
	switch strategy {
	case "forced_batched_direct":
		return "Test control strategy: after fixture.inventory succeeds, submit every independent fixture.detail call in one model response. Do not call program.catalog or program.execute.", true
	case "forced_batched_direct_only":
		return "Test control strategy: after fixture.inventory succeeds, submit every independent fixture.detail call in one model response.", true
	case "forced_ptc":
		return "Test control strategy: call program.catalog, then call program.execute exactly once with a program you write from the catalog language. Do not call fixture tools directly from the model.", true
	default:
		return "", false
	}
}

type liveRound struct {
	round            int
	runID            string
	step             int
	adapterStarted   bool
	pacingCanceled   bool
	tools            []string
	toolCalls        int
	usage            core.TokenUsage
	hasUsage         bool
	usageConsistent  bool
	elapsed          time.Duration
	pacingWait       time.Duration
	providerElapsed  time.Duration
	finish           string
	status           int
	errorClass       string
	errorType        string
	errorCode        string
	systemBytes      int
	messageBytes     int
	toolSchemaBytes  int
	toolSchemaNames  []string
	toolSchemaSHA256 string
}

// liveModel records only safe protocol metadata. In particular it never logs
// prompts, tool arguments, assistant text, provider errors, HTTP bodies, or
// credentials. It calls the supplied adapter exactly once per model round.
type liveModel struct {
	inner     core.LlmAdapter
	t         *testing.T
	caseName  string
	maxRounds int
	pacer     *liveRequestPacer

	mu           sync.Mutex
	roundsN      int
	observations []liveRound
	stopCases    bool
}

func newLiveModel(t *testing.T, inner core.LlmAdapter, caseName string, maxRounds int, pacer *liveRequestPacer) *liveModel {
	return &liveModel{inner: inner, t: t, caseName: caseName, maxRounds: maxRounds, pacer: pacer}
}

func (m *liveModel) Provider() string { return m.inner.Provider() }

// ArtifactRevision forwards the optional core revision interface unchanged.
// A missing, nil, or panicking implementation is treated as unavailable so
// the v2 admission check remains fail-closed without exposing revision data.
func (m *liveModel) ArtifactRevision() (revision string) {
	defer func() {
		if recover() != nil {
			revision = ""
		}
	}()
	if m == nil || m.inner == nil {
		return ""
	}
	revisioner, ok := m.inner.(core.ArtifactRevisioner)
	if !ok {
		return ""
	}
	return revisioner.ArtifactRevision()
}

// ModelContextLimits forwards an optional adapter capability without changing
// the live instrumentation contract. An unavailable, panicking, or malformed
// report returns zero values so Core retains its conservative fallback.
func (m *liveModel) ModelContextLimits() (contextWindowTokens, maxOutputTokens int) {
	defer func() {
		if recover() != nil {
			contextWindowTokens, maxOutputTokens = 0, 0
		}
	}()
	if m == nil || m.inner == nil {
		return 0, 0
	}
	reporter, ok := m.inner.(interface{ ModelContextLimits() (int, int) })
	if !ok {
		return 0, 0
	}
	contextWindowTokens, maxOutputTokens = reporter.ModelContextLimits()
	if contextWindowTokens <= 0 || maxOutputTokens <= 0 || maxOutputTokens >= contextWindowTokens {
		return 0, 0
	}
	return contextWindowTokens, maxOutputTokens
}

func (m *liveModel) Stream(ctx context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	m.mu.Lock()
	if m.roundsN >= m.maxRounds {
		m.mu.Unlock()
		return errors.New("live model round budget reached")
	}
	m.roundsN++
	round := m.roundsN
	m.mu.Unlock()

	started := time.Now()
	systemBytes := len(options.System)
	messageBytes := liveJSONBytes(options.Messages)
	toolSchemaBytes := liveJSONBytes(options.Tools)
	toolSchemaNames := liveToolSchemaNames(options.Tools)
	toolSchemaSHA256 := liveSHA256(options.Tools)
	pacingStarted := time.Now()
	seen := map[string]struct{}{}
	seenCalls := map[string]struct{}{}
	var usage core.TokenUsage
	hasUsage := false
	usageConsistent := true
	finish := ""
	var err error
	if m.pacer != nil {
		err = m.pacer.Wait(ctx)
	}
	pacingWait := time.Since(pacingStarted)
	providerElapsed := time.Duration(0)
	adapterStarted := false
	pacingCanceled := err != nil
	if err == nil {
		adapterStarted = true
		providerStarted := time.Now()
		err = m.inner.Stream(ctx, options, func(chunk core.StreamChunk) {
			if chunk.ToolCall != nil && chunk.ToolCall.Name != "" {
				seen[chunk.ToolCall.Name] = struct{}{}
				seenCalls[liveToolCallKey(*chunk.ToolCall)] = struct{}{}
			}
			for _, call := range chunk.ToolCalls {
				if call.Name != "" {
					seen[call.Name] = struct{}{}
					seenCalls[liveToolCallKey(call)] = struct{}{}
				}
			}
			if chunk.Usage != nil {
				if finish != "" || chunk.Usage.InputTokens < 0 || chunk.Usage.OutputTokens < 0 || chunk.Usage.InputTokens > core.MaxReportedTokensPerCall || chunk.Usage.OutputTokens > core.MaxReportedTokensPerCall {
					usageConsistent = false
				} else if hasUsage {
					usageConsistent = false
				} else {
					usage = *chunk.Usage
					hasUsage = true
				}
			}
			if chunk.Kind == core.StreamKindFinish {
				finish = chunk.FinishKind
			}
			emit(chunk)
		})
		providerElapsed = time.Since(providerStarted)
	}
	duration := time.Since(started)
	status := liveHTTPStatus(err)
	errorClass := liveErrorClass(err)
	errorType := liveErrorType(err)
	errorCode := liveErrorCode(err, errorClass)
	tools := sortedKeys(seen)
	m.mu.Lock()
	m.observations = append(m.observations, liveRound{round: round, runID: options.ModelCall.RunID, step: options.ModelCall.Step, adapterStarted: adapterStarted, pacingCanceled: pacingCanceled, tools: tools, toolCalls: len(seenCalls), usage: usage, hasUsage: hasUsage, usageConsistent: usageConsistent, elapsed: duration, pacingWait: pacingWait, providerElapsed: providerElapsed, finish: finish, status: status, errorClass: errorClass, errorType: errorType, errorCode: errorCode, systemBytes: systemBytes, messageBytes: messageBytes, toolSchemaBytes: toolSchemaBytes, toolSchemaNames: toolSchemaNames, toolSchemaSHA256: toolSchemaSHA256})
	m.mu.Unlock()
	if err != nil && (errorClass == "http_status" || errorClass == "transport" || errorClass == "responses_protocol") {
		m.stopCases = true
	}
	m.t.Logf("live case=%s round=%d tools=%s input_tokens=%d output_tokens=%d elapsed_ms=%d http_status=%d error_class=%s error_type=%s error_code=%s",
		m.caseName, round, strings.Join(tools, ","), usage.InputTokens, usage.OutputTokens, duration.Milliseconds(), status, errorClass, errorType, errorCode)
	if err != nil {
		return errors.New("live model request failed; upstream details suppressed")
	}
	return nil
}

func liveToolCallKey(call core.ToolCall) string {
	if call.ID != "" {
		return call.ID
	}
	return "name:" + call.Name
}

func liveJSONBytes(value any) int {
	encoded, err := json.Marshal(value)
	if err != nil {
		return 0
	}
	return len(encoded)
}

// liveSHA256 records a stable digest of already-public structural input. It is
// used for menu-consistency evidence; if a value cannot be encoded, the
// absence of a digest is explicit rather than substituting a made-up value.
func liveSHA256(value any) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(encoded)
	return fmt.Sprintf("%x", sum)
}

// liveToolSchemaNames retains only public tool IDs in the order actually
// passed to the model adapter. Descriptions and schemas remain out of live
// evidence; toolSchemaBytes separately records the serialized schema size.
func liveToolSchemaNames(schemas []core.ToolSchema) []string {
	out := make([]string, 0, len(schemas))
	for _, schema := range schemas {
		out = append(out, schema.Name)
	}
	return out
}

// liveRequestPacer serializes start times across every case in one live test
// invocation. It deliberately holds its mutex during a bounded context-aware
// wait so concurrent callers cannot reserve the same start slot.
type liveRequestPacer struct {
	interval  time.Duration
	mu        sync.Mutex
	lastStart time.Time
}

func (p *liveRequestPacer) Wait(ctx context.Context) error {
	if p == nil || p.interval <= 0 {
		return nil
	}
	if ctx == nil {
		return errors.New("live request context is nil")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	if p.lastStart.IsZero() || !now.Before(p.lastStart.Add(p.interval)) {
		p.lastStart = now
		return nil
	}
	timer := time.NewTimer(time.Until(p.lastStart.Add(p.interval)))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		p.lastStart = time.Now()
		return nil
	}
}

func (m *liveModel) rounds() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.roundsN
}

func (m *liveModel) stopSubsequentCases() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.stopCases
}

func (m *liveModel) usage() (core.TokenUsage, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var total core.TokenUsage
	started := 0
	for _, round := range m.observations {
		if !round.adapterStarted {
			continue
		}
		started++
		if !round.hasUsage || !round.usageConsistent {
			return core.TokenUsage{}, false
		}
		total.InputTokens += round.usage.InputTokens
		total.OutputTokens += round.usage.OutputTokens
	}
	return total, started > 0
}

func (m *liveModel) selectedFixtureTool() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, round := range m.observations {
		for _, tool := range round.tools {
			if tool == listToolID || tool == detailToolID {
				return true
			}
		}
	}
	return false
}

func (m *liveModel) toolCallCount(roundNumber int) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, round := range m.observations {
		if round.round == roundNumber {
			return round.toolCalls
		}
	}
	return 0
}

func liveHTTPStatus(err error) int {
	var legacy *provideropenai.HTTPStatusError
	if errors.As(err, &legacy) {
		return legacy.Status
	}
	var execution *executionopenai.HTTPStatusError
	if errors.As(err, &execution) {
		return execution.Status
	}
	return 0
}

// liveErrorClass is deliberately a closed diagnostic vocabulary. Never put an
// arbitrary provider error into a test log: an upstream error can contain
// request fragments or other sensitive gateway details.
func liveErrorClass(err error) string {
	if err == nil {
		return "none"
	}
	if liveHTTPStatus(err) != 0 {
		return "http_status"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "deadline"
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	message := err.Error()
	switch {
	case message == "openai HTTP transport failed":
		return "transport"
	case strings.HasPrefix(message, "openai responses "):
		return "responses_protocol"
	case strings.HasPrefix(message, "invalid model execution stream state"):
		return "model_execution"
	default:
		return "redacted_unknown"
	}
}

func liveErrorType(err error) string {
	if err == nil {
		return "none"
	}
	var legacy *provideropenai.HTTPStatusError
	if errors.As(err, &legacy) {
		return "provider_openai_http_status"
	}
	var execution *executionopenai.HTTPStatusError
	if errors.As(err, &execution) {
		return "modelexecution_openai_http_status"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "context_deadline"
	}
	if errors.Is(err, context.Canceled) {
		return "context_canceled"
	}
	if strings.HasPrefix(err.Error(), "openai responses ") {
		return "responses_protocol_error"
	}
	return "redacted_unknown_error"
}

// liveErrorCode maps only stable local parser outcomes. Prefixes that can
// contain a wrapped decoder error intentionally collapse to one code.
func liveErrorCode(err error, class string) string {
	if err == nil {
		return "none"
	}
	if class != "responses_protocol" {
		return class
	}
	message := err.Error()
	known := map[string]string{
		"openai responses execution is invalid":                 "execution_invalid",
		"openai responses stream ended before completion":       "stream_ended_before_completion",
		"openai responses stream did not start with created":    "stream_missing_created",
		"openai responses sequence is invalid":                  "sequence_invalid",
		"openai responses stream bounds or terminal state":      "stream_bounds_or_terminal",
		"openai responses completion is invalid":                "completion_invalid",
		"openai responses terminal failure":                     "terminal_failure",
		"openai responses single response is invalid":           "single_response_invalid",
		"openai responses built-in output is unsupported":       "builtin_output_unsupported",
		"openai responses output bound":                         "output_bound",
		"openai responses output item is invalid":               "output_item_invalid",
		"openai responses duplicate output item":                "duplicate_output_item",
		"openai responses function identity is invalid":         "function_identity_invalid",
		"openai responses function completion is out of order":  "function_completion_order",
		"openai responses function completion is invalid":       "function_completion_invalid",
		"openai responses function delta is invalid":            "function_delta_invalid",
		"openai responses content delta is invalid":             "content_delta_invalid",
		"openai responses content completion is invalid":        "content_completion_invalid",
		"openai responses content completion is missing":        "content_completion_missing",
		"openai responses content identity is invalid":          "content_identity_invalid",
		"openai responses final content conflicts":              "final_content_conflict",
		"openai responses final content count conflicts":        "final_content_count_conflict",
		"openai responses final output identity drift":          "final_output_identity_drift",
		"openai responses final function identity drift":        "final_function_identity_drift",
		"openai responses final message content is unsupported": "final_message_content_unsupported",
		"openai responses lifecycle identity drift":             "lifecycle_identity_drift",
		"openai responses content is unsupported":               "content_unsupported",
		"openai responses output identity is invalid":           "output_identity_invalid",
		"openai responses output index is invalid":              "output_index_invalid",
	}
	if code, ok := known[message]; ok {
		return code
	}
	if strings.HasPrefix(message, "openai responses SSE event:") {
		return "sse_event_invalid"
	}
	return "unknown_responses_protocol"
}

func sortedKeys(items map[string]struct{}) []string {
	keys := make([]string, 0, len(items))
	for key := range items {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// diagnoseLiveProgramExecutions records only structural PTC evidence before
// assertions can stop a failing acceptance case. It intentionally hashes the
// submitted source and inspects decoded catalog/result values without logging
// source, arguments, tool content, assistant output, or upstream errors.
func diagnoseLiveProgramExecutions(t *testing.T, events []core.SessionEvent) {
	t.Helper()
	for index, attempt := range liveProgramAttempts(events) {
		t.Logf("program_execute index=%d source_bytes=%d source_sha256=%s compile_ok=%t compile_code=%s compile_diagnostic=%s composite_literal_scan_applicable=%t composite_literal_scan_observed=%t composite_literal_array_count=%d composite_literal_map_count=%d bindings_exact=%t compile_tools_bindings_exact=%t catalog_fixture_bindings_exact=%t compiled_tool_count=%d submitted_binding_count=%d missing_compiled_tool_keys=%d extraneous_submitted_binding_keys=%d catalog_digest_match_count=%d catalog_digest_mismatch_count=%d catalog_digest_matching=%t wire_alias_evidence_available=%t submitted_binding_keys_use_wire_alias=%t submitted_wire_alias_key_count=%d result_found=%t result_ok=%t metadata_code=%s metadata_error_code=%s metadata_message=%s diagnostic=%s",
			index+1, attempt.SourceBytes, attempt.SourceSHA256, attempt.CompileOK, attempt.CompileCode, attempt.CompileDiagnostic, attempt.CompositeLiteralScanApplicable, attempt.CompositeLiteralScanObserved, attempt.CompositeLiteralArrayCount, attempt.CompositeLiteralMapCount, attempt.BindingsExact, attempt.CompileToolsBindingsExact, attempt.CatalogFixtureBindingsExact, attempt.CompiledToolCount, attempt.SubmittedBindingCount, attempt.MissingCompiledToolKeys, attempt.ExtraneousSubmittedBindingKeys, attempt.CatalogDigestMatchCount, attempt.CatalogDigestMismatchCount, attempt.CatalogDigestMatching, attempt.WireAliasEvidenceAvailable, attempt.SubmittedBindingKeysUseWireAlias, attempt.SubmittedWireAliasKeyCount, attempt.ResultFound, attempt.ResultOK, attempt.MetadataCode, attempt.MetadataErrorCode, attempt.MetadataMessage, attempt.Diagnostic)
	}
}

// liveProgramAttempt is a data-free description of one submitted PTC program.
// The same computation feeds both human diagnostic logs and persisted evidence.
type liveProgramAttempt struct {
	SourceSHA256                   string `json:"source_sha256"`
	SourceBytes                    int    `json:"source_bytes"`
	CompileOK                      bool   `json:"compile_ok"`
	CompileCode                    string `json:"compile_code"`
	CompileDiagnostic              string `json:"compile_diagnostic"`
	CompositeLiteralScanApplicable bool   `json:"composite_literal_scan_applicable"`
	CompositeLiteralScanObserved   bool   `json:"composite_literal_scan_observed"`
	CompositeLiteralArrayCount     int    `json:"composite_literal_array_count"`
	CompositeLiteralMapCount       int    `json:"composite_literal_map_count"`
	BindingsExact                  bool   `json:"bindings_exact"`
	CompileToolsBindingsExact      bool   `json:"compile_tools_bindings_exact"`
	CatalogFixtureBindingsExact    bool   `json:"catalog_fixture_bindings_exact"`
	CompiledToolCount              int    `json:"compiled_tool_count"`
	SubmittedBindingCount          int    `json:"submitted_binding_count"`
	MissingCompiledToolKeys        int    `json:"missing_compiled_tool_keys"`
	ExtraneousSubmittedBindingKeys int    `json:"extraneous_submitted_binding_keys"`
	CatalogDigestMatchCount        int    `json:"catalog_digest_match_count"`
	CatalogDigestMismatchCount     int    `json:"catalog_digest_mismatch_count"`
	CatalogDigestMatching          bool   `json:"catalog_digest_matching"`
	// OpenAI protocol aliases are reversed before Core records a ToolCall.
	// These fields distinguish no alias observed from wire-level alias proof,
	// which cannot be reconstructed safely from Session events.
	WireAliasEvidenceAvailable       bool   `json:"wire_alias_evidence_available"`
	SubmittedBindingKeysUseWireAlias bool   `json:"submitted_binding_keys_use_wire_alias"`
	SubmittedWireAliasKeyCount       int    `json:"submitted_wire_alias_key_count"`
	ResultFound                      bool   `json:"result_found"`
	ResultOK                         bool   `json:"result_ok"`
	Diagnostic                       string `json:"diagnostic"`
	MetadataCode                     string `json:"-"`
	MetadataErrorCode                string `json:"-"`
	MetadataMessage                  string `json:"-"`
}

func liveProgramAttempts(events []core.SessionEvent) []liveProgramAttempt {
	calls, results, _, _ := liveSessionEvidence(events)
	resultsByCall := make(map[string]core.ToolResultData, len(results))
	for _, result := range results {
		resultsByCall[result.CallID] = result
	}
	attempts := []liveProgramAttempt{}
	for _, call := range calls {
		if call.Name != programtools.ExecuteID {
			continue
		}
		source, _ := call.Args["source"].(string)
		sum := sha256.Sum256([]byte(source))
		program, compileErr := ptc.Compile([]byte(source))
		compileCode, compileDiagnostic := liveCompileCode(compileErr)
		compositeLiterals := liveCompositeLiteralScan(source, compileDiagnostic)
		submittedBindings, submittedKeys := liveSubmittedBindings(call.Args["bindings"])
		catalogBindings, catalogOK := liveCatalogBindingsBefore(events, call.CallID)
		bindingEvidence := liveBindingEvidence(program, submittedBindings, submittedKeys, catalogBindings, catalogOK)
		result, resultFound := resultsByCall[call.CallID]
		resultOK, code, errorCode, message, diagnostic := false, "absent", "absent", "absent", "absent"
		if resultFound {
			resultOK = result.OK
			code = liveKnownProgramMetadata(result.Metadata, "code")
			errorCode = liveKnownProgramMetadata(result.Metadata, "error_code")
			message = liveKnownProgramMetadata(result.Metadata, "message")
			diagnostic = liveKnownProgramDiagnostic(result.Metadata)
		}
		attempts = append(attempts, liveProgramAttempt{
			SourceSHA256: fmt.Sprintf("%x", sum), SourceBytes: len(source), CompileOK: compileErr == nil, CompileCode: compileCode, CompileDiagnostic: compileDiagnostic,
			CompositeLiteralScanApplicable: compositeLiterals.applicable, CompositeLiteralScanObserved: compositeLiterals.observed,
			CompositeLiteralArrayCount: compositeLiterals.arrays, CompositeLiteralMapCount: compositeLiterals.maps,
			BindingsExact: bindingEvidence.toolsExact && bindingEvidence.catalogDigestMatching, CompileToolsBindingsExact: bindingEvidence.toolsExact, CatalogFixtureBindingsExact: bindingEvidence.catalogDigestMatching,
			CompiledToolCount: bindingEvidence.compiledToolCount, SubmittedBindingCount: bindingEvidence.submittedBindingCount,
			MissingCompiledToolKeys: bindingEvidence.missingCompiledToolKeys, ExtraneousSubmittedBindingKeys: bindingEvidence.extraneousSubmittedBindingKeys,
			CatalogDigestMatchCount: bindingEvidence.catalogDigestMatchCount, CatalogDigestMismatchCount: bindingEvidence.catalogDigestMismatchCount,
			CatalogDigestMatching:      bindingEvidence.catalogDigestMatching,
			WireAliasEvidenceAvailable: false, SubmittedBindingKeysUseWireAlias: false, SubmittedWireAliasKeyCount: 0,
			ResultFound: resultFound, ResultOK: resultOK, Diagnostic: diagnostic,
			MetadataCode: code, MetadataErrorCode: errorCode, MetadataMessage: message,
		})
	}
	return attempts
}

func liveSubmittedBindings(value any) (map[string]string, map[string]struct{}) {
	values, ok := value.(map[string]any)
	if !ok {
		return nil, nil
	}
	bindings := make(map[string]string, len(values))
	keys := make(map[string]struct{}, len(values))
	for key, value := range values {
		keys[key] = struct{}{}
		if text, ok := value.(string); ok {
			bindings[key] = text
		}
	}
	return bindings, keys
}

type liveBindingsEvidence struct {
	compiledToolCount              int
	submittedBindingCount          int
	missingCompiledToolKeys        int
	extraneousSubmittedBindingKeys int
	catalogDigestMatchCount        int
	catalogDigestMismatchCount     int
	toolsExact                     bool
	catalogDigestMatching          bool
}

func liveBindingEvidence(program *ptc.Program, submitted map[string]string, submittedKeys map[string]struct{}, catalog map[string]string, catalogOK bool) liveBindingsEvidence {
	evidence := liveBindingsEvidence{submittedBindingCount: len(submittedKeys)}
	if program == nil {
		return evidence
	}
	tools := program.Tools()
	evidence.compiledToolCount = len(tools)
	compiled := make(map[string]struct{}, len(tools))
	for _, tool := range tools {
		compiled[tool] = struct{}{}
		if _, present := submittedKeys[tool]; !present {
			evidence.missingCompiledToolKeys++
		}
	}
	for key := range submittedKeys {
		if _, expected := compiled[key]; !expected {
			evidence.extraneousSubmittedBindingKeys++
		}
	}
	evidence.toolsExact = evidence.missingCompiledToolKeys == 0 && evidence.extraneousSubmittedBindingKeys == 0
	if !catalogOK {
		return evidence
	}
	for _, tool := range tools {
		submittedDigest, submittedOK := submitted[tool]
		catalogDigest, catalogHasTool := catalog[tool]
		if submittedOK && submittedDigest != "" && catalogHasTool && catalogDigest != "" && submittedDigest == catalogDigest {
			evidence.catalogDigestMatchCount++
			continue
		}
		evidence.catalogDigestMismatchCount++
	}
	evidence.catalogDigestMatching = evidence.toolsExact && evidence.catalogDigestMismatchCount == 0
	return evidence
}

func liveCatalogBindingsBefore(events []core.SessionEvent, executeCallID string) (map[string]string, bool) {
	catalogCalls := make(map[string]struct{})
	var latest map[string]string
	for _, event := range events {
		switch event.Type {
		case core.EvToolCall:
			var call core.ToolCallData
			if json.Unmarshal(event.Data, &call) != nil {
				continue
			}
			if call.CallID == executeCallID {
				if latest == nil {
					return nil, false
				}
				return latest, true
			}
			if call.Name == programtools.CatalogID {
				catalogCalls[call.CallID] = struct{}{}
			}
		case core.EvToolResult:
			var result core.ToolResultData
			if json.Unmarshal(event.Data, &result) != nil || !result.OK {
				continue
			}
			if _, catalogCall := catalogCalls[result.CallID]; !catalogCall {
				continue
			}
			if bindings, ok := liveCatalogBindings(result.Content); ok {
				latest = bindings
			}
		}
	}
	return nil, false
}

func liveCatalogBindings(content string) (map[string]string, bool) {
	var catalog struct {
		Tools []struct {
			Schema struct {
				Name string `json:"name"`
			} `json:"schema"`
			BindingDigest string `json:"binding_digest"`
		} `json:"tools"`
	}
	if json.Unmarshal([]byte(content), &catalog) != nil || len(catalog.Tools) == 0 {
		return nil, false
	}
	bindings := make(map[string]string, len(catalog.Tools))
	for _, tool := range catalog.Tools {
		if tool.Schema.Name == "" || tool.BindingDigest == "" {
			return nil, false
		}
		if _, duplicate := bindings[tool.Schema.Name]; duplicate {
			return nil, false
		}
		bindings[tool.Schema.Name] = tool.BindingDigest
	}
	return bindings, true
}

func liveCompileCode(err error) (string, string) {
	diagnostic := liveCompileDiagnostic(err)
	switch {
	case err == nil:
		return "none", "absent"
	case diagnostic != "absent":
		return "invalid_program", diagnostic
	case errors.Is(err, ptc.ErrInvalidProgram):
		return "invalid_program", "absent"
	case errors.Is(err, ptc.ErrProgramLimit):
		return "program_limit", "absent"
	default:
		return "unknown_compile_error", "absent"
	}
}

func liveCompileDiagnostic(err error) string {
	diagnostic, ok := ptc.Diagnostic(err)
	if !ok || !liveKnownCompileDiagnostic(diagnostic) {
		return "absent"
	}
	return diagnostic
}

func liveKnownCompileDiagnostic(value string) bool {
	return map[string]bool{
		"compile_source_invalid": true, "compile_json_invalid": true, "compile_limit_exceeded": true,
		"compile_top_level_invalid": true, "compile_version_invalid": true, "compile_body_invalid": true,
		"compile_statement_invalid": true, "compile_statement_op_invalid": true,
		"compile_expression_invalid": true, "compile_expression_op_invalid": true,
	}[value]
}

type liveCompositeLiteralScanEvidence struct {
	applicable bool
	observed   bool
	arrays     int
	maps       int
}

// liveCompositeLiteralScan examines only a bounded, already-classified
// compile_expression_invalid source. It records shape counts, never source
// text, keys, values, or decoded program data.
func liveCompositeLiteralScan(source, diagnostic string) liveCompositeLiteralScanEvidence {
	evidence := liveCompositeLiteralScanEvidence{applicable: diagnostic == "compile_expression_invalid"}
	if !evidence.applicable || len(source) == 0 || len(source) > ptc.HardMaxSourceBytes {
		return evidence
	}
	decoder := json.NewDecoder(strings.NewReader(source))
	decoder.UseNumber()
	var root any
	if decoder.Decode(&root) != nil || decoder.More() {
		return evidence
	}
	if token, err := decoder.Token(); err != io.EOF || token != nil {
		return evidence
	}
	seen := 0
	if !liveCountCompositeLiterals(root, 0, &seen, &evidence) {
		return liveCompositeLiteralScanEvidence{applicable: true}
	}
	evidence.observed = true
	return evidence
}

func liveCountCompositeLiterals(value any, depth int, seen *int, evidence *liveCompositeLiteralScanEvidence) bool {
	if depth > ptc.HardMaxJSONDepth || *seen >= ptc.HardMaxNodes {
		return false
	}
	(*seen)++
	switch typed := value.(type) {
	case map[string]any:
		if typed["op"] == "literal" {
			switch typed["value"].(type) {
			case []any:
				evidence.arrays++
			case map[string]any:
				evidence.maps++
			}
			return true
		}
		for _, item := range typed {
			if !liveCountCompositeLiterals(item, depth+1, seen, evidence) {
				return false
			}
		}
	case []any:
		for _, item := range typed {
			if !liveCountCompositeLiterals(item, depth+1, seen, evidence) {
				return false
			}
		}
	}
	return true
}

func liveKnownProgramMetadata(metadata map[string]any, key string) string {
	value, ok := metadata[key].(string)
	if !ok || value == "" {
		return "absent"
	}
	known := map[string]bool{
		"invalid_args": true, "program_invalid": true, "program_bindings_mismatch": true,
		"program_invocation_invalid": true, "program_limit_exceeded": true,
		"program_tool_data_invalid": true, "program_output_too_large": true,
		"program_catalog_unavailable": true, "program_catalog_invalid": true,
	}
	if known[value] {
		return value
	}
	return "unrecognized"
}

func liveKnownProgramDiagnostic(metadata map[string]any) string {
	value, ok := metadata["diagnostic"].(string)
	if !ok || value == "" {
		return "absent"
	}
	known := map[string]bool{
		"compile_source_invalid": true, "compile_json_invalid": true, "compile_limit_exceeded": true,
		"compile_top_level_invalid": true, "compile_version_invalid": true, "compile_body_invalid": true,
		"compile_statement_invalid": true, "compile_statement_op_invalid": true,
		"compile_expression_invalid": true, "compile_expression_op_invalid": true,
		"runtime_invalid": true, "runtime_append_target_not_list": true,
		"runtime_call_args_not_object": true, "runtime_compare_incompatible": true,
		"runtime_for_not_list": true, "runtime_get_missing_key": true,
		"runtime_get_not_object": true, "runtime_index_invalid": true,
		"runtime_integer_invalid": true, "runtime_length_unsupported": true,
		"runtime_variable_missing": true,
	}
	if known[value] {
		return value
	}
	return "unrecognized"
}

type liveEvidenceRecord struct {
	Schema             string                    `json:"schema"`
	CaseID             string                    `json:"case_id"`
	RunID              string                    `json:"run_id"`
	OutputContracts    bool                      `json:"output_contracts"`
	FixtureVariant     string                    `json:"fixture_variant,omitempty"`
	DetailPaddingBytes int                       `json:"detail_padding_bytes,omitempty"`
	Strategy           string                    `json:"strategy,omitempty"`
	RequestedModel     string                    `json:"requested_model"`
	Protocol           string                    `json:"protocol"`
	SourceRevision     string                    `json:"source_revision"`
	PromptSHA256       string                    `json:"prompt_sha256"`
	RuntimeStatus      string                    `json:"runtime_status"`
	AcceptancePassed   bool                      `json:"acceptance_passed"`
	TotalElapsedMS     int64                     `json:"total_elapsed_ms"`
	PacingWaitMS       int64                     `json:"pacing_wait_ms"`
	ProviderRequestMS  int64                     `json:"provider_request_ms"`
	ToolResults        liveEvidenceTools         `json:"tool_results"`
	FixtureEffects     liveFixtureEffectEvidence `json:"fixture_effects"`
	Rounds             []liveEvidenceRound       `json:"rounds"`
	InvocationEvidence *liveInvocationEvidence   `json:"invocation_evidence,omitempty"`
	ProgramAttempts    []liveProgramAttempt      `json:"program_attempts"`
	LargeDetail        *liveLargeDetailEvidence  `json:"large_detail,omitempty"`
	Memory             *liveMemoryEvidence       `json:"memory,omitempty"`
	Summary            *liveSummaryEvidence      `json:"summary,omitempty"`
}

type liveEvidenceTools struct {
	Observed            bool `json:"observed"`
	Succeeded           int  `json:"succeeded"`
	Failed              int  `json:"failed"`
	BudgetDeniedResults int  `json:"budget_denied_results"`
}

// liveInvocationEvidence contains aggregate predicates and counts only. The
// ModelCall RunID/Step and durable invocation IDs remain in memory so evidence
// can reconcile them without persisting additional identifiers.
type liveInvocationEvidence struct {
	AdapterCallsObserved        bool `json:"adapter_calls_observed"`
	ActualAdapterCalls          int  `json:"actual_adapter_calls"`
	PacingCanceledBeforeAdapter int  `json:"pacing_canceled_before_adapter"`
	ReportedUsageComplete       bool `json:"reported_usage_complete"`
	UsageProtocolConsistent     bool `json:"usage_protocol_consistent"`
	LedgerUsageMatched          bool `json:"ledger_usage_matched"`
	UniqueLedgerInvocationIDs   bool `json:"unique_ledger_invocation_ids"`
}

// liveFixtureEffectEvidence comes from the fixture providers themselves,
// rather than inferred Session events. Observed=false means setup did not
// produce a fixture from which actual effects can be read.
type liveFixtureEffectEvidence struct {
	Observed       bool `json:"observed"`
	InventoryCalls int  `json:"inventory_calls"`
	DetailCalls    int  `json:"detail_calls"`
}

type liveEvidenceRound struct {
	Round             int      `json:"round"`
	InputTokens       int64    `json:"input_tokens"`
	OutputTokens      int64    `json:"output_tokens"`
	UsageReported     bool     `json:"usage_reported"`
	ToolNames         []string `json:"tool_names"`
	ToolCallCount     int      `json:"tool_call_count"`
	Finish            string   `json:"finish"`
	HTTPStatus        int      `json:"http_status"`
	ErrorClass        string   `json:"error_class"`
	ErrorType         string   `json:"error_type"`
	ErrorCode         string   `json:"error_code"`
	PacingWaitMS      int64    `json:"pacing_wait_ms"`
	ProviderRequestMS int64    `json:"provider_request_ms"`
	SystemBytes       int      `json:"system_bytes"`
	MessageBytes      int      `json:"message_bytes"`
	ToolSchemaBytes   int      `json:"tool_schema_bytes"`
	ToolSchemaNames   []string `json:"tool_schema_names"`
	ToolSchemaSHA256  string   `json:"tool_schema_sha256"`
}

// liveMemoryEvidence contains only predicates and counts. It deliberately
// excludes the opaque memory value, tool arguments, tool content, and model
// text, which may contain the fixture nonce.
type liveMemoryEvidence struct {
	RecallResultFound    bool `json:"recall_result_found"`
	RecallEntriesParsed  bool `json:"recall_entries_parsed"`
	RecallEntryCount     int  `json:"recall_entry_count"`
	ContextExactMatch    bool `json:"context_exact_match"`
	FirstTurnObserved    bool `json:"first_turn_observed"`
	NonceCheckApplicable bool `json:"nonce_check_applicable"`
	FirstTurnNonceAbsent bool `json:"first_turn_nonce_absent"`
	AnswerMatches        bool `json:"answer_matches"`
	DurableAnswerMatches bool `json:"durable_answer_matches"`
	UsageMatches         bool `json:"usage_matches"`
}

// liveSummaryEvidence keeps only aggregate request/usage accounting and
// acceptance predicates. It never persists synthetic history, summary text,
// final model text, or the opaque history fact.
type liveSummaryEvidence struct {
	SyntheticHistory      bool            `json:"synthetic_history"`
	SummaryMode           string          `json:"summary_mode"`
	SummaryRequests       int             `json:"summary_requests"`
	AnswerRequests        int             `json:"answer_requests"`
	TotalRequests         int             `json:"total_requests"`
	SummaryUsage          core.TokenUsage `json:"summary_usage"`
	AnswerUsage           core.TokenUsage `json:"answer_usage"`
	TotalUsage            core.TokenUsage `json:"total_usage"`
	UsageReported         bool            `json:"usage_reported"`
	SessionUsageMatches   bool            `json:"session_usage_matches"`
	SummaryUsageInLedger  bool            `json:"summary_usage_in_ledger"`
	SummaryEventFound     bool            `json:"summary_event_found"`
	SummaryContextPresent bool            `json:"summary_context_present"`
	FactInFinalContext    bool            `json:"fact_in_final_context"`
	FactInSummaryContext  bool            `json:"fact_in_summary_context"`
	SummaryMatchesDurable bool            `json:"summary_matches_durable"`
	AnswerMatches         bool            `json:"answer_matches"`
	DurableAnswerMatches  bool            `json:"durable_answer_matches"`
}

func liveEvidenceFromCase(test liveCase, result core.TurnResult, requestedModel, protocol, sourceRevision, prompt string, events []core.SessionEvent, model *liveModel, totalElapsed time.Duration) liveEvidenceRecord {
	sum := sha256.Sum256([]byte(prompt))
	record := liveEvidenceRecord{
		Schema: "harness.programmatic.live-evidence/v1", CaseID: test.name, RunID: result.RunID, OutputContracts: test.outputContracts,
		FixtureVariant: liveFixtureVariantLabel(test.fixtureVariant), DetailPaddingBytes: liveDetailPaddingEvidence(test.detailPaddingBytes), Strategy: liveStrategyLabel(test.strategyLabel),
		RequestedModel: requestedModel, Protocol: protocol, SourceRevision: sourceRevision,
		PromptSHA256: fmt.Sprintf("%x", sum), RuntimeStatus: liveRunStatus(result),
		TotalElapsedMS: totalElapsed.Milliseconds(),
	}
	_, results, _, _ := liveSessionEvidence(events)
	record.ProgramAttempts = liveProgramAttempts(events)
	record.ToolResults.Observed = events != nil
	for _, result := range results {
		if result.OK {
			record.ToolResults.Succeeded++
		} else {
			record.ToolResults.Failed++
		}
		if code, _ := result.Metadata["code"].(string); code == core.CodeBudgetExceeded {
			record.ToolResults.BudgetDeniedResults++
		}
	}
	if model == nil {
		return record
	}
	record.InvocationEvidence = liveInvocationEvidenceFromRounds(model.evidenceRounds(), events)
	for _, round := range model.evidenceRounds() {
		record.PacingWaitMS += round.pacingWait.Milliseconds()
		record.ProviderRequestMS += round.providerElapsed.Milliseconds()
		record.Rounds = append(record.Rounds, liveEvidenceRound{
			Round: round.round, InputTokens: round.usage.InputTokens, OutputTokens: round.usage.OutputTokens,
			UsageReported: round.hasUsage, ToolNames: append([]string(nil), round.tools...), ToolCallCount: round.toolCalls,
			Finish: round.finish, HTTPStatus: round.status, ErrorClass: round.errorClass, ErrorType: round.errorType,
			ErrorCode: round.errorCode, PacingWaitMS: round.pacingWait.Milliseconds(), ProviderRequestMS: round.providerElapsed.Milliseconds(),
			SystemBytes: round.systemBytes, MessageBytes: round.messageBytes, ToolSchemaBytes: round.toolSchemaBytes, ToolSchemaNames: append([]string(nil), round.toolSchemaNames...), ToolSchemaSHA256: round.toolSchemaSHA256,
		})
	}
	return record
}

func liveInvocationEvidenceFromRounds(rounds []liveRound, events []core.SessionEvent) *liveInvocationEvidence {
	evidence := &liveInvocationEvidence{AdapterCallsObserved: true}
	type observedCall struct {
		usage           core.TokenUsage
		hasUsage        bool
		usageConsistent bool
	}
	calls := make(map[string]observedCall)
	runIDs := make(map[string]struct{})
	identitiesValid := true
	for _, round := range rounds {
		if round.pacingCanceled && !round.adapterStarted {
			evidence.PacingCanceledBeforeAdapter++
		}
		if !round.adapterStarted {
			continue
		}
		evidence.ActualAdapterCalls++
		if round.runID == "" || round.step < 0 {
			identitiesValid = false
			continue
		}
		key := round.runID + "\x00" + fmt.Sprintf("%d", round.step)
		if _, exists := calls[key]; exists {
			identitiesValid = false
			continue
		}
		calls[key] = observedCall{usage: round.usage, hasUsage: round.hasUsage, usageConsistent: round.usageConsistent}
		runIDs[round.runID] = struct{}{}
	}
	if evidence.ActualAdapterCalls == 0 || !identitiesValid || len(calls) != evidence.ActualAdapterCalls {
		return evidence
	}

	stepSeq := make(map[string]int64, len(calls))
	usageByRunInvocation := make(map[string]core.TokenUsage, len(calls))
	uniqueLedgerIDs := events != nil
	ledgerMatched := events != nil
	for _, event := range events {
		if _, relevant := runIDs[event.RunID]; !relevant {
			continue
		}
		switch event.Type {
		case core.EvStepStart:
			var data core.StepData
			if json.Unmarshal(event.Data, &data) != nil || data.Index < 0 {
				uniqueLedgerIDs = false
				ledgerMatched = false
				continue
			}
			key := event.RunID + "\x00" + fmt.Sprintf("%d", data.Index)
			if _, exists := stepSeq[key]; exists {
				uniqueLedgerIDs = false
				ledgerMatched = false
				continue
			}
			stepSeq[key] = event.Seq
		case core.EvRunUsage:
			var data core.RunUsageData
			if json.Unmarshal(event.Data, &data) != nil {
				uniqueLedgerIDs = false
				ledgerMatched = false
				continue
			}
			if strings.HasPrefix(data.InvocationID, "summary:") {
				// Summary usage belongs to the same run but is not represented by a
				// liveModel adapter round. Preserve model-ID uniqueness while making
				// the ordinary model-only subtotal explicitly incomplete.
				ledgerMatched = false
				continue
			}
			if !strings.HasPrefix(data.InvocationID, "model:") || data.InvocationID == "model:" {
				uniqueLedgerIDs = false
				ledgerMatched = false
				continue
			}
			key := event.RunID + "\x00" + data.InvocationID
			if _, exists := usageByRunInvocation[key]; exists {
				uniqueLedgerIDs = false
				ledgerMatched = false
				continue
			}
			usageByRunInvocation[key] = core.TokenUsage{InputTokens: data.InputTokens, OutputTokens: data.OutputTokens}
		}
	}
	evidence.UniqueLedgerInvocationIDs = uniqueLedgerIDs && len(usageByRunInvocation) > 0

	reportedComplete := true
	protocolConsistent := true
	expectedUsage := make(map[string]struct{}, len(calls))
	for key, call := range calls {
		if !call.hasUsage {
			reportedComplete = false
		}
		if !call.usageConsistent {
			protocolConsistent = false
		}
		seq, found := stepSeq[key]
		if !found {
			ledgerMatched = false
			continue
		}
		runID := strings.SplitN(key, "\x00", 2)[0]
		usageKey := runID + "\x00model:" + fmt.Sprintf("%d", seq)
		expectedUsage[usageKey] = struct{}{}
		ledger, found := usageByRunInvocation[usageKey]
		if !found || !call.hasUsage || !call.usageConsistent || ledger != call.usage {
			ledgerMatched = false
		}
	}
	for key := range usageByRunInvocation {
		if _, expected := expectedUsage[key]; !expected {
			ledgerMatched = false
		}
	}
	evidence.UsageProtocolConsistent = protocolConsistent
	evidence.ReportedUsageComplete = reportedComplete && protocolConsistent
	evidence.LedgerUsageMatched = ledgerMatched && evidence.UniqueLedgerInvocationIDs && evidence.ReportedUsageComplete
	return evidence
}

func liveFixtureVariantLabel(value string) string {
	switch value {
	case "":
		return ""
	case "large_detail_v1", "large_detail_v2_prompt_scope", "large_detail_v3_conditional_catalog", "large_detail_v4_visible_output_size", "large_detail_v5_adapter_limits", "large_detail_v6_direct_only_adapter_limits":
		return value
	default:
		return "unrecognized"
	}
}

func liveDetailPaddingEvidence(value int) int {
	switch value {
	case 0, 2 << 10, 4 << 10:
		return value
	default:
		return 0
	}
}

func liveStrategyLabel(value string) string {
	switch value {
	case "", "forced_batched_direct", "forced_batched_direct_only", "forced_ptc":
		return value
	default:
		return "unrecognized"
	}
}

func liveFixtureEffectEvidenceFromFixture(f *fixture) liveFixtureEffectEvidence {
	if f == nil || f.list == nil || f.detail == nil {
		return liveFixtureEffectEvidence{}
	}
	return liveFixtureEffectEvidence{Observed: true, InventoryCalls: f.list.calls(), DetailCalls: f.detail.calls()}
}

func finalizedLiveEvidence(test liveCase, result core.TurnResult, requestedModel, protocol, sourceRevision, prompt string, events []core.SessionEvent, model *liveModel, totalElapsed time.Duration, acceptancePassed bool) liveEvidenceRecord {
	record := liveEvidenceFromCase(test, result, requestedModel, protocol, sourceRevision, prompt, events, model, totalElapsed)
	record.AcceptancePassed = acceptancePassed
	return record
}

func liveRunStatus(result core.TurnResult) string {
	if result.Status == "" {
		return "runtime_error"
	}
	return string(result.Status)
}

func (m *liveModel) evidenceRounds() []liveRound {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]liveRound, len(m.observations))
	copy(out, m.observations)
	for index := range out {
		out[index].tools = append([]string(nil), out[index].tools...)
		out[index].toolSchemaNames = append([]string(nil), out[index].toolSchemaNames...)
	}
	return out
}

func liveProtocol() string {
	if protocol := strings.TrimSpace(os.Getenv("HARNESS_PROGRAMMATIC_PROTOCOL")); protocol != "" {
		return protocol
	}
	return "chat"
}

func liveSourceRevision() string {
	value := strings.TrimSpace(os.Getenv("HARNESS_PROGRAMMATIC_SOURCE_REVISION"))
	if value == "" {
		return "unspecified"
	}
	if len(value) > 128 {
		return "invalid"
	}
	for _, character := range value {
		if !((character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') || (character >= '0' && character <= '9') || character == '.' || character == '_' || character == ':' || character == '-') {
			return "invalid"
		}
	}
	return value
}

func writeLiveEvidence(directory string, record liveEvidenceRecord) error {
	if strings.TrimSpace(directory) == "" {
		return nil
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return errors.New("live evidence directory is unavailable")
	}
	payload, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return errors.New("live evidence encoding failed")
	}
	identity := sha256.Sum256([]byte(record.RunID + "\x00" + record.CaseID + "\x00" + record.PromptSHA256 + "\x00" + time.Now().UTC().Format(time.RFC3339Nano)))
	filename := "programmatic-live-" + fmt.Sprintf("%x", identity[:12]) + ".json"
	path := filepath.Join(directory, filename)
	return writeLiveEvidenceFile(path, append(payload, '\n'))
}

func writeLiveEvidenceFile(path string, payload []byte) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return errors.New("live evidence file already exists or cannot be created")
	}
	defer file.Close()
	if _, err := file.Write(payload); err != nil {
		return errors.New("live evidence file write failed")
	}
	return nil
}

func assertLiveEvidence(t *testing.T, f *fixture, test liveCase, model *liveModel, turn core.TurnResult) {
	t.Helper()
	if model.rounds() > test.maxModelRounds {
		t.Fatalf("model exceeded round budget: got %d, want at most %d", model.rounds(), test.maxModelRounds)
	}
	if got := f.list.calls(); got != 1 {
		t.Fatalf("inventory fixture effect count: got %d, want 1", got)
	}
	if got := f.detail.calls(); got != test.activeRows {
		t.Fatalf("detail fixture effect count: got %d, want %d", got, test.activeRows)
	}

	calls, results, usage, final := liveSessionEvidence(f.session.Events())
	for _, result := range results {
		if !result.OK {
			t.Fatalf("fixture produced a non-OK tool result for call %q", result.CallID)
		}
	}
	wantFinal := "FINAL: " + strings.Join(expectedValue(test.activeRows), ", ")
	if got := strings.TrimSpace(turn.Answer); got != wantFinal {
		t.Fatalf("final result does not match required fixture aggregate")
	}
	if got := strings.TrimSpace(final); got != wantFinal {
		t.Fatalf("durable final assistant result does not match required fixture aggregate")
	}
	modelUsage, completeUsage := model.usage()
	if !completeUsage {
		t.Fatal("live model did not report protocol-consistent usage for every actual adapter call")
	}
	if modelUsage != usage {
		t.Fatalf("stream usage and durable usage ledger differ")
	}
	invocations := liveInvocationEvidenceFromRounds(model.evidenceRounds(), f.session.Events())
	if invocations == nil || !invocations.AdapterCallsObserved || !invocations.ReportedUsageComplete || !invocations.UsageProtocolConsistent || !invocations.LedgerUsageMatched || !invocations.UniqueLedgerInvocationIDs {
		t.Fatalf("live model invocation usage evidence is incomplete: %#v", invocations)
	}

	counts := map[string]int{}
	for _, call := range calls {
		counts[call.Name]++
	}
	t.Logf("live case=%s selection catalog=%d execute=%d inventory=%d detail=%d", test.name, counts[programtools.CatalogID], counts[programtools.ExecuteID], counts[listToolID], counts[detailToolID])
	assertFixtureIDsOnce(t, calls, test.activeRows)
	if test.requireBatchedDirect {
		if counts[programtools.CatalogID] != 0 || counts[programtools.ExecuteID] != 0 || counts[listToolID] != 1 || counts[detailToolID] != test.activeRows {
			t.Fatalf("batched direct baseline tool selection mismatch")
		}
		if got := model.toolCallCount(2); got != test.activeRows {
			t.Fatalf("batched direct baseline second response submitted %d tool calls, want %d", got, test.activeRows)
		}
		return
	}
	if test.requireProgram {
		if counts[programtools.CatalogID] != 1 || counts[programtools.ExecuteID] != 1 {
			t.Fatalf("programmatic selection mismatch: catalog=%d execute=%d", counts[programtools.CatalogID], counts[programtools.ExecuteID])
		}
		assertForcedProgramChildren(t, calls, results, model)
		return
	}
}

func assertForcedProgramChildren(t *testing.T, calls []core.ToolCallData, results []core.ToolResultData, model *liveModel) {
	t.Helper()
	executeIDs := map[string]bool{}
	for _, call := range calls {
		if call.Name == programtools.ExecuteID {
			executeIDs[call.CallID] = true
		}
	}
	if len(executeIDs) != 1 {
		t.Fatal("forced PTC case has no unique program.execute call identity")
	}
	var executeID string
	for executeID = range executeIDs {
	}
	for _, call := range calls {
		if call.Name == listToolID || call.Name == detailToolID {
			if !isProgramChildCallID(executeID, call.CallID) {
				t.Fatalf("forced PTC fixture call %q is not a child of program.execute", call.Name)
			}
		}
	}
	if model.selectedFixtureTool() {
		t.Fatal("forced PTC case selected a fixture tool at the model boundary")
	}
	completed := false
	for _, result := range results {
		if executeIDs[result.CallID] && result.OK {
			completed = true
		}
	}
	if !completed {
		t.Fatal("forced PTC case has no successful program.execute result")
	}
}

func isProgramChildCallID(parent, child string) bool {
	prefix := parent + "/"
	if !strings.HasPrefix(child, prefix) {
		return false
	}
	suffix := strings.TrimPrefix(child, prefix)
	if len(suffix) != 64 {
		return false
	}
	for _, character := range suffix {
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
			return false
		}
	}
	return true
}

func assertFixtureIDsOnce(t *testing.T, calls []core.ToolCallData, activeRows int) {
	t.Helper()
	ids := make(map[string]int, activeRows)
	for _, call := range calls {
		if call.Name != detailToolID {
			continue
		}
		id, _ := call.Args["id"].(string)
		ids[id]++
	}
	for index := 1; index <= activeRows; index++ {
		id := fmt.Sprintf("item-%02d", index)
		if ids[id] != 1 {
			t.Fatalf("fixture detail id %q was requested %d times, want exactly once", id, ids[id])
		}
	}
	if len(ids) != activeRows {
		t.Fatalf("fixture detail calls included an unexpected or malformed id")
	}
}

func liveSessionEvidence(events []core.SessionEvent) ([]core.ToolCallData, []core.ToolResultData, core.TokenUsage, string) {
	calls := []core.ToolCallData{}
	results := []core.ToolResultData{}
	var usage core.TokenUsage
	final := ""
	for _, event := range events {
		switch event.Type {
		case core.EvToolCall:
			var data core.ToolCallData
			if json.Unmarshal(event.Data, &data) == nil {
				calls = append(calls, data)
			}
		case core.EvToolResult:
			var data core.ToolResultData
			if json.Unmarshal(event.Data, &data) == nil {
				results = append(results, data)
			}
		case core.EvRunUsage:
			var data core.RunUsageData
			if json.Unmarshal(event.Data, &data) == nil {
				usage.InputTokens += data.InputTokens
				usage.OutputTokens += data.OutputTokens
			}
		case core.EvAssistantMessage:
			var data core.AssistantMessageData
			if json.Unmarshal(event.Data, &data) == nil && len(data.ToolCalls) == 0 && data.ToolCall == nil && data.Text != "" {
				final = data.Text
			}
		}
	}
	return calls, results, usage, final
}
