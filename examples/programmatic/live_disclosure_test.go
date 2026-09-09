package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	programtools "github.com/whhhh1500/auto-agent/pkg/adapter/programmatic/toolcapability"
	core "github.com/whhhh1500/auto-agent/pkg/core"
	ptc "github.com/whhhh1500/auto-agent/pkg/execution/programmatic"
)

// TestLiveProgressiveProgramDisclosure tests a model-facing projection only.
// program.catalog remains advertised from the first round. program.execute is
// advertised after, and only after, a successful catalog result is paired to
// the catalog call in the current user turn. The capability snapshot, guarded
// execution, permissions, and bindings are deliberately unchanged.
func TestLiveProgressiveProgramDisclosure(t *testing.T) {
	if os.Getenv("HARNESS_PROGRAMMATIC_LIVE") != "1" && os.Getenv("HARNESS_ACCEPTANCE_LIVE_PROGRAMMATIC") != "1" {
		t.Skip("set HARNESS_PROGRAMMATIC_LIVE=1 to make intentional live model requests")
	}
	modelID := strings.TrimSpace(os.Getenv("HARNESS_LLM_MODEL"))
	if modelID == "" {
		t.Fatal("live model configuration is incomplete or invalid")
	}
	interval, err := liveMinRequestInterval(os.Getenv("HARNESS_PROGRAMMATIC_MIN_REQUEST_INTERVAL"))
	if err != nil {
		t.Fatal("live request interval is invalid")
	}
	pacer := &liveRequestPacer{interval: interval}

	for _, scenario := range []struct {
		name            string
		outputContracts bool
		requireProgram  bool
		maxModelRounds  int
		maxToolCalls    int
		prompt          string
	}{
		{
			name: "batch_n8_progressive_disclosure_auto", maxModelRounds: 10, maxToolCalls: 11,
			prompt: livePrompt(8, false),
		},
		{
			name: "batch_n8_progressive_disclosure_forced_ptc_typed", outputContracts: true, requireProgram: true, maxModelRounds: 6, maxToolCalls: 11,
			prompt: livePrompt(8, true),
		},
	} {
		stopAfterTransportOrProtocolFailure := false
		passed := t.Run(scenario.name, func(t *testing.T) {
			caseDef := liveCase{
				name: scenario.name, activeRows: 8, maxModelRounds: scenario.maxModelRounds, maxToolCalls: scenario.maxToolCalls,
				requireProgram: scenario.requireProgram, outputContracts: scenario.outputContracts, prompt: scenario.prompt,
			}
			runID := "live-" + caseDef.name
			var inner *liveModel
			var model *progressiveCatalogDisclosureModel
			var result core.TurnResult
			var events []core.SessionEvent
			var elapsed time.Duration
			var fixture *fixture
			t.Cleanup(func() {
				acceptancePassed := !t.Failed() && result.Status == core.RunCompleted
				standard := finalizedLiveEvidence(caseDef, result, modelID, liveProtocol(), liveSourceRevision(), caseDef.prompt, events, inner, elapsed, acceptancePassed)
				if standard.RunID == "" {
					standard.RunID = runID
				}
				standard.FixtureEffects = liveFixtureEffectEvidenceFromFixture(fixture)
				if err := writeLiveEvidence(os.Getenv("HARNESS_PROGRAMMATIC_EVIDENCE_PATH"), standard); err != nil {
					t.Error("could not write live experiment evidence")
				}
				disclosure := liveDisclosureEvidenceRecord{
					Schema: "harness.programmatic.live-disclosure-evidence/v1", CaseID: caseDef.name, RunID: standard.RunID,
					RequestedModel: modelID, Protocol: liveProtocol(), SourceRevision: liveSourceRevision(),
					RuntimeStatus: liveRunStatus(result), AcceptancePassed: acceptancePassed, TotalElapsedMS: elapsed.Milliseconds(),
					Rounds: model.evidenceRounds(),
				}
				if err := writeLiveDisclosureEvidence(os.Getenv("HARNESS_PROGRAMMATIC_EVIDENCE_PATH"), disclosure); err != nil {
					t.Error("could not write progressive disclosure evidence")
				}
			})
			adapter, err := newLiveAdapter(context.Background())
			if err != nil {
				t.Fatal("live model configuration is incomplete or invalid")
			}
			inner = newLiveModel(t, adapter, caseDef.name, caseDef.maxModelRounds, pacer)
			model = &progressiveCatalogDisclosureModel{inner: inner}

			fixture, err = newFixtureWithOutputContracts(model, true, caseDef.activeRows, caseDef.outputContracts)
			if err != nil {
				t.Fatal("could not construct live programmatic fixture")
			}
			model.session = fixture.session
			if err := configureLiveProfile(fixture, adapter.Provider(), modelID, caseDef.maxModelRounds, caseDef.maxToolCalls); err != nil {
				t.Fatal("could not configure live programmatic fixture profile")
			}
			started := time.Now()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			result, err = fixture.runtime.RunTurn(ctx, fixture.principal, fixture.session, core.TurnInput{RunID: runID, Text: caseDef.prompt}, nil)
			elapsed = time.Since(started)
			events = fixture.session.Events()
			diagnoseLiveProgramExecutions(t, events)
			if err != nil || result.Status != core.RunCompleted {
				stopAfterTransportOrProtocolFailure = inner.stopSubsequentCases()
				t.Fatalf("live progressive disclosure case did not complete (status=%s, model_rounds=%d)", result.Status, inner.rounds())
			}
			assertLiveEvidence(t, fixture, caseDef, inner, result)
			assertProgressiveDisclosure(t, model.evidenceRounds(), inner.evidenceRounds(), caseDef.requireProgram)
		})
		if !passed && stopAfterTransportOrProtocolFailure {
			t.Log("live model HTTP, transport, or responses protocol failure: remaining disclosure cases skipped")
			break
		}
	}
}

// progressiveCatalogDisclosureModel performs only model-context projection.
// It cannot grant a capability that the core snapshot did not already permit.
type progressiveCatalogDisclosureModel struct {
	inner   core.LlmAdapter
	session *core.Session

	mu     sync.Mutex
	rounds []liveDisclosureRound
}

type liveDisclosureRound struct {
	CatalogResultPaired bool     `json:"catalog_result_paired"`
	ToolSchemaNames     []string `json:"tool_schema_names"`
}

func (m *progressiveCatalogDisclosureModel) Provider() string {
	if m == nil || m.inner == nil {
		return ""
	}
	return m.inner.Provider()
}

func (m *progressiveCatalogDisclosureModel) Stream(ctx context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	if m == nil || m.inner == nil {
		return errors.New("progressive disclosure model is invalid")
	}
	paired := m.catalogReady(options)
	projected := options
	projected.Tools = progressiveDisclosureTools(options.Tools, paired)
	m.mu.Lock()
	m.rounds = append(m.rounds, liveDisclosureRound{CatalogResultPaired: paired, ToolSchemaNames: liveToolSchemaNames(projected.Tools)})
	m.mu.Unlock()
	return m.inner.Stream(ctx, projected, emit)
}

func (m *progressiveCatalogDisclosureModel) catalogReady(options core.GenerateOptions) bool {
	ids := currentTurnSuccessfulCatalogCalls(options.Messages)
	if len(ids) == 0 {
		return false
	}
	// Offline projection tests intentionally have no session. A live fixture
	// always sets one, and then the durable result must independently confirm
	// the exact current-run call succeeded.
	if m.session == nil {
		return true
	}
	for _, callID := range ids {
		result, found, err := m.session.ToolResult(options.ModelCall.RunID, callID)
		if err == nil && found && result.OK && successfulCatalogOutput(result.Content) {
			return true
		}
	}
	return false
}

func (m *progressiveCatalogDisclosureModel) evidenceRounds() []liveDisclosureRound {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	out := append([]liveDisclosureRound(nil), m.rounds...)
	for index := range out {
		out[index].ToolSchemaNames = append([]string(nil), out[index].ToolSchemaNames...)
	}
	return out
}

// progressiveDisclosureTools always clones the slice before filtering so the
// caller's GenerateOptions and its backing array remain unchanged.
func progressiveDisclosureTools(tools []core.ToolSchema, catalogReady bool) []core.ToolSchema {
	cloned := append([]core.ToolSchema(nil), tools...)
	if catalogReady {
		return cloned
	}
	out := make([]core.ToolSchema, 0, len(cloned))
	for _, tool := range cloned {
		if tool.Name != programtools.ExecuteID {
			out = append(out, tool)
		}
	}
	return out
}

// currentTurnHasSuccessfulCatalog checks only paired model/tool history after
// the latest user message. Old turns, an orphan result, and a failed catalog
// output cannot disclose program.execute.
func currentTurnHasSuccessfulCatalog(messages []core.ChatMessage) bool {
	return len(currentTurnSuccessfulCatalogCalls(messages)) > 0
}

func currentTurnSuccessfulCatalogCalls(messages []core.ChatMessage) []string {
	start := -1
	for index, message := range messages {
		if message.Role == core.RoleUser {
			start = index
		}
	}
	if start < 0 {
		return nil
	}
	pending := map[string]struct{}{}
	for _, message := range messages[start+1:] {
		if message.Role == core.RoleAssistant {
			calls := message.ToolCalls
			if len(calls) == 0 && message.ToolCall != nil {
				calls = []core.ToolCall{*message.ToolCall}
			}
			for _, call := range calls {
				if call.ID != "" && call.Name == programtools.CatalogID {
					pending[call.ID] = struct{}{}
				}
			}
			continue
		}
		if message.Role != core.RoleTool {
			continue
		}
		if _, paired := pending[message.ToolCallID]; !paired {
			continue
		}
		delete(pending, message.ToolCallID)
		if successfulCatalogOutput(message.Content) {
			return []string{message.ToolCallID}
		}
	}
	return nil
}

// successfulCatalogOutput mirrors the public marshalCatalog contract without
// retaining the catalog content: version, language, and an array-valued tools
// field must all be present and match the current interpreter publication.
func successfulCatalogOutput(content string) bool {
	var output struct {
		Version  string          `json:"version"`
		Language string          `json:"language"`
		Tools    json.RawMessage `json:"tools"`
	}
	if json.Unmarshal([]byte(content), &output) != nil || output.Version != ptc.Version || output.Language != ptc.LanguageGuide || len(output.Tools) == 0 {
		return false
	}
	var tools []json.RawMessage
	return json.Unmarshal(output.Tools, &tools) == nil
}

func assertProgressiveDisclosure(t *testing.T, disclosure []liveDisclosureRound, actual []liveRound, requireExecute bool) {
	t.Helper()
	if len(disclosure) == 0 || len(disclosure) != len(actual) {
		t.Fatalf("disclosure rounds=%d, live model rounds=%d", len(disclosure), len(actual))
	}
	if !containsToolSchema(disclosure[0].ToolSchemaNames, programtools.CatalogID) || containsToolSchema(disclosure[0].ToolSchemaNames, programtools.ExecuteID) {
		t.Fatalf("first progressive projection did not expose only catalog: %v", disclosure[0].ToolSchemaNames)
	}
	seenExecute := false
	for index, round := range disclosure {
		if !sameStrings(round.ToolSchemaNames, actual[index].toolSchemaNames) {
			t.Fatalf("live model schema record differs from filtered projection at round %d", index+1)
		}
		executeVisible := containsToolSchema(round.ToolSchemaNames, programtools.ExecuteID)
		if executeVisible && !round.CatalogResultPaired {
			t.Fatalf("program.execute was visible without a paired catalog result at round %d", index+1)
		}
		if !round.CatalogResultPaired && containsToolSchema(actual[index].tools, programtools.ExecuteID) {
			t.Fatalf("model called hidden program.execute before a paired catalog result at round %d", index+1)
		}
		seenExecute = seenExecute || executeVisible
	}
	if requireExecute && !seenExecute {
		t.Fatal("forced PTC case never disclosed program.execute after catalog")
	}
}

func containsToolSchema(schemas []string, name string) bool {
	for _, schema := range schemas {
		if schema == name {
			return true
		}
	}
	return false
}

type liveDisclosureEvidenceRecord struct {
	Schema           string                `json:"schema"`
	CaseID           string                `json:"case_id"`
	RunID            string                `json:"run_id"`
	RequestedModel   string                `json:"requested_model"`
	Protocol         string                `json:"protocol"`
	SourceRevision   string                `json:"source_revision"`
	RuntimeStatus    string                `json:"runtime_status"`
	AcceptancePassed bool                  `json:"acceptance_passed"`
	TotalElapsedMS   int64                 `json:"total_elapsed_ms"`
	Rounds           []liveDisclosureRound `json:"rounds"`
}

func writeLiveDisclosureEvidence(directory string, record liveDisclosureEvidenceRecord) error {
	if strings.TrimSpace(directory) == "" {
		return nil
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return errors.New("live disclosure evidence directory is unavailable")
	}
	payload, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return errors.New("live disclosure evidence encoding failed")
	}
	identity := sha256.Sum256([]byte(record.RunID + "\x00" + record.CaseID + "\x00" + time.Now().UTC().Format(time.RFC3339Nano)))
	path := filepath.Join(directory, "programmatic-live-disclosure-"+fmt.Sprintf("%x", identity[:12])+".json")
	return writeLiveEvidenceFile(path, append(payload, '\n'))
}

func TestProgressiveDisclosureRequiresCurrentSuccessfulCatalogPair(t *testing.T) {
	success := catalogSuccessMessage()
	catalog := core.ToolCall{ID: "catalog-current", Name: programtools.CatalogID}
	for _, test := range []struct {
		name     string
		messages []core.ChatMessage
		want     bool
	}{
		{
			name: "current_success", want: true,
			messages: []core.ChatMessage{{Role: core.RoleUser, Content: "current"}, {Role: core.RoleAssistant, ToolCall: &catalog}, {Role: core.RoleTool, ToolCallID: catalog.ID, Content: success}},
		},
		{
			name: "failed_result", want: false,
			messages: []core.ChatMessage{{Role: core.RoleUser, Content: "current"}, {Role: core.RoleAssistant, ToolCall: &catalog}, {Role: core.RoleTool, ToolCallID: catalog.ID, Content: "catalog unavailable"}},
		},
		{
			name: "orphan_result", want: false,
			messages: []core.ChatMessage{{Role: core.RoleUser, Content: "current"}, {Role: core.RoleTool, ToolCallID: catalog.ID, Content: success}},
		},
		{
			name: "old_turn", want: false,
			messages: []core.ChatMessage{{Role: core.RoleUser, Content: "old"}, {Role: core.RoleAssistant, ToolCall: &catalog}, {Role: core.RoleTool, ToolCallID: catalog.ID, Content: success}, {Role: core.RoleUser, Content: "current"}},
		},
		{
			name: "wrong_call_id", want: false,
			messages: []core.ChatMessage{{Role: core.RoleUser, Content: "current"}, {Role: core.RoleAssistant, ToolCall: &catalog}, {Role: core.RoleTool, ToolCallID: "different", Content: success}},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := currentTurnHasSuccessfulCatalog(test.messages); got != test.want {
				t.Fatalf("catalog-pair readiness=%t, want %t", got, test.want)
			}
		})
	}
}

func TestProgressiveDisclosureClonesBeforeFiltering(t *testing.T) {
	original := []core.ToolSchema{{Name: programtools.CatalogID}, {Name: programtools.ExecuteID}, {Name: "fixture.inventory"}}
	projected := progressiveDisclosureTools(original, false)
	if len(projected) != 2 || projected[0].Name != programtools.CatalogID || projected[1].Name != "fixture.inventory" {
		t.Fatalf("unexpected filtered projection: %#v", projected)
	}
	projected[1].Name = "changed"
	if len(original) != 3 || original[1].Name != programtools.ExecuteID || original[2].Name != "fixture.inventory" {
		t.Fatalf("tool projection mutated caller slice: %#v", original)
	}
}

func catalogSuccessMessage() string {
	encoded, _ := json.Marshal(map[string]any{"version": ptc.Version, "language": ptc.LanguageGuide, "tools": []any{}})
	return string(encoded)
}
