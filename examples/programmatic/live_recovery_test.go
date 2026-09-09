package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	appcontextassembly "github.com/whhhh1500/auto-agent/pkg/app/contextassembly"
	core "github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/storage"

	_ "modernc.org/sqlite"
)

const recoveryFinalPrompt = "Using only the conversation history, what is the current Project Orion code? Reply exactly CURRENT: <code>, or CURRENT: UNKNOWN if it is absent."

// TestLiveSQLiteRecoveryContextAcceptance makes two intentional provider calls
// only when explicitly enabled. The first completed turn is persisted through
// SQL, its database handle is closed, then a fresh SQLite handle loads the
// session before the second turn answers from restored context.
func TestLiveSQLiteRecoveryContextAcceptance(t *testing.T) {
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
	interval, err := liveMinRequestInterval(os.Getenv("HARNESS_PROGRAMMATIC_MIN_REQUEST_INTERVAL"))
	if err != nil {
		t.Fatal("live request interval is invalid")
	}
	value, err := recoveryNonce()
	if err != nil {
		t.Fatal("could not create opaque recovery fixture value")
	}
	firstPrompt := recoveryFirstPrompt(value)
	if strings.Contains(recoveryFinalPrompt, value) {
		t.Fatal("recovery fixture value leaked into final prompt")
	}

	model := newLiveModel(t, adapter, "sqlite_recovery_context", 2, &liveRequestPacer{interval: interval})
	observed := newRecoveryObservedAdapter(model, 2)
	fixture, err := newRecoveryFixture(observed, modelID)
	if err != nil {
		t.Fatal("could not construct recovery fixture")
	}
	runIDs := []string{"live-recovery-first", "live-recovery-final"}
	var results [2]core.TurnResult
	var events []core.SessionEvent
	var loaded *core.Session
	var persistedPrefix []core.SessionEvent
	var recoveredPrefix []core.ChatMessage
	newHandle := false
	t.Cleanup(func() {
		if events == nil {
			if loaded != nil {
				events = loaded.Events()
			} else {
				events = fixture.session.Events()
			}
		}
		evidence := recoveryEvidenceFromRun("sqlite_recovery_context", fixture, observed, runIDs, results, events, loaded, newHandle, persistedPrefix, recoveredPrefix, firstPrompt, value)
		evidence.AcceptancePassed = !t.Failed()
		if err := writeRecoveryEvidence(os.Getenv("HARNESS_PROGRAMMATIC_EVIDENCE_PATH"), evidence); err != nil {
			t.Error("could not write live SQLite recovery evidence")
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	databasePath := filepath.Join(t.TempDir(), "recovery.sqlite")
	db1, store1 := openRecoverySQLite(t, ctx, databasePath)
	if err := store1.Create(ctx, fixture.session); err != nil {
		t.Fatal("could not create SQL-backed session")
	}
	baseVersion := fixture.session.Version()
	first, err := fixture.runtime.RunTurn(ctx, fixture.principal, fixture.session, core.TurnInput{RunID: runIDs[0], Text: firstPrompt}, nil)
	results[0] = first
	if err != nil || first.Status != core.RunCompleted || strings.TrimSpace(first.Answer) != "ACK" {
		t.Fatal("first recovery turn did not acknowledge")
	}
	if err := store1.Save(ctx, fixture.session, baseVersion); err != nil {
		t.Fatal("could not persist completed first turn")
	}
	persistedPrefix = fixture.session.Events()
	if err := db1.Close(); err != nil {
		t.Fatal("could not close first SQLite handle")
	}

	db2, store2 := openRecoverySQLite(t, ctx, databasePath)
	defer db2.Close()
	newHandle = db1 != db2 && store1 != store2
	loaded, err = store2.Load(ctx, fixture.session.ID())
	if err != nil {
		t.Fatal("could not load SQL-persisted session through fresh handle")
	}
	if !reflect.DeepEqual(persistedPrefix, loaded.Events()) {
		t.Fatal("fresh SQLite load did not restore the exact persisted event prefix")
	}
	recoveredPrefix, err = loaded.DeriveMessages()
	if err != nil {
		t.Fatal("could not derive restored context prefix")
	}
	secondExpectedVersion := loaded.Version()
	secondRuntime, err := newRecoveryRuntime(observed, modelID, fixture.product, loaded.ProfileID())
	if err != nil {
		t.Fatal("could not reconstruct runtime after SQLite reopen")
	}
	second, err := secondRuntime.RunTurn(ctx, loaded.Principal(), loaded, core.TurnInput{RunID: runIDs[1], Text: recoveryFinalPrompt}, nil)
	results[1] = second
	if err != nil || second.Status != core.RunCompleted {
		t.Fatal("recovered second turn did not complete")
	}
	if err := store2.Save(ctx, loaded, secondExpectedVersion); err != nil {
		t.Fatal("could not persist completed recovered turn")
	}
	loaded, err = store2.Load(ctx, fixture.session.ID())
	if err != nil {
		t.Fatal("could not reload final SQLite-persisted session")
	}
	events = loaded.Events()
	assertRecoveryAcceptance(t, fixture, observed, runIDs, results, events, loaded, newHandle, persistedPrefix, recoveredPrefix, firstPrompt, value)
}

type recoveryFixture struct {
	runtime        *core.Runtime
	principal      core.Principal
	session        *core.Session
	product        core.ScopePath
	requestedModel string
}

func newRecoveryFixture(model core.LlmAdapter, modelID string) (*recoveryFixture, error) {
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	product, err := global.Child(core.ScopeRef{Kind: core.ScopeProduct, ID: "live-sqlite-recovery"})
	if err != nil {
		return nil, err
	}
	user, err := product.Child(core.ScopeRef{Kind: core.ScopeUser, ID: "fixture-user"})
	if err != nil {
		return nil, err
	}
	principal := core.Principal{TenantID: "fixture-tenant", SubjectID: "fixture-subject", Scope: user, Grants: core.NewPermissionSet(core.PermRead)}
	sessionScope, err := user.Child(core.ScopeRef{Kind: core.ScopeSession, ID: "fixture-session"})
	if err != nil {
		return nil, err
	}
	session, err := core.NewSession(core.SessionOptions{ID: "fixture-session", ProfileID: "example.recovery.live", Principal: principal, Scope: sessionScope})
	if err != nil {
		return nil, err
	}
	runtime, err := newRecoveryRuntime(model, modelID, product, session.ProfileID())
	if err != nil {
		return nil, err
	}
	return &recoveryFixture{runtime: runtime, principal: principal, session: session, product: product, requestedModel: modelID}, nil
}

func newRecoveryRuntime(model core.LlmAdapter, modelID string, product core.ScopePath, profileID string) (*core.Runtime, error) {
	profiles := core.NewAgentProfileRegistry()
	name := "Live SQLite recovery context fixture"
	selection := core.ModelSelection{Provider: model.Provider(), Model: modelID}
	steps, calls := 1, 1
	instructions := core.PromptFragment{ID: "sqlite-recovery-context", Section: core.PromptInstructions, Content: "Use conversation history as data. Follow the user's exact response format and do not invent a Project Orion code."}
	if err := profiles.Bind(core.AgentProfileLayer{Scope: product, ProfileID: profileID, Name: &name, Model: &selection, MaxSteps: &steps, MaxToolCalls: &calls, PutFragments: []core.PromptFragment{instructions}}); err != nil {
		return nil, err
	}
	assembler, err := appcontextassembly.NewAssembler(appcontextassembly.Config{})
	if err != nil {
		return nil, err
	}
	return &core.Runtime{Capabilities: core.NewCapabilityRegistry(), Profiles: profiles, Models: core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) { return model, nil }), ContextAssembler: assembler.AssembleModelContext}, nil
}

func openRecoverySQLite(t *testing.T, ctx context.Context, path string) (*sql.DB, *storage.SQLSessionStore) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal("could not open SQLite")
	}
	store, err := storage.OpenSQLSessionStore(ctx, db, storage.SQLDialectSQLite)
	if err != nil {
		_ = db.Close()
		t.Fatal("could not open SQL session store")
	}
	t.Cleanup(func() { _ = db.Close() })
	return db, store
}

type recoveryObservedAdapter struct {
	inner    core.LlmAdapter
	maxCalls int
	mu       sync.Mutex
	calls    []recoveryObservedCall
}
type recoveryObservedCall struct {
	messages []core.ChatMessage
	usage    core.TokenUsage
	hasUsage bool
}

func newRecoveryObservedAdapter(inner core.LlmAdapter, maxCalls int) *recoveryObservedAdapter {
	return &recoveryObservedAdapter{inner: inner, maxCalls: maxCalls}
}
func (m *recoveryObservedAdapter) Provider() string { return m.inner.Provider() }
func (m *recoveryObservedAdapter) Stream(ctx context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	m.mu.Lock()
	if m.maxCalls <= 0 || len(m.calls) >= m.maxCalls {
		m.mu.Unlock()
		return errors.New("SQLite recovery model call budget reached")
	}
	index := len(m.calls)
	m.calls = append(m.calls, recoveryObservedCall{messages: cloneRecoveryMessages(options.Messages)})
	m.mu.Unlock()
	return m.inner.Stream(ctx, options, func(chunk core.StreamChunk) {
		if chunk.Usage != nil {
			m.mu.Lock()
			m.calls[index].usage = *chunk.Usage
			m.calls[index].hasUsage = true
			m.mu.Unlock()
		}
		emit(chunk)
	})
}
func (m *recoveryObservedAdapter) snapshot() []recoveryObservedCall {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]recoveryObservedCall, len(m.calls))
	for index := range m.calls {
		result[index] = m.calls[index]
		result[index].messages = cloneRecoveryMessages(m.calls[index].messages)
	}
	return result
}
func cloneRecoveryMessages(messages []core.ChatMessage) []core.ChatMessage {
	result := append([]core.ChatMessage(nil), messages...)
	for index := range result {
		result[index].ToolCalls = append([]core.ToolCall(nil), result[index].ToolCalls...)
	}
	return result
}

type recoveryRunUsage struct {
	RunID        string `json:"run_id"`
	InvocationID string `json:"invocation_id"`
	InputTokens  int64  `json:"input_tokens"`
	OutputTokens int64  `json:"output_tokens"`
	Complete     bool   `json:"complete"`
}
type recoveryEvidence struct {
	Schema                    string             `json:"schema"`
	CaseID                    string             `json:"case_id"`
	RequestedModel            string             `json:"requested_model"`
	SourceRevision            string             `json:"source_revision"`
	FirstPromptSHA256         string             `json:"first_prompt_sha256"`
	FinalPromptSHA256         string             `json:"final_prompt_sha256"`
	RecoveredIDMatches        bool               `json:"recovered_id_matches"`
	SQLiteNewHandle           bool               `json:"sqlite_new_handle"`
	RecoveredPrefixExact      bool               `json:"recovered_prefix_exact"`
	DurableContextSourceExact bool               `json:"durable_context_source_exact"`
	FinalContextPrefixExact   bool               `json:"final_context_prefix_exact"`
	ModelCalls                int                `json:"model_calls"`
	RunUsage                  []recoveryRunUsage `json:"run_usage"`
	UsageComplete             bool               `json:"usage_complete"`
	UsageLedgerMatches        bool               `json:"usage_ledger_matches"`
	FirstAnswerAcknowledged   bool               `json:"first_answer_acknowledged"`
	FinalAnswerCorrect        bool               `json:"final_answer_correct"`
	DurableFinalAnswerCorrect bool               `json:"durable_final_answer_correct"`
	AcceptancePassed          bool               `json:"acceptance_passed"`
}

func recoveryEvidenceFromRun(caseID string, fixture *recoveryFixture, observed *recoveryObservedAdapter, runIDs []string, results [2]core.TurnResult, events []core.SessionEvent, loaded *core.Session, newHandle bool, persistedPrefix []core.SessionEvent, recoveredPrefix []core.ChatMessage, firstPrompt, value string) recoveryEvidence {
	firstHash, finalHash := sha256.Sum256([]byte(firstPrompt)), sha256.Sum256([]byte(recoveryFinalPrompt))
	evidence := recoveryEvidence{Schema: "harness.programmatic.live-sqlite-recovery/v1", CaseID: caseID, SourceRevision: liveSourceRevision(), FirstPromptSHA256: fmt.Sprintf("%x", firstHash), FinalPromptSHA256: fmt.Sprintf("%x", finalHash), SQLiteNewHandle: newHandle}
	if fixture != nil {
		evidence.RequestedModel = fixture.requestedModel
		evidence.RecoveredIDMatches = loaded != nil && loaded.ID() == fixture.session.ID()
	}
	calls := observed.snapshot()
	evidence.ModelCalls = len(calls)
	evidence.RecoveredPrefixExact = len(persistedPrefix) > 0 && len(events) >= len(persistedPrefix) && reflect.DeepEqual(events[:len(persistedPrefix)], persistedPrefix)
	evidence.FinalContextPrefixExact = recoveryFinalContextPrefixExact(recoveredPrefix, calls)
	evidence.DurableContextSourceExact = evidence.RecoveredPrefixExact && evidence.FinalContextPrefixExact && recoveryDurableContextSourceExact(loaded, firstPrompt)
	evidence.RunUsage, evidence.UsageComplete, evidence.UsageLedgerMatches = recoveryUsageFacts(events, runIDs, calls)
	expected := "CURRENT: " + value
	evidence.FirstAnswerAcknowledged = strings.TrimSpace(results[0].Answer) == "ACK" && strings.TrimSpace(recoveryDurableAnswer(events, runIDs[0])) == "ACK"
	evidence.FinalAnswerCorrect = strings.TrimSpace(results[1].Answer) == expected
	evidence.DurableFinalAnswerCorrect = strings.TrimSpace(recoveryDurableAnswer(events, runIDs[1])) == expected
	return evidence
}

func recoveryDurableContextSourceExact(loaded *core.Session, firstPrompt string) bool {
	if loaded == nil {
		return false
	}
	messages, err := loaded.DeriveMessages()
	if err != nil || !recoveryMessagesContain(messages, firstPrompt) || !recoveryMessagesContain(messages, "ACK") {
		return false
	}
	return true
}
func recoveryFinalContextPrefixExact(prefix []core.ChatMessage, calls []recoveryObservedCall) bool {
	if len(prefix) == 0 || len(calls) != 2 || len(calls[1].messages) != len(prefix)+1 {
		return false
	}
	for index := range prefix {
		if !reflect.DeepEqual(calls[1].messages[index], prefix[index]) {
			return false
		}
	}
	last := calls[1].messages[len(calls[1].messages)-1]
	return last.Role == core.RoleUser && last.Content == recoveryFinalPrompt
}
func recoveryMessagesContain(messages []core.ChatMessage, content string) bool {
	for _, message := range messages {
		if message.Content == content {
			return true
		}
	}
	return false
}

func recoveryUsageFacts(events []core.SessionEvent, runIDs []string, calls []recoveryObservedCall) ([]recoveryRunUsage, bool, bool) {
	result := make([]recoveryRunUsage, len(runIDs))
	byRun := make(map[string]*recoveryRunUsage, len(runIDs))
	for index, runID := range runIDs {
		result[index].RunID = runID
		byRun[runID] = &result[index]
	}
	valid, reports := true, make(map[string]int, len(runIDs))
	unique := map[string]struct{}{}
	var ledger core.TokenUsage
	for _, event := range events {
		entry := byRun[event.RunID]
		if entry == nil || event.Type != core.EvRunUsage {
			continue
		}
		var data core.RunUsageData
		if json.Unmarshal(event.Data, &data) != nil || !strings.HasPrefix(data.InvocationID, "model:") || data.InvocationID == "model:" {
			valid = false
			continue
		}
		if _, exists := unique[data.InvocationID]; exists {
			valid = false
		}
		unique[data.InvocationID] = struct{}{}
		reports[event.RunID]++
		entry.InvocationID = data.InvocationID
		entry.InputTokens += data.InputTokens
		entry.OutputTokens += data.OutputTokens
		ledger.InputTokens += data.InputTokens
		ledger.OutputTokens += data.OutputTokens
	}
	complete := valid && len(calls) == len(runIDs) && len(unique) == len(runIDs)
	var observed core.TokenUsage
	for index := range result {
		hasCall := index < len(calls)
		if hasCall {
			observed.InputTokens += calls[index].usage.InputTokens
			observed.OutputTokens += calls[index].usage.OutputTokens
		}
		result[index].Complete = hasCall && reports[result[index].RunID] == 1 && result[index].InputTokens+result[index].OutputTokens > 0 && calls[index].hasUsage && calls[index].usage.InputTokens+calls[index].usage.OutputTokens > 0 && result[index].InputTokens == calls[index].usage.InputTokens && result[index].OutputTokens == calls[index].usage.OutputTokens
		complete = complete && result[index].Complete
	}
	return result, complete, complete && ledger == observed
}
func recoveryDurableAnswer(events []core.SessionEvent, runID string) string {
	answer := ""
	for _, event := range events {
		if event.RunID != runID || event.Type != core.EvAssistantMessage {
			continue
		}
		var data core.AssistantMessageData
		if json.Unmarshal(event.Data, &data) == nil && data.ToolCall == nil && len(data.ToolCalls) == 0 {
			answer = data.Text
		}
	}
	return answer
}

func assertRecoveryAcceptance(t *testing.T, fixture *recoveryFixture, observed *recoveryObservedAdapter, runIDs []string, results [2]core.TurnResult, events []core.SessionEvent, loaded *core.Session, newHandle bool, persistedPrefix []core.SessionEvent, recoveredPrefix []core.ChatMessage, firstPrompt, value string) {
	t.Helper()
	evidence := recoveryEvidenceFromRun("assert", fixture, observed, runIDs, results, events, loaded, newHandle, persistedPrefix, recoveredPrefix, firstPrompt, value)
	if !evidence.RecoveredIDMatches || !evidence.SQLiteNewHandle || !evidence.RecoveredPrefixExact || !evidence.DurableContextSourceExact || !evidence.FinalContextPrefixExact || evidence.ModelCalls != 2 || len(evidence.RunUsage) != 2 || !evidence.UsageComplete || !evidence.UsageLedgerMatches || !evidence.FirstAnswerAcknowledged || !evidence.FinalAnswerCorrect || !evidence.DurableFinalAnswerCorrect {
		t.Fatalf("SQLite recovery acceptance predicates failed: %+v", evidence)
	}
}

func recoveryFirstPrompt(value string) string {
	return "Record this Project Orion current code: " + value + ". Reply exactly ACK."
}
func recoveryNonce() (string, error) {
	bytes := make([]byte, 12)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return "recover-" + hex.EncodeToString(bytes), nil
}
func writeRecoveryEvidence(directory string, evidence recoveryEvidence) error {
	if strings.TrimSpace(directory) == "" {
		return nil
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return errors.New("live recovery evidence directory is unavailable")
	}
	payload, err := json.MarshalIndent(evidence, "", "  ")
	if err != nil {
		return errors.New("live recovery evidence encoding failed")
	}
	identity := sha256.Sum256([]byte(evidence.CaseID + "\x00" + time.Now().UTC().Format(time.RFC3339Nano)))
	return writeLiveEvidenceFile(filepath.Join(directory, "programmatic-live-sqlite-recovery-"+fmt.Sprintf("%x", identity[:12])+".json"), append(payload, '\n'))
}
