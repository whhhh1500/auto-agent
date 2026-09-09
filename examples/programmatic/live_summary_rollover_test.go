package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	appcontextassembly "github.com/whhhh1500/auto-agent/pkg/app/contextassembly"
	core "github.com/whhhh1500/auto-agent/pkg/core"
)

const (
	summaryRolloverMaxMessages = 3
	summaryRolloverKeepTail    = 1
	summaryRolloverSeedTurns   = 16
)

const summaryRolloverFinalPrompt = "Using only the conversation history, what is the current Project Orion launch code? Reply exactly CURRENT: <code>, or CURRENT: UNKNOWN if no current code is present."

type summaryRolloverValues struct{ Old, New string }

func newSummaryRolloverValues() (summaryRolloverValues, error) {
	oldValue, err := summaryRolloverNonce()
	if err != nil {
		return summaryRolloverValues{}, err
	}
	newValue, err := summaryRolloverNonce()
	if err != nil {
		return summaryRolloverValues{}, err
	}
	return summaryRolloverValues{Old: oldValue, New: newValue}, nil
}

func summaryRolloverNonce() (string, error) {
	bytes := make([]byte, 12)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return "roll-" + hex.EncodeToString(bytes), nil
}

func summaryRolloverCorrectionPrompt(values summaryRolloverValues) string {
	return "Record correction: Project Orion current launch code: " + values.New + ". This newer record supersedes the prior Project Orion code " + values.Old + ". Reply exactly ACK."
}

const summaryRolloverNeutralPrompt = "Acknowledge this continuity note without changing the Project Orion record. Reply exactly ACK."

// TestLiveSummaryRolloverAcceptance is an opt-in development-contract sample.
// It uses the production ExtractiveSummarizer and Assembler, while forcing
// three actual Runtime turns across repeated rolling replacement. The final
// prompt contains neither opaque value.
func TestLiveSummaryRolloverAcceptance(t *testing.T) {
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
	values, err := newSummaryRolloverValues()
	if err != nil {
		t.Fatal("could not create opaque rollover fixture values")
	}
	if strings.Contains(summaryRolloverFinalPrompt, values.Old) || strings.Contains(summaryRolloverFinalPrompt, values.New) {
		t.Fatal("rollover fixture value leaked into final prompt")
	}
	model := newLiveModel(t, adapter, "summary_rollover", 3, &liveRequestPacer{interval: interval})
	observed := newSummaryRolloverObservedAdapter(model, 3)
	fixture, err := newSummaryRolloverFixture(observed, modelID, values)
	if err != nil {
		t.Fatal("could not construct rollover fixture")
	}
	runIDs := []string{"live-summary-rollover-correction", "live-summary-rollover-neutral", "live-summary-rollover-final"}
	inputs := []string{summaryRolloverCorrectionPrompt(values), summaryRolloverNeutralPrompt, summaryRolloverFinalPrompt}
	var results [3]core.TurnResult
	var events []core.SessionEvent
	t.Cleanup(func() {
		if events == nil {
			events = fixture.session.Events()
		}
		evidence := summaryRolloverEvidenceFromRun("summary_rollover", fixture, observed, runIDs, results, events, values)
		evidence.AcceptancePassed = !t.Failed()
		if err := writeSummaryRolloverEvidence(os.Getenv("HARNESS_PROGRAMMATIC_EVIDENCE_PATH"), evidence); err != nil {
			t.Error("could not write live summary rollover evidence")
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	for index := range runIDs {
		result, runErr := fixture.runtime.RunTurn(ctx, fixture.principal, fixture.session, core.TurnInput{RunID: runIDs[index], Text: inputs[index]}, nil)
		results[index] = result
		if runErr != nil || result.Status != core.RunCompleted {
			t.Fatalf("rollover run %d did not complete", index+1)
		}
	}
	events = fixture.session.Events()
	assertSummaryRolloverAcceptance(t, fixture, observed, runIDs, results, events, values)
}

type summaryRolloverFixture struct {
	runtime        *core.Runtime
	principal      core.Principal
	session        *core.Session
	requestedModel string
}

func newSummaryRolloverFixture(model core.LlmAdapter, modelID string, values summaryRolloverValues) (*summaryRolloverFixture, error) {
	seedNonce, err := summaryNonce()
	if err != nil {
		return nil, err
	}
	base, err := newSummaryAcceptanceFixture(model, model, modelID, summaryModeExtractive, seedNonce)
	if err != nil {
		return nil, err
	}
	extractive, err := appcontextassembly.NewExtractiveSummarizer(appcontextassembly.ExtractiveSummarizerConfig{})
	if err != nil {
		return nil, err
	}
	base.runtime.Summarizer = &appcontextassembly.RollingSummarizer{MaxMessages: summaryRolloverMaxMessages, KeepTail: summaryRolloverKeepTail, Summarizer: extractive}
	if err := seedSummaryRolloverHistory(base.session, values.Old); err != nil {
		return nil, err
	}
	return &summaryRolloverFixture{runtime: base.runtime, principal: base.principal, session: base.session, requestedModel: modelID}, nil
}

// seedSummaryRolloverHistory deliberately uses a fixed, high-volume archive.
// Every entry is a legal completed run and every user payload stays below the
// extractive per-message cap. Do not reduce this corpus to make retention pass.
func seedSummaryRolloverHistory(session *core.Session, oldValue string) error {
	for index := 0; index < summaryRolloverSeedTurns; index++ {
		runID := "rollover-seed-" + strconv.Itoa(index)
		if _, err := session.Append(runID, core.EvRunStart, core.RunStartData{}); err != nil {
			return err
		}
		user := "Historical Project Orion archive record " + strconv.Itoa(index) + ": " + strings.Repeat("stable background detail ", 39)
		if index == 0 {
			user = "Historical Project Orion previous launch code: " + oldValue + ". " + strings.Repeat("stable background detail ", 36)
		}
		// This fixed packing record fills the remaining extractive envelope after
		// the high-volume records above. Its position is deliberate: it keeps the
		// first durable summary below the prior-summary fit boundary, where a later
		// correction can otherwise be displaced on the next rollover.
		if index == summaryRolloverSeedTurns-1 {
			user = "Historical Project Orion archive packing record: " + strings.Repeat("stable background detail ", 27)
		}
		if _, err := session.Append(runID, core.EvUserMessage, core.UserMessageData{Text: user}); err != nil {
			return err
		}
		if _, err := session.Append(runID, core.EvAssistantMessage, core.AssistantMessageData{Text: "Acknowledged archival record " + strconv.Itoa(index) + "."}); err != nil {
			return err
		}
		if _, err := session.Append(runID, core.EvRunEnd, core.RunEndData{Status: core.RunCompleted}); err != nil {
			return err
		}
	}
	return nil
}

type summaryRolloverObservedAdapter struct {
	inner    core.LlmAdapter
	maxCalls int
	mu       sync.Mutex
	calls    []summaryRolloverObservedCall
}
type summaryRolloverObservedCall struct {
	messages []core.ChatMessage
	usage    core.TokenUsage
	hasUsage bool
}

func newSummaryRolloverObservedAdapter(inner core.LlmAdapter, maxCalls int) *summaryRolloverObservedAdapter {
	return &summaryRolloverObservedAdapter{inner: inner, maxCalls: maxCalls}
}
func (m *summaryRolloverObservedAdapter) Provider() string { return m.inner.Provider() }
func (m *summaryRolloverObservedAdapter) Stream(ctx context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	m.mu.Lock()
	if m.maxCalls <= 0 || len(m.calls) >= m.maxCalls {
		m.mu.Unlock()
		return errors.New("summary rollover model call budget reached")
	}
	index := len(m.calls)
	m.calls = append(m.calls, summaryRolloverObservedCall{messages: cloneSummaryMessages(options.Messages)})
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
func (m *summaryRolloverObservedAdapter) snapshot() []summaryRolloverObservedCall {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]summaryRolloverObservedCall, len(m.calls))
	for index := range m.calls {
		result[index] = m.calls[index]
		result[index].messages = cloneSummaryMessages(m.calls[index].messages)
	}
	return result
}

type summaryRolloverRunUsage struct {
	RunID        string `json:"run_id"`
	InvocationID string `json:"invocation_id"`
	InputTokens  int64  `json:"input_tokens"`
	OutputTokens int64  `json:"output_tokens"`
	Reports      int    `json:"reports"`
	Complete     bool   `json:"complete"`
}
type summaryRolloverEvidence struct {
	Schema                    string                    `json:"schema"`
	CaseID                    string                    `json:"case_id"`
	RequestedModel            string                    `json:"requested_model"`
	BinaryRevision            string                    `json:"source_revision"`
	FinalPromptSHA256         string                    `json:"final_prompt_sha256"`
	TotalSeedTurns            int                       `json:"total_seed_turns"`
	TotalSeedBytes            int                       `json:"total_seed_bytes"`
	RunUsage                  []summaryRolloverRunUsage `json:"run_usage"`
	UsageComplete             bool                      `json:"usage_complete"`
	UsageLedgerMatches        bool                      `json:"usage_ledger_matches"`
	UniqueInvocationIDs       bool                      `json:"unique_invocation_ids"`
	ModelCalls                int                       `json:"model_calls"`
	SummaryEvents             int                       `json:"summary_events"`
	SummaryBytes              int                       `json:"summary_bytes"`
	SummaryByteCounts         []int                     `json:"summary_byte_counts"`
	SummaryHasOmissionMarker  bool                      `json:"summary_has_omission_marker"`
	EffectiveProjectionExact  bool                      `json:"effective_projection_exact"`
	SummaryCoverageAdvanced   bool                      `json:"summary_coverage_advanced"`
	FinalContextObserved      bool                      `json:"final_context_observed"`
	NewInFinalContext         bool                      `json:"new_in_final_context"`
	OldInFinalContext         bool                      `json:"old_in_final_context"`
	FinalAnswerCorrect        bool                      `json:"final_answer_correct"`
	DurableFinalAnswerCorrect bool                      `json:"durable_final_answer_correct"`
	AcknowledgementsExact     bool                      `json:"acknowledgements_exact"`
	AcceptancePassed          bool                      `json:"acceptance_passed"`
}

func summaryRolloverEvidenceFromRun(caseID string, fixture *summaryRolloverFixture, observed *summaryRolloverObservedAdapter, runIDs []string, results [3]core.TurnResult, events []core.SessionEvent, values summaryRolloverValues) summaryRolloverEvidence {
	hash := sha256.Sum256([]byte(summaryRolloverFinalPrompt))
	evidence := summaryRolloverEvidence{Schema: "harness.programmatic.live-summary-rollover/v1", CaseID: caseID, BinaryRevision: liveSourceRevision(), FinalPromptSHA256: fmt.Sprintf("%x", hash)}
	if fixture != nil {
		evidence.RequestedModel = fixture.requestedModel
	}
	evidence.TotalSeedTurns, evidence.TotalSeedBytes = summaryRolloverSeedFacts(events)
	evidence.SummaryEvents, evidence.SummaryBytes, evidence.SummaryByteCounts, evidence.SummaryHasOmissionMarker, evidence.SummaryCoverageAdvanced = summaryRolloverSummaryFacts(events)
	if fixture != nil {
		evidence.EffectiveProjectionExact = summaryRolloverEffectiveProjectionExact(fixture.session, events)
	}
	calls := observed.snapshot()
	evidence.ModelCalls = len(calls)
	evidence.FinalContextObserved = len(calls) == len(runIDs) && len(calls) > 0
	if evidence.FinalContextObserved {
		finalContext := calls[len(calls)-1].messages
		evidence.NewInFinalContext = summaryRolloverMessagesContain(finalContext, values.New)
		evidence.OldInFinalContext = summaryRolloverMessagesContain(finalContext, values.Old)
	}
	evidence.RunUsage, evidence.UsageComplete, evidence.UsageLedgerMatches, evidence.UniqueInvocationIDs = summaryRolloverUsageFacts(events, runIDs, calls)
	evidence.AcknowledgementsExact = summaryRolloverAcknowledgementsExact(events, runIDs, results)
	expected := "CURRENT: " + values.New
	evidence.FinalAnswerCorrect = strings.TrimSpace(results[len(results)-1].Answer) == expected
	durable := summaryRolloverDurableAnswer(events, runIDs[len(runIDs)-1])
	evidence.DurableFinalAnswerCorrect = strings.TrimSpace(durable) == expected
	return evidence
}

func summaryRolloverSeedFacts(events []core.SessionEvent) (turns, bytes int) {
	for _, event := range events {
		if !strings.HasPrefix(event.RunID, "history-seed-") && !strings.HasPrefix(event.RunID, "rollover-seed-") {
			continue
		}
		if event.Type == core.EvRunStart {
			turns++
		}
		switch event.Type {
		case core.EvUserMessage:
			var data core.UserMessageData
			if json.Unmarshal(event.Data, &data) == nil {
				bytes += len(data.Text)
			}
		case core.EvAssistantMessage:
			var data core.AssistantMessageData
			if json.Unmarshal(event.Data, &data) == nil {
				bytes += len(data.Text)
			}
		}
	}
	return turns, bytes
}

func summaryRolloverSummaryFacts(events []core.SessionEvent) (count, bytes int, sizes []int, omitted, advanced bool) {
	previousEnd := int64(-1)
	advanced = true
	for _, event := range events {
		if event.Type != core.EvContextSummary {
			continue
		}
		var data core.ContextSummaryData
		if json.Unmarshal(event.Data, &data) != nil {
			return count, bytes, sizes, omitted, false
		}
		count++
		bytes += len(data.Summary)
		sizes = append(sizes, len(data.Summary))
		omitted = omitted || strings.Contains(data.Summary, "[extractive omitted") || strings.Contains(data.Summary, "[omitted bytes=")
		if data.Start < 0 || data.End < data.Start || (previousEnd >= 0 && data.End <= previousEnd) {
			advanced = false
		}
		previousEnd = data.End
	}
	return count, bytes, sizes, omitted, advanced && count > 0
}

func summaryRolloverEffectiveProjectionExact(session *core.Session, events []core.SessionEvent) bool {
	if session == nil {
		return false
	}
	bySequence := summaryEventsBySequence(events)
	messages, err := session.DeriveMessages()
	if err != nil {
		return false
	}
	found := false
	for _, message := range messages {
		if message.Provenance == nil || message.Provenance.Kind != "summary" {
			continue
		}
		found = true
		data, exists := bySequence[message.SourceSeq]
		if !exists || message.Provenance.SourceStart != data.Start || message.Provenance.SourceEnd != data.End || message.Content != fmt.Sprintf("[conversation summary of events %d–%d]\n%s", data.Start, data.End, data.Summary) {
			return false
		}
	}
	return found
}
func summaryRolloverMessagesContain(messages []core.ChatMessage, value string) bool {
	for _, message := range messages {
		if value != "" && strings.Contains(message.Content, value) {
			return true
		}
	}
	return false
}

func summaryRolloverAcknowledgementsExact(events []core.SessionEvent, runIDs []string, results [3]core.TurnResult) bool {
	if len(runIDs) < 2 || strings.TrimSpace(results[0].Answer) != "ACK" || strings.TrimSpace(results[1].Answer) != "ACK" {
		return false
	}
	return strings.TrimSpace(summaryRolloverDurableAnswer(events, runIDs[0])) == "ACK" && strings.TrimSpace(summaryRolloverDurableAnswer(events, runIDs[1])) == "ACK"
}

func summaryRolloverUsageFacts(events []core.SessionEvent, runIDs []string, calls []summaryRolloverObservedCall) ([]summaryRolloverRunUsage, bool, bool, bool) {
	byRun := make(map[string]*summaryRolloverRunUsage, len(runIDs))
	for _, runID := range runIDs {
		byRun[runID] = &summaryRolloverRunUsage{RunID: runID}
	}
	var ledger core.TokenUsage
	valid := true
	invocationIDs := make(map[string]struct{}, len(runIDs))
	for _, event := range events {
		target := byRun[event.RunID]
		if target == nil || event.Type != core.EvRunUsage {
			continue
		}
		var data core.RunUsageData
		if json.Unmarshal(event.Data, &data) != nil || !strings.HasPrefix(data.InvocationID, "model:") || data.InvocationID == "model:" {
			valid = false
			continue
		}
		if _, exists := invocationIDs[data.InvocationID]; exists {
			valid = false
		}
		invocationIDs[data.InvocationID] = struct{}{}
		target.Reports++
		target.InvocationID = data.InvocationID
		target.InputTokens += data.InputTokens
		target.OutputTokens += data.OutputTokens
		ledger.InputTokens += data.InputTokens
		ledger.OutputTokens += data.OutputTokens
	}
	result := make([]summaryRolloverRunUsage, 0, len(runIDs))
	complete := len(calls) == len(runIDs)
	var observed core.TokenUsage
	for index, runID := range runIDs {
		entry := byRun[runID]
		hasCall := index < len(calls)
		if hasCall {
			observed.InputTokens += calls[index].usage.InputTokens
			observed.OutputTokens += calls[index].usage.OutputTokens
		}
		entry.Complete = hasCall && entry.Reports == 1 && (entry.InputTokens != 0 || entry.OutputTokens != 0) && calls[index].hasUsage && (calls[index].usage.InputTokens != 0 || calls[index].usage.OutputTokens != 0) && entry.InputTokens == calls[index].usage.InputTokens && entry.OutputTokens == calls[index].usage.OutputTokens
		complete = complete && entry.Complete
		result = append(result, *entry)
	}
	unique := valid && len(invocationIDs) == len(runIDs)
	return result, complete && unique, complete && unique && ledger == observed, unique
}
func summaryRolloverDurableAnswer(events []core.SessionEvent, runID string) string {
	answer := ""
	for _, event := range events {
		if event.RunID != runID || event.Type != core.EvAssistantMessage {
			continue
		}
		var data core.AssistantMessageData
		if json.Unmarshal(event.Data, &data) == nil && len(data.ToolCalls) == 0 && data.ToolCall == nil {
			answer = data.Text
		}
	}
	return answer
}

func assertSummaryRolloverAcceptance(t *testing.T, fixture *summaryRolloverFixture, observed *summaryRolloverObservedAdapter, runIDs []string, results [3]core.TurnResult, events []core.SessionEvent, values summaryRolloverValues) {
	t.Helper()
	evidence := summaryRolloverEvidenceFromRun("assert", fixture, observed, runIDs, results, events, values)
	if evidence.ModelCalls != len(runIDs) || evidence.SummaryEvents != len(runIDs) || len(evidence.SummaryByteCounts) != len(runIDs) || evidence.SummaryByteCounts[0] < 11900 || evidence.SummaryByteCounts[0] > 12064 || !evidence.EffectiveProjectionExact || !evidence.SummaryCoverageAdvanced || !evidence.FinalContextObserved || !evidence.NewInFinalContext || !evidence.UsageComplete || !evidence.UsageLedgerMatches || !evidence.UniqueInvocationIDs || !evidence.AcknowledgementsExact || !evidence.FinalAnswerCorrect || !evidence.DurableFinalAnswerCorrect {
		t.Fatalf("summary rollover acceptance predicates failed: %+v", evidence)
	}
}

func writeSummaryRolloverEvidence(directory string, evidence summaryRolloverEvidence) error {
	if strings.TrimSpace(directory) == "" {
		return nil
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return errors.New("live summary rollover evidence directory is unavailable")
	}
	payload, err := json.MarshalIndent(evidence, "", "  ")
	if err != nil {
		return errors.New("live summary rollover evidence encoding failed")
	}
	identity := sha256.Sum256([]byte(evidence.CaseID + "\x00" + time.Now().UTC().Format(time.RFC3339Nano)))
	return writeLiveEvidenceFile(filepath.Join(directory, "programmatic-live-summary-rollover-"+fmt.Sprintf("%x", identity[:12])+".json"), append(payload, '\n'))
}
