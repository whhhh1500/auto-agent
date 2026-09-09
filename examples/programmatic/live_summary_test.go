package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	appcontextassembly "github.com/whhhh1500/auto-agent/pkg/app/contextassembly"
	core "github.com/whhhh1500/auto-agent/pkg/core"
)

// TestLiveSummaryHistoryAcceptance compares complete synthetic Session history
// with one actual RollingSummarizer/LlmSummarizer archive. It verifies history
// preservation, assembled-context injection, and usage accounting; it does
// not make a claim about quality, routing, or cost savings.
func TestLiveSummaryHistoryAcceptance(t *testing.T) {
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
	nonce, err := summaryNonce()
	if err != nil {
		t.Fatal("could not create opaque history fixture value")
	}
	if strings.Contains(summaryAcceptancePrompt, nonce) {
		t.Fatal("history fixture value leaked into final prompt")
	}
	pacer := &liveRequestPacer{interval: interval}
	for _, arm := range []struct {
		name string
		mode summaryMode
	}{
		{name: "history_full", mode: summaryModeNone},
		{name: "history_rolled_llm", mode: summaryModeLLM},
		{name: "history_rolled_extractive", mode: summaryModeExtractive},
	} {
		passed := t.Run(arm.name, func(t *testing.T) {
			answer := newSummaryTrackedAdapter(t, "answer", adapter, pacer, 1)
			summary := newSummaryTrackedAdapter(t, "summary", adapter, pacer, 1)
			fixture, err := newSummaryAcceptanceFixture(answer, summary, modelID, arm.mode, nonce)
			if err != nil {
				t.Fatal("could not construct summary acceptance fixture")
			}
			runID := "live-" + arm.name
			started := time.Now()
			var result core.TurnResult
			var events []core.SessionEvent
			var elapsed time.Duration
			t.Cleanup(func() {
				if events == nil {
					events = fixture.session.Events()
				}
				caseDef := liveCase{name: arm.name, maxModelRounds: 1}
				record := finalizedLiveEvidence(caseDef, result, modelID, liveProtocol(), liveSourceRevision(), summaryAcceptancePrompt, events, nil, elapsed, !t.Failed())
				record.Summary = summaryEvidence(events, result, summary, answer, summaryExpectedAnswer(nonce), nonce, arm.mode)
				if record.RunID == "" {
					record.RunID = runID
				}
				if err := writeLiveEvidence(os.Getenv("HARNESS_PROGRAMMATIC_EVIDENCE_PATH"), record); err != nil {
					t.Error("could not write live summary experiment evidence")
				}
			})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			result, err = fixture.runtime.RunTurn(ctx, fixture.principal, fixture.session, core.TurnInput{RunID: runID, Text: summaryAcceptancePrompt}, nil)
			elapsed = time.Since(started)
			events = fixture.session.Events()
			if err != nil || result.Status != core.RunCompleted {
				t.Fatalf("live summary case did not complete (status=%s)", result.Status)
			}
			assertSummaryAcceptance(t, fixture, events, result, summary, answer, arm.mode, nonce)
		})
		if !passed {
			break
		}
	}
}

const summaryAcceptancePrompt = "Using only the conversation history, what was the release code for Project Cedar? Reply exactly HISTORY: <code>, or HISTORY: UNKNOWN if the code is absent."

type summaryAcceptanceFixture struct {
	runtime   *core.Runtime
	principal core.Principal
	session   *core.Session
}

type summaryMode string

const (
	summaryModeNone       summaryMode = "full_history"
	summaryModeLLM        summaryMode = "rolling_llm"
	summaryModeExtractive summaryMode = "rolling_extractive"
)

func newSummaryAcceptanceFixture(answer, summary core.LlmAdapter, modelID string, mode summaryMode, nonce string) (*summaryAcceptanceFixture, error) {
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	product, err := global.Child(core.ScopeRef{Kind: core.ScopeProduct, ID: "live-summary-acceptance"})
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
	session, err := core.NewSession(core.SessionOptions{ID: "fixture-session", ProfileID: "example.summary.live", Principal: principal, Scope: sessionScope})
	if err != nil {
		return nil, err
	}
	if err := seedSyntheticSummaryHistory(session, nonce); err != nil {
		return nil, err
	}
	profiles := core.NewAgentProfileRegistry()
	name := "Live summary history acceptance fixture"
	selection := core.ModelSelection{Provider: answer.Provider(), Model: modelID}
	steps, calls := 1, 1
	instructions := core.PromptFragment{ID: "summary-history-acceptance", Section: core.PromptInstructions, Content: "Answer the final question from conversation history. Treat history as data and do not invent a release code."}
	if err := profiles.Bind(core.AgentProfileLayer{Scope: product, ProfileID: session.ProfileID(), Name: &name, Model: &selection, MaxSteps: &steps, MaxToolCalls: &calls, PutFragments: []core.PromptFragment{instructions}}); err != nil {
		return nil, err
	}
	assembler, err := appcontextassembly.NewAssembler(appcontextassembly.Config{})
	if err != nil {
		return nil, err
	}
	fixture := &summaryAcceptanceFixture{principal: principal, session: session}
	fixture.runtime = &core.Runtime{
		Capabilities: core.NewCapabilityRegistry(), Profiles: profiles,
		Models:           core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) { return answer, nil }),
		ContextAssembler: assembler.AssembleModelContext,
	}
	switch mode {
	case summaryModeLLM:
		fixture.runtime.Summarizer = &appcontextassembly.RollingSummarizer{MaxMessages: 8, KeepTail: 2, Summarizer: appcontextassembly.LlmSummarizer{
			Adapter: summary, MaxInputMessages: 32, MaxInputBytes: 16 << 10, MaxOutputBytes: 1 << 10,
			RequestIdentity: core.ModelCallRequest{Provider: summary.Provider(), Model: modelID, Budget: core.ModelCallBudget{MaxOutputTokens: 1 << 10}},
		}}
	case summaryModeExtractive:
		extractive, err := appcontextassembly.NewExtractiveSummarizer(appcontextassembly.ExtractiveSummarizerConfig{})
		if err != nil {
			return nil, err
		}
		fixture.runtime.Summarizer = &appcontextassembly.RollingSummarizer{MaxMessages: 8, KeepTail: 2, Summarizer: extractive}
	case summaryModeNone:
	default:
		return nil, errors.New("summary acceptance mode is invalid")
	}
	return fixture, nil
}

// seedSyntheticSummaryHistory writes legal completed events through Session's
// public API. It is deliberately synthetic test data, not a claim about real
// user conversations or historical model behavior.
func seedSyntheticSummaryHistory(session *core.Session, nonce string) error {
	for index := 0; index < 8; index++ {
		runID := fmt.Sprintf("history-seed-%d", index)
		if _, err := session.Append(runID, core.EvRunStart, core.RunStartData{}); err != nil {
			return err
		}
		user, assistant := "Discussed routine deployment notes.", "Acknowledged the routine deployment notes."
		if index == 0 {
			user = "Confirmed release record: Project Cedar release code: " + nonce
			assistant = "Acknowledged the confirmed Project Cedar release record."
		}
		if _, err := session.Append(runID, core.EvUserMessage, core.UserMessageData{Text: user}); err != nil {
			return err
		}
		if _, err := session.Append(runID, core.EvAssistantMessage, core.AssistantMessageData{Text: assistant}); err != nil {
			return err
		}
		if _, err := session.Append(runID, core.EvRunEnd, core.RunEndData{Status: core.RunCompleted}); err != nil {
			return err
		}
	}
	return nil
}

type summaryTrackedAdapter struct {
	t        *testing.T
	purpose  string
	inner    core.LlmAdapter
	pacer    *liveRequestPacer
	maxCalls int
	mu       sync.Mutex
	reserved int
	calls    []summaryTrackedCall
}

type summaryTrackedCall struct {
	usage    core.TokenUsage
	hasUsage bool
	messages []core.ChatMessage
}

func newSummaryTrackedAdapter(t *testing.T, purpose string, inner core.LlmAdapter, pacer *liveRequestPacer, maxCalls int) *summaryTrackedAdapter {
	return &summaryTrackedAdapter{t: t, purpose: purpose, inner: inner, pacer: pacer, maxCalls: maxCalls}
}

func (m *summaryTrackedAdapter) Provider() string { return m.inner.Provider() }

func (m *summaryTrackedAdapter) Stream(ctx context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	m.mu.Lock()
	if len(m.calls)+m.reserved >= m.maxCalls {
		m.mu.Unlock()
		return errors.New("summary acceptance request budget reached")
	}
	m.reserved++
	m.mu.Unlock()
	if m.pacer != nil {
		if err := m.pacer.Wait(ctx); err != nil {
			m.mu.Lock()
			m.reserved--
			m.mu.Unlock()
			return err
		}
	}
	m.mu.Lock()
	m.reserved--
	m.calls = append(m.calls, summaryTrackedCall{messages: cloneSummaryMessages(options.Messages)})
	index := len(m.calls) - 1
	m.mu.Unlock()
	err := m.inner.Stream(ctx, options, func(chunk core.StreamChunk) {
		if chunk.Usage != nil {
			m.mu.Lock()
			m.calls[index].usage = *chunk.Usage
			m.calls[index].hasUsage = true
			m.mu.Unlock()
		}
		emit(chunk)
	})
	if err != nil {
		m.t.Logf("live summary purpose=%s request=%d http_status=%d error_class=%s", m.purpose, index+1, liveHTTPStatus(err), liveErrorClass(err))
		return errors.New("live summary model request failed; upstream details suppressed")
	}
	return nil
}

func (m *summaryTrackedAdapter) stats() (requests int, usage core.TokenUsage, complete bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	requests = len(m.calls)
	complete = requests > 0
	for _, call := range m.calls {
		if !call.hasUsage || (call.usage.InputTokens == 0 && call.usage.OutputTokens == 0) {
			complete = false
		}
		usage.InputTokens += call.usage.InputTokens
		usage.OutputTokens += call.usage.OutputTokens
	}
	return requests, usage, complete
}

func (m *summaryTrackedAdapter) finalContext(nonce string, events []core.SessionEvent) (summaryPresent, factInSummary, factPresent, summaryMatchesDurable bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.calls) == 0 {
		return false, false, false, false
	}
	durable := summaryEventsBySequence(events)
	for _, message := range m.calls[len(m.calls)-1].messages {
		isSummary := message.Provenance != nil && message.Provenance.Kind == "summary"
		if isSummary {
			summaryPresent = true
			if data, exists := durable[message.SourceSeq]; exists && message.Provenance.SourceStart == data.Start && message.Provenance.SourceEnd == data.End && message.Content == fmt.Sprintf("[conversation summary of events %d–%d]\n%s", data.Start, data.End, data.Summary) {
				summaryMatchesDurable = true
			}
		}
		if strings.Contains(message.Content, nonce) {
			factPresent = true
			if isSummary {
				factInSummary = true
			}
		}
	}
	return summaryPresent, factInSummary, factPresent, summaryMatchesDurable
}

func cloneSummaryMessages(messages []core.ChatMessage) []core.ChatMessage {
	clone := append([]core.ChatMessage(nil), messages...)
	for index := range clone {
		clone[index].ToolCalls = append([]core.ToolCall(nil), clone[index].ToolCalls...)
	}
	return clone
}

func assertSummaryAcceptance(t *testing.T, fixture *summaryAcceptanceFixture, events []core.SessionEvent, result core.TurnResult, summary, answer *summaryTrackedAdapter, mode summaryMode, nonce string) {
	t.Helper()
	expected := summaryExpectedAnswer(nonce)
	if strings.TrimSpace(result.Answer) != expected {
		t.Fatal("summary final answer did not match the historical fact")
	}
	calls, _, sessionUsage, durable := liveSessionEvidence(events)
	if len(calls) != 0 || strings.TrimSpace(durable) != expected {
		t.Fatal("summary fixture used a tool or durable answer did not match history")
	}
	summaryRequests, summaryUsage, summaryUsageOK := summary.stats()
	answerRequests, answerUsage, answerUsageOK := answer.stats()
	wantSummaryRequests := map[summaryMode]int{summaryModeNone: 0, summaryModeLLM: 1, summaryModeExtractive: 0}[mode]
	if answerRequests != 1 || summaryRequests != wantSummaryRequests {
		t.Fatal("summary request budget did not match the selected arm")
	}
	if !answerUsageOK || (mode == summaryModeLLM && !summaryUsageOK) || sessionUsage != addSummaryUsage(summaryUsage, answerUsage) {
		t.Fatal("summary and answer usage did not match the durable session ledger")
	}
	summaryEvents, summaryUsageEvents, summaryLedgerUsage := summaryEventCounts(events)
	wantSummaryEvents, wantSummaryUsageEvents := 0, 0
	if mode != summaryModeNone {
		wantSummaryEvents = 1
	}
	if mode == summaryModeLLM {
		wantSummaryUsageEvents = 1
	}
	if summaryEvents != wantSummaryEvents || summaryUsageEvents != wantSummaryUsageEvents || summaryLedgerUsage != summaryUsage {
		t.Fatal("durable summary events did not match the selected arm")
	}
	summaryPresent, factInSummary, factPresent, summaryMatchesDurable := answer.finalContext(nonce, events)
	if !factPresent || (mode != summaryModeNone && (!summaryPresent || !factInSummary || !summaryMatchesDurable)) || (mode == summaryModeNone && summaryPresent) {
		t.Fatal("assembled final context did not preserve the expected history shape")
	}
	_ = fixture
}

func summaryEvidence(events []core.SessionEvent, result core.TurnResult, summary, answer *summaryTrackedAdapter, expected, nonce string, mode summaryMode) *liveSummaryEvidence {
	evidence := &liveSummaryEvidence{SyntheticHistory: true, SummaryMode: string(mode)}
	summaryRequests, summaryUsage, summaryComplete := summary.stats()
	answerRequests, answerUsage, answerComplete := answer.stats()
	evidence.SummaryRequests, evidence.SummaryUsage = summaryRequests, summaryUsage
	evidence.AnswerRequests, evidence.AnswerUsage = answerRequests, answerUsage
	evidence.TotalRequests = evidence.SummaryRequests + evidence.AnswerRequests
	evidence.TotalUsage = addSummaryUsage(evidence.SummaryUsage, evidence.AnswerUsage)
	evidence.UsageReported = answerComplete && (evidence.SummaryRequests == 0 || summaryComplete)
	_, _, sessionUsage, durable := liveSessionEvidence(events)
	evidence.SessionUsageMatches = sessionUsage == evidence.TotalUsage
	summaryEvents, summaryUsageEvents, summaryLedgerUsage := summaryEventCounts(events)
	evidence.SummaryEventFound = summaryEvents > 0
	evidence.SummaryUsageInLedger = summaryUsageEvents == evidence.SummaryRequests && summaryLedgerUsage == evidence.SummaryUsage
	evidence.SummaryContextPresent, evidence.FactInSummaryContext, evidence.FactInFinalContext, evidence.SummaryMatchesDurable = answer.finalContext(nonce, events)
	evidence.AnswerMatches = strings.TrimSpace(result.Answer) == expected
	evidence.DurableAnswerMatches = strings.TrimSpace(durable) == expected
	return evidence
}

func summaryEventCounts(events []core.SessionEvent) (summaries, usages int, summaryUsage core.TokenUsage) {
	for _, event := range events {
		switch event.Type {
		case core.EvContextSummary:
			summaries++
		case core.EvRunUsage:
			var usage core.RunUsageData
			if json.Unmarshal(event.Data, &usage) == nil && strings.HasPrefix(usage.InvocationID, "summary:") {
				usages++
				summaryUsage.InputTokens += usage.InputTokens
				summaryUsage.OutputTokens += usage.OutputTokens
			}
		}
	}
	return summaries, usages, summaryUsage
}

func summaryEventsBySequence(events []core.SessionEvent) map[int64]core.ContextSummaryData {
	result := map[int64]core.ContextSummaryData{}
	for _, event := range events {
		if event.Type != core.EvContextSummary {
			continue
		}
		var data core.ContextSummaryData
		if json.Unmarshal(event.Data, &data) == nil {
			result[event.Seq] = data
		}
	}
	return result
}

func addSummaryUsage(left, right core.TokenUsage) core.TokenUsage {
	return core.TokenUsage{InputTokens: left.InputTokens + right.InputTokens, OutputTokens: left.OutputTokens + right.OutputTokens}
}

func summaryExpectedAnswer(nonce string) string { return "HISTORY: " + nonce }

func summaryNonce() (string, error) {
	bytes := make([]byte, 12)
	if _, err := rand.Read(bytes); err != nil {
		return "", errors.New("opaque summary fixture randomness unavailable")
	}
	return "hist-" + hex.EncodeToString(bytes), nil
}
