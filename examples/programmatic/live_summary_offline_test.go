package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	core "github.com/whhhh1500/auto-agent/pkg/core"
)

func TestSummaryAcceptanceFixtureOfflineSuccessArms(t *testing.T) {
	const nonce = "hist-offline-fixture-value"
	for _, mode := range []summaryMode{summaryModeNone, summaryModeLLM} {
		mode := mode
		t.Run(string(mode), func(t *testing.T) {
			summaryInner := &offlineSummaryAdapter{purpose: "summary"}
			answerInner := &offlineSummaryAdapter{purpose: "answer"}
			summary := newSummaryTrackedAdapter(t, "summary", summaryInner, nil, 1)
			answer := newSummaryTrackedAdapter(t, "answer", answerInner, nil, 1)
			fixture, err := newSummaryAcceptanceFixture(answer, summary, "offline-summary-probe", mode, nonce)
			if err != nil {
				t.Fatal("could not construct offline summary fixture")
			}
			result, err := fixture.runtime.RunTurn(context.Background(), fixture.principal, fixture.session, core.TurnInput{RunID: "offline-summary-" + string(mode), Text: summaryAcceptancePrompt}, nil)
			if err != nil || result.Status != core.RunCompleted {
				t.Fatal("offline summary fixture did not complete")
			}
			assertSummaryAcceptance(t, fixture, fixture.session.Events(), result, summary, answer, mode, nonce)
			evidence := summaryEvidence(fixture.session.Events(), result, summary, answer, summaryExpectedAnswer(nonce), nonce, mode)
			wantRequests := map[summaryMode]int{summaryModeNone: 1, summaryModeLLM: 2}[mode]
			if !evidence.UsageReported || !evidence.SessionUsageMatches || !evidence.FactInFinalContext || !evidence.AnswerMatches || !evidence.DurableAnswerMatches || evidence.AnswerRequests != 1 || evidence.TotalRequests != wantRequests || evidence.SummaryMode != string(mode) {
				t.Fatal("offline success evidence did not represent the selected arm")
			}
			if mode == summaryModeLLM && !(evidence.SummaryRequests == 1 && evidence.SummaryEventFound && evidence.SummaryUsageInLedger && evidence.SummaryContextPresent && evidence.FactInSummaryContext && evidence.SummaryMatchesDurable) {
				t.Fatal("offline summary evidence did not distinguish rolling history")
			}
		})
	}
}

func TestExtractiveSummaryEvidenceRetainsEarlierFact(t *testing.T) {
	const nonce = "hist-offline-extractive-earlier"
	summary := newSummaryTrackedAdapter(t, "summary", &offlineSummaryAdapter{purpose: "summary"}, nil, 1)
	answer := newSummaryTrackedAdapter(t, "answer", &offlineSummaryAdapter{purpose: "answer"}, nil, 1)
	fixture, err := newSummaryAcceptanceFixture(answer, summary, "offline-summary-probe", summaryModeExtractive, nonce)
	if err != nil {
		t.Fatal("could not construct extractive retention fixture")
	}
	result, err := fixture.runtime.RunTurn(context.Background(), fixture.principal, fixture.session, core.TurnInput{RunID: "offline-summary-extractive-loss", Text: summaryAcceptancePrompt}, nil)
	if err != nil || result.Status != core.RunCompleted {
		t.Fatal("extractive retention fixture did not complete")
	}
	evidence := summaryEvidence(fixture.session.Events(), result, summary, answer, summaryExpectedAnswer(nonce), nonce, summaryModeExtractive)
	if !evidence.UsageReported || !evidence.SessionUsageMatches || !evidence.SummaryEventFound || !evidence.SummaryUsageInLedger || !evidence.SummaryContextPresent || !evidence.SummaryMatchesDurable || !evidence.FactInFinalContext || !evidence.FactInSummaryContext || !evidence.AnswerMatches || !evidence.DurableAnswerMatches || evidence.TotalRequests != 1 || evidence.SummaryRequests != 0 {
		t.Fatal("extractive earlier-history retention was not represented by safe evidence")
	}
}

func TestSummaryEvidenceRejectsMissingSummaryUsage(t *testing.T) {
	const nonce = "hist-offline-no-usage"
	summaryInner := &offlineSummaryAdapter{purpose: "summary", omitUsage: true}
	answerInner := &offlineSummaryAdapter{purpose: "answer"}
	summary := newSummaryTrackedAdapter(t, "summary", summaryInner, nil, 1)
	answer := newSummaryTrackedAdapter(t, "answer", answerInner, nil, 1)
	fixture, err := newSummaryAcceptanceFixture(answer, summary, "offline-summary-probe", summaryModeLLM, nonce)
	if err != nil {
		t.Fatal("could not construct missing-usage fixture")
	}
	result, err := fixture.runtime.RunTurn(context.Background(), fixture.principal, fixture.session, core.TurnInput{RunID: "offline-summary-missing-usage", Text: summaryAcceptancePrompt}, nil)
	if err != nil || result.Status != core.RunCompleted {
		t.Fatal("missing-usage fixture did not complete")
	}
	evidence := summaryEvidence(fixture.session.Events(), result, summary, answer, summaryExpectedAnswer(nonce), nonce, summaryModeLLM)
	if evidence.UsageReported || !evidence.SummaryUsageInLedger || evidence.SummaryUsage.InputTokens != 0 || evidence.SummaryUsage.OutputTokens != 0 {
		t.Fatal("missing reported summary usage was accepted as complete accounting")
	}
}

func TestSummaryEvidencePersistsFactRetentionFailure(t *testing.T) {
	const nonce = "hist-offline-lost-fact"
	summaryInner := &offlineSummaryAdapter{purpose: "summary", dropFact: true}
	answerInner := &offlineSummaryAdapter{purpose: "answer"}
	summary := newSummaryTrackedAdapter(t, "summary", summaryInner, nil, 1)
	answer := newSummaryTrackedAdapter(t, "answer", answerInner, nil, 1)
	fixture, err := newSummaryAcceptanceFixture(answer, summary, "offline-summary-probe", summaryModeLLM, nonce)
	if err != nil {
		t.Fatal("could not construct fact-loss fixture")
	}
	result, err := fixture.runtime.RunTurn(context.Background(), fixture.principal, fixture.session, core.TurnInput{RunID: "offline-summary-lost-fact", Text: summaryAcceptancePrompt}, nil)
	if err != nil || result.Status != core.RunCompleted {
		t.Fatal("fact-loss fixture did not complete")
	}
	record := finalizedLiveEvidence(liveCase{name: "summary_completed_fact_loss"}, result, "offline-summary-probe", "test", "test-revision", summaryAcceptancePrompt, fixture.session.Events(), nil, 0, false)
	record.Summary = summaryEvidence(fixture.session.Events(), result, summary, answer, summaryExpectedAnswer(nonce), nonce, summaryModeLLM)
	if record.Summary == nil || !record.Summary.SummaryContextPresent || record.Summary.FactInFinalContext || record.Summary.FactInSummaryContext || !record.Summary.SummaryMatchesDurable || record.Summary.AnswerMatches || record.Summary.DurableAnswerMatches {
		t.Fatal("fact retention failure was not visible in safe summary evidence")
	}
	directory := t.TempDir()
	if err := writeLiveEvidence(directory, record); err != nil {
		t.Fatal("fact retention failure evidence was not written")
	}
	entries, err := os.ReadDir(directory)
	if err != nil || len(entries) != 1 {
		t.Fatal("fact retention evidence did not create one file")
	}
	payload, err := os.ReadFile(filepath.Join(directory, entries[0].Name()))
	if err != nil || strings.Contains(string(payload), nonce) {
		t.Fatal("summary evidence was unreadable or leaked the history fact")
	}
	var decoded liveEvidenceRecord
	if json.Unmarshal(payload, &decoded) != nil || decoded.AcceptancePassed || decoded.Summary == nil || decoded.Summary.FactInFinalContext || decoded.Summary.AnswerMatches {
		t.Fatal("persisted summary evidence lost fact-retention failure predicates")
	}
}

type offlineSummaryAdapter struct {
	purpose   string
	omitUsage bool
	dropFact  bool
}

func (*offlineSummaryAdapter) Provider() string { return "offline-summary-probe" }

func (m *offlineSummaryAdapter) Stream(_ context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	code, found := summaryCodeInMessages(options.Messages)
	text := "HISTORY: UNKNOWN"
	if m.purpose == "summary" {
		text = "Archived history contains no release code."
		if found && !m.dropFact {
			text = "Archive fact: Project Cedar release code: " + code
		}
	} else if found {
		text = "HISTORY: " + code
	}
	chunk := core.StreamChunk{Kind: core.StreamKindAssistant, Text: text}
	if !m.omitUsage {
		chunk.Usage = &core.TokenUsage{InputTokens: 7, OutputTokens: 2}
	}
	emit(chunk)
	emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop})
	return nil
}

func summaryCodeInMessages(messages []core.ChatMessage) (string, bool) {
	const prefix = "Project Cedar release code: "
	for _, message := range messages {
		index := strings.Index(message.Content, prefix)
		if index < 0 {
			continue
		}
		value := message.Content[index+len(prefix):]
		if newline := strings.IndexByte(value, '\n'); newline >= 0 {
			value = value[:newline]
		}
		if quote := strings.IndexByte(value, '"'); quote >= 0 {
			value = value[:quote]
		}
		value = strings.TrimSpace(value)
		if value != "" {
			return value, true
		}
	}
	return "", false
}
