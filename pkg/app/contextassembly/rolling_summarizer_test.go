package contextassembly

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/whhhh1500/auto-agent/pkg/core"
)

func rollingTestSession(t testing.TB) (*core.Session, core.SessionOptions) {
	t.Helper()
	scope := core.MustScopePath(core.ScopeRef{Kind: core.ScopeProduct, ID: "summary"})
	scope, _ = scope.Child(core.ScopeRef{Kind: core.ScopeSession, ID: "rolling"})
	opts := core.SessionOptions{ID: "rolling", ProfileID: "summary", Scope: scope, Principal: core.Principal{SubjectID: "tester", TenantID: "tenant", Scope: scope}}
	session, err := core.NewSession(opts)
	if err != nil {
		t.Fatal(err)
	}
	return session, opts
}

func rollingAppend(t testing.TB, session *core.Session, turn int) {
	t.Helper()
	for _, event := range []struct {
		kind core.SessionEventType
		data any
	}{
		{core.EvUserMessage, core.UserMessageData{Text: fmt.Sprintf("user-%d", turn)}},
		{core.EvAssistantMessage, core.AssistantMessageData{Text: fmt.Sprintf("answer-%d", turn)}},
	} {
		if _, err := session.Append("seed", event.kind, event.data); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRollingSummarizerRepeatedRangesAndRestore(t *testing.T) {
	for _, limits := range [][2]int{{6, 2}, {8, 7}, {120, 60}} {
		t.Run(fmt.Sprint(limits), func(t *testing.T) {
			session, opts := rollingTestSession(t)
			r := &RollingSummarizer{MaxMessages: limits[0], KeepTail: limits[1]}
			var archived []core.ChatMessage
			r.Summarizer = ContextSummarizerFunc(func(_ context.Context, messages []core.ChatMessage) (string, error) {
				archived = append([]core.ChatMessage(nil), messages...)
				return "confirmed-fact", nil
			})
			for cycle := 0; cycle < 4; cycle++ {
				for i := 0; i < limits[0]/2; i++ {
					rollingAppend(t, session, cycle*limits[0]+i)
				}
				before, err := session.DeriveMessages()
				if err != nil {
					t.Fatal(err)
				}
				result, err := r.EnsureSummarized(context.Background(), session, "summarize", nil, before)
				if err != nil {
					t.Fatalf("cycle=%d: %v", cycle, err)
				}
				if len(before) <= limits[0] {
					continue
				}
				if len(result) != 1+len(before)-len(archived) || !reflect.DeepEqual(result[1:], before[len(archived):]) {
					t.Fatalf("cycle=%d: projection must be exactly replacement + unmodified tail: before=%d archived=%d after=%d", cycle, len(before), len(archived), len(result))
				}
				if result[0].Provenance.SourceStart != 0 {
					t.Fatalf("cycle=%d: original provenance lost: %+v", cycle, result[0].Provenance)
				}
				if result[len(result)-1].Content != before[len(before)-1].Content {
					t.Fatal("latest turn lost")
				}
				restored, err := core.RestoreSession(opts, session.Events())
				if err != nil {
					t.Fatal(err)
				}
				replayed, err := restored.DeriveMessages()
				if err != nil || !reflect.DeepEqual(result, replayed) {
					t.Fatalf("restored projection differs: %v", err)
				}
				session = restored
			}
		})
	}
}

func TestExtractiveSummarizerPreservesLargePriorSummary(t *testing.T) {
	s, err := NewExtractiveSummarizer(ExtractiveSummarizerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	fact := "immutable-anchor-97"
	prior := fact + strings.Repeat(" verified historical constraint;", 50)
	for cycle := 0; cycle < 3; cycle++ {
		prior, err = s.Summarize(context.Background(), []core.ChatMessage{
			{Role: core.RoleUser, Content: prior, Provenance: &core.ContextProvenance{Kind: "summary"}},
			{Role: core.RoleUser, Content: "continue the task"},
		})
		if err != nil || !strings.Contains(prior, fact) || len(prior) > DefaultExtractiveSummaryMaxBytes {
			t.Fatalf("cycle=%d: old summary within total budget lost its fact: bytes=%d err=%v", cycle, len(prior), err)
		}
	}
}

func TestRollingSummarizerClosesRangeBeforeRetainingTail(t *testing.T) {
	session, _ := rollingTestSession(t)
	for i := 0; i < 5; i++ {
		rollingAppend(t, session, i)
	}
	r := &RollingSummarizer{MaxMessages: 8, KeepTail: 7, Summarizer: ContextSummarizerFunc(func(context.Context, []core.ChatMessage) (string, error) { return "summary", nil })}
	before, _ := session.DeriveMessages()
	if _, err := r.EnsureSummarized(context.Background(), session, "summary", nil, before); err != nil {
		t.Fatal(err)
	}
	rollingAppend(t, session, 5)
	before, _ = session.DeriveMessages()
	after, err := r.EnsureSummarized(context.Background(), session, "summary", nil, before)
	if err != nil || len(after) != 3 || !reflect.DeepEqual(after[1:], before[len(before)-2:]) {
		t.Fatalf("must archive all messages preceding the old summary event and preserve latest turn: messages=%d err=%v", len(after), err)
	}
}

func TestRollingSummarizerArchivesCompleteProgramGroupAcrossThreshold(t *testing.T) {
	session, _ := rollingTestSession(t)
	parent := core.ToolCall{ID: "program-42", Name: "program.execute", Args: map[string]any{"program": "batch"}}
	appendEvent := func(kind core.SessionEventType, data any) {
		t.Helper()
		if _, err := session.Append("program-turn", kind, data); err != nil {
			t.Fatal(err)
		}
	}
	appendEvent(core.EvUserMessage, core.UserMessageData{Text: "process this batch"})
	appendEvent(core.EvAssistantMessage, core.AssistantMessageData{ToolCall: &parent, ToolCalls: []core.ToolCall{parent}})
	appendEvent(core.EvToolCall, core.ToolCallData{CallID: parent.ID, Name: parent.Name, Args: parent.Args})
	appendEvent(core.EvToolResult, core.ToolResultData{CallID: parent.ID + "/read", Content: "large raw child result", OK: true})
	approvalID := "apr_0123456789abcdef0123456789abcdef"
	appendEvent(core.EvApprovalRequested, core.ApprovalRequestedData{ApprovalID: approvalID, ToolCall: parent, ResumeCall: parent})
	appendEvent(core.EvApprovalResolved, core.ApprovalResolvedData{ApprovalID: approvalID, CallID: parent.ID, Decision: core.ApprovalApproved, ResolvedAt: time.Unix(1, 0).UTC()})
	appendEvent(core.EvToolResult, core.ToolResultData{CallID: parent.ID + "/write", Content: "child result after approval", OK: true})
	appendEvent(core.EvToolResult, core.ToolResultData{CallID: parent.ID, Content: `{"status":"completed"}`, OK: true})
	appendEvent(core.EvUserMessage, core.UserMessageData{Text: "now explain the result"})
	appendEvent(core.EvAssistantMessage, core.AssistantMessageData{Text: "latest answer"})

	before, err := session.DeriveMessages()
	if err != nil || len(before) != 7 {
		t.Fatalf("projected program fixture messages=%#v err=%v", before, err)
	}
	archived := []core.ChatMessage(nil)
	extractive, err := NewExtractiveSummarizer(ExtractiveSummarizerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	r := &RollingSummarizer{MaxMessages: 5, KeepTail: 2, Summarizer: ContextSummarizerFunc(func(ctx context.Context, messages []core.ChatMessage) (string, error) {
		archived = append([]core.ChatMessage(nil), messages...)
		return extractive.Summarize(ctx, messages)
	})}
	after, err := r.EnsureSummarized(context.Background(), session, "summary", nil, before)
	if err != nil {
		t.Fatal(err)
	}
	if len(archived) != 5 || !containsToolCall(archived, parent.ID, parent.Name) || !containsMessage(archived, `{"status":"completed"}`) {
		t.Fatalf("program parent/result were split from archived prefix: %#v", archived)
	}
	if len(after) != 3 || after[1].Content != "now explain the result" || after[2].Content != "latest answer" {
		t.Fatalf("unexpected summarized tail: %#v", after)
	}
	if strings.Contains(after[0].Content, "large raw child result") || strings.Contains(after[0].Content, "child result after approval") || !strings.Contains(after[0].Content, `{"status":"completed"}`) {
		t.Fatalf("program summary leaked nested results or lost parent outcome: %q", after[0].Content)
	}
}

func TestRollingSummarizerFailureDoesNotAppend(t *testing.T) {
	for _, failure := range []string{"no_safe_boundary", "cancelled", "invalid_output"} {
		t.Run(failure, func(t *testing.T) {
			session, _ := rollingTestSession(t)
			for i := 0; i < 3; i++ {
				rollingAppend(t, session, i)
			}
			messages, _ := session.DeriveMessages()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			calls, emits := 0, 0
			r := &RollingSummarizer{MaxMessages: 4, KeepTail: 2, Summarizer: ContextSummarizerFunc(func(context.Context, []core.ChatMessage) (string, error) {
				calls++
				if failure == "cancelled" {
					cancel()
				}
				if failure == "invalid_output" {
					return "", nil
				}
				return "summary", nil
			})}
			if failure == "no_safe_boundary" {
				messages[0].Provenance = &core.ContextProvenance{Kind: "summary", SourceStart: 0, SourceEnd: 100}
			}
			version := session.Version()
			if _, err := r.EnsureSummarized(ctx, session, "summary", func(core.SessionEvent) { emits++ }, messages); err == nil {
				t.Fatal("expected failure")
			}
			if emits != 0 || version != session.Version() || (failure == "no_safe_boundary" && calls != 0) {
				t.Fatal("failure performed a forbidden model call or mutation")
			}
		})
	}
}

func TestExtractiveSummarizerPairsReusedToolIDs(t *testing.T) {
	s, _ := NewExtractiveSummarizer(ExtractiveSummarizerConfig{})
	text, err := s.Summarize(context.Background(), []core.ChatMessage{
		{Role: core.RoleAssistant, ToolCalls: []core.ToolCall{{ID: "reused", Name: "first.lookup"}}},
		{Role: core.RoleTool, ToolCallID: "reused", Content: "first-result"},
		{Role: core.RoleUser, Content: "next task"},
		{Role: core.RoleAssistant, ToolCalls: []core.ToolCall{{ID: "reused", Name: "second.lookup"}}},
		{Role: core.RoleTool, ToolCallID: "reused", Content: "second-result"},
	})
	if err != nil || strings.Contains(text, "status=unpaired") || !strings.Contains(text, "name=second.lookup\ntool_result id=reused result=second-result") {
		t.Fatalf("reused ID paired incorrectly: %q %v", text, err)
	}
}

func TestExtractiveSummarizerOversizePriorIsExplicitlyOmitted(t *testing.T) {
	s, _ := NewExtractiveSummarizer(ExtractiveSummarizerConfig{})
	text, err := s.Summarize(context.Background(), []core.ChatMessage{
		{Role: core.RoleUser, Content: strings.Repeat("x", DefaultExtractiveSummaryMaxBytes), Provenance: &core.ContextProvenance{Kind: "summary"}},
		{Role: core.RoleUser, Content: "current constraint"},
	})
	if err != nil || len(text) > DefaultExtractiveSummaryMaxBytes || !strings.Contains(text, "prior_summary: [omitted bytes=") || !strings.Contains(text, "current constraint") {
		t.Fatalf("unbounded or unlabeled prior: %q %v", text, err)
	}
}

// These benchmarks separate archive selection and local extraction from the
// complete restore/append/projection path. B/op is allocation, not retained RSS.
func BenchmarkRollingSummary(b *testing.B) {
	session, opts := rollingTestSession(b)
	for i := 0; i < 64; i++ {
		rollingAppend(b, session, i)
	}
	extractive, err := NewExtractiveSummarizer(ExtractiveSummarizerConfig{})
	if err != nil {
		b.Fatal(err)
	}
	r := &RollingSummarizer{MaxMessages: 120, KeepTail: 60, Summarizer: extractive}
	messages, _ := session.DeriveMessages()
	if _, err := r.EnsureSummarized(context.Background(), session, "summary", nil, messages); err != nil {
		b.Fatal(err)
	}
	for i := 64; i < 94; i++ {
		rollingAppend(b, session, i)
	}
	messages, _ = session.DeriveMessages()
	boundary, _, _ := summarizeArchiveRange(messages, 60)
	if len(messages) != 121 || boundary == 0 {
		b.Fatal("invalid rolling benchmark fixture")
	}
	events := session.Events()
	b.Run("closed_range_121_messages", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if n, _, _ := summarizeArchiveRange(messages, 60); n != boundary {
				b.Fatal("invalid range")
			}
		}
	})
	b.Run("extractive_archived_prefix", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := extractive.Summarize(context.Background(), messages[:boundary]); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("restore_and_archive_121_messages", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			restored, err := core.RestoreSession(opts, events)
			if err != nil {
				b.Fatal(err)
			}
			projected, err := restored.DeriveMessages()
			if err != nil {
				b.Fatal(err)
			}
			if _, err := r.EnsureSummarized(context.Background(), restored, "summary", nil, projected); err != nil {
				b.Fatal(err)
			}
		}
	})
}
