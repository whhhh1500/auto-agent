package contextassembly

import (
	"context"
	"strings"
	"testing"

	"github.com/whhhh1500/auto-agent/pkg/core"
)

func TestExtractiveSummarizerBoundsLargeToolResultsAndPreservesEvidence(t *testing.T) {
	summarizer, err := NewExtractiveSummarizer(ExtractiveSummarizerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	large := strings.Repeat("TOP-SECRET-TOOL-PAYLOAD", 1<<16)
	messages := []core.ChatMessage{
		{Role: core.RoleUser, Content: "old request", SourceSeq: 1},
		{Role: core.RoleUser, Content: "prior durable context", SourceSeq: 2, Provenance: &core.ContextProvenance{Kind: "summary", Revision: "2"}},
		{Role: core.RoleAssistant, ToolCalls: []core.ToolCall{{ID: "call-1", Name: "rag.search"}, {ID: "call-2", Name: "memory.recall"}}, SourceSeq: 3},
		{Role: core.RoleTool, ToolCallID: "call-1", Content: large, SourceSeq: 4},
		{Role: core.RoleUser, Content: "latest goal: keep tenant facts", SourceSeq: 5},
	}
	first, err := summarizer.Summarize(context.Background(), messages)
	if err != nil {
		t.Fatal(err)
	}
	second, err := summarizer.Summarize(context.Background(), messages)
	if err != nil || first != second {
		t.Fatalf("summary is not deterministic: first=%q second=%q err=%v", first, second, err)
	}
	if len(first) > DefaultExtractiveSummaryMaxBytes || strings.Contains(first, "TOP-SECRET-TOOL-PAYLOAD") {
		t.Fatalf("large result leaked or output exceeded budget: bytes=%d", len(first))
	}
	for _, want := range []string{
		"[extractive summary source_messages=5]",
		"prior_summary: prior durable context",
		"recent_user_goal_or_constraint: latest goal: keep tenant facts",
		"tool_call id=call-1 name=rag.search",
		"tool_result id=call-1 result=[omitted bytes=",
		"tool_call id=call-2 name=memory.recall status=unresolved",
		"sha256=",
	} {
		if !strings.Contains(first, want) {
			t.Fatalf("summary missing %q: %q", want, first)
		}
	}
}

func TestExtractiveSummarizerRejectsInvalidConfigAndContext(t *testing.T) {
	for _, config := range []ExtractiveSummarizerConfig{
		{MaxBytes: minimumExtractiveSummaryBytes - 1},
		{MaxBytes: DefaultExtractiveSummaryMaxBytes + 1},
		{MaxMessageBytes: -1},
		{MaxToolBytes: -1},
	} {
		if _, err := NewExtractiveSummarizer(config); err == nil {
			t.Fatalf("invalid config accepted: %#v", config)
		}
	}
	summarizer, err := NewExtractiveSummarizer(ExtractiveSummarizerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	var nilContext context.Context
	if _, err := summarizer.Summarize(nilContext, []core.ChatMessage{{Role: core.RoleUser, Content: "x"}}); err == nil {
		t.Fatal("nil context accepted")
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := summarizer.Summarize(cancelled, []core.ChatMessage{{Role: core.RoleUser, Content: "x"}}); err == nil {
		t.Fatal("cancelled context accepted")
	}
}

func TestExtractiveSummarizerKeepsExactHardOutputCap(t *testing.T) {
	summarizer, err := NewExtractiveSummarizer(ExtractiveSummarizerConfig{MaxBytes: minimumExtractiveSummaryBytes, MaxMessageBytes: 256, MaxToolBytes: 256})
	if err != nil {
		t.Fatal(err)
	}
	messages := make([]core.ChatMessage, 64)
	for i := range messages {
		messages[i] = core.ChatMessage{Role: core.RoleUser, Content: strings.Repeat("payload ", 64)}
	}
	summary, err := summarizer.Summarize(context.Background(), messages)
	if err != nil || len(summary) > minimumExtractiveSummaryBytes || !strings.Contains(summary, "extractive omitted_records") {
		t.Fatalf("hard cap result bytes=%d summary=%q err=%v", len(summary), summary, err)
	}
}

func TestExtractiveSummarizerReservesRecentUserAfterNearLimitPriorSummary(t *testing.T) {
	summarizer, err := NewExtractiveSummarizer(ExtractiveSummarizerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	const recent = "latest update: retain the Cedar release constraint before answering"
	messages := []core.ChatMessage{
		{Role: core.RoleUser, Content: strings.Repeat("P", 11950), Provenance: &core.ContextProvenance{Kind: "summary"}},
		{Role: core.RoleUser, Content: recent},
	}
	summary, err := summarizer.Summarize(context.Background(), messages)
	if err != nil {
		t.Fatal(err)
	}
	if len(summary) > DefaultExtractiveSummaryMaxBytes {
		t.Fatalf("summary exceeded 12KiB cap: %d", len(summary))
	}
	if !strings.Contains(summary, "recent_user_goal_or_constraint: "+recent) {
		t.Fatal("near-limit prior summary displaced the recent user update")
	}
}

func TestExtractiveSummarizerKeepsFittingRecentUsersWhenTheirTotalExceedsEnvelope(t *testing.T) {
	summarizer, err := NewExtractiveSummarizer(ExtractiveSummarizerConfig{
		MaxBytes:        minimumExtractiveSummaryBytes,
		MaxMessageBytes: 128,
		MaxToolBytes:    128,
	})
	if err != nil {
		t.Fatal(err)
	}
	updates := []string{
		"recent-1 " + strings.Repeat("a", 112),
		"recent-2 " + strings.Repeat("b", 112),
		"recent-3 " + strings.Repeat("c", 112),
		"recent-4 " + strings.Repeat("d", 112),
	}
	messages := []core.ChatMessage{
		{Role: core.RoleUser, Content: strings.Repeat("P", 240), Provenance: &core.ContextProvenance{Kind: "summary"}},
	}
	for _, update := range updates {
		messages = append(messages, core.ChatMessage{Role: core.RoleUser, Content: update})
	}
	summary, err := summarizer.Summarize(context.Background(), messages)
	if err != nil {
		t.Fatal(err)
	}
	if len(summary) > minimumExtractiveSummaryBytes {
		t.Fatalf("summary exceeded hard cap: %d", len(summary))
	}
	if !strings.Contains(summary, "recent_user_goal_or_constraint: "+updates[0]) {
		t.Fatalf("no fitting recent record was retained: %q", summary)
	}
}

func TestExtractiveSummarizerRetainsEarlierUserWhenBudgetAllows(t *testing.T) {
	summarizer, err := NewExtractiveSummarizer(ExtractiveSummarizerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	messages := []core.ChatMessage{{Role: core.RoleUser, Content: "earliest confirmed fact: Cedar=alpha", SourceSeq: 1}}
	for index := 0; index < 5; index++ {
		messages = append(messages, core.ChatMessage{Role: core.RoleUser, Content: "recent goal " + string(rune('a'+index)), SourceSeq: int64(index + 2)})
	}
	summary, err := summarizer.Summarize(context.Background(), messages)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(summary, "earlier_user_history source_index=0: earliest confirmed fact: Cedar=alpha") {
		t.Fatalf("earlier fact missing: %q", summary)
	}
	for _, want := range []string{"recent_user_goal_or_constraint: recent goal b", "recent_user_goal_or_constraint: recent goal e"} {
		if !strings.Contains(summary, want) {
			t.Fatalf("recent user priority missing %q: %q", want, summary)
		}
	}
}

func TestExtractiveSummarizerBoundsAndLabelsOmittedEarlierUsers(t *testing.T) {
	summarizer, err := NewExtractiveSummarizer(ExtractiveSummarizerConfig{MaxBytes: minimumExtractiveSummaryBytes, MaxMessageBytes: 128, MaxToolBytes: 128})
	if err != nil {
		t.Fatal(err)
	}
	messages := make([]core.ChatMessage, 12)
	for index := range messages {
		messages[index] = core.ChatMessage{Role: core.RoleUser, Content: strings.Repeat("record ", 16) + string(rune('a'+index)), SourceSeq: int64(index + 1)}
	}
	summary, err := summarizer.Summarize(context.Background(), messages)
	if err != nil || len(summary) > minimumExtractiveSummaryBytes {
		t.Fatalf("summary bytes=%d err=%v", len(summary), err)
	}
	if !strings.Contains(summary, "omitted_earlier_users=") || strings.Contains(summary, "omitted_earlier_users=0") {
		t.Fatalf("omitted earlier-user count missing: %q", summary)
	}
}

func TestExtractiveSummarizerOmitsOversizedEarlierUsersWithoutHashRecords(t *testing.T) {
	summarizer, err := NewExtractiveSummarizer(ExtractiveSummarizerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	oversized := strings.Repeat("old-history ", 1<<17)
	messages := []core.ChatMessage{
		{Role: core.RoleUser, Content: oversized, SourceSeq: 1},
		{Role: core.RoleUser, Content: oversized, SourceSeq: 2},
		{Role: core.RoleUser, Content: "recent one", SourceSeq: 3},
		{Role: core.RoleUser, Content: "recent two", SourceSeq: 4},
		{Role: core.RoleUser, Content: "recent three", SourceSeq: 5},
		{Role: core.RoleUser, Content: "recent four", SourceSeq: 6},
	}
	summary, err := summarizer.Summarize(context.Background(), messages)
	if err != nil || len(summary) > DefaultExtractiveSummaryMaxBytes {
		t.Fatalf("summary bytes=%d err=%v", len(summary), err)
	}
	if strings.Contains(summary, "sha256=") || strings.Contains(summary, "earlier_user_history source_index=0") || !strings.Contains(summary, "omitted_earlier_users=2") {
		t.Fatalf("oversized earlier users were retained or hashed: %q", summary)
	}
}

func TestExtractiveSummarizerPrioritizesRecentGoalsAndToolPairsOverEarlierUsers(t *testing.T) {
	summarizer, err := NewExtractiveSummarizer(ExtractiveSummarizerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	messages := make([]core.ChatMessage, 0, 166)
	for index := 0; index < 160; index++ {
		messages = append(messages, core.ChatMessage{Role: core.RoleUser, Content: strings.Repeat("old history ", 96) + string(rune('a'+index)), SourceSeq: int64(index + 1)})
	}
	for index, value := range []string{"recent goal one", "recent goal two", "recent goal three", "recent goal four"} {
		messages = append(messages, core.ChatMessage{Role: core.RoleUser, Content: value, SourceSeq: int64(index + 161)})
	}
	messages = append(messages,
		core.ChatMessage{Role: core.RoleAssistant, ToolCall: &core.ToolCall{ID: "priority-pair", Name: "memory.recall"}, SourceSeq: 165},
		core.ChatMessage{Role: core.RoleTool, ToolCallID: "priority-pair", Content: "priority result", SourceSeq: 166},
	)
	summary, err := summarizer.Summarize(context.Background(), messages)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"recent_user_goal_or_constraint: recent goal four", "tool_call id=priority-pair name=memory.recall", "tool_result id=priority-pair result=priority result"} {
		if !strings.Contains(summary, want) {
			t.Fatalf("priority evidence missing %q: %q", want, summary)
		}
	}
	if !strings.Contains(summary, "omitted_earlier_users=") || strings.Contains(summary, "omitted_earlier_users=0") {
		t.Fatalf("test did not exhaust earlier-user budget: %q", summary)
	}
}

func TestExtractiveSummarizerNeverSplitsToolPairAtOutputBoundary(t *testing.T) {
	summarizer, err := NewExtractiveSummarizer(ExtractiveSummarizerConfig{MaxBytes: minimumExtractiveSummaryBytes, MaxMessageBytes: 256, MaxToolBytes: 256})
	if err != nil {
		t.Fatal(err)
	}
	messages := []core.ChatMessage{
		{Role: core.RoleUser, Content: strings.Repeat("goal ", 50)},
		{Role: core.RoleUser, Content: strings.Repeat("constraint ", 32)},
		{Role: core.RoleAssistant, ToolCall: &core.ToolCall{ID: "call-pair", Name: "rag.search"}},
		{Role: core.RoleTool, ToolCallID: "call-pair", Content: strings.Repeat("result ", 64)},
	}
	summary, err := summarizer.Summarize(context.Background(), messages)
	if err != nil {
		t.Fatal(err)
	}
	callPresent := strings.Contains(summary, "tool_call id=call-pair")
	resultPresent := strings.Contains(summary, "tool_result id=call-pair")
	if callPresent != resultPresent {
		t.Fatalf("tool pair split at summary boundary: %q", summary)
	}
}

func TestExtractiveSummarizerLargeHashDoesNotCopySourcePerCall(t *testing.T) {
	summarizer, err := NewExtractiveSummarizer(ExtractiveSummarizerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	messages := []core.ChatMessage{{Role: core.RoleTool, ToolCallID: "call-1", Content: strings.Repeat("x", 1<<20)}}
	allocs := testing.AllocsPerRun(5, func() {
		summary, runErr := summarizer.Summarize(context.Background(), messages)
		if runErr != nil || len(summary) > DefaultExtractiveSummaryMaxBytes {
			t.Fatalf("summary bytes=%d err=%v", len(summary), runErr)
		}
	})
	// This is a fixed-object allocation budget, not a source-size budget. A
	// regression that copies the 1 MiB input would be visible in the benchmark's
	// alloc_bytes/op report; the test guards against unbounded object churn.
	if allocs > 32 {
		t.Fatalf("large extractive summary allocs/run=%.1f, want <=32", allocs)
	}
}

func TestExtractiveSummaryFitsFinalModelContextBudget(t *testing.T) {
	summarizer, err := NewExtractiveSummarizer(ExtractiveSummarizerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	summary, err := summarizer.Summarize(context.Background(), []core.ChatMessage{
		{Role: core.RoleUser, Content: "goal"},
		{Role: core.RoleAssistant, ToolCall: &core.ToolCall{ID: "call-1", Name: "rag.search"}},
		{Role: core.RoleTool, ToolCallID: "call-1", Content: strings.Repeat("x", 1<<20)},
	})
	if err != nil {
		t.Fatal(err)
	}
	assembler, err := NewAssembler(Config{SafetyMarginTokens: 16})
	if err != nil {
		t.Fatal(err)
	}
	result, err := assembler.AssembleModelContext(context.Background(), core.ModelContext{
		ContextWindowTokens: 16 << 10, MaxOutputTokens: 256,
		Messages: []core.ChatMessage{{Role: core.RoleUser, Content: summary, Provenance: &core.ContextProvenance{Kind: "summary", Revision: "1"}}, {Role: core.RoleUser, Content: "current request"}},
	})
	if err != nil || result.InputTokens > int64((16<<10)-256-16) {
		t.Fatalf("assembled summary exceeds model input budget: result=%#v err=%v", result, err)
	}
}

// BenchmarkExtractiveSummarizerLargeToolResult is a reproducible default
// budget check: a 1 MiB source result must only allocate/write the fixed 12 KiB
// summary envelope plus a fixed 4 KiB hash buffer, never a transcript-sized
// copy. The alloc_bytes/op output is expected to remain below 64 KiB.
func BenchmarkExtractiveSummarizerLargeToolResult(b *testing.B) {
	summarizer, err := NewExtractiveSummarizer(ExtractiveSummarizerConfig{})
	if err != nil {
		b.Fatal(err)
	}
	messages := []core.ChatMessage{
		{Role: core.RoleAssistant, ToolCall: &core.ToolCall{ID: "call-1", Name: "rag.search"}},
		{Role: core.RoleTool, ToolCallID: "call-1", Content: strings.Repeat("x", 1<<20)},
	}
	b.ReportAllocs()
	b.ReportMetric(64<<10, "max_alloc_bytes/op")
	b.SetBytes(1 << 20)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		summary, err := summarizer.Summarize(context.Background(), messages)
		if err != nil || len(summary) > DefaultExtractiveSummaryMaxBytes {
			b.Fatalf("summary bytes=%d err=%v", len(summary), err)
		}
	}
}

// BenchmarkExtractiveSummarizerOversizedEarlierUsers measures the bounded
// rejection path. Fixture bytes describe the input, not bytes scanned: the
// oversized payloads are skipped. Compare ns/op and allocations for regressions.
func BenchmarkExtractiveSummarizerOversizedEarlierUsers(b *testing.B) {
	summarizer, err := NewExtractiveSummarizer(ExtractiveSummarizerConfig{})
	if err != nil {
		b.Fatal(err)
	}
	oversized := strings.Repeat("old-history ", 1<<17)
	messages := make([]core.ChatMessage, 0, 12)
	for index := 0; index < 8; index++ {
		messages = append(messages, core.ChatMessage{Role: core.RoleUser, Content: oversized})
	}
	for _, content := range []string{"recent one", "recent two", "recent three", "recent four"} {
		messages = append(messages, core.ChatMessage{Role: core.RoleUser, Content: content})
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.ReportMetric(float64(len(oversized)*8), "fixture_bytes")
	for index := 0; index < b.N; index++ {
		summary, runErr := summarizer.Summarize(context.Background(), messages)
		if runErr != nil || len(summary) > DefaultExtractiveSummaryMaxBytes {
			b.Fatalf("summary bytes=%d err=%v", len(summary), runErr)
		}
	}
}
