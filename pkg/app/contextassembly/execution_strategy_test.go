package contextassembly

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/whhhh1500/auto-agent/pkg/core"
)

func TestProgramContextKeepsOnlyParentOutcomeAfterApprovalResume(t *testing.T) {
	const parentID = "program-17"
	childOne := strings.Repeat("nested-child-one-", 1000)
	childTwo := strings.Repeat("nested-child-two-", 1000)
	request := core.ModelContext{
		ContextWindowTokens: 32768,
		MaxOutputTokens:     4096,
		Messages: []core.ChatMessage{
			{Role: core.RoleUser, Content: "process the batch", SourceSeq: 1},
			{Role: core.RoleAssistant, ToolCalls: []core.ToolCall{{ID: parentID, Name: "program.execute", Args: map[string]any{"program": "batch"}}}, SourceSeq: 2},
			// These are the protected nested results recorded before an approval
			// pause and after the approved retry. Approval events themselves are
			// intentionally not model-visible.
			{Role: core.RoleTool, ToolCallID: parentID + "/read", Content: childOne, SourceSeq: 4},
			{Role: core.RoleTool, ToolCallID: parentID + "/write", Content: childTwo, SourceSeq: 8},
			{Role: core.RoleTool, ToolCallID: parentID, Content: `{"status":"completed","processed":2}`, SourceSeq: 9},
		},
	}

	assembler, err := NewAssembler(Config{})
	if err != nil {
		t.Fatal(err)
	}
	assembled, err := assembler.AssembleModelContext(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if containsMessage(assembled.Messages, childOne) || containsMessage(assembled.Messages, childTwo) {
		t.Fatalf("nested program results leaked into final model context: %#v", assembled.Messages)
	}
	if !containsMessage(assembled.Messages, `{"status":"completed","processed":2}`) || !containsToolCall(assembled.Messages, parentID, "program.execute") {
		t.Fatalf("parent program pair was not retained: %#v", assembled.Messages)
	}
	if assembled.InputTokens >= int64(len(childOne)+len(childTwo)) {
		t.Fatalf("nested payloads were charged to final model context: %d", assembled.InputTokens)
	}

	summarizer, err := NewExtractiveSummarizer(ExtractiveSummarizerConfig{MaxBytes: 4096, MaxMessageBytes: 512, MaxToolBytes: 512})
	if err != nil {
		t.Fatal(err)
	}
	summary, err := summarizer.Summarize(context.Background(), request.Messages)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(summary, childOne[:64]) || strings.Contains(summary, childTwo[:64]) {
		t.Fatalf("nested program results leaked into summary: %q", summary)
	}
	if !strings.Contains(summary, "name=program.execute") || !strings.Contains(summary, `{"status":"completed","processed":2}`) {
		t.Fatalf("parent program outcome was not summarized: %q", summary)
	}
}

func TestProgramContextRejectsOrphanNestedResult(t *testing.T) {
	request := core.ModelContext{
		ContextWindowTokens: 32768,
		MaxOutputTokens:     4096,
		Messages: []core.ChatMessage{
			{Role: core.RoleUser, Content: "process the batch", SourceSeq: 1},
			{Role: core.RoleAssistant, ToolCalls: []core.ToolCall{{ID: "program-17", Name: "program.execute"}}, SourceSeq: 2},
			{Role: core.RoleTool, ToolCallID: "program-17/write", Content: "untrusted nested result", SourceSeq: 3},
		},
	}
	assembler, err := NewAssembler(Config{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := assembler.AssembleModelContext(context.Background(), request); !errors.Is(err, ErrInvalid) {
		t.Fatalf("orphan nested result accepted by assembler: %v", err)
	}
	summarizer, err := NewExtractiveSummarizer(ExtractiveSummarizerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := summarizer.Summarize(context.Background(), request.Messages); !errors.Is(err, ErrInvalid) {
		t.Fatalf("orphan nested result accepted by summarizer: %v", err)
	}
}

func TestProgramContextRetainsDirectToolCallWithSlashID(t *testing.T) {
	request := core.ModelContext{
		ContextWindowTokens: 32768,
		MaxOutputTokens:     4096,
		Messages: []core.ChatMessage{
			{Role: core.RoleUser, Content: "check the record", SourceSeq: 1},
			{Role: core.RoleAssistant, ToolCalls: []core.ToolCall{{ID: "direct/with-slash", Name: "records.get"}}, SourceSeq: 2},
			{Role: core.RoleTool, ToolCallID: "direct/with-slash", Content: "record result", SourceSeq: 3},
		},
	}
	assembler, err := NewAssembler(Config{})
	if err != nil {
		t.Fatal(err)
	}
	assembled, err := assembler.AssembleModelContext(context.Background(), request)
	if err != nil || !containsToolCall(assembled.Messages, "direct/with-slash", "records.get") || !containsMessage(assembled.Messages, "record result") {
		t.Fatalf("direct slash-ID pair was rejected or changed: result=%#v err=%v", assembled, err)
	}
	summarizer, err := NewExtractiveSummarizer(ExtractiveSummarizerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	summary, err := summarizer.Summarize(context.Background(), request.Messages)
	if err != nil || !strings.Contains(summary, "id=direct/with-slash") || !strings.Contains(summary, "record result") {
		t.Fatalf("direct slash-ID pair was not summarized: %q err=%v", summary, err)
	}
}

func containsMessage(messages []core.ChatMessage, content string) bool {
	for _, message := range messages {
		if message.Content == content {
			return true
		}
	}
	return false
}

func containsToolCall(messages []core.ChatMessage, id, name string) bool {
	for _, message := range messages {
		for _, call := range message.ToolCalls {
			if call.ID == id && call.Name == name {
				return true
			}
		}
	}
	return false
}
