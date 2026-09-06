package contextassembly

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/cc-auto-agent/harness-core/pkg/core"
)

func TestAssemblerEstimatesEnglishCJKAndRequiredSystem(t *testing.T) {
	a, err := NewAssembler(Config{SafetyMarginTokens: 4})
	if err != nil {
		t.Fatal(err)
	}
	english, err := a.AssembleModelContext(context.Background(), core.ModelContext{System: "sys", ContextWindowTokens: 48, MaxOutputTokens: 8, Messages: []core.ChatMessage{{Role: core.RoleUser, Content: "abcdefghijkl"}}})
	if err != nil || english.InputTokens <= 0 {
		t.Fatalf("english=%#v err=%v", english, err)
	}
	cjk, err := a.AssembleModelContext(context.Background(), core.ModelContext{System: "系统", ContextWindowTokens: 48, MaxOutputTokens: 8, Messages: []core.ChatMessage{{Role: core.RoleUser, Content: "你好世界"}}})
	if err != nil || cjk.InputTokens <= 0 {
		t.Fatalf("cjk=%#v err=%v", cjk, err)
	}
	if _, err := a.AssembleModelContext(context.Background(), core.ModelContext{System: strings.Repeat("x", 64), ContextWindowTokens: 20, MaxOutputTokens: 8}); err == nil {
		t.Fatal("oversized required system accepted")
	}
}

func TestAssemblerNormalizesProjectedToolAliasWithoutLosingContinuation(t *testing.T) {
	call := core.ToolCall{ID: "projected-call", Name: "memory.remember", Args: map[string]any{"key": "color", "content": "blue"}, Continuation: "opaque-continuation"}
	messages := []core.ChatMessage{
		{Role: core.RoleUser, Content: "remember my color", SourceSeq: 1},
		{Role: core.RoleAssistant, ToolCall: &call, ToolCalls: []core.ToolCall{call}, SourceSeq: 2},
		{Role: core.RoleTool, ToolCallID: call.ID, Content: "stored", SourceSeq: 3},
	}
	a, err := NewAssembler(Config{})
	if err != nil {
		t.Fatal(err)
	}
	result, err := a.AssembleModelContext(context.Background(), core.ModelContext{ContextWindowTokens: 32768, MaxOutputTokens: 4096, Messages: messages})
	if err != nil {
		t.Fatalf("valid Session projection rejected: %v", err)
	}
	if len(result.Messages) != 3 || result.Messages[1].ToolCall != nil || len(result.Messages[1].ToolCalls) != 1 || result.Messages[1].ToolCalls[0].Continuation != call.Continuation || result.Messages[2].ToolCallID != call.ID {
		t.Fatalf("tool pairing or continuation changed: %#v", result.Messages)
	}
	if messages[1].ToolCall == nil || messages[1].ToolCalls[0].Args["content"] != "blue" {
		t.Fatal("assembly mutated durable input")
	}
}

func TestAssemblerRejectsConflictingProjectedToolAliases(t *testing.T) {
	for _, field := range []string{"id", "name", "args", "continuation"} {
		t.Run(field, func(t *testing.T) {
			alias := core.ToolCall{ID: "call", Name: "tool", Args: map[string]any{"n": 1}, Continuation: "original"}
			first := alias
			switch field {
			case "id":
				first.ID = "different"
			case "name":
				first.Name = "different"
			case "args":
				first.Args = map[string]any{"n": 2}
			case "continuation":
				first.Continuation = "different"
			}
			a, _ := NewAssembler(Config{})
			_, err := a.AssembleModelContext(context.Background(), core.ModelContext{ContextWindowTokens: 32768, MaxOutputTokens: 4096, Messages: []core.ChatMessage{{Role: core.RoleAssistant, ToolCall: &alias, ToolCalls: []core.ToolCall{first}}}})
			if !errors.Is(err, ErrInvalid) {
				t.Fatalf("conflicting %s alias accepted: %v", field, err)
			}
		})
	}
}

func TestAssemblerKeepsOuterWorkflowResultAndDurableAuditInput(t *testing.T) {
	call := core.ToolCall{ID: "flow", Name: "pipeline", Args: map[string]any{"n": 7}}
	messages := []core.ChatMessage{
		{Role: core.RoleUser, Content: "run pipeline", SourceSeq: 1},
		{Role: core.RoleAssistant, ToolCall: &call, ToolCalls: []core.ToolCall{call}, SourceSeq: 2},
		{Role: core.RoleTool, ToolCallID: "flow/double", Content: `{"value":14}`, SourceSeq: 4},
		{Role: core.RoleTool, ToolCallID: "flow/plus", Content: `{"value":17}`, SourceSeq: 6},
		{Role: core.RoleTool, ToolCallID: "flow", Content: `{"value":17}`, SourceSeq: 7},
	}
	a, err := NewAssembler(Config{})
	if err != nil {
		t.Fatal(err)
	}
	request := core.ModelContext{ContextWindowTokens: 32768, MaxOutputTokens: 4096, Messages: messages}
	result, err := a.AssembleModelContext(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Messages) != 3 || result.Messages[2].ToolCallID != "flow" || result.Messages[2].Content != `{"value":17}` {
		t.Fatalf("model context must contain only the outer response: %#v", result.Messages)
	}
	if messages[2].ToolCallID != "flow/double" || messages[2].Content != `{"value":14}` || messages[1].ToolCall == nil {
		t.Fatal("durable audit projection mutated")
	}
	request.Messages = messages[:4]
	if _, err := a.AssembleModelContext(context.Background(), request); !errors.Is(err, ErrInvalid) {
		t.Fatalf("missing outer result must fail closed: %v", err)
	}
	request.Messages = []core.ChatMessage{messages[0], messages[1], messages[4], messages[1], messages[2]}
	if _, err := a.AssembleModelContext(context.Background(), request); !errors.Is(err, ErrInvalid) {
		t.Fatalf("an earlier result with a reused call ID must not complete a later workflow: %v", err)
	}
}

func TestAssemblerPreservesExplicitModelCallWithSlashID(t *testing.T) {
	a, err := NewAssembler(Config{})
	if err != nil {
		t.Fatal(err)
	}
	request := core.ModelContext{ContextWindowTokens: 32768, MaxOutputTokens: 4096, Messages: []core.ChatMessage{
		{Role: core.RoleUser, Content: "call both"},
		{Role: core.RoleAssistant, ToolCalls: []core.ToolCall{{ID: "flow", Name: "first"}, {ID: "flow/step", Name: "second"}}},
		{Role: core.RoleTool, ToolCallID: "flow/step", Content: "second"},
		{Role: core.RoleTool, ToolCallID: "flow", Content: "first"},
	}}
	result, err := a.AssembleModelContext(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Messages) != 4 || result.Messages[2].ToolCallID != "flow/step" {
		t.Fatalf("explicitly requested tool result removed: %#v", result.Messages)
	}
}

func TestAssemblerPrioritizesLatestAndSummaryAndKeepsToolGroupsAtomic(t *testing.T) {
	a, err := NewAssembler(Config{SafetyMarginTokens: 4, MaxToolResultBytes: 16})
	if err != nil {
		t.Fatal(err)
	}
	messages := []core.ChatMessage{
		{Role: core.RoleUser, Content: strings.Repeat("old ", 40), SourceSeq: 1},
		{Role: core.RoleUser, Content: "durable summary", SourceSeq: 2, Provenance: &core.ContextProvenance{Kind: "summary", Revision: "2"}},
		{Role: core.RoleAssistant, Content: "tool call", SourceSeq: 3, ToolCalls: []core.ToolCall{{ID: "old-call", Name: "tool", Args: map[string]any{}}}},
		{Role: core.RoleTool, ToolCallID: "old-call", Content: strings.Repeat("secret-result", 50), SourceSeq: 4},
		{Role: core.RoleUser, Content: "latest request", SourceSeq: 5},
	}
	result, err := a.AssembleModelContext(context.Background(), core.ModelContext{ContextWindowTokens: 90, MaxOutputTokens: 12, Messages: messages})
	if err != nil {
		t.Fatal(err)
	}
	contents := make([]string, len(result.Messages))
	for i, m := range result.Messages {
		contents[i] = m.Content
	}
	if !containsContent(contents, "durable summary") || !containsContent(contents, "latest request") {
		t.Fatalf("summary/latest starved: %q", contents)
	}
	toolCall, toolResult := false, false
	for _, m := range result.Messages {
		if m.SourceSeq == 3 {
			toolCall = true
		}
		if m.SourceSeq == 4 {
			toolResult = true
			if strings.Contains(m.Content, "secret-result") {
				t.Fatalf("old tool result leaked: %q", m.Content)
			}
		}
	}
	if toolCall != toolResult {
		t.Fatalf("tool group split: %#v", result.Messages)
	}
	for i := 0; i < 10; i++ {
		again, err := a.AssembleModelContext(context.Background(), core.ModelContext{ContextWindowTokens: 90, MaxOutputTokens: 12, Messages: messages})
		if err != nil || strings.Join(messageContents(again.Messages), "|") != strings.Join(contents, "|") {
			t.Fatalf("unstable assembly=%#v err=%v", again, err)
		}
	}
}

func TestAssemblerMakesCurrentTurnRequiredAndSummaryOutranksOlderExactHistory(t *testing.T) {
	a, err := NewAssembler(Config{SafetyMarginTokens: 4})
	if err != nil {
		t.Fatal(err)
	}
	messages := []core.ChatMessage{
		{Role: core.RoleUser, Content: strings.Repeat("old ", 16), SourceSeq: 1},
		{Role: core.RoleUser, Content: "summary anchor", SourceSeq: 2, Provenance: &core.ContextProvenance{Kind: "summary", Revision: "2"}},
		{Role: core.RoleUser, Content: "current", SourceSeq: 3},
	}
	result, err := a.AssembleModelContext(context.Background(), core.ModelContext{ContextWindowTokens: 56, MaxOutputTokens: 12, Messages: messages})
	if err != nil {
		t.Fatal(err)
	}
	contents := messageContents(result.Messages)
	if !containsContent(contents, "summary anchor") || !containsContent(contents, "current") || containsContent(contents, messages[0].Content) {
		t.Fatalf("priority result=%q", contents)
	}
	_, err = a.AssembleModelContext(context.Background(), core.ModelContext{ContextWindowTokens: 20, MaxOutputTokens: 12, Messages: []core.ChatMessage{{Role: core.RoleUser, Content: strings.Repeat("must fit ", 30)}}})
	if !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("required current turn error=%v", err)
	}
}

func TestUTF8ByteUpperBoundAllowsMultilineAndCountsToolArguments(t *testing.T) {
	if _, err := utf8ByteUpperBoundTokens("line one\nline two\tand carriage\rreturn"); err != nil {
		t.Fatalf("ordinary multiline text rejected: %v", err)
	}
	if _, err := utf8ByteUpperBoundTokens("bad\x00text"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("NUL accepted: %v", err)
	}
	message := core.ChatMessage{Role: core.RoleAssistant, Content: "x", ToolCalls: []core.ToolCall{{ID: "call-cjk", Name: "工具", Args: map[string]any{"参数": strings.Repeat("界", 128)}}}}
	cost, err := (ConservativeEstimator{}).Estimate(message)
	if err != nil || cost.Tokens <= 100 {
		t.Fatalf("tool args missing from estimate: cost=%+v err=%v", cost, err)
	}
	a, _ := NewAssembler(Config{SafetyMarginTokens: 4})
	_, err = a.AssembleModelContext(context.Background(), core.ModelContext{ContextWindowTokens: 48, MaxOutputTokens: 12, Messages: []core.ChatMessage{{Role: core.RoleUser, Content: "current"}, message}})
	if !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("large required CJK args accepted: %v", err)
	}
}

func TestUTF8ByteUpperBoundCoversASCIIEmojiAndCJKBudgetEdges(t *testing.T) {
	for name, text := range map[string]string{
		"ascii code":         "for(i=0;i<16;i++){x+=a[i];}// []{}()!@#$%^&*-_=+",
		"random punctuation": "~`!@#$%^&*()-_=+[]{}|;:',.<>/?\\\"",
		"emoji":              "🧪🚀🙂",
		"cjk":                "上下文预算边界测试",
	} {
		t.Run(name, func(t *testing.T) {
			got, err := utf8ByteUpperBoundTokens(text)
			if err != nil || got != int64(len(text)) {
				t.Fatalf("tokens=%d bytes=%d err=%v", got, len(text), err)
			}
		})
	}
	text := "func main(){println(\"🧪上下文\")}//!"
	a, err := NewAssembler(Config{SafetyMarginTokens: 1})
	if err != nil {
		t.Fatal(err)
	}
	// The message estimator adds four structural tokens, so this is the exact
	// input-token boundary for a one-message request without a system prompt.
	contextWindowTokens, maxOutputTokens := len(text)+4+8+1, 8
	if _, err := a.AssembleModelContext(context.Background(), core.ModelContext{ContextWindowTokens: contextWindowTokens, MaxOutputTokens: maxOutputTokens, Messages: []core.ChatMessage{{Role: core.RoleUser, Content: text}}}); err != nil {
		t.Fatalf("exact byte upper-bound rejected: %v", err)
	}
	contextWindowTokens--
	if _, err := a.AssembleModelContext(context.Background(), core.ModelContext{ContextWindowTokens: contextWindowTokens, MaxOutputTokens: maxOutputTokens, Messages: []core.ChatMessage{{Role: core.RoleUser, Content: text}}}); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("one-token-over budget accepted: %v", err)
	}
}

func TestAssemblerCancellationAndInvalidConfigFailClosed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	a, err := NewAssembler(Config{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.AssembleModelContext(ctx, core.ModelContext{ContextWindowTokens: 100, MaxOutputTokens: 10, Messages: []core.ChatMessage{{Role: core.RoleUser, Content: "x"}}}); err == nil {
		t.Fatal("cancelled assembly accepted")
	}
	if _, err := NewAssembler(Config{SafetyMarginTokens: -1}); err == nil {
		t.Fatal("invalid config accepted")
	}
}

func containsContent(values []string, want string) bool {
	for _, v := range values {
		if v == want {
			return true
		}
	}
	return false
}
func messageContents(messages []core.ChatMessage) []string {
	values := make([]string, len(messages))
	for i, message := range messages {
		values[i] = message.Content
	}
	return values
}

func BenchmarkAssemblerLongHistory(b *testing.B) {
	a, _ := NewAssembler(Config{})
	messages := benchmarkHistory(1024)
	request := core.ModelContext{ContextWindowTokens: 32768, MaxOutputTokens: 4096, Messages: messages}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := a.AssembleModelContext(context.Background(), request); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAssemblerDefaultHistory(b *testing.B) {
	a, _ := NewAssembler(Config{})
	request := core.ModelContext{ContextWindowTokens: 32768, MaxOutputTokens: 4096, Messages: benchmarkHistory(120)}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := a.AssembleModelContext(context.Background(), request); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkHistory(count int) []core.ChatMessage {
	messages := make([]core.ChatMessage, 0, count)
	for i := 0; i < count; i++ {
		messages = append(messages, core.ChatMessage{Role: core.RoleUser, Content: strings.Repeat("long history ", 20), SourceSeq: int64(i + 1)})
	}
	return messages
}
