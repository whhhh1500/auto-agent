package contextassembly

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/whhhh1500/auto-agent/pkg/core"
)

func BenchmarkAssemblerToolHistory(b *testing.B) {
	a, err := NewAssembler(Config{})
	if err != nil {
		b.Fatal(err)
	}
	messages := make([]core.ChatMessage, 0, 121)
	for i := 0; i < 40; i++ {
		call := core.ToolCall{ID: fmt.Sprintf("call-%d", i), Name: "documents.search", Args: map[string]any{"query": "deployment evidence", "filters": map[string]any{"tags": []any{"approved", "published"}, "limit": float64(8)}}, Continuation: "bounded-protocol-state"}
		messages = append(messages,
			core.ChatMessage{Role: core.RoleUser, Content: "Find deployment evidence and verify its source", SourceSeq: int64(i*3 + 1)},
			core.ChatMessage{Role: core.RoleAssistant, ToolCalls: []core.ToolCall{call}, SourceSeq: int64(i*3 + 2)},
			core.ChatMessage{Role: core.RoleTool, ToolCallID: call.ID, Content: strings.Repeat("verified document excerpt ", 32), SourceSeq: int64(i*3 + 3)},
		)
	}
	messages = append(messages, core.ChatMessage{Role: core.RoleUser, Content: "Summarize the available evidence", SourceSeq: 121})
	request := core.ModelContext{ContextWindowTokens: 32768, MaxOutputTokens: 4096, Messages: messages}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := a.AssembleModelContext(context.Background(), request); err != nil {
			b.Fatal(err)
		}
	}
}
