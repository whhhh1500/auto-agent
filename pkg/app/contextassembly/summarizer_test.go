package contextassembly

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/cc-auto-agent/harness-core/pkg/core"
)

type summaryAdapterFunc func(context.Context, core.GenerateOptions, func(core.StreamChunk)) error

func (summaryAdapterFunc) Provider() string { return "summary-test" }
func (f summaryAdapterFunc) Stream(ctx context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	return f(ctx, options, emit)
}

func TestLlmSummarizerBoundsInputAndSanitizesFailures(t *testing.T) {
	for name, adapter := range map[string]core.LlmAdapter{
		"error": summaryAdapterFunc(func(context.Context, core.GenerateOptions, func(core.StreamChunk)) error {
			return errors.New("TOP-SECRET adapter error")
		}),
		"panic": summaryAdapterFunc(func(context.Context, core.GenerateOptions, func(core.StreamChunk)) error {
			panic("TOP-SECRET adapter panic")
		}),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := (LlmSummarizer{Adapter: adapter}).Summarize(context.Background(), []core.ChatMessage{{Role: core.RoleUser, Content: "history"}})
			if err == nil || strings.Contains(err.Error(), "TOP-SECRET") {
				t.Fatalf("adapter failure leaked: %v", err)
			}
		})
	}

	var transcript string
	summary, err := (LlmSummarizer{Adapter: summaryAdapterFunc(func(_ context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
		transcript = options.Messages[0].Content
		emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: "bounded summary"})
		emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop})
		return nil
	}), MaxInputMessages: 1, MaxInputBytes: 512}).Summarize(context.Background(), []core.ChatMessage{
		{Role: core.RoleUser, Content: strings.Repeat("first ", 100)},
		{Role: core.RoleUser, Content: strings.Repeat("second ", 100)},
	})
	if err != nil || summary != "bounded summary" || len(transcript) > 512 || !strings.Contains(transcript, "omitted") {
		t.Fatalf("input bound failed: summary=%q transcript=%q err=%v", summary, transcript, err)
	}
}

func TestLlmSummarizerRejectsToolCallsAndProtocolViolations(t *testing.T) {
	cancelled := make(chan struct{})
	toolCall := summaryAdapterFunc(func(ctx context.Context, _ core.GenerateOptions, emit func(core.StreamChunk)) error {
		emit(core.StreamChunk{Kind: core.StreamKindAssistant, ToolCall: &core.ToolCall{ID: "call-1", Name: "rag.search"}})
		<-ctx.Done()
		close(cancelled)
		return ctx.Err()
	})
	if _, err := (LlmSummarizer{Adapter: toolCall}).Summarize(context.Background(), []core.ChatMessage{{Role: core.RoleUser, Content: "history"}}); err == nil {
		t.Fatal("summary tool call was accepted")
	}
	select {
	case <-cancelled:
	default:
		t.Fatal("summary output limiter did not cancel the upstream adapter")
	}

	noFinish := summaryAdapterFunc(func(_ context.Context, _ core.GenerateOptions, emit func(core.StreamChunk)) error {
		emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: "partial"})
		return nil
	})
	if _, err := (LlmSummarizer{Adapter: noFinish}).Summarize(context.Background(), []core.ChatMessage{{Role: core.RoleUser, Content: "history"}}); err == nil {
		t.Fatal("stream without finish was accepted")
	}
}
