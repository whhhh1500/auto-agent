package core

import (
	"context"
	"errors"
	"fmt"
)

const (
	conservativeContextWindowTokens = 32_768
	conservativeMaxOutputTokens     = 4_096
)

var errModelContextAssembly = errors.New("model context assembly failed")

// ModelContext is the narrow kernel value for one final context selection.
// The app layer owns richer budgeting policies and DTOs. Before assembly it
// carries the input and limits; after assembly it also carries telemetry.
type ModelContext struct {
	System   string
	Messages []ChatMessage
	// Tools is the final exposed schema snapshot, supplied for budgeting only.
	// An assembler may inspect its private copy; it cannot change the tools
	// sent to the model by changing this field in its result.
	Tools               []ToolSchema
	ContextWindowTokens int
	MaxOutputTokens     int
	InputBytes          int64
	InputTokens         int64
	DroppedGroups       int
}

// ModelContextAssembler selects a bounded, ephemeral model context. The
// function must return a private message slice and must not retain input data.
type ModelContextAssembler func(context.Context, ModelContext) (ModelContext, error)

type modelContextLimitsReporter interface {
	ModelContextLimits() (contextWindowTokens, maxOutputTokens int)
}

func modelContextLimits(adapter LlmAdapter) (contextWindowTokens, maxOutputTokens int) {
	if reporter, ok := adapter.(modelContextLimitsReporter); ok {
		window, output := reporter.ModelContextLimits()
		if window > 0 && output > 0 && output < window {
			return window, output
		}
	}
	return conservativeContextWindowTokens, conservativeMaxOutputTokens
}

func safeAssembleModelContext(assembler ModelContextAssembler, ctx context.Context, request ModelContext) (result ModelContext, err error) {
	if request.ContextWindowTokens <= 0 || request.MaxOutputTokens <= 0 || request.MaxOutputTokens >= request.ContextWindowTokens || len(request.System) > MaxSessionEventDataBytes {
		return ModelContext{}, fmt.Errorf("%w: invalid request", errModelContextAssembly)
	}
	if ctx == nil {
		return ModelContext{}, fmt.Errorf("%w: nil context", errModelContextAssembly)
	}
	if err := ctx.Err(); err != nil {
		return ModelContext{}, fmt.Errorf("%w: %w", errModelContextAssembly, err)
	}
	if assembler == nil {
		return ModelContext{System: request.System, Messages: cloneChatMessages(request.Messages), ContextWindowTokens: request.ContextWindowTokens, MaxOutputTokens: request.MaxOutputTokens}, nil
	}
	defer func() {
		if recover() != nil {
			result = ModelContext{}
			err = fmt.Errorf("%w: assembler panic", errModelContextAssembly)
		}
	}()
	result, err = assembler(ctx, ModelContext{
		System: request.System, Messages: cloneChatMessages(request.Messages),
		Tools:               cloneToolSchemas(request.Tools),
		ContextWindowTokens: request.ContextWindowTokens, MaxOutputTokens: request.MaxOutputTokens,
	})
	if err != nil {
		return ModelContext{}, fmt.Errorf("%w: %w", errModelContextAssembly, err)
	}
	if result.System != request.System || result.ContextWindowTokens != request.ContextWindowTokens || result.MaxOutputTokens != request.MaxOutputTokens || result.Messages == nil || result.InputBytes < 0 || result.InputBytes > MaxSessionEventDataBytes || result.InputTokens < 0 || result.InputTokens > int64(MaxSessionEventDataBytes) || result.DroppedGroups < 0 || result.DroppedGroups > len(request.Messages) {
		return ModelContext{}, fmt.Errorf("%w: invalid result", errModelContextAssembly)
	}
	result.Messages = cloneChatMessages(result.Messages)
	result.Tools = nil // input-only; tool authority remains with the caller
	return result, nil
}
