package core

import (
	"context"
	"fmt"
	"strconv"
	"time"
)

// callModel owns model-call identity construction and the optional trust gate.
// Keeping this orchestration separate makes the turn loop focus on state and
// tool sequencing while preserving one exact request per model step.
func (a *Agent) callModel(ctx context.Context, info RunInfo, step int, messages []ChatMessage) (modelStreamResult, string, error) {
	var stream modelStreamResult
	contextWindowTokens, maxOutputTokens := modelContextLimits(a.opts.LLM)
	assembled, err := safeAssembleModelContext(a.opts.ContextAssembler, ctx, ModelContext{
		System: a.opts.System, Messages: messages, ContextWindowTokens: contextWindowTokens, MaxOutputTokens: maxOutputTokens,
	})
	if err != nil {
		return stream, "model_context_failed", err
	}
	messages = assembled.Messages
	contextEvidence := TelemetryAttributes{
		"model.context.input_bytes":    strconv.FormatInt(assembled.InputBytes, 10),
		"model.context.input_tokens":   strconv.FormatInt(assembled.InputTokens, 10),
		"model.context.dropped_groups": strconv.Itoa(assembled.DroppedGroups),
	}
	modelCtx, modelSpan := StartTelemetry(a.opts.Telemetry, ctx, SpanModelCall, TelemetryAttributes{
		"run.id": info.RunID, "session.id": info.SessionID, "profile.id": info.ProfileID,
		"model.provider": a.opts.Provider, "model.name": a.opts.Model,
		"run.step":                  fmt.Sprintf("%d", step),
		"model.context.input_bytes": contextEvidence["model.context.input_bytes"], "model.context.input_tokens": contextEvidence["model.context.input_tokens"],
		"model.context.dropped_groups": contextEvidence["model.context.dropped_groups"],
	})
	modelStarted := time.Now()
	modelRequest := ModelCallRequest{
		Principal: info.Principal, Scope: a.opts.Session.Scope(), SessionID: info.SessionID,
		RunID: info.RunID, Step: step, Provider: a.opts.Provider, Model: a.opts.Model,
	}
	if deadline, ok := ctx.Deadline(); ok {
		modelRequest.Deadline = deadline
	}
	acceptedCall := AcceptedModelCall{}
	if err := validateModelCallPrincipalClaims(modelRequest.Principal); err != nil {
		modelSpan.End(err, modelCallEndAttributes(contextEvidence, "invalid_request", ""))
		return stream, "model_gate_rejected", err
	}
	if a.opts.ModelCallGate != nil {
		if err := modelRequest.Validate(); err != nil {
			modelSpan.End(err, modelCallEndAttributes(contextEvidence, "invalid_request", ""))
			return stream, "model_gate_rejected", err
		}
		if err := safeAuthorizeModelCall(a.opts.ModelCallGate, modelCtx, modelRequest); err != nil {
			modelSpan.End(err, modelCallEndAttributes(contextEvidence, "gate_rejected", ""))
			return stream, "model_gate_rejected", err
		}
		acceptedCall = mintAcceptedModelCall(modelRequest)
	}
	stream, err = consumeModelStream(modelCtx, a.opts.LLM, GenerateOptions{
		Provider: a.opts.Provider, Model: a.opts.Model, System: assembled.System,
		Messages: messages, Tools: a.tools.Schemas(), ModelCall: modelRequest,
		AcceptedCall: acceptedCall,
	}, func(chunk StreamChunk) error {
		if a.opts.StreamChunks && chunk.Text != "" {
			return a.append(info.RunID, EvAssistantChunk, AssistantChunkData{Text: chunk.Text})
		}
		return nil
	})
	modelOutcome := "ok"
	if err != nil {
		modelOutcome = "error"
	}
	modelSpan.End(err, modelCallEndAttributes(contextEvidence, modelOutcome, string(stream.FinishKind)))
	modelMetricAttrs := TelemetryAttributes{
		"model.provider": a.opts.Provider, "model.name": a.opts.Model, "model.outcome": modelOutcome,
	}
	AddTelemetryCounter(a.opts.Telemetry, modelCtx, MetricModelCalls, 1, modelMetricAttrs)
	RecordTelemetryHistogram(a.opts.Telemetry, modelCtx, MetricModelDuration, time.Since(modelStarted).Seconds(), "s", modelMetricAttrs)
	AddTelemetryCounter(a.opts.Telemetry, modelCtx, MetricModelContextInputBytes, assembled.InputBytes, modelMetricAttrs)
	AddTelemetryCounter(a.opts.Telemetry, modelCtx, MetricModelContextInputTokens, assembled.InputTokens, modelMetricAttrs)
	AddTelemetryCounter(a.opts.Telemetry, modelCtx, MetricModelContextDroppedGroups, int64(assembled.DroppedGroups), modelMetricAttrs)
	if err != nil {
		return stream, "model_failed", err
	}
	return stream, "", nil
}

func modelCallEndAttributes(contextEvidence TelemetryAttributes, outcome, finish string) TelemetryAttributes {
	attributes := TelemetryAttributes{
		"model.outcome":             outcome,
		"model.context.input_bytes": contextEvidence["model.context.input_bytes"], "model.context.input_tokens": contextEvidence["model.context.input_tokens"],
		"model.context.dropped_groups": contextEvidence["model.context.dropped_groups"],
	}
	if finish != "" {
		attributes["model.finish"] = finish
	}
	return attributes
}
