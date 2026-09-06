// Package corebridge adapts a protocol-neutral modelexecution Registry to the
// legacy core.LlmAdapter seam. It is intentionally outside pkg/app so the app
// contract remains independent of core's session-loop vocabulary.
package corebridge

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/cc-auto-agent/harness-core/pkg/app/modelcontrol"
	"github.com/cc-auto-agent/harness-core/pkg/app/modelexecution"
	"github.com/cc-auto-agent/harness-core/pkg/core"
)

// Adapter is a thin compatibility bridge; Registry and Plan are injected and
// immutable for its lifetime. It does not select models or resolve profiles.
type Adapter struct {
	Registry *modelexecution.Registry
	Plan     modelcontrol.ProviderPlan
}

func (a *Adapter) Provider() string {
	if a == nil {
		return ""
	}
	return a.Plan.Provider.Ref.ID
}
func (a *Adapter) ArtifactRevision() string {
	if a == nil {
		return ""
	}
	return a.Plan.SnapshotRevision + "/" + a.Plan.Provider.ImplementationRevision + "/" + a.Plan.Protocol.ImplementationRevision
}
func (a *Adapter) ModelContextLimits() (int, int) {
	if a == nil {
		return 0, 0
	}
	return a.Plan.Catalog.Capabilities.ContextWindowTokens, a.Plan.Catalog.Capabilities.MaxOutputTokens
}

func (a *Adapter) Stream(ctx context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	if a == nil || a.Registry == nil || emit == nil {
		return fmt.Errorf("model execution core bridge is incomplete")
	}
	request, err := requestFromCore(a.Plan, options)
	if err != nil {
		return err
	}
	state := newBridgeStream()
	err = a.Registry.Execute(ctx, request, func(event modelexecution.Event) error {
		switch event.Kind {
		case modelexecution.EventTextDelta:
			emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: event.Text})
		case modelexecution.EventToolCallDelta:
			if err := state.addToolDelta(*event.ToolCall); err != nil {
				return err
			}
		case modelexecution.EventUsage:
			state.usage = &core.TokenUsage{InputTokens: event.Usage.InputTokens, OutputTokens: event.Usage.OutputTokens}
		case modelexecution.EventFinish:
			calls, err := state.calls()
			if err != nil {
				return err
			}
			if len(calls) > 0 {
				first := calls[0]
				emit(core.StreamChunk{Kind: core.StreamKindAssistant, ToolCall: &first, ToolCalls: calls})
			}
			finish := core.FinishStop
			if event.Finish == modelexecution.FinishToolCalls {
				finish = core.FinishToolCalls
			}
			emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: finish, Usage: state.usage})
		}
		return nil
	})
	return err
}

func requestFromCore(plan modelcontrol.ProviderPlan, options core.GenerateOptions) (modelexecution.Request, error) {
	request := modelexecution.Request{Plan: plan, System: options.System, Messages: make([]modelexecution.Message, 0, len(options.Messages)), Tools: make([]modelexecution.Tool, 0, len(options.Tools))}
	for _, message := range options.Messages {
		m := modelexecution.Message{Role: string(message.Role), Content: message.Content, ToolCallID: message.ToolCallID}
		calls := message.ToolCalls
		if len(calls) == 0 && message.ToolCall != nil {
			calls = []core.ToolCall{*message.ToolCall}
		}
		for _, call := range calls {
			arguments := []byte("{}")
			var err error
			if call.Args != nil {
				arguments, err = json.Marshal(call.Args)
			}
			if err != nil {
				return modelexecution.Request{}, err
			}
			m.ToolCalls = append(m.ToolCalls, modelexecution.ToolCall{ID: call.ID, Name: call.Name, Arguments: arguments, Continuation: call.Continuation})
		}
		request.Messages = append(request.Messages, m)
	}
	for _, tool := range options.Tools {
		parameters := []byte("{}")
		var err error
		if tool.Parameters != nil {
			parameters, err = json.Marshal(tool.Parameters)
		}
		if err != nil {
			return modelexecution.Request{}, err
		}
		request.Tools = append(request.Tools, modelexecution.Tool{Name: tool.Name, Description: tool.Description, Parameters: parameters})
	}
	if len(request.Messages) == 0 {
		request.Messages = []modelexecution.Message{{Role: "user", Content: ""}}
	}
	return request, nil
}

type bridgeTool struct {
	id, name     string
	arguments    strings.Builder
	continuation string
}
type bridgeStream struct {
	tools map[int]*bridgeTool
	usage *core.TokenUsage
}

func newBridgeStream() *bridgeStream { return &bridgeStream{tools: make(map[int]*bridgeTool)} }
func (s *bridgeStream) addToolDelta(delta modelexecution.ToolCallDelta) error {
	tool := s.tools[delta.Index]
	if tool == nil {
		tool = &bridgeTool{}
		s.tools[delta.Index] = tool
	}
	if delta.ID != "" {
		tool.id = delta.ID
	}
	if delta.Name != "" {
		tool.name = delta.Name
	}
	if delta.Continuation != "" {
		if tool.continuation != "" && tool.continuation != delta.Continuation {
			return fmt.Errorf("model tool continuation changed within one call")
		}
		tool.continuation = delta.Continuation
	}
	_, err := tool.arguments.Write(delta.ArgumentsFragment)
	return err
}
func (s *bridgeStream) calls() ([]core.ToolCall, error) {
	indexes := make([]int, 0, len(s.tools))
	for index := range s.tools {
		indexes = append(indexes, index)
	}
	sort.Ints(indexes)
	calls := make([]core.ToolCall, 0, len(indexes))
	for position, index := range indexes {
		tool := s.tools[index]
		if tool.id == "" {
			tool.id = fmt.Sprintf("call-%d", position+1)
		}
		if tool.name == "" {
			return nil, fmt.Errorf("model tool call %d has no name", index)
		}
		var args map[string]any
		if raw := strings.TrimSpace(tool.arguments.String()); raw != "" && json.Unmarshal([]byte(raw), &args) != nil {
			return nil, fmt.Errorf("model tool call %d arguments are invalid JSON", index)
		}
		calls = append(calls, core.ToolCall{ID: tool.id, Name: tool.name, Args: args, Continuation: tool.continuation})
	}
	return calls, nil
}
