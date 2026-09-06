package corebridge

import (
	"context"
	"testing"

	"github.com/cc-auto-agent/harness-core/pkg/app/modelcontrol"
	"github.com/cc-auto-agent/harness-core/pkg/app/modelexecution"
	"github.com/cc-auto-agent/harness-core/pkg/core"
)

func TestAdapterCollapsesToolDeltasAtCoreBoundary(t *testing.T) {
	providerRef, protocolRef := modelcontrol.Ref{ID: "provider", Version: "1"}, modelcontrol.Ref{ID: "protocol", Version: "1"}
	plan := modelcontrol.ProviderPlan{Catalog: modelcontrol.CatalogModel{WireModel: "wire", Provider: providerRef, Protocol: protocolRef}, Provider: modelcontrol.ImplementationBinding{Ref: providerRef, ImplementationRevision: "provider-impl"}, Endpoint: modelcontrol.EndpointRef{ID: "endpoint", Revision: "1"}, Protocol: modelcontrol.ImplementationBinding{Ref: protocolRef, ImplementationRevision: "protocol-impl"}, SnapshotRevision: "snapshot"}
	registry, err := modelexecution.NewRegistry(plan.SnapshotRevision, []modelcontrol.ProviderPlan{plan}, []modelexecution.ProviderRegistration{{Binding: plan.Provider, Factory: func() (modelexecution.Provider, error) { return provider{}, nil }}}, []modelexecution.ProtocolRegistration{{Binding: plan.Protocol, Factory: func() (modelexecution.Protocol, error) { return protocol{}, nil }}})
	if err != nil {
		t.Fatal(err)
	}
	adapter := Adapter{Registry: registry, Plan: plan}
	var chunks []core.StreamChunk
	if err := adapter.Stream(context.Background(), core.GenerateOptions{Messages: []core.ChatMessage{{Role: core.RoleUser, Content: "hi"}}}, func(chunk core.StreamChunk) { chunks = append(chunks, chunk) }); err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 2 || chunks[0].ToolCall == nil || chunks[0].ToolCall.Name != "weather" || chunks[0].ToolCall.Args["city"] != "Paris" || chunks[1].FinishKind != core.FinishToolCalls {
		t.Fatalf("chunks=%#v", chunks)
	}
}

func TestAdapterReportsCatalogContextLimits(t *testing.T) {
	adapter := Adapter{Plan: modelcontrol.ProviderPlan{Catalog: modelcontrol.CatalogModel{Capabilities: modelcontrol.ModelCapabilities{
		ContextWindowTokens: 128_000,
		MaxOutputTokens:     8_192,
	}}}}
	gotWindow, gotOutput := adapter.ModelContextLimits()
	if gotWindow != 128_000 || gotOutput != 8_192 {
		t.Fatalf("limits=(%d,%d) want=(%d,%d)", gotWindow, gotOutput, 128_000, 8_192)
	}
}

func TestContinuationSurvivesCoreBridge(t *testing.T) {
	state := newBridgeStream()
	for _, delta := range []modelexecution.ToolCallDelta{
		{Index: 0, ID: "call-a", Name: "test.lookup", ArgumentsFragment: []byte(`{}`)},
		{Index: 0, Continuation: "opaque-state"},
	} {
		if err := state.addToolDelta(delta); err != nil {
			t.Fatal(err)
		}
	}
	calls, err := state.calls()
	if err != nil || len(calls) != 1 || calls[0].Continuation != "opaque-state" {
		t.Fatal("inbound continuation lost")
	}
	for _, message := range []core.ChatMessage{
		{Role: core.RoleAssistant, ToolCalls: calls},
		{Role: core.RoleAssistant, ToolCall: &calls[0]},
	} {
		request, err := requestFromCore(modelcontrol.ProviderPlan{}, core.GenerateOptions{Messages: []core.ChatMessage{message}})
		if err != nil || request.Messages[0].ToolCalls[0].Continuation != "opaque-state" {
			t.Fatal("outbound continuation lost")
		}
	}
}

type provider struct{}

func (provider) Send(context.Context, modelcontrol.ProviderPlan, modelexecution.OutboundRequest) (modelexecution.InboundResponse, error) {
	return modelexecution.InboundResponse{}, nil
}

type protocol struct{}

func (protocol) Execute(_ context.Context, _ modelexecution.Request, _ modelexecution.Provider, emit modelexecution.Emit) error {
	if err := emit(modelexecution.Event{Kind: modelexecution.EventToolCallDelta, ToolCall: &modelexecution.ToolCallDelta{Index: 0, ID: "call", Name: "weather", ArgumentsFragment: []byte(`{"city":"`)}}); err != nil {
		return err
	}
	if err := emit(modelexecution.Event{Kind: modelexecution.EventToolCallDelta, ToolCall: &modelexecution.ToolCallDelta{Index: 0, ArgumentsFragment: []byte(`Paris"}`)}}); err != nil {
		return err
	}
	return emit(modelexecution.Event{Kind: modelexecution.EventFinish, Finish: modelexecution.FinishToolCalls})
}
