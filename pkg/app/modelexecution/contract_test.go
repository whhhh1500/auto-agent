package modelexecution

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/cc-auto-agent/harness-core/pkg/app/modelcontrol"
)

func TestStreamValidatorPreservesToolDeltasAndRejectsInvalidOrder(t *testing.T) {
	v := NewStreamValidator()
	fragment := []byte(`{"city":`)
	event, err := v.Accept(Event{Kind: EventToolCallDelta, ToolCall: &ToolCallDelta{Index: 2, ID: "call-1", Name: "weather", ArgumentsFragment: fragment}})
	if err != nil || string(event.ToolCall.ArgumentsFragment) != string(fragment) {
		t.Fatalf("event=%#v err=%v", event, err)
	}
	fragment[0] = 'x'
	if string(event.ToolCall.ArgumentsFragment) != `{"city":` {
		t.Fatalf("event aliases input: %q", event.ToolCall.ArgumentsFragment)
	}
	if _, err := v.Accept(Event{Kind: EventUsage, Usage: &Usage{InputTokens: 1, OutputTokens: 2}}); err != nil {
		t.Fatal(err)
	}
	if _, err := v.Accept(Event{Kind: EventFinish, Finish: FinishToolCalls}); err != nil {
		t.Fatal(err)
	}
	if _, err := v.Accept(Event{Kind: EventTextDelta, Text: "late"}); !errors.Is(err, ErrStreamState) {
		t.Fatalf("late=%v", err)
	}
}

func TestRegistryExactBindingAndPanicContainment(t *testing.T) {
	request := testRequest()
	provider := ProviderRegistration{Binding: request.Plan.Provider, Factory: func() (Provider, error) { return testProvider{}, nil }}
	protocol := ProtocolRegistration{Binding: request.Plan.Protocol, Factory: func() (Protocol, error) { return testProtocol{}, nil }}
	r, err := NewRegistry(request.Plan.SnapshotRevision, []modelcontrol.ProviderPlan{request.Plan}, []ProviderRegistration{provider}, []ProtocolRegistration{protocol})
	if err != nil {
		t.Fatal(err)
	}
	var events []Event
	if err := r.Execute(context.Background(), request, func(event Event) error { events = append(events, event); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[0].Kind != EventTextDelta || events[1].Kind != EventFinish {
		t.Fatalf("events=%#v", events)
	}
	drift := request
	drift.Plan.Provider.ImplementationRevision = "other"
	if err := r.Execute(context.Background(), drift, func(Event) error { return nil }); !errors.Is(err, ErrBindingDrift) {
		t.Fatalf("drift=%v", err)
	}
	drift = request
	drift.Plan.Catalog.WireModel = "forged"
	if err := r.Execute(context.Background(), drift, func(Event) error { return nil }); !errors.Is(err, ErrBindingDrift) {
		t.Fatalf("plan mix=%v", err)
	}
	provider.Factory = func() (Provider, error) { panic(secretPanic{}) }
	r, err = NewRegistry(request.Plan.SnapshotRevision, []modelcontrol.ProviderPlan{request.Plan}, []ProviderRegistration{provider}, []ProtocolRegistration{protocol})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Execute(context.Background(), request, func(Event) error { return nil }); !errors.Is(err, ErrFactoryPanic) {
		t.Fatalf("panic=%v", err)
	}
}

func TestRegistryRejectsUnclosedRegisteredPlan(t *testing.T) {
	request := testRequest()
	request.Plan.Catalog.Protocol = modelcontrol.Ref{ID: "other", Version: "1"}
	_, err := NewRegistry(request.Plan.SnapshotRevision, []modelcontrol.ProviderPlan{request.Plan}, []ProviderRegistration{{Binding: request.Plan.Provider, Factory: func() (Provider, error) { return testProvider{}, nil }}}, []ProtocolRegistration{{Binding: request.Plan.Protocol, Factory: func() (Protocol, error) { return testProtocol{}, nil }}})
	if !errors.Is(err, ErrBindingDrift) {
		t.Fatalf("closure=%v", err)
	}
}

func TestStreamValidatorRejectsToolIdentityConflict(t *testing.T) {
	v := NewStreamValidator()
	if _, err := v.Accept(Event{Kind: EventToolCallDelta, ToolCall: &ToolCallDelta{Index: 0, ID: "call-a", Name: "tool", ArgumentsFragment: []byte("{")}}); err != nil {
		t.Fatal(err)
	}
	if _, err := v.Accept(Event{Kind: EventToolCallDelta, ToolCall: &ToolCallDelta{Index: 0, ID: "call-b", ArgumentsFragment: []byte("}")}}); !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("id conflict=%v", err)
	}
}

func TestRegistryContainsProtocolPanic(t *testing.T) {
	request := testRequest()
	r, err := NewRegistry(request.Plan.SnapshotRevision, []modelcontrol.ProviderPlan{request.Plan}, []ProviderRegistration{{Binding: request.Plan.Provider, Factory: func() (Provider, error) { return testProvider{}, nil }}}, []ProtocolRegistration{{Binding: request.Plan.Protocol, Factory: func() (Protocol, error) { return panicProtocol{}, nil }}})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Execute(context.Background(), request, func(Event) error { return nil }); !errors.Is(err, ErrStreamState) {
		t.Fatalf("panic=%v", err)
	}
}

func TestRegistryStopsOnEmitError(t *testing.T) {
	request := testRequest()
	r, err := NewRegistry(request.Plan.SnapshotRevision, []modelcontrol.ProviderPlan{request.Plan}, []ProviderRegistration{{Binding: request.Plan.Provider, Factory: func() (Provider, error) { return testProvider{}, nil }}}, []ProtocolRegistration{{Binding: request.Plan.Protocol, Factory: func() (Protocol, error) { return testProtocol{}, nil }}})
	if err != nil {
		t.Fatal(err)
	}
	want := errors.New("stop")
	if err := r.Execute(context.Background(), request, func(Event) error { return want }); !errors.Is(err, want) {
		t.Fatalf("emit=%v", err)
	}
}

func TestRegistryRejectsProtocolWithoutFinish(t *testing.T) {
	request := testRequest()
	r, err := NewRegistry(request.Plan.SnapshotRevision, []modelcontrol.ProviderPlan{request.Plan}, []ProviderRegistration{{Binding: request.Plan.Provider, Factory: func() (Provider, error) { return testProvider{}, nil }}}, []ProtocolRegistration{{Binding: request.Plan.Protocol, Factory: func() (Protocol, error) { return noFinishProtocol{}, nil }}})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Execute(context.Background(), request, func(Event) error { return nil }); !errors.Is(err, ErrStreamState) {
		t.Fatalf("finish=%v", err)
	}
}

func TestCredentialMaterialClearsAndCopies(t *testing.T) {
	material := NewCredentialMaterial([]byte("secret"))
	copy := material.Bytes()
	copy[0] = 'x'
	if string(material.Bytes()) != "secret" {
		t.Fatal("credential bytes alias material")
	}
	material.Clear()
	if material.Bytes() != nil {
		t.Fatal("credential clear retained material")
	}
}

func TestRequestByteBudgetIsExactAggregatedAndOverflowSafe(t *testing.T) {
	exact := testRequest()
	// "user" + "hello" are already counted by validateRequest.
	exact.System = strings.Repeat("a", int(DefaultMaxRequestBytes)-len("user")-len("hello"))
	if err := validateRequest(exact); err != nil {
		t.Fatalf("exact=%v", err)
	}
	over := exact
	over.System += "x"
	if err := validateRequest(over); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("one byte over=%v", err)
	}
	aggregated := testRequest()
	aggregated.System = strings.Repeat("a", int(DefaultMaxRequestBytes)-len("user")-len("hello")-len("assistant")-len("call")-len("tool")-len(`{}`))
	aggregated.Messages = append(aggregated.Messages, Message{Role: "assistant", ToolCalls: []ToolCall{{ID: "call", Name: "tool", Arguments: []byte(`{}`)}}})
	if err := validateRequest(aggregated); err != nil {
		t.Fatalf("aggregate exact=%v", err)
	}
	aggregated.Messages[1].ToolCalls[0].Arguments = []byte(`{"x":1}`)
	if err := validateRequest(aggregated); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("aggregate overflow=%v", err)
	}
	total := int64(math.MaxInt64)
	if err := addRequestBytes(&total, 1); err == nil {
		t.Fatal("integer overflow accepted")
	}
}

func TestRequestRoleStructure(t *testing.T) {
	for _, message := range []Message{
		{Role: "user", Content: "x", ToolCallID: "call"},
		{Role: "tool", Content: "", ToolCallID: ""},
		{Role: "tool", Content: "", ToolCallID: "call", ToolCalls: []ToolCall{{ID: "id", Name: "name", Arguments: []byte(`{}`)}}},
		{Role: "assistant", Content: "", ToolCallID: "call"},
	} {
		request := testRequest()
		request.Messages = []Message{message}
		if err := validateRequest(request); !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("message=%#v err=%v", message, err)
		}
	}
	for _, message := range []Message{{Role: "assistant", Content: ""}, {Role: "tool", Content: "", ToolCallID: "call"}} {
		request := testRequest()
		request.Messages = []Message{message}
		if err := validateRequest(request); err != nil {
			t.Fatalf("compatible message=%#v err=%v", message, err)
		}
	}
}

func TestIdentifierRejectsWhitespaceAndInvisibleUnicode(t *testing.T) {
	for _, value := range []string{" call", "call ", "call\u200b", "call\ue000"} {
		if !invalidIdentifier(value) {
			t.Fatalf("accepted %q", value)
		}
	}
	request := testRequest()
	request.Messages = []Message{{Role: "tool", ToolCallID: " call"}}
	if err := validateRequest(request); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("tool id=%v", err)
	}
}

func testRequest() Request {
	provider := modelcontrol.Ref{ID: "provider", Version: "1"}
	protocol := modelcontrol.Ref{ID: "protocol", Version: "1"}
	return Request{Plan: modelcontrol.ProviderPlan{Catalog: modelcontrol.CatalogModel{WireModel: "wire", Provider: provider, Protocol: protocol}, Provider: modelcontrol.ImplementationBinding{Ref: provider, ImplementationRevision: "provider-impl"}, Endpoint: modelcontrol.EndpointRef{ID: "endpoint", Revision: "1"}, Protocol: modelcontrol.ImplementationBinding{Ref: protocol, ImplementationRevision: "protocol-impl"}, SnapshotRevision: "sha256:test"}, Messages: []Message{{Role: "user", Content: "hello"}}}
}

type testProvider struct{}

func (testProvider) Send(context.Context, modelcontrol.ProviderPlan, OutboundRequest) (InboundResponse, error) {
	return InboundResponse{}, nil
}

type testProtocol struct{}

func (testProtocol) Execute(_ context.Context, _ Request, _ Provider, emit Emit) error {
	if err := emit(Event{Kind: EventTextDelta, Text: "ok"}); err != nil {
		return err
	}
	return emit(Event{Kind: EventFinish, Finish: FinishStop})
}

type noFinishProtocol struct{}

func (noFinishProtocol) Execute(_ context.Context, _ Request, _ Provider, emit Emit) error {
	return emit(Event{Kind: EventTextDelta, Text: "ok"})
}

type panicProtocol struct{}

func (panicProtocol) Execute(context.Context, Request, Provider, Emit) error { panic(secretPanic{}) }

type secretPanic struct{}

func (secretPanic) String() string { panic("must not render") }
