package modelexecution

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"unicode"

	"github.com/whhhh1500/auto-agent/pkg/app/modelcontrol"
)

const (
	DefaultMaxEvents                  = 4096
	DefaultMaxTextBytes               = 8 << 20
	DefaultMaxToolCalls               = 128
	DefaultMaxToolArgBytes            = 8 << 20
	DefaultMaxContinuationBytes       = 64 << 10
	DefaultMaxRequestBytes      int64 = 16 << 20
	DefaultMaxResponseBytes     int64 = 32 << 20
)

var (
	ErrInvalidRequest  = errors.New("invalid model execution request")
	ErrInvalidEvent    = errors.New("invalid model execution event")
	ErrBindingNotFound = errors.New("model execution binding not found")
	ErrBindingDrift    = errors.New("model execution binding drift")
	ErrFactoryPanic    = errors.New("model execution factory panicked")
	ErrStreamState     = errors.New("invalid model execution stream state")
)

// Message is a protocol-neutral conversation message. ToolArguments is a
// canonical JSON object supplied by the core bridge, never a provider payload.
type Message struct {
	Role       string     `json:"role"`
	Content    string     `json:"content"`
	ToolCallID string     `json:"tool_call_id,omitempty"`
	ToolCalls  []ToolCall `json:"tool_calls,omitempty"`
}

// ToolCall preserves the model's call identity and full JSON arguments.
type ToolCall struct {
	ID           string `json:"id"`
	Name         string `json:"name"`
	Arguments    []byte `json:"arguments"`
	Continuation string `json:"continuation,omitempty"`
}

// Tool describes a callable capability using a canonical JSON Schema object.
type Tool struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Parameters  []byte `json:"parameters"`
}

// Request is the protocol-neutral immutable input for one model call.
// Plan is created by modelcontrol and retains only opaque credential evidence.
type Request struct {
	Plan     modelcontrol.ProviderPlan `json:"plan"`
	System   string                    `json:"system,omitempty"`
	Messages []Message                 `json:"messages"`
	Tools    []Tool                    `json:"tools,omitempty"`
}

// EventKind describes an ordered normalized model stream.
type EventKind string

const (
	EventTextDelta     EventKind = "text_delta"
	EventToolCallDelta EventKind = "tool_call_delta"
	EventUsage         EventKind = "usage"
	EventFinish        EventKind = "finish"
)

// FinishReason intentionally has only the outcomes the existing core loop can
// execute. Protocol-specific stop reasons must map to one of these or fail.
type FinishReason string

const (
	FinishStop      FinishReason = "stop"
	FinishToolCalls FinishReason = "tool_calls"
)

// ToolCallDelta is lossless protocol output: index, identifier, name, and raw
// argument fragment are not parsed or rewritten at the protocol boundary.
type ToolCallDelta struct {
	Index             int    `json:"index"`
	ID                string `json:"id,omitempty"`
	Name              string `json:"name,omitempty"`
	ArgumentsFragment []byte `json:"arguments_fragment,omitempty"`
	// Continuation is one complete opaque value, never a text fragment.
	Continuation string `json:"continuation,omitempty"`
}

// Usage is provider-normalized metering for one request.
type Usage struct {
	InputTokens  int64 `json:"input_tokens"`
	OutputTokens int64 `json:"output_tokens"`
}

// Event carries exactly one EventKind payload.
type Event struct {
	Kind     EventKind      `json:"kind"`
	Text     string         `json:"text,omitempty"`
	ToolCall *ToolCallDelta `json:"tool_call,omitempty"`
	Usage    *Usage         `json:"usage,omitempty"`
	Finish   FinishReason   `json:"finish,omitempty"`
}

// Emit receives events synchronously. Returning an error stops provider I/O;
// protocols must return it rather than continuing to consume a response.
type Emit func(Event) error

// CredentialMaterial holds a short-lived resolved secret. It deliberately has
// no String, Error, JSON, or text-marshalling method. Bytes returns a copy so a
// provider may build a request. Callers must overwrite the returned byte copy
// after use; Clear zeroes only material owned by this value.
type CredentialMaterial struct{ bytes []byte }

// NewCredentialMaterial copies bytes for an authorization resolver. It is for
// resolver implementations and provider adapters, never plans or logs.
func NewCredentialMaterial(value []byte) CredentialMaterial {
	return CredentialMaterial{bytes: append([]byte(nil), value...)}
}
func (m *CredentialMaterial) Bytes() []byte {
	if m == nil {
		return nil
	}
	return append([]byte(nil), m.bytes...)
}
func (m *CredentialMaterial) Clear() {
	if m != nil {
		for i := range m.bytes {
			m.bytes[i] = 0
		}
		m.bytes = nil
	}
}

// CredentialResolver resolves the opaque plan reference just before transport.
// The zero CredentialRef is keyless and must not cause a resolver call.
type CredentialResolver interface {
	ResolveCredential(context.Context, modelcontrol.CredentialRef) (CredentialMaterial, error)
}

// OutboundRequest is wire-independent transport input constructed by a
// Protocol. The Provider resolves endpoint/authentication and sends it.
type OutboundRequest struct {
	Method           string
	Path             string
	Headers          map[string]string
	Body             []byte
	MaxResponseBytes int64
}

// InboundResponse is a bounded transport response. Body belongs to caller and
// must be closed. StatusBody is already bounded and safe to surface only after
// provider-specific redaction.
type InboundResponse struct {
	Status     int
	Headers    map[string]string
	Body       io.ReadCloser
	StatusBody string
}

// Provider owns endpoint, auth resolution, connection reuse, and transport.
// It cannot choose protocol or mutate a plan.
type Provider interface {
	Send(context.Context, modelcontrol.ProviderPlan, OutboundRequest) (InboundResponse, error)
}

// Protocol owns wire path/body construction and response parsing. A single
// protocol can therefore be paired with many Providers.
type Protocol interface {
	Execute(context.Context, Request, Provider, Emit) error
}

type ProviderFactory func() (Provider, error)
type ProtocolFactory func() (Protocol, error)

type ProviderRegistration struct {
	Binding modelcontrol.ImplementationBinding
	Factory ProviderFactory
}
type ProtocolRegistration struct {
	Binding modelcontrol.ImplementationBinding
	Factory ProtocolFactory
}

func (r Request) Clone() Request {
	r.Plan = clonePlan(r.Plan)
	r.Messages = append([]Message(nil), r.Messages...)
	for i := range r.Messages {
		r.Messages[i].ToolCalls = cloneToolCalls(r.Messages[i].ToolCalls)
	}
	r.Tools = append([]Tool(nil), r.Tools...)
	for i := range r.Tools {
		r.Tools[i].Parameters = append([]byte(nil), r.Tools[i].Parameters...)
	}
	return r
}
func cloneToolCalls(in []ToolCall) []ToolCall {
	out := append([]ToolCall(nil), in...)
	for i := range out {
		out[i].Arguments = append([]byte(nil), out[i].Arguments...)
	}
	return out
}

func validateRequest(r Request) error {
	if r.Plan.SnapshotRevision == "" || r.Plan.Provider.Ref.ID == "" || r.Plan.Provider.Ref.Version == "" || r.Plan.Provider.ImplementationRevision == "" || r.Plan.Protocol.Ref.ID == "" || r.Plan.Protocol.Ref.Version == "" || r.Plan.Protocol.ImplementationRevision == "" || r.Plan.Endpoint.ID == "" || r.Plan.Endpoint.Revision == "" || r.Plan.Catalog.WireModel == "" {
		return fmt.Errorf("%w: incomplete plan", ErrInvalidRequest)
	}
	if len(r.Messages) == 0 || len(r.Messages) > DefaultMaxEvents {
		return fmt.Errorf("%w: no messages", ErrInvalidRequest)
	}
	if len(r.Tools) > DefaultMaxToolCalls {
		return fmt.Errorf("%w: too many tools", ErrInvalidRequest)
	}
	var total int64
	if invalidText(r.System) || addRequestBytes(&total, len(r.System)) != nil {
		return fmt.Errorf("%w: system", ErrInvalidRequest)
	}
	for _, message := range r.Messages {
		if message.Role != "user" && message.Role != "assistant" && message.Role != "tool" || len(message.ToolCallID) > 256 || invalidText(message.Content) || addRequestBytes(&total, len(message.Role), len(message.Content), len(message.ToolCallID)) != nil {
			return fmt.Errorf("%w: message", ErrInvalidRequest)
		}
		if len(message.ToolCalls) > DefaultMaxToolCalls || (message.Role == "tool" && (invalidIdentifier(message.ToolCallID) || len(message.ToolCalls) > 0)) || (message.Role != "tool" && message.ToolCallID != "") || (message.Role != "assistant" && len(message.ToolCalls) > 0) {
			return fmt.Errorf("%w: message bounds", ErrInvalidRequest)
		}
		for _, call := range message.ToolCalls {
			if invalidIdentifier(call.ID) || invalidIdentifier(call.Name) || len(call.Arguments) > DefaultMaxToolArgBytes || !jsonObject(call.Arguments) || len(call.Continuation) > DefaultMaxContinuationBytes || !utf8Valid(call.Continuation) || addRequestBytes(&total, len(call.ID), len(call.Name), len(call.Arguments), len(call.Continuation)) != nil {
				return fmt.Errorf("%w: tool call", ErrInvalidRequest)
			}
		}
	}
	for _, tool := range r.Tools {
		if invalidIdentifier(tool.Name) || len(tool.Description) > 4096 || invalidText(tool.Description) || len(tool.Parameters) > DefaultMaxToolArgBytes || (len(tool.Parameters) > 0 && !jsonObject(tool.Parameters)) || addRequestBytes(&total, len(tool.Name), len(tool.Description), len(tool.Parameters)) != nil {
			return fmt.Errorf("%w: tool", ErrInvalidRequest)
		}
	}
	return nil
}
func addRequestBytes(total *int64, sizes ...int) error {
	if total == nil || *total < 0 {
		return fmt.Errorf("invalid request byte total")
	}
	for _, size := range sizes {
		if size < 0 || int64(size) > DefaultMaxRequestBytes-*total {
			return fmt.Errorf("request byte limit")
		}
		*total += int64(size)
	}
	return nil
}
func invalidIdentifier(value string) bool {
	if value == "" || value != strings.TrimSpace(value) || len(value) > 256 || invalidText(value) {
		return true
	}
	for _, character := range value {
		if unicode.Is(unicode.Cf, character) || unicode.Is(unicode.Cs, character) || unicode.Is(unicode.Co, character) {
			return true
		}
	}
	return false
}
func invalidText(value string) bool {
	if !utf8Valid(value) {
		return true
	}
	for _, character := range value {
		if unicode.IsControl(character) && character != '\n' && character != '\t' {
			return true
		}
	}
	return false
}
func utf8Valid(value string) bool { return strings.ToValidUTF8(value, "") == value }
func jsonObject(value []byte) bool {
	if len(value) == 0 {
		return false
	}
	var object map[string]json.RawMessage
	return json.Unmarshal(value, &object) == nil && object != nil
}
func clonePlan(plan modelcontrol.ProviderPlan) modelcontrol.ProviderPlan {
	plan.Catalog.Capabilities.Modalities = append([]modelcontrol.Modality(nil), plan.Catalog.Capabilities.Modalities...)
	return plan
}
