package openai

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cc-auto-agent/harness-core/pkg/adapter/modelexecution/corebridge"
	executionopenai "github.com/cc-auto-agent/harness-core/pkg/adapter/modelexecution/openai"
	"github.com/cc-auto-agent/harness-core/pkg/app/modelcontrol"
	"github.com/cc-auto-agent/harness-core/pkg/app/modelexecution"
	core "github.com/cc-auto-agent/harness-core/pkg/core"
)

// OpenAIAdapterConfig configures one OpenAI-compatible endpoint.
type OpenAIAdapterConfig struct {
	BaseURL       string
	APIKey        string
	Model         string
	MaxTokens     int
	AllowedModels []string
}

// NewOpenAIAdapterFromEnv reads the public HARNESS_LLM_* environment contract.
func NewOpenAIAdapterFromEnv() (*OpenAICompatibleAdapter, error) {
	base := os.Getenv("HARNESS_LLM_BASE_URL")
	if base == "" {
		base = "https://api.openai.com/v1"
	}
	key := os.Getenv("HARNESS_LLM_API_KEY")
	model := os.Getenv("HARNESS_LLM_MODEL")
	if key == "" {
		return nil, fmt.Errorf("HARNESS_LLM_API_KEY not set")
	}
	if model == "" {
		return nil, fmt.Errorf("HARNESS_LLM_MODEL not set")
	}
	allowed := splitNonEmpty(os.Getenv("HARNESS_LLM_ALLOWED_MODELS"))
	if len(allowed) > 0 && !stringInSlice(model, allowed) {
		return nil, fmt.Errorf("model %q not allowed by HARNESS_LLM_ALLOWED_MODELS", model)
	}
	maxTokens := 0
	if raw := os.Getenv("HARNESS_LLM_MAX_TOKENS"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value <= 0 {
			return nil, fmt.Errorf("HARNESS_LLM_MAX_TOKENS must be a positive integer")
		}
		maxTokens = value
	}
	return NewOpenAICompatibleAdapter(OpenAIAdapterConfig{
		BaseURL: base, APIKey: key, Model: model, MaxTokens: maxTokens, AllowedModels: allowed,
	}), nil
}

func stringInSlice(s string, list []string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

func splitNonEmpty(value string) []string {
	out := []string{}
	for _, item := range strings.Split(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			out = append(out, item)
		}
	}
	return out
}

// OpenAICompatibleAdapter is the real LLM adapter for an OpenAI-compatible
// /chat/completions gateway. It converts the harness's messages/tools to the
// OpenAI wire format, streams (falling back to a single JSON response), and
// emits one assistant chunk (full text + optional tool call) then a finish.
type OpenAICompatibleAdapter struct {
	cfg         OpenAIAdapterConfig
	http        *http.Client
	bridgeOnce  sync.Once
	bridge      *corebridge.Adapter
	bridgeErr   error
	credentials *executionopenai.StaticCredentialResolver
}

func NewOpenAICompatibleAdapter(cfg OpenAIAdapterConfig) *OpenAICompatibleAdapter {
	return &OpenAICompatibleAdapter{cfg: cfg, http: &http.Client{
		Timeout:       180 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}
}

func (a *OpenAICompatibleAdapter) Provider() string { return "openai-compatible" }

func (a *OpenAICompatibleAdapter) ArtifactRevision() string {
	return "openai-compatible/chat-completions/v2-terminal-sentinel"
}

func (a *OpenAICompatibleAdapter) Stream(ctx context.Context, opts core.GenerateOptions, emit func(core.StreamChunk)) error {
	if err := a.validate(); err != nil {
		return err
	}
	bridge, err := a.m2Bridge()
	if err != nil {
		return err
	}
	err = bridge.Stream(ctx, opts, emit)
	var status *executionopenai.HTTPStatusError
	if errors.As(err, &status) {
		return &HTTPStatusError{Status: status.Status, Body: "upstream response redacted"}
	}
	return err
}

func (a *OpenAICompatibleAdapter) m2Bridge() (*corebridge.Adapter, error) {
	if a == nil {
		return nil, fmt.Errorf("openai adapter is nil")
	}
	a.bridgeOnce.Do(func() { a.bridge, a.bridgeErr = a.buildM2Bridge() })
	return a.bridge, a.bridgeErr
}

func (a *OpenAICompatibleAdapter) buildM2Bridge() (*corebridge.Adapter, error) {
	providerRef := modelcontrol.Ref{ID: "openai-compatible", Version: "1"}
	protocolRef := modelcontrol.Ref{ID: "openai-chat-completions", Version: "1"}
	endpoint := modelcontrol.EndpointRef{ID: "legacy-openai-endpoint", Revision: "1"}
	credential := modelcontrol.CredentialRef{ID: "legacy-openai-credential", Revision: "1"}
	catalog, err := modelcontrol.NewRegistry([]modelcontrol.CatalogModel{{Ref: modelcontrol.Ref{ID: "legacy-openai-model", Version: "1"}, WireModel: a.cfg.Model, Provider: providerRef, Protocol: protocolRef, Credential: credential, Capabilities: modelcontrol.ModelCapabilities{ContextWindowTokens: 128000, MaxOutputTokens: 16384, ToolCalls: true, Modalities: []modelcontrol.Modality{modelcontrol.ModalityText}}}}, []modelcontrol.ProviderSpec{{Ref: providerRef, Endpoint: endpoint, ImplementationRevision: "openai-compatible-http-v1"}}, []modelcontrol.ProtocolSpec{{Ref: protocolRef, ImplementationRevision: "openai-chat-completions-v2-terminal-sentinel"}}, []modelcontrol.Compatibility{{Provider: providerRef, Protocol: protocolRef}})
	if err != nil {
		return nil, err
	}
	plan, err := catalog.Resolve(modelcontrol.ResolveInput{Catalog: modelcontrol.Ref{ID: "legacy-openai-model", Version: "1"}, CompositionRevision: "legacy-openai-facade-v1"})
	if err != nil {
		return nil, err
	}
	credentials := executionopenai.NewStaticCredentialResolver([]byte(a.cfg.APIKey))
	a.credentials = credentials
	provider, err := executionopenai.NewHTTPProvider(executionopenai.HTTPProviderConfig{Endpoint: endpoint, BaseURL: a.cfg.BaseURL, Credentials: credentials, Client: a.http})
	if err != nil {
		credentials.Clear()
		return nil, err
	}
	protocol := executionopenai.ChatCompletionsProtocol{MaxTokens: a.cfg.MaxTokens}
	registry, err := modelexecution.NewRegistry(plan.SnapshotRevision, []modelcontrol.ProviderPlan{plan}, []modelexecution.ProviderRegistration{{Binding: plan.Provider, Factory: func() (modelexecution.Provider, error) {
		return provider, nil
	}}}, []modelexecution.ProtocolRegistration{{Binding: plan.Protocol, Factory: func() (modelexecution.Protocol, error) {
		return protocol, nil
	}}})
	if err != nil {
		credentials.Clear()
		return nil, err
	}
	return &corebridge.Adapter{Registry: registry, Plan: plan}, nil
}

func (a *OpenAICompatibleAdapter) validate() error {
	if a == nil {
		return fmt.Errorf("openai adapter is nil")
	}
	if strings.TrimSpace(a.cfg.APIKey) == "" {
		return fmt.Errorf("openai adapter API key is empty")
	}
	if strings.ContainsAny(a.cfg.APIKey, "\r\n\x00") {
		return fmt.Errorf("openai adapter API key is invalid")
	}
	if strings.TrimSpace(a.cfg.Model) == "" {
		return fmt.Errorf("openai adapter model is empty")
	}
	if strings.ContainsAny(a.cfg.Model, "\r\n\x00") {
		return fmt.Errorf("openai adapter model is invalid")
	}
	if a.cfg.MaxTokens < 0 {
		return fmt.Errorf("openai adapter max tokens must not be negative")
	}
	if len(a.cfg.AllowedModels) > 0 && !stringInSlice(a.cfg.Model, a.cfg.AllowedModels) {
		return fmt.Errorf("model %q is not in the adapter allow-list", a.cfg.Model)
	}
	parsed, err := url.Parse(strings.TrimSpace(a.cfg.BaseURL))
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Hostname() == "" {
		return fmt.Errorf("openai adapter base URL is invalid")
	}
	if parsed.User != nil {
		return fmt.Errorf("openai adapter base URL must not contain user information")
	}
	return nil
}

// HTTPStatusError carries the gateway status so retry policies can classify
// failures (429/5xx retryable, 4xx not) without parsing message text.
type HTTPStatusError struct {
	Status int
	Body   string
}

func (e *HTTPStatusError) Error() string {
	return fmt.Sprintf("llm: status %d: %s", e.Status, e.Body)
}

// Retryable reports whether the same request may succeed on retry.
func (e *HTTPStatusError) Retryable() bool {
	return e.Status == http.StatusTooManyRequests || e.Status >= http.StatusInternalServerError
}

// ----- response parsing (stream or single JSON) -----

func parseOpenAIResponse(r io.Reader, emit func(core.StreamChunk)) error {
	reader := bufio.NewReader(&boundedReader{reader: r, remaining: 32 << 20})
	first, err := firstNonSpaceByte(reader)
	if err != nil {
		return err
	}
	// The gateway may ignore stream:true and return a single JSON response.
	if first == 'd' {
		_, calls, usage, err := parseSSEReader(reader, emit)
		if err != nil {
			return err
		}
		// Text deltas were already emitted while bytes arrived. The terminal
		// chunk carries calls and usage only, avoiding duplicated assistant text.
		finalizeParse(emit, "", calls, usage, len(calls) > 0)
		return nil
	}
	raw, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	text, calls, usage, err := parseSingle(raw)
	if err != nil {
		return err
	}
	finalizeParse(emit, text, calls, usage, len(calls) > 0)
	return nil
}

func firstNonSpaceByte(reader *bufio.Reader) (byte, error) {
	for {
		peek, err := reader.Peek(1)
		if err != nil {
			return 0, err
		}
		switch peek[0] {
		case ' ', '\t', '\r', '\n':
			_, _ = reader.Discard(1)
		default:
			return peek[0], nil
		}
	}
}

type boundedReader struct {
	reader    io.Reader
	remaining int64
}

func (r *boundedReader) Read(payload []byte) (int, error) {
	if r.remaining <= 0 {
		return 0, fmt.Errorf("llm response exceeded 32 MiB")
	}
	if int64(len(payload)) > r.remaining {
		payload = payload[:r.remaining]
	}
	n, err := r.reader.Read(payload)
	r.remaining -= int64(n)
	return n, err
}

// finalizeParse emits the terminal assistant chunk (with all tool calls) and
// the finish chunk after the adapter has already streamed text deltas.
func finalizeParse(emit func(core.StreamChunk), text string, calls []core.ToolCall, usage *core.TokenUsage, hasCalls bool) {
	chunk := core.StreamChunk{Kind: "assistant", Text: text, ToolCalls: calls}
	if len(calls) > 0 {
		first := calls[0]
		chunk.ToolCall = &first
	}
	emit(chunk)
	if hasCalls {
		emit(core.StreamChunk{Kind: "finish", FinishKind: "tool-calls", Usage: usage})
	} else {
		emit(core.StreamChunk{Kind: "finish", FinishKind: "stop", Usage: usage})
	}
}

const maxOpenAIToolArgsBytes = core.DefaultMaxCapabilityOutputBytes

type toolCallAccumulator struct {
	id   string
	name string
	args strings.Builder
}

// parseSSE consumes an OpenAI-compatible SSE stream. Tool-call deltas are
// merged by their declared index, so models issuing several parallel calls in
// one step parse correctly. Text deltas are emitted as they arrive.
func parseSSE(raw []byte, emit func(core.StreamChunk)) (string, []core.ToolCall, *core.TokenUsage, error) {
	return parseSSEReader(bytes.NewReader(raw), emit)
}

func parseSSEReader(reader io.Reader, emit func(core.StreamChunk)) (string, []core.ToolCall, *core.TokenUsage, error) {
	var text strings.Builder
	accumulators := map[int]*toolCallAccumulator{}
	var usage *core.TokenUsage

	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 0, 1<<20), 1<<20)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if data == "[DONE]" {
			break
		}
		var chunk struct {
			Choices []struct {
				Delta struct {
					Content   *string `json:"content"`
					ToolCalls []struct {
						Index    *int   `json:"index"`
						ID       string `json:"id"`
						Function struct {
							Name      string `json:"name"`
							Arguments string `json:"arguments"`
						} `json:"function"`
					} `json:"tool_calls"`
				} `json:"delta"`
			} `json:"choices"`
			Usage *struct {
				PromptTokens     int64 `json:"prompt_tokens"`
				CompletionTokens int64 `json:"completion_tokens"`
			} `json:"usage"`
		}
		if err := json.Unmarshal([]byte(data), &chunk); err != nil {
			continue
		}
		if chunk.Usage != nil && (chunk.Usage.PromptTokens > 0 || chunk.Usage.CompletionTokens > 0) {
			usage = &core.TokenUsage{InputTokens: chunk.Usage.PromptTokens, OutputTokens: chunk.Usage.CompletionTokens}
		}
		for _, c := range chunk.Choices {
			if c.Delta.Content != nil && *c.Delta.Content != "" {
				text.WriteString(*c.Delta.Content)
				emit(core.StreamChunk{Kind: "assistant", Text: *c.Delta.Content})
			}
			for _, tc := range c.Delta.ToolCalls {
				index := 0
				if tc.Index != nil {
					index = *tc.Index
				}
				if err := accumulateToolCall(accumulators, index, tc.ID, tc.Function.Name, tc.Function.Arguments); err != nil {
					return text.String(), nil, usage, err
				}
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return text.String(), nil, usage, err
	}
	calls := buildToolCalls(accumulators)
	return text.String(), calls, usage, nil
}

func buildToolCalls(accumulators map[int]*toolCallAccumulator) []core.ToolCall {
	if len(accumulators) == 0 {
		return nil
	}
	indexes := make([]int, 0, len(accumulators))
	for index := range accumulators {
		indexes = append(indexes, index)
	}
	sort.Ints(indexes)
	calls := make([]core.ToolCall, 0, len(indexes))
	for position, index := range indexes {
		acc := accumulators[index]
		var parsedArgs map[string]any
		if trimmed := strings.TrimSpace(acc.args.String()); trimmed != "" {
			_ = json.Unmarshal([]byte(trimmed), &parsedArgs)
		}
		id := acc.id
		if id == "" {
			id = fmt.Sprintf("call-%d", position+1)
		}
		calls = append(calls, core.ToolCall{ID: id, Name: acc.name, Args: parsedArgs})
	}
	return calls
}

// parseSingle parses a non-stream JSON response and emits its content once.
func parseSingle(raw []byte) (string, []core.ToolCall, *core.TokenUsage, error) {
	var resp struct {
		Choices []struct {
			Message struct {
				Content   string `json:"content"`
				ToolCalls []struct {
					ID       string `json:"id"`
					Function struct {
						Name      string `json:"name"`
						Arguments string `json:"arguments"`
					} `json:"function"`
				} `json:"tool_calls"`
			} `json:"message"`
		} `json:"choices"`
		Usage *struct {
			PromptTokens     int64 `json:"prompt_tokens"`
			CompletionTokens int64 `json:"completion_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		return "", nil, nil, err
	}
	var usage *core.TokenUsage
	if resp.Usage != nil {
		usage = &core.TokenUsage{InputTokens: resp.Usage.PromptTokens, OutputTokens: resp.Usage.CompletionTokens}
	}
	if len(resp.Choices) == 0 {
		return "", nil, usage, nil
	}
	msg := resp.Choices[0].Message
	if len(msg.ToolCalls) > core.HardMaxToolCalls {
		return "", nil, usage, fmt.Errorf("openai tool call count exceeds %d", core.HardMaxToolCalls)
	}
	accumulators := map[int]*toolCallAccumulator{}
	for i, tc := range msg.ToolCalls {
		if err := accumulateToolCall(accumulators, i, tc.ID, tc.Function.Name, tc.Function.Arguments); err != nil {
			return "", nil, usage, err
		}
	}
	calls := buildToolCalls(accumulators)
	return msg.Content, calls, usage, nil
}

func accumulateToolCall(accumulators map[int]*toolCallAccumulator, index int, id, name, arguments string) error {
	if index < 0 || index >= core.HardMaxToolCalls {
		return fmt.Errorf("openai tool call index %d is out of range", index)
	}
	if id != "" && (len(id) > 256 || strings.ContainsAny(id, "\r\n\x00")) {
		return fmt.Errorf("openai tool call id is invalid")
	}
	acc := accumulators[index]
	if acc == nil {
		acc = &toolCallAccumulator{}
		accumulators[index] = acc
	}
	if id != "" {
		acc.id = id
	}
	if name != "" {
		acc.name = name
	}
	if arguments == "" {
		return nil
	}
	if acc.args.Len()+len(arguments) > maxOpenAIToolArgsBytes {
		return fmt.Errorf("openai tool call arguments exceeded %d bytes", maxOpenAIToolArgsBytes)
	}
	acc.args.WriteString(arguments)
	return nil
}
