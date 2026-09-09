package programmatic

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
	"sync"

	"github.com/whhhh1500/auto-agent/pkg/core"
)

const (
	// DefaultCatalogToolID and DefaultExecuteToolID are the standard direct-tool
	// IDs. They are defaults only: a host may use different names at composition
	// time without this package depending on a particular provider adapter.
	DefaultCatalogToolID    = "program.catalog"
	DefaultExecuteToolID    = "program.execute"
	ptcCatalogVersion       = "ptc-ir/v1"
	maxCatalogResultSize    = 256 << 10
	maxFirstActionBytes     = 8 << 20
	maxFirstActionChunks    = 4096
	maxFirstActionCalls     = 128
	maxProjectedSchemaBytes = 256 << 10
	maxProjectedSchemaDepth = 64
)

// RouteMode controls the model-facing direct-tool/PTC projection. Values
// outside these constants are rejected by NewModelResolver and NewLlmAdapter.
type RouteMode string

const (
	RouteAutoFirstAction RouteMode = "auto_first_action"
	RouteDirectOnly      RouteMode = "direct_only"
	RoutePTCOnly         RouteMode = "ptc_only"
)

var (
	ErrInvalidRouteProjection = errors.New("invalid programmatic route projection")
	ErrInvalidModelResolver   = errors.New("programmatic route projection model resolver is invalid")
	ErrInvalidLlmAdapter      = errors.New("programmatic route projection llm adapter is invalid")
)

// RouteProjectionOptions configures a stateless model-context projection. It
// neither grants capabilities nor retains messages, catalog bodies, or tool
// results. Empty tool IDs use the standard program.catalog/program.execute
// names. CodePTC deliberately has no route or candidate in this projection.
type RouteProjectionOptions struct {
	Mode                  RouteMode
	CatalogToolID         string
	ExecuteToolID         string
	CatalogResultVerifier CatalogResultVerifier
	ExecuteToolVerifier   ExecuteToolVerifier
	// ProbePlanVerifier identifies a completed initial action as either an
	// ordinary Direct call or a neutral probe and, for the latter, reconstructs
	// the immutable plan from durable evidence. It is used only by v2.
	ProbePlanVerifier ProbePlanVerifier
	// ProbeToolClassifier identifies neutral probe schemas before their first
	// call is journaled. It is deliberately separate from ProbePlanVerifier:
	// new model output has no durable result from which a plan can be rebuilt.
	ProbeToolClassifier ProbeToolClassifier
	// ChoiceResultVerifier confirms from authoritative evidence that a completed
	// v2 choice call belongs to the reconstructed plan and completed
	// successfully. Chat history only carries model-visible content, so it
	// cannot establish that result status on its own.
	ChoiceResultVerifier ChoiceResultVerifier
	// ChoiceAdmission validates one complete buffered v2 choice batch before the
	// adapter emits it to Core. It is pre-effect validation, not a durable
	// reservation or compare-and-swap. The callback receives the model's original
	// execute arguments; host-owned generic execute arguments are substituted only
	// after validation succeeds.
	ChoiceAdmission ChoiceAdmission
}

// ProbeToolClassifier classifies one already-authorized model tool by name.
// It does not grant authority and must fail closed when the frozen capability
// evidence is unavailable or changes.
type ProbeToolClassifier func(ctx context.Context, runID, toolName string) (bool, error)

// ChoiceRouteKind identifies the v2 choice form whose result is being
// verified. It is deliberately distinct from the model-facing schema name:
// the execute schema is route-specific while the journal receives the generic
// program.execute call after the adapter transforms its arguments.
type ChoiceRouteKind string

const (
	ChoiceRouteDirect  ChoiceRouteKind = "direct"
	ChoiceRouteExecute ChoiceRouteKind = "execute"
)

// ChoiceResultVerifier proves that the completed choice call is the exact
// durable result for plan and kind. A nil verifier intentionally never
// unlocks the final model call; providers must not synthesize OK=true from
// model-visible tool content.
type ChoiceResultVerifier func(ctx context.Context, runID, callID string, plan ProbePlan, kind ChoiceRouteKind) (bool, error)

// ChoiceAdmission validates the frozen run capacity for one complete buffered
// selected v2 choice batch before Core sees it. A callback must fail closed
// when it cannot reconstruct the prior call count or selected child count from
// authoritative evidence. It neither receives nor changes Core execution
// state, so it is not a durable reservation or compare-and-swap.
type ChoiceAdmission func(ctx context.Context, runID string, plan ProbePlan, kind ChoiceRouteKind, calls []core.ToolCall) error

// CatalogResultVerifier confirms that a strictly paired catalog call completed
// successfully in the host's durable tool-result record. ChatMessage has no
// result status, so a nil verifier intentionally never unlocks execute.
// Implementations must not retain model content.
type CatalogResultVerifier func(context.Context, string, string) (bool, error)

// ExecuteToolVerifier proves that this exact execute schema belongs to the
// same frozen capability snapshot as the verified catalog result. A nil
// verifier intentionally withholds program.execute to fail closed on schema
// drift or revocation.
type ExecuteToolVerifier func(context.Context, string, core.ToolSchema) (bool, error)

type routeProjectionConfig struct {
	mode          RouteMode
	catalogID     string
	executeID     string
	verify        CatalogResultVerifier
	verifyExecute ExecuteToolVerifier
	verifyProbe   ProbePlanVerifier
	classifyProbe ProbeToolClassifier
	verifyChoice  ChoiceResultVerifier
	admitChoice   ChoiceAdmission
}

// ModelResolver wraps a provider-neutral resolver. The returned model adapter
// projects only the already-authorized tool snapshot supplied by core.
type ModelResolver struct {
	inner  core.ModelResolver
	config routeProjectionConfig
}

// NewModelResolver returns a resolver that wraps each resolved LLM adapter.
// It has no server or configuration registration side effects.
func NewModelResolver(inner core.ModelResolver, options RouteProjectionOptions) (*ModelResolver, error) {
	config, err := newRouteProjectionConfig(options)
	if err != nil {
		return nil, err
	}
	if inner == nil {
		return nil, ErrInvalidModelResolver
	}
	return &ModelResolver{inner: inner, config: config}, nil
}

// ResolveModel preserves the delegate selection and wraps only a non-nil
// result. A faulty optional integration cannot panic through this boundary.
func (r *ModelResolver) ResolveModel(ctx context.Context, selection core.ModelSelection) (adapter core.LlmAdapter, err error) {
	defer func() {
		if recover() != nil {
			adapter = nil
			err = ErrInvalidModelResolver
		}
	}()
	if r == nil || r.inner == nil {
		return nil, ErrInvalidModelResolver
	}
	inner, err := r.inner.ResolveModel(ctx, selection)
	if err != nil {
		return nil, err
	}
	if inner == nil {
		return nil, ErrInvalidLlmAdapter
	}
	return &LlmAdapter{inner: inner, config: r.config}, nil
}

// LlmAdapter wraps one provider-neutral adapter. It stores only its immutable
// configuration and delegate; routing is recomputed from each input context.
type LlmAdapter struct {
	inner  core.LlmAdapter
	config routeProjectionConfig
}

// NewLlmAdapter wraps one model adapter for callers that do not resolve models
// dynamically.
func NewLlmAdapter(inner core.LlmAdapter, options RouteProjectionOptions) (*LlmAdapter, error) {
	config, err := newRouteProjectionConfig(options)
	if err != nil {
		return nil, err
	}
	if inner == nil {
		return nil, ErrInvalidLlmAdapter
	}
	return &LlmAdapter{inner: inner, config: config}, nil
}

func (a *LlmAdapter) Provider() (provider string) {
	defer func() {
		if recover() != nil {
			provider = ""
		}
	}()
	if a == nil || a.inner == nil {
		return ""
	}
	return a.inner.Provider()
}

// ArtifactRevision forwards the optional core revision interface unchanged.
func (a *LlmAdapter) ArtifactRevision() (revision string) {
	defer func() {
		if recover() != nil {
			revision = ""
		}
	}()
	if a == nil || a.inner == nil {
		return ""
	}
	if revisioner, ok := a.inner.(core.ArtifactRevisioner); ok {
		return revisioner.ArtifactRevision()
	}
	return ""
}

// ModelContextLimits forwards the optional core model-context limits. A nil,
// missing, or panicking implementation returns zero values so core applies its
// conservative defaults.
func (a *LlmAdapter) ModelContextLimits() (window, output int) {
	defer func() {
		if recover() != nil {
			window, output = 0, 0
		}
	}()
	if a == nil || a.inner == nil {
		return 0, 0
	}
	if limits, ok := a.inner.(interface{ ModelContextLimits() (int, int) }); ok {
		return limits.ModelContextLimits()
	}
	return 0, 0
}

// Stream forwards the original request except for a private filtered Tools
// slice. It never changes Messages or their content and never retains either.
func (a *LlmAdapter) Stream(ctx context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) (err error) {
	defer func() {
		if recover() != nil {
			err = ErrInvalidLlmAdapter
		}
	}()
	if a == nil || a.inner == nil {
		return ErrInvalidLlmAdapter
	}
	projected := options
	route := routeAuto
	var probePlan ProbePlan
	var routeErr error
	if a.config.mode == RouteAutoProbeOnce {
		state, err := probeRouteFromMessages(ctx, options.Messages, options.ModelCall.RunID, a.config)
		route, probePlan, routeErr = state.route, state.plan, err
	} else {
		route, routeErr = routeFromMessages(ctx, options.Messages, options.ModelCall.RunID, a.config)
	}
	if routeErr != nil {
		return ErrInvalidRouteProjection
	}
	var tools []core.ToolSchema
	var projectErr error
	if a.config.mode == RouteAutoProbeOnce {
		tools, projectErr = projectProbeRouteTools(ctx, options.ModelCall.RunID, options.Tools, route, probePlan, a.config)
	} else {
		tools, projectErr = projectRouteTools(ctx, options.ModelCall.RunID, options.Tools, route, a.config)
	}
	if projectErr != nil {
		return ErrInvalidRouteProjection
	}
	projected.Tools = tools
	if a.config.mode == RouteAutoFirstAction && route == routeAuto {
		return a.streamFirstAction(ctx, projected, emit)
	}
	if a.config.mode == RouteAutoProbeOnce {
		switch route {
		case routeProbeInitial:
			return a.streamProbeInitial(ctx, projected, emit)
		case routeProbeChoice:
			return a.streamProbeChoice(ctx, projected, probePlan, emit)
		}
	}
	return a.streamAllowedTools(ctx, projected, emit)
}

// streamAllowedTools enforces the projection at the provider boundary. Core
// checks snapshot authority, while this adapter also rejects a model call that
// names a capability intentionally omitted from this round's model surface.
func (a *LlmAdapter) streamAllowedTools(ctx context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	allowed := allowedToolNames(options.Tools)
	var mu sync.Mutex
	illegal := false
	err := a.inner.Stream(ctx, options, func(chunk core.StreamChunk) {
		mu.Lock()
		defer mu.Unlock()
		if illegal {
			return
		}
		if !chunkUsesOnlyAllowedTools(chunk, allowed) {
			illegal = true
			return
		}
		if emit != nil {
			emit(chunk)
		}
	})
	mu.Lock()
	blocked := illegal
	mu.Unlock()
	if blocked {
		return ErrInvalidRouteProjection
	}
	return err
}

// streamFirstAction withholds every chunk until it has inspected the complete
// first action. That prevents a mixed catalog/direct batch from reaching core
// before this projection can reject it. Ordinary direct-only batches remain
// valid; only catalog ambiguity is rejected.
func (a *LlmAdapter) streamFirstAction(ctx context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	var mu sync.Mutex
	chunks := make([]core.StreamChunk, 0, 8)
	bytes := 0
	callCount := 0
	overflow := false
	illegal := false
	closed := false
	allowed := allowedToolNames(options.Tools)
	catalogs := make([]core.ToolCall, 0, 1)
	direct := false
	err := a.inner.Stream(ctx, options, func(chunk core.StreamChunk) {
		mu.Lock()
		defer mu.Unlock()
		if closed || overflow || illegal {
			return
		}
		calls := streamCalls(chunk)
		if !chunkUsesOnlyAllowedTools(chunk, allowed) {
			illegal = true
			return
		}
		chunkBytes, sizeErr := bufferedChunkBytes(chunk, maxFirstActionBytes-bytes)
		if len(chunks) >= maxFirstActionChunks || sizeErr != nil || callCount+len(calls) > maxFirstActionCalls {
			overflow = true
			return
		}
		copied, copyErr := cloneStreamChunk(chunk)
		if copyErr != nil {
			overflow = true
			return
		}
		bytes += chunkBytes
		callCount += len(calls)
		chunks = append(chunks, copied)
		for _, call := range calls {
			if call.Name == a.config.catalogID {
				if !containsEquivalentCall(catalogs, call) {
					catalogs = append(catalogs, call)
				}
				continue
			}
			direct = true
		}
	})
	mu.Lock()
	closed = true
	blocked, exceeded := illegal, overflow
	buffered := append([]core.StreamChunk(nil), chunks...)
	mu.Unlock()
	if err != nil || exceeded || blocked {
		if err != nil {
			return err
		}
		if blocked {
			return ErrInvalidRouteProjection
		}
		return ErrInvalidLlmAdapter
	}
	if len(catalogs) > 1 || (len(catalogs) == 1 && direct) {
		return ErrInvalidRouteProjection
	}
	for _, chunk := range buffered {
		if emit != nil {
			emit(chunk)
		}
	}
	return nil
}

// streamProbeInitial buffers the complete first v2 action so a classified
// neutral probe cannot be mixed with catalog or ordinary Direct calls before
// the model stream reaches Core.
func (a *LlmAdapter) streamProbeInitial(ctx context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	return a.streamBufferedRoute(ctx, options, emit, func(chunks []core.StreamChunk) error {
		calls, err := uniqueBufferedCalls(chunks)
		if err != nil {
			return err
		}
		catalogs, probes := 0, 0
		for _, call := range calls {
			if call.Name == a.config.catalogID {
				catalogs++
				continue
			}
			isProbe, classifyErr := classifiedProbeTool(ctx, options.ModelCall.RunID, call.Name, a.config.classifyProbe)
			if classifyErr != nil {
				return classifyErr
			}
			if isProbe {
				probes++
			}
		}
		if catalogs > 1 || probes > 1 || (catalogs == 1 && len(calls) != 1) || (probes == 1 && len(calls) != 1) {
			return ErrInvalidRouteProjection
		}
		return nil
	})
}

// streamProbeChoice validates the model's selected plan form before emitting
// anything. Execute is replaced with host-owned generic arguments here, so
// Core and its journal never receive candidate indices.
func (a *LlmAdapter) streamProbeChoice(ctx context.Context, options core.GenerateOptions, plan ProbePlan, emit func(core.StreamChunk)) error {
	return a.streamBufferedRoute(ctx, options, emit, func(chunks []core.StreamChunk) error {
		calls, err := uniqueBufferedCalls(chunks)
		if err != nil {
			return err
		}
		if len(calls) == 0 {
			return nil
		}
		directName := plan.DirectToolSchema().Name
		executeName := plan.ExecuteToolSchema().Name
		if directName == "" {
			return ErrInvalidRouteProjection
		}
		execute := -1
		for index, call := range calls {
			switch call.Name {
			case executeName:
				if executeName == "" {
					return ErrInvalidRouteProjection
				}
				if execute >= 0 || len(calls) != 1 {
					return ErrInvalidRouteProjection
				}
				execute = index
			case directName:
				if plan.ValidateDirect(call.Args) != nil {
					return ErrInvalidRouteProjection
				}
				for previous := 0; previous < index; previous++ {
					if calls[previous].Name == directName && sameArgs(calls[previous].Args, call.Args) {
						return ErrInvalidRouteProjection
					}
				}
			default:
				return ErrInvalidRouteProjection
			}
		}
		if execute < 0 {
			if len(calls) > plan.maxSelections || !admitChoice(ctx, options.ModelCall.RunID, plan, ChoiceRouteDirect, calls, a.config.admitChoice) {
				return ErrInvalidRouteProjection
			}
			return nil
		}
		prepared, err := plan.PrepareExecute(calls[execute].Args)
		if err != nil {
			return ErrInvalidRouteProjection
		}
		if !admitChoice(ctx, options.ModelCall.RunID, plan, ChoiceRouteExecute, calls, a.config.admitChoice) {
			return ErrInvalidRouteProjection
		}
		return replaceBufferedToolArgs(chunks, calls[execute], prepared)
	})
}

func admitChoice(ctx context.Context, runID string, plan ProbePlan, kind ChoiceRouteKind, calls []core.ToolCall, admit ChoiceAdmission) (ok bool) {
	defer func() {
		if recover() != nil {
			ok = false
		}
	}()
	encoded, err := json.Marshal(calls)
	if err != nil || len(encoded) > maxFirstActionBytes {
		return false
	}
	var copied []core.ToolCall
	if json.Unmarshal(encoded, &copied) != nil || len(copied) != len(calls) {
		return false
	}
	if admit == nil || admit(ctx, runID, plan, kind, copied) != nil {
		return false
	}
	return true
}

func (a *LlmAdapter) streamBufferedRoute(ctx context.Context, options core.GenerateOptions, emit func(core.StreamChunk), validate func([]core.StreamChunk) error) error {
	var mu sync.Mutex
	chunks := make([]core.StreamChunk, 0, 8)
	bytes, callCount := 0, 0
	overflow, illegal, closed := false, false, false
	allowed := allowedToolNames(options.Tools)
	err := a.inner.Stream(ctx, options, func(chunk core.StreamChunk) {
		mu.Lock()
		defer mu.Unlock()
		if closed || overflow || illegal {
			return
		}
		calls := streamCalls(chunk)
		if !chunkUsesOnlyAllowedTools(chunk, allowed) {
			illegal = true
			return
		}
		chunkBytes, sizeErr := bufferedChunkBytes(chunk, maxFirstActionBytes-bytes)
		if len(chunks) >= maxFirstActionChunks || sizeErr != nil || callCount+len(calls) > maxFirstActionCalls {
			overflow = true
			return
		}
		copy, copyErr := cloneStreamChunk(chunk)
		if copyErr != nil {
			overflow = true
			return
		}
		bytes += chunkBytes
		callCount += len(calls)
		chunks = append(chunks, copy)
	})
	mu.Lock()
	closed = true
	blocked, exceeded := illegal, overflow
	buffered := append([]core.StreamChunk(nil), chunks...)
	mu.Unlock()
	if err != nil {
		return err
	}
	if blocked || exceeded || validate(buffered) != nil {
		return ErrInvalidRouteProjection
	}
	for _, chunk := range buffered {
		if emit != nil {
			emit(chunk)
		}
	}
	return nil
}

func uniqueBufferedCalls(chunks []core.StreamChunk) ([]core.ToolCall, error) {
	calls := make([]core.ToolCall, 0)
	seen := make(map[string]core.ToolCall)
	for _, chunk := range chunks {
		for _, call := range streamCalls(chunk) {
			if prior, found := seen[call.ID]; found {
				if !containsEquivalentCall([]core.ToolCall{prior}, call) {
					return nil, ErrInvalidRouteProjection
				}
				continue
			}
			seen[call.ID] = call
			calls = append(calls, call)
		}
	}
	return calls, nil
}

func replaceBufferedToolArgs(chunks []core.StreamChunk, selected core.ToolCall, args map[string]any) error {
	for index := range chunks {
		if chunks[index].ToolCall != nil && sameToolCallForProjection(*chunks[index].ToolCall, selected) {
			copy, err := cloneToolCall(*chunks[index].ToolCall)
			if err != nil {
				return err
			}
			copy.Args = args
			chunks[index].ToolCall = &copy
		}
		for callIndex := range chunks[index].ToolCalls {
			if !sameToolCallForProjection(chunks[index].ToolCalls[callIndex], selected) {
				continue
			}
			copy, err := cloneToolCall(chunks[index].ToolCalls[callIndex])
			if err != nil {
				return err
			}
			copy.Args = args
			chunks[index].ToolCalls[callIndex] = copy
		}
	}
	return nil
}

func sameToolCallForProjection(left, right core.ToolCall) bool {
	return left.ID == right.ID && left.Name == right.Name && left.Continuation == right.Continuation && sameArgs(left.Args, right.Args)
}

// bufferedChunkBytes measures every value retained by the first-action buffer.
// Tool-call args are counted from their JSON encoding because cloning them
// produces a second JSON-native object graph. The limited writer rejects an
// oversized value without materializing another full encoded copy.
func bufferedChunkBytes(chunk core.StreamChunk, remaining int) (int, error) {
	if remaining < 0 {
		return 0, ErrInvalidRouteProjection
	}
	used := len(chunk.Text) + len(chunk.FinishKind)
	if used > remaining {
		return 0, ErrInvalidRouteProjection
	}
	countCall := func(call core.ToolCall) error {
		fixed := len(call.ID) + len(call.Name) + len(call.Continuation)
		if fixed > remaining-used {
			return ErrInvalidRouteProjection
		}
		used += fixed
		counter := limitedJSONByteCounter{remaining: remaining - used}
		if err := json.NewEncoder(&counter).Encode(call.Args); err != nil {
			return ErrInvalidRouteProjection
		}
		used += counter.used
		return nil
	}
	if chunk.ToolCall != nil {
		if err := countCall(*chunk.ToolCall); err != nil {
			return 0, err
		}
	}
	for _, call := range chunk.ToolCalls {
		if err := countCall(call); err != nil {
			return 0, err
		}
	}
	return used, nil
}

type limitedJSONByteCounter struct {
	remaining int
	used      int
}

func (c *limitedJSONByteCounter) Write(bytes []byte) (int, error) {
	if len(bytes) > c.remaining {
		return 0, ErrInvalidRouteProjection
	}
	c.remaining -= len(bytes)
	c.used += len(bytes)
	return len(bytes), nil
}

func allowedToolNames(tools []core.ToolSchema) map[string]struct{} {
	allowed := make(map[string]struct{}, len(tools))
	for _, tool := range tools {
		allowed[tool.Name] = struct{}{}
	}
	return allowed
}

func chunkUsesOnlyAllowedTools(chunk core.StreamChunk, allowed map[string]struct{}) bool {
	for _, call := range streamCalls(chunk) {
		if _, ok := allowed[call.Name]; !ok {
			return false
		}
	}
	return true
}

func cloneStreamChunk(chunk core.StreamChunk) (core.StreamChunk, error) {
	copy := chunk
	if chunk.ToolCall != nil {
		call, err := cloneToolCall(*chunk.ToolCall)
		if err != nil {
			return core.StreamChunk{}, err
		}
		copy.ToolCall = &call
	}
	copy.ToolCalls = make([]core.ToolCall, len(chunk.ToolCalls))
	for index, call := range chunk.ToolCalls {
		cloned, err := cloneToolCall(call)
		if err != nil {
			return core.StreamChunk{}, err
		}
		copy.ToolCalls[index] = cloned
	}
	return copy, nil
}

func cloneToolCall(call core.ToolCall) (core.ToolCall, error) {
	copy := call
	if call.Args == nil {
		return copy, nil
	}
	encoded, err := json.Marshal(call.Args)
	if err != nil {
		return core.ToolCall{}, err
	}
	copy.Args = make(map[string]any, len(call.Args))
	if err := json.Unmarshal(encoded, &copy.Args); err != nil {
		return core.ToolCall{}, err
	}
	return copy, nil
}

func streamCalls(chunk core.StreamChunk) []core.ToolCall {
	calls := make([]core.ToolCall, 0, len(chunk.ToolCalls)+1)
	if chunk.ToolCall != nil {
		calls = append(calls, *chunk.ToolCall)
	}
	for _, call := range chunk.ToolCalls {
		if !containsEquivalentCall(calls, call) {
			calls = append(calls, call)
		}
	}
	return calls
}

func containsEquivalentCall(calls []core.ToolCall, candidate core.ToolCall) bool {
	for _, call := range calls {
		if call.ID == candidate.ID && call.Name == candidate.Name && call.Continuation == candidate.Continuation && sameArgs(call.Args, candidate.Args) {
			return true
		}
	}
	return false
}

func sameArgs(left, right map[string]any) bool {
	encodedLeft, leftErr := json.Marshal(left)
	encodedRight, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && string(encodedLeft) == string(encodedRight)
}

func newRouteProjectionConfig(options RouteProjectionOptions) (routeProjectionConfig, error) {
	if options.Mode != RouteAutoFirstAction && options.Mode != RouteAutoProbeOnce && options.Mode != RouteDirectOnly && options.Mode != RoutePTCOnly {
		return routeProjectionConfig{}, ErrInvalidRouteProjection
	}
	catalogID := options.CatalogToolID
	if catalogID == "" {
		catalogID = DefaultCatalogToolID
	}
	executeID := options.ExecuteToolID
	if executeID == "" {
		executeID = DefaultExecuteToolID
	}
	if catalogID == executeID || core.ValidateNamespacedID(catalogID) != nil || core.ValidateNamespacedID(executeID) != nil {
		return routeProjectionConfig{}, ErrInvalidRouteProjection
	}
	if options.Mode == RouteAutoProbeOnce && (options.ProbeToolClassifier == nil || options.ProbePlanVerifier == nil || options.ChoiceResultVerifier == nil || options.ChoiceAdmission == nil) {
		return routeProjectionConfig{}, ErrInvalidRouteProjection
	}
	return routeProjectionConfig{mode: options.Mode, catalogID: catalogID, executeID: executeID, verify: options.CatalogResultVerifier, verifyExecute: options.ExecuteToolVerifier, verifyProbe: options.ProbePlanVerifier, classifyProbe: options.ProbeToolClassifier, verifyChoice: options.ChoiceResultVerifier, admitChoice: options.ChoiceAdmission}, nil
}

type projectedRoute uint8

const (
	routeAuto projectedRoute = iota
	routeDirect
	routePTC
	routeFinal
	routePTCCatalog
	routeProbeInitial
	routeProbeChoice
)

func routeFromMessages(ctx context.Context, messages []core.ChatMessage, runID string, config routeProjectionConfig) (projectedRoute, error) {
	switch config.mode {
	case RouteDirectOnly:
		return routeFromTurn(ctx, messages, runID, config, routeDirect, false)
	case RoutePTCOnly:
		return routeFromTurn(ctx, messages, runID, config, routePTCCatalog, true)
	default:
		return routeFromTurn(ctx, messages, runID, config, routeAuto, true)
	}
}

type probeRouteState struct {
	route projectedRoute
	plan  ProbePlan
}

type probeRouteCallRecord struct {
	id, name  string
	args      map[string]any
	index     int
	batchSize int
}

// probeRouteFromMessages is the v2 reducer. It deliberately reads no model
// text and trusts a probe only when the host verifier can reconstruct an
// exact durable plan. The ordinary catalog/PTC path retains the v1 verifier.
func probeRouteFromMessages(ctx context.Context, messages []core.ChatMessage, runID string, config routeProjectionConfig) (probeRouteState, error) {
	state := probeRouteState{route: routeProbeInitial}
	start := -1
	for index, message := range messages {
		if message.Role == core.RoleUser {
			start = index
		}
	}
	if start < 0 {
		return state, nil
	}
	type resultRecord struct {
		content string
		index   int
		count   int
	}
	calls := make([]probeRouteCallRecord, 0)
	callCounts := make(map[string]int)
	results := make(map[string]resultRecord)
	for index, message := range messages[start+1:] {
		switch message.Role {
		case core.RoleAssistant:
			batch := assistantCalls(message)
			for _, call := range batch {
				if !validModelCallID(call.ID) {
					return probeRouteState{}, errors.New("model tool call identifier is invalid")
				}
				calls = append(calls, probeRouteCallRecord{id: call.ID, name: call.Name, args: call.Args, index: index, batchSize: len(batch)})
				callCounts[call.ID]++
			}
		case core.RoleTool:
			if !validModelCallID(message.ToolCallID) {
				return probeRouteState{}, errors.New("tool result identifier is invalid")
			}
			record := results[message.ToolCallID]
			record.content, record.index, record.count = message.Content, index, record.count+1
			results[message.ToolCallID] = record
		}
	}
	byID := make(map[string]probeRouteCallRecord, len(calls))
	for _, call := range calls {
		byID[call.id] = call
	}
	for id, result := range results {
		call, found := byID[id]
		if !found {
			if nestedProbeRouteResult(id, result.index, calls, config.executeID) {
				continue
			}
			return probeRouteState{}, errors.New("current turn tool evidence is incomplete or ambiguous")
		}
		if callCounts[id] != 1 || result.count != 1 || result.index <= call.index {
			return probeRouteState{}, errors.New("current turn tool evidence is incomplete or ambiguous")
		}
	}
	for callIndex, call := range calls {
		result, paired := results[call.id]
		if !paired || callCounts[call.id] != 1 || result.count != 1 || result.index <= call.index {
			return probeRouteState{}, errors.New("current turn tool evidence is incomplete or ambiguous")
		}

		switch state.route {
		case routeProbeInitial:
			if call.name == config.catalogID {
				if call.batchSize != 1 || !validCatalogResult(result.content) {
					return probeRouteState{}, errors.New("catalog action is mixed or invalid")
				}
				verified, err := verifiedCatalogResult(ctx, runID, call.id, config.verify)
				if err != nil || !verified {
					return probeRouteState{}, errors.New("catalog result is not durably verified")
				}
				state.route = routePTC
				continue
			}
			plan, isProbe, err := verifiedProbePlan(ctx, runID, call.id, config.verifyProbe)
			if err != nil {
				return probeRouteState{}, err
			}
			if !isProbe {
				state.route = routeDirect
				continue
			}
			if call.batchSize != 1 {
				return probeRouteState{}, errors.New("probe action is mixed")
			}
			if _, err := plan.ChoiceTools(); err != nil {
				return probeRouteState{}, err
			}
			state.route, state.plan = routeProbeChoice, plan
		case routeProbeChoice:
			if call.name == state.plan.executeID {
				if call.batchSize != 1 || state.plan.ValidateReturn(core.CapabilityResult{Content: result.content}) != nil {
					return probeRouteState{}, errors.New("execute action is mixed or its return is invalid")
				}
				verified, err := verifiedChoiceResult(ctx, runID, call.id, state.plan, ChoiceRouteExecute, config.verifyChoice)
				if err != nil || !verified {
					return probeRouteState{}, errors.New("execute result is not durably verified")
				}
				state.route = routeFinal
				continue
			}
			if call.name != state.plan.followupID || state.plan.ValidateDirect(call.args) != nil {
				return probeRouteState{}, errors.New("planned direct action is invalid")
			}
			for prior := callIndex - 1; prior >= 0 && calls[prior].index == call.index; prior-- {
				if calls[prior].name == call.name && sameArgs(calls[prior].args, call.args) {
					return probeRouteState{}, errors.New("planned direct action is duplicated")
				}
			}
			if callIndex+1 < len(calls) && calls[callIndex+1].index == call.index {
				continue
			}
			for prior := callIndex; prior >= 0 && calls[prior].index == call.index; prior-- {
				verified, err := verifiedChoiceResult(ctx, runID, calls[prior].id, state.plan, ChoiceRouteDirect, config.verifyChoice)
				if err != nil || !verified {
					return probeRouteState{}, errors.New("direct result is not durably verified")
				}
			}
			state.route = routeFinal
		case routeDirect:
			if call.name == config.catalogID || call.name == config.executeID {
				return probeRouteState{}, errors.New("direct route changed after selection")
			}
		case routePTC:
			if call.name != config.executeID {
				return probeRouteState{}, errors.New("PTC route changed after selection")
			}
			state.route = routeFinal
		case routeFinal:
			return probeRouteState{}, errors.New("tool action follows final route")
		}
	}
	return state, nil
}

// nestedProbeRouteResult recognizes only the deterministic public shape used
// by the protected program bridge: <top-level execute call id>/<sha256>. The
// authoritative choice verifier still proves the exact child event and
// journal chain before this history can unlock a final model call; this helper
// merely prevents a legitimate child result from being mistaken for an
// orphaned model-level call while reducing ChatMessage history.
func nestedProbeRouteResult(callID string, resultIndex int, calls []probeRouteCallRecord, executeID string) bool {
	for _, parent := range calls {
		if parent.name != executeID || resultIndex <= parent.index {
			continue
		}
		prefix := parent.id + "/"
		if !strings.HasPrefix(callID, prefix) {
			continue
		}
		digest := strings.TrimPrefix(callID, prefix)
		if len(digest) != 64 {
			continue
		}
		valid := true
		for _, char := range digest {
			if (char < '0' || char > '9') && (char < 'a' || char > 'f') {
				valid = false
				break
			}
		}
		if valid {
			return true
		}
	}
	return false
}

func verifiedProbePlan(ctx context.Context, runID, callID string, verify ProbePlanVerifier) (plan ProbePlan, isProbe bool, err error) {
	if verify == nil || strings.TrimSpace(runID) == "" {
		return ProbePlan{}, false, errors.New("probe verifier is unavailable")
	}
	defer func() {
		if recover() != nil {
			plan, isProbe, err = ProbePlan{}, false, errors.New("probe verifier panicked")
		}
	}()
	return verify(ctx, runID, callID)
}

func verifiedChoiceResult(ctx context.Context, runID, callID string, plan ProbePlan, kind ChoiceRouteKind, verify ChoiceResultVerifier) (ok bool, err error) {
	if verify == nil || strings.TrimSpace(runID) == "" || strings.TrimSpace(callID) == "" || !plan.valid() || (kind != ChoiceRouteDirect && kind != ChoiceRouteExecute) {
		return false, errors.New("choice result verifier is unavailable")
	}
	defer func() {
		if recover() != nil {
			ok, err = false, errors.New("choice result verifier panicked")
		}
	}()
	return verify(ctx, runID, callID, plan, kind)
}

func classifiedProbeTool(ctx context.Context, runID, toolName string, classify ProbeToolClassifier) (probe bool, err error) {
	if classify == nil || strings.TrimSpace(runID) == "" || strings.TrimSpace(toolName) == "" {
		return false, errors.New("probe classifier is unavailable")
	}
	defer func() {
		if recover() != nil {
			probe, err = false, errors.New("probe classifier panicked")
		}
	}()
	return classify(ctx, runID, toolName)
}

// routeFromTurn reads only the current turn. Strict catalog evidence is
// required only for auto/PTC modes: direct-only deliberately ignores catalog
// history so a stale or malformed catalog record cannot change its surface.
func routeFromTurn(ctx context.Context, messages []core.ChatMessage, runID string, config routeProjectionConfig, initial projectedRoute, strictCatalog bool) (projectedRoute, error) {
	start := -1
	for index, message := range messages {
		if message.Role == core.RoleUser {
			start = index
		}
	}
	if start < 0 {
		return initial, nil
	}
	turn := messages[start+1:]
	type callRecord struct {
		id, name  string
		index     int
		batchSize int
	}
	type resultRecord struct {
		content string
		index   int
		count   int
	}
	calls := make([]callRecord, 0)
	callCounts := make(map[string]int)
	results := make(map[string]resultRecord)
	for index, message := range turn {
		if message.Role == core.RoleAssistant {
			assistantBatch := assistantCalls(message)
			for _, call := range assistantBatch {
				if !validModelCallID(call.ID) {
					if strictCatalog {
						return initial, errors.New("model tool call identifier is invalid")
					}
					continue
				}
				calls = append(calls, callRecord{id: call.ID, name: call.Name, index: index, batchSize: len(assistantBatch)})
				callCounts[call.ID]++
			}
			continue
		}
		if message.Role == core.RoleTool {
			if !validModelCallID(message.ToolCallID) {
				if strictCatalog {
					return initial, errors.New("tool result identifier is invalid")
				}
				continue
			}
			record := results[message.ToolCallID]
			record.count++
			record.content, record.index = message.Content, index
			results[message.ToolCallID] = record
		}
	}

	byID := make(map[string]callRecord, len(calls))
	for _, call := range calls {
		byID[call.id] = call
	}
	if strictCatalog {
		for id, result := range results {
			call, found := byID[id]
			if !found || callCounts[id] != 1 || result.count != 1 || result.index <= call.index {
				return initial, errors.New("current turn tool evidence is incomplete or ambiguous")
			}
		}
		for _, call := range calls {
			result, paired := results[call.id]
			if !paired || callCounts[call.id] != 1 || result.count != 1 || result.index <= call.index {
				return initial, errors.New("current turn tool evidence is incomplete or ambiguous")
			}
		}
	}
	// A completed, unambiguous execute is terminal. It must win over a
	// previously completed catalog in the same turn, which is normal after the
	// next model call has already selected execute.
	for _, call := range calls {
		result, paired := results[call.id]
		if call.name == config.executeID && paired && callCounts[call.id] == 1 && result.count == 1 && result.index > call.index {
			return routeFinal, nil
		}
	}

	route := initial
	catalogSeen := false
	for _, call := range calls {
		result, paired := results[call.id]
		if !paired || callCounts[call.id] != 1 || result.count != 1 || result.index <= call.index {
			if strictCatalog && call.name == config.catalogID {
				return initial, errors.New("catalog evidence is incomplete or ambiguous")
			}
			continue
		}
		if route != routeAuto && route != routePTCCatalog {
			continue
		}
		switch call.name {
		case config.catalogID:
			if catalogSeen || (strictCatalog && call.batchSize != 1) {
				return initial, errors.New("catalog action is mixed or duplicated")
			}
			catalogSeen = true
			if !strictCatalog {
				continue
			}
			if !validCatalogResult(result.content) {
				return initial, errors.New("catalog result is invalid")
			}
			verified, verifyErr := verifiedCatalogResult(ctx, runID, call.id, config.verify)
			if verifyErr != nil || !verified {
				return initial, errors.New("catalog result is not durably verified")
			}
			route = routePTC
		default:
			if route == routeAuto {
				route = routeDirect
			}
		}
	}
	return route, nil
}

func verifiedCatalogResult(ctx context.Context, runID, callID string, verify CatalogResultVerifier) (ok bool, err error) {
	if verify == nil || strings.TrimSpace(runID) == "" {
		return false, errors.New("catalog verifier is unavailable")
	}
	defer func() {
		if recover() != nil {
			ok, err = false, errors.New("catalog verifier panicked")
		}
	}()
	return verify(ctx, runID, callID)
}

func assistantCalls(message core.ChatMessage) []core.ToolCall {
	if len(message.ToolCalls) != 0 {
		return message.ToolCalls
	}
	if message.ToolCall != nil {
		return []core.ToolCall{*message.ToolCall}
	}
	return nil
}

func validModelCallID(id string) bool {
	return strings.TrimSpace(id) != "" && len(id) <= 256 && !strings.ContainsAny(id, "\r\n\x00")
}

// validCatalogResult mirrors the public catalog envelope only. Parsed content
// is short-lived: no catalog data, schema, binding, or user content is kept by
// the projection component.
func validCatalogResult(content string) bool {
	// This is the public program.catalog output ceiling. Refuse oversized text
	// before decoding; route selection never needs to retain a catalog body.
	if len(content) > maxCatalogResultSize {
		return false
	}
	var envelope struct {
		Version  string          `json:"version"`
		Language string          `json:"language"`
		Tools    json.RawMessage `json:"tools"`
	}
	if json.Unmarshal([]byte(content), &envelope) != nil || envelope.Version != ptcCatalogVersion || strings.TrimSpace(envelope.Language) == "" || len(envelope.Tools) == 0 {
		return false
	}
	var tools []json.RawMessage
	if json.Unmarshal(envelope.Tools, &tools) != nil || len(tools) == 0 {
		return false
	}
	for _, raw := range tools {
		var descriptor map[string]json.RawMessage
		if json.Unmarshal(raw, &descriptor) != nil || descriptor == nil {
			return false
		}
	}
	return true
}

func projectRouteTools(ctx context.Context, runID string, tools []core.ToolSchema, route projectedRoute, config routeProjectionConfig) ([]core.ToolSchema, error) {
	cloned := append([]core.ToolSchema(nil), tools...)
	out := make([]core.ToolSchema, 0, len(cloned))
	for _, tool := range cloned {
		keep := false
		switch route {
		case routeDirect:
			if tool.Name != config.catalogID && tool.Name != config.executeID {
				keep = true
			}
		case routePTC:
			if tool.Name == config.executeID && verifiedExecuteTool(ctx, runID, tool, config.verifyExecute) {
				keep = true
			}
		case routeFinal:
			// No tool can be retried after an execute result of unknown effect.
		case routePTCCatalog:
			if tool.Name == config.catalogID {
				keep = true
			}
		default: // auto first action: catalog plus ordinary direct tools.
			if tool.Name != config.executeID {
				keep = true
			}
		}
		if keep {
			clonedTool, err := cloneProjectedToolSchema(tool)
			if err != nil {
				return nil, err
			}
			out = append(out, clonedTool)
		}
	}
	return out, nil
}

func projectProbeRouteTools(ctx context.Context, runID string, tools []core.ToolSchema, route projectedRoute, plan ProbePlan, config routeProjectionConfig) ([]core.ToolSchema, error) {
	if route != routeProbeChoice {
		if route == routeProbeInitial {
			return projectRouteTools(ctx, runID, tools, routeAuto, config)
		}
		if route == routeDirect {
			out := make([]core.ToolSchema, 0, len(tools))
			for _, tool := range tools {
				if tool.Name == config.catalogID || tool.Name == config.executeID {
					continue
				}
				probe, err := classifiedProbeTool(ctx, runID, tool.Name, config.classifyProbe)
				if err != nil {
					return nil, ErrInvalidRouteProjection
				}
				if probe {
					continue
				}
				cloned, err := cloneProjectedToolSchema(tool)
				if err != nil {
					return nil, err
				}
				out = append(out, cloned)
			}
			return out, nil
		}
		return projectRouteTools(ctx, runID, tools, route, config)
	}
	choice, err := plan.ChoiceTools()
	if err != nil || len(choice) == 0 || !hasUniqueTool(tools, choice[0].Name) || !hasExactUniqueTool(tools, plan.directSourceSchema) {
		return nil, ErrInvalidRouteProjection
	}
	if len(choice) == 2 && (choice[1].Name != config.executeID || !hasUniqueTool(tools, config.executeID)) {
		return nil, ErrInvalidRouteProjection
	}
	if len(choice) > 2 {
		return nil, ErrInvalidRouteProjection
	}
	out := make([]core.ToolSchema, 0, len(choice))
	for _, tool := range choice {
		cloned, err := cloneProjectedToolSchema(tool)
		if err != nil {
			return nil, err
		}
		out = append(out, cloned)
	}
	return out, nil
}

// hasExactUniqueTool verifies that Core's frozen direct schema has not drifted
// before ProbePlan replaces its model-facing description with host-owned route
// guidance. ProbePlan.ValidateDirect separately validates exact candidates.
func hasExactUniqueTool(tools []core.ToolSchema, want core.ToolSchema) bool {
	count := 0
	for _, tool := range tools {
		if tool.Name == want.Name && reflect.DeepEqual(tool, want) {
			count++
		}
	}
	return count == 1
}

func hasUniqueTool(tools []core.ToolSchema, name string) bool {
	count := 0
	for _, tool := range tools {
		if tool.Name == name {
			count++
		}
	}
	return count == 1
}

func cloneProjectedToolSchema(tool core.ToolSchema) (core.ToolSchema, error) {
	if tool.Parameters == nil {
		return tool, nil
	}
	encoded, err := json.Marshal(tool.Parameters)
	if err != nil || len(encoded) > maxProjectedSchemaBytes || projectedJSONDepth(encoded) > maxProjectedSchemaDepth {
		return core.ToolSchema{}, ErrInvalidRouteProjection
	}
	var parameters map[string]any
	if err := json.Unmarshal(encoded, &parameters); err != nil || parameters == nil {
		return core.ToolSchema{}, ErrInvalidRouteProjection
	}
	tool.Parameters = parameters
	return tool, nil
}

func projectedJSONDepth(encoded []byte) int {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	depth, maximum := 0, 0
	for {
		token, err := decoder.Token()
		if err == io.EOF {
			return maximum
		}
		if err != nil {
			return maxProjectedSchemaDepth + 1
		}
		if delimiter, ok := token.(json.Delim); ok {
			switch delimiter {
			case '{', '[':
				depth++
				if depth > maximum {
					maximum = depth
				}
			case '}', ']':
				depth--
			}
		}
	}
}

func verifiedExecuteTool(ctx context.Context, runID string, tool core.ToolSchema, verify ExecuteToolVerifier) (ok bool) {
	if verify == nil || strings.TrimSpace(runID) == "" {
		return false
	}
	defer func() {
		if recover() != nil {
			ok = false
		}
	}()
	ok, err := verify(ctx, runID, tool)
	return err == nil && ok
}
