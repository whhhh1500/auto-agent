// Package executionroute selects and freezes the application-level run
// executor for a single run. It is internal because the metadata contract is
// shared by the server and evaluator, not a new public core extension point.
package executionroute

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/whhhh1500/auto-agent/pkg/app/programmatic"
	"github.com/whhhh1500/auto-agent/pkg/app/runexecutor"
	"github.com/whhhh1500/auto-agent/pkg/core"
)

const (
	ExecutorIDKey             = "harness.executor.id"
	ExecutorVersionKey        = "harness.executor.version"
	ExecutorImplementationKey = "harness.executor.implementation_revision"

	// RouteVersionKey through RouteImplementationKey freeze the model-facing
	// tool projection independently of the executor. They are deliberately
	// internal application metadata rather than a core extension point.
	RouteVersionKey        = "harness.programmatic.route.version"
	RouteModeKey           = "harness.programmatic.route.mode"
	RouteCatalogToolIDKey  = "harness.programmatic.route.catalog_tool_id"
	RouteExecuteToolIDKey  = "harness.programmatic.route.execute_tool_id"
	RouteImplementationKey = "harness.programmatic.route.implementation_revision"

	RouteVersion                = "1"
	RouteImplementationRevision = "programmatic-route-projection/v1"

	// RouteProbeVersion is the opt-in neutral-probe routing protocol. Keeping a
	// distinct version and implementation revision prevents old runs from being
	// reinterpreted when the new state machine is enabled.
	RouteProbeVersion                = "2"
	RouteProbeImplementationRevision = "programmatic-route-projection/v2-probe-once-host-projected-ptc-choice-capacity-admission"
)

var ErrSelection = errors.New("run executor selection failed")

// Request contains the already selected runtime and any caller-owned durable
// metadata (for example a canary assignment). Resolve copies BaseMetadata; it
// never retains a caller map.
type Request struct {
	Runtime      *core.Runtime
	Registry     *runexecutor.Registry
	Principal    core.Principal
	Session      *core.Session
	RunID        string
	Resume       bool
	BaseMetadata map[string]string
}

// Decision is the exact executor selection and metadata that must be passed to
// the executor. Core persists this metadata in the existing run composition.
type Decision struct {
	Executor            runexecutor.RunExecutor
	Metadata            runexecutor.Metadata
	CompositionMetadata map[string]string
}

// Resolve chooses an executor from the profile for a new run, or from durable
// run/start evidence for a resume. Explicit selections always resolve exactly;
// in particular an unregistered CodePTC reservation cannot fall back.
func Resolve(_ context.Context, request Request) (Decision, error) {
	if request.Runtime == nil || request.Runtime.Profiles == nil || request.Registry == nil || request.Session == nil {
		return Decision{}, fmt.Errorf("%w: incomplete dependencies", ErrSelection)
	}
	if err := core.ValidateRunID(request.RunID); err != nil {
		return Decision{}, fmt.Errorf("%w: %v", ErrSelection, err)
	}
	selection, err := selectExecutor(request)
	if err != nil {
		return Decision{}, fmt.Errorf("%w: %w", ErrSelection, err)
	}
	route, err := selectRoute(request)
	if err != nil {
		return Decision{}, fmt.Errorf("%w: %w", ErrSelection, err)
	}
	runtime, err := runtimeWithRouteProjection(request.Runtime, request.Session, request.Principal, request.RunID, request.Resume, route)
	if err != nil {
		return Decision{}, fmt.Errorf("%w: %w", ErrSelection, err)
	}
	executor, metadata, err := request.Registry.ResolveOrDefault(selection.id, selection.version, runexecutor.Dependencies{Runtime: runtime})
	if err != nil {
		return Decision{}, fmt.Errorf("%w: %w", ErrSelection, err)
	}
	if selection.implementation != "" && selection.implementation != metadata.ImplementationRevision {
		return Decision{}, fmt.Errorf("%w: implementation revision drift", ErrSelection)
	}
	composition := cloneMetadata(request.BaseMetadata)
	if composition == nil {
		composition = map[string]string{}
	}
	composition[ExecutorIDKey] = metadata.ID
	composition[ExecutorVersionKey] = metadata.Version
	composition[ExecutorImplementationKey] = metadata.ImplementationRevision
	route.writeMetadata(composition)
	if err := core.ValidateRunCompositionMetadata(composition); err != nil {
		return Decision{}, fmt.Errorf("%w: metadata: %w", ErrSelection, err)
	}
	return Decision{Executor: executor, Metadata: metadata, CompositionMetadata: composition}, nil
}

type selection struct{ id, version, implementation string }

func selectExecutor(request Request) (selection, error) {
	if request.Resume {
		return frozenSelection(request.Session, request.RunID)
	}
	profile, err := request.Runtime.Profiles.Resolve(request.Principal, request.Session.Scope(), request.Session.ProfileID())
	if err != nil {
		return selection{}, err
	}
	id, hasID := profile.Metadata[ExecutorIDKey]
	version, hasVersion := profile.Metadata[ExecutorVersionKey]
	if hasID != hasVersion {
		return selection{}, errors.New("executor id and version must be configured together")
	}
	if !hasID {
		return selection{}, nil
	}
	if id == "" || version == "" {
		return selection{}, errors.New("executor id and version cannot be empty")
	}
	return selection{id: id, version: version}, nil
}

func frozenSelection(session *core.Session, runID string) (selection, error) {
	var found *selection
	for _, event := range session.Events() {
		if event.Type != core.EvRunStart || event.RunID != runID {
			continue
		}
		var data core.RunStartData
		if err := json.Unmarshal(event.Data, &data); err != nil {
			return selection{}, err
		}
		metadata := map[string]string{}
		if data.Composition != nil {
			metadata = data.Composition.Metadata
		}
		id, hasID := metadata[ExecutorIDKey]
		version, hasVersion := metadata[ExecutorVersionKey]
		implementation, hasImplementation := metadata[ExecutorImplementationKey]
		if !hasID && !hasVersion && !hasImplementation {
			continue
		}
		if !hasID || !hasVersion || !hasImplementation || id == "" || version == "" || implementation == "" {
			return selection{}, errors.New("run start has incomplete executor evidence")
		}
		candidate := selection{id: id, version: version, implementation: implementation}
		if found != nil && *found != candidate {
			return selection{}, errors.New("run start executor evidence changed")
		}
		found = &candidate
	}
	if found == nil {
		// Runs created before executor evidence retain the sequential contract.
		return selection{}, nil
	}
	return *found, nil
}

type routeSelection struct {
	version, catalogID, executeID, implementation string
	mode                                          programmatic.RouteMode
}

func defaultRouteSelection() routeSelection {
	return routeSelection{
		version: RouteVersion, mode: programmatic.RouteDirectOnly,
		catalogID: programmatic.DefaultCatalogToolID, executeID: programmatic.DefaultExecuteToolID,
		implementation: RouteImplementationRevision,
	}
}

func selectRoute(request Request) (routeSelection, error) {
	if request.Resume {
		return frozenRouteSelection(request.Session, request.RunID)
	}
	profile, err := request.Runtime.Profiles.Resolve(request.Principal, request.Session.Scope(), request.Session.ProfileID())
	if err != nil {
		return routeSelection{}, err
	}
	return configuredRouteSelection(profile.Metadata)
}

func configuredRouteSelection(metadata map[string]string) (routeSelection, error) {
	keys := []string{RouteVersionKey, RouteModeKey, RouteCatalogToolIDKey, RouteExecuteToolIDKey, RouteImplementationKey}
	present := 0
	for _, key := range keys {
		if _, ok := metadata[key]; ok {
			present++
		}
	}
	if present == 0 {
		return defaultRouteSelection(), nil
	}
	version, hasVersion := metadata[RouteVersionKey]
	mode, hasMode := metadata[RouteModeKey]
	if !hasVersion || !hasMode {
		return routeSelection{}, errors.New("programmatic route version and mode must be configured together")
	}
	selection := defaultRouteSelection()
	selection.version = version
	selection.mode = programmatic.RouteMode(mode)
	switch version {
	case RouteVersion:
		if !routeModeSupported(version, selection.mode) {
			return routeSelection{}, errors.New("programmatic route v1 mode is unknown")
		}
	case RouteProbeVersion:
		if !routeModeSupported(version, selection.mode) {
			return routeSelection{}, errors.New("programmatic route v2 requires auto_probe_once")
		}
		// v2 has no migration registry. Every retired implementation revision
		// stays unsupported until an explicit, audited migration is added.
		selection.implementation = RouteProbeImplementationRevision
	default:
		return routeSelection{}, errors.New("programmatic route version is unknown")
	}
	if catalog, ok := metadata[RouteCatalogToolIDKey]; ok {
		selection.catalogID = catalog
	}
	if execute, ok := metadata[RouteExecuteToolIDKey]; ok {
		selection.executeID = execute
	}
	if selection.catalogID == "" || selection.executeID == "" || selection.catalogID == selection.executeID ||
		core.ValidateNamespacedID(selection.catalogID) != nil || core.ValidateNamespacedID(selection.executeID) != nil {
		return routeSelection{}, errors.New("programmatic route tool identifiers are invalid")
	}
	if implementation, ok := metadata[RouteImplementationKey]; ok && implementation != selection.implementation {
		return routeSelection{}, errors.New("programmatic route implementation revision is unknown")
	}
	return selection, nil
}

func frozenRouteSelection(session *core.Session, runID string) (routeSelection, error) {
	var found *routeSelection
	for _, event := range session.Events() {
		if event.Type != core.EvRunStart || event.RunID != runID {
			continue
		}
		var data core.RunStartData
		if err := json.Unmarshal(event.Data, &data); err != nil {
			return routeSelection{}, fmt.Errorf("decode frozen programmatic route: %w", err)
		}
		if err := validateRouteCompositionEvidence(data.Composition, data.CompositionRevision, data.AssignmentRevision); err != nil {
			return routeSelection{}, fmt.Errorf("invalid frozen programmatic route composition: %w", err)
		}
		metadata := map[string]string{}
		if data.Composition != nil {
			metadata = data.Composition.Metadata
		}
		candidate, err := frozenRouteMetadata(metadata)
		if err != nil {
			return routeSelection{}, err
		}
		if found != nil && *found != candidate {
			return routeSelection{}, errors.New("run start programmatic route evidence changed")
		}
		found = &candidate
	}
	if found == nil {
		// Older runs that predate route evidence retain the direct-only surface.
		return defaultRouteSelection(), nil
	}
	return *found, nil
}

func frozenRouteMetadata(metadata map[string]string) (routeSelection, error) {
	keys := []string{RouteVersionKey, RouteModeKey, RouteCatalogToolIDKey, RouteExecuteToolIDKey, RouteImplementationKey}
	present := 0
	for _, key := range keys {
		if _, ok := metadata[key]; ok {
			present++
		}
	}
	if present == 0 {
		return defaultRouteSelection(), nil
	}
	if present != len(keys) {
		return routeSelection{}, errors.New("run start has incomplete programmatic route evidence")
	}
	selection, err := configuredRouteSelection(metadata)
	if err != nil {
		return routeSelection{}, fmt.Errorf("invalid frozen programmatic route: %w", err)
	}
	if selection.implementation != implementationForRouteVersion(selection.version) {
		return routeSelection{}, errors.New("programmatic route implementation revision drift")
	}
	return selection, nil
}

func (selection routeSelection) writeMetadata(metadata map[string]string) {
	metadata[RouteVersionKey] = selection.version
	metadata[RouteModeKey] = string(selection.mode)
	metadata[RouteCatalogToolIDKey] = selection.catalogID
	metadata[RouteExecuteToolIDKey] = selection.executeID
	metadata[RouteImplementationKey] = selection.implementation
}

func (selection routeSelection) matchesMetadata(metadata map[string]string) bool {
	if metadata == nil {
		return false
	}
	return metadata[RouteVersionKey] == selection.version && metadata[RouteModeKey] == string(selection.mode) &&
		metadata[RouteCatalogToolIDKey] == selection.catalogID && metadata[RouteExecuteToolIDKey] == selection.executeID &&
		metadata[RouteImplementationKey] == selection.implementation
}

func implementationForRouteVersion(version string) string {
	switch version {
	case RouteVersion:
		return RouteImplementationRevision
	case RouteProbeVersion:
		return RouteProbeImplementationRevision
	default:
		return ""
	}
}

// SupportsRouteIdentity reports whether the version, mode and implementation
// tuple names one frozen route protocol supported by this package. It performs
// no execution, authorization or evidence verification.
func SupportsRouteIdentity(version, mode, implementation string) bool {
	return implementation != "" && implementation == implementationForRouteVersion(version) &&
		routeModeSupported(version, programmatic.RouteMode(mode))
}

func routeModeSupported(version string, mode programmatic.RouteMode) bool {
	switch version {
	case RouteVersion:
		return mode == programmatic.RouteAutoFirstAction || mode == programmatic.RouteDirectOnly || mode == programmatic.RoutePTCOnly
	case RouteProbeVersion:
		return mode == programmatic.RouteAutoProbeOnce
	default:
		return false
	}
}

func runtimeWithRouteProjection(runtime *core.Runtime, session *core.Session, principal core.Principal, runID string, resume bool, route routeSelection) (*core.Runtime, error) {
	if runtime == nil || runtime.Models == nil || session == nil {
		return nil, errors.New("programmatic route runtime is incomplete")
	}
	var reader core.ToolInvocationReader
	if route.mode == programmatic.RouteAutoFirstAction || route.mode == programmatic.RoutePTCOnly || route.mode == programmatic.RouteAutoProbeOnce {
		var ok bool
		reader, ok = runtime.ToolJournal.(core.ToolInvocationReader)
		if !ok || reader == nil {
			return nil, errors.New("programmatic route requires a readable tool journal")
		}
	}
	options := programmatic.RouteProjectionOptions{
		Mode: route.mode, CatalogToolID: route.catalogID, ExecuteToolID: route.executeID,
	}
	modelResolver := runtime.Models
	if reader != nil {
		options.CatalogResultVerifier = func(ctx context.Context, runID, callID string) (bool, error) {
			return verifyCatalogResult(ctx, session, principal, reader, runID, route.catalogID, callID)
		}
		options.ExecuteToolVerifier = func(ctx context.Context, runID string, schema core.ToolSchema) (bool, error) {
			return verifyExecuteTool(session, runID, route, schema)
		}
		if route.mode == programmatic.RouteAutoProbeOnce {
			options.ProbeToolClassifier = func(_ context.Context, runID, toolName string) (bool, error) {
				return IsNeutralProbe(session, principal, runID, toolName)
			}
			options.ProbePlanVerifier = func(ctx context.Context, runID, callID string) (programmatic.ProbePlan, bool, error) {
				return ResolveProbePlan(ctx, session, principal, reader, runID, callID)
			}
			options.ChoiceResultVerifier = func(ctx context.Context, runID, callID string, plan programmatic.ProbePlan, kind programmatic.ChoiceRouteKind) (bool, error) {
				return VerifyChoiceResult(ctx, session, principal, reader, runID, callID, plan, kind)
			}
			options.ChoiceAdmission = func(ctx context.Context, choiceRunID string, plan programmatic.ProbePlan, kind programmatic.ChoiceRouteKind, calls []core.ToolCall) error {
				return admitV2Choice(ctx, session, principal, choiceRunID, plan, kind, calls)
			}
		}
	}
	if route.mode == programmatic.RouteAutoProbeOnce {
		modelResolver = newV2RouteAdmissionResolver(runtime, session, principal, runID, resume, modelResolver)
	}
	resolver, err := programmatic.NewModelResolver(modelResolver, options)
	if err != nil {
		return nil, err
	}
	wrapped := *runtime
	wrapped.Models = resolver
	return &wrapped, nil
}

type catalogEvidence struct {
	call      core.ToolCall
	assistant int64
	toolCall  int64
	result    int64
	output    core.CapabilityResult
}

func verifyCatalogResult(ctx context.Context, session *core.Session, principal core.Principal, reader core.ToolInvocationReader, runID, catalogID, callID string) (ok bool, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			ok, err = false, fmt.Errorf("catalog verifier panicked: %T", recovered)
		}
	}()
	evidence, err := catalogEvidenceFor(session.Events(), runID, catalogID, callID)
	if err != nil {
		return false, err
	}
	invocation, err := core.NewToolInvocation(core.RunInfo{
		RunID: runID, SessionID: session.ID(), ProfileID: session.ProfileID(), Principal: principal,
	}, evidence.call, true)
	if err != nil {
		return false, err
	}
	record, found, err := reader.GetToolInvocation(ctx, invocation)
	if err != nil || !found || record.ToolInvocation != invocation || record.State != core.ToolInvocationCompleted || record.Result == nil {
		if err != nil {
			return false, err
		}
		return false, errors.New("catalog invocation is not durably completed")
	}
	if !canonicalResultEqual(*record.Result, evidence.output) {
		return false, errors.New("catalog journal result differs from event result")
	}
	return true, nil
}

func catalogEvidenceFor(events []core.SessionEvent, runID, catalogID, callID string) (catalogEvidence, error) {
	if err := core.ValidateRunID(runID); err != nil {
		return catalogEvidence{}, err
	}
	var catalog *catalogEvidence
	assistantCalls := map[string]catalogEvidence{}
	toolCalls := map[string]catalogEvidence{}
	results := map[string]catalogEvidence{}
	for _, event := range events {
		if event.RunID != runID {
			continue
		}
		switch event.Type {
		case core.EvAssistantMessage:
			var data core.AssistantMessageData
			if err := json.Unmarshal(event.Data, &data); err != nil {
				return catalogEvidence{}, fmt.Errorf("decode assistant tool evidence: %w", err)
			}
			calls, err := strictAssistantCalls(data)
			if err != nil {
				return catalogEvidence{}, err
			}
			for _, call := range calls {
				if _, exists := assistantCalls[call.ID]; exists {
					return catalogEvidence{}, errors.New("duplicate assistant tool call evidence")
				}
				record := catalogEvidence{call: call, assistant: event.Seq}
				assistantCalls[call.ID] = record
				if call.Name == catalogID {
					if catalog != nil {
						return catalogEvidence{}, errors.New("duplicate catalog assistant evidence")
					}
					copyOf := record
					catalog = &copyOf
				}
			}
		case core.EvToolCall:
			var data core.ToolCallData
			if err := json.Unmarshal(event.Data, &data); err != nil {
				return catalogEvidence{}, fmt.Errorf("decode tool call evidence: %w", err)
			}
			if _, exists := toolCalls[data.CallID]; exists {
				return catalogEvidence{}, errors.New("duplicate tool call evidence")
			}
			assistant, found := assistantCalls[data.CallID]
			if !found || assistant.assistant >= event.Seq || assistant.call.Name != data.Name || !sameArgs(assistant.call.Args, data.Args) {
				return catalogEvidence{}, errors.New("orphan or mismatched tool call evidence")
			}
			assistant.toolCall = event.Seq
			toolCalls[data.CallID] = assistant
		case core.EvToolResult:
			var data core.ToolResultData
			if err := json.Unmarshal(event.Data, &data); err != nil {
				return catalogEvidence{}, fmt.Errorf("decode tool result evidence: %w", err)
			}
			if _, exists := results[data.CallID]; exists {
				return catalogEvidence{}, errors.New("duplicate tool result evidence")
			}
			call, found := toolCalls[data.CallID]
			if !found || call.toolCall >= event.Seq {
				return catalogEvidence{}, errors.New("orphan tool result evidence")
			}
			call.result = event.Seq
			call.output = core.CapabilityResult{Content: data.Content, OK: data.OK, Metadata: data.Metadata}
			results[data.CallID] = call
		}
	}
	if catalog == nil || catalog.call.Name != catalogID || (callID != "" && catalog.call.ID != callID) {
		return catalogEvidence{}, errors.New("catalog assistant evidence is missing or changed")
	}
	if callID == "" {
		callID = catalog.call.ID
	}
	completed, found := results[callID]
	if !found || completed.assistant != catalog.assistant || completed.toolCall == 0 || completed.result == 0 || !completed.output.OK {
		return catalogEvidence{}, errors.New("catalog result evidence is incomplete or unsuccessful")
	}
	return completed, nil
}

func strictAssistantCalls(data core.AssistantMessageData) ([]core.ToolCall, error) {
	calls := append([]core.ToolCall(nil), data.ToolCalls...)
	if data.ToolCall != nil {
		if len(calls) == 0 {
			calls = []core.ToolCall{*data.ToolCall}
		} else if !sameToolCall(calls[0], *data.ToolCall) {
			return nil, errors.New("assistant legacy tool call differs from tool call list")
		}
	}
	seen := make(map[string]bool, len(calls))
	for _, call := range calls {
		if call.ID == "" || call.Name == "" || seen[call.ID] {
			return nil, errors.New("assistant tool call is invalid or duplicated")
		}
		seen[call.ID] = true
		if _, err := json.Marshal(call.Args); err != nil {
			return nil, fmt.Errorf("encode assistant tool arguments: %w", err)
		}
	}
	return calls, nil
}

func sameToolCall(left, right core.ToolCall) bool {
	return left.ID == right.ID && left.Name == right.Name && left.Continuation == right.Continuation && sameArgs(left.Args, right.Args)
}

func sameArgs(left, right map[string]any) bool {
	encodedLeft, leftErr := json.Marshal(left)
	encodedRight, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && bytes.Equal(encodedLeft, encodedRight)
}

func canonicalResultEqual(left, right core.CapabilityResult) bool {
	encodedLeft, leftErr := json.Marshal(left)
	encodedRight, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && bytes.Equal(encodedLeft, encodedRight)
}

func verifyExecuteTool(session *core.Session, runID string, route routeSelection, current core.ToolSchema) (ok bool, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			ok, err = false, fmt.Errorf("execute verifier panicked: %T", recovered)
		}
	}()
	events := session.Events()
	catalog, err := catalogEvidenceFor(events, runID, route.catalogID, "")
	if err != nil {
		return false, fmt.Errorf("catalog composition evidence is unavailable: %w", err)
	}
	catalogComposition, err := runCompositionBefore(events, runID, catalog.assistant)
	if err != nil {
		return false, fmt.Errorf("catalog composition is unavailable: %w", err)
	}
	composition, err := latestRunComposition(events, runID)
	if err != nil {
		return false, err
	}
	catalogRevision, err := core.CompositionRevision(catalogComposition)
	if err != nil {
		return false, err
	}
	currentRevision, err := core.CompositionRevision(composition)
	if err != nil {
		return false, err
	}
	if catalogRevision != currentRevision {
		return false, errors.New("current composition differs from the catalog composition")
	}
	if !route.matchesMetadata(composition.Metadata) {
		return false, errors.New("current composition route metadata differs from frozen route")
	}
	var expected *core.ToolSchema
	for _, capability := range composition.Capabilities {
		if capability.Manifest.ID != route.executeID {
			continue
		}
		if expected != nil {
			return false, errors.New("current composition has duplicate execute capability")
		}
		if capability.Manifest.Tool == nil {
			return false, errors.New("current composition execute capability is not a tool")
		}
		schema := schemaFromManifest(capability.Manifest)
		expected = &schema
	}
	if expected == nil || !reflect.DeepEqual(*expected, current) {
		return false, errors.New("current composition execute schema drifted or was revoked")
	}
	return true, nil
}

func latestRunComposition(events []core.SessionEvent, runID string) (*core.RunCompositionData, error) {
	return runCompositionBefore(events, runID, 0)
}

// runCompositionBefore returns the last validated composition strictly before
// beforeSeq. A zero cutoff selects the latest composition in the run.
func runCompositionBefore(events []core.SessionEvent, runID string, beforeSeq int64) (*core.RunCompositionData, error) {
	if err := core.ValidateRunID(runID); err != nil {
		return nil, err
	}
	var latest *core.RunCompositionData
	for _, event := range events {
		if event.RunID != runID || (event.Type != core.EvRunStart && event.Type != core.EvRunResume) {
			continue
		}
		if beforeSeq > 0 && event.Seq >= beforeSeq {
			continue
		}
		var composition *core.RunCompositionData
		var compositionRevision, assignmentRevision string
		if event.Type == core.EvRunStart {
			var data core.RunStartData
			if err := json.Unmarshal(event.Data, &data); err != nil {
				return nil, fmt.Errorf("decode run start composition: %w", err)
			}
			composition, compositionRevision, assignmentRevision = data.Composition, data.CompositionRevision, data.AssignmentRevision
		} else {
			var data core.RunResumeData
			if err := json.Unmarshal(event.Data, &data); err != nil {
				return nil, fmt.Errorf("decode run resume composition: %w", err)
			}
			composition, compositionRevision, assignmentRevision = data.Composition, data.CompositionRevision, data.AssignmentRevision
		}
		if err := validateRouteCompositionEvidence(composition, compositionRevision, assignmentRevision); err != nil {
			return nil, err
		}
		if composition == nil {
			return nil, errors.New("current composition is missing")
		}
		copyOf := *composition
		latest = &copyOf
	}
	if latest == nil {
		return nil, errors.New("current composition is missing")
	}
	return latest, nil
}

func validateRouteCompositionEvidence(composition *core.RunCompositionData, compositionRevision, assignmentRevision string) error {
	if composition == nil {
		if compositionRevision != "" || assignmentRevision != "" {
			return errors.New("composition revisions require a composition")
		}
		return nil
	}
	if err := core.ValidateRunCompositionMetadata(composition.Metadata); err != nil {
		return err
	}
	calculatedComposition, err := core.CompositionRevision(composition)
	if err != nil {
		return err
	}
	calculatedAssignment, err := core.CompositionMetadataRevision(composition.Metadata)
	if err != nil {
		return err
	}
	if (compositionRevision != "" && compositionRevision != calculatedComposition) || (assignmentRevision != "" && assignmentRevision != calculatedAssignment) {
		return errors.New("composition revision evidence drifted")
	}
	return nil
}

func schemaFromManifest(manifest core.CapabilityManifest) core.ToolSchema {
	description := manifest.Tool.Description
	if description == "" {
		description = manifest.Description
	}
	if description == "" {
		description = manifest.Name
	}
	parameters := manifest.Tool.Parameters
	if parameters == nil {
		parameters = manifest.InputSchema
	}
	return core.ToolSchema{Name: manifest.ID, Description: description, Parameters: cloneJSONMap(parameters)}
}

func cloneJSONMap(input map[string]any) map[string]any {
	if input == nil {
		return nil
	}
	encoded, err := json.Marshal(input)
	if err != nil {
		return nil
	}
	var copyOf map[string]any
	if json.Unmarshal(encoded, &copyOf) != nil {
		return nil
	}
	return copyOf
}

func cloneMetadata(input map[string]string) map[string]string {
	if input == nil {
		return nil
	}
	out := make(map[string]string, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
}
