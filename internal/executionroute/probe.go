package executionroute

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/whhhh1500/auto-agent/pkg/app/programmatic"
	"github.com/whhhh1500/auto-agent/pkg/core"
	ptc "github.com/whhhh1500/auto-agent/pkg/execution/programmatic"
)

const probeRouteMode = "auto_probe_once"

// ErrProbePlanResolution marks proof that cannot safely unlock the v2 choice
// surface. Callers intentionally receive no raw tool content or journal data.
var ErrProbePlanResolution = errors.New("probe plan resolution failed")

// IsNeutralProbe classifies a model-proposed tool name before the call is
// admitted. It reads only the authoritative frozen v2 composition; no
// assistant event, tool result, or journal record is required at this stage.
// A marked capability whose frozen contract is incomplete or unsafe is an
// error rather than an ordinary Direct tool.
func IsNeutralProbe(session *core.Session, principal core.Principal, runID, toolName string) (isProbe bool, err error) {
	defer func() {
		if recover() != nil {
			err = probePlanFailure("neutral probe classification panicked")
		}
	}()
	if session == nil {
		return false, probePlanFailure("session proof source is unavailable")
	}
	if err := core.ValidateRunID(runID); err != nil {
		return false, probePlanFailure("run identity is invalid")
	}
	if core.ValidateNamespacedID(toolName) != nil {
		return false, nil
	}
	frozen, err := frozenProbeComposition(session.Events(), runID)
	if err != nil {
		return false, err
	}
	candidates := frozen.matchingCapabilities(toolName)
	if len(candidates) == 0 {
		return false, nil
	}
	marked := false
	for _, candidate := range candidates {
		if _, present := candidate.Manifest.Metadata[programmatic.ProbeManifestKey]; present {
			marked = true
			break
		}
	}
	if !marked {
		if len(candidates) != 1 || strings.TrimSpace(candidates[0].ProviderRevision) == "" {
			return false, probePlanFailure("frozen capability evidence is ambiguous or incomplete")
		}
		return false, nil
	}
	isProbe = true
	if len(candidates) != 1 || strings.TrimSpace(candidates[0].ProviderRevision) == "" {
		return true, probePlanFailure("frozen probe capability evidence is ambiguous or incomplete")
	}
	probe := candidates[0]
	if probe.Manifest.Metadata[programmatic.ProbeManifestKey] != programmatic.ProbeManifestVersion {
		return true, probePlanFailure("probe marker version is unsupported")
	}
	if err := validateProbeSessionPrincipal(session, principal); err != nil {
		return true, err
	}
	if _, err := validateProbeRouteComposition(frozen.composition); err != nil {
		return true, err
	}
	if err := validateProbeCompositionArtifacts(frozen.composition); err != nil {
		return true, err
	}
	if err := validateNeutralProbeCapability(probe, frozen.composition.EffectivePermissions, principal.Grants); err != nil {
		return true, err
	}
	return true, nil
}

// ResolveProbePlan reconstructs a trusted v2 probe plan for one completed
// tool call. A false isProbe result is reserved for an otherwise authoritative
// frozen capability that has no neutral-probe marker. Once a marker is present,
// missing, conflicting, or incomplete evidence is an error and must block the
// route projection.
//
// The function uses only durable session evidence and the read-only journal.
// Current runtime authorization and provider state are deliberately rechecked
// by the route projection's admission callback before another model request.
func ResolveProbePlan(ctx context.Context, session *core.Session, principal core.Principal, reader core.ToolInvocationReader, runID, callID string) (plan programmatic.ProbePlan, isProbe bool, err error) {
	defer func() {
		if recover() != nil {
			plan = programmatic.ProbePlan{}
			err = probePlanFailure("proof reconstruction panicked")
		}
	}()
	if session == nil {
		return programmatic.ProbePlan{}, false, probePlanFailure("session proof source is unavailable")
	}
	if err := core.ValidateRunID(runID); err != nil {
		return programmatic.ProbePlan{}, false, probePlanFailure("run identity is invalid")
	}
	if strings.TrimSpace(callID) == "" || len(callID) > 256 || strings.ContainsAny(callID, "\r\n\x00") {
		return programmatic.ProbePlan{}, false, probePlanFailure("tool call identity is invalid")
	}
	events := session.Events()
	// Keep classification conservative when the receipt itself is corrupted:
	// a marker observed in the durable composition plus the requested call ID
	// means callers must never reinterpret the failure as an ordinary Direct
	// action. The hint is never used to authorize a plan.
	markerHint := probeMarkerHint(events, runID, callID)
	frozen, err := frozenProbeComposition(events, runID)
	if err != nil {
		return programmatic.ProbePlan{}, markerHint, err
	}
	_, err = validateProbeRouteComposition(frozen.composition)
	if err != nil {
		return programmatic.ProbePlan{}, markerHint, err
	}
	evidence, err := probeReceiptEvidence(events, runID, callID, frozen)
	if err != nil {
		return programmatic.ProbePlan{}, markerHint, err
	}
	if !frozen.hasStart || frozen.startSeq >= evidence.assistant {
		return programmatic.ProbePlan{}, markerHint, probePlanFailure("probe receipt precedes its frozen run start")
	}
	probe, err := frozen.capability(evidence.call.Name)
	if err != nil {
		return programmatic.ProbePlan{}, markerHint, err
	}
	marker, marked := probe.Manifest.Metadata[programmatic.ProbeManifestKey]
	if !marked {
		return programmatic.ProbePlan{}, false, nil
	}
	isProbe = true
	if marker != programmatic.ProbeManifestVersion {
		return programmatic.ProbePlan{}, true, probePlanFailure("probe marker version is unsupported")
	}
	if ctx == nil || reader == nil {
		return programmatic.ProbePlan{}, true, probePlanFailure("required proof source is unavailable")
	}
	if err := validateProbeSessionPrincipal(session, principal); err != nil {
		return programmatic.ProbePlan{}, true, err
	}

	plan, err = resolveTrustedProbePlan(ctx, session, principal, reader, runID, frozen, evidence, probe)
	if err != nil {
		return programmatic.ProbePlan{}, true, err
	}
	if plan.Digest() == "" {
		return programmatic.ProbePlan{}, true, probePlanFailure("probe plan digest is unavailable")
	}
	return plan, true, nil
}

type probeComposition struct {
	composition         core.RunCompositionData
	compositionRevision string
	assignmentRevision  string
	startSeq            int64
	hasStart            bool
}

// probeMarkerHint is deliberately weaker than probeReceiptEvidence. It is
// only a conservative classification aid for an already-failing receipt, so
// it must not reject or grant any capability. Strict decoding, revisions, and
// complete call/result pairing still happen in the resolver below.
func probeMarkerHint(events []core.SessionEvent, runID, callID string) bool {
	marked := make(map[string]struct{})
	for _, event := range events {
		if event.RunID != runID {
			continue
		}
		var composition *core.RunCompositionData
		switch event.Type {
		case core.EvRunStart:
			var data core.RunStartData
			if json.Unmarshal(event.Data, &data) != nil {
				continue
			}
			composition = data.Composition
		case core.EvRunResume:
			var data core.RunResumeData
			if json.Unmarshal(event.Data, &data) != nil {
				continue
			}
			composition = data.Composition
		}
		if composition == nil {
			continue
		}
		for _, capability := range composition.Capabilities {
			if _, markedCapability := capability.Manifest.Metadata[programmatic.ProbeManifestKey]; markedCapability {
				marked[capability.Manifest.ID] = struct{}{}
			}
		}
	}
	if len(marked) == 0 {
		return false
	}
	for _, event := range events {
		if event.RunID != runID {
			continue
		}
		switch event.Type {
		case core.EvAssistantMessage:
			var data core.AssistantMessageData
			if json.Unmarshal(event.Data, &data) != nil {
				continue
			}
			if data.ToolCall != nil && data.ToolCall.ID == callID {
				if _, found := marked[data.ToolCall.Name]; found {
					return true
				}
			}
			for _, call := range data.ToolCalls {
				if call.ID == callID {
					if _, found := marked[call.Name]; found {
						return true
					}
				}
			}
		case core.EvToolCall:
			var data core.ToolCallData
			if json.Unmarshal(event.Data, &data) == nil && data.CallID == callID {
				if _, found := marked[data.Name]; found {
					return true
				}
			}
		}
	}
	return false
}

func frozenProbeComposition(events []core.SessionEvent, runID string) (probeComposition, error) {
	var frozen *probeComposition
	for _, event := range events {
		if event.RunID != runID {
			continue
		}
		switch event.Type {
		case core.EvRunStart:
			if frozen != nil {
				return probeComposition{}, probePlanFailure("run start evidence is duplicated")
			}
			var data core.RunStartData
			if json.Unmarshal(event.Data, &data) != nil {
				return probeComposition{}, probePlanFailure("run start composition cannot be decoded")
			}
			candidate, err := exactProbeComposition(data.Composition, data.CompositionRevision, data.AssignmentRevision)
			if err != nil {
				return probeComposition{}, err
			}
			candidate.startSeq, candidate.hasStart = event.Seq, true
			frozen = &candidate
		case core.EvRunResume:
			if frozen == nil {
				return probeComposition{}, probePlanFailure("run resume has no frozen run start")
			}
			var data core.RunResumeData
			if json.Unmarshal(event.Data, &data) != nil {
				return probeComposition{}, probePlanFailure("run resume composition cannot be decoded")
			}
			candidate, err := exactProbeComposition(data.Composition, data.CompositionRevision, data.AssignmentRevision)
			if err != nil {
				return probeComposition{}, err
			}
			if candidate.compositionRevision != frozen.compositionRevision || candidate.assignmentRevision != frozen.assignmentRevision {
				return probeComposition{}, probePlanFailure("current composition or assignment drifted")
			}
		}
	}
	if frozen == nil {
		return probeComposition{}, probePlanFailure("frozen run composition is missing")
	}
	return *frozen, nil
}

func exactProbeComposition(composition *core.RunCompositionData, compositionRevision, assignmentRevision string) (probeComposition, error) {
	if composition == nil || compositionRevision == "" || assignmentRevision == "" {
		return probeComposition{}, probePlanFailure("composition revision evidence is incomplete")
	}
	if err := core.ValidateRunCompositionMetadata(composition.Metadata); err != nil {
		return probeComposition{}, probePlanFailure("composition metadata is invalid")
	}
	calculatedComposition, err := core.CompositionRevision(composition)
	if err != nil || calculatedComposition != compositionRevision {
		return probeComposition{}, probePlanFailure("composition revision drifted")
	}
	calculatedAssignment, err := core.CompositionMetadataRevision(composition.Metadata)
	if err != nil || calculatedAssignment != assignmentRevision {
		return probeComposition{}, probePlanFailure("assignment revision drifted")
	}
	copyOf := *composition
	return probeComposition{composition: copyOf, compositionRevision: calculatedComposition, assignmentRevision: calculatedAssignment}, nil
}

func (f probeComposition) capability(id string) (core.SnapshotCapability, error) {
	matches := f.matchingCapabilities(id)
	if len(matches) > 1 {
		return core.SnapshotCapability{}, probePlanFailure("frozen composition has duplicate capability evidence")
	}
	if len(matches) != 1 || strings.TrimSpace(matches[0].ProviderRevision) == "" {
		return core.SnapshotCapability{}, probePlanFailure("frozen capability or provider revision is unavailable")
	}
	return matches[0], nil
}

func (f probeComposition) matchingCapabilities(id string) []core.SnapshotCapability {
	matches := make([]core.SnapshotCapability, 0, 1)
	for _, capability := range f.composition.Capabilities {
		if capability.Manifest.ID == id {
			matches = append(matches, capability)
		}
	}
	return matches
}

type probeEvidence struct {
	call       core.ToolCall
	result     core.CapabilityResult
	assistant  int64
	toolCall   int64
	toolResult int64
}

// probeReceiptEvidence checks the complete event chain, not only the selected
// call. That makes duplicate IDs, orphan calls/results, and swapped arguments
// in any part of the current run fail before a plan can be derived.
func probeReceiptEvidence(events []core.SessionEvent, runID, callID string, frozen probeComposition) (probeEvidence, error) {
	assistantCalls := map[string]probeEvidence{}
	toolCalls := map[string]probeEvidence{}
	results := map[string]probeEvidence{}
	nested := map[string]nestedProgramChildEvidence{}
	var selected *probeEvidence

	for _, event := range events {
		if event.RunID != runID {
			continue
		}
		switch event.Type {
		case core.EvAssistantMessage:
			var data core.AssistantMessageData
			if json.Unmarshal(event.Data, &data) != nil {
				return probeEvidence{}, probePlanFailure("assistant call evidence cannot be decoded")
			}
			calls, err := strictAssistantCalls(data)
			if err != nil {
				return probeEvidence{}, probePlanFailure("assistant call evidence is invalid")
			}
			for _, call := range calls {
				if _, exists := assistantCalls[call.ID]; exists {
					return probeEvidence{}, probePlanFailure("assistant call evidence is duplicated")
				}
				candidate := probeEvidence{call: call, assistant: event.Seq}
				assistantCalls[call.ID] = candidate
				if call.ID == callID {
					if len(calls) != 1 || selected != nil {
						return probeEvidence{}, probePlanFailure("probe call is mixed or duplicated")
					}
					copyOf := candidate
					selected = &copyOf
				}
			}
		case core.EvToolCall:
			var data core.ToolCallData
			if json.Unmarshal(event.Data, &data) != nil {
				return probeEvidence{}, probePlanFailure("tool call evidence cannot be decoded")
			}
			if _, exists := toolCalls[data.CallID]; exists {
				return probeEvidence{}, probePlanFailure("tool call evidence is duplicated")
			}
			assistant, found := assistantCalls[data.CallID]
			if !found {
				parent, nestedOK := nestedProgramChildParent(assistantCalls, toolCalls, results, frozen, data)
				if !nestedOK {
					return probeEvidence{}, probePlanFailure("tool call evidence is orphaned or changed")
				}
				nested[data.CallID] = nestedProgramChildEvidence{parentCallID: parent, call: core.ToolCall{ID: data.CallID, Name: data.Name, Args: data.Args}, toolCall: event.Seq}
				continue
			}
			if assistant.assistant >= event.Seq || assistant.call.Name != data.Name || !sameArgs(assistant.call.Args, data.Args) {
				return probeEvidence{}, probePlanFailure("tool call evidence is orphaned or changed")
			}
			assistant.toolCall = event.Seq
			toolCalls[data.CallID] = assistant
		case core.EvToolResult:
			var data core.ToolResultData
			if json.Unmarshal(event.Data, &data) != nil {
				return probeEvidence{}, probePlanFailure("tool result evidence cannot be decoded")
			}
			if _, exists := results[data.CallID]; exists {
				return probeEvidence{}, probePlanFailure("tool result evidence is duplicated")
			}
			call, found := toolCalls[data.CallID]
			if !found {
				nestedCall, nestedFound := nested[data.CallID]
				if !nestedFound || nestedCall.toolResult != 0 || nestedCall.toolCall >= event.Seq {
					return probeEvidence{}, probePlanFailure("tool result evidence is orphaned")
				}
				nestedCall.toolResult = event.Seq
				nested[data.CallID] = nestedCall
				continue
			}
			if call.toolCall >= event.Seq {
				return probeEvidence{}, probePlanFailure("tool result evidence is orphaned")
			}
			call.toolResult = event.Seq
			call.result = core.CapabilityResult{Content: data.Content, OK: data.OK, Metadata: data.Metadata}
			results[data.CallID] = call
		}
	}

	for id, assistant := range assistantCalls {
		call, called := toolCalls[id]
		result, completed := results[id]
		if !called || !completed || call.assistant != assistant.assistant || call.toolCall == 0 || result.toolResult == 0 {
			return probeEvidence{}, probePlanFailure("tool receipt evidence is incomplete")
		}
	}
	for _, child := range nested {
		if child.toolResult == 0 {
			return probeEvidence{}, probePlanFailure("nested program child evidence is incomplete")
		}
	}
	if selected == nil {
		return probeEvidence{}, probePlanFailure("assistant probe call is missing")
	}
	completed, found := results[callID]
	if !found || completed.assistant != selected.assistant || completed.toolCall == 0 || completed.toolResult == 0 || !completed.result.OK {
		return probeEvidence{}, probePlanFailure("probe result evidence is incomplete or unsuccessful")
	}
	return completed, nil
}

type nestedProgramChildEvidence struct {
	parentCallID string
	call         core.ToolCall
	toolCall     int64
	toolResult   int64
}

// nestedProgramChildParent recognizes only the child-call shape emitted while
// a declared generic execute call is still open. The parent carries the
// host-owned bindings and targets written by ProbePlan.PrepareExecute, so this
// is provenance rather than a call-ID naming convention.
func nestedProgramChildParent(assistants, toolCalls, results map[string]probeEvidence, frozen probeComposition, child core.ToolCallData) (string, bool) {
	executeID, err := validateProbeRouteComposition(frozen.composition)
	if err != nil {
		return "", false
	}
	parentID := ""
	for id, parent := range toolCalls {
		if _, completed := results[id]; completed || parent.call.Name != executeID || !genericExecutePermitsChild(parent.call.Args, id, child, frozen) {
			continue
		}
		if parentID != "" {
			return "", false
		}
		if _, declared := assistants[id]; !declared {
			return "", false
		}
		parentID = id
	}
	return parentID, parentID != ""
}

func genericExecutePermitsChild(parent map[string]any, parentID string, child core.ToolCallData, frozen probeComposition) bool {
	if !validNestedProgramChildID(parentID, child.CallID) {
		return false
	}
	followup, err := frozen.capability(child.Name)
	if err != nil || followup.Manifest.Metadata[programmatic.ExposureKey] != programmatic.ExposureVersion {
		return false
	}
	bindings, bindingsOK := parent["bindings"].(map[string]any)
	input, inputOK := parent["input"].(map[string]any)
	targets, targetsOK := input["targets"].([]any)
	if !bindingsOK || !inputOK || !targetsOK || len(bindings) != 1 || len(targets) == 0 {
		return false
	}
	for name, binding := range bindings {
		if name != child.Name {
			continue
		}
		if digest, ok := binding.(string); !ok || digest == "" {
			return false
		}
		for _, rawTarget := range targets {
			target, ok := rawTarget.(map[string]any)
			if !ok {
				return false
			}
			args, ok := target["args"].(map[string]any)
			if !ok {
				return false
			}
			if sameArgs(args, child.Args) {
				return true
			}
		}
	}
	return false
}

func validNestedProgramChildID(parentID, childID string) bool {
	prefix := parentID + "/"
	if !strings.HasPrefix(childID, prefix) || len(childID) != len(prefix)+64 {
		return false
	}
	for _, value := range childID[len(prefix):] {
		if (value < '0' || value > '9') && (value < 'a' || value > 'f') {
			return false
		}
	}
	return true
}

func resolveTrustedProbePlan(ctx context.Context, session *core.Session, principal core.Principal, reader core.ToolInvocationReader, runID string, frozen probeComposition, evidence probeEvidence, probe core.SnapshotCapability) (programmatic.ProbePlan, error) {
	executeID, err := validateProbeRouteComposition(frozen.composition)
	if err != nil {
		return programmatic.ProbePlan{}, err
	}
	if err := validateProbeCompositionArtifacts(frozen.composition); err != nil {
		return programmatic.ProbePlan{}, err
	}
	if err := validateNeutralProbeCapability(probe, frozen.composition.EffectivePermissions, principal.Grants); err != nil {
		return programmatic.ProbePlan{}, err
	}
	if len(evidence.result.Content) > probe.Manifest.MaxOutputBytes || core.ValidateCapabilityResult(evidence.result) != nil {
		return programmatic.ProbePlan{}, probePlanFailure("probe result is outside its durable bounds")
	}
	if err := validateProbeCallArguments(probe.Manifest, evidence.call); err != nil {
		return programmatic.ProbePlan{}, err
	}
	if err := validateProbeOutput(probe.Manifest, evidence.result); err != nil {
		return programmatic.ProbePlan{}, err
	}

	invocation, err := core.NewToolInvocation(core.RunInfo{
		RunID: runID, SessionID: session.ID(), ProfileID: session.ProfileID(), Principal: principal,
	}, evidence.call, true)
	if err != nil {
		return programmatic.ProbePlan{}, probePlanFailure("probe invocation identity is invalid")
	}
	record, found, readErr := readProbeInvocation(ctx, reader, invocation)
	if readErr != nil || !found || record.ToolInvocation != invocation || record.State != core.ToolInvocationCompleted || record.Result == nil {
		return programmatic.ProbePlan{}, probePlanFailure("probe journal is not durably completed")
	}
	if core.ValidateCapabilityResult(*record.Result) != nil || !probeResultEqual(*record.Result, evidence.result) {
		return programmatic.ProbePlan{}, probePlanFailure("probe journal result conflicts with event evidence")
	}

	followupID, err := probeFollowupCapabilityID(evidence.result)
	if err != nil {
		return programmatic.ProbePlan{}, err
	}
	followup, err := frozen.capability(followupID)
	if err != nil {
		return programmatic.ProbePlan{}, err
	}
	if err := validateFollowupCapability(followup, frozen.composition.EffectivePermissions, principal.Grants); err != nil {
		return programmatic.ProbePlan{}, err
	}
	if _, err := frozen.capability(executeID); err != nil {
		return programmatic.ProbePlan{}, probePlanFailure("frozen execute capability is unavailable")
	}

	plan, err := programmatic.NewProbePlan(probe, followup, evidence.result, programmatic.ProbePlanOptions{
		ExecuteToolID:   executeID,
		ProgramContract: probeProgramContract(),
	})
	if err != nil {
		return programmatic.ProbePlan{}, probePlanFailure("probe facts cannot form a trusted plan")
	}
	return plan, nil
}

func probeProgramContract() programmatic.ProbeProgramContract {
	return programmatic.ProbeProgramContract{
		Version:        ptc.Version,
		LanguageGuide:  ptc.LanguageGuide,
		MaxSourceBytes: ptc.HardMaxSourceBytes,
		MaxToolCalls:   int(ptc.DefaultMaxToolCalls),
		ValidateSource: func(source string) ([]string, error) {
			program, err := ptc.Compile([]byte(source))
			if err != nil {
				return nil, err
			}
			return program.Tools(), nil
		},
	}
}

func validateProbeRouteComposition(composition core.RunCompositionData) (string, error) {
	metadata := composition.Metadata
	if metadata[RouteVersionKey] != RouteProbeVersion || metadata[RouteModeKey] != probeRouteMode ||
		metadata[RouteImplementationKey] != RouteProbeImplementationRevision {
		return "", probePlanFailure("frozen route metadata is not v2")
	}
	catalogID := metadata[RouteCatalogToolIDKey]
	executeID := metadata[RouteExecuteToolIDKey]
	if core.ValidateNamespacedID(catalogID) != nil || core.ValidateNamespacedID(executeID) != nil || catalogID == executeID {
		return "", probePlanFailure("frozen route tool identities are invalid")
	}
	return executeID, nil
}

func validateProbeSessionPrincipal(session *core.Session, principal core.Principal) error {
	owner := session.Principal()
	if principal.SubjectID != owner.SubjectID || principal.TenantID != owner.TenantID || !principal.Scope.Equal(owner.Scope) {
		return probePlanFailure("principal does not own the session")
	}
	return nil
}

func validateProbeCompositionArtifacts(composition core.RunCompositionData) error {
	if strings.TrimSpace(composition.Model.Provider) == "" || strings.TrimSpace(composition.Model.Model) == "" ||
		strings.TrimSpace(composition.ResolvedProvider) == "" || strings.TrimSpace(composition.ModelRevision) == "" {
		return probePlanFailure("model or provider artifact evidence is incomplete")
	}
	return nil
}

func validateNeutralProbeCapability(capability core.SnapshotCapability, effective, grants core.PermissionSet) error {
	manifest := capability.Manifest
	if manifest.Metadata[programmatic.ProbeManifestKey] != programmatic.ProbeManifestVersion || manifest.MaxOutputBytes <= 0 || manifest.MaxOutputBytes > programmatic.MaxProbeOutputBytes {
		return probePlanFailure("neutral probe contract is invalid")
	}
	if _, exposed := manifest.Metadata[programmatic.ExposureKey]; exposed {
		return probePlanFailure("neutral probe is exposed through the program catalog")
	}
	if err := validatePassiveCapability(manifest, effective, grants); err != nil {
		return err
	}
	return nil
}

func validateFollowupCapability(capability core.SnapshotCapability, effective, grants core.PermissionSet) error {
	if capability.Manifest.Metadata[programmatic.ExposureKey] != programmatic.ExposureVersion {
		return probePlanFailure("follow-up capability is not programmatic")
	}
	return validatePassiveCapability(capability.Manifest, effective, grants)
}

func validatePassiveCapability(manifest core.CapabilityManifest, effective, grants core.PermissionSet) error {
	if core.ValidateNamespacedID(manifest.ID) != nil || manifest.Kind != core.KindTool || manifest.Tool == nil || !manifest.Idempotent || manifest.RequiresApproval {
		return probePlanFailure("capability is not a passive tool")
	}
	for _, permission := range manifest.RequiredPermissions {
		name := strings.ToLower(string(permission))
		if permission == core.PermWrite || permission == core.PermSend || strings.HasSuffix(name, ".write") || strings.HasSuffix(name, ".send") {
			return probePlanFailure("capability requires a write or send permission")
		}
	}
	if !effective.Allows(manifest.RequiredPermissions) || !grants.Allows(manifest.RequiredPermissions) {
		return probePlanFailure("capability is no longer authorized")
	}
	if manifest.Execution != nil && (manifest.Execution.Writes || (manifest.Execution.Sandbox.Mode != "" && manifest.Execution.Sandbox.Mode != core.SandboxReadOnly)) {
		return probePlanFailure("capability declares a write-capable execution")
	}
	return nil
}

func validateProbeCallArguments(manifest core.CapabilityManifest, call core.ToolCall) error {
	if call.Name != manifest.ID {
		return probePlanFailure("probe call capability changed")
	}
	schema := manifest.InputSchema
	if manifest.Tool != nil && manifest.Tool.Parameters != nil {
		schema = manifest.Tool.Parameters
	}
	if err := core.ValidateArgs(schema, call.Args); err != nil {
		return probePlanFailure("probe call arguments violate the frozen schema")
	}
	return nil
}

func validateProbeOutput(manifest core.CapabilityManifest, result core.CapabilityResult) error {
	if len(manifest.OutputSchema) == 0 {
		return nil
	}
	var output any
	if json.Unmarshal([]byte(result.Content), &output) != nil || core.ValidateJSONValue(manifest.OutputSchema, output) != nil {
		return probePlanFailure("probe result violates the frozen output schema")
	}
	return nil
}

func probeFollowupCapabilityID(result core.CapabilityResult) (string, error) {
	raw, found := result.Metadata[programmatic.ProbeFactsMetadataKey]
	if !found {
		return "", probePlanFailure("probe facts are missing")
	}
	facts, ok := raw.(map[string]any)
	if !ok {
		return "", probePlanFailure("probe facts are malformed")
	}
	id, ok := facts["followup_capability_id"].(string)
	if !ok || core.ValidateNamespacedID(id) != nil {
		return "", probePlanFailure("probe follow-up identity is invalid")
	}
	return id, nil
}

func readProbeInvocation(ctx context.Context, reader core.ToolInvocationReader, invocation core.ToolInvocation) (record core.ToolInvocationRecord, found bool, err error) {
	defer func() {
		if recover() != nil {
			record, found, err = core.ToolInvocationRecord{}, false, errors.New("journal reader panicked")
		}
	}()
	return reader.GetToolInvocation(ctx, invocation)
}

func probeResultEqual(left, right core.CapabilityResult) bool {
	encodedLeft, leftErr := json.Marshal(left)
	encodedRight, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && string(encodedLeft) == string(encodedRight)
}

func probePlanFailure(reason string) error {
	return fmt.Errorf("%w: %s", ErrProbePlanResolution, reason)
}
