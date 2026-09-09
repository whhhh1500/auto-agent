package executionroute

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"

	"github.com/whhhh1500/auto-agent/pkg/app/programmatic"
	"github.com/whhhh1500/auto-agent/pkg/core"
)

// v2RouteAdmissionResolver resolves the provider once for Core's composition,
// then pins that exact adapter through the later start-evidence admission. It
// deliberately does not resolve the model again when Stream runs.
type v2RouteAdmissionResolver struct {
	runtime   *core.Runtime
	session   *core.Session
	principal core.Principal
	runID     string
	resume    bool
	inner     core.ModelResolver
}

func newV2RouteAdmissionResolver(runtime *core.Runtime, session *core.Session, principal core.Principal, runID string, resume bool, inner core.ModelResolver) core.ModelResolver {
	return &v2RouteAdmissionResolver{runtime: runtime, session: session, principal: principal, runID: runID, resume: resume, inner: inner}
}

func (r *v2RouteAdmissionResolver) ResolveModel(ctx context.Context, selection core.ModelSelection) (adapter core.LlmAdapter, err error) {
	defer func() {
		if recover() != nil {
			adapter, err = nil, errors.New("v2 route model resolver panicked")
		}
	}()
	if r == nil || r.inner == nil || r.runtime == nil || r.session == nil || core.ValidateRunID(r.runID) != nil {
		return nil, errors.New("v2 route model resolver is incomplete")
	}
	inner, err := r.inner.ResolveModel(ctx, selection)
	if err != nil || inner == nil {
		if err != nil {
			return nil, err
		}
		return nil, errors.New("v2 route model resolver returned nil")
	}
	if r.resume {
		if err := verifyCurrentV2RouteRuntime(ctx, r.runtime, r.session, r.principal, r.runID, selection, inner); err != nil {
			return nil, err
		}
	} else if err := verifyNewV2RouteModel(r.runtime, r.session, r.principal, selection, inner); err != nil {
		return nil, err
	}
	return &v2RouteAdmissionAdapter{runtime: r.runtime, session: r.session, principal: r.principal, runID: r.runID, resume: r.resume, selection: selection, inner: inner}, nil
}

type v2RouteAdmissionAdapter struct {
	runtime   *core.Runtime
	session   *core.Session
	principal core.Principal
	runID     string
	resume    bool
	selection core.ModelSelection
	inner     core.LlmAdapter
}

func (a *v2RouteAdmissionAdapter) Provider() string {
	if a == nil || a.inner == nil {
		return ""
	}
	return v2AdapterProvider(a.inner)
}

func (a *v2RouteAdmissionAdapter) ArtifactRevision() string {
	if a == nil || a.inner == nil {
		return ""
	}
	return v2AdapterArtifactLabel(a.inner)
}

// ModelContextLimits transparently exposes a pinned adapter's optional model
// context budget. Invalid or untrusted values intentionally become zero so
// Core applies its conservative defaults rather than admitting a malformed
// limit through the v2 route wrapper.
func (a *v2RouteAdmissionAdapter) ModelContextLimits() (window, output int) {
	defer func() {
		if recover() != nil || window <= 0 || output <= 0 || output >= window {
			window, output = 0, 0
		}
	}()
	if a == nil || a.inner == nil {
		return 0, 0
	}
	limits, ok := a.inner.(interface{ ModelContextLimits() (int, int) })
	if !ok || limits == nil {
		return 0, 0
	}
	return limits.ModelContextLimits()
}

func (a *v2RouteAdmissionAdapter) Stream(ctx context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) (err error) {
	defer func() {
		if recover() != nil {
			err = errors.New("v2 route admission panicked")
		}
	}()
	if a == nil || a.inner == nil || ctx == nil {
		return errors.New("v2 route admission is incomplete")
	}
	if options.ModelCall.RunID != a.runID {
		return probePlanFailure("v2 model call run identity differs from its route admission")
	}
	if err := verifyCurrentV2RouteRuntime(ctx, a.runtime, a.session, a.principal, a.runID, a.selection, a.inner); err != nil {
		return err
	}
	return a.inner.Stream(ctx, options, emit)
}

func verifyNewV2RouteModel(runtime *core.Runtime, session *core.Session, principal core.Principal, selection core.ModelSelection, adapter core.LlmAdapter) error {
	if runtime == nil || session == nil || adapter == nil {
		return probePlanFailure("new v2 route model proof source is unavailable")
	}
	current, err := resolveCurrentV2RouteRuntime(runtime, session, principal)
	if err != nil {
		return err
	}
	if current.profile.ProfileID != session.ProfileID() || current.profile.ID == "" || selection != current.profile.Model ||
		v2AdapterProvider(adapter) == "" || v2ModelArtifactRevision(adapter) == "" {
		return probePlanFailure("new v2 route model cannot be pinned")
	}
	return nil
}

type currentV2RouteRuntime struct {
	profile      *core.AgentProfileSnapshot
	capabilities *core.CapabilitySnapshot
	effective    core.PermissionSet
}

func verifyCurrentV2RouteRuntime(ctx context.Context, runtime *core.Runtime, session *core.Session, principal core.Principal, runID string, selection core.ModelSelection, adapter core.LlmAdapter) error {
	if ctx == nil || runtime == nil || session == nil || adapter == nil || core.ValidateRunID(runID) != nil {
		return probePlanFailure("current v2 runtime proof source is unavailable")
	}
	if err := validateProbeSessionPrincipal(session, principal); err != nil {
		return err
	}
	frozen, err := frozenProbeComposition(session.Events(), runID)
	if err != nil {
		return err
	}
	if _, err := validateProbeRouteComposition(frozen.composition); err != nil {
		return err
	}
	if err := validateProbeCompositionArtifacts(frozen.composition); err != nil {
		return err
	}
	current, err := resolveCurrentV2RouteRuntime(runtime, session, principal)
	if err != nil {
		return err
	}
	if current.profile.ProfileID != session.ProfileID() || current.profile.ProfileID != frozen.composition.Profile.ProfileID ||
		current.profile.ID == "" || current.profile.ID != frozen.composition.Profile.ID {
		return probePlanFailure("current profile differs from the frozen start")
	}
	if selection != current.profile.Model || selection != frozen.composition.Model {
		return probePlanFailure("current model selection differs from the frozen start")
	}
	provider := v2AdapterProvider(adapter)
	revision := v2ModelArtifactRevision(adapter)
	if provider == "" || revision == "" || provider != frozen.composition.ResolvedProvider || revision != frozen.composition.ModelRevision {
		return probePlanFailure("current resolved model artifact differs from the frozen start")
	}
	return verifyCurrentV2Capabilities(frozen, current, principal)
}

func resolveCurrentV2RouteRuntime(runtime *core.Runtime, session *core.Session, principal core.Principal) (currentV2RouteRuntime, error) {
	if runtime == nil || runtime.Profiles == nil || runtime.Capabilities == nil || session == nil {
		return currentV2RouteRuntime{}, probePlanFailure("current v2 runtime dependencies are incomplete")
	}
	profile, err := runtime.Profiles.Resolve(principal, session.Scope(), session.ProfileID())
	if err != nil {
		return currentV2RouteRuntime{}, probePlanFailure("current profile cannot be resolved")
	}
	capabilities, err := (core.CapabilityResolver{Registry: runtime.Capabilities, Credentials: runtime.Credentials}).Resolve(principal, session.Scope())
	if err != nil {
		return currentV2RouteRuntime{}, probePlanFailure("current capabilities cannot be resolved")
	}
	capabilities, err = profile.FilterCapabilities(capabilities)
	if err != nil {
		return currentV2RouteRuntime{}, probePlanFailure("current profile capabilities do not match")
	}
	effective := principal.Grants.Clone()
	if runtime.Policy != nil {
		policy, err := runtime.Policy.Resolve(principal, session.Scope())
		if err != nil {
			return currentV2RouteRuntime{}, probePlanFailure("current policy cannot be resolved")
		}
		effective = policy.Permissions.Clone()
	}
	return currentV2RouteRuntime{profile: profile, capabilities: capabilities.FilterByPermissions(effective), effective: effective}, nil
}

func verifyCurrentV2Capabilities(frozen probeComposition, current currentV2RouteRuntime, principal core.Principal) error {
	executeID, err := validateProbeRouteComposition(frozen.composition)
	if err != nil {
		return err
	}
	seen := map[string]bool{}
	for _, capability := range frozen.composition.Capabilities {
		role := ""
		switch {
		case capability.Manifest.ID == executeID:
			role = "execute"
		case capability.Manifest.Metadata[programmatic.ProbeManifestKey] == programmatic.ProbeManifestVersion:
			role = "probe"
		case capability.Manifest.Metadata[programmatic.ExposureKey] == programmatic.ExposureVersion:
			role = "followup"
		default:
			continue
		}
		if seen[capability.Manifest.ID] {
			return probePlanFailure("frozen v2 capability evidence is duplicated")
		}
		seen[capability.Manifest.ID] = true
		if _, err := frozen.capability(capability.Manifest.ID); err != nil {
			return err
		}
		currentCapability, found := exactCurrentCapability(current.capabilities.Capabilities(), capability.Manifest.ID)
		if !found || !sameSnapshotCapability(currentCapability, capability) {
			return probePlanFailure("current v2 capability artifact differs from the frozen start: " + capability.Manifest.ID)
		}
		switch role {
		case "probe":
			if err := validateNeutralProbeCapability(currentCapability, current.effective, principal.Grants); err != nil {
				return err
			}
		case "followup":
			if err := validateFollowupCapability(currentCapability, current.effective, principal.Grants); err != nil {
				return err
			}
		case "execute":
			manifest := currentCapability.Manifest
			if manifest.Tool == nil || !current.effective.Allows(manifest.RequiredPermissions) || !principal.Grants.Allows(manifest.RequiredPermissions) {
				return probePlanFailure("current generic execute capability is no longer authorized")
			}
		}
	}
	if !seen[executeID] {
		return probePlanFailure("frozen generic execute capability is unavailable")
	}
	return nil
}

func sameSnapshotCapability(left, right core.SnapshotCapability) bool {
	encodedLeft, leftErr := json.Marshal(left)
	encodedRight, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && bytes.Equal(encodedLeft, encodedRight)
}

func exactCurrentCapability(capabilities []core.SnapshotCapability, id string) (core.SnapshotCapability, bool) {
	var found *core.SnapshotCapability
	for _, capability := range capabilities {
		if capability.Manifest.ID != id {
			continue
		}
		if found != nil {
			return core.SnapshotCapability{}, false
		}
		copyOf := capability
		found = &copyOf
	}
	if found == nil {
		return core.SnapshotCapability{}, false
	}
	return *found, true
}

func v2AdapterProvider(adapter core.LlmAdapter) (provider string) {
	defer func() {
		if recover() != nil {
			provider = ""
		}
	}()
	if adapter == nil {
		return ""
	}
	return strings.TrimSpace(adapter.Provider())
}

func v2AdapterArtifactLabel(adapter core.LlmAdapter) (revision string) {
	defer func() {
		if recover() != nil {
			revision = ""
		}
	}()
	revisioner, ok := adapter.(core.ArtifactRevisioner)
	if !ok {
		return ""
	}
	revision = strings.TrimSpace(revisioner.ArtifactRevision())
	if len(revision) > 4096 {
		return ""
	}
	return revision
}

func v2ModelArtifactRevision(adapter core.LlmAdapter) string {
	label := v2AdapterArtifactLabel(adapter)
	if label == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(label))
	return "sha256:" + hex.EncodeToString(sum[:])
}

// admitV2Choice reserves the complete selected route before the projection
// emits an assistant batch to Core. Core executes tool calls sequentially, so
// this is the only v2 boundary that can reject an over-capacity Direct batch
// or execute parent plus children without leaving a partial choice effect.
func admitV2Choice(ctx context.Context, session *core.Session, principal core.Principal, runID string, plan programmatic.ProbePlan, kind programmatic.ChoiceRouteKind, calls []core.ToolCall) (err error) {
	defer func() {
		if recover() != nil {
			err = probePlanFailure("v2 choice capacity admission panicked")
		}
	}()
	if ctx == nil || session == nil || core.ValidateRunID(runID) != nil || plan.Digest() == "" {
		return probePlanFailure("v2 choice capacity proof source is unavailable")
	}
	if err := validateProbeSessionPrincipal(session, principal); err != nil {
		return err
	}
	events := session.Events()
	frozen, err := frozenProbeComposition(events, runID)
	if err != nil {
		return err
	}
	executeID, err := validateProbeRouteComposition(frozen.composition)
	if err != nil {
		return err
	}
	if err := validateProbeCompositionArtifacts(frozen.composition); err != nil {
		return err
	}
	if frozen.composition.MaxToolCalls < 1 {
		return probePlanFailure("frozen tool-call budget is unavailable")
	}
	transcript, err := choiceTranscriptFor(events, runID, frozen)
	if err != nil {
		return err
	}
	counts, err := v2CompletedChoiceCallCounts(events, runID, transcript)
	if err != nil {
		return err
	}

	directID := plan.DirectToolSchema().Name
	if directID == "" {
		return probePlanFailure("choice direct surface is unavailable")
	}
	var required int
	reserved := map[string]int{}
	switch kind {
	case programmatic.ChoiceRouteDirect:
		if len(calls) == 0 {
			return probePlanFailure("direct choice batch is empty")
		}
		if len(calls) > plan.MaxSelections() {
			return probePlanFailure("direct choice batch exceeds the trusted plan")
		}
		for index, call := range calls {
			if call.Name != directID || plan.ValidateDirect(call.Args) != nil {
				return probePlanFailure("direct choice is outside the trusted plan")
			}
			for previous := 0; previous < index; previous++ {
				if sameArgs(calls[previous].Args, call.Args) {
					return probePlanFailure("direct choice batch contains a duplicate candidate")
				}
			}
		}
		required = len(calls)
		reserved[directID] = required
	case programmatic.ChoiceRouteExecute:
		if plan.ExecuteToolSchema().Name != executeID || len(calls) != 1 || calls[0].Name != executeID {
			return probePlanFailure("generic execute choice is mixed or changed")
		}
		children, err := v2PreparedExecuteChildren(plan, calls[0].Args, directID)
		if err != nil {
			return err
		}
		required = 1 + children
		reserved[executeID] = 1
		reserved[directID] = children
	default:
		return probePlanFailure("choice route kind is invalid")
	}
	if required < 1 || counts.total > frozen.composition.MaxToolCalls || required > frozen.composition.MaxToolCalls-counts.total {
		return probePlanFailure("choice batch exceeds frozen remaining tool-call budget")
	}
	for capabilityID, amount := range reserved {
		capability, err := frozen.capability(capabilityID)
		if err != nil {
			return err
		}
		if capabilityID == directID && capability.Manifest.Metadata[programmatic.ExposureKey] != programmatic.ExposureVersion {
			return probePlanFailure("choice follow-up is not a frozen programmatic capability")
		}
		if amount < 1 {
			return probePlanFailure("choice reservation is invalid")
		}
		if limit := capability.Manifest.PerTurnBudget; limit > 0 && (counts.byCapability[capabilityID] > limit || amount > limit-counts.byCapability[capabilityID]) {
			return probePlanFailure("choice batch exceeds frozen capability budget")
		}
	}
	return nil
}

type v2ChoiceCallCounts struct {
	total        int
	byCapability map[string]int
}

// v2CompletedChoiceCallCounts derives the exact Core call count from the
// already paired session evidence. It independently checks the raw event ID
// sets so a malformed nested child cannot be hidden by a map overwrite during
// transcript reconstruction.
func v2CompletedChoiceCallCounts(events []core.SessionEvent, runID string, transcript choiceTranscript) (v2ChoiceCallCounts, error) {
	counts := v2ChoiceCallCounts{byCapability: map[string]int{}}
	callEvents := map[string]core.ToolCallData{}
	resultEvents := map[string]core.ToolResultData{}
	for _, event := range events {
		if event.RunID != runID {
			continue
		}
		switch event.Type {
		case core.EvToolCall:
			var data core.ToolCallData
			if json.Unmarshal(event.Data, &data) != nil || strings.TrimSpace(data.CallID) == "" || core.ValidateNamespacedID(data.Name) != nil {
				return v2ChoiceCallCounts{}, probePlanFailure("choice tool-call capacity evidence is invalid")
			}
			if _, exists := callEvents[data.CallID]; exists {
				return v2ChoiceCallCounts{}, probePlanFailure("choice tool-call capacity evidence is duplicated")
			}
			callEvents[data.CallID] = data
			counts.total++
			counts.byCapability[data.Name]++
		case core.EvToolResult:
			var data core.ToolResultData
			if json.Unmarshal(event.Data, &data) != nil || strings.TrimSpace(data.CallID) == "" {
				return v2ChoiceCallCounts{}, probePlanFailure("choice tool-result capacity evidence is invalid")
			}
			if _, exists := resultEvents[data.CallID]; exists {
				return v2ChoiceCallCounts{}, probePlanFailure("choice tool-result capacity evidence is duplicated")
			}
			if _, found := callEvents[data.CallID]; !found {
				return v2ChoiceCallCounts{}, probePlanFailure("choice tool-result capacity evidence is orphaned")
			}
			resultEvents[data.CallID] = data
		}
	}
	if len(callEvents) != len(resultEvents) || len(callEvents) != len(transcript.calls)+len(transcript.nested) {
		return v2ChoiceCallCounts{}, probePlanFailure("choice tool-call capacity evidence is incomplete")
	}
	for callID, event := range callEvents {
		if _, found := resultEvents[callID]; !found {
			return v2ChoiceCallCounts{}, probePlanFailure("choice tool-result capacity evidence is incomplete")
		}
		if record, found := transcript.calls[callID]; found {
			if record.call.Name != event.Name || !sameArgs(record.call.Args, event.Args) {
				return v2ChoiceCallCounts{}, probePlanFailure("choice tool-call capacity evidence changed")
			}
			continue
		}
		nested, found := transcript.nested[callID]
		if !found || nested.call.Name != event.Name || !sameArgs(nested.call.Args, event.Args) {
			return v2ChoiceCallCounts{}, probePlanFailure("nested tool-call capacity evidence changed")
		}
	}
	return counts, nil
}

// v2PreparedExecuteChildren derives the exact child reservation from the
// original model selection while it is still route-specific. PrepareExecute
// creates the canonical source that calls the one frozen follow-up once for
// each target, so this count is the program's dynamic child requirement.
func v2PreparedExecuteChildren(plan programmatic.ProbePlan, args map[string]any, followupID string) (int, error) {
	prepared, err := plan.PrepareExecute(args)
	if err != nil {
		return 0, probePlanFailure("generic execute selection cannot be prepared")
	}
	input, inputOK := prepared["input"].(map[string]any)
	targets, targetsOK := input["targets"].([]any)
	bindings, bindingsOK := prepared["bindings"].(map[string]any)
	if !inputOK || !targetsOK || !bindingsOK || len(targets) == 0 || len(bindings) != 1 {
		return 0, probePlanFailure("generic execute child reservation is unavailable")
	}
	if _, found := bindings[followupID]; !found {
		return 0, probePlanFailure("generic execute binding differs from the trusted follow-up")
	}
	for _, target := range targets {
		if _, ok := target.(map[string]any); !ok {
			return 0, probePlanFailure("generic execute target reservation is malformed")
		}
	}
	return len(targets), nil
}
