package executionroute

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/whhhh1500/auto-agent/pkg/app/programmatic"
	"github.com/whhhh1500/auto-agent/pkg/core"
)

// VerifyChoiceResult proves that a v2 choice result came from one complete,
// planned assistant batch and the exact completed journal records. The model
// history is used only to locate the durable event chain; OK and metadata come
// from the canonical journal result.
func VerifyChoiceResult(ctx context.Context, session *core.Session, principal core.Principal, reader core.ToolInvocationReader, runID, callID string, plan programmatic.ProbePlan, kind programmatic.ChoiceRouteKind) (ok bool, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			ok, err = false, probePlanFailure(fmt.Sprintf("choice result verification panicked: %T", recovered))
		}
	}()
	if ctx == nil || session == nil || reader == nil || core.ValidateRunID(runID) != nil || callID == "" || plan.Digest() == "" {
		return false, probePlanFailure("choice result proof source is unavailable")
	}
	if kind != programmatic.ChoiceRouteDirect && kind != programmatic.ChoiceRouteExecute {
		return false, probePlanFailure("choice result route kind is invalid")
	}
	if err := validateProbeSessionPrincipal(session, principal); err != nil {
		return false, err
	}

	events := session.Events()
	frozen, err := frozenProbeComposition(events, runID)
	if err != nil {
		return false, err
	}
	if _, err := validateProbeRouteComposition(frozen.composition); err != nil {
		return false, err
	}
	if err := validateProbeCompositionArtifacts(frozen.composition); err != nil {
		return false, err
	}

	transcript, err := choiceTranscriptFor(events, runID, frozen)
	if err != nil {
		return false, err
	}
	batch, selected, err := transcript.batchFor(callID)
	if err != nil {
		return false, err
	}
	if err := choiceHasTrustedPriorProbe(ctx, session, principal, reader, runID, frozen, transcript, selected.assistant, plan); err != nil {
		return false, err
	}
	if err := validateChoiceBatch(plan, kind, batch); err != nil {
		return false, err
	}
	for _, call := range batch {
		capability, err := frozen.capability(call.call.Name)
		if err != nil {
			return false, err
		}
		if err := verifyChoiceInvocation(ctx, session, principal, reader, runID, call, capability); err != nil {
			return false, err
		}
		if kind == programmatic.ChoiceRouteExecute {
			if err := plan.ValidateExecuteReturn(call.output, call.call.Args); err != nil {
				return false, probePlanFailure("generic execute result violates its planned return bound")
			}
		}
	}
	if err := verifyNestedChoiceEvidence(ctx, session, principal, reader, runID, frozen, plan, transcript, batch, kind); err != nil {
		return false, err
	}
	return true, nil
}

type choiceCallEvidence struct {
	call      core.ToolCall
	assistant int64
	toolCall  int64
	result    int64
	output    core.CapabilityResult
}

type choiceTranscript struct {
	calls   map[string]choiceCallEvidence
	batches map[int64][]choiceCallEvidence
	nested  map[string]nestedChoiceEvidence
}

type nestedChoiceEvidence struct {
	parentCallID string
	call         core.ToolCall
	toolCall     int64
	result       int64
	output       core.CapabilityResult
}

func choiceTranscriptFor(events []core.SessionEvent, runID string, frozen probeComposition) (choiceTranscript, error) {
	if err := core.ValidateRunID(runID); err != nil {
		return choiceTranscript{}, probePlanFailure("choice run identity is invalid")
	}
	transcript := choiceTranscript{calls: map[string]choiceCallEvidence{}, batches: map[int64][]choiceCallEvidence{}, nested: map[string]nestedChoiceEvidence{}}
	toolCalls := map[string]struct{}{}
	results := map[string]struct{}{}
	for _, event := range events {
		if event.RunID != runID {
			continue
		}
		switch event.Type {
		case core.EvAssistantMessage:
			var data core.AssistantMessageData
			if json.Unmarshal(event.Data, &data) != nil {
				return choiceTranscript{}, probePlanFailure("choice assistant evidence cannot be decoded")
			}
			calls, err := strictAssistantCalls(data)
			if err != nil {
				return choiceTranscript{}, probePlanFailure("choice assistant evidence is invalid")
			}
			batch := make([]choiceCallEvidence, 0, len(calls))
			for _, call := range calls {
				if _, exists := transcript.calls[call.ID]; exists {
					return choiceTranscript{}, probePlanFailure("choice assistant call evidence is duplicated")
				}
				record := choiceCallEvidence{call: call, assistant: event.Seq}
				transcript.calls[call.ID] = record
				batch = append(batch, record)
			}
			if len(batch) > 0 {
				transcript.batches[event.Seq] = batch
			}
		case core.EvToolCall:
			var data core.ToolCallData
			if json.Unmarshal(event.Data, &data) != nil {
				return choiceTranscript{}, probePlanFailure("choice tool call evidence cannot be decoded")
			}
			if _, exists := toolCalls[data.CallID]; exists {
				return choiceTranscript{}, probePlanFailure("choice tool call evidence is duplicated")
			}
			record, found := transcript.calls[data.CallID]
			if !found {
				parent, nestedOK := choiceNestedProgramChildParent(transcript.calls, frozen, data)
				if !nestedOK {
					return choiceTranscript{}, probePlanFailure("choice tool call evidence is orphaned or changed")
				}
				transcript.nested[data.CallID] = nestedChoiceEvidence{parentCallID: parent, call: core.ToolCall{ID: data.CallID, Name: data.Name, Args: data.Args}, toolCall: event.Seq}
				continue
			}
			if record.assistant >= event.Seq || record.call.Name != data.Name || !sameArgs(record.call.Args, data.Args) {
				return choiceTranscript{}, probePlanFailure("choice tool call evidence is orphaned or changed")
			}
			record.toolCall = event.Seq
			transcript.calls[data.CallID] = record
			toolCalls[data.CallID] = struct{}{}
		case core.EvToolResult:
			var data core.ToolResultData
			if json.Unmarshal(event.Data, &data) != nil {
				return choiceTranscript{}, probePlanFailure("choice tool result evidence cannot be decoded")
			}
			if _, exists := results[data.CallID]; exists {
				return choiceTranscript{}, probePlanFailure("choice tool result evidence is duplicated")
			}
			record, found := transcript.calls[data.CallID]
			if !found {
				nested, nestedFound := transcript.nested[data.CallID]
				if !nestedFound || nested.result != 0 || nested.toolCall >= event.Seq {
					return choiceTranscript{}, probePlanFailure("choice tool result evidence is orphaned")
				}
				nested.result = event.Seq
				nested.output = core.CapabilityResult{Content: data.Content, OK: data.OK, Metadata: data.Metadata}
				transcript.nested[data.CallID] = nested
				continue
			}
			if record.toolCall == 0 || record.toolCall >= event.Seq {
				return choiceTranscript{}, probePlanFailure("choice tool result evidence is orphaned")
			}
			record.result = event.Seq
			record.output = core.CapabilityResult{Content: data.Content, OK: data.OK, Metadata: data.Metadata}
			transcript.calls[data.CallID] = record
			results[data.CallID] = struct{}{}
		}
	}
	for id, record := range transcript.calls {
		if record.toolCall == 0 || record.result == 0 {
			return choiceTranscript{}, probePlanFailure("choice tool evidence is incomplete")
		}
		batch := transcript.batches[record.assistant]
		for index := range batch {
			if batch[index].call.ID == id {
				batch[index] = record
				break
			}
		}
		transcript.batches[record.assistant] = batch
	}
	for _, nested := range transcript.nested {
		if nested.result == 0 {
			return choiceTranscript{}, probePlanFailure("nested program child evidence is incomplete")
		}
	}
	return transcript, nil
}

func choiceNestedProgramChildParent(calls map[string]choiceCallEvidence, frozen probeComposition, child core.ToolCallData) (string, bool) {
	executeID, err := validateProbeRouteComposition(frozen.composition)
	if err != nil {
		return "", false
	}
	parentID := ""
	for id, parent := range calls {
		if parent.toolCall == 0 || parent.result != 0 || parent.call.Name != executeID || !genericExecutePermitsChild(parent.call.Args, id, child, frozen) {
			continue
		}
		if parentID != "" {
			return "", false
		}
		parentID = id
	}
	return parentID, parentID != ""
}

func (t choiceTranscript) batchFor(callID string) ([]choiceCallEvidence, choiceCallEvidence, error) {
	selected, found := t.calls[callID]
	if !found {
		return nil, choiceCallEvidence{}, probePlanFailure("choice assistant call is missing")
	}
	batch := t.batches[selected.assistant]
	if len(batch) == 0 {
		return nil, choiceCallEvidence{}, probePlanFailure("choice assistant batch is missing")
	}
	return batch, selected, nil
}

func choiceHasTrustedPriorProbe(ctx context.Context, session *core.Session, principal core.Principal, reader core.ToolInvocationReader, runID string, frozen probeComposition, transcript choiceTranscript, before int64, plan programmatic.ProbePlan) error {
	matches := 0
	for sequence, batch := range transcript.batches {
		if sequence >= before {
			continue
		}
		for _, call := range batch {
			capability, err := frozen.capability(call.call.Name)
			if err != nil || capability.Manifest.Metadata[programmatic.ProbeManifestKey] != programmatic.ProbeManifestVersion {
				continue
			}
			candidate, isProbe, err := ResolveProbePlan(ctx, session, principal, reader, runID, call.call.ID)
			if err != nil || !isProbe || candidate.Digest() != plan.Digest() {
				continue
			}
			matches++
		}
	}
	if matches != 1 {
		return probePlanFailure("choice has no unique prior trusted probe plan")
	}
	return nil
}

func validateChoiceBatch(plan programmatic.ProbePlan, kind programmatic.ChoiceRouteKind, batch []choiceCallEvidence) error {
	direct := plan.DirectToolSchema().Name
	execute := plan.ExecuteToolSchema().Name
	if direct == "" || execute == "" || len(batch) == 0 {
		return probePlanFailure("choice plan surface is unavailable")
	}
	switch kind {
	case programmatic.ChoiceRouteDirect:
		if len(batch) > plan.MaxSelections() {
			return probePlanFailure("direct choice batch exceeds the trusted plan")
		}
		seen := make([]map[string]any, 0, len(batch))
		for _, call := range batch {
			if call.call.Name != direct || plan.ValidateDirect(call.call.Args) != nil {
				return probePlanFailure("direct choice is outside the trusted plan")
			}
			for _, previous := range seen {
				if sameArgs(previous, call.call.Args) {
					return probePlanFailure("direct choice batch contains a duplicate candidate")
				}
			}
			seen = append(seen, call.call.Args)
		}
	case programmatic.ChoiceRouteExecute:
		if len(batch) != 1 || batch[0].call.Name != execute {
			return probePlanFailure("generic execute choice is mixed or changed")
		}
		if err := preparedExecuteMatches(plan, batch[0].call.Args); err != nil {
			return err
		}
	default:
		return probePlanFailure("choice result route kind is invalid")
	}
	return nil
}

func preparedExecuteMatches(plan programmatic.ProbePlan, actual map[string]any) error {
	_, sourceOK := actual["source"].(string)
	input, inputOK := actual["input"].(map[string]any)
	targets, targetsOK := input["targets"].([]any)
	projection, projectionOK := input["projection"].(string)
	if !sourceOK || !inputOK || !targetsOK || !projectionOK || len(targets) == 0 {
		return probePlanFailure("generic execute arguments are not host-owned")
	}
	facts := plan.Facts()
	selection := make([]any, 0, len(targets))
	seen := map[int]bool{}
	for _, rawTarget := range targets {
		target, ok := rawTarget.(map[string]any)
		if !ok {
			return probePlanFailure("generic execute target is malformed")
		}
		index := -1
		for candidateIndex, candidate := range facts.Candidates {
			expected := map[string]any{"args": candidate.Args, "facts": candidate.Facts}
			if !sameArgs(expected, target) {
				continue
			}
			if index >= 0 {
				return probePlanFailure("generic execute target maps to multiple candidates")
			}
			index = candidateIndex
		}
		if index < 0 || seen[index] {
			return probePlanFailure("generic execute target is unknown or duplicated")
		}
		seen[index] = true
		selection = append(selection, float64(index))
	}
	prepared, err := plan.PrepareExecute(map[string]any{"selection": selection, "projection": projection})
	if err != nil || !sameArgs(prepared, actual) {
		return probePlanFailure("generic execute arguments differ from plan preparation")
	}
	return nil
}

func verifyChoiceInvocation(ctx context.Context, session *core.Session, principal core.Principal, reader core.ToolInvocationReader, runID string, evidence choiceCallEvidence, capability core.SnapshotCapability) error {
	invocation, err := core.NewToolInvocation(core.RunInfo{
		RunID: runID, SessionID: session.ID(), ProfileID: session.ProfileID(), Principal: principal,
	}, evidence.call, capability.Manifest.Idempotent)
	if err != nil {
		return probePlanFailure("choice invocation identity is invalid")
	}
	record, found, err := readProbeInvocation(ctx, reader, invocation)
	if err != nil || !found || record.ToolInvocation != invocation || record.State != core.ToolInvocationCompleted || record.Result == nil {
		return probePlanFailure("choice journal is not durably completed")
	}
	if !record.Result.OK || core.ValidateCapabilityResult(*record.Result) != nil || !probeResultEqual(*record.Result, evidence.output) {
		return probePlanFailure("choice journal result conflicts with event evidence")
	}
	if capability.Manifest.MaxOutputBytes > 0 && len(record.Result.Content) > capability.Manifest.MaxOutputBytes {
		return probePlanFailure("choice result exceeds its frozen capability bound")
	}
	return nil
}

func verifyNestedChoiceEvidence(ctx context.Context, session *core.Session, principal core.Principal, reader core.ToolInvocationReader, runID string, frozen probeComposition, plan programmatic.ProbePlan, transcript choiceTranscript, batch []choiceCallEvidence, kind programmatic.ChoiceRouteKind) error {
	if kind == programmatic.ChoiceRouteDirect {
		if len(transcript.nested) != 0 {
			return probePlanFailure("direct choice has unexpected nested program children")
		}
		return nil
	}
	if len(batch) != 1 {
		return probePlanFailure("generic execute batch is invalid")
	}
	expectedTargets, err := nestedChoiceTargetCounts(plan, batch[0].call.Args)
	if err != nil {
		return err
	}
	if len(transcript.nested) != len(expectedTargets) {
		return probePlanFailure("nested program child coverage differs from prepared targets")
	}
	observedTargets := make(map[string]int, len(transcript.nested))
	for _, child := range transcript.nested {
		key, keyErr := nestedChoiceTargetKey(child.call.Args)
		if keyErr != nil {
			return probePlanFailure("nested program child coverage differs from prepared targets")
		}
		observedTargets[key]++
	}
	for key, expectedCount := range expectedTargets {
		if observedTargets[key] != expectedCount {
			return probePlanFailure("nested program child coverage differs from prepared targets")
		}
	}
	parentID := batch[0].call.ID
	followupID := ""
	for _, child := range transcript.nested {
		if child.parentCallID != parentID {
			return probePlanFailure("nested program child belongs to another execute call")
		}
		if followupID == "" {
			followupID = child.call.Name
		} else if followupID != child.call.Name {
			return probePlanFailure("generic execute has mixed nested child tools")
		}
		capability, err := frozen.capability(child.call.Name)
		if err != nil || capability.Manifest.Metadata[programmatic.ExposureKey] != programmatic.ExposureVersion {
			return probePlanFailure("nested program child is not a frozen follow-up capability")
		}
		invocation, err := core.NewToolInvocation(core.RunInfo{
			RunID: runID, SessionID: session.ID(), ProfileID: session.ProfileID(), Principal: principal,
		}, child.call, capability.Manifest.Idempotent)
		if err != nil {
			return probePlanFailure("nested program child invocation identity is invalid")
		}
		record, found, err := readProbeInvocation(ctx, reader, invocation)
		if err != nil || !found || record.ToolInvocation != invocation || record.State != core.ToolInvocationCompleted || record.Result == nil || !record.Result.OK ||
			core.ValidateCapabilityResult(*record.Result) != nil || !probeResultEqual(*record.Result, child.output) {
			return probePlanFailure("nested program child journal result conflicts with event evidence")
		}
	}
	if len(transcript.nested) == 0 {
		return probePlanFailure("generic execute has no completed nested program child")
	}
	return nil
}

// nestedChoiceTargetCounts derives the exact host-prepared child argument set.
// preparedExecuteMatches verifies the source, bindings, projection and target
// selection before this helper reduces the targets to canonical child args.
func nestedChoiceTargetCounts(plan programmatic.ProbePlan, prepared map[string]any) (map[string]int, error) {
	if err := preparedExecuteMatches(plan, prepared); err != nil {
		return nil, err
	}
	input, ok := prepared["input"].(map[string]any)
	if !ok {
		return nil, probePlanFailure("generic execute targets are unavailable")
	}
	targets, ok := input["targets"].([]any)
	if !ok || len(targets) == 0 {
		return nil, probePlanFailure("generic execute targets are unavailable")
	}
	counts := make(map[string]int, len(targets))
	for _, rawTarget := range targets {
		target, ok := rawTarget.(map[string]any)
		if !ok {
			return nil, probePlanFailure("generic execute target is malformed")
		}
		args, ok := target["args"].(map[string]any)
		if !ok {
			return nil, probePlanFailure("generic execute target arguments are malformed")
		}
		key, err := nestedChoiceTargetKey(args)
		if err != nil || counts[key] != 0 {
			return nil, probePlanFailure("generic execute target coverage is invalid")
		}
		counts[key] = 1
	}
	return counts, nil
}

func nestedChoiceTargetKey(args map[string]any) (string, error) {
	encoded, err := json.Marshal(args)
	if err != nil || len(encoded) == 0 {
		return "", probePlanFailure("nested program child arguments are invalid")
	}
	return string(encoded), nil
}
