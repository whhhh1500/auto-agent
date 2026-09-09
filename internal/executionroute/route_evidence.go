package executionroute

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strings"

	"github.com/whhhh1500/auto-agent/pkg/app/programmatic"
	"github.com/whhhh1500/auto-agent/pkg/core"
)

// EvidenceStatus says whether the associated fields were reconstructed from
// the durable v2 route protocol. Unavailable deliberately covers missing,
// conflicting, incomplete, or out-of-scope evidence; callers must not treat it
// as a negative result.
type EvidenceStatus string

const (
	EvidenceStatusUnavailable EvidenceStatus = "unavailable"
	EvidenceStatusVerified    EvidenceStatus = "verified"
)

// RouteEvidenceProjection is a content-free, read-only summary of what the
// durable auto_probe_once protocol can prove. It intentionally has no prompt,
// answer, candidate, argument, call ID, result, provider receipt, or trace
// data. It is not a replacement for the canonical Session or Tool Journal.
type RouteEvidenceProjection struct {
	Identity   RouteIdentityEvidence
	Selection  SelectedRouteObservation
	Coverage   RouteCoverageEvidence
	Effects    RouteEffectsEvidence
	Accounting RouteAccountingEvidence
	Context    RouteContextEvidence
}

// RouteIdentityEvidence attributes a durable Run to the exact frozen v2 route
// implementation. Its fields are fixed protocol metadata, not model or tool
// content. Identity can be verified before a model emits a choice.
type RouteIdentityEvidence struct {
	Status         EvidenceStatus
	Version        string
	Mode           string
	Implementation string
}

// CoverageStatus is intentionally distinct from generic evidence status.
// Complete requires a trusted probe plan plus canonical completion evidence.
// Incomplete means that the same plan proves fewer candidates completed (or no
// choice was durably recorded); unavailable means that coverage cannot safely
// be reconstructed.
type CoverageStatus string

const (
	CoverageStatusUnavailable CoverageStatus = "unavailable"
	CoverageStatusComplete    CoverageStatus = "complete"
	CoverageStatusIncomplete  CoverageStatus = "incomplete"
)

// RouteCoverageEvidence describes coverage of the trusted probe plan's
// candidates only. It never claims semantic sufficiency for the user request.
// Fields are meaningful only when Status is complete or incomplete.
type RouteCoverageEvidence struct {
	Status              CoverageStatus
	CandidateCount      int
	SelectedTargetCount int
	ExecutedTargetCount int
	DuplicateCandidate  bool
	ExactOnce           bool
}

// RouteEffectsEvidence separates local completed Journal proof from proof of
// an external effect. The current ToolInvocationJournal validates canonical
// capability results, but has no provider-neutral target receipt/read-back
// contract, so ExternalStatus remains unavailable.
type RouteEffectsEvidence struct {
	JournalStatus               EvidenceStatus
	CompletedJournalInvocations int
	ExternalStatus              EvidenceStatus
}

// RouteAccountingEvidence validates only the durable Session usage ledger.
// LedgerStatus verified means every durable assistant outcome for this v2 run
// has exactly one adjacent, matching model:<step-start-seq> usage event. It
// does not prove provider billing, provider receipt, or an outcome lost before
// the Session suffix became durable.
type RouteAccountingEvidence struct {
	LedgerStatus   EvidenceStatus
	ModelCallCount int
	InputTokens    int64
	OutputTokens   int64
}

// RouteContextEvidence exposes only durable archival facts. SummaryEvents is
// not evidence of the final context assembled for any model request: context
// byte/token limits and dropped groups are telemetry-only in the current
// runtime, so AssemblyStatus remains unavailable.
type RouteContextEvidence struct {
	ArchiveStatus  EvidenceStatus
	AssemblyStatus EvidenceStatus
	SummaryEvents  int
}

// ExpectsRouteEvidence reports whether a RunStart advertises any exact marker
// of the frozen v2 route. It is intentionally weaker than verification and is
// used only to surface a content-free unavailable receipt when the complete
// route identity cannot be reconstructed. It must never authorize execution
// or turn malformed evidence into a verified projection.
func ExpectsRouteEvidence(session *core.Session, runID string) bool {
	if session == nil || core.ValidateRunID(runID) != nil {
		return false
	}
	for _, event := range session.Events() {
		if event.RunID != runID || event.Type != core.EvRunStart {
			continue
		}
		var start core.RunStartData
		if json.Unmarshal(event.Data, &start) != nil || start.Composition == nil {
			continue
		}
		metadata := start.Composition.Metadata
		if metadata[RouteVersionKey] == RouteProbeVersion ||
			metadata[RouteModeKey] == probeRouteMode ||
			metadata[RouteImplementationKey] == RouteProbeImplementationRevision {
			return true
		}
	}
	return false
}

// ObserveRouteEvidence projects only the frozen v2 auto_probe_once route. It
// is read-only and fail-closed. A verified identity is retained even before a
// choice exists, so accounting and context archival can be attributed without
// guessing a selected route. Choice/Journal proof is still required for
// selection and completed candidate coverage.
func ObserveRouteEvidence(ctx context.Context, session *core.Session, principal core.Principal, reader core.ToolInvocationReader, runID string) (projection RouteEvidenceProjection) {
	projection = unavailableRouteEvidence()
	defer func() {
		if recover() != nil {
			projection = unavailableRouteEvidence()
		}
	}()

	frozen, events, ok := observedV2RouteIdentity(session, principal, runID)
	if !ok {
		return projection
	}
	projection.Identity = RouteIdentityEvidence{
		Status: EvidenceStatusVerified, Version: RouteProbeVersion, Mode: probeRouteMode,
		Implementation: RouteProbeImplementationRevision,
	}

	if calls, input, output, ok := observedV2UsageLedger(events, runID); ok {
		projection.Accounting = RouteAccountingEvidence{
			LedgerStatus: EvidenceStatusVerified, ModelCallCount: calls, InputTokens: input, OutputTokens: output,
		}
	}
	if summaries, ok := observedV2SummaryEvents(events, runID); ok {
		projection.Context.ArchiveStatus = EvidenceStatusVerified
		projection.Context.SummaryEvents = summaries
	}

	if ctx == nil || reader == nil {
		return projection
	}
	// Identity, ledger, and archival facts use the revision-validated route
	// composition above. Staged probe/choice effects additionally require the
	// frozen model/provider artifact evidence used by the selection observer.
	if validateProbeCompositionArtifacts(frozen.composition) != nil {
		return projection
	}
	transcript, err := choiceTranscriptFor(events, runID, frozen)
	if err != nil {
		return projection
	}
	probe, plan, ok := observedProbePlan(ctx, session, principal, reader, runID, frozen, transcript)
	if !ok || len(plan.Facts().Candidates) == 0 {
		return projection
	}
	projection.Effects.JournalStatus = EvidenceStatusVerified
	projection.Effects.CompletedJournalInvocations = 1 // trusted neutral probe

	choice, present, chainOK := observedPostProbeChoice(transcript, probe)
	if !chainOK {
		return projection
	}
	if !present {
		projection.Coverage = RouteCoverageEvidence{Status: CoverageStatusIncomplete, CandidateCount: len(plan.Facts().Candidates)}
		return projection
	}
	kind, route, ok := observedChoiceKind(plan, choice)
	if !ok {
		return projection
	}
	verified, err := VerifyChoiceResult(ctx, session, principal, reader, runID, choice[0].call.ID, plan, kind)
	if err != nil || !verified {
		return projection
	}

	projection.Selection = SelectedRouteObservation{Route: route, Status: SelectionStatusVerified}
	projection.Effects.CompletedJournalInvocations += len(choice) + len(transcript.nested)
	projection.Coverage = verifiedV2CandidateCoverage(plan, kind, choice, transcript)
	return projection
}

func observedV2RouteIdentity(session *core.Session, principal core.Principal, runID string) (probeComposition, []core.SessionEvent, bool) {
	if session == nil || core.ValidateRunID(runID) != nil || validateProbeSessionPrincipal(session, principal) != nil {
		return probeComposition{}, nil, false
	}
	events := session.Events()
	frozen, err := frozenProbeComposition(events, runID)
	if err != nil {
		return probeComposition{}, nil, false
	}
	if _, err := validateProbeRouteComposition(frozen.composition); err != nil {
		return probeComposition{}, nil, false
	}
	return frozen, events, true
}

func observedPostProbeChoice(transcript choiceTranscript, probe choiceCallEvidence) ([]choiceCallEvidence, bool, bool) {
	var selected []choiceCallEvidence
	for sequence, batch := range transcript.batches {
		if sequence == probe.assistant {
			if len(batch) != 1 || batch[0].call.ID != probe.call.ID {
				return nil, false, false
			}
			continue
		}
		if sequence < probe.assistant || selected != nil || len(batch) == 0 {
			return nil, false, false
		}
		selected = batch
	}
	return selected, selected != nil, true
}

func verifiedV2CandidateCoverage(plan programmatic.ProbePlan, kind programmatic.ChoiceRouteKind, batch []choiceCallEvidence, transcript choiceTranscript) RouteCoverageEvidence {
	coverage := RouteCoverageEvidence{CandidateCount: len(plan.Facts().Candidates), DuplicateCandidate: false}
	switch kind {
	case programmatic.ChoiceRouteDirect:
		coverage.SelectedTargetCount = len(batch)
		coverage.ExecutedTargetCount = len(batch)
		coverage.ExactOnce = true
	case programmatic.ChoiceRouteExecute:
		selected, err := nestedChoiceTargetCounts(plan, batch[0].call.Args)
		if err != nil || len(selected) == 0 || len(selected) != len(transcript.nested) {
			return RouteCoverageEvidence{Status: CoverageStatusUnavailable}
		}
		coverage.SelectedTargetCount = len(selected)
		coverage.ExecutedTargetCount = len(transcript.nested)
		coverage.ExactOnce = true
	default:
		return RouteCoverageEvidence{Status: CoverageStatusUnavailable}
	}
	if coverage.ExecutedTargetCount == coverage.CandidateCount {
		coverage.Status = CoverageStatusComplete
	} else {
		coverage.Status = CoverageStatusIncomplete
	}
	return coverage
}

func unavailableRouteEvidence() RouteEvidenceProjection {
	return RouteEvidenceProjection{
		Identity:   RouteIdentityEvidence{Status: EvidenceStatusUnavailable},
		Selection:  SelectedRouteObservation{Route: SelectedRouteUnavailable, Status: SelectionStatusUnavailable},
		Coverage:   RouteCoverageEvidence{Status: CoverageStatusUnavailable},
		Effects:    RouteEffectsEvidence{JournalStatus: EvidenceStatusUnavailable, ExternalStatus: EvidenceStatusUnavailable},
		Accounting: RouteAccountingEvidence{LedgerStatus: EvidenceStatusUnavailable},
		Context:    RouteContextEvidence{ArchiveStatus: EvidenceStatusUnavailable, AssemblyStatus: EvidenceStatusUnavailable},
	}
}

type verifiedV2Choice struct {
	route      SelectedRoute
	kind       programmatic.ChoiceRouteKind
	plan       programmatic.ProbePlan
	batch      []choiceCallEvidence
	transcript choiceTranscript
}

// observeVerifiedV2Choice is the shared proof path for the compact selection
// observer and the richer internal projection. It deliberately requires the
// same complete evidence as ObserveSelectedRoute.
func observeVerifiedV2Choice(ctx context.Context, session *core.Session, principal core.Principal, reader core.ToolInvocationReader, runID string) (verifiedV2Choice, bool) {
	if ctx == nil || session == nil || reader == nil || core.ValidateRunID(runID) != nil {
		return verifiedV2Choice{}, false
	}
	events := session.Events()
	frozen, err := frozenProbeComposition(events, runID)
	if err != nil || validateProbeSessionPrincipal(session, principal) != nil || validateProbeCompositionArtifacts(frozen.composition) != nil {
		return verifiedV2Choice{}, false
	}
	if _, err := validateProbeRouteComposition(frozen.composition); err != nil {
		return verifiedV2Choice{}, false
	}
	transcript, err := choiceTranscriptFor(events, runID, frozen)
	if err != nil {
		return verifiedV2Choice{}, false
	}
	probe, plan, ok := observedProbePlan(ctx, session, principal, reader, runID, frozen, transcript)
	if !ok || len(transcript.batches) != 2 {
		return verifiedV2Choice{}, false
	}
	choice, ok := observedChoiceBatch(transcript, probe)
	if !ok {
		return verifiedV2Choice{}, false
	}
	kind, route, ok := observedChoiceKind(plan, choice)
	if !ok {
		return verifiedV2Choice{}, false
	}
	verified, err := VerifyChoiceResult(ctx, session, principal, reader, runID, choice[0].call.ID, plan, kind)
	if err != nil || !verified {
		return verifiedV2Choice{}, false
	}
	return verifiedV2Choice{route: route, kind: kind, plan: plan, batch: choice, transcript: transcript}, true
}

func observedV2UsageLedger(events []core.SessionEvent, runID string) (calls int, input, output int64, ok bool) {
	stepSeq := int64(0)
	started, completed := 0, 0
	for index := 0; index < len(events); index++ {
		event := events[index]
		if event.RunID != runID {
			continue
		}
		switch event.Type {
		case core.EvStepStart:
			var step core.StepData
			if json.Unmarshal(event.Data, &step) != nil || event.Seq <= 0 {
				return 0, 0, 0, false
			}
			stepSeq, started = event.Seq, started+1
		case core.EvAssistantMessage:
			if stepSeq == 0 || index+1 >= len(events) {
				return 0, 0, 0, false
			}
			usageEvent := events[index+1]
			if usageEvent.RunID != runID || usageEvent.Type != core.EvRunUsage {
				return 0, 0, 0, false
			}
			var usage core.RunUsageData
			if json.Unmarshal(usageEvent.Data, &usage) != nil || usage.InvocationID != fmt.Sprintf("model:%d", stepSeq) ||
				usage.InputTokens < 0 || usage.OutputTokens < 0 || input > math.MaxInt64-usage.InputTokens || output > math.MaxInt64-usage.OutputTokens {
				return 0, 0, 0, false
			}
			calls, completed = calls+1, completed+1
			input, output = input+usage.InputTokens, output+usage.OutputTokens
			index++
		case core.EvRunUsage:
			var usage core.RunUsageData
			if json.Unmarshal(event.Data, &usage) != nil || strings.HasPrefix(usage.InvocationID, "model:") {
				return 0, 0, 0, false
			}
		}
	}
	if started == 0 || started != completed {
		return 0, 0, 0, false
	}
	return calls, input, output, true
}

func observedV2SummaryEvents(events []core.SessionEvent, runID string) (int, bool) {
	count := 0
	for _, event := range events {
		if event.RunID != runID || event.Type != core.EvContextSummary {
			continue
		}
		var summary core.ContextSummaryData
		if json.Unmarshal(event.Data, &summary) != nil || summary.Op != "replace" || summary.Start < 0 || summary.End < summary.Start {
			return 0, false
		}
		count++
	}
	return count, true
}
