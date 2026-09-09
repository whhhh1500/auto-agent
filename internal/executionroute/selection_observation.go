package executionroute

import (
	"context"

	"github.com/whhhh1500/auto-agent/pkg/app/programmatic"
	"github.com/whhhh1500/auto-agent/pkg/core"
)

// SelectedRoute is the durable outcome of a completed v2 choice batch.
// Unavailable is deliberately a route value rather than an inferred default.
type SelectedRoute string

const (
	SelectedRouteUnavailable SelectedRoute = "unavailable"
	SelectedRouteDirect      SelectedRoute = "direct"
	SelectedRoutePTC         SelectedRoute = "ptc"
)

// SelectionStatus states whether SelectedRoute was reconstructed from the
// complete frozen composition, event chain, and canonical journal records.
type SelectionStatus string

const (
	SelectionStatusUnavailable SelectionStatus = "unavailable"
	SelectionStatusVerified    SelectionStatus = "verified"
)

// SelectedRouteObservation contains only the route classification and its
// proof status. It intentionally excludes model text, tool arguments, tool
// results, call IDs, and journal records.
type SelectedRouteObservation struct {
	Route  SelectedRoute
	Status SelectionStatus
}

// ObserveSelectedRoute reconstructs the v2 choice from durable evidence.
// It is read-only and fails closed: direct or ptc is returned only when there
// is exactly one trusted probe batch, exactly one later choice batch, and the
// existing choice verifier proves its completed journal results. Every missing,
// mixed, duplicated, in-progress, uncertain, or conflicting record is
// unavailable.
func ObserveSelectedRoute(ctx context.Context, session *core.Session, principal core.Principal, reader core.ToolInvocationReader, runID string) (observation SelectedRouteObservation) {
	observation = SelectedRouteObservation{Route: SelectedRouteUnavailable, Status: SelectionStatusUnavailable}
	defer func() {
		if recover() != nil {
			observation = SelectedRouteObservation{Route: SelectedRouteUnavailable, Status: SelectionStatusUnavailable}
		}
	}()
	if ctx == nil || session == nil || reader == nil || core.ValidateRunID(runID) != nil {
		return observation
	}

	details, ok := observeVerifiedV2Choice(ctx, session, principal, reader, runID)
	if !ok {
		return observation
	}
	return SelectedRouteObservation{Route: details.route, Status: SelectionStatusVerified}
}

func observedProbePlan(ctx context.Context, session *core.Session, principal core.Principal, reader core.ToolInvocationReader, runID string, frozen probeComposition, transcript choiceTranscript) (choiceCallEvidence, programmatic.ProbePlan, bool) {
	var selected choiceCallEvidence
	var plan programmatic.ProbePlan
	found := false
	for _, batch := range transcript.batches {
		for _, call := range batch {
			capability, err := frozen.capability(call.call.Name)
			if err != nil {
				return choiceCallEvidence{}, programmatic.ProbePlan{}, false
			}
			if _, marked := capability.Manifest.Metadata[programmatic.ProbeManifestKey]; !marked {
				continue
			}
			candidate, isProbe, err := ResolveProbePlan(ctx, session, principal, reader, runID, call.call.ID)
			if err != nil || !isProbe || candidate.Digest() == "" || found {
				return choiceCallEvidence{}, programmatic.ProbePlan{}, false
			}
			selected, plan, found = call, candidate, true
		}
	}
	return selected, plan, found
}

func observedChoiceBatch(transcript choiceTranscript, probe choiceCallEvidence) ([]choiceCallEvidence, bool) {
	var selected []choiceCallEvidence
	for sequence, batch := range transcript.batches {
		if sequence == probe.assistant {
			if len(batch) != 1 || batch[0].call.ID != probe.call.ID {
				return nil, false
			}
			continue
		}
		if sequence < probe.assistant || selected != nil || len(batch) == 0 {
			return nil, false
		}
		selected = batch
	}
	return selected, selected != nil
}

func observedChoiceKind(plan programmatic.ProbePlan, batch []choiceCallEvidence) (programmatic.ChoiceRouteKind, SelectedRoute, bool) {
	if validateChoiceBatch(plan, programmatic.ChoiceRouteDirect, batch) == nil {
		return programmatic.ChoiceRouteDirect, SelectedRouteDirect, true
	}
	if validateChoiceBatch(plan, programmatic.ChoiceRouteExecute, batch) == nil {
		return programmatic.ChoiceRouteExecute, SelectedRoutePTC, true
	}
	return "", SelectedRouteUnavailable, false
}
