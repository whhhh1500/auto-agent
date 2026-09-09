package server

import (
	"context"
	"strconv"
	"time"

	"github.com/whhhh1500/auto-agent/internal/executionroute"
	"github.com/whhhh1500/auto-agent/pkg/core"
)

const routeEvidenceSpan = "harness.programmatic.route.evidence"

const routeEvidenceObservationTimeout = 500 * time.Millisecond

// observeTerminalRouteEvidence emits one content-free trace receipt for a
// terminal v2 auto_probe_once run. The canonical evidence remains the Session
// and Tool Journal; this projection is read-only and telemetry failures cannot
// affect the run outcome.
func (s *Server) observeTerminalRouteEvidence(ctx context.Context, runtime *core.Runtime, session *core.Session, principal core.Principal, runID string, status core.RunStatus) {
	if s == nil || s.telemetry == nil || runtime == nil || session == nil || status == core.RunWaitingApproval {
		return
	}
	base := context.Background()
	if ctx != nil {
		base = context.WithoutCancel(ctx)
	}
	observeCtx, cancel := context.WithTimeout(base, routeEvidenceObservationTimeout)
	defer cancel()
	reader, _ := runtime.ToolJournal.(core.ToolInvocationReader)
	evidence := executionroute.ObserveRouteEvidence(observeCtx, session, principal, reader, runID)
	if evidence.Identity.Status != executionroute.EvidenceStatusVerified {
		if executionroute.ExpectsRouteEvidence(session, runID) {
			_, span := core.StartTelemetry(s.telemetry, observeCtx, routeEvidenceSpan, core.TelemetryAttributes{
				"run.id":                             runID,
				"session.id":                         session.ID(),
				"run.status":                         string(status),
				"programmatic.route.identity.status": string(executionroute.EvidenceStatusUnavailable),
			})
			span.End(nil, nil)
		}
		return
	}
	_, span := core.StartTelemetry(s.telemetry, observeCtx, routeEvidenceSpan, routeEvidenceAttributes(session.ID(), runID, status, evidence))
	span.End(nil, nil)
}

func routeEvidenceAttributes(sessionID, runID string, status core.RunStatus, evidence executionroute.RouteEvidenceProjection) core.TelemetryAttributes {
	return core.TelemetryAttributes{
		"run.id":                                       runID,
		"session.id":                                   sessionID,
		"run.status":                                   string(status),
		"programmatic.route.identity.status":           string(evidence.Identity.Status),
		"programmatic.route.version":                   evidence.Identity.Version,
		"programmatic.route.mode":                      evidence.Identity.Mode,
		"programmatic.route.implementation":            evidence.Identity.Implementation,
		"programmatic.route.selection":                 string(evidence.Selection.Route),
		"programmatic.route.selection.status":          string(evidence.Selection.Status),
		"programmatic.route.coverage.status":           string(evidence.Coverage.Status),
		"programmatic.route.coverage.candidates":       strconv.Itoa(evidence.Coverage.CandidateCount),
		"programmatic.route.coverage.selected":         strconv.Itoa(evidence.Coverage.SelectedTargetCount),
		"programmatic.route.coverage.executed":         strconv.Itoa(evidence.Coverage.ExecutedTargetCount),
		"programmatic.route.coverage.duplicate":        strconv.FormatBool(evidence.Coverage.DuplicateCandidate),
		"programmatic.route.coverage.exact_once":       strconv.FormatBool(evidence.Coverage.ExactOnce),
		"programmatic.route.effects.journal.status":    string(evidence.Effects.JournalStatus),
		"programmatic.route.effects.journal.completed": strconv.Itoa(evidence.Effects.CompletedJournalInvocations),
		"programmatic.route.effects.external.status":   string(evidence.Effects.ExternalStatus),
		"programmatic.route.accounting.status":         string(evidence.Accounting.LedgerStatus),
		"programmatic.route.accounting.model_calls":    strconv.Itoa(evidence.Accounting.ModelCallCount),
		"programmatic.route.context.archive.status":    string(evidence.Context.ArchiveStatus),
		"programmatic.route.context.assembly.status":   string(evidence.Context.AssemblyStatus),
		"programmatic.route.context.summary_events":    strconv.Itoa(evidence.Context.SummaryEvents),
	}
}
