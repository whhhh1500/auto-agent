package evaluation

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/whhhh1500/auto-agent/internal/executionroute"
	"github.com/whhhh1500/auto-agent/pkg/app/effectreceipt"
	core "github.com/whhhh1500/auto-agent/pkg/core"
)

// coverageRuntimeEvidenceTimeout bounds best-effort reads made after a case
// has already reached a terminal state. A read failure leaves only the
// affected requirement unavailable; it must not change the evaluation turn.
const coverageRuntimeEvidenceTimeout = 500 * time.Millisecond

// coverageRunToolInvocationLister is an optional local extension of the core
// write journal. It intentionally remains structural: evaluation does not
// require a storage implementation, and normal execution does not gain a new
// dependency on evaluation.
//
// Results must be the complete, exact-principal/session/run journal scope.
// The collector independently validates that invariant before counting them.
type coverageRunToolInvocationLister interface {
	ListRunToolInvocations(context.Context, core.Principal, string, string) ([]core.ToolInvocationRecord, error)
}

// collectCoverageEvidence gathers only content-free, durable projections, then
// delegates all contract semantics to VerifyCoverage. It is called after the
// Session is terminal and the normal execution evidence and ledger exist.
func (r *Runner) collectCoverageEvidence(
	ctx context.Context,
	session *core.Session,
	principal core.Principal,
	contract *CoverageContract,
	caseResult CaseResult,
) (CoverageEvidence, error) {
	if contract == nil {
		return CoverageEvidence{}, fmt.Errorf("coverage contract is nil")
	}
	normalized := cloneCoverageContract(*contract)
	if err := ValidateCoverageContract(&normalized); err != nil {
		return CoverageEvidence{}, err
	}
	input := CoverageVerificationInput{CasePassed: caseResult.Passed}
	if normalized.Route != nil {
		route := r.coverageRouteEvidence(ctx, session, principal, caseResult.AgentRunID)
		input.Route = &route
	}
	if len(normalized.Effects) != 0 {
		effects := r.coverageEffectEvidence(ctx, principal, caseResult.SessionID, caseResult.AgentRunID, normalized.Effects)
		input.Effects = effects
	}
	if normalized.Cost != nil {
		cost := coverageCostEvidence(caseResult.Evidence, caseResult.Ledger)
		input.Cost = &cost
	}
	return VerifyCoverage(normalized, input)
}

func (r *Runner) coverageRouteEvidence(ctx context.Context, session *core.Session, principal core.Principal, runID string) CoverageRouteEvidence {
	if r == nil || r.Runtime == nil {
		return CoverageRouteEvidence{Status: CoverageUnavailable}
	}
	reader, _ := r.Runtime.ToolJournal.(core.ToolInvocationReader)
	readCtx, cancel := coverageRuntimeContext(ctx)
	defer cancel()
	projection := executionroute.ObserveRouteEvidence(readCtx, session, principal, reader, runID)

	if projection.Identity.Status != executionroute.EvidenceStatusVerified {
		return CoverageRouteEvidence{Status: CoverageUnavailable}
	}
	route := CoverageRouteEvidence{
		Protocol:         CoverageRouteProtocolAutoProbeOnceV2,
		IdentityVerified: true,
	}
	switch projection.Coverage.Status {
	case executionroute.CoverageStatusComplete:
		route.Status = CoverageSatisfied
	case executionroute.CoverageStatusIncomplete:
		route.Status = CoverageFailed
	case executionroute.CoverageStatusUnavailable:
		return CoverageRouteEvidence{Status: CoverageUnavailable}
	default:
		return CoverageRouteEvidence{Status: CoverageUnavailable}
	}
	route.CandidateTargetCount = projection.Coverage.CandidateCount
	route.SelectedTargetCount = projection.Coverage.SelectedTargetCount
	route.ExecutedTargetCount = projection.Coverage.ExecutedTargetCount
	route.ExactOnce = projection.Coverage.ExactOnce
	return route
}

func (r *Runner) coverageEffectEvidence(
	ctx context.Context,
	principal core.Principal,
	sessionID, runID string,
	requirements []EffectCoverageRequirement,
) []CoverageEffectEvidence {
	needsJournal, needsProvider := false, false
	for _, requirement := range requirements {
		switch requirement.ReceiptLevel {
		case EffectReceiptJournalCompleted:
			needsJournal = true
		case EffectReceiptProviderReadBack:
			needsProvider = true
		}
	}
	var journalCounts map[string]int
	var journalUnavailable map[string]bool
	journalAvailable := false
	if needsJournal {
		var err error
		journalCounts, journalUnavailable, journalAvailable, err = r.coverageJournalCounts(ctx, principal, sessionID, runID)
		if err != nil {
			// A malformed, conflicting, or otherwise unreadable source cannot
			// establish a negative cardinality. Preserve it as unavailable.
			journalCounts, journalUnavailable, journalAvailable = nil, nil, false
		}
	}
	var providerCounts map[string]int
	var providerUnavailable map[string]bool
	providerAvailable := false
	if needsProvider {
		var err error
		providerCounts, providerUnavailable, providerAvailable, err = r.coverageProviderCounts(ctx, principal, sessionID, runID)
		if err != nil {
			// Provider receipt conflicts are evidence gaps, not a reason to
			// rewrite an already-terminal evaluation case as failed execution.
			providerCounts, providerUnavailable, providerAvailable = nil, nil, false
		}
	}

	evidence := make([]CoverageEffectEvidence, 0, len(requirements))
	for _, requirement := range requirements {
		effect := CoverageEffectEvidence{
			ID: requirement.ID, CapabilityID: requirement.CapabilityID,
			ReceiptLevel: requirement.ReceiptLevel, Status: CoverageUnavailable,
		}
		switch requirement.ReceiptLevel {
		case EffectReceiptJournalCompleted:
			if journalAvailable && !journalUnavailable[requirement.CapabilityID] {
				effect.Status = CoverageSatisfied
				effect.Occurrences = journalCounts[requirement.CapabilityID]
			}
		case EffectReceiptProviderReadBack:
			if providerAvailable && !providerUnavailable[requirement.CapabilityID] {
				effect.Status = CoverageSatisfied
				effect.Occurrences = providerCounts[requirement.CapabilityID]
			}
		}
		evidence = append(evidence, effect)
	}
	sort.Slice(evidence, func(i, j int) bool { return evidence[i].ID < evidence[j].ID })
	return evidence
}

func (r *Runner) coverageJournalCounts(
	ctx context.Context,
	principal core.Principal,
	sessionID, runID string,
) (map[string]int, map[string]bool, bool, error) {
	if r == nil || r.Runtime == nil {
		return nil, nil, false, nil
	}
	lister, ok := r.Runtime.ToolJournal.(coverageRunToolInvocationLister)
	if !ok {
		return nil, nil, false, nil
	}
	readCtx, cancel := coverageRuntimeContext(ctx)
	defer cancel()
	records, ok := coverageListJournal(readCtx, lister, principal, sessionID, runID)
	if !ok {
		return nil, nil, false, nil
	}
	if len(records) > core.MaxSessionEvents {
		return nil, nil, false, fmt.Errorf("coverage journal result exceeds bounded record count")
	}
	counts := make(map[string]int)
	unavailable := make(map[string]bool)
	callIDs := make(map[string]bool, len(records))
	for _, record := range records {
		if err := validateCoverageInvocationScope(record.ToolInvocation, principal, sessionID, runID); err != nil {
			return nil, nil, false, err
		}
		if callIDs[record.CallID] {
			return nil, nil, false, fmt.Errorf("coverage journal contains duplicate call id %q", record.CallID)
		}
		callIDs[record.CallID] = true
		switch record.State {
		case core.ToolInvocationStarted, core.ToolInvocationUncertain:
			// A terminal evaluation with a local in-flight or uncertain call
			// cannot establish that this capability occurred zero times.
			unavailable[record.CapabilityID] = true
		case core.ToolInvocationCompleted:
			if record.Result == nil {
				return nil, nil, false, fmt.Errorf("coverage journal completed call %q has no result", record.CallID)
			}
			counts[record.CapabilityID]++
		default:
			return nil, nil, false, fmt.Errorf("coverage journal call %q has invalid state", record.CallID)
		}
	}
	return counts, unavailable, true, nil
}

func coverageListJournal(
	ctx context.Context,
	lister coverageRunToolInvocationLister,
	principal core.Principal,
	sessionID, runID string,
) (records []core.ToolInvocationRecord, available bool) {
	defer func() {
		if recover() != nil {
			records, available = nil, false
		}
	}()
	if ctx == nil || ctx.Err() != nil {
		return nil, false
	}
	rows, err := lister.ListRunToolInvocations(ctx, principal, sessionID, runID)
	if err != nil || ctx.Err() != nil {
		return nil, false
	}
	return rows, true
}

func (r *Runner) coverageProviderCounts(
	ctx context.Context,
	principal core.Principal,
	sessionID, runID string,
) (map[string]int, map[string]bool, bool, error) {
	if r == nil || r.EffectReceipts == nil {
		return nil, nil, false, nil
	}
	readCtx, cancel := coverageRuntimeContext(ctx)
	defer cancel()
	scope := effectreceipt.RunScope{
		TenantID: principal.TenantID, SubjectID: principal.SubjectID, SessionID: sessionID, RunID: runID,
	}
	records, ok := coverageListProvider(readCtx, r.EffectReceipts, scope)
	if !ok {
		return nil, nil, false, nil
	}
	counts := make(map[string]int)
	unavailable := make(map[string]bool)
	callIDs := make(map[string]bool, len(records))
	for _, record := range records {
		if err := effectreceipt.ValidateRecord(record.Record); err != nil {
			return nil, nil, false, fmt.Errorf("coverage effect receipt is invalid")
		}
		invocation := record.Intent.Invocation
		if err := validateCoverageInvocationScope(invocation, principal, sessionID, runID); err != nil {
			return nil, nil, false, err
		}
		if callIDs[invocation.CallID] {
			return nil, nil, false, fmt.Errorf("coverage effect receipts contain duplicate call id %q", invocation.CallID)
		}
		callIDs[invocation.CallID] = true
		switch record.State {
		case effectreceipt.StateConfirmed:
			counts[invocation.CapabilityID]++
		case effectreceipt.StatePrepared, effectreceipt.StateDispatching, effectreceipt.StateAccepted, effectreceipt.StateUnknown:
			// An acknowledgement or unknown read-back is never evidence that
			// the external effect occurred. Preserve this as unavailable even
			// when other calls of the same capability are confirmed.
			unavailable[invocation.CapabilityID] = true
		case effectreceipt.StateRejected:
			// A rejected read-back establishes no external occurrence, but it
			// does establish the bounded zero contribution for this call.
		default:
			return nil, nil, false, fmt.Errorf("coverage effect receipt has invalid state")
		}
	}
	return counts, unavailable, true, nil
}

func coverageListProvider(
	ctx context.Context,
	reader effectreceipt.RunReader,
	scope effectreceipt.RunScope,
) (records []effectreceipt.RecoveryRecord, available bool) {
	defer func() {
		if recover() != nil {
			records, available = nil, false
		}
	}()
	if ctx == nil || ctx.Err() != nil {
		return nil, false
	}
	const maxPages = (core.MaxSessionEvents / effectreceipt.MaxRecoveryPageSize) + 1
	cursor := effectreceipt.RunCursor{}
	seen := 0
	for page := 0; page < maxPages; page++ {
		rows, next, err := reader.ListRun(ctx, scope, cursor, effectreceipt.MaxRecoveryPageSize)
		if err != nil || ctx.Err() != nil || len(rows) > effectreceipt.MaxRecoveryPageSize {
			return nil, false
		}
		if len(rows) == 0 {
			return records, true
		}
		lastCallID := cursor.CallID
		for _, row := range rows {
			callID := row.Intent.Invocation.CallID
			if callID <= lastCallID {
				return nil, false
			}
			lastCallID = callID
		}
		if next.CallID != lastCallID {
			return nil, false
		}
		seen += len(rows)
		if seen > core.MaxSessionEvents {
			return nil, false
		}
		records = append(records, rows...)
		if len(rows) < effectreceipt.MaxRecoveryPageSize {
			return records, true
		}
		cursor = next
	}
	return nil, false
}

func validateCoverageInvocationScope(invocation core.ToolInvocation, principal core.Principal, sessionID, runID string) error {
	if err := core.ValidateToolInvocation(invocation); err != nil {
		return fmt.Errorf("coverage invocation is invalid")
	}
	if invocation.TenantID != principal.TenantID || invocation.SubjectID != principal.SubjectID ||
		invocation.SessionID != sessionID || invocation.RunID != runID {
		return fmt.Errorf("coverage invocation scope conflicts with evaluation case")
	}
	return nil
}

func coverageCostEvidence(evidence *ExecutionEvidence, ledger *ExecutionLedger) CoverageCostEvidence {
	if evidence == nil || ledger == nil || !ledger.Complete ||
		validateExecutionEvidence(*evidence) != nil || validateExecutionLedger(*ledger) != nil {
		return CoverageCostEvidence{Status: CoverageUnavailable}
	}
	if evidence.NoUsageReported || evidence.UsageReports < len(ledger.ModelCalls) ||
		evidence.ReportedInputTokens > core.MaxReportedTokensPerRun-evidence.ReportedOutputTokens {
		return CoverageCostEvidence{Status: CoverageUnavailable}
	}
	var ledgerInput, ledgerOutput int64
	for _, call := range ledger.ModelCalls {
		if call.Usage == nil || ledgerInput > evidence.ReportedInputTokens-call.Usage.InputTokens ||
			ledgerOutput > evidence.ReportedOutputTokens-call.Usage.OutputTokens {
			return CoverageCostEvidence{Status: CoverageUnavailable}
		}
		ledgerInput += call.Usage.InputTokens
		ledgerOutput += call.Usage.OutputTokens
	}
	// The durable evidence includes summaries as well as model calls, so it may
	// exceed the reconciled model subtotal. It may never be smaller.
	if evidence.ReportedInputTokens < ledgerInput || evidence.ReportedOutputTokens < ledgerOutput {
		return CoverageCostEvidence{Status: CoverageUnavailable}
	}
	total := evidence.ReportedInputTokens + evidence.ReportedOutputTokens
	if total > core.MaxReportedTokensPerRun {
		return CoverageCostEvidence{Status: CoverageUnavailable}
	}
	return CoverageCostEvidence{
		Status: CoverageSatisfied, LedgerComplete: true,
		ReportedTotalTokens: total, ModelCalls: len(ledger.ModelCalls),
	}
}

func coverageRuntimeContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithTimeout(context.WithoutCancel(ctx), coverageRuntimeEvidenceTimeout)
}
