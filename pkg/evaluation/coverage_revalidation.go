package evaluation

import (
	"context"
	"fmt"

	core "github.com/whhhh1500/auto-agent/pkg/core"
)

// RevalidateCoverageGate rebuilds every required CoverageContract verdict from
// the canonical terminal Session and the Runner's optional evidence readers.
// It never authorizes a release from CaseResult.Coverage alone: that snapshot
// must match the frozen contract, while route and effect inputs are read again.
//
// The persisted, content-free ExecutionEvidence and ExecutionLedger are used
// only for cost contracts after strict validation and an exact reconciliation
// with the canonical Session usage projection. RequireContracts makes legacy
// cases without a contract unavailable, preserving JSON compatibility while
// providing a strict release/canary policy.
func (r *Runner) RevalidateCoverageGate(
	ctx context.Context,
	dataset Dataset,
	run RunResult,
	principal core.Principal,
	policy CoverageGatePolicy,
) (CoverageGateResult, error) {
	if ctx == nil {
		return CoverageGateResult{}, fmt.Errorf("coverage revalidation context is nil")
	}
	if err := ValidateDataset(&dataset); err != nil {
		return CoverageGateResult{}, err
	}
	if err := ValidateRunResult(run, true); err != nil {
		return CoverageGateResult{}, err
	}
	if run.Status != RunCompleted || run.DatasetID != dataset.ID || run.DatasetVersion != dataset.Version || run.DatasetRevision != dataset.Revision {
		return CoverageGateResult{}, fmt.Errorf("coverage revalidation run does not match completed dataset")
	}
	if run.TenantID != principal.TenantID || run.SubjectID != principal.SubjectID ||
		run.Metadata["evaluation.principal_scope"] != principal.Scope.String() {
		return CoverageGateResult{}, fmt.Errorf("coverage revalidation principal does not own run %s", run.ID)
	}

	// Only the Coverage pointers are replaced below; avoid mutating the caller's
	// audit snapshot while preserving all other immutable result projections.
	revalidated := run
	revalidated.Cases = append([]CaseResult(nil), run.Cases...)
	results := make(map[string]int, len(revalidated.Cases))
	for index := range revalidated.Cases {
		results[revalidated.Cases[index].CaseID] = index
	}
	for _, evalCase := range dataset.Cases {
		if evalCase.Coverage == nil {
			continue
		}
		index, found := results[evalCase.ID]
		if !found {
			continue
		}
		stored := revalidated.Cases[index]
		revalidated.Cases[index].Coverage = r.revalidateCoverageCase(ctx, dataset, run, evalCase, stored, principal)
	}
	return EvaluateCoverageGate(dataset, revalidated, policy)
}

func (r *Runner) revalidateCoverageCase(
	ctx context.Context,
	dataset Dataset,
	run RunResult,
	evalCase Case,
	stored CaseResult,
	principal core.Principal,
) *CoverageEvidence {
	contract := cloneCoverageContract(*evalCase.Coverage)
	if err := ValidateCoverageContract(&contract); err != nil {
		// Dataset validation already establishes the contract. Retain a bounded,
		// unavailable projection if a caller somehow races a malformed copy.
		return nil
	}
	if stored.Coverage == nil {
		return unavailableCoverageEvidence(contract, CoverageReasonCaseCoverageMissing)
	}
	if stored.Coverage.ContractVersion != contract.Version || stored.Coverage.ContractRevision != contract.Revision {
		return unavailableCoverageEvidence(contract, CoverageReasonContractMismatch)
	}

	sessionID, agentRunID, _ := evaluationCaseIDs(run.ID, evalCase.ID)
	if stored.SessionID != sessionID || stored.AgentRunID != agentRunID {
		return unavailableCoverageEvidence(contract, CoverageReasonCaseCoverageUnavailable)
	}
	sessions := r.coverageRevalidationSessions()
	if sessions == nil {
		return unavailableCoverageEvidence(contract, CoverageReasonCaseCoverageUnavailable)
	}
	session, err := sessions.Load(ctx, sessionID)
	if err != nil {
		return unavailableCoverageEvidence(contract, CoverageReasonCaseCoverageUnavailable)
	}
	profileID := evalCase.ProfileID
	if profileID == "" || profileID == dataset.ProfileID {
		profileID = run.ProfileID
	}
	if err := validateEvaluationSession(session, principal, profileID, coverageRevalidationMetadata(run, dataset, evalCase)); err != nil {
		return unavailableCoverageEvidence(contract, CoverageReasonCaseCoverageUnavailable)
	}
	status, terminal := session.RunStatus(agentRunID)
	if !terminal || status == "" || status == core.RunWaitingApproval || status != stored.Status {
		return unavailableCoverageEvidence(contract, CoverageReasonCaseCoverageUnavailable)
	}

	input := CoverageVerificationInput{CasePassed: stored.Passed}
	if contract.Route != nil {
		route := r.coverageRouteEvidence(ctx, session, principal, agentRunID)
		input.Route = &route
	}
	if len(contract.Effects) != 0 {
		input.Effects = r.coverageEffectEvidence(ctx, principal, sessionID, agentRunID, contract.Effects)
	}
	if contract.Cost != nil {
		durable, err := executionEvidence(session.Events(), agentRunID)
		if err != nil || stored.Evidence == nil || validateExecutionEvidence(*stored.Evidence) != nil || *stored.Evidence != *durable {
			cost := CoverageCostEvidence{Status: CoverageUnavailable}
			input.Cost = &cost
		} else {
			cost := coverageCostEvidence(durable, stored.Ledger)
			input.Cost = &cost
		}
	}
	evidence, err := VerifyCoverage(contract, input)
	if err != nil {
		return unavailableCoverageEvidence(contract, CoverageReasonCaseCoverageUnavailable)
	}
	return &evidence
}

func (r *Runner) coverageRevalidationSessions() core.SessionStore {
	if r == nil {
		return nil
	}
	return r.Sessions
}

func coverageRevalidationMetadata(run RunResult, dataset Dataset, evalCase Case) map[string]string {
	metadata := map[string]string{
		"evaluation.run_id":           run.ID,
		"evaluation.dataset_id":       dataset.ID,
		"evaluation.dataset_version":  fmt.Sprintf("%d", dataset.Version),
		"evaluation.dataset_revision": dataset.Revision,
		"evaluation.case_id":          evalCase.ID,
	}
	for key, value := range evalCase.Metadata {
		metadata["evaluation.case."+key] = value
	}
	return metadata
}

func unavailableCoverageEvidence(contract CoverageContract, reason CoverageReasonCode) *CoverageEvidence {
	evidence := CoverageEvidence{
		ContractVersion: contract.Version, ContractRevision: contract.Revision,
		Status: CoverageUnavailable, ReasonCodes: []CoverageReasonCode{reason},
	}
	if err := ValidateCoverageEvidence(evidence); err != nil {
		return nil
	}
	return &evidence
}
