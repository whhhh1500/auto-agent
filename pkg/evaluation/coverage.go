package evaluation

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	core "github.com/whhhh1500/auto-agent/pkg/core"
)

const (
	// CoverageContractV1 is the only coverage-contract schema accepted by this
	// package. Unknown versions are rejected before a dataset revision is made.
	CoverageContractV1 = "coverage-contract/v1"

	// CoverageRouteProtocolAutoProbeOnceV2 identifies the content-free route
	// evidence projection currently supported by the contract.
	CoverageRouteProtocolAutoProbeOnceV2 = "auto_probe_once/v2"

	MaxCoverageEffects = 64
	MaxCoverageReasons = 16
)

// CoverageStatus distinguishes a proven requirement from a known
// contradiction and from evidence that cannot establish either result.
type CoverageStatus string

const (
	CoverageSatisfied   CoverageStatus = "satisfied"
	CoverageFailed      CoverageStatus = "failed"
	CoverageUnavailable CoverageStatus = "unavailable"
)

// EffectReceiptLevel describes the strongest durable receipt that the host
// reconstructed for an effect. Provider read-back satisfies journal receipt;
// the inverse does not.
type EffectReceiptLevel string

const (
	EffectReceiptJournalCompleted EffectReceiptLevel = "journal_completed"
	EffectReceiptProviderReadBack EffectReceiptLevel = "provider_read_back"
)

// CoverageContract is an optional, task-level definition for evidence that a
// host must reconstruct independently of an evaluation result. It contains no
// prompts, answers, tool arguments, provider request bodies, or receipts.
// Revision is the SHA-256 digest of the canonical v1 definition.
type CoverageContract struct {
	Version           string                      `json:"version"`
	RequireCasePassed bool                        `json:"require_case_passed,omitempty"`
	Route             *RouteCoverageRequirement   `json:"route,omitempty"`
	Effects           []EffectCoverageRequirement `json:"effects,omitempty"`
	Cost              *CostCoverageRequirement    `json:"cost,omitempty"`
	Revision          string                      `json:"revision"`
}

// RouteCoverageRequirement asks the host to prove an AutoProbe target
// projection. The projection itself is supplied separately to VerifyCoverage.
type RouteCoverageRequirement struct {
	Protocol                string `json:"protocol"`
	RequireVerifiedIdentity bool   `json:"require_verified_identity,omitempty"`
	RequireCompleteTargets  bool   `json:"require_complete_targets,omitempty"`
	RequireExactOnce        bool   `json:"require_exact_once,omitempty"`
}

// EffectCoverageRequirement is provider-neutral. ID identifies the contract
// entry; CapabilityID identifies the capability whose reconstructed effects are
// counted. ReceiptLevel is the minimum receipt authority required for that
// count.
type EffectCoverageRequirement struct {
	ID             string             `json:"id"`
	CapabilityID   string             `json:"capability_id"`
	MinOccurrences int                `json:"min_occurrences"`
	MaxOccurrences int                `json:"max_occurrences"`
	ReceiptLevel   EffectReceiptLevel `json:"receipt_level"`
}

// CostCoverageRequirement constrains only bounded, content-free accounting
// summaries reconstructed by the host.
type CostCoverageRequirement struct {
	RequireCompleteLedger  bool   `json:"require_complete_ledger,omitempty"`
	MaxReportedTotalTokens *int64 `json:"max_reported_total_tokens,omitempty"`
	MaxModelCalls          *int   `json:"max_model_calls,omitempty"`
}

// CoverageRouteEvidence is a host-reconstructed, content-free route summary.
// Counts describe the already-authoritative target set; no route IDs or model
// content are included here.
type CoverageRouteEvidence struct {
	Status               CoverageStatus `json:"status"`
	Protocol             string         `json:"protocol,omitempty"`
	IdentityVerified     bool           `json:"identity_verified,omitempty"`
	CandidateTargetCount int            `json:"candidate_target_count,omitempty"`
	SelectedTargetCount  int            `json:"selected_target_count,omitempty"`
	ExecutedTargetCount  int            `json:"executed_target_count,omitempty"`
	ExactOnce            bool           `json:"exact_once,omitempty"`
}

// CoverageEffectEvidence is a host-reconstructed cardinality and receipt
// summary. It intentionally excludes provider receipt bodies and identifiers.
type CoverageEffectEvidence struct {
	ID           string             `json:"id"`
	CapabilityID string             `json:"capability_id"`
	Occurrences  int                `json:"occurrences"`
	ReceiptLevel EffectReceiptLevel `json:"receipt_level,omitempty"`
	Status       CoverageStatus     `json:"status"`
}

// CoverageCostEvidence is a host-reconstructed accounting summary. A false
// LedgerComplete is insufficient to prove a complete-cost requirement.
type CoverageCostEvidence struct {
	Status              CoverageStatus `json:"status"`
	LedgerComplete      bool           `json:"ledger_complete,omitempty"`
	ReportedTotalTokens int64          `json:"reported_total_tokens,omitempty"`
	ModelCalls          int            `json:"model_calls,omitempty"`
}

// CoverageVerificationInput is supplied by the integration host after it has
// reconstructed authoritative evidence. It is deliberately independent of
// internal route packages, servers, sessions, and storage implementations.
type CoverageVerificationInput struct {
	CasePassed bool                     `json:"case_passed"`
	Route      *CoverageRouteEvidence   `json:"route,omitempty"`
	Effects    []CoverageEffectEvidence `json:"effects,omitempty"`
	Cost       *CoverageCostEvidence    `json:"cost,omitempty"`
}

// CoverageReasonCode makes a persisted coverage verdict explainable without
// allowing arbitrary text or content-bearing host errors into the result.
type CoverageReasonCode string

const (
	CoverageReasonCaseNotPassed            CoverageReasonCode = "case_not_passed"
	CoverageReasonRouteUnavailable         CoverageReasonCode = "route_unavailable"
	CoverageReasonRouteRequirementFailed   CoverageReasonCode = "route_requirement_failed"
	CoverageReasonEffectUnavailable        CoverageReasonCode = "effect_unavailable"
	CoverageReasonEffectCardinalityFailed  CoverageReasonCode = "effect_cardinality_failed"
	CoverageReasonEffectReceiptUnavailable CoverageReasonCode = "effect_receipt_unavailable"
	CoverageReasonCostUnavailable          CoverageReasonCode = "cost_unavailable"
	CoverageReasonCostRequirementFailed    CoverageReasonCode = "cost_requirement_failed"
	CoverageReasonContractMissing          CoverageReasonCode = "contract_missing"
	CoverageReasonContractMismatch         CoverageReasonCode = "contract_mismatch"
	CoverageReasonCaseCoverageMissing      CoverageReasonCode = "case_coverage_missing"
	CoverageReasonCaseCoverageUnavailable  CoverageReasonCode = "case_coverage_unavailable"
	CoverageReasonCaseCoverageFailed       CoverageReasonCode = "case_coverage_failed"
)

var coverageReasonCodes = map[CoverageReasonCode]bool{
	CoverageReasonCaseNotPassed:            true,
	CoverageReasonRouteUnavailable:         true,
	CoverageReasonRouteRequirementFailed:   true,
	CoverageReasonEffectUnavailable:        true,
	CoverageReasonEffectCardinalityFailed:  true,
	CoverageReasonEffectReceiptUnavailable: true,
	CoverageReasonCostUnavailable:          true,
	CoverageReasonCostRequirementFailed:    true,
	CoverageReasonContractMissing:          true,
	CoverageReasonContractMismatch:         true,
	CoverageReasonCaseCoverageMissing:      true,
	CoverageReasonCaseCoverageUnavailable:  true,
	CoverageReasonCaseCoverageFailed:       true,
}

// CoverageEvidence is the content-free verdict produced by VerifyCoverage.
// It is an observation snapshot only: a release surface must rerun verification
// against its authoritative sources before treating it as a release receipt.
type CoverageEvidence struct {
	ContractVersion  string                   `json:"contract_version"`
	ContractRevision string                   `json:"contract_revision"`
	Status           CoverageStatus           `json:"status"`
	ReasonCodes      []CoverageReasonCode     `json:"reason_codes,omitempty"`
	Route            *CoverageRouteEvidence   `json:"route,omitempty"`
	Effects          []CoverageEffectEvidence `json:"effects,omitempty"`
	Cost             *CoverageCostEvidence    `json:"cost,omitempty"`
}

// CoverageGatePolicy enables strict rollout behavior for datasets still being
// migrated. With RequireContracts false, only cases that declare Coverage are
// considered. With it true, an omitted legacy contract is unavailable.
type CoverageGatePolicy struct {
	RequireContracts bool `json:"require_contracts,omitempty"`
}

// CoverageGateResult aggregates only content-free coverage outcomes. Failed
// and unavailable are both blocking; the distinction retains why the evidence
// cannot support a release.
type CoverageGateResult struct {
	Status           CoverageStatus       `json:"status"`
	RequiredCases    int                  `json:"required_cases"`
	SatisfiedCases   int                  `json:"satisfied_cases"`
	FailedCases      int                  `json:"failed_cases"`
	UnavailableCases int                  `json:"unavailable_cases"`
	ReasonCodes      []CoverageReasonCode `json:"reason_codes,omitempty"`
}

// CoverageContractRevision returns the SHA-256 digest of the normalized v1
// definition. The Revision field itself is excluded from the digest.
func CoverageContractRevision(contract CoverageContract) (string, error) {
	normalized, err := normalizedCoverageContract(contract)
	if err != nil {
		return "", err
	}
	projection := struct {
		Version           string                      `json:"version"`
		RequireCasePassed bool                        `json:"require_case_passed,omitempty"`
		Route             *RouteCoverageRequirement   `json:"route,omitempty"`
		Effects           []EffectCoverageRequirement `json:"effects,omitempty"`
		Cost              *CostCoverageRequirement    `json:"cost,omitempty"`
	}{
		Version:           normalized.Version,
		RequireCasePassed: normalized.RequireCasePassed,
		Route:             normalized.Route,
		Effects:           normalized.Effects,
		Cost:              normalized.Cost,
	}
	encoded, err := json.Marshal(projection)
	if err != nil {
		return "", fmt.Errorf("encode coverage contract revision: %w", err)
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

// ValidateCoverageContract strictly validates and canonicalizes contract in
// place. A known v1 revision is assigned when absent; a supplied revision must
// exactly match the canonical definition.
func ValidateCoverageContract(contract *CoverageContract) error {
	if contract == nil {
		return fmt.Errorf("coverage contract is nil")
	}
	normalized, err := normalizedCoverageContract(*contract)
	if err != nil {
		return err
	}
	revision, err := CoverageContractRevision(normalized)
	if err != nil {
		return err
	}
	if contract.Revision != "" && contract.Revision != revision {
		return fmt.Errorf("coverage contract revision does not match definition")
	}
	normalized.Revision = revision
	*contract = normalized
	return nil
}

func normalizedCoverageContract(contract CoverageContract) (CoverageContract, error) {
	contract = cloneCoverageContract(contract)
	if contract.Version != CoverageContractV1 {
		return CoverageContract{}, fmt.Errorf("coverage contract version %q is unsupported", contract.Version)
	}
	if !contract.RequireCasePassed && contract.Route == nil && len(contract.Effects) == 0 && contract.Cost == nil {
		return CoverageContract{}, fmt.Errorf("coverage contract has no requirements")
	}
	if contract.Route != nil {
		route := contract.Route
		if route.Protocol != CoverageRouteProtocolAutoProbeOnceV2 {
			return CoverageContract{}, fmt.Errorf("coverage route protocol %q is unsupported", route.Protocol)
		}
		if !route.RequireVerifiedIdentity && !route.RequireCompleteTargets && !route.RequireExactOnce {
			return CoverageContract{}, fmt.Errorf("coverage route has no requirements")
		}
		if route.RequireCompleteTargets && !route.RequireVerifiedIdentity {
			return CoverageContract{}, fmt.Errorf("complete route coverage requires verified identity")
		}
		if route.RequireExactOnce && (!route.RequireVerifiedIdentity || !route.RequireCompleteTargets) {
			return CoverageContract{}, fmt.Errorf("exact-once route coverage requires verified identity and complete targets")
		}
	}
	if len(contract.Effects) > MaxCoverageEffects {
		return CoverageContract{}, fmt.Errorf("coverage effects exceed %d", MaxCoverageEffects)
	}
	sort.Slice(contract.Effects, func(i, j int) bool { return contract.Effects[i].ID < contract.Effects[j].ID })
	capabilities := map[string]bool{}
	for index := range contract.Effects {
		effect := contract.Effects[index]
		if !caseIDPattern.MatchString(effect.ID) {
			return CoverageContract{}, fmt.Errorf("coverage effect id %q is invalid", effect.ID)
		}
		if index > 0 && contract.Effects[index-1].ID == effect.ID {
			return CoverageContract{}, fmt.Errorf("duplicate coverage effect id %q", effect.ID)
		}
		if err := core.ValidateNamespacedID(effect.CapabilityID); err != nil {
			return CoverageContract{}, fmt.Errorf("coverage effect %q capability: %w", effect.ID, err)
		}
		if capabilities[effect.CapabilityID] {
			return CoverageContract{}, fmt.Errorf("duplicate coverage effect capability %q", effect.CapabilityID)
		}
		capabilities[effect.CapabilityID] = true
		if effect.MinOccurrences < 0 || effect.MaxOccurrences < effect.MinOccurrences || effect.MaxOccurrences > core.MaxSessionEvents {
			return CoverageContract{}, fmt.Errorf("coverage effect %q cardinality is invalid", effect.ID)
		}
		if !validReceiptLevel(effect.ReceiptLevel) {
			return CoverageContract{}, fmt.Errorf("coverage effect %q receipt level %q is invalid", effect.ID, effect.ReceiptLevel)
		}
	}
	if contract.Cost != nil {
		cost := contract.Cost
		if !cost.RequireCompleteLedger && cost.MaxReportedTotalTokens == nil && cost.MaxModelCalls == nil {
			return CoverageContract{}, fmt.Errorf("coverage cost has no requirements")
		}
		if cost.MaxReportedTotalTokens != nil && (*cost.MaxReportedTotalTokens < 0 || *cost.MaxReportedTotalTokens > core.MaxReportedTokensPerRun) {
			return CoverageContract{}, fmt.Errorf("coverage max reported total tokens is invalid")
		}
		if cost.MaxModelCalls != nil && (*cost.MaxModelCalls < 0 || *cost.MaxModelCalls > core.HardMaxSteps) {
			return CoverageContract{}, fmt.Errorf("coverage max model calls is invalid")
		}
	}
	return contract, nil
}

func cloneCoverageContract(contract CoverageContract) CoverageContract {
	out := contract
	if contract.Route != nil {
		route := *contract.Route
		out.Route = &route
	}
	if contract.Effects != nil {
		out.Effects = append([]EffectCoverageRequirement(nil), contract.Effects...)
	}
	if contract.Cost != nil {
		cost := *contract.Cost
		if cost.MaxReportedTotalTokens != nil {
			value := *cost.MaxReportedTotalTokens
			cost.MaxReportedTotalTokens = &value
		}
		if cost.MaxModelCalls != nil {
			value := *cost.MaxModelCalls
			cost.MaxModelCalls = &value
		}
		out.Cost = &cost
	}
	return out
}

// VerifyCoverage evaluates one normalized contract using content-free evidence
// reconstructed by the caller. It does not reach into a Session, route plan,
// provider, server, or storage implementation.
func VerifyCoverage(contract CoverageContract, input CoverageVerificationInput) (CoverageEvidence, error) {
	if err := ValidateCoverageContract(&contract); err != nil {
		return CoverageEvidence{}, err
	}
	if err := validateCoverageVerificationInput(input); err != nil {
		return CoverageEvidence{}, err
	}
	evidence := CoverageEvidence{
		ContractVersion:  contract.Version,
		ContractRevision: contract.Revision,
		Status:           CoverageSatisfied,
	}
	var reasons []CoverageReasonCode
	apply := func(status CoverageStatus, reason CoverageReasonCode) {
		switch status {
		case CoverageFailed:
			evidence.Status = CoverageFailed
		case CoverageUnavailable:
			if evidence.Status != CoverageFailed {
				evidence.Status = CoverageUnavailable
			}
		}
		reasons = append(reasons, reason)
	}
	if contract.RequireCasePassed && !input.CasePassed {
		apply(CoverageFailed, CoverageReasonCaseNotPassed)
	}
	if contract.Route != nil {
		if input.Route == nil || input.Route.Status == CoverageUnavailable {
			apply(CoverageUnavailable, CoverageReasonRouteUnavailable)
		} else {
			route := *input.Route
			evidence.Route = &route
			if route.Status == CoverageFailed {
				apply(CoverageFailed, CoverageReasonRouteRequirementFailed)
			} else if route.Protocol != contract.Route.Protocol ||
				(contract.Route.RequireVerifiedIdentity && !route.IdentityVerified) ||
				(contract.Route.RequireCompleteTargets && (route.SelectedTargetCount != route.CandidateTargetCount || route.ExecutedTargetCount != route.CandidateTargetCount)) ||
				(contract.Route.RequireExactOnce && !route.ExactOnce) {
				apply(CoverageFailed, CoverageReasonRouteRequirementFailed)
			}
		}
	}
	if len(contract.Effects) != 0 {
		byID := make(map[string]CoverageEffectEvidence, len(input.Effects))
		for _, effect := range input.Effects {
			byID[effect.ID] = effect
		}
		for _, requirement := range contract.Effects {
			effect, ok := byID[requirement.ID]
			if !ok || effect.Status == CoverageUnavailable {
				apply(CoverageUnavailable, CoverageReasonEffectUnavailable)
				continue
			}
			evidence.Effects = append(evidence.Effects, effect)
			if effect.Status == CoverageFailed || effect.CapabilityID != requirement.CapabilityID {
				apply(CoverageFailed, CoverageReasonEffectCardinalityFailed)
				continue
			}
			if !receiptSatisfies(effect.ReceiptLevel, requirement.ReceiptLevel) {
				apply(CoverageUnavailable, CoverageReasonEffectReceiptUnavailable)
				continue
			}
			if effect.Occurrences < requirement.MinOccurrences || effect.Occurrences > requirement.MaxOccurrences {
				apply(CoverageFailed, CoverageReasonEffectCardinalityFailed)
			}
		}
	}
	if contract.Cost != nil {
		if input.Cost == nil || input.Cost.Status == CoverageUnavailable {
			apply(CoverageUnavailable, CoverageReasonCostUnavailable)
		} else {
			cost := *input.Cost
			evidence.Cost = &cost
			if cost.Status == CoverageFailed {
				apply(CoverageFailed, CoverageReasonCostRequirementFailed)
			} else if contract.Cost.RequireCompleteLedger && !cost.LedgerComplete {
				apply(CoverageUnavailable, CoverageReasonCostUnavailable)
			} else if (contract.Cost.MaxReportedTotalTokens != nil && cost.ReportedTotalTokens > *contract.Cost.MaxReportedTotalTokens) ||
				(contract.Cost.MaxModelCalls != nil && cost.ModelCalls > *contract.Cost.MaxModelCalls) {
				apply(CoverageFailed, CoverageReasonCostRequirementFailed)
			}
		}
	}
	evidence.ReasonCodes = canonicalCoverageReasons(reasons)
	if err := ValidateCoverageEvidence(evidence); err != nil {
		return CoverageEvidence{}, err
	}
	return evidence, nil
}

// ValidateCoverageEvidence validates a persisted v1, content-free verifier
// projection. It verifies structure, not the external sources from which a
// host reconstructed the projection.
func ValidateCoverageEvidence(evidence CoverageEvidence) error {
	if evidence.ContractVersion != CoverageContractV1 {
		return fmt.Errorf("coverage evidence contract version %q is unsupported", evidence.ContractVersion)
	}
	if !validCoverageRevision(evidence.ContractRevision) {
		return fmt.Errorf("coverage evidence contract revision is invalid")
	}
	if !validCoverageStatus(evidence.Status) {
		return fmt.Errorf("coverage evidence status %q is invalid", evidence.Status)
	}
	if len(evidence.ReasonCodes) > MaxCoverageReasons || !sort.SliceIsSorted(evidence.ReasonCodes, func(i, j int) bool { return evidence.ReasonCodes[i] < evidence.ReasonCodes[j] }) {
		return fmt.Errorf("coverage evidence reasons are not canonical")
	}
	for index, reason := range evidence.ReasonCodes {
		if !coverageReasonCodes[reason] || (index > 0 && evidence.ReasonCodes[index-1] == reason) {
			return fmt.Errorf("coverage evidence reason %q is invalid", reason)
		}
	}
	if evidence.Status == CoverageSatisfied && len(evidence.ReasonCodes) != 0 {
		return fmt.Errorf("satisfied coverage evidence has reasons")
	}
	if evidence.Status != CoverageSatisfied && len(evidence.ReasonCodes) == 0 {
		return fmt.Errorf("non-satisfied coverage evidence has no reason")
	}
	if evidence.Route != nil {
		if err := validateCoverageRouteEvidence(*evidence.Route); err != nil {
			return err
		}
	}
	if len(evidence.Effects) > MaxCoverageEffects || !sort.SliceIsSorted(evidence.Effects, func(i, j int) bool { return evidence.Effects[i].ID < evidence.Effects[j].ID }) {
		return fmt.Errorf("coverage evidence effects are not canonical")
	}
	for index, effect := range evidence.Effects {
		if index > 0 && evidence.Effects[index-1].ID == effect.ID {
			return fmt.Errorf("duplicate coverage evidence effect %q", effect.ID)
		}
		if err := validateCoverageEffectEvidence(effect); err != nil {
			return err
		}
	}
	if evidence.Cost != nil {
		if err := validateCoverageCostEvidence(*evidence.Cost); err != nil {
			return err
		}
	}
	return nil
}

func validateCoverageVerificationInput(input CoverageVerificationInput) error {
	if input.Route != nil {
		if err := validateCoverageRouteEvidence(*input.Route); err != nil {
			return err
		}
	}
	if len(input.Effects) > MaxCoverageEffects {
		return fmt.Errorf("coverage input effects exceed %d", MaxCoverageEffects)
	}
	seen := map[string]bool{}
	for _, effect := range input.Effects {
		if seen[effect.ID] {
			return fmt.Errorf("duplicate coverage input effect %q", effect.ID)
		}
		seen[effect.ID] = true
		if err := validateCoverageEffectEvidence(effect); err != nil {
			return err
		}
	}
	if input.Cost != nil {
		return validateCoverageCostEvidence(*input.Cost)
	}
	return nil
}

func validateCoverageRouteEvidence(route CoverageRouteEvidence) error {
	if !validCoverageStatus(route.Status) {
		return fmt.Errorf("coverage route status %q is invalid", route.Status)
	}
	if route.Status != CoverageUnavailable && route.Protocol != CoverageRouteProtocolAutoProbeOnceV2 {
		return fmt.Errorf("coverage route protocol %q is invalid", route.Protocol)
	}
	for name, value := range map[string]int{
		"candidate target count": route.CandidateTargetCount,
		"selected target count":  route.SelectedTargetCount,
		"executed target count":  route.ExecutedTargetCount,
	} {
		if value < 0 || value > core.MaxSessionEvents {
			return fmt.Errorf("coverage route %s is invalid", name)
		}
	}
	return nil
}

func validateCoverageEffectEvidence(effect CoverageEffectEvidence) error {
	if !caseIDPattern.MatchString(effect.ID) {
		return fmt.Errorf("coverage effect evidence id %q is invalid", effect.ID)
	}
	if err := core.ValidateNamespacedID(effect.CapabilityID); err != nil {
		return fmt.Errorf("coverage effect evidence %q capability: %w", effect.ID, err)
	}
	if effect.Occurrences < 0 || effect.Occurrences > core.MaxSessionEvents || !validCoverageStatus(effect.Status) {
		return fmt.Errorf("coverage effect evidence %q is invalid", effect.ID)
	}
	if effect.ReceiptLevel == "" && effect.Status == CoverageUnavailable {
		return nil
	}
	if !validReceiptLevel(effect.ReceiptLevel) {
		return fmt.Errorf("coverage effect evidence %q receipt level %q is invalid", effect.ID, effect.ReceiptLevel)
	}
	return nil
}

func validateCoverageCostEvidence(cost CoverageCostEvidence) error {
	if !validCoverageStatus(cost.Status) || cost.ReportedTotalTokens < 0 || cost.ReportedTotalTokens > core.MaxReportedTokensPerRun || cost.ModelCalls < 0 || cost.ModelCalls > core.HardMaxSteps {
		return fmt.Errorf("coverage cost evidence is invalid")
	}
	return nil
}

func validCoverageStatus(status CoverageStatus) bool {
	return status == CoverageSatisfied || status == CoverageFailed || status == CoverageUnavailable
}

func validReceiptLevel(level EffectReceiptLevel) bool {
	return level == EffectReceiptJournalCompleted || level == EffectReceiptProviderReadBack
}

func receiptSatisfies(actual, required EffectReceiptLevel) bool {
	if actual == EffectReceiptProviderReadBack {
		return required == EffectReceiptProviderReadBack || required == EffectReceiptJournalCompleted
	}
	return actual == EffectReceiptJournalCompleted && required == EffectReceiptJournalCompleted
}

func validCoverageRevision(value string) bool {
	if len(value) != sha256.Size*2 || value != strings.ToLower(value) {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func canonicalCoverageReasons(reasons []CoverageReasonCode) []CoverageReasonCode {
	if len(reasons) == 0 {
		return nil
	}
	seen := make(map[CoverageReasonCode]bool, len(reasons))
	out := make([]CoverageReasonCode, 0, len(reasons))
	for _, reason := range reasons {
		if !seen[reason] {
			seen[reason] = true
			out = append(out, reason)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// EvaluateCoverageGate aggregates persisted case coverage snapshots for the
// supplied frozen dataset definition. This helper treats a missing contract or
// evidence as unavailable when the policy requires it, preserving legacy JSON
// compatibility while remaining fail-closed for a strict release policy.
func EvaluateCoverageGate(dataset Dataset, run RunResult, policy CoverageGatePolicy) (CoverageGateResult, error) {
	if err := ValidateDataset(&dataset); err != nil {
		return CoverageGateResult{}, err
	}
	if err := ValidateRunResult(run, true); err != nil {
		return CoverageGateResult{}, err
	}
	if run.Status != RunCompleted {
		return CoverageGateResult{}, fmt.Errorf("coverage evaluation run is not completed")
	}
	if run.DatasetID != dataset.ID || run.DatasetVersion != dataset.Version || run.DatasetRevision != dataset.Revision {
		return CoverageGateResult{}, fmt.Errorf("coverage evaluation run uses a different dataset definition")
	}
	results := make(map[string]CaseResult, len(run.Cases))
	for _, result := range run.Cases {
		if _, exists := results[result.CaseID]; exists {
			return CoverageGateResult{}, fmt.Errorf("duplicate coverage case result %q", result.CaseID)
		}
		results[result.CaseID] = result
	}
	result := CoverageGateResult{Status: CoverageSatisfied}
	var reasons []CoverageReasonCode
	apply := func(status CoverageStatus, reason CoverageReasonCode) {
		switch status {
		case CoverageFailed:
			result.FailedCases++
			result.Status = CoverageFailed
		case CoverageUnavailable:
			result.UnavailableCases++
			if result.Status != CoverageFailed {
				result.Status = CoverageUnavailable
			}
		}
		reasons = append(reasons, reason)
	}
	for _, evalCase := range dataset.Cases {
		required := evalCase.Coverage != nil || policy.RequireContracts
		if !required {
			continue
		}
		result.RequiredCases++
		if evalCase.Coverage == nil {
			apply(CoverageUnavailable, CoverageReasonContractMissing)
			continue
		}
		caseResult, ok := results[evalCase.ID]
		if !ok || caseResult.Coverage == nil {
			apply(CoverageUnavailable, CoverageReasonCaseCoverageMissing)
			continue
		}
		coverage := caseResult.Coverage
		if coverage.ContractVersion != evalCase.Coverage.Version || coverage.ContractRevision != evalCase.Coverage.Revision {
			apply(CoverageUnavailable, CoverageReasonContractMismatch)
			continue
		}
		switch coverage.Status {
		case CoverageSatisfied:
			result.SatisfiedCases++
		case CoverageFailed:
			apply(CoverageFailed, CoverageReasonCaseCoverageFailed)
			reasons = append(reasons, coverage.ReasonCodes...)
		case CoverageUnavailable:
			apply(CoverageUnavailable, CoverageReasonCaseCoverageUnavailable)
			reasons = append(reasons, coverage.ReasonCodes...)
		}
	}
	result.ReasonCodes = canonicalCoverageReasons(reasons)
	return result, nil
}
