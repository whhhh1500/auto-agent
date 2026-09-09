package evaluation

import (
	"encoding/json"
	"testing"
	"time"
)

func TestCoverageContractCanonicalRevisionFreezesDataset(t *testing.T) {
	contract := CoverageContract{
		Version:           CoverageContractV1,
		RequireCasePassed: true,
		Effects: []EffectCoverageRequirement{
			{ID: "write", CapabilityID: "example.write", MinOccurrences: 1, MaxOccurrences: 1, ReceiptLevel: EffectReceiptJournalCompleted},
			{ID: "read", CapabilityID: "example.read", MinOccurrences: 0, MaxOccurrences: 2, ReceiptLevel: EffectReceiptProviderReadBack},
		},
	}
	if err := ValidateCoverageContract(&contract); err != nil {
		t.Fatal(err)
	}
	if contract.Revision == "" || contract.Effects[0].ID != "read" {
		t.Fatalf("contract was not canonicalized: %#v", contract)
	}
	reordered := CoverageContract{
		Version:           CoverageContractV1,
		RequireCasePassed: true,
		Effects: []EffectCoverageRequirement{
			{ID: "read", CapabilityID: "example.read", MinOccurrences: 0, MaxOccurrences: 2, ReceiptLevel: EffectReceiptProviderReadBack},
			{ID: "write", CapabilityID: "example.write", MinOccurrences: 1, MaxOccurrences: 1, ReceiptLevel: EffectReceiptJournalCompleted},
		},
	}
	revision, err := CoverageContractRevision(reordered)
	if err != nil {
		t.Fatal(err)
	}
	if revision != contract.Revision {
		t.Fatalf("canonical revision=%q want=%q", revision, contract.Revision)
	}

	withCoverage := baseDataset("eval.lookup")
	withCoverage.Cases[0].Coverage = &contract
	if err := ValidateDataset(&withCoverage); err != nil {
		t.Fatal(err)
	}
	withoutCoverage := baseDataset("eval.lookup")
	if err := ValidateDataset(&withoutCoverage); err != nil {
		t.Fatal(err)
	}
	if withCoverage.Revision == withoutCoverage.Revision {
		t.Fatalf("dataset revision ignored coverage contract: %q", withCoverage.Revision)
	}
	rawCoverage := contract
	rawCoverage.Revision = ""
	rawCoverage.Effects = []EffectCoverageRequirement{contract.Effects[1], contract.Effects[0]}
	rawDataset := withCoverage
	rawDataset.Revision = ""
	rawDataset.Cases = append([]Case(nil), withCoverage.Cases...)
	rawDataset.Cases[0].Coverage = &rawCoverage
	rawRevision, err := DatasetRevision(rawDataset)
	if err != nil {
		t.Fatal(err)
	}
	if rawRevision != withCoverage.Revision {
		t.Fatalf("direct dataset revision=%q want canonical %q", rawRevision, withCoverage.Revision)
	}
}

func TestCoverageContractRejectsUnknownVersion(t *testing.T) {
	contract := CoverageContract{Version: "coverage-contract/v2", RequireCasePassed: true}
	if err := ValidateCoverageContract(&contract); err == nil {
		t.Fatal("unknown coverage contract version was accepted")
	}
	dataset := baseDataset("eval.lookup")
	dataset.Cases[0].Coverage = &contract
	if err := ValidateDataset(&dataset); err == nil {
		t.Fatal("dataset accepted unknown coverage contract version")
	}
}

func TestCoverageContractRejectsDuplicateEffectCapability(t *testing.T) {
	contract := CoverageContract{
		Version: CoverageContractV1,
		Effects: []EffectCoverageRequirement{
			{ID: "first", CapabilityID: "example.write", MaxOccurrences: 1, ReceiptLevel: EffectReceiptJournalCompleted},
			{ID: "second", CapabilityID: "example.write", MaxOccurrences: 1, ReceiptLevel: EffectReceiptJournalCompleted},
		},
	}
	if err := ValidateCoverageContract(&contract); err == nil {
		t.Fatal("duplicate coverage effect capability was accepted")
	}
}

func TestVerifyCoverageSatisfiesRouteEffectAndCostRequirements(t *testing.T) {
	maxTokens, maxCalls := int64(12), 1
	contract := CoverageContract{
		Version:           CoverageContractV1,
		RequireCasePassed: true,
		Route: &RouteCoverageRequirement{
			Protocol: CoverageRouteProtocolAutoProbeOnceV2, RequireVerifiedIdentity: true,
			RequireCompleteTargets: true, RequireExactOnce: true,
		},
		Effects: []EffectCoverageRequirement{{
			ID: "write", CapabilityID: "example.write", MinOccurrences: 1, MaxOccurrences: 1,
			ReceiptLevel: EffectReceiptJournalCompleted,
		}},
		Cost: &CostCoverageRequirement{
			RequireCompleteLedger: true, MaxReportedTotalTokens: &maxTokens, MaxModelCalls: &maxCalls,
		},
	}
	evidence, err := VerifyCoverage(contract, CoverageVerificationInput{
		CasePassed: true,
		Route: &CoverageRouteEvidence{
			Status: CoverageSatisfied, Protocol: CoverageRouteProtocolAutoProbeOnceV2, IdentityVerified: true,
			CandidateTargetCount: 2, SelectedTargetCount: 2, ExecutedTargetCount: 2, ExactOnce: true,
		},
		Effects: []CoverageEffectEvidence{{
			ID: "write", CapabilityID: "example.write", Occurrences: 1,
			ReceiptLevel: EffectReceiptProviderReadBack, Status: CoverageSatisfied,
		}},
		Cost: &CoverageCostEvidence{Status: CoverageSatisfied, LedgerComplete: true, ReportedTotalTokens: 12, ModelCalls: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if evidence.Status != CoverageSatisfied || evidence.ContractRevision == "" || len(evidence.ReasonCodes) != 0 || len(evidence.Effects) != 1 {
		t.Fatalf("coverage evidence=%#v", evidence)
	}
}

func TestVerifyCoverageDistinguishesFailedFromUnavailable(t *testing.T) {
	providerContract := CoverageContract{
		Version: CoverageContractV1,
		Effects: []EffectCoverageRequirement{{
			ID: "write", CapabilityID: "example.write", MinOccurrences: 1, MaxOccurrences: 1,
			ReceiptLevel: EffectReceiptProviderReadBack,
		}},
	}
	for name, input := range map[string]CoverageVerificationInput{
		"known cardinality contradiction": {
			Effects: []CoverageEffectEvidence{{
				ID: "write", CapabilityID: "example.write", Occurrences: 0,
				ReceiptLevel: EffectReceiptProviderReadBack, Status: CoverageSatisfied,
			}},
		},
		"receipt authority unavailable": {
			Effects: []CoverageEffectEvidence{{
				ID: "write", CapabilityID: "example.write", Occurrences: 1,
				ReceiptLevel: EffectReceiptJournalCompleted, Status: CoverageSatisfied,
			}},
		},
		"effect source unavailable": {},
	} {
		t.Run(name, func(t *testing.T) {
			evidence, err := VerifyCoverage(providerContract, input)
			if err != nil {
				t.Fatal(err)
			}
			switch name {
			case "known cardinality contradiction":
				if evidence.Status != CoverageFailed || !containsCoverageReason(evidence.ReasonCodes, CoverageReasonEffectCardinalityFailed) {
					t.Fatalf("coverage evidence=%#v", evidence)
				}
			default:
				if evidence.Status != CoverageUnavailable {
					t.Fatalf("coverage evidence=%#v", evidence)
				}
			}
		})
	}
}

func TestCoverageGateIsStrictWithoutBreakingLegacyJSON(t *testing.T) {
	dataset := baseDataset("eval.lookup")
	dataset.Cases[0].Coverage = &CoverageContract{Version: CoverageContractV1, RequireCasePassed: true}
	if err := ValidateDataset(&dataset); err != nil {
		t.Fatal(err)
	}
	coverage, err := VerifyCoverage(*dataset.Cases[0].Coverage, CoverageVerificationInput{CasePassed: true})
	if err != nil {
		t.Fatal(err)
	}
	caseResult := memoryStoreCaseResult(dataset.Cases[0].ID)
	caseResult.Coverage = &coverage
	run := coverageGateRun(dataset, caseResult)
	gate, err := EvaluateCoverageGate(dataset, run, CoverageGatePolicy{RequireContracts: true})
	if err != nil || gate.Status != CoverageSatisfied || gate.RequiredCases != 1 || gate.SatisfiedCases != 1 {
		t.Fatalf("coverage gate=%#v err=%v", gate, err)
	}

	run.Cases[0].Coverage = nil
	gate, err = EvaluateCoverageGate(dataset, run, CoverageGatePolicy{RequireContracts: true})
	if err != nil || gate.Status != CoverageUnavailable || gate.UnavailableCases != 1 || !containsCoverageReason(gate.ReasonCodes, CoverageReasonCaseCoverageMissing) {
		t.Fatalf("missing coverage gate=%#v err=%v", gate, err)
	}

	legacyResult := memoryStoreCaseResult("case-legacy")
	raw, err := json.Marshal(legacyResult)
	if err != nil {
		t.Fatal(err)
	}
	var restored CaseResult
	if err := json.Unmarshal(raw, &restored); err != nil {
		t.Fatal(err)
	}
	if restored.Coverage != nil || ValidateCaseResult(restored) != nil {
		t.Fatalf("legacy case result was not compatible: %#v", restored)
	}

	legacyDataset := baseDataset("eval.lookup")
	if err := ValidateDataset(&legacyDataset); err != nil {
		t.Fatal(err)
	}
	legacyRun := coverageGateRun(legacyDataset, memoryStoreCaseResult(legacyDataset.Cases[0].ID))
	gate, err = EvaluateCoverageGate(legacyDataset, legacyRun, CoverageGatePolicy{})
	if err != nil || gate.Status != CoverageSatisfied || gate.RequiredCases != 0 {
		t.Fatalf("permissive legacy gate=%#v err=%v", gate, err)
	}
	gate, err = EvaluateCoverageGate(legacyDataset, legacyRun, CoverageGatePolicy{RequireContracts: true})
	if err != nil || gate.Status != CoverageUnavailable || !containsCoverageReason(gate.ReasonCodes, CoverageReasonContractMissing) {
		t.Fatalf("strict legacy gate=%#v err=%v", gate, err)
	}
}

func TestMemoryStoreDeepCopiesCoverageContract(t *testing.T) {
	store := NewMemoryStore()
	dataset := baseDataset("eval.lookup")
	dataset.Cases[0].Coverage = &CoverageContract{
		Version: CoverageContractV1,
		Effects: []EffectCoverageRequirement{{
			ID: "write", CapabilityID: "example.write", MaxOccurrences: 1, ReceiptLevel: EffectReceiptJournalCompleted,
		}},
	}
	stored, created, err := store.PutDataset(t.Context(), dataset)
	if err != nil || !created {
		t.Fatalf("put dataset: stored=%#v created=%t err=%v", stored, created, err)
	}
	dataset.Cases[0].Coverage.Effects[0].CapabilityID = "example.mutated"
	if got := stored.Cases[0].Coverage.Effects[0].CapabilityID; got != "example.write" {
		t.Fatalf("returned dataset aliased caller coverage: %q", got)
	}
	loaded, err := store.GetDataset(t.Context(), stored.ID, stored.Version)
	if err != nil {
		t.Fatal(err)
	}
	if got := loaded.Cases[0].Coverage.Effects[0].CapabilityID; got != "example.write" {
		t.Fatalf("stored dataset aliased caller coverage: %q", got)
	}
	loaded.Cases[0].Coverage.Effects[0].CapabilityID = "example.returned"
	reloaded, err := store.GetDataset(t.Context(), stored.ID, stored.Version)
	if err != nil {
		t.Fatal(err)
	}
	if got := reloaded.Cases[0].Coverage.Effects[0].CapabilityID; got != "example.write" {
		t.Fatalf("returned dataset aliased stored coverage: %q", got)
	}
}

func coverageGateRun(dataset Dataset, result CaseResult) RunResult {
	return RunResult{
		ID: "evalrun-coverage-gate", DatasetID: dataset.ID, DatasetVersion: dataset.Version, DatasetRevision: dataset.Revision,
		TenantID: "acme", SubjectID: "alice", ProfileID: dataset.ProfileID, Status: RunCompleted,
		Score: 1, Passed: true, TotalCases: 1, PassedCases: 1, Cases: []CaseResult{result},
		CreatedAt: time.Unix(1, 0).UTC(), CompletedAt: time.Unix(2, 0).UTC(),
	}
}

func containsCoverageReason(reasons []CoverageReasonCode, want CoverageReasonCode) bool {
	for _, reason := range reasons {
		if reason == want {
			return true
		}
	}
	return false
}
