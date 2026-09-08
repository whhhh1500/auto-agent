package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	core "github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/evaluation"
	"github.com/whhhh1500/auto-agent/pkg/storage"
)

var errReleaseGateUnavailable = errors.New("release evaluation gate is unavailable")

type releaseEvaluationGateRequest struct {
	DatasetID                 string            `json:"dataset_id"`
	DatasetVersion            int               `json:"dataset_version"`
	TenantID                  string            `json:"tenant_id,omitempty"`
	SubjectID                 string            `json:"subject_id"`
	BaselineRunID             string            `json:"baseline_run_id,omitempty"`
	RequirePassed             *bool             `json:"require_passed,omitempty"`
	MinScore                  *float64          `json:"min_score,omitempty"`
	MaxRegression             *float64          `json:"max_regression,omitempty"`
	AllowCapabilities         []string          `json:"allow_capabilities,omitempty"`
	AllowBreakingCapabilities []string          `json:"allow_breaking_capabilities,omitempty"`
	Metadata                  map[string]string `json:"metadata,omitempty"`
}

type releaseGateEvaluation struct {
	CandidateRevision   string                `json:"candidate_revision"`
	BaseReleaseRevision string                `json:"base_release_revision"`
	CandidateRun        evaluation.RunResult  `json:"candidate_run"`
	BaselineRun         *evaluation.RunResult `json:"baseline_run,omitempty"`
	Gate                evaluation.GateResult `json:"gate"`
}

func (s *Server) evaluateReleaseCandidate(
	ctx context.Context,
	operator core.Principal,
	scope core.ScopePath,
	layer core.AgentProfileLayer,
	request releaseEvaluationGateRequest,
) (*releaseGateEvaluation, error) {
	if s.evaluationRunner == nil || s.evaluations == nil || s.Releases == nil {
		return nil, fmt.Errorf("%w: evaluation/release services are not configured", errReleaseGateUnavailable)
	}
	if strings.TrimSpace(request.SubjectID) == "" {
		return nil, fmt.Errorf("release evaluation subject_id is required")
	}
	tenantID := request.TenantID
	if roleOf(operator) == storage.RoleAccountAdmin {
		if strings.TrimSpace(tenantID) == "" {
			return nil, fmt.Errorf("release evaluation tenant_id is required for platform admins")
		}
	} else {
		if tenantID != "" && tenantID != operator.TenantID {
			return nil, fmt.Errorf("release evaluation cannot target another tenant")
		}
		if len(request.AllowCapabilities) > 0 {
			return nil, fmt.Errorf("only platform admins may explicitly allow non-idempotent release evaluation capabilities")
		}
		if len(request.AllowBreakingCapabilities) > 0 {
			return nil, fmt.Errorf("only platform admins may allow breaking capability changes")
		}
		tenantID = operator.TenantID
	}
	target, err := s.resolveEvaluationPrincipal(ctx, tenantID, request.SubjectID)
	if err != nil {
		return nil, err
	}
	if !scope.IsAncestorOf(target.Scope) {
		return nil, fmt.Errorf("release evaluation subject is outside the publish scope")
	}
	dataset, err := s.evaluations.GetDataset(ctx, request.DatasetID, request.DatasetVersion)
	if err != nil {
		return nil, err
	}
	candidateProfiles, candidateRevision, baseReleaseRevision, err := s.Releases.PrepareCandidateAtCurrentBaseline(scope, layer)
	if err != nil {
		return nil, err
	}
	baselineCapabilityRevision, baselineCapabilities, err := s.releaseCapabilityDeclarations(target, s.runtime.Profiles, layer.ProfileID)
	if err != nil {
		return nil, fmt.Errorf("resolve live release capabilities: %w", err)
	}
	candidateCapabilityRevision, candidateCapabilities, err := s.releaseCapabilityDeclarations(target, candidateProfiles, layer.ProfileID)
	if err != nil {
		return nil, fmt.Errorf("resolve candidate release capabilities: %w", err)
	}
	compatibility, err := evaluation.CompareCapabilitySnapshots(
		baselineCapabilityRevision, baselineCapabilities,
		candidateCapabilityRevision, candidateCapabilities,
		evaluation.CapabilityCompatibilityPolicy{AllowBreakingCapabilities: request.AllowBreakingCapabilities},
	)
	if err != nil {
		return nil, err
	}
	compatibilityRevision, err := evaluation.CapabilityCompatibilityRevision(compatibility)
	if err != nil {
		return nil, err
	}
	metadata := map[string]string{
		"release.candidate_revision":                candidateRevision,
		"release.base_revision":                     baseReleaseRevision,
		"release.profile_id":                        layer.ProfileID,
		"release.scope":                             scope.String(),
		"release.capabilities_compatible":           strconv.FormatBool(compatibility.Compatible),
		"release.baseline_capability_snapshot":      baselineCapabilityRevision,
		"release.candidate_capability_snapshot":     candidateCapabilityRevision,
		"release.capability_compatibility_revision": compatibilityRevision,
	}
	compositionMetadata := map[string]string{
		"release.candidate_revision":                candidateRevision,
		"release.base_revision":                     baseReleaseRevision,
		"release.profile_id":                        layer.ProfileID,
		"release.scope":                             scope.String(),
		"release.capabilities_compatible":           strconv.FormatBool(compatibility.Compatible),
		"release.baseline_capability_snapshot":      baselineCapabilityRevision,
		"release.candidate_capability_snapshot":     candidateCapabilityRevision,
		"release.capability_compatibility_revision": compatibilityRevision,
	}
	for key, value := range request.Metadata {
		metadata[key] = value
	}
	var baseline *evaluation.RunResult
	if request.BaselineRunID != "" {
		loaded, err := s.evaluations.GetRun(ctx, request.BaselineRunID)
		if err != nil {
			return nil, fmt.Errorf("load release baseline: %w", err)
		}
		if loaded.TenantID != tenantID || loaded.SubjectID != request.SubjectID ||
			loaded.DatasetID != dataset.ID || loaded.DatasetVersion != dataset.Version ||
			loaded.DatasetRevision != dataset.Revision || loaded.ProfileID != layer.ProfileID {
			return nil, fmt.Errorf("release baseline does not match target principal, profile, or dataset revision")
		}
		baseline = &loaded
	} else if request.MaxRegression != nil {
		baselineMetadata := map[string]string{
			"release.baseline_for_revision": candidateRevision,
			"release.profile_id":            layer.ProfileID,
		}
		baselineRun, err := s.evaluationRunner.Run(ctx, evaluation.RunRequest{
			DatasetID: dataset.ID, DatasetVersion: dataset.Version, Principal: target,
			ProfileID: layer.ProfileID, Metadata: baselineMetadata,
		})
		if err != nil {
			return nil, fmt.Errorf("run release baseline evaluation: %w", err)
		}
		baseline = &baselineRun
	}
	candidateRuntime := *s.runtime
	candidateRuntime.Profiles = candidateProfiles
	candidateRunner := *s.evaluationRunner
	candidateRunner.Runtime = &candidateRuntime
	candidateRun, err := candidateRunner.Run(ctx, evaluation.RunRequest{
		DatasetID: dataset.ID, DatasetVersion: dataset.Version, Principal: target,
		ProfileID: layer.ProfileID, BaselineRunID: request.BaselineRunID,
		AllowCapabilities: request.AllowCapabilities, CompositionMetadata: compositionMetadata, Metadata: metadata,
	})
	if err != nil {
		return nil, fmt.Errorf("run release candidate evaluation: %w", err)
	}
	requirePassed := true
	if request.RequirePassed != nil {
		requirePassed = *request.RequirePassed
	}
	gate, err := evaluation.EvaluateGate(candidateRun, baseline, evaluation.GatePolicy{
		RequirePassed: requirePassed, MinScore: request.MinScore, MaxRegression: request.MaxRegression,
	})
	if err != nil {
		return nil, err
	}
	gate.CapabilityCompatibility = &compatibility
	if !compatibility.Compatible {
		gate.Passed = false
		gate.Reasons = append(gate.Reasons, "candidate capability declarations are incompatible with live")
	}
	return &releaseGateEvaluation{
		CandidateRevision: candidateRevision, BaseReleaseRevision: baseReleaseRevision,
		CandidateRun: candidateRun, BaselineRun: baseline, Gate: gate,
	}, nil
}

func (s *Server) releaseCapabilityDeclarations(
	principal core.Principal,
	profiles *core.AgentProfileRegistry,
	profileID string,
) (string, []core.SnapshotCapability, error) {
	if profiles == nil || s.runtime == nil || s.runtime.Capabilities == nil {
		return "", nil, fmt.Errorf("release capability composition is not configured")
	}
	profile, err := profiles.Resolve(principal, principal.Scope, profileID)
	if err != nil {
		return "", nil, err
	}
	entries, err := s.runtime.Capabilities.Entries(principal.Scope)
	if err != nil {
		return "", nil, err
	}
	byID := make(map[string]core.SnapshotCapability, len(entries))
	for _, entry := range entries {
		byID[entry.Manifest.ID] = entry
	}
	selected := make([]core.SnapshotCapability, 0, len(profile.Capabilities))
	for _, id := range profile.Capabilities {
		entry, exists := byID[id]
		if !exists {
			return "", nil, fmt.Errorf("agent profile selects unavailable capability %q", id)
		}
		selected = append(selected, entry)
	}
	revision, err := evaluation.CapabilityDeclarationRevision(selected)
	if err != nil {
		return "", nil, err
	}
	return revision, selected, nil
}

func (s *Server) registerEvaluationRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/admin/evaluations/datasets", s.handlePutEvaluationDataset)
	mux.HandleFunc("GET /v1/admin/evaluations/datasets", s.handleListEvaluationDatasets)
	mux.HandleFunc("GET /v1/admin/evaluations/datasets/{datasetID}/versions/{version}", s.handleGetEvaluationDataset)
	mux.HandleFunc("POST /v1/admin/evaluations/runs", s.handleRunEvaluation)
	mux.HandleFunc("GET /v1/admin/evaluations/runs", s.handleListEvaluationRuns)
	mux.HandleFunc("GET /v1/admin/evaluations/runs/{evaluationRunID}", s.handleGetEvaluationRun)
	mux.HandleFunc("POST /v1/admin/evaluations/runs/{evaluationRunID}/resume", s.handleResumeEvaluation)
	mux.HandleFunc("POST /v1/admin/evaluations/runs/{evaluationRunID}/gate", s.handleEvaluationGate)
}

func (s *Server) handlePutEvaluationDataset(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	if !requireAdmin(w, principal) {
		return
	}
	if s.evaluations == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "evaluation store is not configured"})
		return
	}
	var dataset evaluation.Dataset
	if !s.decodeJSON(w, r, &dataset) {
		return
	}
	dataset.CreatedAt = time.Time{}
	if err := evaluation.ValidateDataset(&dataset); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	stored, created, err := s.evaluations.PutDataset(r.Context(), dataset)
	if err != nil {
		writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
		return
	}
	if created {
		s.recordAudit(r, principal, "evaluation.dataset.create", stored.ID, map[string]any{
			"version": stored.Version, "revision": stored.Revision, "cases": len(stored.Cases),
		})
		writeJSON(w, http.StatusCreated, stored)
		return
	}
	writeJSON(w, http.StatusOK, stored)
}

func (s *Server) handleListEvaluationDatasets(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	if !requireTenantOperator(w, principal) {
		return
	}
	if s.evaluations == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "evaluation store is not configured"})
		return
	}
	limit, ok := evaluationLimit(w, r)
	if !ok {
		return
	}
	datasets, err := s.evaluations.ListDatasets(r.Context(), r.URL.Query().Get("id"), limit)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"datasets": datasets})
}

func (s *Server) handleGetEvaluationDataset(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	if !requireTenantOperator(w, principal) {
		return
	}
	if s.evaluations == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "evaluation store is not configured"})
		return
	}
	version, err := strconv.Atoi(r.PathValue("version"))
	if err != nil || version < 1 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "dataset version must be positive"})
		return
	}
	dataset, err := s.evaluations.GetDataset(r.Context(), r.PathValue("datasetID"), version)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, evaluation.ErrDatasetNotFound) {
			status = http.StatusNotFound
		}
		writeJSON(w, status, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, dataset)
}

func (s *Server) handleRunEvaluation(w http.ResponseWriter, r *http.Request) {
	operator, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	if !requireTenantOperator(w, operator) {
		return
	}
	if s.evaluationRunner == nil || s.evaluations == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "evaluation runner is not configured"})
		return
	}
	if !s.ensureControlPlane(w, r) {
		return
	}
	var request struct {
		DatasetID           string            `json:"dataset_id"`
		DatasetVersion      int               `json:"dataset_version"`
		TenantID            string            `json:"tenant_id,omitempty"`
		SubjectID           string            `json:"subject_id"`
		ProfileID           string            `json:"profile_id,omitempty"`
		BaselineRunID       string            `json:"baseline_run_id,omitempty"`
		AllowCapabilities   []string          `json:"allow_capabilities,omitempty"`
		CompositionMetadata map[string]string `json:"composition_metadata,omitempty"`
		Metadata            map[string]string `json:"metadata,omitempty"`
	}
	if !s.decodeJSON(w, r, &request) {
		return
	}
	if strings.TrimSpace(request.SubjectID) == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "subject_id is required"})
		return
	}
	if _, err := s.evaluations.GetDataset(r.Context(), request.DatasetID, request.DatasetVersion); err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, evaluation.ErrDatasetNotFound) {
			status = http.StatusNotFound
		}
		writeJSON(w, status, map[string]string{"error": err.Error()})
		return
	}
	tenantID := request.TenantID
	if roleOf(operator) == storage.RoleAccountAdmin {
		if strings.TrimSpace(tenantID) == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "tenant_id is required for platform-admin evaluations"})
			return
		}
	} else {
		if tenantID != "" && tenantID != operator.TenantID {
			writeJSON(w, http.StatusForbidden, map[string]string{"error": "tenant administrator cannot evaluate another tenant"})
			return
		}
		tenantID = operator.TenantID
		if len(request.AllowCapabilities) > 0 {
			writeJSON(w, http.StatusForbidden, map[string]string{"error": "only platform admins may explicitly allow non-idempotent evaluation capabilities"})
			return
		}
	}
	target, err := s.resolveEvaluationPrincipal(r.Context(), tenantID, request.SubjectID)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if request.BaselineRunID != "" {
		baseline, err := s.evaluations.GetRun(r.Context(), request.BaselineRunID)
		if err != nil || baseline.TenantID != tenantID || baseline.DatasetID != request.DatasetID || baseline.DatasetVersion != request.DatasetVersion {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "baseline run is unavailable or uses another tenant/dataset version"})
			return
		}
	}
	run, runErr := s.evaluationRunner.Run(r.Context(), evaluation.RunRequest{
		DatasetID: request.DatasetID, DatasetVersion: request.DatasetVersion,
		Principal: target, ProfileID: request.ProfileID, BaselineRunID: request.BaselineRunID,
		AllowCapabilities: request.AllowCapabilities, CompositionMetadata: request.CompositionMetadata, Metadata: request.Metadata,
	})
	if run.ID != "" {
		s.recordAudit(r, operator, "evaluation.run", run.ID, map[string]any{
			"dataset": run.DatasetID, "version": run.DatasetVersion, "subject": target.SubjectID,
			"score": run.Score, "passed": run.Passed,
		})
	}
	if runErr != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"run": run, "error": runErr.Error()})
		return
	}
	writeJSON(w, http.StatusOK, run)
}

func (s *Server) handleListEvaluationRuns(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	if !requireTenantOperator(w, principal) {
		return
	}
	if s.evaluations == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "evaluation store is not configured"})
		return
	}
	limit, ok := evaluationLimit(w, r)
	if !ok {
		return
	}
	tenantID := r.URL.Query().Get("tenant")
	if roleOf(principal) != storage.RoleAccountAdmin {
		tenantID = principal.TenantID
	}
	query := evaluation.RunQuery{
		DatasetID: r.URL.Query().Get("dataset_id"), TenantID: tenantID,
		CompositionRevision: r.URL.Query().Get("composition_revision"),
		AssignmentRevision:  r.URL.Query().Get("assignment_revision"), Limit: limit,
	}
	if err := query.Validate(); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	var runs []evaluation.RunResult
	var err error
	if queryStore, supported := s.evaluations.(evaluation.QueryStore); supported {
		runs, err = queryStore.QueryRuns(r.Context(), query)
	} else if query.CompositionRevision != "" || query.AssignmentRevision != "" {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "evaluation revision queries are not supported by this store"})
		return
	} else {
		runs, err = s.evaluations.ListRuns(r.Context(), query.DatasetID, query.TenantID, query.Limit)
	}
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"runs": runs})
}

func (s *Server) handleGetEvaluationRun(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	if !requireTenantOperator(w, principal) {
		return
	}
	run, ok := s.loadEvaluationRun(w, r, principal)
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, run)
}

func (s *Server) handleResumeEvaluation(w http.ResponseWriter, r *http.Request) {
	operator, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	if !requireTenantOperator(w, operator) {
		return
	}
	if s.evaluationRunner == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "evaluation runner is not configured"})
		return
	}
	stored, ok := s.loadEvaluationRun(w, r, operator)
	if !ok {
		return
	}
	target, err := s.resolveEvaluationPrincipal(r.Context(), stored.TenantID, stored.SubjectID)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	resumed, resumeErr := s.evaluationRunner.Resume(r.Context(), stored.ID, target)
	if resumed.ID != "" {
		s.recordAudit(r, operator, "evaluation.resume", resumed.ID, map[string]any{
			"status": resumed.Status, "completed_cases": len(resumed.Cases), "total_cases": resumed.TotalCases,
		})
	}
	if resumeErr != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"run": resumed, "error": resumeErr.Error()})
		return
	}
	writeJSON(w, http.StatusOK, resumed)
}

func (s *Server) handleEvaluationGate(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	if !requireTenantOperator(w, principal) {
		return
	}
	current, ok := s.loadEvaluationRun(w, r, principal)
	if !ok {
		return
	}
	var request struct {
		BaselineRunID string   `json:"baseline_run_id,omitempty"`
		RequirePassed bool     `json:"require_passed,omitempty"`
		MinScore      *float64 `json:"min_score,omitempty"`
		MaxRegression *float64 `json:"max_regression,omitempty"`
	}
	if !s.decodeJSON(w, r, &request) {
		return
	}
	baselineID := request.BaselineRunID
	if baselineID == "" {
		baselineID = current.BaselineRunID
	}
	var baseline *evaluation.RunResult
	if baselineID != "" {
		loaded, err := s.evaluations.GetRun(r.Context(), baselineID)
		if err != nil || !canReadEvaluationRun(principal, loaded) {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "baseline evaluation run not found"})
			return
		}
		baseline = &loaded
	}
	gate, err := evaluation.EvaluateGate(current, baseline, evaluation.GatePolicy{
		RequirePassed: request.RequirePassed, MinScore: request.MinScore, MaxRegression: request.MaxRegression,
	})
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	outcome := "failed"
	if gate.Passed {
		outcome = "passed"
	}
	core.AddTelemetryCounter(s.telemetry, r.Context(), core.MetricEvaluationGates, 1,
		core.TelemetryAttributes{"evaluation.gate.outcome": outcome, "profile.id": current.ProfileID})
	writeJSON(w, http.StatusOK, gate)
}

func (s *Server) loadEvaluationRun(w http.ResponseWriter, r *http.Request, principal core.Principal) (evaluation.RunResult, bool) {
	if s.evaluations == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "evaluation store is not configured"})
		return evaluation.RunResult{}, false
	}
	run, err := s.evaluations.GetRun(r.Context(), r.PathValue("evaluationRunID"))
	if err != nil || !canReadEvaluationRun(principal, run) {
		status := http.StatusInternalServerError
		message := "evaluation run lookup failed"
		if errors.Is(err, evaluation.ErrRunNotFound) || (err == nil && !canReadEvaluationRun(principal, run)) {
			status, message = http.StatusNotFound, "evaluation run not found"
		} else if err != nil {
			message = err.Error()
		}
		writeJSON(w, status, map[string]string{"error": message})
		return evaluation.RunResult{}, false
	}
	return run, true
}

func canReadEvaluationRun(principal core.Principal, run evaluation.RunResult) bool {
	return roleOf(principal) == storage.RoleAccountAdmin ||
		(principal.TenantID != "" && principal.TenantID == run.TenantID)
}

func (s *Server) resolveEvaluationPrincipal(ctx context.Context, tenantID, subjectID string) (principal core.Principal, err error) {
	if s.runPrincipal == nil {
		return core.Principal{}, fmt.Errorf("current principal resolver is not configured")
	}
	defer func() {
		if recover() != nil {
			principal = core.Principal{}
			err = fmt.Errorf("evaluation principal resolver panicked")
		}
	}()
	principal, err = s.runPrincipal.ResolveRunPrincipal(ctx, tenantID, subjectID)
	if err != nil {
		return core.Principal{}, err
	}
	if principal.TenantID != tenantID || principal.SubjectID != subjectID {
		return core.Principal{}, fmt.Errorf("resolved evaluation principal identity does not match request")
	}
	return principal, nil
}

func evaluationLimit(w http.ResponseWriter, r *http.Request) (int, bool) {
	limit := 100
	if raw := r.URL.Query().Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > 500 {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "limit must be between 1 and 500"})
			return 0, false
		}
		limit = parsed
	}
	return limit, true
}
