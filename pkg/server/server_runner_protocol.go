package server

import (
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	runnerhttp "github.com/whhhh1500/auto-agent/pkg/adapter/httpapi/runner"
	core "github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/extensions/runner"
)

// handleRunnerClaim hands the next queued task to a private runner.
type runnerClaimResponse struct {
	ID              string                      `json:"id"`
	Capability      string                      `json:"capability"`
	IdempotencyKey  string                      `json:"idempotency_key,omitempty"`
	Args            map[string]any              `json:"args,omitempty"`
	TraceContext    *core.TelemetryTraceContext `json:"trace_context,omitempty"`
	CancelRequested bool                        `json:"cancel_requested,omitempty"`
	LeaseExpiresAt  time.Time                   `json:"lease_expires_at"`
	Attempt         int                         `json:"attempt"`
	MaxAttempts     int                         `json:"max_attempts"`
	Generation      int64                       `json:"generation"`
}

func (s *Server) handleRunnerClaim(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticateRunnerPrincipal(w, r)
	if !ok {
		return
	}
	if s.runners == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "runner surface is not configured"})
		return
	}
	request, ok := s.decodeRunnerClaimRequest(w, r)
	if !ok {
		return
	}
	command, err := request.ToCommand(principal.SubjectID, s.runners.LeaseTTL)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	requested := command.Capabilities
	capabilities := requested
	if s.runnerAuth != nil {
		rawGrant, present := principal.Attributes[RunnerCapabilitiesAttribute]
		if !present || strings.TrimSpace(rawGrant) == "" {
			writeJSON(w, http.StatusForbidden, map[string]string{"error": "runner capability grant is required"})
			return
		}
		allowed, all, err := ParseRunnerCapabilitiesGrant(rawGrant)
		if err != nil {
			writeJSON(w, http.StatusForbidden, map[string]string{"error": "runner capability grant is invalid"})
			return
		}
		if !all {
			if len(requested) == 0 {
				capabilities = allowed
			} else if !runnerCapabilitySubset(requested, allowed) {
				writeJSON(w, http.StatusForbidden, map[string]string{"error": "requested capabilities exceed runner grant"})
				return
			}
		}
	}
	command.Capabilities = capabilities
	task, claimed, err := s.runners.Claim(r.Context(), command)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if !claimed {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	writeJSON(w, http.StatusOK, runnerClaimResponse{
		ID: task.ID, Capability: task.Capability, IdempotencyKey: task.IdempotencyKey,
		Args: task.Args, TraceContext: runnerTraceContextResponse(task.TraceContext),
		CancelRequested: task.CancelRequested,
		LeaseExpiresAt:  task.LeaseExpiresAt, Attempt: task.Attempt,
		MaxAttempts: task.MaxAttempts, Generation: task.Generation,
	})
}

func runnerTraceContextResponse(traceContext core.TelemetryTraceContext) *core.TelemetryTraceContext {
	if traceContext.TraceParent == "" && traceContext.TraceState == "" {
		return nil
	}
	return &traceContext
}

// ParseRunnerCapabilitiesGrant validates and canonicalizes a trusted runner
// capability grant. A grant is either the standalone wildcard "*" or at most
// runner.MaxClaimCapabilities comma-separated namespaced capability IDs.
func ParseRunnerCapabilitiesGrant(raw string) (capabilities []string, all bool, err error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, false, fmt.Errorf("runner capability grant is empty")
	}
	if raw == "*" {
		return nil, true, nil
	}
	capabilities, err = normalizeRunnerClaimCapabilities(strings.Split(raw, ","))
	if err != nil {
		return nil, false, err
	}
	if len(capabilities) == 0 {
		return nil, false, fmt.Errorf("runner capability grant is empty")
	}
	return capabilities, false, nil
}

func normalizeRunnerClaimCapabilities(values []string) ([]string, error) {
	if len(values) == 0 {
		return nil, nil
	}
	if len(values) > runner.MaxClaimCapabilities {
		return nil, fmt.Errorf("runner claim supports at most %d capabilities", runner.MaxClaimCapabilities)
	}
	unique := make(map[string]struct{}, len(values))
	for _, value := range values {
		capability := strings.TrimSpace(value)
		if capability == "*" {
			return nil, fmt.Errorf("runner capability selector cannot include %q", capability)
		}
		if err := core.ValidateNamespacedID(capability); err != nil {
			return nil, fmt.Errorf("invalid runner capability %q: %w", capability, err)
		}
		unique[capability] = struct{}{}
	}
	capabilities := make([]string, 0, len(unique))
	for capability := range unique {
		capabilities = append(capabilities, capability)
	}
	sort.Strings(capabilities)
	return capabilities, nil
}

func runnerCapabilitySubset(requested, allowed []string) bool {
	for _, capability := range requested {
		index := sort.SearchStrings(allowed, capability)
		if index == len(allowed) || allowed[index] != capability {
			return false
		}
	}
	return true
}

func (s *Server) decodeRunnerClaimRequest(w http.ResponseWriter, r *http.Request) (runnerhttp.ClaimRequest, bool) {
	var request runnerhttp.ClaimRequest
	if !s.decodeOptionalJSON(w, r, &request) {
		return runnerhttp.ClaimRequest{}, false
	}
	return request, true
}

func (s *Server) handleRunnerRenew(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticateRunnerPrincipal(w, r)
	if !ok {
		return
	}
	if s.runners == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "runner surface is not configured"})
		return
	}
	var request runnerhttp.GenerationRequest
	if !s.decodeJSON(w, r, &request) {
		return
	}
	if err := request.Validate(); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	renewed, err := s.runners.Renew(r.Context(), r.PathValue("id"), principal.SubjectID, request.Generation, 0)
	if err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, runner.ErrTaskNotFound) {
			status = http.StatusNotFound
		}
		writeJSON(w, status, map[string]string{"error": err.Error()})
		return
	}
	if !renewed {
		writeJSON(w, http.StatusConflict, map[string]string{"error": "runner claim is stale or expired"})
		return
	}
	task, err := s.runners.Task(r.Context(), r.PathValue("id"))
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "renewed", "cancel_requested": task.CancelRequested, "lease_expires_at": task.LeaseExpiresAt})
}

// handleRunnerComplete delivers one runner's outcome.
func (s *Server) handleRunnerComplete(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticateRunnerPrincipal(w, r)
	if !ok {
		return
	}
	if s.runners == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "runner surface is not configured"})
		return
	}
	var request runnerhttp.CompletionRequest
	if !s.decodeJSON(w, r, &request) {
		return
	}
	if err := request.Validate(); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	task, completed, err := s.runners.Complete(r.Context(), r.PathValue("id"), principal.SubjectID, request.Generation, request.ToResult())
	if err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, runner.ErrTaskNotFound) {
			status = http.StatusNotFound
		}
		writeJSON(w, status, map[string]string{"error": err.Error()})
		return
	}
	if !completed && task.State != runner.TaskCompleted {
		writeJSON(w, http.StatusConflict, map[string]string{"error": "runner claim is stale or expired"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "delivered"})
}

func (s *Server) authenticateRunnerPrincipal(w http.ResponseWriter, r *http.Request) (core.Principal, bool) {
	if s.runnerAuth != nil {
		principal, err := s.runnerAuth.Authenticate(r)
		if err != nil {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": err.Error()})
			return core.Principal{}, false
		}
		if strings.TrimSpace(principal.SubjectID) == "" {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "runner identity is missing subject"})
			return core.Principal{}, false
		}
		return principal, true
	}
	principal, ok := s.authenticate(w, r)
	if !ok || !requireAdmin(w, principal) {
		return core.Principal{}, false
	}
	return principal, true
}

func (s *Server) authenticateRunner(w http.ResponseWriter, r *http.Request) bool {
	_, ok := s.authenticateRunnerPrincipal(w, r)
	return ok
}
