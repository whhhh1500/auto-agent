package server

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
	"github.com/cc-auto-agent/harness-core/pkg/extensions/runner"
	"github.com/cc-auto-agent/harness-core/pkg/storage"
)

// handleAdminRunnerTaskList exposes the metadata-only task catalog to account
// and tenant administrators. It deliberately uses the TaskCatalog surface,
// never the full Task record, so task arguments, outcomes, idempotency data,
// and trace context remain private to the runner protocol.
func (s *Server) handleAdminRunnerTaskList(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	if !requireTenantOperator(w, principal) {
		return
	}
	if !s.requireRunnerTaskAdministration(w) {
		return
	}
	query, ok := parseAdminRunnerTaskQuery(w, r, principal, "", true)
	if !ok {
		return
	}
	tasks, total, err := s.runners.ListTasks(r.Context(), query)
	if !writeAdminRunnerCatalogError(w, err) {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"tasks": tasks, "total": total, "limit": query.Limit, "offset": query.Offset,
	})
}

// handleAdminRunnerTaskDetail returns one authorized metadata summary. An ID
// predicate is passed to ListTasks rather than loading a full Task, preventing
// a privileged administrative read from exposing runner payloads.
func (s *Server) handleAdminRunnerTaskDetail(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	if !requireTenantOperator(w, principal) {
		return
	}
	if !s.requireRunnerTaskAdministration(w) {
		return
	}
	task, _, ok := s.loadVisibleAdminRunnerTask(w, r, principal)
	if !ok {
		return
	}
	_, retry := s.retryDecision(r.Context(), task)
	writeJSON(w, http.StatusOK, map[string]any{"task": task, "actions": runnerTaskActions(task, retry)})
}

// handleAdminRunnerTaskCancel applies the queue's cancellation state machine
// after resolving a visible metadata summary. The second catalog lookup makes
// the returned task reflect the committed cancellation disposition.
func (s *Server) handleAdminRunnerTaskCancel(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	if !requireTenantOperator(w, principal) {
		return
	}
	if !s.requireRunnerTaskAdministration(w) {
		return
	}
	task, query, ok := s.loadVisibleAdminRunnerTask(w, r, principal)
	if !ok {
		return
	}
	disposition, err := s.runners.Cancel(r.Context(), task.ID)
	if err != nil {
		if errors.Is(err, runner.ErrTaskNotFound) {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "runner task not found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	tasks, _, err := s.runners.ListTasks(r.Context(), query)
	if !writeAdminRunnerCatalogError(w, err) {
		return
	}
	if len(tasks) != 1 {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "runner task not found"})
		return
	}
	updated := tasks[0]
	s.recordAudit(r, principal, "runner.task.cancel", task.ID, map[string]any{
		"task_id": task.ID, "capability": task.Capability,
		"previous_state": task.State, "disposition": disposition,
	})
	writeJSON(w, http.StatusOK, map[string]any{"disposition": disposition, "task": updated})
}

func (s *Server) handleAdminRunnerTaskRetry(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	if !requireTenantOperator(w, principal) || !s.requireRunnerTaskAdministration(w) {
		return
	}
	var request struct {
		Confirm bool   `json:"confirm"`
		Reason  string `json:"reason"`
	}
	if !s.decodeJSON(w, r, &request) {
		return
	}
	reason := strings.TrimSpace(request.Reason)
	if !request.Confirm || reason == "" || len(reason) > 1024 || strings.IndexFunc(reason, unicode.IsControl) >= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "confirm must be true and reason must be 1-1024 non-control characters"})
		return
	}
	summary, _, ok := s.loadVisibleAdminRunnerTask(w, r, principal)
	if !ok {
		return
	}
	full, decision := s.retryDecision(r.Context(), summary)
	if !decision.Allowed {
		writeJSON(w, retryDecisionStatus(decision), map[string]string{"error": decision.Description})
		return
	}
	retried, created, err := s.runners.Retry(r.Context(), full.ID)
	if err != nil {
		if errors.Is(err, runner.ErrCatalogUnsupported) {
			writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "runner retry is not supported"})
		} else {
			writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
		}
		return
	}
	s.recordAudit(r, principal, "runner.task.retry", full.ID, map[string]any{"reason": reason, "original_task_id": full.ID, "retry_task_id": retried.ID, "created": created})
	writeJSON(w, http.StatusOK, map[string]any{"original": summary, "retry": taskSummaryForRetry(retried), "created": created})
}

type runnerTaskAction struct {
	Allowed     bool   `json:"allowed"`
	Reason      string `json:"reason"`
	Description string `json:"description"`
}
type runnerTaskActionSet struct {
	Cancel runnerTaskAction `json:"cancel"`
	Retry  runnerTaskAction `json:"retry"`
}

func runnerTaskActions(task runner.TaskSummary, retry runnerTaskAction) runnerTaskActionSet {
	cancel := runnerTaskAction{Allowed: task.State == runner.TaskQueued || task.State == runner.TaskClaimed, Reason: "task_terminal", Description: "task is already terminal"}
	if cancel.Allowed {
		cancel.Reason, cancel.Description = "ok", "task can be cancelled"
	}
	return runnerTaskActionSet{Cancel: cancel, Retry: retry}
}

func (s *Server) retryDecision(ctx context.Context, summary runner.TaskSummary) (runner.Task, runnerTaskAction) {
	deny := func(reason, description string) (runner.Task, runnerTaskAction) {
		return runner.Task{}, runnerTaskAction{Reason: reason, Description: description}
	}
	if summary.State != runner.TaskFailed {
		return deny("task_not_failed", "only failed runner tasks can be retried")
	}
	full, err := s.runners.Task(ctx, summary.ID)
	if err != nil {
		return deny("task_unavailable", "runner task is unavailable")
	}
	if full.State != summary.State || full.TenantID != summary.TenantID || full.SubjectID != summary.SubjectID || full.Scope != summary.Scope || full.Capability != summary.Capability {
		return deny("task_changed", "runner task changed while retrying")
	}
	scope, err := storage.ParseScopePath(full.Scope)
	if err != nil {
		return deny("invalid_scope", "runner task has invalid scope")
	}
	if s.runPrincipal == nil {
		return deny("principal_resolver_unavailable", "runner retry authorization is not configured")
	}
	taskPrincipal, err := s.runPrincipal.ResolveRunPrincipal(ctx, full.TenantID, full.SubjectID)
	if err != nil || taskPrincipal.TenantID != full.TenantID || taskPrincipal.SubjectID != full.SubjectID || taskPrincipal.Scope.String() != scope.String() {
		return deny("authorization_revoked", "runner task is no longer authorized")
	}
	if s.runtime == nil || s.runtime.Capabilities == nil {
		return deny("capability_unavailable", "runner capability is no longer available")
	}
	snapshot, err := (core.CapabilityResolver{Registry: s.runtime.Capabilities}).Resolve(taskPrincipal, scope)
	if err != nil {
		return deny("authorization_revoked", "runner task is no longer authorized")
	}
	manifest, found := snapshot.ManifestFor(full.Capability)
	if !found || !snapshot.Authorized(full.Capability) {
		return deny("capability_unavailable", "runner capability is no longer authorized")
	}
	if !manifest.Idempotent {
		return deny("capability_not_idempotent", "runner capability is not idempotent")
	}
	return full, runnerTaskAction{Allowed: true, Reason: "ok", Description: "failed idempotent task can be retried"}
}

func retryDecisionStatus(decision runnerTaskAction) int {
	if decision.Reason == "principal_resolver_unavailable" {
		return http.StatusNotImplemented
	}
	if decision.Reason == "task_unavailable" {
		return http.StatusNotFound
	}
	if decision.Reason == "authorization_revoked" {
		return http.StatusForbidden
	}
	return http.StatusConflict
}

func taskSummaryForRetry(task runner.Task) runner.TaskSummary {
	return runner.TaskSummary{ID: task.ID, RetriedFromID: task.RetriedFromID, Capability: task.Capability, TenantID: task.TenantID, SubjectID: task.SubjectID, Scope: task.Scope, State: task.State, AvailableAt: task.AvailableAt, MaxAttempts: task.MaxAttempts, CreatedAt: task.CreatedAt, UpdatedAt: task.UpdatedAt, HasTraceContext: task.TraceContext.TraceParent != "" || task.TraceContext.TraceState != ""}
}

// handleAdminRunnerRecover runs one explicit expiration-recovery pass. It is a
// platform operation because it affects tasks from every tenant.
func (s *Server) handleAdminRunnerRecover(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	if !requireAdmin(w, principal) {
		return
	}
	if !s.requireRunnerTaskAdministration(w) {
		return
	}
	requeued, terminal, err := s.runners.RecoverExpired(r.Context(), time.Now().UTC())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	s.recordAudit(r, principal, "runner.tasks.recover", "runners/tasks", map[string]any{
		"requeued": requeued, "terminal": terminal,
	})
	writeJSON(w, http.StatusOK, map[string]any{"requeued": requeued, "terminal": terminal})
}

func (s *Server) requireRunnerTaskAdministration(w http.ResponseWriter) bool {
	if s.runners == nil || s.runners.Store == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "runner task administration is not configured"})
		return false
	}
	return true
}

// loadVisibleAdminRunnerTask performs an ID-filtered TaskCatalog lookup under
// the caller's tenant and scope boundary. Missing, filtered, and unauthorized
// IDs intentionally share the same 404 response.
func (s *Server) loadVisibleAdminRunnerTask(w http.ResponseWriter, r *http.Request, principal core.Principal) (runner.TaskSummary, runner.TaskQuery, bool) {
	query, ok := parseAdminRunnerTaskQuery(w, r, principal, r.PathValue("taskID"), false)
	if !ok {
		return runner.TaskSummary{}, runner.TaskQuery{}, false
	}
	tasks, _, err := s.runners.ListTasks(r.Context(), query)
	if !writeAdminRunnerCatalogError(w, err) {
		return runner.TaskSummary{}, runner.TaskQuery{}, false
	}
	if len(tasks) != 1 {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "runner task not found"})
		return runner.TaskSummary{}, runner.TaskQuery{}, false
	}
	return tasks[0], query, true
}

// parseAdminRunnerTaskQuery validates the catalog selectors and imposes the
// caller's tenancy boundary. Platform admins may omit tenant and scope; tenant
// admins are always limited to their tenant scope or one of its descendants.
func parseAdminRunnerTaskQuery(w http.ResponseWriter, r *http.Request, principal core.Principal, taskID string, paginated bool) (runner.TaskQuery, bool) {
	values := r.URL.Query()
	query := runner.TaskQuery{
		ID:         taskID,
		TenantID:   values.Get("tenant"),
		SubjectID:  values.Get("subject_id"),
		Capability: values.Get("capability"),
		WorkerID:   values.Get("worker_id"),
	}
	if roleOf(principal) == storage.RoleAccountAdmin {
		if rawScope := values.Get("scope"); rawScope != "" {
			scope, err := storage.ParseScopePath(rawScope)
			if err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
				return runner.TaskQuery{}, false
			}
			query.ScopePrefix = scope.String()
		}
	} else {
		requestedTenant := strings.TrimSpace(values.Get("tenant"))
		if requestedTenant != "" && requestedTenant != principal.TenantID {
			writeJSON(w, http.StatusForbidden, map[string]string{"error": "tenant administrator cannot query another tenant"})
			return runner.TaskQuery{}, false
		}
		query.TenantID = principal.TenantID
		query.ScopePrefix = principal.Scope.String()
		if rawScope := values.Get("scope"); rawScope != "" {
			scope, err := storage.ParseScopePath(rawScope)
			if err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
				return runner.TaskQuery{}, false
			}
			if !principal.Scope.IsAncestorOf(scope) {
				writeJSON(w, http.StatusForbidden, map[string]string{"error": "tenant administrator cannot query an ancestor or sibling scope"})
				return runner.TaskQuery{}, false
			}
			query.ScopePrefix = scope.String()
		}
	}

	for _, raw := range values["state"] {
		for _, state := range strings.Split(raw, ",") {
			if state = strings.TrimSpace(state); state != "" {
				query.States = append(query.States, runner.TaskState(state))
			}
		}
	}
	if paginated {
		if raw := values.Get("limit"); raw != "" {
			limit, err := strconv.Atoi(raw)
			if err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": "limit must be an integer"})
				return runner.TaskQuery{}, false
			}
			query.Limit = limit
		}
		if raw := values.Get("offset"); raw != "" {
			offset, err := strconv.Atoi(raw)
			if err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": "offset must be an integer"})
				return runner.TaskQuery{}, false
			}
			query.Offset = offset
		}
	} else {
		query.Limit = 1
	}
	normalized, err := query.Normalize()
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return runner.TaskQuery{}, false
	}
	return normalized, true
}

func writeAdminRunnerCatalogError(w http.ResponseWriter, err error) bool {
	if err == nil {
		return true
	}
	if errors.Is(err, runner.ErrCatalogUnsupported) {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "runner task catalog is not supported"})
		return false
	}
	writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
	return false
}
