package server

import (
	"context"
	"errors"
	"net/http"
	"strconv"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
	"github.com/cc-auto-agent/harness-core/pkg/extensions/subagent"
	"github.com/cc-auto-agent/harness-core/pkg/storage"
)

// cancelDirectDelegations propagates an explicit parent cancellation to the
// recorded direct children only. The tenant filter is applied at the catalog
// query and checked again before each side effect; no link can cross tenants.
func (s *Server) cancelDirectDelegations(ctx context.Context, parentSessionID, parentRunID, tenantID string) error {
	if s.delegationCatalog == nil || parentRunID == "" || tenantID == "" {
		return nil
	}
	links, err := s.delegationCatalog.List(ctx, subagent.DelegationLinkFilter{
		ParentSessionID: parentSessionID, ParentRunID: parentRunID, TenantID: tenantID, Limit: 500,
	})
	if err != nil {
		return err
	}
	for _, link := range links {
		if link.TenantID != tenantID || link.ParentSessionID != parentSessionID || link.ParentRunID != parentRunID {
			continue
		}
		if s.runControl != nil {
			requested, requestErr := s.runControl.RequestRunCancel(ctx, link.ChildRunID)
			if requestErr != nil && !errors.Is(requestErr, storage.ErrRunNotFound) {
				return requestErr
			}
			if requested && s.approvals != nil {
				if _, closeErr := s.approvals.CancelRunApprovals(ctx, link.ChildRunID, "system:parent_cancel"); closeErr != nil {
					return closeErr
				}
			}
		}
		if s.sessions != nil {
			if child, loadErr := s.sessions.Load(ctx, link.ChildSessionID); loadErr == nil {
				if status, exists := child.RunStatus(link.ChildRunID); exists && status == core.RunWaitingApproval {
					if closeErr := s.closeWaitingSessionRun(ctx, link.ChildSessionID, link.ChildRunID); closeErr != nil {
						return closeErr
					}
				}
			}
		}
		s.runsMu.Lock()
		active := s.runs[link.ChildSessionID]
		s.runsMu.Unlock()
		if active != nil && active.runID == link.ChildRunID {
			active.cancel()
		}
	}
	return nil
}

type delegationLinkView struct {
	subagent.Link
	ChildStatus string `json:"child_status"`
}

// handleAdminDelegations exposes only stable delegation relationship metadata.
// Authorization is applied before querying so tenant administrators cannot use
// pagination or missing-child errors as a cross-tenant oracle.
func (s *Server) handleAdminDelegations(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok || !requireTenantOperator(w, principal) {
		return
	}
	if s.delegationCatalog == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "delegation catalog is not configured"})
		return
	}
	query := r.URL.Query()
	filter := subagent.DelegationLinkFilter{
		ParentSessionID: query.Get("parent_session_id"),
		ParentRunID:     query.Get("parent_run_id"),
		ChildSessionID:  query.Get("child_session_id"),
		TenantID:        query.Get("tenant"),
		Limit:           100,
	}
	if raw := query.Get("limit"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 1 || value > 500 {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "limit must be between 1 and 500"})
			return
		}
		filter.Limit = value
	}
	if raw := query.Get("offset"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 0 {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "offset must be non-negative"})
			return
		}
		filter.Offset = value
	}
	if roleOf(principal) == storage.RoleAccountTenantAdmin {
		if filter.TenantID != "" && filter.TenantID != principal.TenantID {
			writeJSON(w, http.StatusForbidden, map[string]string{"error": "tenant scope does not permit this query"})
			return
		}
		filter.TenantID = principal.TenantID
	}
	if err := subagent.ValidateLinkFilter(filter); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	links, err := s.delegationCatalog.List(r.Context(), filter)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "delegation catalog query failed"})
		return
	}
	views := make([]delegationLinkView, 0, len(links))
	for _, link := range links {
		status := "unknown"
		if s.sessions != nil {
			if session, loadErr := s.sessions.Load(r.Context(), link.ChildSessionID); loadErr == nil {
				if runStatus, exists := session.RunStatus(link.ChildRunID); exists {
					status = string(runStatus)
				}
			}
		}
		views = append(views, delegationLinkView{Link: link, ChildStatus: status})
	}
	writeJSON(w, http.StatusOK, map[string]any{"delegations": views, "limit": filter.Limit, "offset": filter.Offset})
}
