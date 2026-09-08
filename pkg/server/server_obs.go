// Observability surface: watch rules configured from the console, matched
// against every run's event stream, and recorded hits that link back to the
// session for review and backtesting. Matching is best-effort: rule
// evaluation and hit recording never alter run semantics.
package server

import (
	"context"
	"encoding/json"
	"github.com/whhhh1500/auto-agent/pkg/evaluation"
	"github.com/whhhh1500/auto-agent/pkg/storage"
	"net/http"
	"strconv"
	"sync"
	"time"

	core "github.com/whhhh1500/auto-agent/pkg/core"
)

// obsEnabledRules returns the cached enabled rule set.
func (s *Server) obsEnabledRules(ctx context.Context) []storage.ObsRule {
	if s.obsMatcher == nil {
		return nil
	}
	return s.obsMatcher.enabled(ctx)
}

// obsMatcher caches enabled rules for a short TTL so the hot emit path does
// not query the store per event.
type obsMatcher struct {
	store    storage.ObsStore
	ttl      time.Duration
	mu       sync.Mutex
	rules    []storage.ObsRule
	loadedAt time.Time
}

func newObsMatcher(store storage.ObsStore) *obsMatcher {
	return &obsMatcher{store: store, ttl: 5 * time.Second}
}

func (m *obsMatcher) invalidate() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.loadedAt = time.Time{}
}

func (m *obsMatcher) enabled(ctx context.Context) []storage.ObsRule {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.loadedAt.IsZero() || time.Since(m.loadedAt) > m.ttl {
		if rules, err := m.store.ListRules(ctx); err == nil {
			enabledRules := rules[:0]
			for _, rule := range rules {
				if rule.Enabled {
					enabledRules = append(enabledRules, rule)
				}
			}
			m.rules = enabledRules
			m.loadedAt = time.Now()
		}
	}
	return m.rules
}

// observe evaluates one emitted session event against the enabled rules and
// records at most one hit per rule per run (deduplication set owned by the
// caller's run).
func (s *Server) observeEvent(r *http.Request, seen map[string]bool, session *core.Session, event core.SessionEvent) {
	s.observeEventContext(r.Context(), seen, session, event)
}

func (s *Server) observeEventContext(ctx context.Context, seen map[string]bool, session *core.Session, event core.SessionEvent) {
	if s.obs == nil {
		return
	}
	hit := storage.MatchEvent(s.obsEnabledRules(ctx), event)
	if hit == nil {
		return
	}
	if seen[hit.RuleID] {
		return
	}
	seen[hit.RuleID] = true
	principal := session.Principal()
	hit.SessionID = session.ID()
	hit.Actor = principal.SubjectID
	hit.TenantID = principal.TenantID
	if err := s.obs.RecordHit(ctx, *hit); err != nil && s.logger != nil {
		s.logger.ErrorContext(ctx, "obs hit record failed", slogString("rule", hit.RuleID), slogString("error", err.Error()))
	}
}

// --- routes ---

func (s *Server) registerObsRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/admin/obs/rules", s.handleObsListRules)
	mux.HandleFunc("POST /v1/admin/obs/rules", s.handleObsCreateRule)
	mux.HandleFunc("PUT /v1/admin/obs/rules/{id}", s.handleObsUpdateRule)
	mux.HandleFunc("DELETE /v1/admin/obs/rules/{id}", s.handleObsDeleteRule)
	mux.HandleFunc("GET /v1/admin/obs/hits", s.handleObsListHits)
	mux.HandleFunc("POST /v1/admin/backtests", s.handleBacktest)
	mux.HandleFunc("GET /v1/admin/metrics", s.handleMetrics)
	mux.HandleFunc("GET /v1/admin/tool-library/observations", s.handleLibraryObservations)
}

func (s *Server) handleObsListRules(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	if !requireAdmin(w, principal) || s.obs == nil {
		if s.obs == nil {
			writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "obs store is not configured"})
		}
		return
	}
	rules, err := s.obs.ListRules(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"rules": rules})
}

func (s *Server) handleObsCreateRule(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	if !requireAdmin(w, principal) {
		return
	}
	if s.obs == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "obs store is not configured"})
		return
	}
	var request struct {
		Name    string `json:"name"`
		Kind    string `json:"kind"`
		Pattern string `json:"pattern"`
		Enabled *bool  `json:"enabled"`
	}
	if !s.decodeJSON(w, r, &request) {
		return
	}
	rule := storage.ObsRule{Name: request.Name, Kind: request.Kind, Pattern: request.Pattern, Enabled: request.Enabled == nil || *request.Enabled}
	created, err := s.obs.CreateRule(r.Context(), rule)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	s.recordAudit(r, principal, "obs.rule_create", created.ID, map[string]any{"kind": created.Kind, "pattern": created.Pattern})
	if s.obsMatcher != nil {
		s.obsMatcher.invalidate()
	}
	writeJSON(w, http.StatusCreated, created)
}

func (s *Server) handleObsUpdateRule(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	if !requireAdmin(w, principal) {
		return
	}
	if s.obs == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "obs store is not configured"})
		return
	}
	var request struct {
		Name    string `json:"name"`
		Kind    string `json:"kind"`
		Pattern string `json:"pattern"`
		Enabled bool   `json:"enabled"`
	}
	if !s.decodeJSON(w, r, &request) {
		return
	}
	rule := storage.ObsRule{ID: r.PathValue("id"), Name: request.Name, Kind: request.Kind, Pattern: request.Pattern, Enabled: request.Enabled}
	if err := s.obs.UpdateRule(r.Context(), rule); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	s.recordAudit(r, principal, "obs.rule_update", rule.ID, map[string]any{"enabled": rule.Enabled})
	if s.obsMatcher != nil {
		s.obsMatcher.invalidate()
	}
	writeJSON(w, http.StatusOK, rule)
}

func (s *Server) handleObsDeleteRule(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	if !requireAdmin(w, principal) {
		return
	}
	if s.obs == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "obs store is not configured"})
		return
	}
	if err := s.obs.DeleteRule(r.Context(), r.PathValue("id")); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	s.recordAudit(r, principal, "obs.rule_delete", r.PathValue("id"), nil)
	if s.obsMatcher != nil {
		s.obsMatcher.invalidate()
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
}

func (s *Server) handleObsListHits(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	if !requireTenantOperator(w, principal) {
		return
	}
	if s.obs == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "obs store is not configured"})
		return
	}
	filter := storage.ObsHitFilter{
		RuleID:    r.URL.Query().Get("rule_id"),
		SessionID: r.URL.Query().Get("session_id"),
	}
	// Tenant admins see their tenant's hits only.
	if roleOf(principal) != storage.RoleAccountAdmin {
		filter.TenantID = principal.TenantID
	}
	if raw := r.URL.Query().Get("limit"); raw != "" {
		if parsed, err := strconv.ParseInt(raw, 10, 64); err == nil {
			filter.Limit = int(parsed)
		}
	}
	if raw := r.URL.Query().Get("offset"); raw != "" {
		if parsed, err := strconv.ParseInt(raw, 10, 64); err == nil {
			filter.Offset = int(parsed)
		}
	}
	hits, total, err := s.obs.ListHits(r.Context(), filter)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"hits": hits, "total": total})
}

// handleBacktest replays one recorded session's assembled context through a
// chosen profile. The new session carries the source history (up to the
// cutoff), runs synchronously, and links back to the source for review.
func (s *Server) handleBacktest(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	if !requireAdmin(w, principal) {
		return
	}
	if !s.ensureControlPlane(w, r) {
		return
	}
	var request struct {
		SourceSessionID string `json:"source_session_id"`
		ProfileID       string `json:"profile_id"`
		// Message is the probe sent on top of the copied context. Empty
		// replays the source session's last user message verbatim.
		Message           string   `json:"message"`
		CutoffSeq         int64    `json:"cutoff_seq"`
		AllowCapabilities []string `json:"allow_capabilities,omitempty"`
	}
	if !s.decodeJSON(w, r, &request) {
		return
	}
	source, err := s.sessions.Load(r.Context(), request.SourceSessionID)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "source session not found"})
		return
	}
	// Only operators whose chain covers the source may replay it.
	if !canReadScope(principal, source.Principal().Scope) {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "principal does not own the source session scope"})
		return
	}

	events := source.Events()
	lastUserText := ""
	for i := len(events) - 1; i >= 0; i-- {
		if events[i].Type == core.EvUserMessage {
			var data core.UserMessageData
			if jsonErr := json.Unmarshal(events[i].Data, &data); jsonErr == nil {
				lastUserText = data.Text
			}
			break
		}
	}
	if request.CutoffSeq > 0 {
		adjusted, err := core.AdjustBacktestCutoff(events, request.CutoffSeq)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		events = events[:adjusted]
	}

	probe := request.Message
	if probe == "" {
		probe = lastUserText
	}
	if probe == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "backtest requires a message or a source user message"})
		return
	}
	capabilityFilter, err := evaluation.SafeCapabilityFilter(request.AllowCapabilities)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	backtestID, err := core.NewID("sess_")
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	// Replay runs under the SOURCE principal so capability resolution and
	// policies match the original run's tenant context.
	sourcePrincipal := source.Principal()
	scope, err := sourcePrincipal.Scope.Child(core.ScopeRef{Kind: core.ScopeSession, ID: backtestID})
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	metadata := map[string]string{"backtest_of": source.ID(), "profile": request.ProfileID}
	sourceEvidence := core.RunCompositionEvidence{}
	sourceEvidenceFound := false
	if sourceRunID := latestCompositionRunID(events); sourceRunID != "" {
		if evidence, found, evidenceErr := core.ExtractRunCompositionEvidence(events, sourceRunID); evidenceErr == nil && found {
			sourceEvidence = evidence
			sourceEvidenceFound = true
			metadata["backtest.source_composition_revision"] = evidence.CompositionRevision
			metadata["backtest.source_assignment_revision"] = evidence.AssignmentRevision
		}
	}
	if source.Metadata()["backtest_of"] != "" {
		metadata["backtest_of"] = source.Metadata()["backtest_of"] // chains stay linked
	}
	replay, err := core.RestoreSession(core.SessionOptions{
		ID: backtestID, ProfileID: request.ProfileID, Principal: sourcePrincipal,
		Scope: scope, Metadata: metadata,
	}, events)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if err := s.sessions.Create(r.Context(), replay); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	runID, err := core.NewID("run_")
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	// Backtest runs are observable too: watch rules apply to replays.
	seenObsHits := map[string]bool{}
	emit := func(event core.SessionEvent) {
		s.observeEvent(r, seenObsHits, replay, event)
	}
	result, runErr := s.runtime.RunTurn(r.Context(), sourcePrincipal, replay, core.TurnInput{
		RunID: runID, Text: probe, CapabilityFilter: capabilityFilter,
		CompositionMetadata: map[string]string{
			"backtest.source_session_id":           source.ID(),
			"backtest.source_composition_revision": metadata["backtest.source_composition_revision"],
			"backtest.source_assignment_revision":  metadata["backtest.source_assignment_revision"],
		},
	}, emit)
	evidence, found, evidenceErr := core.ExtractRunCompositionEvidence(replay.Events(), runID)
	if evidenceErr != nil {
		evidence = core.RunCompositionEvidence{}
		found = false
	}
	if saveErr := s.sessions.Save(r.Context(), replay, int64(len(events))); saveErr != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": saveErr.Error()})
		return
	}
	if runErr != nil {
		writeJSON(w, http.StatusOK, map[string]any{
			"backtest_session_id": backtestID, "run_id": runID, "status": string(result.Status), "error": runErr.Error(),
			"context_events": len(events), "probe": probe, "composition_revision": evidence.CompositionRevision,
			"assignment_revision": evidence.AssignmentRevision, "composition_evidence_found": found,
			"source_composition_revision":       sourceEvidence.CompositionRevision,
			"source_assignment_revision":        sourceEvidence.AssignmentRevision,
			"source_composition_evidence_found": sourceEvidenceFound,
		})
		return
	}
	s.recordAudit(r, principal, "backtest.run", request.SourceSessionID, map[string]any{
		"backtest_session_id": backtestID, "profile": request.ProfileID, "context_events": len(events),
		"allow_capabilities":   request.AllowCapabilities,
		"composition_revision": evidence.CompositionRevision, "assignment_revision": evidence.AssignmentRevision,
		"source_composition_revision": sourceEvidence.CompositionRevision,
		"source_assignment_revision":  sourceEvidence.AssignmentRevision,
	})
	writeJSON(w, http.StatusOK, map[string]any{
		"backtest_session_id": backtestID, "run_id": runID, "status": string(result.Status), "answer": result.Answer,
		"composition_revision": evidence.CompositionRevision, "assignment_revision": evidence.AssignmentRevision,
		"composition_evidence_found":        found,
		"source_composition_revision":       sourceEvidence.CompositionRevision,
		"source_assignment_revision":        sourceEvidence.AssignmentRevision,
		"source_composition_evidence_found": sourceEvidenceFound,
		"context_events":                    len(events), "probe": probe,
	})
}

func latestCompositionRunID(events []core.SessionEvent) string {
	for index := len(events) - 1; index >= 0; index-- {
		if events[index].Type == core.EvRunStart || events[index].Type == core.EvRunResume {
			return events[index].RunID
		}
	}
	return ""
}

// handleMetrics serves aggregated run/token metrics. Platform admins may
// query any tenant; tenant admins are pinned to their own.
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	if !requireTenantOperator(w, principal) {
		return
	}
	if s.runStats == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "run stats store is not configured"})
		return
	}
	tenant := principal.TenantID
	if tenantOverride := r.URL.Query().Get("tenant"); tenantOverride != "" {
		if !requireAdmin(w, principal) {
			return
		}
		tenant = tenantOverride
	}
	metrics, err := s.runStats.Metrics(r.Context(), tenant)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, metrics)
}

func (s *Server) handleLibraryObservations(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	if !requireTenantOperator(w, principal) {
		return
	}
	if s.libraryObserver == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "tool library observer is not configured"})
		return
	}
	limit := 100
	if raw := r.URL.Query().Get("limit"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil {
			limit = parsed
		}
	}
	tenant := ""
	if roleOf(principal) != storage.RoleAccountAdmin {
		tenant = principal.TenantID
	} else if raw := r.URL.Query().Get("tenant_id"); raw != "" {
		tenant = raw
	}
	records, err := s.libraryObserver.ListForTenant(r.Context(), tenant, limit)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"observations": records})
}
