// Admin surface: configuration endpoints backing the web console. Every
// route authenticates through the same Authenticator seam as the rest of the
// adapter, and mutating routes require the target scope to sit on the
// caller's own chain. Credential values are accepted at bind time and are
// never returned by any read.
package server

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/cc-auto-agent/harness-core/pkg/storage"
	"net/http"
	"sync"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
)

// adminState tracks dynamically mounted bindings so the console can undo
// exactly what it applied: disable bindings, policy layers, and credential
// references are all registered through the same reversible mount primitive.
type adminState struct {
	mu          sync.Mutex
	next        int64
	maxBindings int
	handles     map[string]*adminBinding
}

type adminBinding struct {
	Kind    string         `json:"kind"` // policy | credential | disable
	Summary any            `json:"summary"`
	Scope   core.ScopePath `json:"scope"`
	unmount func()
}

func newAdminState() *adminState {
	return &adminState{handles: map[string]*adminBinding{}}
}

// add registers a binding. preferredID is used verbatim when restoring a
// journaled binding; otherwise a fresh id is minted (advancing past any
// restored id so ids never collide).
func (a *adminState) add(kind, preferredID string, summary any, scope core.ScopePath, unmount func()) (string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.maxBindings <= 0 {
		a.maxBindings = storage.MaxAdminBindings
	}
	id := preferredID
	if id == "" {
		a.next++
		id = fmt.Sprintf("adm_%d", a.next)
	}
	if _, exists := a.handles[id]; exists {
		return "", fmt.Errorf("admin binding %s already exists", id)
	}
	if len(a.handles) >= a.maxBindings {
		return "", fmt.Errorf("admin bindings exceed maximum of %d", a.maxBindings)
	}
	if suffix := int64(idTrimNumber(id)); suffix >= a.next {
		a.next = suffix + 1
	}
	a.handles[id] = &adminBinding{Kind: kind, Summary: summary, Scope: scope, unmount: unmount}
	return id, nil
}

func idTrimNumber(id string) int {
	digits := ""
	for i := len(id) - 1; i >= 0 && id[i] >= '0' && id[i] <= '9'; i-- {
		digits = string(id[i]) + digits
	}
	if digits == "" {
		return -1
	}
	value := 0
	for _, c := range digits {
		value = value*10 + int(c-'0')
	}
	return value
}

func (a *adminState) remove(id string) (*adminBinding, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	binding, ok := a.handles[id]
	if ok {
		delete(a.handles, id)
	}
	return binding, ok
}

func (a *adminState) get(id string) (*adminBinding, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	binding, ok := a.handles[id]
	return binding, ok
}

func (a *Server) adminStateFor() *adminState {
	a.adminOnce.Do(func() { a.admin = newAdminState() })
	return a.admin
}

func (s *Server) registerAdminRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/admin/overview", s.handleAdminOverview)
	mux.HandleFunc("GET /v1/admin/capability-runtimes", s.handleAdminCapabilityRuntimes)
	mux.HandleFunc("GET /v1/admin/policies", s.handleAdminListPolicies)
	mux.HandleFunc("POST /v1/admin/policies", s.handleAdminBindPolicy)
	mux.HandleFunc("DELETE /v1/admin/bindings/{id}", s.handleAdminUnbind)
	mux.HandleFunc("GET /v1/admin/credentials", s.handleAdminListCredentials)
	mux.HandleFunc("POST /v1/admin/credentials", s.handleAdminBindCredential)
	mux.HandleFunc("POST /v1/admin/capabilities/{id}/disable", s.handleAdminDisableCapability)
	mux.HandleFunc("POST /v1/admin/capabilities/{id}/enable", s.handleAdminEnableCapability)
	mux.HandleFunc("GET /v1/admin/bindings", s.handleAdminListBindings)
}

func (s *Server) handleAdminCapabilityRuntimes(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.authenticate(w, r); !ok {
		return
	}
	if !s.ensureControlPlane(w, r) {
		return
	}
	if s.capabilityRuntimes == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "capability runtime registry unavailable"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"runtimes": s.capabilityRuntimes.List()})
}

func (s *Server) handleAdminOverview(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	if !s.ensureControlPlane(w, r) {
		return
	}
	capabilities, err := s.runtime.Capabilities.Entries(principal.Scope)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	profiles, err := s.runtime.Profiles.ListProfiles(principal.Scope)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	policies, _ := s.policyList(principal.Scope)
	credentials, _ := s.credentialList(principal.Scope)
	_, sessionsListable := s.sessions.(storage.SessionLister)
	writeJSON(w, http.StatusOK, map[string]any{
		"principal": map[string]string{
			"subject": principal.SubjectID, "tenant": principal.TenantID,
			"scope": principal.Scope.String(),
		},
		"store": fmt.Sprintf("%T", s.sessions),
		"features": map[string]bool{
			"leases":      s.leaser != nil,
			"runners":     s.runners != nil,
			"releases":    s.Releases != nil,
			"evaluations": s.evaluations != nil,
			"telemetry":   s.telemetry != nil,
		},
		"counts": map[string]int{
			"capabilities": len(capabilities),
			"profiles":     len(profiles),
			"policies":     len(policies),
			"credentials":  len(credentials),
		},
		"sessions_listable": sessionsListable,
	})
}

func (s *Server) handleAdminListPolicies(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	scope, err := s.scopeFromQuery(r, principal)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	layers, err := s.policyList(scope)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"policies": layers})
}

func (s *Server) handleAdminBindPolicy(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	var request struct {
		Scope core.ScopePath `json:"scope"`
		Allow []string       `json:"allow_permissions"`
		Deny  []string       `json:"deny_permissions"`
		// MaxSteps/MaxToolCalls arrive as pointers: omitted means no cap.
		MaxSteps     *int `json:"max_steps"`
		MaxToolCalls *int `json:"max_tool_calls"`
	}
	if !s.decodeJSON(w, r, &request) {
		return
	}
	if !canMutateScope(principal, request.Scope) {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "principal does not own the scope"})
		return
	}
	layer := core.PolicyLayer{
		Scope:            request.Scope,
		AllowPermissions: toPermissions(request.Allow),
		DenyPermissions:  toPermissions(request.Deny),
		MaxSteps:         request.MaxSteps,
		MaxToolCalls:     request.MaxToolCalls,
	}
	unmount, err := s.policyRegistry().Mount(layer)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	payload := map[string]any{
		"scope": request.Scope.String(), "allow": toStrings(layer.AllowPermissions), "deny": toStrings(layer.DenyPermissions),
	}
	if layer.MaxSteps != nil {
		payload["max_steps"] = *layer.MaxSteps
	}
	if layer.MaxToolCalls != nil {
		payload["max_tool_calls"] = *layer.MaxToolCalls
	}
	id, err := s.addBinding(r.Context(), "policy", request.Scope, payload, unmount)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	s.recordAudit(r, principal, "policy.bind", request.Scope.String(), map[string]any{"binding_id": id})
	writeJSON(w, http.StatusCreated, map[string]string{"binding_id": id})
}

func (s *Server) handleAdminListCredentials(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	scope, err := s.scopeFromQuery(r, principal)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	refs, err := s.credentialList(scope)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"credentials": refs})
}

func (s *Server) handleAdminBindCredential(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	var request struct {
		Scope   core.ScopePath `json:"scope"`
		Ref     string         `json:"ref"`
		Mode    string         `json:"mode"`
		Kind    string         `json:"kind"` // static | env
		Value   string         `json:"value"`
		EnvName string         `json:"env_name"`
	}
	if !s.decodeJSON(w, r, &request) {
		return
	}
	if !canMutateScope(principal, request.Scope) {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "principal does not own the scope"})
		return
	}
	var provider core.CredentialProvider
	switch request.Kind {
	case "static":
		if request.Value == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "static credential requires a value"})
			return
		}
		provider = core.StaticCredentialProvider{Value: request.Value, Source: "admin"}
	case "env":
		if request.EnvName == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "env credential requires env_name"})
			return
		}
		provider = core.EnvironmentCredentialProvider{Name: request.EnvName}
	default:
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "kind must be static or env"})
		return
	}
	mode := core.CredentialBindingMode(request.Mode)
	binding := core.CredentialBinding{
		Scope: request.Scope, Ref: core.CredentialRef(request.Ref),
		Mode: mode, Provider: provider,
	}
	unmount, err := s.credentialRegistry().Mount(binding)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	// Static credential values are never journaled; env credentials only
	// persist the variable name, so restarts can re-apply them.
	var bindingID string
	if request.Kind == "env" {
		bindingID, err = s.addBinding(r.Context(), "credential", request.Scope, map[string]any{
			"scope": request.Scope.String(), "ref": request.Ref, "kind": "env", "env_name": request.EnvName,
		}, unmount)
	} else {
		bindingID, err = s.addEphemeralBinding("credential", request.Scope, map[string]any{
			"scope": request.Scope.String(), "ref": request.Ref, "kind": "static",
		}, unmount)
	}
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	s.recordAudit(r, principal, "credential.bind", request.Ref, map[string]any{"scope": request.Scope.String(), "kind": request.Kind})
	writeJSON(w, http.StatusCreated, map[string]any{"binding_id": bindingID, "kind": request.Kind})
}

func (s *Server) handleAdminDisableCapability(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	capabilityID := r.PathValue("id")
	var request struct {
		Scope core.ScopePath `json:"scope"`
	}
	if !s.decodeJSON(w, r, &request) {
		return
	}
	if request.Scope.Depth() == 0 {
		request.Scope = principal.Scope // console convenience: disable at own scope
	}
	if !canMutateScope(principal, request.Scope) {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "principal does not own the scope"})
		return
	}
	manifest := core.CapabilityManifest{ID: capabilityID, Version: "admin"}
	unmount, err := s.runtime.Capabilities.Mount(core.CapabilityBinding{
		Scope: request.Scope, Mode: core.BindingDisable, Manifest: manifest,
	})
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	id, err := s.addBinding(r.Context(), "disable", request.Scope, map[string]any{
		"scope": request.Scope.String(), "capability": capabilityID,
	}, unmount)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	s.recordAudit(r, principal, "capability.disable", capabilityID, map[string]any{"scope": request.Scope.String()})
	writeJSON(w, http.StatusCreated, map[string]string{"binding_id": id})
}

func (s *Server) handleAdminEnableCapability(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	var request struct {
		BindingID string `json:"binding_id"`
	}
	if !s.decodeJSON(w, r, &request) {
		return
	}
	binding, exists := s.adminStateFor().get(request.BindingID)
	if !exists || binding.Kind != "disable" {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown disable binding"})
		return
	}
	if !canMutateScope(principal, binding.Scope) {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "principal does not own the binding scope"})
		return
	}
	found, err := s.unbindBinding(r.Context(), request.BindingID, principal)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if !found {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown binding"})
		return
	}
	s.recordAudit(r, principal, "capability.enable", r.PathValue("id"), map[string]any{"binding_id": request.BindingID})
	writeJSON(w, http.StatusOK, map[string]string{"status": "enabled"})
}

func (s *Server) handleAdminListBindings(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	state := s.adminStateFor()
	state.mu.Lock()
	defer state.mu.Unlock()
	bindings := []map[string]any{}
	for id, binding := range state.handles {
		if canMutateScope(principal, binding.Scope) {
			bindings = append(bindings, map[string]any{"binding_id": id, "kind": binding.Kind, "scope": binding.Scope, "summary": binding.Summary})
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"bindings": bindings})
}

func (s *Server) handleAdminUnbind(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	found, err := s.unbindBinding(r.Context(), r.PathValue("id"), principal)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if !found {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown binding"})
		return
	}
	s.recordAudit(r, principal, "binding.unbind", r.PathValue("id"), nil)
	writeJSON(w, http.StatusOK, map[string]string{"status": "unbound"})
}

func (s *Server) scopeFromQuery(r *http.Request, principal core.Principal) (core.ScopePath, error) {
	raw := r.URL.Query().Get("scope")
	if raw == "" {
		return principal.Scope, nil
	}
	scope, err := storage.ParseScopePath(raw)
	if err != nil {
		return core.ScopePath{}, err
	}
	if !canReadScope(principal, scope) {
		return core.ScopePath{}, fmt.Errorf("principal cannot read scope %q", scope)
	}
	return scope, nil
}

func toPermissions(raw []string) []core.Permission {
	out := make([]core.Permission, 0, len(raw))
	for _, item := range raw {
		if trimmed := trimSpace(item); trimmed != "" {
			out = append(out, core.Permission(trimmed))
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func trimSpace(value string) string {
	start, end := 0, len(value)
	for start < end && (value[start] == ' ' || value[start] == '\t') {
		start++
	}
	for end > start && (value[end-1] == ' ' || value[end-1] == '\t') {
		end--
	}
	return value[start:end]
}

// policyRegistry returns the runtime's policy registry, or nil when the
// deployment did not configure layered policies.
func (s *Server) policyRegistry() *core.PolicyRegistry {
	if s.runtime == nil {
		return nil
	}
	return s.runtime.Policy
}

func (s *Server) policyList(scope core.ScopePath) ([]core.PolicyLayerView, error) {
	if registry := s.policyRegistry(); registry != nil {
		return registry.List(scope)
	}
	return []core.PolicyLayerView{}, nil
}

func (s *Server) credentialList(scope core.ScopePath) ([]core.CredentialRefView, error) {
	if registry := s.credentialRegistry(); registry != nil {
		return registry.List(scope)
	}
	return []core.CredentialRefView{}, nil
}

// credentialRegistry returns the credential registry when the deployment
// wired one (the runtime stores it behind the CredentialResolver interface).
func (s *Server) credentialRegistry() *core.CredentialRegistry {
	if s.runtime == nil {
		return nil
	}
	registry, _ := s.runtime.Credentials.(*core.CredentialRegistry)
	return registry
}

func toStrings(perms []core.Permission) []string {
	out := make([]string, 0, len(perms))
	for _, p := range perms {
		out = append(out, string(p))
	}
	return out
}

// addBinding registers a dynamic binding and journals it for restart
// recovery. Journal failure rolls back the mounted in-memory contribution.
func (s *Server) addBinding(ctx context.Context, kind string, scope core.ScopePath, payload any, unmount func()) (string, error) {
	id, err := s.adminStateFor().add(kind, "", payload, scope, unmount)
	if err != nil {
		if unmount != nil {
			unmount()
		}
		return "", err
	}
	if s.journal != nil {
		encoded, err := json.Marshal(payload)
		if err == nil {
			err = s.journal.Record(ctx, storage.BindingRecord{ID: id, Kind: kind, Payload: encoded})
		}
		if err != nil {
			_, _ = s.adminStateFor().remove(id)
			if unmount != nil {
				unmount()
			}
			return "", fmt.Errorf("persist binding %s: %w", id, err)
		}
	}
	return id, nil
}

func (s *Server) addEphemeralBinding(kind string, scope core.ScopePath, payload any, unmount func()) (string, error) {
	id, err := s.adminStateFor().add(kind, "", payload, scope, unmount)
	if err != nil {
		if unmount != nil {
			unmount()
		}
		return "", err
	}
	return id, nil
}

// unbindBinding removes a journaled binding and drops its journal record.
// Returns true when the binding existed.
func (s *Server) unbindBinding(ctx context.Context, id string, principal core.Principal) (bool, error) {
	binding, ok := s.adminStateFor().get(id)
	if !ok {
		return false, nil
	}
	if !canMutateScope(principal, binding.Scope) {
		return true, fmt.Errorf("principal does not own binding scope %q", binding.Scope)
	}
	if s.journal != nil {
		if err := s.journal.Delete(ctx, id); err != nil {
			return true, fmt.Errorf("delete binding journal %s: %w", id, err)
		}
	}
	if _, ok := s.adminStateFor().remove(id); !ok {
		return false, nil
	}
	if binding.unmount != nil {
		binding.unmount()
	}
	return true, nil
}
