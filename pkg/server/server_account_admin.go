package server

import (
	"errors"
	"net/http"

	accountadmin "github.com/whhhh1500/auto-agent/pkg/adapter/httpapi/accountadmin"
	appidentity "github.com/whhhh1500/auto-agent/pkg/app/identity"
	core "github.com/whhhh1500/auto-agent/pkg/core"
)

// --- accounts and tenants ---

func (s *Server) handleListAccounts(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	// Keep this coarse route gate ahead of feature availability. Besides
	// preserving the established 403 response, it prevents an unauthorized
	// caller from learning whether the optional administration service exists.
	if !requireTenantOperator(w, principal) {
		return
	}
	if s.accountAdminUseCases == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "account store is not configured"})
		return
	}
	result, err := s.accountAdminUseCases.ListAccounts(r.Context(), appidentity.ListAccountsCommand{Actor: accountAdminActor(principal)})
	if err != nil {
		writeAccountAdminError(w, err, http.StatusInternalServerError)
		return
	}
	views := make([]accountadmin.AccountView, 0, len(result.Accounts))
	for _, account := range result.Accounts {
		views = append(views, accountAdminView(account))
	}
	writeJSON(w, http.StatusOK, accountadmin.AccountListResponse{Accounts: views})
}

func (s *Server) handleCreateAccount(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	if !requireAdmin(w, principal) {
		return
	}
	if s.accountAdminUseCases == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "account store is not configured"})
		return
	}
	var request accountadmin.CreateAccountRequest
	if !s.decodeJSON(w, r, &request) {
		return
	}
	result, err := s.accountAdminUseCases.CreateAccount(r.Context(), appidentity.CreateAccountCommand{
		Actor: accountAdminActor(principal), AccountID: request.Account, Email: request.Email,
		Password: request.Password, Role: request.Role, TenantID: request.TenantID,
	})
	if err != nil {
		writeAccountAdminError(w, err, http.StatusBadRequest)
		return
	}
	s.recordAudit(r, principal, "account.create", result.Account.ID, map[string]any{"role": result.Account.Role, "tenant": result.Account.TenantID})
	writeJSON(w, http.StatusCreated, accountadmin.AccountCreatedResponse{
		Account: result.Account.ID, Email: result.Account.Email, Role: result.Account.Role, TenantID: result.Account.TenantID,
	})
}

func (s *Server) handleSetPassword(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	accountID := r.PathValue("account")
	// Preserve the legacy self-or-platform gate before feature checks and body
	// decoding. AdminService enforces the same rule for non-HTTP callers.
	if principal.SubjectID != accountID && !requireAdmin(w, principal) {
		return
	}
	if s.accountAdminUseCases == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "account store is not configured"})
		return
	}
	var request accountadmin.SetAccountPasswordRequest
	if !s.decodeJSON(w, r, &request) {
		return
	}
	if err := s.accountAdminUseCases.SetAccountPassword(r.Context(), appidentity.SetAccountPasswordCommand{
		Actor: accountAdminActor(principal), AccountID: accountID, Password: request.Password,
	}); err != nil {
		writeAccountAdminError(w, err, http.StatusBadRequest)
		return
	}
	s.recordAudit(r, principal, "account.password_set", accountID, nil)
	writeJSON(w, http.StatusOK, accountadmin.PasswordUpdatedResponse{Status: "updated"})
}

func (s *Server) handleSetAccountStatus(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	if !requireAdmin(w, principal) {
		return
	}
	if s.accountAdminUseCases == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "account store is not configured"})
		return
	}
	var request accountadmin.SetAccountStatusRequest
	if !s.decodeJSON(w, r, &request) {
		return
	}
	result, err := s.accountAdminUseCases.SetAccountStatus(r.Context(), appidentity.SetAccountStatusCommand{
		Actor: accountAdminActor(principal), AccountID: r.PathValue("account"), Status: request.Status,
	})
	if err != nil {
		writeAccountAdminError(w, err, http.StatusBadRequest)
		return
	}
	s.recordAudit(r, principal, "account.status_set", result.AccountID, map[string]any{"status": result.Status})
	writeJSON(w, http.StatusOK, accountadmin.AccountStatusResponse{Status: result.Status})
}

func (s *Server) handleListTenants(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	if !requireAdmin(w, principal) {
		return
	}
	if s.accountAdminUseCases == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "account store is not configured"})
		return
	}
	result, err := s.accountAdminUseCases.ListTenants(r.Context(), appidentity.ListTenantsCommand{Actor: accountAdminActor(principal)})
	if err != nil {
		writeAccountAdminError(w, err, http.StatusInternalServerError)
		return
	}
	views := make([]accountadmin.TenantView, 0, len(result.Tenants))
	for _, tenant := range result.Tenants {
		views = append(views, accountadmin.TenantView{ID: tenant.ID, Name: tenant.Name, CreatedAt: tenant.CreatedAt})
	}
	writeJSON(w, http.StatusOK, accountadmin.TenantListResponse{Tenants: views})
}

func (s *Server) handleCreateTenant(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	if !requireAdmin(w, principal) {
		return
	}
	if s.accountAdminUseCases == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "account store is not configured"})
		return
	}
	var request accountadmin.CreateTenantRequest
	if !s.decodeJSON(w, r, &request) {
		return
	}
	result, err := s.accountAdminUseCases.CreateTenant(r.Context(), appidentity.CreateTenantCommand{
		Actor: accountAdminActor(principal), ID: request.ID, Name: request.Name,
	})
	if err != nil {
		writeAccountAdminError(w, err, http.StatusBadRequest)
		return
	}
	s.recordAudit(r, principal, "tenant.create", result.Tenant.ID, nil)
	writeJSON(w, http.StatusCreated, accountadmin.TenantCreatedResponse{ID: result.Tenant.ID, Name: result.Tenant.Name})
}

func accountAdminActor(principal core.Principal) appidentity.AdminActor {
	return appidentity.AdminActor{AccountID: principal.SubjectID, TenantID: principal.TenantID, Role: appidentity.Role(roleOf(principal))}
}

func accountAdminView(account appidentity.Account) accountadmin.AccountView {
	return accountadmin.AccountView{
		Account: account.ID, Email: account.Email, Role: account.Role, TenantID: account.TenantID,
		Status: account.Status, MustChangePassword: account.MustChangePassword, CreatedAt: account.CreatedAt,
	}
}

func writeAccountAdminError(w http.ResponseWriter, err error, operationalStatus int) {
	switch {
	case errors.Is(err, appidentity.ErrForbidden):
		writeJSON(w, http.StatusForbidden, map[string]string{"error": err.Error()})
	case errors.Is(err, appidentity.ErrInvalidInput):
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
	default:
		writeJSON(w, operationalStatus, map[string]string{"error": err.Error()})
	}
}
