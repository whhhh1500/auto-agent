// Control-plane surface: built-in account authentication (first-boot admin,
// bearer-token login), resource management over S3-path semantics, tenant
// and account administration, deployment settings, and dynamic capability
// creation. All routes flow through the same Authenticator seam; the admin
// routes additionally require the account's role.
package server

import (
	"net/http"
)

func (s *Server) registerControlRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/auth/login", s.handleLogin)
	mux.HandleFunc("POST /v1/auth/logout", s.handleLogout)
	mux.HandleFunc("POST /v1/auth/activate", s.handleActivateAccount)
	mux.HandleFunc("GET /v1/resources", s.handleResourceList)
	mux.HandleFunc("GET /v1/resources/{key...}", s.handleResourceGet)
	mux.HandleFunc("PUT /v1/resources/{key...}", s.handleResourcePut)
	mux.HandleFunc("DELETE /v1/resources/{key...}", s.handleResourceDelete)
	mux.HandleFunc("GET /v1/admin/accounts", s.handleListAccounts)
	mux.HandleFunc("POST /v1/admin/accounts", s.handleCreateAccount)
	mux.HandleFunc("POST /v1/admin/accounts/{account}/password", s.handleSetPassword)
	mux.HandleFunc("POST /v1/admin/accounts/{account}/status", s.handleSetAccountStatus)
	mux.HandleFunc("GET /v1/admin/tenants", s.handleListTenants)
	mux.HandleFunc("POST /v1/admin/tenants", s.handleCreateTenant)
	mux.HandleFunc("GET /v1/admin/settings/{key}", s.handleGetSetting)
	mux.HandleFunc("PUT /v1/admin/settings/{key}", s.handlePutSetting)
	mux.HandleFunc("GET /v1/admin/model-settings/llm", s.handleGetModelSettings)
	mux.HandleFunc("PUT /v1/admin/model-settings/llm", s.handlePutModelSettings)
	mux.HandleFunc("POST /v1/admin/capabilities/bind", s.handleBindCapability)
	mux.HandleFunc("GET /v1/admin/audit", s.handleAuditList)
}
