package server

import (
	"context"
	"errors"
	"fmt"
	httpauth "github.com/whhhh1500/auto-agent/pkg/adapter/httpapi/auth"
	appidentity "github.com/whhhh1500/auto-agent/pkg/app/identity"
	"github.com/whhhh1500/auto-agent/pkg/storage"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	core "github.com/whhhh1500/auto-agent/pkg/core"
)

// AccountAuthenticator authenticates requests with bearer tokens issued by
// the built-in account store. When DevHeaderFallback is enabled and no
// bearer token is present, it falls back to the development header identity
// source — for local runs only.
type AccountAuthenticator struct {
	Store             storage.AccountStore
	Root              []core.ScopeRef
	DevHeaderFallback bool
}

func bearerToken(header string) (string, bool) {
	const prefix = "Bearer "
	if !strings.HasPrefix(header, prefix) {
		return "", false
	}
	token := header[len(prefix):]
	if token == "" {
		return "", false
	}
	return token, true
}

func (a AccountAuthenticator) Authenticate(r *http.Request) (core.Principal, error) {
	if token, ok := bearerToken(r.Header.Get("Authorization")); ok {
		account, err := a.Store.ResolveToken(r.Context(), token)
		if err != nil {
			return core.Principal{}, err
		}
		principal, err := storage.PrincipalForAccount(account, a.Root)
		if err != nil {
			return core.Principal{}, err
		}
		principal.Attributes["auth.source"] = "account"
		principal.Attributes["account.must_change_password"] = strconv.FormatBool(account.MustChangePassword)
		return principal, nil
	}
	if a.DevHeaderFallback {
		principal, err := AuthChain{Source: HeaderIdentitySource{}, Mapper: StaticPrincipalMapper{Root: a.Root}}.Authenticate(r)
		if err == nil {
			principal.Attributes["auth.source"] = "dev-header"
			return principal, nil
		}
	}
	return core.Principal{}, fmt.Errorf("missing bearer token")
}

// roleOf reads the caller's role attribute.
func roleOf(principal core.Principal) string {
	return principal.Attributes["role"]
}

// requireAdmin answers 403 unless the caller is the platform admin.
func requireAdmin(w http.ResponseWriter, principal core.Principal) bool {
	if roleOf(principal) != storage.RoleAccountAdmin {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "platform admin role required"})
		return false
	}
	return true
}

func requireTenantOperator(w http.ResponseWriter, principal core.Principal) bool {
	switch roleOf(principal) {
	case storage.RoleAccountAdmin, storage.RoleAccountTenantAdmin:
		return true
	default:
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "tenant administrator role required"})
		return false
	}
}

// loginLimiter is a small in-memory failure tracker: after
// maxFailures within the window, further attempts from the same email are
// locked out until the window clears. State is per instance; production
// deployments behind multiple instances should enforce limits at the gateway.
type loginLimiter struct {
	mu         sync.Mutex
	window     time.Duration
	max        int
	maxEntries int
	now        func() time.Time
	failures   map[string][]time.Time
}

const maxLoginLimiterEntries = 10000

func newLoginLimiter() *loginLimiter {
	return &loginLimiter{
		window: 15 * time.Minute, max: 10, maxEntries: maxLoginLimiterEntries,
		failures: map[string][]time.Time{},
	}
}

func (l *loginLimiter) clock() time.Time {
	if l != nil && l.now != nil {
		return l.now()
	}
	return time.Now()
}

func (l *loginLimiter) lockedOut(email string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.pruneExpiredLocked(email, l.clock())) >= l.max
}

func (l *loginLimiter) recordFailure(email string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.clock()
	if l.maxEntries <= 0 {
		l.maxEntries = maxLoginLimiterEntries
	}
	l.pruneExpiredLocked(email, now)
	if _, exists := l.failures[email]; !exists {
		if len(l.failures) >= l.maxEntries {
			l.evictExpiredLocked(now)
		}
		if len(l.failures) >= l.maxEntries {
			l.evictOldestUnlockedLocked()
		}
		if len(l.failures) >= l.maxEntries {
			// The table is full of live lockouts. Drop this unseen email
			// rather than wiping everyone who is already blocked.
			return
		}
	}
	l.failures[email] = append(l.failures[email], now)
}

func (l *loginLimiter) pruneExpiredLocked(email string, now time.Time) []time.Time {
	attempts := l.failures[email]
	fresh := attempts[:0]
	for _, at := range attempts {
		if now.Sub(at) < l.window {
			fresh = append(fresh, at)
		}
	}
	if len(fresh) == 0 {
		delete(l.failures, email)
		return nil
	}
	l.failures[email] = fresh
	return fresh
}

func (l *loginLimiter) evictExpiredLocked(now time.Time) {
	for email := range l.failures {
		l.pruneExpiredLocked(email, now)
	}
}

func (l *loginLimiter) evictOldestUnlockedLocked() {
	oldest := ""
	var oldestAt time.Time
	found := false
	for email, attempts := range l.failures {
		if len(attempts) >= l.max {
			continue
		}
		last := attempts[len(attempts)-1]
		if !found || last.Before(oldestAt) {
			oldest, oldestAt, found = email, last, true
		}
	}
	if found {
		delete(l.failures, oldest)
	}
}

func (l *loginLimiter) clear(email string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.failures, email)
}

// Check implements identity.LoginThrottle. This local policy cannot fail from
// I/O; injected policies may return an error, which identity.Service refuses
// rather than bypassing throttling.
func (l *loginLimiter) Check(_ context.Context, accountID string) error {
	if l.lockedOut(strings.ToLower(accountID)) {
		return appidentity.ErrThrottled
	}
	return nil
}

func (l *loginLimiter) RecordFailure(_ context.Context, accountID string) error {
	l.recordFailure(strings.ToLower(accountID))
	return nil
}

func (l *loginLimiter) RecordSuccess(_ context.Context, accountID string) error {
	l.clear(strings.ToLower(accountID))
	return nil
}

// handleLogout revokes the presented bearer token when the built-in account
// store issued it. Development header sessions without a bearer token still
// succeed: there is nothing to revoke.
func (s *Server) handleLogout(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	if revoker, ok := s.accounts.(interface {
		RevokeToken(ctx context.Context, token string) error
	}); ok {
		if token, ok := bearerToken(r.Header.Get("Authorization")); ok {
			if err := revoker.RevokeToken(r.Context(), token); err != nil {
				writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
				return
			}
		}
	}
	s.recordAudit(r, principal, "auth.logout", principal.SubjectID, nil)
	writeJSON(w, http.StatusOK, map[string]string{"status": "logged out"})
}

func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	if s.identityUseCases == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "account store is not configured"})
		return
	}
	var request httpauth.LoginRequest
	if !s.decodeJSON(w, r, &request) {
		return
	}
	accountID, err := request.AccountID()
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	result, err := s.identityUseCases.Login(r.Context(), appidentity.LoginCommand{AccountID: accountID, Password: request.Password})
	if err != nil {
		if errors.Is(err, appidentity.ErrInvalidInput) {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		if errors.Is(err, appidentity.ErrThrottled) {
			s.auditLogin(r, accountID, false, "locked out: too many failures")
			writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": appidentity.ErrThrottled.Error()})
			return
		}
		if errors.Is(err, appidentity.ErrInvalidCredentials) {
			s.auditLogin(r, accountID, false, "invalid credentials")
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": appidentity.ErrInvalidCredentials.Error()})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	s.auditLogin(r, result.Account.ID, true, "")
	writeJSON(w, http.StatusOK, httpauth.LoginResponse{Token: result.Token, Account: result.Account.ID,
		Email: result.Account.Email, Role: string(result.Account.Role), Tenant: result.Account.TenantID,
		MustChangePassword: result.Account.MustChangePassword})
}

func (s *Server) handleActivateAccount(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	if principal.Attributes["account.status"] != storage.AccountPendingActivation {
		writeJSON(w, http.StatusConflict, map[string]string{"error": "account activation is not pending"})
		return
	}
	if s.identityUseCases == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "account store is not configured"})
		return
	}
	var request httpauth.ActivationRequest
	if !s.decodeJSON(w, r, &request) {
		return
	}
	if err := request.Validate(); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	result, err := s.identityUseCases.Activate(r.Context(), appidentity.ActivateCommand{AccountID: principal.SubjectID, Password: request.Password})
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	s.recordAudit(r, principal, "account.activate", principal.SubjectID, nil)
	writeJSON(w, http.StatusOK, httpauth.ActivationResponse{Token: result.Token, Account: result.Account.ID})
}
