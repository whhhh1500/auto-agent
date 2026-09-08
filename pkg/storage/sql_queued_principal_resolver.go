package storage

import (
	"context"
	"errors"
	"fmt"

	core "github.com/whhhh1500/auto-agent/pkg/core"
)

var (
	// ErrQueuedPrincipalNotFound indicates that no durable account can own the
	// queued run. It intentionally does not disclose the requested identity.
	ErrQueuedPrincipalNotFound = errors.New("queued principal is unavailable")
	// ErrQueuedPrincipalIdentityMismatch indicates that a durable account does
	// not belong to the queued run's tenant/subject identity.
	ErrQueuedPrincipalIdentityMismatch = errors.New("queued principal identity does not match")
	// ErrQueuedPrincipalInactive indicates that the durable account is not
	// eligible to own new queued execution.
	ErrQueuedPrincipalInactive = errors.New("queued principal is inactive")
	// ErrQueuedPrincipalInvalid indicates a durable account cannot be mapped
	// into a valid Principal. Callers must not retry it without an account fix.
	ErrQueuedPrincipalInvalid = errors.New("queued principal is invalid")
)

// SQLQueuedPrincipalResolver resolves queued-run owners from the native SQL
// account authority. It is storage-only: its method structurally satisfies
// server.RunPrincipalResolver without importing the server package.
type SQLQueuedPrincipalResolver struct {
	accounts *SQLAccountStore
	root     []core.ScopeRef
}

// sqlQueuedPrincipalResolverProvider is sealed so only the native resolver,
// or a transparent wrapper embedding it, can pass same-authority validation.
type sqlQueuedPrincipalResolverProvider interface {
	sqlQueuedPrincipalResolver() *SQLQueuedPrincipalResolver
}

var _ AuthorizationEpochReader = (*SQLQueuedPrincipalResolver)(nil)

// NewSQLQueuedPrincipalResolver validates and copies the product root used to
// map durable accounts into Principals. Empty roots use the embedded global
// default accepted by PrincipalForAccount.
func NewSQLQueuedPrincipalResolver(accounts *SQLAccountStore, root []core.ScopeRef) (*SQLQueuedPrincipalResolver, error) {
	if accounts == nil {
		return nil, fmt.Errorf("SQL queued principal resolver requires an account store")
	}
	if _, err := newAtomicSessionFenceDomain(accounts.db, accounts.dialect); err != nil {
		return nil, fmt.Errorf("SQL queued principal resolver account authority: %w", err)
	}
	if len(root) == 0 {
		root = []core.ScopeRef{{Kind: core.ScopeGlobal, ID: "global"}}
	}
	path, err := core.NewScopePath(root...)
	if err != nil {
		return nil, fmt.Errorf("SQL queued principal resolver root: %w", err)
	}
	segments := path.Segments()
	if segments[0].Kind != core.ScopeGlobal {
		return nil, fmt.Errorf("SQL queued principal resolver root must begin at global scope")
	}
	switch segments[len(segments)-1].Kind {
	case core.ScopeGlobal, core.ScopeDeployment, core.ScopeProduct:
	default:
		return nil, fmt.Errorf("SQL queued principal resolver root must end at product scope or above")
	}
	return &SQLQueuedPrincipalResolver{accounts: accounts, root: segments}, nil
}

// ResolveRunPrincipal returns the current active account principal for a
// queued tenant/subject pair. Expected account-state and identity failures use
// stable sentinels; database and context failures remain retryable errors.
func (r *SQLQueuedPrincipalResolver) ResolveRunPrincipal(ctx context.Context, tenantID, subjectID string) (core.Principal, error) {
	if r == nil || r.accounts == nil {
		return core.Principal{}, fmt.Errorf("SQL queued principal resolver is unavailable")
	}
	if err := validateScopeIdentifier(core.ScopeTenant, tenantID); err != nil {
		return core.Principal{}, ErrQueuedPrincipalIdentityMismatch
	}
	if err := validateAccountID(subjectID); err != nil {
		return core.Principal{}, ErrQueuedPrincipalIdentityMismatch
	}
	account, err := r.accounts.GetAccount(ctx, subjectID)
	if err != nil {
		if errors.Is(err, errAccountNotFound) {
			return core.Principal{}, ErrQueuedPrincipalNotFound
		}
		return core.Principal{}, err
	}
	if account.TenantID != tenantID {
		return core.Principal{}, ErrQueuedPrincipalIdentityMismatch
	}
	if account.Status != AccountActive {
		return core.Principal{}, ErrQueuedPrincipalInactive
	}
	principal, err := PrincipalForAccount(account, r.root)
	if err != nil || principal.TenantID != tenantID || principal.SubjectID != subjectID {
		return core.Principal{}, ErrQueuedPrincipalInvalid
	}
	return principal, nil
}

// AuthorizationEpoch reads the account authority's shared SQL epoch.
func (r *SQLQueuedPrincipalResolver) AuthorizationEpoch(ctx context.Context) (int64, error) {
	if r == nil || r.accounts == nil || r.accounts.db == nil {
		return 0, fmt.Errorf("SQL queued principal resolver is unavailable")
	}
	return readAuthorizationEpoch(ctx, r.accounts.db, r.accounts.dialect)
}

func (r *SQLQueuedPrincipalResolver) atomicSessionFenceDomain() atomicSessionFenceDomain {
	if r == nil || r.accounts == nil {
		return atomicSessionFenceDomain{}
	}
	domain, _ := newAtomicSessionFenceDomain(r.accounts.db, r.accounts.dialect)
	return domain
}

func (r *SQLQueuedPrincipalResolver) sqlQueuedPrincipalResolver() *SQLQueuedPrincipalResolver {
	return r
}
