package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	core "github.com/whhhh1500/auto-agent/pkg/core"
)

type transparentSQLQueuedPrincipalResolver struct{ *SQLQueuedPrincipalResolver }

type unsealedQueuedPrincipalResolver struct{}

func (unsealedQueuedPrincipalResolver) ResolveRunPrincipal(context.Context, string, string) (core.Principal, error) {
	return core.Principal{}, nil
}

func (unsealedQueuedPrincipalResolver) AuthorizationEpoch(context.Context) (int64, error) {
	return 0, nil
}

func newSQLQueuedPrincipalResolverFixture(t *testing.T) (*SQLSessionStore, *SQLAccountStore, *SQLRunControlStore, *SQLToolInvocationJournal, *SQLQueuedPrincipalResolver) {
	t.Helper()
	sessions := newTestSQLStore(t)
	accounts, err := NewSQLAccountStore(sessions.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	queue, err := NewSQLRunControlStore(sessions.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	journal, err := NewSQLToolInvocationJournal(sessions.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := NewSQLQueuedPrincipalResolver(accounts, []core.ScopeRef{
		{Kind: core.ScopeGlobal, ID: "global"},
		{Kind: core.ScopeProduct, ID: "product"},
	})
	if err != nil {
		t.Fatal(err)
	}
	return sessions, accounts, queue, journal, resolver
}

func createQueuedPrincipalAccount(t *testing.T, accounts *SQLAccountStore, id, role, tenant, status string) {
	t.Helper()
	if err := accounts.CreateAccount(context.Background(), Account{
		AccountID: id, Role: role, TenantID: tenant, Status: status,
		MustChangePassword: status == AccountPendingActivation,
	}, "queued-principal-password-123"); err != nil {
		t.Fatal(err)
	}
}

func TestSQLQueuedPrincipalResolverMapsActiveAccountRoles(t *testing.T) {
	_, accounts, _, _, resolver := newSQLQueuedPrincipalResolverFixture(t)
	for _, test := range []struct {
		name      string
		id        string
		role      string
		tenant    string
		wantScope core.ScopePath
		wantSend  bool
	}{
		{
			name: "admin", id: "principal-admin", role: RoleAccountAdmin, tenant: "system",
			wantScope: core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"}), wantSend: true,
		},
		{
			name: "tenant_admin", id: "principal-tenant-admin", role: RoleAccountTenantAdmin, tenant: "acme",
			wantScope: core.MustScopePath(
				core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"},
				core.ScopeRef{Kind: core.ScopeProduct, ID: "product"},
				core.ScopeRef{Kind: core.ScopeTenant, ID: "acme"},
			),
		},
		{
			name: "user", id: "principal-user", role: RoleAccountUser, tenant: "acme",
			wantScope: core.MustScopePath(
				core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"},
				core.ScopeRef{Kind: core.ScopeProduct, ID: "product"},
				core.ScopeRef{Kind: core.ScopeTenant, ID: "acme"},
				core.ScopeRef{Kind: core.ScopeUser, ID: "principal-user"},
			),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			createQueuedPrincipalAccount(t, accounts, test.id, test.role, test.tenant, AccountActive)
			principal, err := resolver.ResolveRunPrincipal(context.Background(), test.tenant, test.id)
			if err != nil {
				t.Fatal(err)
			}
			if principal.SubjectID != test.id || principal.TenantID != test.tenant || !principal.Scope.Equal(test.wantScope) {
				t.Fatalf("principal=%#v, want subject=%q tenant=%q scope=%s", principal, test.id, test.tenant, test.wantScope)
			}
			if !principal.Grants[core.PermRead] || !principal.Grants[core.PermWrite] || principal.Grants[core.PermSend] != test.wantSend {
				t.Fatalf("principal grants=%#v, want send=%t", principal.Grants, test.wantSend)
			}
			if principal.Attributes["role"] != test.role || principal.Attributes["account.status"] != AccountActive {
				t.Fatalf("principal attributes=%#v", principal.Attributes)
			}
		})
	}
}

func TestSQLQueuedPrincipalResolverRejectsUnavailableOrInvalidAccounts(t *testing.T) {
	_, accounts, _, _, resolver := newSQLQueuedPrincipalResolverFixture(t)
	createQueuedPrincipalAccount(t, accounts, "principal-disabled", RoleAccountUser, "acme", AccountDisabled)
	createQueuedPrincipalAccount(t, accounts, "principal-pending", RoleAccountUser, "acme", AccountPendingActivation)
	createQueuedPrincipalAccount(t, accounts, "principal-other-tenant", RoleAccountUser, "other", AccountActive)
	createQueuedPrincipalAccount(t, accounts, "principal-invalid", RoleAccountUser, "acme", AccountActive)
	if _, err := accounts.db.ExecContext(context.Background(), "UPDATE accounts SET role = 'invalid' WHERE account_id = ?", "principal-invalid"); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name    string
		tenant  string
		subject string
		want    error
	}{
		{name: "missing", tenant: "acme", subject: "principal-missing", want: ErrQueuedPrincipalNotFound},
		{name: "tenant_mismatch", tenant: "acme", subject: "principal-other-tenant", want: ErrQueuedPrincipalIdentityMismatch},
		{name: "disabled", tenant: "acme", subject: "principal-disabled", want: ErrQueuedPrincipalInactive},
		{name: "pending", tenant: "acme", subject: "principal-pending", want: ErrQueuedPrincipalInactive},
		{name: "invalid_account", tenant: "acme", subject: "principal-invalid", want: ErrQueuedPrincipalInvalid},
		{name: "invalid_queued_tenant", tenant: "bad/tenant", subject: "principal-pending", want: ErrQueuedPrincipalIdentityMismatch},
		{name: "invalid_queued_subject", tenant: "acme", subject: "bad\x00subject", want: ErrQueuedPrincipalIdentityMismatch},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := resolver.ResolveRunPrincipal(context.Background(), test.tenant, test.subject)
			if !errors.Is(err, test.want) {
				t.Fatalf("ResolveRunPrincipal error=%v, want %v", err, test.want)
			}
			if err.Error() != test.want.Error() {
				t.Fatalf("principal error leaked detail: %q", err)
			}
		})
	}
}

func TestSQLQueuedPrincipalResolverPreservesPublicNotFoundAndRetryableContextErrors(t *testing.T) {
	_, accounts, _, _, resolver := newSQLQueuedPrincipalResolverFixture(t)
	_, err := accounts.GetAccount(context.Background(), "principal-public-missing")
	if err == nil || err.Error() != `account "principal-public-missing" not found` || !errors.Is(err, errAccountNotFound) {
		t.Fatalf("GetAccount missing error=%v, want stable message and internal sentinel", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = resolver.ResolveRunPrincipal(ctx, "acme", "principal-context-cancelled")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled resolver error=%v, want context cancellation", err)
	}
	for _, permanent := range []error{ErrQueuedPrincipalNotFound, ErrQueuedPrincipalIdentityMismatch, ErrQueuedPrincipalInactive, ErrQueuedPrincipalInvalid} {
		if errors.Is(err, permanent) {
			t.Fatalf("context failure=%v was misclassified as permanent identity error %v", err, permanent)
		}
	}
}

func TestSQLQueuedPrincipalResolverCopiesAndValidatesRoot(t *testing.T) {
	sessions := newTestSQLStore(t)
	accounts, err := NewSQLAccountStore(sessions.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	root := []core.ScopeRef{{Kind: core.ScopeGlobal, ID: "global"}, {Kind: core.ScopeProduct, ID: "product"}}
	resolver, err := NewSQLQueuedPrincipalResolver(accounts, root)
	if err != nil {
		t.Fatal(err)
	}
	root[1].ID = "mutated"
	createQueuedPrincipalAccount(t, accounts, "principal-root-copy", RoleAccountUser, "acme", AccountActive)
	principal, err := resolver.ResolveRunPrincipal(context.Background(), "acme", "principal-root-copy")
	if err != nil {
		t.Fatal(err)
	}
	want := core.MustScopePath(
		core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"},
		core.ScopeRef{Kind: core.ScopeProduct, ID: "product"},
		core.ScopeRef{Kind: core.ScopeTenant, ID: "acme"},
		core.ScopeRef{Kind: core.ScopeUser, ID: "principal-root-copy"},
	)
	if !principal.Scope.Equal(want) {
		t.Fatalf("principal scope=%s, want %s", principal.Scope, want)
	}
	defaultResolver, err := NewSQLQueuedPrincipalResolver(accounts, nil)
	if err != nil {
		t.Fatal(err)
	}
	defaultPrincipal, err := defaultResolver.ResolveRunPrincipal(context.Background(), "acme", "principal-root-copy")
	if err != nil || defaultPrincipal.Scope.String() != "global:global/tenant:acme/user:principal-root-copy" {
		t.Fatalf("default root principal=%#v err=%v", defaultPrincipal, err)
	}
	for _, root := range [][]core.ScopeRef{
		{{Kind: core.ScopeProduct, ID: "product"}},
		{{Kind: core.ScopeGlobal, ID: "global"}, {Kind: core.ScopeTenant, ID: "acme"}},
		{{Kind: core.ScopeGlobal, ID: "bad/id"}},
	} {
		if _, err := NewSQLQueuedPrincipalResolver(accounts, root); err == nil {
			t.Fatalf("invalid root %#v was accepted", root)
		}
	}
}

func TestSQLQueuedPrincipalResolverReadsEpochAndIsSafeDuringStatusChanges(t *testing.T) {
	sessions, accounts, _, _, resolver := newSQLQueuedPrincipalResolverFixture(t)
	sessions.db.SetMaxOpenConns(1)
	sessions.db.SetMaxIdleConns(1)
	createQueuedPrincipalAccount(t, accounts, "principal-epoch", RoleAccountUser, "acme", AccountActive)
	before, err := resolver.AuthorizationEpoch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := accounts.SetAccountStatus(context.Background(), "principal-epoch", AccountDisabled); err != nil {
		t.Fatal(err)
	}
	after, err := resolver.AuthorizationEpoch(context.Background())
	if err != nil || after != before+1 {
		t.Fatalf("authorization epoch after disable=%d err=%v, want %d", after, err, before+1)
	}
	if err := accounts.SetAccountStatus(context.Background(), "principal-epoch", AccountActive); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	errCh := make(chan error, 2)
	var group sync.WaitGroup
	group.Add(2)
	go func() {
		defer group.Done()
		<-start
		for index := 0; index < 40; index++ {
			status := AccountDisabled
			if index%2 == 1 {
				status = AccountActive
			}
			if err := accounts.SetAccountStatus(context.Background(), "principal-epoch", status); err != nil {
				errCh <- err
				return
			}
		}
	}()
	go func() {
		defer group.Done()
		<-start
		for index := 0; index < 160; index++ {
			_, err := resolver.ResolveRunPrincipal(context.Background(), "acme", "principal-epoch")
			if err != nil && !errors.Is(err, ErrQueuedPrincipalInactive) {
				errCh <- fmt.Errorf("resolve during status change: %w", err)
				return
			}
		}
	}()
	close(start)
	group.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
}

func TestSQLQueuedPrincipalResolverLeavesDatabaseFailuresRetryable(t *testing.T) {
	sessions, accounts, _, _, resolver := newSQLQueuedPrincipalResolverFixture(t)
	createQueuedPrincipalAccount(t, accounts, "principal-db-failure", RoleAccountUser, "acme", AccountActive)
	if err := sessions.db.Close(); err != nil {
		t.Fatal(err)
	}
	_, err := resolver.ResolveRunPrincipal(context.Background(), "acme", "principal-db-failure")
	if err == nil {
		t.Fatal("closed database unexpectedly resolved queued principal")
	}
	for _, permanent := range []error{ErrQueuedPrincipalNotFound, ErrQueuedPrincipalIdentityMismatch, ErrQueuedPrincipalInactive, ErrQueuedPrincipalInvalid} {
		if errors.Is(err, permanent) {
			t.Fatalf("database failure=%v was misclassified as permanent identity error %v", err, permanent)
		}
	}
}

func TestValidateAuthorizationEpochSQLPrincipalAuthority(t *testing.T) {
	sessions, accounts, queue, journal, resolver := newSQLQueuedPrincipalResolverFixture(t)
	if err := ValidateAuthorizationEpochSQLPrincipalAuthority(resolver, sessions, queue, sessions, journal); err != nil {
		t.Fatalf("native shared SQL authority rejected: %v", err)
	}
	if err := ValidateAuthorizationEpochSQLPrincipalAuthority(&transparentSQLQueuedPrincipalResolver{SQLQueuedPrincipalResolver: resolver}, sessions, queue, sessions, journal); err != nil {
		t.Fatalf("transparent resolver wrapper rejected: %v", err)
	}
	if err := ValidateAuthorizationEpochSQLPrincipalAuthority(unsealedQueuedPrincipalResolver{}, sessions, queue, sessions, journal); err == nil || !strings.Contains(err.Error(), "native SQL queued principal resolver") {
		t.Fatalf("unsealed resolver error=%v", err)
	}
	other := newTestSQLStore(t)
	otherAccounts, err := NewSQLAccountStore(other.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	crossResolver, err := NewSQLQueuedPrincipalResolver(otherAccounts, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateAuthorizationEpochSQLPrincipalAuthority(crossResolver, sessions, queue, sessions, journal); err == nil || !strings.Contains(err.Error(), "share one SQL database handle") {
		t.Fatalf("cross database resolver error=%v", err)
	}
	differentDialectAccounts, err := NewSQLAccountStore(accounts.db, SQLDialectPostgres)
	if err != nil {
		t.Fatal(err)
	}
	differentDialect, err := NewSQLQueuedPrincipalResolver(differentDialectAccounts, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateAuthorizationEpochSQLPrincipalAuthority(differentDialect, sessions, queue, sessions, journal); err == nil || !strings.Contains(err.Error(), "share one SQL database handle and dialect") {
		t.Fatalf("different dialect resolver error=%v", err)
	}
	otherJournal, err := NewSQLToolInvocationJournal(other.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateAuthorizationEpochSQLPrincipalAuthority(resolver, sessions, queue, sessions, otherJournal); err == nil || !strings.Contains(err.Error(), "sessions and tool journal") {
		t.Fatalf("cross journal authority error=%v", err)
	}
}

func TestPostgresSQLQueuedPrincipalResolver(t *testing.T) {
	db := newPostgresTestDB(t)
	ctx := context.Background()
	sessions, err := OpenSQLSessionStore(ctx, db, SQLDialectPostgres)
	if err != nil {
		t.Fatal(err)
	}
	accounts, err := NewSQLAccountStore(db, SQLDialectPostgres)
	if err != nil {
		t.Fatal(err)
	}
	queue, err := NewSQLRunControlStore(db, SQLDialectPostgres)
	if err != nil {
		t.Fatal(err)
	}
	journal, err := NewSQLToolInvocationJournal(db, SQLDialectPostgres)
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := NewSQLQueuedPrincipalResolver(accounts, nil)
	if err != nil {
		t.Fatal(err)
	}
	createQueuedPrincipalAccount(t, accounts, "principal-postgres", RoleAccountUser, "acme", AccountActive)
	principal, err := resolver.ResolveRunPrincipal(ctx, "acme", "principal-postgres")
	if err != nil || principal.SubjectID != "principal-postgres" {
		t.Fatalf("postgres principal=%#v err=%v", principal, err)
	}
	if err := ValidateAuthorizationEpochSQLPrincipalAuthority(resolver, sessions, queue, sessions, journal); err != nil {
		t.Fatal(err)
	}
}
