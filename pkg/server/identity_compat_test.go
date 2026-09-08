package server

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	appidentity "github.com/whhhh1500/auto-agent/pkg/app/identity"
	"github.com/whhhh1500/auto-agent/pkg/storage"

	_ "modernc.org/sqlite"
)

func TestLegacyIdentityActivationDoesNotReadAfterCommit(t *testing.T) {
	db, err := sql.Open("sqlite", t.TempDir()+"/identity.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := storage.OpenSQLSessionStore(context.Background(), db, storage.SQLDialectSQLite); err != nil {
		t.Fatal(err)
	}
	legacy, err := storage.NewSQLAccountStore(db, storage.SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	if err := legacy.CreateAccount(context.Background(), storage.Account{
		AccountID: "pending_admin", Role: storage.RoleAccountAdmin, TenantID: "system",
		Status: storage.AccountPendingActivation, MustChangePassword: true,
	}, "initial-password-123"); err != nil {
		t.Fatal(err)
	}
	restricted, err := legacy.CreateToken(context.Background(), "pending_admin", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	tracked := &getCountingAccountStore{AccountStore: legacy}
	credentials, err := newLegacyIdentityCredentials(tracked)
	if err != nil {
		t.Fatal(err)
	}
	result, err := credentials.ActivateInitial(context.Background(), appidentity.ActivateCommand{
		AccountID: "pending_admin", Password: "replacement-password-456",
	}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if tracked.getCalls != 1 {
		t.Fatalf("activation performed %d account reads; want exactly pre-commit read", tracked.getCalls)
	}
	if result.Token == "" || result.Account.Status != appidentity.StatusActive || result.Account.MustChangePassword {
		t.Fatalf("activation result=%#v", result)
	}
	if _, err := legacy.ResolveToken(context.Background(), restricted); err == nil {
		t.Fatal("restricted token survived activation")
	}
	if _, err := legacy.ResolveToken(context.Background(), result.Token); err != nil {
		t.Fatalf("normal token was not committed: %v", err)
	}
}

type getCountingAccountStore struct {
	storage.AccountStore
	getCalls  int
	activated bool
}

func (store *getCountingAccountStore) GetAccount(ctx context.Context, accountID string) (storage.Account, error) {
	store.getCalls++
	if store.activated {
		return storage.Account{}, errors.New("post-commit account reads are unavailable")
	}
	return store.AccountStore.GetAccount(ctx, accountID)
}

func (store *getCountingAccountStore) ActivateInitialAccount(ctx context.Context, accountID, password string, ttl time.Duration) (string, error) {
	token, err := store.AccountStore.ActivateInitialAccount(ctx, accountID, password, ttl)
	if err == nil {
		store.activated = true
	}
	return token, err
}
