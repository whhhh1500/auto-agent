package storage

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/whhhh1500/auto-agent/pkg/control"
	core "github.com/whhhh1500/auto-agent/pkg/core"
)

func TestVerifyNativeStrictStaticControlRejectsEveryDynamicArtifact(t *testing.T) {
	for _, test := range []struct {
		name string
		seed func(*testing.T, *SQLSessionStore)
	}{
		{
			name: "binding",
			seed: func(t *testing.T, sessions *SQLSessionStore) {
				journal, err := NewSQLBindingJournal(sessions.db, SQLDialectSQLite)
				if err != nil {
					t.Fatal(err)
				}
				if err := journal.Record(context.Background(), BindingRecord{ID: "native-preflight-binding", Kind: "policy"}); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "release",
			seed: func(t *testing.T, sessions *SQLSessionStore) {
				_, product, _, _ := testScopes()
				name := "Native preflight release"
				layer := core.AgentProfileLayer{Scope: product, ProfileID: "native.preflight", Name: &name}
				revision, err := core.ProfileLayerRevision(layer)
				if err != nil {
					t.Fatal(err)
				}
				if err := sessions.RecordRelease(context.Background(), control.ReleaseInfo{
					ProfileID: layer.ProfileID, Version: 1, Scope: product, Layer: &layer,
					Revision: revision, CreatedAt: time.Now().UTC(),
				}); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "canary",
			seed: func(t *testing.T, sessions *SQLSessionStore) {
				store, err := NewSQLCanaryStore(sessions.db, SQLDialectSQLite)
				if err != nil {
					t.Fatal(err)
				}
				_, product, _, _ := testScopes()
				if err := store.CreateCanary(context.Background(), sqlCanaryTestRecord(t, "native-preflight-canary", product)); err != nil {
					t.Fatal(err)
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			sessions := newTestSQLStore(t)
			if err := VerifyNativeStrictStaticControl(context.Background(), sessions.db, SQLDialectSQLite); err != nil {
				t.Fatalf("empty preflight: %v", err)
			}
			test.seed(t, sessions)
			if err := VerifyNativeStrictStaticControl(context.Background(), sessions.db, SQLDialectSQLite); err == nil ||
				!errors.Is(err, ErrNativeStrictDynamicControl) || !strings.Contains(err.Error(), test.name) {
				t.Fatalf("%s artifact error=%v", test.name, err)
			}
		})
	}
}

func TestVerifyNativeStrictStaticControlDoesNotClassifyInjectedInfrastructureFailureAsArtifact(t *testing.T) {
	sessions := newTestSQLStore(t)
	transient := errors.New("temporary transaction read failure")
	err := verifyNativeStrictStaticControl(context.Background(), sessions.db, SQLDialectSQLite, func(int) error {
		return transient
	})
	if !errors.Is(err, transient) {
		t.Fatalf("injected preflight failure=%v", err)
	}
	if errors.Is(err, ErrNativeStrictDynamicControl) {
		t.Fatalf("infrastructure failure was classified as dynamic control: %v", err)
	}
	if err := VerifyNativeStrictStaticControl(context.Background(), sessions.db, SQLDialectSQLite); err != nil {
		t.Fatalf("next preflight did not recover after transient failure: %v", err)
	}
}

func TestVerifyNativeStrictStaticControlUsesOneConsistentReadSnapshot(t *testing.T) {
	sessions := newTestSQLStore(t)
	_, product, _, _ := testScopes()
	name := "Concurrent release"
	layer := core.AgentProfileLayer{Scope: product, ProfileID: "native.snapshot", Name: &name}
	revision, err := core.ProfileLayerRevision(layer)
	if err != nil {
		t.Fatal(err)
	}
	var hookErr error
	err = verifyNativeStrictStaticControl(context.Background(), sessions.db, SQLDialectSQLite, func(index int) error {
		if index != 0 {
			return nil
		}
		hookErr = sessions.RecordRelease(context.Background(), control.ReleaseInfo{
			ProfileID: layer.ProfileID, Version: 1, Scope: product, Layer: &layer,
			Revision: revision, CreatedAt: time.Now().UTC(),
		})
		return hookErr
	})
	if err != nil {
		t.Fatalf("preflight mixed snapshots after concurrent insert: %v", err)
	}
	if hookErr != nil {
		t.Fatalf("concurrent release insert: %v", hookErr)
	}
	if err := VerifyNativeStrictStaticControl(context.Background(), sessions.db, SQLDialectSQLite); err == nil || !strings.Contains(err.Error(), "release") {
		t.Fatalf("next snapshot did not observe release: %v", err)
	}
}

func TestPostgresVerifyNativeStrictStaticControl(t *testing.T) {
	ctx := context.Background()
	db := newPostgresTestDB(t)
	sessions, err := OpenSQLSessionStore(ctx, db, SQLDialectPostgres)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("empty", func(t *testing.T) {
		if err := VerifyNativeStrictStaticControl(ctx, sessions.db, SQLDialectPostgres); err != nil {
			t.Fatalf("empty preflight: %v", err)
		}
	})

	for _, test := range []struct {
		name string
		seed func(*testing.T, *SQLSessionStore)
	}{
		{
			name: "binding",
			seed: func(t *testing.T, sessions *SQLSessionStore) {
				journal, err := NewSQLBindingJournal(sessions.db, SQLDialectPostgres)
				if err != nil {
					t.Fatal(err)
				}
				if err := journal.Record(context.Background(), BindingRecord{ID: "postgres-native-preflight-binding", Kind: "policy"}); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "release",
			seed: func(t *testing.T, sessions *SQLSessionStore) {
				_, product, _, _ := testScopes()
				name := "Postgres native preflight release"
				layer := core.AgentProfileLayer{Scope: product, ProfileID: "native.preflight", Name: &name}
				revision, err := core.ProfileLayerRevision(layer)
				if err != nil {
					t.Fatal(err)
				}
				if err := sessions.RecordRelease(context.Background(), control.ReleaseInfo{
					ProfileID: layer.ProfileID, Version: 1, Scope: product, Layer: &layer,
					Revision: revision, CreatedAt: time.Now().UTC(),
				}); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "canary",
			seed: func(t *testing.T, sessions *SQLSessionStore) {
				store, err := NewSQLCanaryStore(sessions.db, SQLDialectPostgres)
				if err != nil {
					t.Fatal(err)
				}
				_, product, _, _ := testScopes()
				if err := store.CreateCanary(context.Background(), sqlCanaryTestRecord(t, "postgres-native-preflight-canary", product)); err != nil {
					t.Fatal(err)
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			db := newPostgresTestDB(t)
			sessions, err := OpenSQLSessionStore(ctx, db, SQLDialectPostgres)
			if err != nil {
				t.Fatal(err)
			}
			if err := VerifyNativeStrictStaticControl(ctx, sessions.db, SQLDialectPostgres); err != nil {
				t.Fatalf("empty preflight: %v", err)
			}
			test.seed(t, sessions)
			err = VerifyNativeStrictStaticControl(ctx, sessions.db, SQLDialectPostgres)
			if !errors.Is(err, ErrNativeStrictDynamicControl) || !strings.Contains(err.Error(), test.name) {
				t.Fatalf("%s artifact error=%v", test.name, err)
			}
		})
	}
}

func TestPostgresVerifyNativeStrictStaticControlUsesRepeatableReadSnapshot(t *testing.T) {
	ctx := context.Background()
	db := newPostgresTestDB(t)
	sessions, err := OpenSQLSessionStore(ctx, db, SQLDialectPostgres)
	if err != nil {
		t.Fatal(err)
	}
	_, product, _, _ := testScopes()
	name := "Postgres concurrent release"
	layer := core.AgentProfileLayer{Scope: product, ProfileID: "native.snapshot", Name: &name}
	revision, err := core.ProfileLayerRevision(layer)
	if err != nil {
		t.Fatal(err)
	}

	var releaseErr error
	err = verifyNativeStrictStaticControl(ctx, sessions.db, SQLDialectPostgres, func(index int) error {
		if index != 0 {
			return nil
		}
		releaseErr = sessions.RecordRelease(ctx, control.ReleaseInfo{
			ProfileID: layer.ProfileID, Version: 1, Scope: product, Layer: &layer,
			Revision: revision, CreatedAt: time.Now().UTC(),
		})
		return releaseErr
	})
	if err != nil {
		t.Fatalf("preflight mixed PostgreSQL snapshots after concurrent insert: %v", err)
	}
	if releaseErr != nil {
		t.Fatalf("concurrent release insert: %v", releaseErr)
	}
	if err := VerifyNativeStrictStaticControl(ctx, sessions.db, SQLDialectPostgres); !errors.Is(err, ErrNativeStrictDynamicControl) || !strings.Contains(err.Error(), "release") {
		t.Fatalf("next PostgreSQL snapshot did not observe release: %v", err)
	}
}
