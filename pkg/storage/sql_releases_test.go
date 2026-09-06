package storage

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/cc-auto-agent/harness-core/pkg/control"
	core "github.com/cc-auto-agent/harness-core/pkg/core"
)

func TestSQLReleaseStoreRejectsInvalidProfileIDsBeforeSQL(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	store := &SQLSessionStore{db: db, dialect: SQLDialectSQLite}
	ctx := context.Background()
	assertBeforeSQL := func(name string, err error) {
		t.Helper()
		if err == nil {
			t.Fatalf("%s was accepted", name)
		}
		if strings.Contains(err.Error(), "no such table") {
			t.Fatalf("%s reached SQL before validation: %v", name, err)
		}
	}
	assertBeforeSQL("load", func() error {
		_, err := store.LoadReleases(ctx, "bad id")
		return err
	}())
	assertBeforeSQL("rollback", store.MarkRolledBack(ctx, "nodot", []int{1}))
	assertBeforeSQL("record", store.RecordRelease(ctx, control.ReleaseInfo{ProfileID: "bad id", Version: 1}))
}

func TestSQLReleaseStoreLoadReleasesAcceptsNamespacedProfile(t *testing.T) {
	store := newTestSQLStore(t)
	ctx := context.Background()
	infos, err := store.LoadReleases(ctx, "storage.agent")
	if err != nil || len(infos) != 0 {
		t.Fatalf("empty namespaced load = %#v err=%v", infos, err)
	}
}

func sqlReleaseInfo(t *testing.T, profileID string, version int, scope core.ScopePath) control.ReleaseInfo {
	t.Helper()
	name := "Release Candidate"
	layer := core.AgentProfileLayer{Scope: scope, ProfileID: profileID, Name: &name}
	revision, err := core.ProfileLayerRevision(layer)
	if err != nil {
		t.Fatal(err)
	}
	return control.ReleaseInfo{
		ProfileID: profileID, Version: version, Scope: scope, Layer: &layer, Revision: revision,
		CreatedAt: time.Now().UTC(),
	}
}

func TestSQLReleaseStoreRejectsHistoryOverflow(t *testing.T) {
	store := newTestSQLStore(t)
	store.maxReleaseHistory = 2
	ctx := context.Background()
	_, product, _, _ := testScopes()
	if err := store.RecordRelease(ctx, sqlReleaseInfo(t, "storage.agent", 1, product)); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordRelease(ctx, sqlReleaseInfo(t, "storage.agent", 2, product)); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordRelease(ctx, sqlReleaseInfo(t, "storage.agent", 3, product)); err == nil {
		t.Fatal("release history overflow was accepted")
	}
	if err := store.RecordRelease(ctx, sqlReleaseInfo(t, "storage.other", 1, product)); err != nil {
		t.Fatalf("other profile should have its own history cap: %v", err)
	}
}
