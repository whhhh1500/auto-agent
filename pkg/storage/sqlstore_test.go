package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	. "github.com/cc-auto-agent/harness-core/pkg/core"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cc-auto-agent/harness-core/pkg/evaluation"

	_ "modernc.org/sqlite" // pure-Go demo driver; the kernel itself stays driver-free
)

func newTestSQLStore(t *testing.T) *SQLSessionStore {
	t.Helper()
	db, err := sql.Open("sqlite", t.TempDir()+"/sessions.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	store, err := OpenSQLSessionStore(context.Background(), db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func TestSQLSchemaV31MigratesAccountIdentity(t *testing.T) {
	db, err := sql.Open("sqlite", t.TempDir()+"/legacy.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := OpenSQLSessionStore(context.Background(), db, SQLDialectSQLite); err != nil {
		t.Fatal(err)
	}
	legacy := `
		DROP TABLE auth_tokens;
		DROP TABLE accounts;
		CREATE TABLE accounts (
			email TEXT PRIMARY KEY, password_hash TEXT NOT NULL, role TEXT NOT NULL,
			tenant_id TEXT NOT NULL, status TEXT NOT NULL, created_at BIGINT NOT NULL
		);
		CREATE TABLE auth_tokens (token_hash TEXT PRIMARY KEY, email TEXT NOT NULL, expires_at BIGINT NOT NULL);
		INSERT INTO accounts VALUES ('legacy@example.com', 'hash', 'admin', 'system', 'active', 1234);
		INSERT INTO auth_tokens VALUES ('token-hash', 'legacy@example.com', 9999999999999);
		UPDATE store_meta SET value = '31' WHERE key = 'schema_version';
	`
	if _, err := db.Exec(legacy); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSQLSessionStore(context.Background(), db, SQLDialectSQLite); err != nil {
		t.Fatal(err)
	}
	accounts, err := NewSQLAccountStore(db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	account, err := accounts.GetAccount(context.Background(), "legacy@example.com")
	if err != nil {
		t.Fatal(err)
	}
	if account.AccountID != "legacy@example.com" || account.Email != "legacy@example.com" || account.MustChangePassword {
		t.Fatalf("migrated account = %#v", account)
	}
	var tokenAccount string
	if err := db.QueryRow("SELECT account_id FROM auth_tokens WHERE token_hash='token-hash'").Scan(&tokenAccount); err != nil || tokenAccount != account.AccountID {
		t.Fatalf("migrated token account=%q err=%v", tokenAccount, err)
	}
	var version string
	if err := db.QueryRow("SELECT value FROM store_meta WHERE key='schema_version'").Scan(&version); err != nil || version != strconv.Itoa(SQLSchemaVersion) {
		t.Fatalf("schema version=%q err=%v", version, err)
	}
}

func TestSQLSchemaV31RepairsMissingAuthTokensDuringAccountMigration(t *testing.T) {
	db, err := sql.Open("sqlite", t.TempDir()+"/legacy-missing-tokens.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := OpenSQLSessionStore(context.Background(), db, SQLDialectSQLite); err != nil {
		t.Fatal(err)
	}
	legacy := `
		DROP TABLE auth_tokens;
		DROP TABLE accounts;
		CREATE TABLE accounts (
			email TEXT PRIMARY KEY, password_hash TEXT NOT NULL, role TEXT NOT NULL,
			tenant_id TEXT NOT NULL, status TEXT NOT NULL, created_at BIGINT NOT NULL
		);
		INSERT INTO accounts VALUES ('legacy@example.com', 'hash', 'admin', 'system', 'active', 1234);
		UPDATE store_meta SET value = '31' WHERE key = 'schema_version';
	`
	if _, err := db.Exec(legacy); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSQLSessionStore(context.Background(), db, SQLDialectSQLite); err != nil {
		t.Fatal(err)
	}
	var accountIDColumn int
	if err := db.QueryRow("SELECT COUNT(*) FROM pragma_table_info('auth_tokens') WHERE name='account_id'").Scan(&accountIDColumn); err != nil || accountIDColumn != 1 {
		t.Fatalf("repaired auth_tokens account_id column=%d err=%v", accountIDColumn, err)
	}
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM auth_tokens").Scan(&count); err != nil || count != 0 {
		t.Fatalf("repaired auth token count=%d err=%v", count, err)
	}
}

func TestSQLSessionStoreRoundTripAndConflict(t *testing.T) {
	store := newTestSQLStore(t)
	_, _, _, user := testScopes()
	principal := testPrincipal(user)
	session := mustSession(t, user, principal)
	ctx := context.Background()

	if err := store.Create(ctx, session); err != nil {
		t.Fatal(err)
	}
	if err := store.Create(ctx, session); !errors.Is(err, ErrSessionConflict) {
		t.Fatalf("duplicate create must conflict, got %v", err)
	}

	session.Append("run-a", EvUserMessage, UserMessageData{Text: "hello"})
	session.Append("run-a", EvAssistantMessage, AssistantMessageData{Text: "world"})
	if err := store.Save(ctx, session, 0); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.Load(ctx, session.ID())
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Version() != session.Version() {
		t.Fatalf("round trip lost events: %d vs %d", loaded.Version(), session.Version())
	}
	original := session.Events()
	restored := loaded.Events()
	if string(original[0].Data) != string(restored[0].Data) {
		t.Fatalf("event payload diverged: %s vs %s", original[0].Data, restored[0].Data)
	}
	if loaded.Principal().SubjectID != "user-a" || loaded.ProfileID() != session.ProfileID() {
		t.Fatalf("header not restored: %#v", loaded)
	}

	// Optimistic concurrency inside the transaction.
	session.Append("run-a", EvRunEnd, RunEndData{Status: RunCompleted})
	if err := store.Save(ctx, session, 0); !errors.Is(err, ErrSessionConflict) {
		t.Fatalf("stale save must conflict, got %v", err)
	}
	if err := store.Save(ctx, session, 2); err != nil {
		t.Fatalf("fresh save failed: %v", err)
	}
	again, err := store.Load(ctx, session.ID())
	if err != nil {
		t.Fatal(err)
	}
	if again.Version() != session.Version() {
		t.Fatalf("append lost events: %d vs %d", again.Version(), session.Version())
	}

	// Missing sessions map to the canonical error.
	if _, err := store.Load(ctx, "missing"); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("missing session error: %v", err)
	}
}

func TestSQLSessionStoreRejectsInvalidIDsBeforeSQL(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	bare := &SQLSessionStore{db: db, dialect: SQLDialectSQLite}
	store := newTestSQLStore(t)
	ctx := context.Background()
	assertBeforeSQL := func(name string, err error) {
		t.Helper()
		if err == nil {
			t.Fatalf("%s was accepted", name)
		}
		if errors.Is(err, ErrSessionNotFound) {
			t.Fatalf("%s looked up SQL instead of validating: %v", name, err)
		}
		if strings.Contains(err.Error(), "no such table") {
			t.Fatalf("%s reached SQL before validation: %v", name, err)
		}
	}
	for _, id := range []string{"", "bad id", "slash/id", "nul\x00id"} {
		assertBeforeSQL("bare load "+id, func() error { _, err := bare.Load(ctx, id); return err }())
		assertBeforeSQL("store load "+id, func() error { _, err := store.Load(ctx, id); return err }())
		assertBeforeSQL("append "+id, store.AppendEvents(ctx, id, 0, nil))
		assertBeforeSQL("acquire "+id, func() error { _, err := store.AcquireSessionLease(ctx, id, "holder", time.Minute); return err }())
		assertBeforeSQL("renew "+id, func() error { _, err := store.RenewSessionLease(ctx, id, "holder", time.Minute); return err }())
		assertBeforeSQL("release "+id, store.ReleaseSessionLease(ctx, id, "holder"))
	}
	assertBeforeSQL("acquire empty holder", func() error {
		_, err := bare.AcquireSessionLease(ctx, "sess-1", "", time.Minute)
		return err
	}())
	assertBeforeSQL("acquire NUL holder", func() error {
		_, err := bare.AcquireSessionLease(ctx, "sess-1", "inst\x00a", time.Minute)
		return err
	}())
	assertBeforeSQL("renew control holder", func() error {
		_, err := store.RenewSessionLease(ctx, "sess-1", "inst\n", time.Minute)
		return err
	}())
	assertBeforeSQL("release empty holder", store.ReleaseSessionLease(ctx, "sess-1", "   "))
}

func TestSQLSessionStoreRepairsInterruptedTail(t *testing.T) {
	store := newTestSQLStore(t)
	_, _, _, user := testScopes()
	principal := testPrincipal(user)
	session := mustSession(t, user, principal)
	ctx := context.Background()
	if err := store.Create(ctx, session); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Append("run-crash", EvRunStart, RunStartData{}); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Append("run-crash", EvUserMessage, UserMessageData{Text: "hi"}); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Append("run-crash", EvToolCall, ToolCallData{CallID: "c1", Name: "x.tool"}); err != nil {
		t.Fatal(err)
	}
	if err := store.Save(ctx, session, 0); err != nil {
		t.Fatal(err)
	}

	loaded, err := store.Load(ctx, session.ID())
	if err != nil {
		t.Fatal(err)
	}
	types := []SessionEventType{}
	for _, event := range loaded.Events() {
		types = append(types, event.Type)
	}
	if types[len(types)-1] != EvRunEnd || !containsEventType(types, EvRunError) {
		t.Fatalf("SQL load did not repair the interrupted tail: %v", types)
	}
	second, err := store.Load(ctx, session.ID())
	if err != nil {
		t.Fatal(err)
	}
	if second.Version() != loaded.Version() {
		t.Fatal("SQL repair is not idempotent")
	}
}

func TestSQLSessionStoreListsCatalog(t *testing.T) {
	store := newTestSQLStore(t)
	_, _, _, user := testScopes()
	principal := testPrincipal(user)
	ctx := context.Background()

	for _, id := range []string{"sess-a", "sess-b"} {
		sessionScope, _ := user.Child(ScopeRef{Kind: ScopeSession, ID: id})
		session, err := NewSession(SessionOptions{
			ID: id, ProfileID: "product.agent", Principal: principal, Scope: sessionScope,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Create(ctx, session); err != nil {
			t.Fatal(err)
		}
		session.Append("run-1", EvUserMessage, UserMessageData{Text: "hi"})
		if err := store.Save(ctx, session, 0); err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Millisecond) // distinct updated_at ordering
		if id == "sess-b" {
			// touch sess-b last so it sorts first
		}
	}

	lister, ok := any(store).(SessionLister)
	if !ok {
		t.Fatal("SQLSessionStore must implement SessionLister")
	}
	catalog, err := lister.ListSessions(ctx, "tenant-a", "user-a", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(catalog) != 2 {
		t.Fatalf("unexpected catalog size: %#v", catalog)
	}
	if catalog[0].ID != "sess-b" || catalog[0].EventCount != 1 {
		t.Fatalf("catalog ordering or counts wrong: %#v", catalog)
	}
	empty, err := lister.ListSessions(ctx, "tenant-a", "nobody", 10)
	if err != nil || len(empty) != 0 {
		t.Fatalf("unexpected rows for other user: %#v %v", empty, err)
	}
}

func TestSQLSchemaVersionRefusesFuture(t *testing.T) {
	db, err := sql.Open("sqlite", t.TempDir()+"/future.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store, err := OpenSQLSessionStore(context.Background(), db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	// Simulate a store written by a NEWER build.
	if _, err := db.Exec("UPDATE store_meta SET value = '999' WHERE key = 'schema_version'"); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSQLSessionStore(context.Background(), db, SQLDialectSQLite); err == nil {
		t.Fatal("future schema version must be refused")
	}
	_ = store
}

func TestSQLFutureSchemaIsRefusedBeforeCurrentDDL(t *testing.T) {
	db, err := sql.Open("sqlite", t.TempDir()+"/future-minimal.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE store_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
		INSERT INTO store_meta (key, value) VALUES ('schema_version', '999');
	`); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSQLSessionStore(ctx, db, SQLDialectSQLite); err == nil {
		t.Fatal("future schema version must be refused")
	}
	var created int
	if err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='run_submissions'",
	).Scan(&created); err != nil {
		t.Fatal(err)
	}
	if created != 0 {
		t.Fatal("current DDL ran before future schema refusal")
	}
}

func TestSQLSchemaV9UpgradesQueueGenerationAndApprovals(t *testing.T) {
	db, err := sql.Open("sqlite", t.TempDir()+"/v9.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, sqlSchemaV9); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, sqlInsertMetaRow.bind(SQLDialectSQLite), "9"); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSQLSessionStore(ctx, db, SQLDialectSQLite); err != nil {
		t.Fatalf("v9 upgrade failed: %v", err)
	}
	rows, err := db.QueryContext(ctx, "PRAGMA table_info(run_queue)")
	if err != nil {
		t.Fatal(err)
	}
	foundGeneration := false
	foundTraceParent := false
	foundTraceState := false
	for rows.Next() {
		var cid int
		var name, columnType string
		var notNull, primaryKey int
		var defaultValue any
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			t.Fatal(err)
		}
		if name == "generation" {
			foundGeneration = true
		} else if name == "trace_parent" {
			foundTraceParent = true
		} else if name == "trace_state" {
			foundTraceState = true
		}
	}
	_ = rows.Close()
	if !foundGeneration {
		t.Fatal("v10 migration did not add run_queue.generation")
	}
	if !foundTraceParent || !foundTraceState {
		t.Fatalf("v12 migration did not add trace carrier: parent=%t state=%t", foundTraceParent, foundTraceState)
	}
	var approvalTable string
	if err := db.QueryRowContext(ctx, "SELECT name FROM sqlite_master WHERE type='table' AND name='approval_requests'").Scan(&approvalTable); err != nil || approvalTable != "approval_requests" {
		t.Fatalf("v10 approval table missing: table=%q err=%v", approvalTable, err)
	}
	var submissionTable string
	if err := db.QueryRowContext(ctx, "SELECT name FROM sqlite_master WHERE type='table' AND name='run_submissions'").Scan(&submissionTable); err != nil || submissionTable != "run_submissions" {
		t.Fatalf("v11 submission table missing: table=%q err=%v", submissionTable, err)
	}
	for _, table := range []string{"evaluation_datasets", "evaluation_runs", "evaluation_case_results"} {
		var name string
		if err := db.QueryRowContext(ctx, "SELECT name FROM sqlite_master WHERE type='table' AND name=?", table).Scan(&name); err != nil || name != table {
			t.Fatalf("v13 evaluation table %s missing: name=%q err=%v", table, name, err)
		}
	}
	for _, column := range []string{"layer_json", "revision"} {
		var count int
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM pragma_table_info('profile_releases') WHERE name=?", column).Scan(&count); err != nil || count != 1 {
			t.Fatalf("v14 release column %s missing: count=%d err=%v", column, count, err)
		}
	}
	var canaryTable string
	if err := db.QueryRowContext(ctx, "SELECT name FROM sqlite_master WHERE type='table' AND name='profile_canaries'").Scan(&canaryTable); err != nil || canaryTable != "profile_canaries" {
		t.Fatalf("v15 canary table missing: table=%q err=%v", canaryTable, err)
	}
	for table, columns := range map[string][]string{
		"profile_releases":        {"operation_id"},
		"profile_canaries":        {"release_version", "base_release_revision"},
		"evaluation_runs":         {"composition_metadata_json"},
		"evaluation_case_results": {"composition_revision", "assignment_revision"},
	} {
		for _, column := range columns {
			var count int
			if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM pragma_table_info('"+table+"') WHERE name=?", column).Scan(&count); err != nil || count != 1 {
				t.Fatalf("v16 %s.%s missing: count=%d err=%v", table, column, count, err)
			}
		}
	}
	var evidenceTable string
	if err := db.QueryRowContext(ctx, "SELECT name FROM sqlite_master WHERE type='table' AND name='run_evidence'").Scan(&evidenceTable); err != nil || evidenceTable != "run_evidence" {
		t.Fatalf("v21 run evidence table missing: table=%q err=%v", evidenceTable, err)
	}
	var evidenceStatusColumn int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM pragma_table_info('run_evidence') WHERE name='status'").Scan(&evidenceStatusColumn); err != nil || evidenceStatusColumn != 1 {
		t.Fatalf("v22 run evidence status missing: count=%d err=%v", evidenceStatusColumn, err)
	}
	var evidenceVariantColumn int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM pragma_table_info('run_evidence') WHERE name='assignment_variant'").Scan(&evidenceVariantColumn); err != nil || evidenceVariantColumn != 1 {
		t.Fatalf("v23 run evidence assignment variant missing: count=%d err=%v", evidenceVariantColumn, err)
	}
	var evidenceVariantIndex int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='run_evidence_variant_status'").Scan(&evidenceVariantIndex); err != nil || evidenceVariantIndex != 1 {
		t.Fatalf("v23 run evidence variant/status index missing: count=%d err=%v", evidenceVariantIndex, err)
	}
	var controlRevision string
	if err := db.QueryRowContext(ctx, "SELECT value FROM store_meta WHERE key='control_revision'").Scan(&controlRevision); err != nil || controlRevision != "0" {
		t.Fatalf("v18 control revision row missing: value=%q err=%v", controlRevision, err)
	}
}

func TestSQLSchemaV22UpgradesRunEvidenceAssignmentVariant(t *testing.T) {
	ctx := context.Background()
	store := newTestSQLStore(t)
	want := seedV22RunEvidenceVariantSessions(t, store)
	if _, err := store.db.ExecContext(ctx, "DROP INDEX run_evidence_variant_status"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(ctx, "ALTER TABLE run_evidence DROP COLUMN assignment_variant"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(ctx, sqlUpdateMetaRow.bind(SQLDialectSQLite), "22"); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenSQLSessionStore(ctx, store.db, SQLDialectSQLite)
	if err != nil {
		t.Fatalf("v22 to v23 run evidence migration failed: %v", err)
	}
	var columnCount int
	if err := reopened.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM pragma_table_info('run_evidence') WHERE name='assignment_variant'").Scan(&columnCount); err != nil || columnCount != 1 {
		t.Fatalf("v23 assignment variant column missing after migration: count=%d err=%v", columnCount, err)
	}
	var indexCount int
	if err := reopened.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='run_evidence_variant_status'").Scan(&indexCount); err != nil || indexCount != 1 {
		t.Fatalf("v23 variant/status index missing after migration: count=%d err=%v", indexCount, err)
	}
	rows, err := reopened.db.QueryContext(ctx, "SELECT run_id, assignment_variant FROM run_evidence ORDER BY run_id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	got := map[string]AssignmentVariant{}
	for rows.Next() {
		var runID string
		var variant AssignmentVariant
		if err := rows.Scan(&runID, &variant); err != nil {
			t.Fatal(err)
		}
		got[runID] = variant
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("v23 variant backfill returned %d rows, want %d: %#v", len(got), len(want), got)
	}
	for runID, variant := range want {
		if got[runID] != variant {
			t.Fatalf("v23 variant backfill %s=%q, want %q; all=%#v", runID, got[runID], variant, got)
		}
	}
	var version string
	if err := reopened.db.QueryRowContext(ctx, sqlSelectMetaRow.bind(SQLDialectSQLite)).Scan(&version); err != nil || version != strconv.Itoa(SQLSchemaVersion) {
		t.Fatalf("current schema version was not recorded after v23 migration: version=%q err=%v", version, err)
	}
}

func seedV22RunEvidenceVariantSessions(t *testing.T, store *SQLSessionStore) map[string]AssignmentVariant {
	t.Helper()
	_, _, _, user := testScopes()
	principal := testPrincipal(user)
	specs := []struct {
		sessionID string
		runID     string
		metadata  map[string]string
		variant   AssignmentVariant
	}{
		{sessionID: "v23-candidate-session", runID: "v23-candidate-run", metadata: map[string]string{"harness.canary.candidate": "true"}, variant: AssignmentCandidate},
		{sessionID: "v23-live-session", runID: "v23-live-run", metadata: map[string]string{"harness.assignment.variant": "live"}, variant: AssignmentLive},
		{sessionID: "v23-unassigned-session", runID: "v23-unassigned-run", metadata: map[string]string{}, variant: AssignmentUnassigned},
	}
	want := make(map[string]AssignmentVariant, len(specs))
	for _, spec := range specs {
		scope, err := user.Child(ScopeRef{Kind: ScopeSession, ID: spec.sessionID})
		if err != nil {
			t.Fatal(err)
		}
		session, err := NewSession(SessionOptions{
			ID: spec.sessionID, ProfileID: "v23.migration", Principal: principal, Scope: scope,
		})
		if err != nil {
			t.Fatal(err)
		}
		composition := &RunCompositionData{
			Profile:  AgentProfileSnapshot{ProfileID: session.ProfileID(), Scope: session.Scope()},
			Metadata: spec.metadata,
		}
		compositionRevision, err := CompositionRevision(composition)
		if err != nil {
			t.Fatal(err)
		}
		assignmentRevision, err := CompositionMetadataRevision(composition.Metadata)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := session.Append(spec.runID, EvRunStart, RunStartData{
			CompositionRevision: compositionRevision, AssignmentRevision: assignmentRevision, Composition: composition,
		}); err != nil {
			t.Fatal(err)
		}
		if err := store.Create(context.Background(), session); err != nil {
			t.Fatal(err)
		}
		want[spec.runID] = spec.variant
	}
	return want
}

func TestSQLSchemaV19BackfillsEvaluationRevisionProjections(t *testing.T) {
	db, err := sql.Open("sqlite", t.TempDir()+"/v19.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	// Build a v19-shaped database before v20 opens it. Use v15 base DDL so
	// v16 indexes are not attempted before their migrated columns exist.
	if _, err := db.ExecContext(ctx, sqlSchemaV15); err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		"ALTER TABLE profile_releases ADD COLUMN layer_json TEXT NOT NULL DEFAULT '{}'",
		"ALTER TABLE profile_releases ADD COLUMN revision TEXT NOT NULL DEFAULT ''",
		"ALTER TABLE profile_releases ADD COLUMN operation_id TEXT NOT NULL DEFAULT ''",
		"ALTER TABLE profile_canaries ADD COLUMN base_release_revision TEXT NOT NULL DEFAULT ''",
		"INSERT INTO store_meta (key, value) VALUES ('control_revision', '0')",
	} {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.ExecContext(ctx, "ALTER TABLE evaluation_runs ADD COLUMN composition_metadata_json TEXT NOT NULL DEFAULT '{}' "); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, sqlInsertMetaRow.bind(SQLDialectSQLite), "19"); err != nil {
		t.Fatal(err)
	}
	metadata := map[string]string{"harness.assignment.id": "legacy-assignment"}
	metadataJSON, _ := json.Marshal(metadata)
	if _, err := db.ExecContext(ctx, `INSERT INTO evaluation_runs
		(id, dataset_id, dataset_version, dataset_revision, tenant_id, subject_id, profile_id,
		 baseline_run_id, status, score, passed, total_cases, passed_cases, allow_capabilities,
		 composition_metadata_json, metadata_json, error_message, created_at, completed_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"eval_legacy_projection", "evaluation.sql", 1, strings.Repeat("a", 64), "acme", "alice", "evaluation.agent",
		"", "running", 0, 0, 1, 0, "[]", string(metadataJSON), "{}", "", time.Now().UnixMilli(), 0); err != nil {
		t.Fatal(err)
	}
	caseResult := evaluation.CaseResult{
		CaseID: "case-sql", SessionID: "evalsess_legacy", AgentRunID: "evalcase_legacy",
		Status: RunCompleted, Score: 1, Passed: true, CompletedAt: time.Now().UTC(),
		Artifacts: evaluation.ArtifactSnapshot{
			CompositionRevision: strings.Repeat("b", 64), AssignmentRevision: strings.Repeat("c", 64),
		},
	}
	caseJSON, _ := json.Marshal(caseResult)
	if _, err := db.ExecContext(ctx, `INSERT INTO evaluation_case_results
		(run_id, case_id, result_json, score, passed, completed_at) VALUES (?, ?, ?, ?, ?, ?)`,
		"eval_legacy_projection", caseResult.CaseID, string(caseJSON), 1, 1, time.Now().UnixMilli()); err != nil {
		t.Fatal(err)
	}
	store, err := OpenSQLSessionStore(ctx, db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := NewSQLEvaluationStore(store.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	expectedAssignment, err := CompositionMetadataRevision(metadata)
	if err != nil {
		t.Fatal(err)
	}
	run, err := loaded.GetRun(ctx, "eval_legacy_projection")
	if err != nil || run.AssignmentRevision != expectedAssignment || len(run.Cases) != 1 ||
		run.Cases[0].Artifacts.AssignmentRevision != strings.Repeat("c", 64) ||
		run.Cases[0].Artifacts.CompositionRevision != strings.Repeat("b", 64) {
		t.Fatalf("v19 projection backfill failed: %#v err=%v", run, err)
	}
	matched, err := loaded.QueryRuns(ctx, evaluation.RunQuery{
		AssignmentRevision: strings.Repeat("c", 64), CompositionRevision: strings.Repeat("b", 64), Limit: 10,
	})
	if err != nil || len(matched) != 1 || matched[0].ID != run.ID {
		t.Fatalf("backfilled projection is not queryable: %#v err=%v", matched, err)
	}
}

func TestSQLDialectRebindsPlaceholders(t *testing.T) {
	query := sqlQuery{"SELECT version FROM sessions WHERE id = ? AND tenant_id = ?"}
	if got := query.bind(SQLDialectSQLite); got != query.text {
		t.Fatalf("sqlite passthrough broken: %s", got)
	}
	pg := query.bind(SQLDialectPostgres)
	if pg != "SELECT version FROM sessions WHERE id = $1 AND tenant_id = $2" {
		t.Fatalf("postgres rebinding broken: %s", pg)
	}
	literal := sqlQuery{"SELECT 'a?b' FROM sessions WHERE id = ?"}
	if got := literal.bind(SQLDialectPostgres); got != "SELECT 'a?b' FROM sessions WHERE id = $1" {
		t.Fatalf("quoted placeholder must not rebind: %s", got)
	}
}

func TestSQLStoreMatchesFileStoreSemantics(t *testing.T) {
	// The SQL and file stores must be interchangeable: build the same session
	// in both, save the same event sequence, and compare restored logs.
	sqlStore := newTestSQLStore(t)
	fileStore, err := NewFileSessionStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	_, _, _, user := testScopes()
	principal := testPrincipal(user)
	ctx := context.Background()

	for _, store := range []SessionStore{sqlStore, fileStore} {
		session := mustSession(t, user, principal)
		if err := store.Create(ctx, session); err != nil {
			t.Fatal(err)
		}
		session.Append("run-x", EvRunStart, RunStartData{})
		session.Append("run-x", EvUserMessage, UserMessageData{Text: "same"})
		session.Append("run-x", EvToolCall, ToolCallData{CallID: "c1", Name: "n.tool"})
		if err := store.Save(ctx, session, 0); err != nil {
			t.Fatal(err)
		}
		loaded, err := store.Load(ctx, session.ID())
		if err != nil {
			t.Fatal(err)
		}
		// 3 appended events + 3 synthetic closers (tool result, run/error, run/end).
		if loaded.Version() != 6 {
			t.Fatalf("%T restored %d events", store, loaded.Version())
		}
	}
}

func TestSQLSessionStoreRejectsOverflow(t *testing.T) {
	store := newTestSQLStore(t)
	store.maxSessions = 2
	ctx := context.Background()
	if err := store.Create(ctx, mustNamedSession(t, "session-sql-1")); err != nil {
		t.Fatal(err)
	}
	if err := store.Create(ctx, mustNamedSession(t, "session-sql-2")); err != nil {
		t.Fatal(err)
	}
	if err := store.Create(ctx, mustNamedSession(t, "session-sql-3")); err == nil {
		t.Fatal("sql session overflow was accepted")
	} else if !strings.Contains(err.Error(), "stored sessions exceed maximum of 2") {
		t.Fatalf("unexpected overflow error: %v", err)
	}
	if err := store.Create(ctx, mustNamedSession(t, "session-sql-1")); !errors.Is(err, ErrSessionConflict) {
		t.Fatalf("existing sql session must still conflict: %v", err)
	}
}

func TestSQLSessionStoreRejectsEventChunkOverflow(t *testing.T) {
	store := newTestSQLStore(t)
	store.maxEventChunks = 2
	ctx := context.Background()
	session := mustNamedSession(t, "session-chunk-cap")
	if err := store.Create(ctx, session); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Append("run-a", EvUserMessage, UserMessageData{Text: "one"}); err != nil {
		t.Fatal(err)
	}
	if err := store.Save(ctx, session, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Append("run-a", EvUserMessage, UserMessageData{Text: "two"}); err != nil {
		t.Fatal(err)
	}
	if err := store.Save(ctx, session, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Append("run-a", EvUserMessage, UserMessageData{Text: "three"}); err != nil {
		t.Fatal(err)
	}
	if err := store.Save(ctx, session, 2); err == nil {
		t.Fatal("event chunk overflow was accepted")
	} else if !strings.Contains(err.Error(), "session event chunks exceed maximum of 2") {
		t.Fatalf("unexpected overflow error: %v", err)
	}
}
