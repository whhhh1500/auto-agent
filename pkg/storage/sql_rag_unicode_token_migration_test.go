package storage

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	core "github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/extensions/rag"
	moderncsqlite "modernc.org/sqlite"
)

// These are persisted v46 values, deliberately written without Tokenize. The
// tokenizer under test must replace the one legacy CJK-containing token during
// the v47 schema migration.
const (
	v47LegacyToken = "用户，保留凭证"
	v47TargetID    = "v47-target"
	v47HeldOutID   = "v47-heldout"
	v47SiblingID   = "v47-sibling"
)

type v47RagFixture struct {
	queryScope   core.ScopePath
	targetScope  core.ScopePath
	siblingScope core.ScopePath
}

type v47CanonicalDocument struct {
	scope, id, source, content, tags, tagsJSON string
}

type v47LegacyTokenLimitFixture struct {
	content string
	tokens  []string
}

func TestSQLiteRagUnicodeTokenizerV47Migration(t *testing.T) {
	t.Run("rebuilds_legacy_projection_atomically_and_preserves_search_isolation", func(t *testing.T) {
		testRagUnicodeTokenizerV47Migration(t, SQLDialectSQLite, newSQLiteRagProjectionTestDB(t))
	})
	t.Run("token_insert_failure_rolls_back_and_retries", func(t *testing.T) {
		testRagUnicodeTokenizerV47InsertFailure(t, SQLDialectSQLite, newSQLiteRagProjectionTestDB(t))
	})
	t.Run("in_transaction_cancellation_rolls_back_and_releases_writer", func(t *testing.T) {
		testSQLiteRagUnicodeTokenizerV47Cancellation(t)
	})
	t.Run("8193_new_tokens_rolls_back_then_8192_retries", func(t *testing.T) {
		testRagUnicodeTokenizerV47TokenLimit(t, SQLDialectSQLite, newSQLiteRagProjectionTestDB(t))
	})
	t.Run("concurrent_openers_rebuild_once", func(t *testing.T) {
		testSQLiteRagUnicodeTokenizerV47ConcurrentOpen(t)
	})
}

func TestPostgresRagUnicodeTokenizerV47Migration(t *testing.T) {
	t.Run("rebuilds_legacy_projection_atomically_and_preserves_search_isolation", func(t *testing.T) {
		testRagUnicodeTokenizerV47Migration(t, SQLDialectPostgres, newPostgresTestDB(t))
	})
	t.Run("token_insert_failure_rolls_back_and_retries", func(t *testing.T) {
		testRagUnicodeTokenizerV47InsertFailure(t, SQLDialectPostgres, newPostgresTestDB(t))
	})
	t.Run("in_transaction_cancellation_rolls_back", func(t *testing.T) {
		testPostgresRagUnicodeTokenizerV47Cancellation(t)
	})
	t.Run("8193_new_tokens_rolls_back_then_8192_retries", func(t *testing.T) {
		testRagUnicodeTokenizerV47TokenLimit(t, SQLDialectPostgres, newPostgresTestDB(t))
	})
	t.Run("concurrent_openers_rebuild_once_and_holds_projection_lock", func(t *testing.T) {
		testPostgresRagUnicodeTokenizerV47ConcurrentOpen(t)
	})
}

func testRagUnicodeTokenizerV47Migration(t *testing.T, dialect SQLDialect, db *sql.DB) {
	t.Helper()
	ctx := context.Background()
	fixture := seedV46RagUnicodeTokenizerFixture(t, ctx, db, dialect)
	if _, err := OpenSQLSessionStore(ctx, db, dialect); err != nil {
		t.Fatalf("open v46 fixture through v47: %v", err)
	}
	assertV47RagUnicodeState(t, ctx, db, dialect, fixture, true)

	installV47ProjectionAudit(t, ctx, db, dialect)
	if _, err := OpenSQLSessionStore(ctx, db, dialect); err != nil {
		t.Fatalf("repeat v47 open: %v", err)
	}
	assertV47ProjectionAuditCount(t, ctx, db, dialect, 0)
}

func testRagUnicodeTokenizerV47InsertFailure(t *testing.T, dialect SQLDialect, db *sql.DB) {
	t.Helper()
	ctx := context.Background()
	fixture := seedV46RagUnicodeTokenizerFixture(t, ctx, db, dialect)
	installV47TokenFailureTrigger(t, ctx, db, dialect)
	if _, err := OpenSQLSessionStore(ctx, db, dialect); err == nil {
		t.Fatal("v47 migration accepted injected token insert failure")
	}
	assertV47RagUnicodeState(t, ctx, db, dialect, fixture, false)
	dropV47TokenFailureTrigger(t, ctx, db, dialect)
	if _, err := OpenSQLSessionStore(ctx, db, dialect); err != nil {
		t.Fatalf("retry v47 migration after token trigger removal: %v", err)
	}
	assertV47RagUnicodeState(t, ctx, db, dialect, fixture, true)
}

var v47SQLiteCancellationDriverSequence atomic.Int64

func testSQLiteRagUnicodeTokenizerV47Cancellation(t *testing.T) {
	t.Helper()
	entered := make(chan struct{})
	release := make(chan struct{})
	driverName := "v47_unicode_cancel_" + strconv.FormatInt(v47SQLiteCancellationDriverSequence.Add(1), 10)
	driverInstance := &moderncsqlite.Driver{}
	driverInstance.MustRegisterScalarFunction("v47_unicode_cancel", 0, func(*moderncsqlite.FunctionContext, []driver.Value) (driver.Value, error) {
		close(entered)
		<-release
		return nil, context.Canceled
	})
	sql.Register(driverName, driverInstance)
	db, err := sql.Open(driverName, t.TempDir()+"/v47-cancel.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()
	fixture := seedV46RagUnicodeTokenizerFixture(t, ctx, db, SQLDialectSQLite)
	if _, err := db.ExecContext(ctx, `CREATE TRIGGER v47_cancel_token_insert BEFORE INSERT ON rag_document_tokens
		WHEN NEW.token = '保留凭证' BEGIN SELECT v47_unicode_cancel(); END`); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	result := make(chan error, 1)
	go func() {
		_, err := OpenSQLSessionStore(canceled, db, SQLDialectSQLite)
		result <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		cancel()
		close(release)
		t.Fatal("v47 cancellation trigger was not reached inside the rebuild transaction")
	}
	cancel()
	close(release)
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("v47 migration accepted in-transaction cancellation")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("v47 cancellation did not return")
	}
	assertV47RagUnicodeState(t, ctx, db, SQLDialectSQLite, fixture, false)
	if _, err := db.ExecContext(ctx, "DROP TRIGGER v47_cancel_token_insert"); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSQLSessionStore(ctx, db, SQLDialectSQLite); err != nil {
		t.Fatalf("retry after SQLite in-transaction cancellation retained a lock: %v", err)
	}
	assertV47RagUnicodeState(t, ctx, db, SQLDialectSQLite, fixture, true)
}

func testPostgresRagUnicodeTokenizerV47Cancellation(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	db := newPostgresTestDB(t)
	fixture := seedV46RagUnicodeTokenizerFixture(t, ctx, db, SQLDialectPostgres)
	gate, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer gate.Close()
	var gatePID int
	if err := gate.QueryRowContext(ctx, "SELECT pg_backend_pid()").Scan(&gatePID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `CREATE FUNCTION v47_cancel_token_insert() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN PERFORM pg_advisory_lock(hashtext(current_schema()), 47); RETURN NEW; END; $$;
		CREATE TRIGGER v47_cancel_token_insert BEFORE INSERT ON rag_document_tokens
		FOR EACH ROW EXECUTE FUNCTION v47_cancel_token_insert()`); err != nil {
		t.Fatal(err)
	}
	// The trigger and gate derive a two-int advisory key from this test schema.
	// A pg_locks waiter blocked by gatePID proves that the migration reached a
	// token INSERT in its transaction before cancellation.
	if _, err := gate.ExecContext(ctx, "SELECT pg_advisory_lock(hashtext(current_schema()), 47)"); err != nil {
		t.Fatal(err)
	}
	held := true
	releaseGate := func() error {
		if !held {
			return nil
		}
		held = false
		_, err := gate.ExecContext(context.Background(), "SELECT pg_advisory_unlock(hashtext(current_schema()), 47)")
		return err
	}
	defer func() { _ = releaseGate() }()
	canceled, cancel := context.WithCancel(ctx)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := OpenSQLSessionStore(canceled, db, SQLDialectPostgres)
		result <- err
	}()
	awaitResult := func(label string) error {
		select {
		case err := <-result:
			return err
		case <-time.After(5 * time.Second):
			t.Fatalf("postgres v47 cancellation did not return %s", label)
			return nil
		}
	}
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-deadline.C:
			cancel()
			if err := releaseGate(); err != nil {
				t.Fatal(err)
			}
			_ = awaitResult("after cancellation barrier timeout")
			t.Fatal("postgres v47 cancellation trigger did not block inside token rebuild")
		case <-ticker.C:
			var waiting int
			if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM pg_locks AS waiting
				WHERE waiting.locktype = 'advisory' AND NOT waiting.granted
					AND $1 = ANY(pg_blocking_pids(waiting.pid))`, gatePID).Scan(&waiting); err != nil {
				t.Fatal(err)
			}
			if waiting > 0 {
				goto blocked
			}
		}
	}

blocked:
	cancel()
	if err := awaitResult("after in-transaction cancellation"); err == nil {
		t.Fatal("postgres v47 migration accepted in-transaction cancellation")
	}
	if err := releaseGate(); err != nil {
		t.Fatal(err)
	}
	assertV47RagUnicodeState(t, ctx, db, SQLDialectPostgres, fixture, false)
	if _, err := db.ExecContext(ctx, `DROP TRIGGER v47_cancel_token_insert ON rag_document_tokens;
		DROP FUNCTION v47_cancel_token_insert()`); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSQLSessionStore(ctx, db, SQLDialectPostgres); err != nil {
		t.Fatalf("retry after PostgreSQL in-transaction cancellation: %v", err)
	}
	assertV47RagUnicodeState(t, ctx, db, SQLDialectPostgres, fixture, true)
}

func testRagUnicodeTokenizerV47TokenLimit(t *testing.T, dialect SQLDialect, db *sql.DB) {
	t.Helper()
	ctx := context.Background()
	if _, err := OpenSQLSessionStore(ctx, db, dialect); err != nil {
		t.Fatal(err)
	}
	limit8193 := v47LegacyCJKDelimiterTokenLimit(rag.MaxDocumentTokens + 1)
	limitScope := seedV46RagUnicodeTokenizerLimitFixture(t, ctx, db, dialect, limit8193)
	if _, err := OpenSQLSessionStore(ctx, db, dialect); err == nil || !strings.Contains(err.Error(), "unique tokens") {
		t.Fatalf("v47 accepted %d-token rebuilt document: %v", rag.MaxDocumentTokens+1, err)
	}
	assertV47TokenLimitRollback(t, ctx, db, dialect, limitScope, limit8193)

	limit8192 := v47LegacyCJKDelimiterTokenLimit(rag.MaxDocumentTokens)
	replaceV46RagUnicodeTokenizerLimitFixture(t, ctx, db, dialect, limitScope, limit8192)
	if _, err := OpenSQLSessionStore(ctx, db, dialect); err != nil {
		t.Fatalf("v47 retry at %d tokens: %v", rag.MaxDocumentTokens, err)
	}
	assertRagSchemaMarker(t, ctx, db, dialect, ragTokenizerSchemaVersionV47)
	var tokens int
	query := sqlQuery{`SELECT COUNT(*) FROM rag_document_tokens WHERE scope = ? AND document_id = ?`}.bind(dialect)
	if err := db.QueryRowContext(ctx, query, limitScope.String(), "v47-limit").Scan(&tokens); err != nil || tokens != rag.MaxDocumentTokens {
		t.Fatalf("v47 rebuilt token count=%d want=%d err=%v", tokens, rag.MaxDocumentTokens, err)
	}
}

func seedV46RagUnicodeTokenizerFixture(t *testing.T, ctx context.Context, db *sql.DB, dialect SQLDialect) v47RagFixture {
	t.Helper()
	if _, err := OpenSQLSessionStore(ctx, db, dialect); err != nil {
		t.Fatal(err)
	}
	_, product, tenant, user := testScopes()
	sibling, err := product.Child(core.ScopeRef{Kind: core.ScopeTenant, ID: "other"})
	if err != nil {
		t.Fatal(err)
	}
	fixture := v47RagFixture{queryScope: user, targetScope: tenant, siblingScope: sibling}
	clearV47RagFixture(t, ctx, db, dialect)
	insertDocument := sqlQuery{`INSERT INTO rag_documents (scope, id, source, content, tags, tags_json)
		VALUES (?, ?, ?, ?, ?, ?)`}.bind(dialect)
	for _, row := range []struct {
		scope core.ScopePath
		id    string
		tags  string
	}{
		{tenant, v47TargetID, `["finance"]`},
		{tenant, v47HeldOutID, `["heldout"]`},
		{sibling, v47SiblingID, `["finance"]`},
	} {
		if _, err := db.ExecContext(ctx, insertDocument, row.scope.String(), row.id, row.id+".md", v47LegacyToken, strings.Trim(row.tags, "[]\""), row.tags); err != nil {
			t.Fatal(err)
		}
	}
	insertToken := sqlQuery{`INSERT INTO rag_document_tokens (scope, document_id, token) VALUES (?, ?, ?)`}.bind(dialect)
	insertTag := sqlQuery{`INSERT INTO rag_document_tags (scope, document_id, tag) VALUES (?, ?, ?)`}.bind(dialect)
	for _, row := range []struct {
		scope   core.ScopePath
		id, tag string
	}{
		{tenant, v47TargetID, "finance"}, {tenant, v47HeldOutID, "heldout"}, {sibling, v47SiblingID, "finance"},
	} {
		if _, err := db.ExecContext(ctx, insertToken, row.scope.String(), row.id, v47LegacyToken); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(ctx, insertTag, row.scope.String(), row.id, row.tag); err != nil {
			t.Fatal(err)
		}
	}
	setRagSchemaMarker(t, ctx, db, dialect, 46)
	return fixture
}

func seedV46RagUnicodeTokenizerLimitFixture(t *testing.T, ctx context.Context, db *sql.DB, dialect SQLDialect, legacy v47LegacyTokenLimitFixture) core.ScopePath {
	t.Helper()
	clearV47RagFixture(t, ctx, db, dialect)
	_, _, tenant, _ := testScopes()
	replaceV46RagUnicodeTokenizerLimitFixture(t, ctx, db, dialect, tenant, legacy)
	setRagSchemaMarker(t, ctx, db, dialect, 46)
	return tenant
}

func replaceV46RagUnicodeTokenizerLimitFixture(t *testing.T, ctx context.Context, db *sql.DB, dialect SQLDialect, scope core.ScopePath, legacy v47LegacyTokenLimitFixture) {
	t.Helper()
	insertDocument := sqlQuery{`INSERT INTO rag_documents (scope, id, source, content, tags, tags_json)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(scope, id) DO UPDATE SET source = excluded.source, content = excluded.content, tags = excluded.tags, tags_json = excluded.tags_json`}.bind(dialect)
	if _, err := db.ExecContext(ctx, insertDocument, scope.String(), "v47-limit", "limit.md", legacy.content, "finance", `["finance"]`); err != nil {
		t.Fatal(err)
	}
	// These hard-coded v46 rows use ASCII whitespace to delimit each old token;
	// CJK delimiters remain inside each token and no current tokenizer constructs
	// this legacy projection.
	deleteTokens := sqlQuery{`DELETE FROM rag_document_tokens WHERE scope = ? AND document_id = ?`}.bind(dialect)
	if _, err := db.ExecContext(ctx, deleteTokens, scope.String(), "v47-limit"); err != nil {
		t.Fatal(err)
	}
	insertToken := sqlQuery{`INSERT INTO rag_document_tokens (scope, document_id, token) VALUES (?, ?, ?)`}.bind(dialect)
	for _, token := range legacy.tokens {
		if _, err := db.ExecContext(ctx, insertToken, scope.String(), "v47-limit", token); err != nil {
			t.Fatal(err)
		}
	}
	insertTag := sqlQuery{`INSERT INTO rag_document_tags (scope, document_id, tag) VALUES (?, ?, ?)
		ON CONFLICT(scope, document_id, tag) DO NOTHING`}.bind(dialect)
	if _, err := db.ExecContext(ctx, insertTag, scope.String(), "v47-limit", "finance"); err != nil {
		t.Fatal(err)
	}
}

func clearV47RagFixture(t *testing.T, ctx context.Context, db *sql.DB, dialect SQLDialect) {
	t.Helper()
	for _, table := range []string{"rag_document_tokens", "rag_document_tags", "rag_documents"} {
		if _, err := db.ExecContext(ctx, "DELETE FROM "+table); err != nil {
			t.Fatalf("clear %s: %v", table, err)
		}
	}
}

func setRagSchemaMarker(t *testing.T, ctx context.Context, db *sql.DB, dialect SQLDialect, version int) {
	t.Helper()
	if _, err := db.ExecContext(ctx, sqlUpdateMetaRow.bind(dialect), strconv.Itoa(version)); err != nil {
		t.Fatal(err)
	}
}

func assertRagSchemaMarker(t *testing.T, ctx context.Context, db *sql.DB, dialect SQLDialect, want int) {
	t.Helper()
	var raw string
	if err := db.QueryRowContext(ctx, sqlSelectMetaRow.bind(dialect)).Scan(&raw); err != nil || raw != strconv.Itoa(want) {
		t.Fatalf("schema marker=%q want=%d err=%v", raw, want, err)
	}
}

func assertV47RagUnicodeState(t *testing.T, ctx context.Context, db *sql.DB, dialect SQLDialect, fixture v47RagFixture, migrated bool) {
	t.Helper()
	assertV47CanonicalDocuments(t, ctx, db, dialect, fixture)
	if !migrated {
		assertRagSchemaMarker(t, ctx, db, dialect, 46)
		for _, row := range []struct {
			scope core.ScopePath
			id    string
			tag   string
		}{{fixture.targetScope, v47TargetID, "finance"}, {fixture.targetScope, v47HeldOutID, "heldout"}, {fixture.siblingScope, v47SiblingID, "finance"}} {
			assertV47ProjectionValues(t, ctx, db, dialect, "rag_document_tokens", "token", row.scope.String(), row.id, []string{v47LegacyToken})
			assertV47ProjectionValues(t, ctx, db, dialect, "rag_document_tags", "tag", row.scope.String(), row.id, []string{row.tag})
		}
		return
	}
	assertRagSchemaMarker(t, ctx, db, dialect, ragTokenizerSchemaVersionV47)
	for _, row := range []struct {
		scope core.ScopePath
		id    string
	}{
		{fixture.targetScope, v47TargetID}, {fixture.targetScope, v47HeldOutID}, {fixture.siblingScope, v47SiblingID},
	} {
		assertV47ProjectionValues(t, ctx, db, dialect, "rag_document_tokens", "token", row.scope.String(), row.id, []string{"保留凭证", "用户"})
	}
	index, err := NewSQLRagIndex(db, dialect)
	if err != nil {
		t.Fatal(err)
	}
	hits, err := index.Search(ctx, fixture.queryScope, core.RagQuery{Query: "保留凭证", Tags: []string{"finance"}, TopK: 5})
	if err != nil || len(hits) != 1 || hits[0].ID != v47TargetID {
		t.Fatalf("v47 scope/tag search=%#v err=%v", hits, err)
	}
}

func assertV47CanonicalDocuments(t *testing.T, ctx context.Context, db *sql.DB, dialect SQLDialect, fixture v47RagFixture) {
	t.Helper()
	want := []v47CanonicalDocument{
		{fixture.targetScope.String(), v47TargetID, v47TargetID + ".md", v47LegacyToken, "finance", `["finance"]`},
		{fixture.targetScope.String(), v47HeldOutID, v47HeldOutID + ".md", v47LegacyToken, "heldout", `["heldout"]`},
		{fixture.siblingScope.String(), v47SiblingID, v47SiblingID + ".md", v47LegacyToken, "finance", `["finance"]`},
	}
	sort.Slice(want, func(i, j int) bool {
		if want[i].scope != want[j].scope {
			return want[i].scope < want[j].scope
		}
		return want[i].id < want[j].id
	})
	query := sqlQuery{`SELECT scope, id, source, content, tags, tags_json FROM rag_documents ORDER BY scope, id`}.bind(dialect)
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []v47CanonicalDocument
	for rows.Next() {
		var row v47CanonicalDocument
		if err := rows.Scan(&row.scope, &row.id, &row.source, &row.content, &row.tags, &row.tagsJSON); err != nil {
			t.Fatal(err)
		}
		got = append(got, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("canonical documents=%#v want=%#v", got, want)
	}
}

func assertV47ProjectionValues(t *testing.T, ctx context.Context, db *sql.DB, dialect SQLDialect, table, column, scope, id string, want []string) {
	t.Helper()
	query := sqlQuery{"SELECT " + column + " FROM " + table + " WHERE scope = ? AND document_id = ? ORDER BY " + column}.bind(dialect)
	rows, err := db.QueryContext(ctx, query, scope, id)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			t.Fatal(err)
		}
		got = append(got, value)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	want = append([]string(nil), want...)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s %s/%s=%#v want=%#v", table, scope, id, got, want)
	}
}

func installV47TokenFailureTrigger(t *testing.T, ctx context.Context, db *sql.DB, dialect SQLDialect) {
	t.Helper()
	if dialect == SQLDialectSQLite {
		if _, err := db.ExecContext(ctx, `CREATE TRIGGER v47_fail_token_insert BEFORE INSERT ON rag_document_tokens
			WHEN NEW.token = '保留凭证' BEGIN SELECT RAISE(ABORT, 'v47 token insert failure'); END`); err != nil {
			t.Fatal(err)
		}
		return
	}
	if _, err := db.ExecContext(ctx, `CREATE FUNCTION v47_fail_token_insert() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN IF NEW.token = '保留凭证' THEN RAISE EXCEPTION 'v47 token insert failure'; END IF; RETURN NEW; END; $$;
		CREATE TRIGGER v47_fail_token_insert BEFORE INSERT ON rag_document_tokens
		FOR EACH ROW EXECUTE FUNCTION v47_fail_token_insert()`); err != nil {
		t.Fatal(err)
	}
}

func dropV47TokenFailureTrigger(t *testing.T, ctx context.Context, db *sql.DB, dialect SQLDialect) {
	t.Helper()
	if dialect == SQLDialectSQLite {
		_, err := db.ExecContext(ctx, "DROP TRIGGER v47_fail_token_insert")
		if err != nil {
			t.Fatal(err)
		}
		return
	}
	if _, err := db.ExecContext(ctx, `DROP TRIGGER v47_fail_token_insert ON rag_document_tokens; DROP FUNCTION v47_fail_token_insert()`); err != nil {
		t.Fatal(err)
	}
}

func assertV47TokenLimitRollback(t *testing.T, ctx context.Context, db *sql.DB, dialect SQLDialect, scope core.ScopePath, legacy v47LegacyTokenLimitFixture) {
	t.Helper()
	assertRagSchemaMarker(t, ctx, db, dialect, 46)
	assertV47ProjectionValues(t, ctx, db, dialect, "rag_document_tokens", "token", scope.String(), "v47-limit", legacy.tokens)
	assertV47ProjectionValues(t, ctx, db, dialect, "rag_document_tags", "tag", scope.String(), "v47-limit", []string{"finance"})
	var got struct{ scope, id, source, content, tags, tagsJSON string }
	query := sqlQuery{`SELECT scope, id, source, content, tags, tags_json FROM rag_documents WHERE scope = ? AND id = ?`}.bind(dialect)
	if err := db.QueryRowContext(ctx, query, scope.String(), "v47-limit").Scan(&got.scope, &got.id, &got.source, &got.content, &got.tags, &got.tagsJSON); err != nil ||
		got.scope != scope.String() || got.id != "v47-limit" || got.source != "limit.md" || got.content != legacy.content || got.tags != "finance" || got.tagsJSON != `["finance"]` {
		t.Fatalf("limit canonical row=%#v want scope=%q id=%q source=limit.md content-len=%d tags=finance tags_json=[finance] err=%v", got, scope.String(), "v47-limit", len(legacy.content), err)
	}
}

func v47LegacyCJKDelimiterTokenLimit(count int) v47LegacyTokenLimitFixture {
	const fragmentsPerLegacyToken = 128
	tokens := make([]string, 0, (count+fragmentsPerLegacyToken-1)/fragmentsPerLegacyToken)
	for first := 0; first < count; first += fragmentsPerLegacyToken {
		last := min(first+fragmentsPerLegacyToken, count)
		fragments := make([]string, 0, last-first)
		for index := first; index < last; index++ {
			fragments = append(fragments, string(rune(0x4e00+index)))
		}
		tokens = append(tokens, strings.Join(fragments, "，"))
	}
	return v47LegacyTokenLimitFixture{content: strings.Join(tokens, " "), tokens: tokens}
}

func installV47ProjectionAudit(t *testing.T, ctx context.Context, db *sql.DB, dialect SQLDialect) {
	t.Helper()
	if _, err := db.ExecContext(ctx, `CREATE TABLE v47_projection_audit (note TEXT NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	if dialect == SQLDialectSQLite {
		_, err := db.ExecContext(ctx, `CREATE TRIGGER v47_projection_audit AFTER INSERT ON rag_document_tokens
			BEGIN INSERT INTO v47_projection_audit (note) VALUES (NEW.document_id || ':' || NEW.token); END`)
		if err != nil {
			t.Fatal(err)
		}
		return
	}
	_, err := db.ExecContext(ctx, `CREATE FUNCTION v47_projection_audit() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN INSERT INTO v47_projection_audit (note) VALUES (NEW.document_id || ':' || NEW.token); RETURN NEW; END; $$;
		CREATE TRIGGER v47_projection_audit AFTER INSERT ON rag_document_tokens
		FOR EACH ROW EXECUTE FUNCTION v47_projection_audit()`)
	if err != nil {
		t.Fatal(err)
	}
}

func assertV47ProjectionAuditCount(t *testing.T, ctx context.Context, db *sql.DB, dialect SQLDialect, want int) {
	t.Helper()
	var got int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM v47_projection_audit").Scan(&got); err != nil || got != want {
		t.Fatalf("v47 same-version rebuild audit=%d want=%d err=%v", got, want, err)
	}
}

func testSQLiteRagUnicodeTokenizerV47ConcurrentOpen(t *testing.T) {
	path := t.TempDir() + "/v47-concurrent.db"
	setup, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	seedV46RagUnicodeTokenizerFixture(t, ctx, setup, SQLDialectSQLite)
	installV47ProjectionAudit(t, ctx, setup, SQLDialectSQLite)
	if err := setup.Close(); err != nil {
		t.Fatal(err)
	}
	first, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	second, err := sql.Open("sqlite", path)
	if err != nil {
		_ = first.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = first.Close() })
	t.Cleanup(func() { _ = second.Close() })
	assertV47ConcurrentOpen(t, ctx, SQLDialectSQLite, first, second, 6)
}

func testPostgresRagUnicodeTokenizerV47ConcurrentOpen(t *testing.T) {
	ctx := context.Background()
	first := newPostgresTestDB(t)
	seedV46RagUnicodeTokenizerFixture(t, ctx, first, SQLDialectPostgres)
	installV47ProjectionAudit(t, ctx, first, SQLDialectPostgres)
	installPostgresRagProjectionLockAssertion(t, first)
	var schema string
	if err := first.QueryRowContext(ctx, `SELECT current_schema()`).Scan(&schema); err != nil {
		t.Fatal(err)
	}
	second := newPostgresGraphCheckpointHistoryHandle(t, schema)
	second.SetMaxOpenConns(1)
	second.SetMaxIdleConns(1)
	assertV47ConcurrentOpen(t, ctx, SQLDialectPostgres, first, second, 6)
}

func assertV47ConcurrentOpen(t *testing.T, ctx context.Context, dialect SQLDialect, first, second *sql.DB, expectedInserts int) {
	t.Helper()
	start := make(chan struct{})
	errs := make(chan error, 2)
	var wait sync.WaitGroup
	for _, db := range []*sql.DB{first, second} {
		wait.Add(1)
		go func(db *sql.DB) {
			defer wait.Done()
			<-start
			_, err := OpenSQLSessionStore(ctx, db, dialect)
			errs <- err
		}(db)
	}
	close(start)
	wait.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent v47 open: %v", err)
		}
	}
	assertRagSchemaMarker(t, ctx, first, dialect, ragTokenizerSchemaVersionV47)
	assertV47ProjectionAuditCount(t, ctx, first, dialect, expectedInserts)
}
