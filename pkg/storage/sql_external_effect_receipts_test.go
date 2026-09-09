package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/whhhh1500/auto-agent/pkg/app/effectreceipt"
	"github.com/whhhh1500/auto-agent/pkg/core"
)

func TestSQLExternalEffectReceiptStoreStateCASAndCanonicalReplay(t *testing.T) {
	ctx := context.Background()
	session := newTestSQLStore(t)
	store, err := NewSQLExternalEffectReceiptStore(session.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	intent := sqlExternalEffectIntent(t, "effect-call-one", "first payload", effectreceipt.DriverRef{ID: "webhook", Version: "v1"})

	prepared, err := store.Ensure(ctx, intent)
	if err != nil || prepared.State != effectreceipt.StatePrepared || prepared.DispatchAttempts != 0 {
		t.Fatalf("ensure prepared=%+v err=%v", prepared, err)
	}
	if _, err := store.MarkAccepted(ctx, intent, sqlExternalEffectSubmission(intent)); !errors.Is(err, effectreceipt.ErrConflict) {
		t.Fatalf("accepted before dispatch error=%v, want ErrConflict", err)
	}
	if record, found, err := store.Get(ctx, intent); err != nil || !found || record.State != effectreceipt.StatePrepared {
		t.Fatalf("failed CAS changed prepared state: record=%+v found=%t err=%v", record, found, err)
	}

	dispatching, begun, err := store.BeginDispatch(ctx, intent)
	if err != nil || !begun || dispatching.State != effectreceipt.StateDispatching || dispatching.DispatchAttempts != 1 {
		t.Fatalf("begin record=%+v begun=%t err=%v", dispatching, begun, err)
	}
	second, begun, err := store.BeginDispatch(ctx, intent)
	if err != nil || begun || second.State != effectreceipt.StateDispatching || second.DispatchAttempts != 1 {
		t.Fatalf("second begin record=%+v begun=%t err=%v", second, begun, err)
	}
	accepted, err := store.MarkAccepted(ctx, intent, sqlExternalEffectSubmission(intent))
	if err != nil || accepted.State != effectreceipt.StateAccepted || accepted.EvidenceDigest != "" {
		t.Fatalf("accepted record=%+v err=%v", accepted, err)
	}
	acceptedReplay, err := store.MarkAccepted(ctx, intent, sqlExternalEffectSubmission(intent))
	if err != nil || acceptedReplay != accepted {
		t.Fatalf("accepted response-lost replay=%+v accepted=%+v err=%v", acceptedReplay, accepted, err)
	}
	changedSubmission := sqlExternalEffectSubmission(intent)
	changedSubmission.ReceiptDigest = effectreceipt.SHA256Digest([]byte("other acknowledgement"))
	if _, err := store.MarkAccepted(ctx, intent, changedSubmission); !errors.Is(err, effectreceipt.ErrConflict) {
		t.Fatalf("accepted receipt replacement error=%v, want ErrConflict", err)
	}
	mismatch := sqlExternalEffectObservation(intent, effectreceipt.ObservationConfirmed)
	mismatch.OperationKey = effectreceipt.SHA256Digest([]byte("wrong operation"))
	if _, err := store.MarkConfirmed(ctx, intent, mismatch); !errors.Is(err, effectreceipt.ErrReadBackMismatch) {
		t.Fatalf("mismatched read-back error=%v, want ErrReadBackMismatch", err)
	}
	confirmed, err := store.MarkConfirmed(ctx, intent, sqlExternalEffectObservation(intent, effectreceipt.ObservationConfirmed))
	if err != nil || confirmed.State != effectreceipt.StateConfirmed || confirmed.EvidenceDigest == "" {
		t.Fatalf("confirmed record=%+v err=%v", confirmed, err)
	}
	confirmedReplay, err := store.MarkConfirmed(ctx, intent, sqlExternalEffectObservation(intent, effectreceipt.ObservationConfirmed))
	if err != nil || confirmedReplay != confirmed {
		t.Fatalf("confirmed response-lost replay=%+v confirmed=%+v err=%v", confirmedReplay, confirmed, err)
	}
	changedEvidence := sqlExternalEffectObservation(intent, effectreceipt.ObservationConfirmed)
	changedEvidence.EvidenceDigest = effectreceipt.SHA256Digest([]byte("other read-back proof"))
	if _, err := store.MarkConfirmed(ctx, intent, changedEvidence); !errors.Is(err, effectreceipt.ErrConflict) {
		t.Fatalf("confirmed evidence replacement error=%v, want ErrConflict", err)
	}
	if _, err := store.MarkUnknown(ctx, intent, effectreceipt.CodeDriverFailure); !errors.Is(err, effectreceipt.ErrConflict) {
		t.Fatalf("terminal record accepted unknown transition: %v", err)
	}
	replay, err := store.Ensure(ctx, intent)
	if err != nil || replay != confirmed {
		t.Fatalf("canonical replay=%+v confirmed=%+v err=%v", replay, confirmed, err)
	}

	changed := sqlExternalEffectIntent(t, "effect-call-one", "changed payload", intent.Driver)
	if _, err := store.Ensure(ctx, changed); !errors.Is(err, effectreceipt.ErrConflict) {
		t.Fatalf("logical identity conflict error=%v, want ErrConflict", err)
	}
	if _, found, err := store.Get(ctx, changed); err != nil || found {
		t.Fatalf("conflicting exact read found=%t err=%v", found, err)
	}
}

func TestSQLExternalEffectReceiptStoreUnknownCanReachRejected(t *testing.T) {
	ctx := context.Background()
	session := newTestSQLStore(t)
	store, err := NewSQLExternalEffectReceiptStore(session.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	intent := sqlExternalEffectIntent(t, "effect-call-two", "second payload", effectreceipt.DriverRef{ID: "webhook", Version: "v1"})
	if _, err := store.Ensure(ctx, intent); err != nil {
		t.Fatal(err)
	}
	if _, begun, err := store.BeginDispatch(ctx, intent); err != nil || !begun {
		t.Fatalf("begin begun=%t err=%v", begun, err)
	}
	unknown, err := store.MarkUnknown(ctx, intent, effectreceipt.CodeReadBackFailure)
	if err != nil || unknown.State != effectreceipt.StateUnknown || unknown.ErrorCode != effectreceipt.CodeReadBackFailure {
		t.Fatalf("unknown record=%+v err=%v", unknown, err)
	}
	unknownReplay, err := store.MarkUnknown(ctx, intent, effectreceipt.CodeReadBackFailure)
	if err != nil || unknownReplay != unknown {
		t.Fatalf("unknown response-lost replay=%+v unknown=%+v err=%v", unknownReplay, unknown, err)
	}
	rejected, err := store.MarkRejected(ctx, intent, sqlExternalEffectObservation(intent, effectreceipt.ObservationRejected))
	if err != nil || rejected.State != effectreceipt.StateRejected || rejected.EvidenceDigest == "" || rejected.ErrorCode != "" {
		t.Fatalf("rejected record=%+v err=%v", rejected, err)
	}
	rejectedReplay, err := store.MarkRejected(ctx, intent, sqlExternalEffectObservation(intent, effectreceipt.ObservationRejected))
	if err != nil || rejectedReplay != rejected {
		t.Fatalf("rejected response-lost replay=%+v rejected=%+v err=%v", rejectedReplay, rejected, err)
	}
	if _, err := store.MarkConfirmed(ctx, intent, sqlExternalEffectObservation(intent, effectreceipt.ObservationConfirmed)); !errors.Is(err, effectreceipt.ErrConflict) {
		t.Fatalf("rejected record accepted confirmed transition: %v", err)
	}
}

func TestSQLExternalEffectReceiptReadersAreBoundedAndScoped(t *testing.T) {
	ctx := context.Background()
	session := newTestSQLStore(t)
	store, err := NewSQLExternalEffectReceiptStore(session.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	driverA := effectreceipt.DriverRef{ID: "driver-a", Version: "v1"}
	driverB := effectreceipt.DriverRef{ID: "driver-b", Version: "v1"}
	first := sqlExternalEffectIntent(t, "effect-call-a", "a", driverA)
	second := sqlExternalEffectIntent(t, "effect-call-b", "b", driverA)
	third := sqlExternalEffectIntent(t, "effect-call-c", "c", driverB)
	for _, intent := range []effectreceipt.Intent{first, second, third} {
		if _, err := store.Ensure(ctx, intent); err != nil {
			t.Fatal(err)
		}
		if _, begun, err := store.BeginDispatch(ctx, intent); err != nil || !begun {
			t.Fatalf("begin %s begun=%t err=%v", intent.Invocation.CallID, begun, err)
		}
	}
	if _, err := store.MarkAccepted(ctx, second, sqlExternalEffectSubmission(second)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkUnknown(ctx, third, effectreceipt.CodeDriverFailure); err != nil {
		t.Fatal(err)
	}

	page, cursor, err := store.ListUnresolved(ctx, effectreceipt.RecoveryQuery{Driver: driverA}, effectreceipt.RecoveryCursor{}, 1)
	if err != nil || len(page) != 1 || page[0].Intent.Driver != driverA || cursor.UpdatedAt.IsZero() {
		t.Fatalf("first unresolved page=%+v cursor=%+v err=%v", page, cursor, err)
	}
	next, _, err := store.ListUnresolved(ctx, effectreceipt.RecoveryQuery{Driver: driverA}, cursor, 2)
	if err != nil || len(next) != 1 || next[0].Intent.Driver != driverA || next[0].Intent.Invocation.CallID == page[0].Intent.Invocation.CallID {
		t.Fatalf("second unresolved page=%+v err=%v", next, err)
	}
	if _, _, err := store.ListUnresolved(ctx, effectreceipt.RecoveryQuery{Driver: driverA}, effectreceipt.RecoveryCursor{}, effectreceipt.MaxRecoveryPageSize+1); err == nil {
		t.Fatal("oversized unresolved page was accepted")
	}
	if _, _, err := store.ListUnresolved(ctx, effectreceipt.RecoveryQuery{}, effectreceipt.RecoveryCursor{}, 1); !errors.Is(err, effectreceipt.ErrInvalidIntent) {
		t.Fatalf("unbounded driver query error=%v", err)
	}

	scope := effectreceipt.RunScope{TenantID: first.Invocation.TenantID, SubjectID: first.Invocation.SubjectID, SessionID: first.Invocation.SessionID, RunID: first.Invocation.RunID}
	runPage, runCursor, err := store.ListRun(ctx, scope, effectreceipt.RunCursor{}, 2)
	if err != nil || len(runPage) != 2 || runCursor.CallID == "" {
		t.Fatalf("run first page=%+v cursor=%+v err=%v", runPage, runCursor, err)
	}
	runNext, _, err := store.ListRun(ctx, scope, runCursor, 2)
	if err != nil || len(runNext) != 1 {
		t.Fatalf("run second page=%+v err=%v", runNext, err)
	}
	wrongScope := scope
	wrongScope.SubjectID = "other-subject"
	if rows, _, err := store.ListRun(ctx, wrongScope, effectreceipt.RunCursor{}, 2); err != nil || len(rows) != 0 {
		t.Fatalf("cross-subject run reader rows=%+v err=%v", rows, err)
	}
}

func TestSQLExternalEffectReceiptStoreConcurrentBeginHasOneWinner(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir() + "/external-effect-concurrent.db"
	firstDB, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = firstDB.Close() })
	if _, err := OpenSQLSessionStore(ctx, firstDB, SQLDialectSQLite); err != nil {
		t.Fatal(err)
	}
	secondDB, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = secondDB.Close() })
	if _, err := OpenSQLSessionStore(ctx, secondDB, SQLDialectSQLite); err != nil {
		t.Fatal(err)
	}
	first, err := NewSQLExternalEffectReceiptStore(firstDB, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewSQLExternalEffectReceiptStore(secondDB, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	intent := sqlExternalEffectIntent(t, "effect-call-concurrent", "payload", effectreceipt.DriverRef{ID: "concurrent", Version: "v1"})
	if _, err := first.Ensure(ctx, intent); err != nil {
		t.Fatal(err)
	}

	const workers = 16
	start := make(chan struct{})
	winners := make(chan bool, workers)
	errs := make(chan error, workers)
	stores := []*SQLExternalEffectReceiptStore{first, second}
	var wg sync.WaitGroup
	for index := 0; index < workers; index++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			<-start
			_, begun, err := stores[index%len(stores)].BeginDispatch(ctx, intent)
			if err != nil {
				errs <- err
				return
			}
			winners <- begun
		}(index)
	}
	close(start)
	wg.Wait()
	close(winners)
	close(errs)
	for err := range errs {
		t.Errorf("concurrent begin: %v", err)
	}
	var count int
	for winner := range winners {
		if winner {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("dispatch transition winners=%d, want 1", count)
	}
	record, found, err := first.Get(ctx, intent)
	if err != nil || !found || record.State != effectreceipt.StateDispatching || record.DispatchAttempts != 1 {
		t.Fatalf("concurrent record=%+v found=%t err=%v", record, found, err)
	}
}

func TestSQLSchemaV50ExternalEffectReceiptMigrationAndContentFreeColumns(t *testing.T) {
	ctx := context.Background()
	session := newTestSQLStore(t)
	for _, statement := range []string{
		"DROP INDEX external_effect_receipts_run",
		"DROP INDEX external_effect_receipts_unresolved",
		"DROP TABLE external_effect_receipts",
		"UPDATE store_meta SET value = '49' WHERE key = 'schema_version'",
	} {
		if _, err := session.db.ExecContext(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := OpenSQLSessionStore(ctx, session.db, SQLDialectSQLite); err != nil {
		t.Fatal(err)
	}
	var version string
	if err := session.db.QueryRowContext(ctx, "SELECT value FROM store_meta WHERE key = 'schema_version'").Scan(&version); err != nil || version != "50" {
		t.Fatalf("schema version=%q err=%v", version, err)
	}
	rows, err := session.db.QueryContext(ctx, "PRAGMA table_info(external_effect_receipts)")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	columns := map[string]bool{}
	for rows.Next() {
		var cid int
		var name, kind string
		var notNull, primaryKey int
		var defaultValue any
		if err := rows.Scan(&cid, &name, &kind, &notNull, &defaultValue, &primaryKey); err != nil {
			t.Fatal(err)
		}
		columns[name] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	for _, column := range []string{"target_digest", "payload_digest", "receipt_digest", "evidence_digest", "intent_digest", "operation_key"} {
		if !columns[column] {
			t.Fatalf("missing digest column %q", column)
		}
	}
	for _, raw := range []string{"target", "payload", "provider_receipt", "provider_response", "receipt_body", "response_body"} {
		if columns[raw] {
			t.Fatalf("raw provider/request column %q is persisted", raw)
		}
	}
}

func TestPostgresExternalEffectReceiptStore(t *testing.T) {
	db := newPostgresTestDB(t)
	ctx := context.Background()
	if _, err := OpenSQLSessionStore(ctx, db, SQLDialectPostgres); err != nil {
		t.Fatal(err)
	}
	store, err := NewSQLExternalEffectReceiptStore(db, SQLDialectPostgres)
	if err != nil {
		t.Fatal(err)
	}
	intent := sqlExternalEffectIntent(t, "effect-call-pg", "postgres", effectreceipt.DriverRef{ID: "postgres-driver", Version: "v1"})
	if _, err := store.Ensure(ctx, intent); err != nil {
		t.Fatal(err)
	}
	if _, begun, err := store.BeginDispatch(ctx, intent); err != nil || !begun {
		t.Fatalf("begin begun=%t err=%v", begun, err)
	}
	if _, err := store.MarkAccepted(ctx, intent, sqlExternalEffectSubmission(intent)); err != nil {
		t.Fatal(err)
	}
	confirmed, err := store.MarkConfirmed(ctx, intent, sqlExternalEffectObservation(intent, effectreceipt.ObservationConfirmed))
	if err != nil || confirmed.State != effectreceipt.StateConfirmed {
		t.Fatalf("confirmed=%+v err=%v", confirmed, err)
	}
}

func sqlExternalEffectIntent(t *testing.T, callID, payload string, driver effectreceipt.DriverRef) effectreceipt.Intent {
	t.Helper()
	invocation, err := core.NewToolInvocation(
		core.RunInfo{RunID: "external-effect-run", SessionID: "external-effect-session", Principal: core.Principal{TenantID: "external-effect-tenant", SubjectID: "external-effect-subject"}},
		core.ToolCall{ID: callID, Name: "effects.dispatch", Args: map[string]any{"payload": payload}},
		false,
	)
	if err != nil {
		t.Fatal(err)
	}
	intent, err := effectreceipt.NewIntent(invocation, driver, []byte("https://example.test/effect-target"), []byte(payload))
	if err != nil {
		t.Fatal(err)
	}
	return intent
}

func sqlExternalEffectSubmission(intent effectreceipt.Intent) effectreceipt.Submission {
	return effectreceipt.Submission{OperationKey: intent.OperationKey, IntentDigest: intent.IntentDigest, ReceiptDigest: effectreceipt.SHA256Digest([]byte("provider accepted " + intent.IntentDigest))}
}

func sqlExternalEffectObservation(intent effectreceipt.Intent, state effectreceipt.ObservationState) effectreceipt.Observation {
	return effectreceipt.Observation{State: state, OperationKey: intent.OperationKey, IntentDigest: intent.IntentDigest,
		EvidenceDigest: effectreceipt.SHA256Digest([]byte(fmt.Sprintf("provider observation %s %s", state, intent.IntentDigest)))}
}
