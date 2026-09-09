package storage

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/whhhh1500/auto-agent/pkg/app/effectreceipt"
	"github.com/whhhh1500/auto-agent/pkg/core"
)

type nativeQueuedEffectDispatchFixture struct {
	*nativeQueuedToolEffectFixture
	intent   effectreceipt.Intent
	receipts *SQLExternalEffectReceiptStore
	admitter *NativeQueuedEffectDispatchAdmitter
}

func newSQLiteNativeQueuedEffectDispatchFixture(t *testing.T) *nativeQueuedEffectDispatchFixture {
	t.Helper()
	return newNativeQueuedEffectDispatchFixture(t, newTestSQLStore(t), "session-native-dispatch", "run-native-dispatch")
}

func newNativeQueuedEffectDispatchFixture(t *testing.T, session *SQLSessionStore, sessionID, runID string) *nativeQueuedEffectDispatchFixture {
	t.Helper()
	witness := newNativeQueuedToolEffectFixture(t, session, sessionID, runID, true)
	if record, decision, err := witness.store.BeginNativeQueuedToolEffectFenced(context.Background(), witness.fence, witness.version, witness.input); err != nil || decision != core.ToolInvocationExecuteNew || record.State != core.ToolInvocationStarted {
		t.Fatalf("prepare native witness record=%+v decision=%q err=%v", record, decision, err)
	}
	receipts, err := NewSQLExternalEffectReceiptStore(witness.store.db, witness.store.dialect)
	if err != nil {
		t.Fatal(err)
	}
	intent, err := effectreceipt.NewIntent(witness.input.Invocation, effectreceipt.DriverRef{ID: "native-driver", Version: "v1"}, []byte("native-target"), []byte("native-payload"))
	if err != nil {
		t.Fatal(err)
	}
	if record, err := receipts.Ensure(context.Background(), intent); err != nil || record.State != effectreceipt.StatePrepared {
		t.Fatalf("prepare receipt record=%+v err=%v", record, err)
	}
	admitter, err := witness.store.NewNativeQueuedEffectDispatchAdmitter(NativeQueuedEffectDispatchAdmission{
		Fence: witness.fence, ExpectedSessionVersion: witness.version, ExpectedAuthorizationEpoch: witness.input.AuthorizationEpoch,
	})
	if err != nil {
		t.Fatal(err)
	}
	return &nativeQueuedEffectDispatchFixture{nativeQueuedToolEffectFixture: witness, intent: intent, receipts: receipts, admitter: admitter}
}

func TestSQLSessionStoreNativeQueuedEffectDispatchAdmission(t *testing.T) {
	fixture := newSQLiteNativeQueuedEffectDispatchFixture(t)
	record, begun, err := fixture.admitter.BeginDispatch(context.Background(), fixture.intent)
	if err != nil || !begun || record.State != effectreceipt.StateDispatching || record.DispatchAttempts != 1 {
		t.Fatalf("first admission record=%+v begun=%t err=%v", record, begun, err)
	}
	replay, begun, err := fixture.admitter.BeginDispatch(context.Background(), fixture.intent)
	if err != nil || begun || replay != record {
		t.Fatalf("response-lost replay record=%+v begun=%t err=%v", replay, begun, err)
	}
	stored, found, err := fixture.receipts.Get(context.Background(), fixture.intent)
	if err != nil || !found || stored != record {
		t.Fatalf("durable receipt=%+v found=%t err=%v", stored, found, err)
	}
}

func TestSQLSessionStoreNativeQueuedEffectDispatchAdmissionRejectsInvalidAuthority(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(t *testing.T, fixture *nativeQueuedEffectDispatchFixture) (*NativeQueuedEffectDispatchAdmitter, effectreceipt.Intent)
		want   error
	}{
		{
			name: "authorization_epoch",
			mutate: func(t *testing.T, fixture *nativeQueuedEffectDispatchFixture) (*NativeQueuedEffectDispatchAdmitter, effectreceipt.Intent) {
				a, err := fixture.store.NewNativeQueuedEffectDispatchAdmitter(NativeQueuedEffectDispatchAdmission{Fence: fixture.fence, ExpectedSessionVersion: fixture.version, ExpectedAuthorizationEpoch: fixture.input.AuthorizationEpoch + 1})
				if err != nil {
					t.Fatal(err)
				}
				return a, fixture.intent
			},
			want: ErrAuthorizationEpochChanged,
		},
		{
			name: "lease",
			mutate: func(t *testing.T, fixture *nativeQueuedEffectDispatchFixture) (*NativeQueuedEffectDispatchAdmitter, effectreceipt.Intent) {
				if _, err := fixture.store.db.ExecContext(context.Background(), "UPDATE session_leases SET holder = ? WHERE session_id = ?", "replaced-holder", fixture.fence.SessionID); err != nil {
					t.Fatal(err)
				}
				return fixture.admitter, fixture.intent
			},
			want: ErrSessionWriteFenceLost,
		},
		{
			name: "generation_without_current_witness",
			mutate: func(t *testing.T, fixture *nativeQueuedEffectDispatchFixture) (*NativeQueuedEffectDispatchAdmitter, effectreceipt.Intent) {
				next := advanceNativeQueuedToolEffectGeneration(t, fixture.nativeQueuedToolEffectFixture)
				a, err := fixture.store.NewNativeQueuedEffectDispatchAdmitter(NativeQueuedEffectDispatchAdmission{Fence: next, ExpectedSessionVersion: fixture.version, ExpectedAuthorizationEpoch: fixture.input.AuthorizationEpoch})
				if err != nil {
					t.Fatal(err)
				}
				return a, fixture.intent
			},
			want: ErrCompletedToolResultProofInvalid,
		},
		{
			name: "session_version",
			mutate: func(t *testing.T, fixture *nativeQueuedEffectDispatchFixture) (*NativeQueuedEffectDispatchAdmitter, effectreceipt.Intent) {
				a, err := fixture.store.NewNativeQueuedEffectDispatchAdmitter(NativeQueuedEffectDispatchAdmission{Fence: fixture.fence, ExpectedSessionVersion: fixture.version + 1, ExpectedAuthorizationEpoch: fixture.input.AuthorizationEpoch})
				if err != nil {
					t.Fatal(err)
				}
				return a, fixture.intent
			},
			want: core.ErrSessionConflict,
		},
		{
			name: "witness",
			mutate: func(t *testing.T, fixture *nativeQueuedEffectDispatchFixture) (*NativeQueuedEffectDispatchAdmitter, effectreceipt.Intent) {
				if _, err := fixture.store.db.ExecContext(context.Background(), "DELETE FROM native_queued_tool_effect_witnesses WHERE session_id = ? AND run_id = ? AND call_id = ?", fixture.fence.SessionID, fixture.fence.RunID, fixture.intent.Invocation.CallID); err != nil {
					t.Fatal(err)
				}
				return fixture.admitter, fixture.intent
			},
			want: ErrCompletedToolResultProofInvalid,
		},
		{
			name: "tool_journal_state",
			mutate: func(t *testing.T, fixture *nativeQueuedEffectDispatchFixture) (*NativeQueuedEffectDispatchAdmitter, effectreceipt.Intent) {
				if _, err := fixture.store.db.ExecContext(context.Background(), "UPDATE tool_invocations SET state = 'uncertain', error_code = 'provider_error' WHERE session_id = ? AND run_id = ? AND call_id = ?", fixture.fence.SessionID, fixture.fence.RunID, fixture.intent.Invocation.CallID); err != nil {
					t.Fatal(err)
				}
				return fixture.admitter, fixture.intent
			},
			want: ErrCompletedToolResultProofInvalid,
		},
		{
			name: "intent",
			mutate: func(t *testing.T, fixture *nativeQueuedEffectDispatchFixture) (*NativeQueuedEffectDispatchAdmitter, effectreceipt.Intent) {
				changed, err := effectreceipt.NewIntent(fixture.intent.Invocation, fixture.intent.Driver, []byte("native-target"), []byte("changed-payload"))
				if err != nil {
					t.Fatal(err)
				}
				return fixture.admitter, changed
			},
			want: effectreceipt.ErrConflict,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newSQLiteNativeQueuedEffectDispatchFixture(t)
			admitter, intent := test.mutate(t, fixture)
			if record, begun, err := admitter.BeginDispatch(context.Background(), intent); begun || !errors.Is(err, test.want) {
				t.Fatalf("record=%+v begun=%t err=%v want=%v", record, begun, err, test.want)
			}
			assertNativeQueuedEffectReceiptPrepared(t, fixture)
		})
	}
}

func TestSQLSessionStoreNativeQueuedEffectDispatchAdmissionHasOneSQLiteWinner(t *testing.T) {
	fixture := newSQLiteNativeQueuedEffectDispatchFixture(t)
	const workers = 12
	start := make(chan struct{})
	results := make(chan bool, workers)
	errs := make(chan error, workers)
	var group sync.WaitGroup
	for index := 0; index < workers; index++ {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			_, begun, err := fixture.admitter.BeginDispatch(context.Background(), fixture.intent)
			if err != nil {
				errs <- err
				return
			}
			results <- begun
		}()
	}
	close(start)
	group.Wait()
	close(results)
	close(errs)
	for err := range errs {
		t.Errorf("concurrent admission: %v", err)
	}
	winners := 0
	for begun := range results {
		if begun {
			winners++
		}
	}
	if winners != 1 {
		t.Fatalf("dispatch admission winners=%d want=1", winners)
	}
	record, found, err := fixture.receipts.Get(context.Background(), fixture.intent)
	if err != nil || !found || record.State != effectreceipt.StateDispatching || record.DispatchAttempts != 1 {
		t.Fatalf("concurrent durable record=%+v found=%t err=%v", record, found, err)
	}
}

func TestPostgresNativeQueuedEffectDispatchAdmission(t *testing.T) {
	db := newPostgresTestDB(t)
	store, err := OpenSQLSessionStore(context.Background(), db, SQLDialectPostgres)
	if err != nil {
		t.Fatal(err)
	}
	fixture := newNativeQueuedEffectDispatchFixture(t, store, "session-native-dispatch-pg", "run-native-dispatch-pg")
	record, begun, err := fixture.admitter.BeginDispatch(context.Background(), fixture.intent)
	if err != nil || !begun || record.State != effectreceipt.StateDispatching {
		t.Fatalf("postgres admission record=%+v begun=%t err=%v", record, begun, err)
	}
}

func assertNativeQueuedEffectReceiptPrepared(t *testing.T, fixture *nativeQueuedEffectDispatchFixture) {
	t.Helper()
	record, found, err := fixture.receipts.Get(context.Background(), fixture.intent)
	if err != nil || !found || record.State != effectreceipt.StatePrepared || record.DispatchAttempts != 0 {
		t.Fatalf("receipt changed on rejected admission: record=%+v found=%t err=%v", record, found, err)
	}
}

func TestNativeQueuedEffectDispatchAdmissionRejectsMissingReceiptWithoutCreatingOne(t *testing.T) {
	fixture := newSQLiteNativeQueuedEffectDispatchFixture(t)
	if _, err := fixture.store.db.ExecContext(context.Background(), "DELETE FROM external_effect_receipts WHERE session_id = ? AND run_id = ? AND call_id = ?", fixture.intent.Invocation.SessionID, fixture.intent.Invocation.RunID, fixture.intent.Invocation.CallID); err != nil {
		t.Fatal(err)
	}
	if record, begun, err := fixture.admitter.BeginDispatch(context.Background(), fixture.intent); begun || !errors.Is(err, effectreceipt.ErrNotFound) {
		t.Fatalf("missing receipt record=%+v begun=%t err=%v", record, begun, err)
	}
	var count int
	if err := fixture.store.db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM external_effect_receipts WHERE session_id = ? AND run_id = ? AND call_id = ?", fixture.intent.Invocation.SessionID, fixture.intent.Invocation.RunID, fixture.intent.Invocation.CallID).Scan(&count); err != nil || count != 0 {
		t.Fatalf("missing receipt was recreated count=%d err=%v", count, err)
	}
}

func TestNativeQueuedEffectDispatchAdmissionRejectsExpiredFence(t *testing.T) {
	fixture := newSQLiteNativeQueuedEffectDispatchFixture(t)
	if _, err := fixture.store.db.ExecContext(context.Background(), "UPDATE run_queue SET lease_expires_at = ? WHERE run_id = ?", time.Now().UTC().Add(-time.Second).UnixMilli(), fixture.fence.RunID); err != nil {
		t.Fatal(err)
	}
	if _, begun, err := fixture.admitter.BeginDispatch(context.Background(), fixture.intent); begun || !errors.Is(err, ErrSessionWriteFenceLost) {
		t.Fatalf("expired fence begun=%t err=%v", begun, err)
	}
}
