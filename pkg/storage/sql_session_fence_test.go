package storage

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
)

type fencedSQLFixture struct {
	store   *SQLSessionStore
	queue   *SQLRunControlStore
	session *core.Session
	claim   QueuedRun
	fence   SessionWriteFence
}

func newSQLiteFencedFixture(t *testing.T) *fencedSQLFixture {
	t.Helper()
	return newFencedSQLFixture(t, newTestSQLStore(t), "session-fenced", "run-fenced")
}

func newFencedSQLFixture(t *testing.T, store *SQLSessionStore, sessionID, runID string) *fencedSQLFixture {
	t.Helper()
	ctx := context.Background()
	session := mustNamedSession(t, sessionID)
	scope := session.Scope()
	composition := &core.RunCompositionData{
		Profile:  core.AgentProfileSnapshot{ProfileID: session.ProfileID(), Scope: scope},
		Metadata: map[string]string{"harness.assignment.id": "fenced-append"},
	}
	compositionRevision, err := core.CompositionRevision(composition)
	if err != nil {
		t.Fatal(err)
	}
	assignmentRevision, err := core.CompositionMetadataRevision(composition.Metadata)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Append(runID, core.EvRunStart, core.RunStartData{
		CompositionRevision: compositionRevision,
		AssignmentRevision:  assignmentRevision,
		Composition:         composition,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Create(ctx, session); err != nil {
		t.Fatal(err)
	}
	queue, err := NewSQLRunControlStore(store.db, store.dialect)
	if err != nil {
		t.Fatal(err)
	}
	principal := session.Principal()
	if err := queue.EnqueueRun(ctx, QueuedRun{
		RunRecord: RunRecord{
			RunID: runID, SessionID: sessionID,
			TenantID: principal.TenantID, SubjectID: principal.SubjectID,
		},
		Message: "fenced append", MaxAttempts: 3,
	}); err != nil {
		t.Fatal(err)
	}
	claim, ok, err := queue.ClaimRun(ctx, "worker-fenced", time.Hour)
	if err != nil || !ok {
		t.Fatalf("claim run: ok=%t err=%v", ok, err)
	}
	holder := "lease-fenced-generation-1"
	acquired, err := store.AcquireSessionLease(ctx, sessionID, holder, time.Hour)
	if err != nil || !acquired {
		t.Fatalf("acquire session lease: acquired=%t err=%v", acquired, err)
	}
	return &fencedSQLFixture{
		store: store, queue: queue, session: session, claim: claim,
		fence: SessionWriteFence{
			SessionID: sessionID, RunID: runID,
			TenantID: principal.TenantID, SubjectID: principal.SubjectID, WorkerID: claim.WorkerID,
			QueueGeneration: claim.Generation, LeaseHolder: holder,
		},
	}
}

func nextFencedEvent(t *testing.T, session *core.Session, runID, text string) core.SessionEvent {
	t.Helper()
	event, err := session.Append(runID, core.EvUserMessage, core.UserMessageData{Text: text})
	if err != nil {
		t.Fatal(err)
	}
	return event
}

func rawFencedEvent(t *testing.T, seq int64, runID, text string) core.SessionEvent {
	t.Helper()
	data, err := json.Marshal(core.UserMessageData{Text: text})
	if err != nil {
		t.Fatal(err)
	}
	return core.SessionEvent{Seq: seq, Time: time.Now().UTC(), RunID: runID, Type: core.EvUserMessage, Data: data}
}

func assertFencedSessionVersion(t *testing.T, fixture *fencedSQLFixture, want int64) *core.Session {
	t.Helper()
	loaded, err := fixture.store.Load(context.Background(), fixture.fence.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Version() != want {
		t.Fatalf("durable session version=%d want=%d", loaded.Version(), want)
	}
	return loaded
}

func assertFenceLostWithoutWrite(t *testing.T, fixture *fencedSQLFixture, fence SessionWriteFence) {
	t.Helper()
	before := assertFencedSessionVersion(t, fixture, fixture.session.Version())
	event := nextFencedEvent(t, fixture.session, fixture.fence.RunID, "must not persist")
	err := fixture.store.AppendEventsFenced(context.Background(), fence, before.Version(), []core.SessionEvent{event})
	if !errors.Is(err, ErrSessionWriteFenceLost) {
		t.Fatalf("fenced append error=%v, want ErrSessionWriteFenceLost", err)
	}
	assertFencedSessionVersion(t, fixture, before.Version())
	var chunks int
	if err := fixture.store.db.QueryRowContext(context.Background(),
		"SELECT COUNT(*) FROM event_chunks WHERE session_id = ?", fixture.fence.SessionID,
	).Scan(&chunks); err != nil {
		t.Fatal(err)
	}
	if chunks != 1 {
		t.Fatalf("ownership loss left partial event chunk: count=%d", chunks)
	}
}

func TestSQLSessionStoreFencedAppendSQLite(t *testing.T) {
	t.Run("success_and_run_evidence", func(t *testing.T) {
		fixture := newSQLiteFencedFixture(t)
		end, err := fixture.session.Append(fixture.fence.RunID, core.EvRunEnd, core.RunEndData{Status: core.RunCompleted})
		if err != nil {
			t.Fatal(err)
		}
		if err := fixture.store.AppendEventsFenced(context.Background(), fixture.fence, 1, []core.SessionEvent{end}); err != nil {
			t.Fatal(err)
		}
		assertFencedSessionVersion(t, fixture, 2)
		var status string
		if err := fixture.store.db.QueryRowContext(context.Background(),
			"SELECT status FROM run_evidence WHERE session_id = ? AND run_id = ?",
			fixture.fence.SessionID, fixture.fence.RunID,
		).Scan(&status); err != nil {
			t.Fatal(err)
		}
		if status != string(core.RunCompleted) {
			t.Fatalf("run evidence status=%q", status)
		}
	})

	tests := []struct {
		name   string
		mutate func(*testing.T, *fencedSQLFixture) SessionWriteFence
	}{
		{"wrong_worker", func(_ *testing.T, fixture *fencedSQLFixture) SessionWriteFence {
			fence := fixture.fence
			fence.WorkerID = "worker-other"
			return fence
		}},
		{"wrong_generation", func(_ *testing.T, fixture *fencedSQLFixture) SessionWriteFence {
			fence := fixture.fence
			fence.QueueGeneration++
			return fence
		}},
		{"expired_queue_lease", func(t *testing.T, fixture *fencedSQLFixture) SessionWriteFence {
			if _, err := fixture.store.db.ExecContext(context.Background(), "UPDATE run_queue SET lease_expires_at = 1 WHERE run_id = ?", fixture.fence.RunID); err != nil {
				t.Fatal(err)
			}
			return fixture.fence
		}},
		{"cancel_requested", func(t *testing.T, fixture *fencedSQLFixture) SessionWriteFence {
			changed, err := fixture.queue.RequestRunCancel(context.Background(), fixture.fence.RunID)
			if err != nil || !changed {
				t.Fatalf("request cancel: changed=%t err=%v", changed, err)
			}
			return fixture.fence
		}},
		{"run_not_running", func(t *testing.T, fixture *fencedSQLFixture) SessionWriteFence {
			if _, err := fixture.store.db.ExecContext(context.Background(), "UPDATE run_control SET status = 'queued' WHERE run_id = ?", fixture.fence.RunID); err != nil {
				t.Fatal(err)
			}
			return fixture.fence
		}},
		{"wrong_session_lease", func(_ *testing.T, fixture *fencedSQLFixture) SessionWriteFence {
			fence := fixture.fence
			fence.LeaseHolder = "lease-other"
			return fence
		}},
		{"expired_session_lease", func(t *testing.T, fixture *fencedSQLFixture) SessionWriteFence {
			if _, err := fixture.store.db.ExecContext(context.Background(), "UPDATE session_leases SET expires_at = 1 WHERE session_id = ?", fixture.fence.SessionID); err != nil {
				t.Fatal(err)
			}
			return fixture.fence
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newSQLiteFencedFixture(t)
			assertFenceLostWithoutWrite(t, fixture, test.mutate(t, fixture))
		})
	}

	t.Run("wrong_run_session_pair", func(t *testing.T) {
		fixture := newSQLiteFencedFixture(t)
		other := mustNamedSession(t, "session-fenced-other")
		if err := fixture.store.Create(context.Background(), other); err != nil {
			t.Fatal(err)
		}
		fence := fixture.fence
		fence.SessionID = other.ID()
		err := fixture.store.AppendEventsFenced(context.Background(), fence, 0, []core.SessionEvent{
			rawFencedEvent(t, 0, fence.RunID, "must not cross sessions"),
		})
		if !errors.Is(err, ErrSessionWriteFenceLost) {
			t.Fatalf("wrong run/session error=%v", err)
		}
		loaded, err := fixture.store.Load(context.Background(), other.ID())
		if err != nil || loaded.Version() != 0 {
			t.Fatalf("wrong run/session changed target: version=%d err=%v", loaded.Version(), err)
		}
	})

	t.Run("version_conflict", func(t *testing.T) {
		fixture := newSQLiteFencedFixture(t)
		winner := nextFencedEvent(t, fixture.session, fixture.fence.RunID, "winner")
		if err := fixture.store.AppendEvents(context.Background(), fixture.fence.SessionID, 1, []core.SessionEvent{winner}); err != nil {
			t.Fatal(err)
		}
		stale := rawFencedEvent(t, 1, fixture.fence.RunID, "stale")
		err := fixture.store.AppendEventsFenced(context.Background(), fixture.fence, 1, []core.SessionEvent{stale})
		if !errors.Is(err, core.ErrSessionConflict) || errors.Is(err, ErrSessionWriteFenceLost) {
			t.Fatalf("version conflict classification=%v", err)
		}
		assertFencedSessionVersion(t, fixture, 2)
	})

	t.Run("empty_suffix_is_a_noop_without_fence_probe", func(t *testing.T) {
		fixture := newSQLiteFencedFixture(t)
		var version, eventCount, updatedAt int64
		if err := fixture.store.db.QueryRowContext(context.Background(),
			"SELECT version, event_count, updated_at FROM sessions WHERE id = ?", fixture.fence.SessionID,
		).Scan(&version, &eventCount, &updatedAt); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.store.db.ExecContext(context.Background(), "UPDATE run_queue SET worker_id = 'worker-other' WHERE run_id = ?", fixture.fence.RunID); err != nil {
			t.Fatal(err)
		}
		if err := fixture.store.AppendEventsFenced(context.Background(), fixture.fence, version, nil); err != nil {
			t.Fatalf("empty fenced append: %v", err)
		}
		var gotVersion, gotEventCount, gotUpdatedAt int64
		if err := fixture.store.db.QueryRowContext(context.Background(),
			"SELECT version, event_count, updated_at FROM sessions WHERE id = ?", fixture.fence.SessionID,
		).Scan(&gotVersion, &gotEventCount, &gotUpdatedAt); err != nil {
			t.Fatal(err)
		}
		if gotVersion != version || gotEventCount != eventCount || gotUpdatedAt != updatedAt {
			t.Fatalf("empty fenced append mutated session: got version=%d events=%d updated_at=%d; want version=%d events=%d updated_at=%d", gotVersion, gotEventCount, gotUpdatedAt, version, eventCount, updatedAt)
		}
	})

	t.Run("final_fence_recheck_rolls_back_chunk_evidence_and_tip", func(t *testing.T) {
		fixture := newSQLiteFencedFixture(t)
		trigger := "fenced_change_owner_after_chunk"
		if _, err := fixture.store.db.ExecContext(context.Background(), `CREATE TRIGGER `+trigger+`
			AFTER INSERT ON event_chunks WHEN NEW.session_id = '`+fixture.fence.SessionID+`'
			BEGIN
				UPDATE run_queue SET worker_id = 'worker-triggered' WHERE run_id = '`+fixture.fence.RunID+`';
			END`); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if _, err := fixture.store.db.ExecContext(context.Background(), "DROP TRIGGER IF EXISTS "+trigger); err != nil {
				t.Error(err)
			}
		})
		end, err := fixture.session.Append(fixture.fence.RunID, core.EvRunEnd, core.RunEndData{Status: core.RunCompleted})
		if err != nil {
			t.Fatal(err)
		}
		err = fixture.store.AppendEventsFenced(context.Background(), fixture.fence, 1, []core.SessionEvent{end})
		if !errors.Is(err, ErrSessionWriteFenceLost) || errors.Is(err, core.ErrSessionConflict) {
			t.Fatalf("final fence recheck error=%v", err)
		}
		assertFencedSessionVersion(t, fixture, 1)
		var chunks int
		if err := fixture.store.db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM event_chunks WHERE session_id = ?", fixture.fence.SessionID).Scan(&chunks); err != nil {
			t.Fatal(err)
		}
		if chunks != 1 {
			t.Fatalf("final fence failure left chunks=%d", chunks)
		}
		var status, workerID string
		if err := fixture.store.db.QueryRowContext(context.Background(), "SELECT status FROM run_evidence WHERE session_id = ? AND run_id = ?", fixture.fence.SessionID, fixture.fence.RunID).Scan(&status); err != nil {
			t.Fatal(err)
		}
		if err := fixture.store.db.QueryRowContext(context.Background(), "SELECT worker_id FROM run_queue WHERE run_id = ?", fixture.fence.RunID).Scan(&workerID); err != nil {
			t.Fatal(err)
		}
		if status != string(RunStatusRunning) || workerID != fixture.fence.WorkerID {
			t.Fatalf("rolled-back state: evidence=%q queue worker=%q", status, workerID)
		}
	})

	t.Run("database_error_is_not_fence_loss", func(t *testing.T) {
		fixture := newSQLiteFencedFixture(t)
		trigger := "fenced_reject_chunk_insert"
		if _, err := fixture.store.db.ExecContext(context.Background(), `CREATE TRIGGER `+trigger+`
			BEFORE INSERT ON event_chunks WHEN NEW.session_id = '`+fixture.fence.SessionID+`'
			BEGIN
				SELECT RAISE(ABORT, 'forced fenced append database error');
			END`); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if _, err := fixture.store.db.ExecContext(context.Background(), "DROP TRIGGER IF EXISTS "+trigger); err != nil {
				t.Error(err)
			}
		})
		end, err := fixture.session.Append(fixture.fence.RunID, core.EvRunEnd, core.RunEndData{Status: core.RunCompleted})
		if err != nil {
			t.Fatal(err)
		}
		err = fixture.store.AppendEventsFenced(context.Background(), fixture.fence, 1, []core.SessionEvent{end})
		if err == nil || errors.Is(err, ErrSessionWriteFenceLost) || errors.Is(err, core.ErrSessionConflict) {
			t.Fatalf("database error classification=%v", err)
		}
		assertFencedSessionVersion(t, fixture, 1)
		var status string
		if err := fixture.store.db.QueryRowContext(context.Background(), "SELECT status FROM run_evidence WHERE session_id = ? AND run_id = ?", fixture.fence.SessionID, fixture.fence.RunID).Scan(&status); err != nil {
			t.Fatal(err)
		}
		if status != string(RunStatusRunning) {
			t.Fatalf("database error changed evidence status=%q", status)
		}
	})
}

func TestSQLSessionStoreFencedAppendRejectsInvalidInput(t *testing.T) {
	fixture := newSQLiteFencedFixture(t)
	valid := fixture.fence
	tests := []struct {
		name   string
		fence  SessionWriteFence
		events []core.SessionEvent
	}{
		{name: "empty_session", fence: func() SessionWriteFence { f := valid; f.SessionID = ""; return f }()},
		{name: "empty_run", fence: func() SessionWriteFence { f := valid; f.RunID = ""; return f }()},
		{name: "empty_tenant", fence: func() SessionWriteFence { f := valid; f.TenantID = ""; return f }()},
		{name: "empty_subject", fence: func() SessionWriteFence { f := valid; f.SubjectID = ""; return f }()},
		{name: "empty_worker", fence: func() SessionWriteFence { f := valid; f.WorkerID = ""; return f }()},
		{name: "zero_generation", fence: func() SessionWriteFence { f := valid; f.QueueGeneration = 0; return f }()},
		{name: "empty_holder", fence: func() SessionWriteFence { f := valid; f.LeaseHolder = ""; return f }()},
		{name: "event_other_run", fence: valid, events: []core.SessionEvent{rawFencedEvent(t, 1, "run-other", "bad")}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := fixture.store.AppendEventsFenced(context.Background(), test.fence, 1, test.events)
			if err == nil || errors.Is(err, ErrSessionWriteFenceLost) || errors.Is(err, core.ErrSessionConflict) {
				t.Fatalf("invalid input error=%v", err)
			}
			assertFencedSessionVersion(t, fixture, 1)
		})
	}
	t.Run("run_owner_mismatch", func(t *testing.T) {
		fixture := newSQLiteFencedFixture(t)
		if _, err := fixture.store.db.ExecContext(context.Background(), "UPDATE run_control SET tenant_id = 'tenant-other' WHERE run_id = ?", fixture.fence.RunID); err != nil {
			t.Fatal(err)
		}
		assertFenceLostWithoutWrite(t, fixture, fixture.fence)
	})
	t.Run("session_header_owner_mismatch", func(t *testing.T) {
		fixture := newSQLiteFencedFixture(t)
		var header string
		if err := fixture.store.db.QueryRowContext(context.Background(), "SELECT header FROM sessions WHERE id = ?", fixture.fence.SessionID).Scan(&header); err != nil {
			t.Fatal(err)
		}
		var options core.SessionOptions
		if err := json.Unmarshal([]byte(header), &options); err != nil {
			t.Fatal(err)
		}
		options.Principal.SubjectID = "subject-other"
		encoded, err := json.Marshal(options)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.store.db.ExecContext(context.Background(), "UPDATE sessions SET header = ? WHERE id = ?", string(encoded), fixture.fence.SessionID); err != nil {
			t.Fatal(err)
		}
		assertFenceLostWithoutWrite(t, fixture, fixture.fence)
	})
	t.Run("session_catalog_owner_mismatch", func(t *testing.T) {
		fixture := newSQLiteFencedFixture(t)
		if _, err := fixture.store.db.ExecContext(context.Background(), "UPDATE sessions SET tenant_id = 'tenant-other' WHERE id = ?", fixture.fence.SessionID); err != nil {
			t.Fatal(err)
		}
		assertFenceLostWithoutWrite(t, fixture, fixture.fence)
	})
	t.Run("session_identity_precedes_version_conflict", func(t *testing.T) {
		fixture := newSQLiteFencedFixture(t)
		if _, err := fixture.store.db.ExecContext(context.Background(), "UPDATE sessions SET tenant_id = 'tenant-other' WHERE id = ?", fixture.fence.SessionID); err != nil {
			t.Fatal(err)
		}
		err := fixture.store.AppendEventsFenced(context.Background(), fixture.fence, 99, []core.SessionEvent{
			rawFencedEvent(t, 99, fixture.fence.RunID, "identity takes priority"),
		})
		if !errors.Is(err, ErrSessionWriteFenceLost) || errors.Is(err, core.ErrSessionConflict) {
			t.Fatalf("identity/version classification=%v", err)
		}
		assertFencedSessionVersion(t, fixture, 1)
	})
}

func TestSQLSessionStoreFencedAppendRejectsReplacedGeneration(t *testing.T) {
	fixture := newSQLiteFencedFixture(t)
	ctx := context.Background()
	if _, err := fixture.store.db.ExecContext(ctx, "UPDATE run_queue SET lease_expires_at = 1 WHERE run_id = ?", fixture.fence.RunID); err != nil {
		t.Fatal(err)
	}
	if requeued, failed, err := fixture.queue.RecoverExpiredRunClaims(ctx, time.Now().UTC()); err != nil || requeued != 1 || failed != 0 {
		t.Fatalf("recover claim: requeued=%d failed=%d err=%v", requeued, failed, err)
	}
	claim, ok, err := fixture.queue.ClaimRun(ctx, "worker-successor", time.Hour)
	if err != nil || !ok || claim.Generation <= fixture.fence.QueueGeneration {
		t.Fatalf("claim successor: %#v ok=%t err=%v", claim, ok, err)
	}
	successorFence := fixture.fence
	successorFence.WorkerID = claim.WorkerID
	successorFence.QueueGeneration = claim.Generation
	oldEvent := rawFencedEvent(t, 1, fixture.fence.RunID, "old generation")
	newEvent := rawFencedEvent(t, 1, fixture.fence.RunID, "new generation")
	start := make(chan struct{})
	var wg sync.WaitGroup
	var oldErr, newErr error
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		oldErr = fixture.store.AppendEventsFenced(ctx, fixture.fence, 1, []core.SessionEvent{oldEvent})
	}()
	go func() {
		defer wg.Done()
		<-start
		newErr = fixture.store.AppendEventsFenced(ctx, successorFence, 1, []core.SessionEvent{newEvent})
	}()
	close(start)
	wg.Wait()
	if !errors.Is(oldErr, ErrSessionWriteFenceLost) {
		t.Fatalf("old generation error=%v", oldErr)
	}
	if newErr != nil {
		t.Fatalf("successor append: %v", newErr)
	}
	loaded := assertFencedSessionVersion(t, fixture, 2)
	events := loaded.Events()
	var message core.UserMessageData
	if err := json.Unmarshal(events[1].Data, &message); err != nil || message.Text != "new generation" {
		t.Fatalf("durable successor event=%#v err=%v", message, err)
	}
}

func TestSQLiteFencedAcquireReservesWriter(t *testing.T) {
	fixture := newSQLiteFencedFixture(t)
	tx, err := fixture.store.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.acquireSQLiteSessionWriteFence(context.Background(), tx, fixture.fence, 1); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}

	result := make(chan error, 1)
	go func() {
		_, err := fixture.store.db.ExecContext(context.Background(),
			"UPDATE run_queue SET worker_id = 'worker-contender' WHERE run_id = ?", fixture.fence.RunID,
		)
		result <- err
	}()

	blocked := false
	select {
	case err := <-result:
		if err == nil {
			_ = tx.Rollback()
			t.Fatal("competing SQLite writer completed before fenced acquisition released")
		}
		if message := strings.ToLower(err.Error()); !strings.Contains(message, "busy") && !strings.Contains(message, "locked") {
			_ = tx.Rollback()
			t.Fatalf("competing SQLite writer failed without a lock error: %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		blocked = true
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if blocked {
		select {
		case err := <-result:
			if err != nil {
				t.Fatalf("competing SQLite writer did not complete after reservation release: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("competing SQLite writer remained blocked after reservation release")
		}
	}
}

func TestRepairInterruptedSessionFenced(t *testing.T) {
	t.Run("once_then_noop", func(t *testing.T) {
		fixture := newSQLiteFencedFixture(t)
		call, err := fixture.session.Append(fixture.fence.RunID, core.EvToolCall, core.ToolCallData{CallID: "call-fenced", Name: "test.tool"})
		if err != nil {
			t.Fatal(err)
		}
		if err := fixture.store.AppendEventsFenced(context.Background(), fixture.fence, 1, []core.SessionEvent{call}); err != nil {
			t.Fatal(err)
		}
		loaded := assertFencedSessionVersion(t, fixture, 2)
		repaired, applied, err := RepairInterruptedSessionFenced(context.Background(), fixture.store, fixture.fence, loaded)
		if err != nil || !applied {
			t.Fatalf("fenced repair: applied=%t err=%v", applied, err)
		}
		persisted := assertFencedSessionVersion(t, fixture, repaired.Version())
		again, applied, err := RepairInterruptedSessionFenced(context.Background(), fixture.store, fixture.fence, persisted)
		if err != nil || applied || again.Version() != persisted.Version() {
			t.Fatalf("second fenced repair: applied=%t version=%d err=%v", applied, again.Version(), err)
		}
	})

	t.Run("ownership_loss_never_writes", func(t *testing.T) {
		fixture := newSQLiteFencedFixture(t)
		call, err := fixture.session.Append(fixture.fence.RunID, core.EvToolCall, core.ToolCallData{CallID: "call-fenced", Name: "test.tool"})
		if err != nil {
			t.Fatal(err)
		}
		if err := fixture.store.AppendEventsFenced(context.Background(), fixture.fence, 1, []core.SessionEvent{call}); err != nil {
			t.Fatal(err)
		}
		loaded := assertFencedSessionVersion(t, fixture, 2)
		if _, err := fixture.store.db.ExecContext(context.Background(), "UPDATE run_queue SET worker_id = 'worker-other' WHERE run_id = ?", fixture.fence.RunID); err != nil {
			t.Fatal(err)
		}
		_, applied, err := RepairInterruptedSessionFenced(context.Background(), fixture.store, fixture.fence, loaded)
		if !errors.Is(err, ErrSessionWriteFenceLost) || applied {
			t.Fatalf("lost-fence repair: applied=%t err=%v", applied, err)
		}
		assertFencedSessionVersion(t, fixture, 2)
		var status string
		if err := fixture.store.db.QueryRowContext(context.Background(),
			"SELECT status FROM run_evidence WHERE session_id = ? AND run_id = ?",
			fixture.fence.SessionID, fixture.fence.RunID,
		).Scan(&status); err != nil {
			t.Fatal(err)
		}
		if status != "running" {
			t.Fatalf("failed repair changed evidence status=%q", status)
		}
	})

	t.Run("stale_open_snapshot_converges_after_another_repair", func(t *testing.T) {
		fixture := newSQLiteFencedFixture(t)
		call, err := fixture.session.Append(fixture.fence.RunID, core.EvToolCall, core.ToolCallData{CallID: "call-fenced", Name: "test.tool"})
		if err != nil {
			t.Fatal(err)
		}
		if err := fixture.store.AppendEventsFenced(context.Background(), fixture.fence, 1, []core.SessionEvent{call}); err != nil {
			t.Fatal(err)
		}
		stale := assertFencedSessionVersion(t, fixture, 2)
		winner, err := fixture.store.Load(context.Background(), fixture.fence.SessionID)
		if err != nil {
			t.Fatal(err)
		}
		winner, applied, err := RepairInterruptedSessionFenced(context.Background(), fixture.store, fixture.fence, winner)
		if err != nil || !applied {
			t.Fatalf("winning repair: applied=%t err=%v", applied, err)
		}
		converged, applied, err := RepairInterruptedSessionFenced(context.Background(), fixture.store, fixture.fence, stale)
		if err != nil || applied {
			t.Fatalf("stale repair convergence: applied=%t err=%v", applied, err)
		}
		if converged.Version() != winner.Version() {
			t.Fatalf("converged version=%d winner version=%d", converged.Version(), winner.Version())
		}
		assertFencedSessionVersion(t, fixture, winner.Version())
	})
}

func TestFencedWriteBehindUsesOptionalAppender(t *testing.T) {
	fixture := newSQLiteFencedFixture(t)
	writer, err := NewFencedWriteBehind(fixture.store, fixture.fence, fixture.session, 1, -1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.session.Append(fixture.fence.RunID, core.EvUserMessage, core.UserMessageData{Text: "checkpoint"}); err != nil {
		t.Fatal(err)
	}
	if err := writer.Checkpoint(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertFencedSessionVersion(t, fixture, 2)
	if _, err := fixture.store.db.ExecContext(context.Background(), "UPDATE run_queue SET worker_id = 'worker-other' WHERE run_id = ?", fixture.fence.RunID); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.session.Append(fixture.fence.RunID, core.EvUserMessage, core.UserMessageData{Text: "stale"}); err != nil {
		t.Fatal(err)
	}
	if err := writer.Checkpoint(context.Background()); !errors.Is(err, ErrSessionWriteFenceLost) {
		t.Fatalf("stale checkpoint error=%v", err)
	}
	assertFencedSessionVersion(t, fixture, 2)

	memory := core.NewMemorySessionStore()
	if _, err := NewFencedWriteBehind(memory, fixture.fence, fixture.session, 2, -1); err == nil {
		t.Fatal("non-atomic store implemented fenced write-behind fallback")
	}
	for _, startVersion := range []int64{-1, fixture.session.Version() + 1} {
		if _, err := NewFencedWriteBehind(fixture.store, fixture.fence, fixture.session, startVersion, -1); err == nil {
			t.Fatalf("invalid fenced write-behind start version %d was accepted", startVersion)
		}
	}

	t.Run("background_fence_loss_stops_future_scheduling", func(t *testing.T) {
		fixture := newSQLiteFencedFixture(t)
		writer, err := NewFencedWriteBehind(fixture.store, fixture.fence, fixture.session, 1, time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.store.db.ExecContext(context.Background(), "UPDATE run_queue SET worker_id = 'worker-other' WHERE run_id = ?", fixture.fence.RunID); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.session.Append(fixture.fence.RunID, core.EvUserMessage, core.UserMessageData{Text: "background stale"}); err != nil {
			t.Fatal(err)
		}
		writer.MarkDirty()
		deadline := time.Now().Add(2 * time.Second)
		for {
			writer.mu.Lock()
			backgroundErr := writer.lastErr
			writer.mu.Unlock()
			if errors.Is(backgroundErr, ErrSessionWriteFenceLost) {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("background fenced flush did not report ownership loss: %v", backgroundErr)
			}
			time.Sleep(time.Millisecond)
		}
		if err := writer.Abort(context.Background()); !errors.Is(err, ErrSessionWriteFenceLost) {
			t.Fatalf("abort after background fence loss=%v", err)
		}
		if _, err := fixture.session.Append(fixture.fence.RunID, core.EvUserMessage, core.UserMessageData{Text: "must remain pending"}); err != nil {
			t.Fatal(err)
		}
		writer.MarkDirty()
		time.Sleep(10 * time.Millisecond)
		if got := assertFencedSessionVersion(t, fixture, 1).Version(); got != 1 {
			t.Fatalf("fence-lost writer persisted after abort: version=%d", got)
		}
	})
}

func TestPostgresSessionStoreFencedAppend(t *testing.T) {
	db := newPostgresTestDB(t)
	store, err := OpenSQLSessionStore(context.Background(), db, SQLDialectPostgres)
	if err != nil {
		t.Fatal(err)
	}
	fixture := newFencedSQLFixture(t, store, "session-pg-fenced", "run-pg-fenced")
	event := nextFencedEvent(t, fixture.session, fixture.fence.RunID, "postgres")
	if err := store.AppendEventsFenced(context.Background(), fixture.fence, 1, []core.SessionEvent{event}); err != nil {
		t.Fatal(err)
	}
	assertFencedSessionVersion(t, fixture, 2)
	if _, err := store.db.ExecContext(context.Background(), "UPDATE run_queue SET generation = generation + 1 WHERE run_id = $1", fixture.fence.RunID); err != nil {
		t.Fatal(err)
	}
	stale := rawFencedEvent(t, 2, fixture.fence.RunID, "stale postgres")
	if err := store.AppendEventsFenced(context.Background(), fixture.fence, 2, []core.SessionEvent{stale}); !errors.Is(err, ErrSessionWriteFenceLost) {
		t.Fatalf("postgres stale generation error=%v", err)
	}
	assertFencedSessionVersion(t, fixture, 2)
}

func TestPostgresFencedSQLBinding(t *testing.T) {
	for name, query := range map[string]sqlQuery{
		"session": sqlFencePostgresLockSession,
		"run":     sqlFencePostgresLockRun,
		"queue":   sqlFencePostgresLockQueue,
		"lease":   sqlFencePostgresLockLease,
		"renew":   sqlLockRunningRunForRenew,
	} {
		bound := query.bind(SQLDialectPostgres)
		if strings.Contains(bound, "?") || !strings.Contains(bound, "$1") || !strings.Contains(bound, "FOR UPDATE") {
			t.Fatalf("postgres %s lock query binding=%q", name, bound)
		}
	}
	tip := fencedSessionTipUpdate(SQLDialectPostgres)
	if strings.Contains(tip, "?") || !strings.Contains(tip, "clock_timestamp()") {
		t.Fatalf("postgres fenced tip query=%q", tip)
	}
	for parameter := 1; parameter <= 13; parameter++ {
		if !strings.Contains(tip, "$"+strconv.Itoa(parameter)) {
			t.Fatalf("postgres fenced tip query is missing $%d: %q", parameter, tip)
		}
	}
	if strings.Contains(tip, "$14") {
		t.Fatalf("postgres fenced tip query has unexpected placeholder: %q", tip)
	}
}
