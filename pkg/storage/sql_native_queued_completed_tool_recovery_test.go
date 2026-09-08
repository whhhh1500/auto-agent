package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	core "github.com/whhhh1500/auto-agent/pkg/core"
)

type nativeQueuedCompletedToolRecoveryFixture struct {
	*nativeQueuedToolEffectFixture
	result core.CapabilityResult
}

func insertNativeQueuedModelAttemptForRecoveryTest(t *testing.T, fixture *nativeQueuedCompletedToolRecoveryFixture, withOutcome bool) {
	t.Helper()
	if _, err := fixture.store.db.ExecContext(context.Background(), (sqlQuery{`INSERT INTO native_queued_model_invocations
		(protocol, tenant_id, subject_id, session_id, run_id, invocation_id, step_index, step_start_seq,
		session_version_at_admission, request_json, request_sha256, authorization_epoch, queue_generation,
		lease_holder_sha256, run_start_seq, profile_snapshot_id, capability_snapshot_id, composition_revision,
		assignment_revision, composition_sha256, bootstrap_revision, model_contract_sha256, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`}).bind(fixture.store.dialect),
		nativeQueuedModelInvocationProtocol, fixture.fence.TenantID, fixture.fence.SubjectID, fixture.fence.SessionID, fixture.fence.RunID, "model:999", 99, 1,
		2, `{}`, strings.Repeat("0", 64), 0, 1, strings.Repeat("0", 64), 0, "profile", "capabilities", strings.Repeat("0", 64), "", strings.Repeat("0", 64), "bootstrap", strings.Repeat("0", 64), 1); err != nil {
		t.Fatal(err)
	}
	if !withOutcome {
		return
	}
	if _, err := fixture.store.db.ExecContext(context.Background(), (sqlQuery{`INSERT INTO native_queued_model_invocation_outcomes
		(protocol, session_id, run_id, invocation_id, attempt_request_sha256, assistant_event_seq,
		usage_event_seq, session_version_after_outcome, outcome_sha256, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`}).bind(fixture.store.dialect),
		nativeQueuedModelOutcomeProtocol, fixture.fence.SessionID, fixture.fence.RunID, "model:999", strings.Repeat("0", 64), 2, 3, 4, strings.Repeat("0", 64), 1); err != nil {
		t.Fatal(err)
	}
}

func newNativeQueuedCompletedToolRecoveryFixture(t *testing.T, store *SQLSessionStore, sessionID, runID string) *nativeQueuedCompletedToolRecoveryFixture {
	t.Helper()
	fixture := newNativeQueuedToolEffectFixture(t, store, sessionID, runID, true)
	if _, decision, err := store.BeginNativeQueuedToolEffectFenced(context.Background(), fixture.fence, fixture.version, fixture.input); err != nil || decision != core.ToolInvocationExecuteNew {
		t.Fatalf("begin decision=%q err=%v", decision, err)
	}
	journal, err := NewSQLToolInvocationJournal(store.db, store.dialect)
	if err != nil {
		t.Fatal(err)
	}
	result := core.CapabilityResult{Content: "native completed result", OK: true, Metadata: map[string]any{"source": "journal"}}
	if _, err := journal.CompleteToolInvocation(context.Background(), fixture.input.Invocation, result); err != nil {
		t.Fatal(err)
	}
	return &nativeQueuedCompletedToolRecoveryFixture{nativeQueuedToolEffectFixture: fixture, result: result}
}

func assertNativeQueuedCompletedToolRecovery(t *testing.T, fixture *nativeQueuedCompletedToolRecoveryFixture) {
	t.Helper()
	loaded := assertFencedSessionVersion(t, fixture.fencedSQLFixture, fixture.version+1)
	event := loaded.Events()[fixture.version]
	if event.Type != core.EvToolResult || event.RunID != fixture.fence.RunID {
		t.Fatalf("recovery tail=%+v", event)
	}
	var result core.ToolResultData
	if err := json.Unmarshal(event.Data, &result); err != nil {
		t.Fatal(err)
	}
	if result.CallID != fixture.input.Invocation.CallID || result.Content != fixture.result.Content || result.OK != fixture.result.OK {
		t.Fatalf("recovery result=%+v", result)
	}
	var sidecars int
	if err := fixture.store.db.QueryRowContext(context.Background(), (sqlQuery{"SELECT COUNT(*) FROM completed_tool_result_recovery_sidecars WHERE session_id = ? AND result_event_seq = ?"}).bind(fixture.store.dialect), fixture.fence.SessionID, fixture.version).Scan(&sidecars); err != nil || sidecars != 1 {
		t.Fatalf("sidecars=%d err=%v", sidecars, err)
	}
}

func assertNativeQueuedCompletedToolRecoveryConcurrent(t *testing.T, fixture *nativeQueuedCompletedToolRecoveryFixture) {
	t.Helper()
	start := make(chan struct{})
	type result struct {
		session   *core.Session
		recovered bool
		err       error
	}
	results := make(chan result, 2)
	for range 2 {
		go func() {
			<-start
			session, recovered, err := fixture.store.RecoverNativeQueuedCompletedToolResultFenced(context.Background(), fixture.fence, fixture.input.AuthorizationEpoch, fixture.version)
			results <- result{session: session, recovered: recovered, err: err}
		}()
	}
	close(start)
	for range 2 {
		outcome := <-results
		if outcome.err != nil || !outcome.recovered || outcome.session == nil || outcome.session.Version() != fixture.version+1 {
			t.Fatalf("concurrent recovery session=%v recovered=%t err=%v", outcome.session, outcome.recovered, outcome.err)
		}
	}
	assertNativeQueuedCompletedToolRecovery(t, fixture)
}

// assertNativeQueuedModelPruneAcceptsCoalescedBaseChunk exercises the
// WriteBehind shape in which a physical chunk begins before the v45 admission
// but contains the later v46 outcome. The v46 digest remains a logical batch
// prefix, so retention must validate it from admission rather than the chunk
// start while still rejecting a hash that starts in the middle of a chunk.
func assertNativeQueuedModelPruneAcceptsCoalescedBaseChunk(t *testing.T, ctx context.Context, store *SQLSessionStore, sessionID, runID string) {
	t.Helper()
	fixture := newNativeQueuedModelOutcomeFixture(t, store, sessionID, runID)
	if appended, err := store.AppendNativeQueuedModelOutcomeFenced(ctx, fixture.fence, fixture.version, fixture.events); err != nil || !appended {
		t.Fatalf("append=%t err=%v", appended, err)
	}
	appendNativeQueuedModelSuccessor(t, fixture)
	var basePayload, outcomePayload string
	if err := store.db.QueryRowContext(ctx, (sqlQuery{"SELECT payload FROM event_chunks WHERE session_id = ? AND start_seq = ?"}).bind(store.dialect), fixture.fence.SessionID, 0).Scan(&basePayload); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRowContext(ctx, (sqlQuery{"SELECT payload FROM event_chunks WHERE session_id = ? AND start_seq = ?"}).bind(store.dialect), fixture.fence.SessionID, fixture.version).Scan(&outcomePayload); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(ctx, (sqlQuery{"UPDATE event_chunks SET payload = ? WHERE session_id = ? AND start_seq = ?"}).bind(store.dialect), basePayload+outcomePayload, fixture.fence.SessionID, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(ctx, (sqlQuery{"DELETE FROM event_chunks WHERE session_id = ? AND start_seq = ?"}).bind(store.dialect), fixture.fence.SessionID, fixture.version); err != nil {
		t.Fatal(err)
	}
	if deleted, err := store.PruneNativeQueuedModelInvocations(ctx, time.Now().UTC().Add(time.Hour)); err != nil || deleted != 1 {
		t.Fatalf("coalesced prune deleted=%d err=%v", deleted, err)
	}
	if attempts, outcomes := nativeQueuedModelPairCounts(t, store, fixture.fence.SessionID); attempts != 0 || outcomes != 0 {
		t.Fatalf("coalesced prune attempts=%d outcomes=%d", attempts, outcomes)
	}
}

func TestSQLSessionStoreRecoverNativeQueuedCompletedToolResultFenced(t *testing.T) {
	t.Run("prune_accepts_v46_after_coalesced_base_chunk", func(t *testing.T) {
		assertNativeQueuedModelPruneAcceptsCoalescedBaseChunk(t, context.Background(), newTestSQLStore(t), "session-native-tool-prune-coalesced", "run-native-tool-prune-coalesced")
	})
	t.Run("prune_rejects_out_of_range_v46_indices", func(t *testing.T) {
		fixture := newNativeQueuedModelOutcomeFixture(t, newTestSQLStore(t), "session-native-tool-prune-index", "run-native-tool-prune-index")
		if appended, err := fixture.store.AppendNativeQueuedModelOutcomeFenced(context.Background(), fixture.fence, fixture.version, fixture.events); err != nil || !appended {
			t.Fatalf("append=%t err=%v", appended, err)
		}
		appendNativeQueuedModelSuccessor(t, fixture)
		if _, err := fixture.store.db.ExecContext(context.Background(), (sqlQuery{`UPDATE native_queued_model_invocation_outcomes
			SET assistant_event_seq = ?, usage_event_seq = ?, session_version_after_outcome = ? WHERE session_id = ?`}).bind(fixture.store.dialect), 99, 100, 101, fixture.fence.SessionID); err != nil {
			t.Fatal(err)
		}
		if deleted, err := fixture.store.PruneNativeQueuedModelInvocations(context.Background(), time.Now().UTC().Add(time.Hour)); deleted != 0 || !errors.Is(err, ErrCompletedToolResultProofInvalid) {
			t.Fatalf("out-of-range prune deleted=%d err=%v", deleted, err)
		}
	})

	t.Run("A_delivery_and_B_readback_response_lost", func(t *testing.T) {
		fixture := newNativeQueuedCompletedToolRecoveryFixture(t, newTestSQLStore(t), "session-native-tool-recovery", "run-native-tool-recovery")
		session, recovered, err := fixture.store.RecoverNativeQueuedCompletedToolResultFenced(context.Background(), fixture.fence, fixture.input.AuthorizationEpoch, fixture.version)
		if err != nil || !recovered || session == nil || session.Version() != fixture.version+1 {
			t.Fatalf("A session=%v recovered=%t err=%v", session, recovered, err)
		}
		assertNativeQueuedCompletedToolRecovery(t, fixture)
		// The original expected version converges after a response lost during A.
		session, recovered, err = fixture.store.RecoverNativeQueuedCompletedToolResultFenced(context.Background(), fixture.fence, fixture.input.AuthorizationEpoch, fixture.version)
		if err != nil || !recovered || session == nil || session.Version() != fixture.version+1 {
			t.Fatalf("B response-lost session=%v recovered=%t err=%v", session, recovered, err)
		}
		// A reloaded owner sees the same exact B tail.
		session, recovered, err = fixture.store.RecoverNativeQueuedCompletedToolResultFenced(context.Background(), fixture.fence, fixture.input.AuthorizationEpoch, fixture.version+1)
		if err != nil || !recovered || session == nil || session.Version() != fixture.version+1 {
			t.Fatalf("B readback session=%v recovered=%t err=%v", session, recovered, err)
		}
	})

	t.Run("empty_session_version_zero_is_not_a_candidate", func(t *testing.T) {
		store := newTestSQLStore(t)
		session := mustNamedSession(t, "session-native-tool-empty")
		if err := store.Create(context.Background(), session); err != nil {
			t.Fatal(err)
		}
		queue, err := NewSQLRunControlStore(store.db, store.dialect)
		if err != nil {
			t.Fatal(err)
		}
		runID := "run-native-tool-empty"
		principal := session.Principal()
		if err := queue.EnqueueRun(context.Background(), QueuedRun{RunRecord: RunRecord{RunID: runID, SessionID: session.ID(), TenantID: principal.TenantID, SubjectID: principal.SubjectID}, Message: "empty", MaxAttempts: 1}); err != nil {
			t.Fatal(err)
		}
		claim, claimed, err := queue.ClaimRun(context.Background(), "worker-native-tool-empty", time.Hour)
		if err != nil || !claimed {
			t.Fatalf("claim=%t err=%v", claimed, err)
		}
		if acquired, err := store.AcquireSessionLease(context.Background(), session.ID(), "lease-native-tool-empty", time.Hour); err != nil || !acquired {
			t.Fatalf("lease=%t err=%v", acquired, err)
		}
		epoch, err := store.AuthorizationEpoch(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		fence := SessionWriteFence{SessionID: session.ID(), RunID: runID, TenantID: principal.TenantID, SubjectID: principal.SubjectID, WorkerID: claim.WorkerID, QueueGeneration: claim.Generation, LeaseHolder: "lease-native-tool-empty"}
		if recoveredSession, recovered, err := store.RecoverNativeQueuedCompletedToolResultFenced(context.Background(), fence, epoch, 0); recoveredSession != nil || recovered || err != nil {
			t.Fatalf("empty session=%v recovered=%t err=%v", recoveredSession, recovered, err)
		}
	})

	t.Run("generic_V3_without_native_witness_is_proof_invalid", func(t *testing.T) {
		fixture := newSQLiteCompletedToolResultRecoverySidecarFixture(t)
		if appended, err := fixture.store.AppendCompletedToolResultRecoverySidecarFenced(context.Background(), fixture.fence, fixture.version, fixture.input); err != nil || !appended {
			t.Fatalf("generic V3 append=%t err=%v", appended, err)
		}
		if session, recovered, err := fixture.store.RecoverNativeQueuedCompletedToolResultFenced(context.Background(), fixture.fence, fixture.input.AuthorizationEpoch, fixture.version+1); session != nil || recovered || !errors.Is(err, ErrCompletedToolResultProofInvalid) {
			t.Fatalf("generic V3 session=%v recovered=%t err=%v", session, recovered, err)
		}
	})

	t.Run("missing_A_witness_is_proof_invalid", func(t *testing.T) {
		fixture := newNativeQueuedCompletedToolRecoveryFixture(t, newTestSQLStore(t), "session-native-tool-no-witness", "run-native-tool-no-witness")
		if _, err := fixture.store.db.ExecContext(context.Background(), (sqlQuery{"DELETE FROM native_queued_tool_effect_witnesses WHERE session_id = ? AND run_id = ?"}).bind(fixture.store.dialect), fixture.fence.SessionID, fixture.fence.RunID); err != nil {
			t.Fatal(err)
		}
		if session, recovered, err := fixture.store.RecoverNativeQueuedCompletedToolResultFenced(context.Background(), fixture.fence, fixture.input.AuthorizationEpoch, fixture.version); session != nil || recovered || !errors.Is(err, ErrCompletedToolResultProofInvalid) {
			t.Fatalf("no witness session=%v recovered=%t err=%v", session, recovered, err)
		}
		assertFencedSessionVersion(t, fixture.fencedSQLFixture, fixture.version)
	})

	t.Run("unknown_v45_blocks_even_a_non_candidate_tail", func(t *testing.T) {
		fixture := newNativeQueuedCompletedToolRecoveryFixture(t, newTestSQLStore(t), "session-native-tool-unknown-v45", "run-native-tool-unknown-v45")
		insertNativeQueuedModelAttemptForRecoveryTest(t, fixture, false)
		if session, recovered, err := fixture.store.RecoverNativeQueuedCompletedToolResultFenced(context.Background(), fixture.fence, fixture.input.AuthorizationEpoch, fixture.version); session != nil || recovered || !errors.Is(err, ErrCompletedToolResultProofInvalid) {
			t.Fatalf("unknown v45 session=%v recovered=%t err=%v", session, recovered, err)
		}
	})

	t.Run("corrupt_v46_does_not_make_v45_known", func(t *testing.T) {
		fixture := newNativeQueuedCompletedToolRecoveryFixture(t, newTestSQLStore(t), "session-native-tool-corrupt-v46", "run-native-tool-corrupt-v46")
		insertNativeQueuedModelAttemptForRecoveryTest(t, fixture, true)
		if session, recovered, err := fixture.store.RecoverNativeQueuedCompletedToolResultFenced(context.Background(), fixture.fence, fixture.input.AuthorizationEpoch, fixture.version); session != nil || recovered || !errors.Is(err, ErrCompletedToolResultProofInvalid) {
			t.Fatalf("corrupt v46 session=%v recovered=%t err=%v", session, recovered, err)
		}
	})

	t.Run("B_requires_matching_v44_identity_and_contract", func(t *testing.T) {
		for _, mutation := range []struct {
			name, column, value string
		}{
			{name: "args", column: "args_digest", value: strings.Repeat("0", 64)},
			{name: "contract", column: "capability_contract_sha256", value: strings.Repeat("0", 64)},
		} {
			t.Run(mutation.name, func(t *testing.T) {
				fixture := newNativeQueuedCompletedToolRecoveryFixture(t, newTestSQLStore(t), "session-native-tool-v44-"+mutation.name, "run-native-tool-v44-"+mutation.name)
				if _, recovered, err := fixture.store.RecoverNativeQueuedCompletedToolResultFenced(context.Background(), fixture.fence, fixture.input.AuthorizationEpoch, fixture.version); err != nil || !recovered {
					t.Fatalf("A recovered=%t err=%v", recovered, err)
				}
				query := fmt.Sprintf("UPDATE native_queued_tool_effect_witnesses SET %s = ? WHERE session_id = ? AND run_id = ?", mutation.column)
				if _, err := fixture.store.db.ExecContext(context.Background(), query, mutation.value, fixture.fence.SessionID, fixture.fence.RunID); err != nil {
					t.Fatal(err)
				}
				if session, recovered, err := fixture.store.RecoverNativeQueuedCompletedToolResultFenced(context.Background(), fixture.fence, fixture.input.AuthorizationEpoch, fixture.version+1); session != nil || recovered || !errors.Is(err, ErrCompletedToolResultProofInvalid) {
					t.Fatalf("B mutation session=%v recovered=%t err=%v", session, recovered, err)
				}
			})
		}
	})

	t.Run("current_epoch_and_fence_fail_closed", func(t *testing.T) {
		fixture := newNativeQueuedCompletedToolRecoveryFixture(t, newTestSQLStore(t), "session-native-tool-fence", "run-native-tool-fence")
		lost := fixture.fence
		lost.QueueGeneration++
		if session, recovered, err := fixture.store.RecoverNativeQueuedCompletedToolResultFenced(context.Background(), lost, fixture.input.AuthorizationEpoch, fixture.version); session != nil || recovered || !errors.Is(err, ErrSessionWriteFenceLost) {
			t.Fatalf("generation session=%v recovered=%t err=%v", session, recovered, err)
		}
		bindings, err := NewSQLBindingJournal(fixture.store.db, fixture.store.dialect)
		if err != nil {
			t.Fatal(err)
		}
		if err := bindings.Record(context.Background(), BindingRecord{ID: "native-recovery-epoch", Kind: "policy", Payload: []byte(`{"scope":"global"}`)}); err != nil {
			t.Fatal(err)
		}
		if session, recovered, err := fixture.store.RecoverNativeQueuedCompletedToolResultFenced(context.Background(), fixture.fence, fixture.input.AuthorizationEpoch, fixture.version); session != nil || recovered || !errors.Is(err, ErrAuthorizationEpochChanged) {
			t.Fatalf("epoch session=%v recovered=%t err=%v", session, recovered, err)
		}
		assertFencedSessionVersion(t, fixture.fencedSQLFixture, fixture.version)
	})

	t.Run("current_lease_and_session_CAS_fail_closed", func(t *testing.T) {
		t.Run("lease_holder", func(t *testing.T) {
			fixture := newNativeQueuedCompletedToolRecoveryFixture(t, newTestSQLStore(t), "session-native-tool-lease", "run-native-tool-lease")
			if _, err := fixture.store.db.ExecContext(context.Background(), (sqlQuery{"UPDATE session_leases SET holder = ? WHERE session_id = ?"}).bind(fixture.store.dialect), "other-native-tool-lease", fixture.fence.SessionID); err != nil {
				t.Fatal(err)
			}
			if session, recovered, err := fixture.store.RecoverNativeQueuedCompletedToolResultFenced(context.Background(), fixture.fence, fixture.input.AuthorizationEpoch, fixture.version); session != nil || recovered || !errors.Is(err, ErrSessionWriteFenceLost) {
				t.Fatalf("lease session=%v recovered=%t err=%v", session, recovered, err)
			}
			assertFencedSessionVersion(t, fixture.fencedSQLFixture, fixture.version)
		})
		t.Run("expected_version", func(t *testing.T) {
			fixture := newNativeQueuedCompletedToolRecoveryFixture(t, newTestSQLStore(t), "session-native-tool-cas", "run-native-tool-cas")
			if session, recovered, err := fixture.store.RecoverNativeQueuedCompletedToolResultFenced(context.Background(), fixture.fence, fixture.input.AuthorizationEpoch, fixture.version+1); session != nil || recovered || !errors.Is(err, core.ErrSessionConflict) {
				t.Fatalf("CAS session=%v recovered=%t err=%v", session, recovered, err)
			}
			assertFencedSessionVersion(t, fixture.fencedSQLFixture, fixture.version)
		})
	})

	t.Run("concurrent_A_and_response_lost_B_deliver_once", func(t *testing.T) {
		fixture := newNativeQueuedCompletedToolRecoveryFixture(t, newTestSQLStore(t), "session-native-tool-concurrent", "run-native-tool-concurrent")
		// SQLite has a single writer. Keeping one connection makes the test assert
		// the intended serialized A/B outcome instead of scheduler-dependent busy
		// errors, while the goroutines still exercise the public boundary.
		fixture.store.db.SetMaxOpenConns(1)
		assertNativeQueuedCompletedToolRecoveryConcurrent(t, fixture)
	})

	t.Run("cancellation_rolls_back_A", func(t *testing.T) {
		fixture := newNativeQueuedCompletedToolRecoveryFixture(t, newTestSQLStore(t), "session-native-tool-cancel", "run-native-tool-cancel")
		if _, err := fixture.store.db.ExecContext(context.Background(), `CREATE TRIGGER cancel_native_tool_recovery AFTER INSERT ON event_chunks
			WHEN NEW.session_id = 'session-native-tool-cancel'
			BEGIN UPDATE run_control SET cancel_requested = 1 WHERE run_id = 'run-native-tool-cancel'; END`); err != nil {
			t.Fatal(err)
		}
		if session, recovered, err := fixture.store.RecoverNativeQueuedCompletedToolResultFenced(context.Background(), fixture.fence, fixture.input.AuthorizationEpoch, fixture.version); session != nil || recovered || !errors.Is(err, ErrSessionWriteFenceLost) {
			t.Fatalf("cancel session=%v recovered=%t err=%v", session, recovered, err)
		}
		assertFencedSessionVersion(t, fixture.fencedSQLFixture, fixture.version)
		var sidecars int
		if err := fixture.store.db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM completed_tool_result_recovery_sidecars WHERE session_id = ?", fixture.fence.SessionID).Scan(&sidecars); err != nil || sidecars != 0 {
			t.Fatalf("rollback sidecars=%d err=%v", sidecars, err)
		}
	})
}

func TestPostgresSQLSessionStoreRecoverNativeQueuedCompletedToolResultFenced(t *testing.T) {
	ctx := newPostgresFenceTestContext(t)
	db := newPostgresTestDB(t)
	store, err := OpenSQLSessionStore(ctx, db, SQLDialectPostgres)
	if err != nil {
		t.Fatal(err)
	}
	t.Run("prune_accepts_v46_after_coalesced_base_chunk", func(t *testing.T) {
		assertNativeQueuedModelPruneAcceptsCoalescedBaseChunk(t, ctx, store, "session-pg-native-tool-prune-coalesced", "run-pg-native-tool-prune-coalesced")
	})
	fixture := newNativeQueuedCompletedToolRecoveryFixture(t, store, "session-pg-native-tool-recovery", "run-pg-native-tool-recovery")
	if session, recovered, err := store.RecoverNativeQueuedCompletedToolResultFenced(ctx, fixture.fence, fixture.input.AuthorizationEpoch, fixture.version); err != nil || !recovered || session == nil || session.Version() != fixture.version+1 {
		t.Fatalf("A session=%v recovered=%t err=%v", session, recovered, err)
	}
	assertNativeQueuedCompletedToolRecovery(t, fixture)
	if session, recovered, err := store.RecoverNativeQueuedCompletedToolResultFenced(ctx, fixture.fence, fixture.input.AuthorizationEpoch, fixture.version); err != nil || !recovered || session == nil || session.Version() != fixture.version+1 {
		t.Fatalf("B session=%v recovered=%t err=%v", session, recovered, err)
	}
	t.Run("generic_V3_without_v44", func(t *testing.T) {
		fixture := newCompletedToolResultRecoverySidecarFixture(t, store, "session-pg-native-generic-v3", "run-pg-native-generic-v3")
		if appended, err := store.AppendCompletedToolResultRecoverySidecarFenced(ctx, fixture.fence, fixture.version, fixture.input); err != nil || !appended {
			t.Fatalf("generic V3 append=%t err=%v", appended, err)
		}
		if session, recovered, err := store.RecoverNativeQueuedCompletedToolResultFenced(ctx, fixture.fence, fixture.input.AuthorizationEpoch, fixture.version+1); session != nil || recovered || !errors.Is(err, ErrCompletedToolResultProofInvalid) {
			t.Fatalf("generic V3 session=%v recovered=%t err=%v", session, recovered, err)
		}
	})
	t.Run("concurrent_A_and_response_lost_B_deliver_once", func(t *testing.T) {
		fixture := newNativeQueuedCompletedToolRecoveryFixture(t, store, "session-pg-native-tool-concurrent", "run-pg-native-tool-concurrent")
		assertNativeQueuedCompletedToolRecoveryConcurrent(t, fixture)
	})
}
