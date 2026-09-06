package storage

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
)

type completedToolResultFixture struct {
	*fencedSQLFixture
	invocation core.ToolInvocation
	result     core.CapabilityResult
	digest     string
}

func newSQLiteCompletedToolResultFixture(t *testing.T) *completedToolResultFixture {
	t.Helper()
	return newCompletedToolResultFixture(t, newTestSQLStore(t), "session-completed-result", "run-completed-result")
}

func newCompletedToolResultFixture(t *testing.T, store *SQLSessionStore, sessionID, runID string) *completedToolResultFixture {
	t.Helper()
	fixture := newFencedSQLFixture(t, store, sessionID, runID)
	call := core.ToolCall{ID: "call-completed-result", Name: "test.completed", Args: map[string]any{"input": "value"}}
	event, err := fixture.session.Append(runID, core.EvToolCall, core.ToolCallData{CallID: call.ID, Name: call.Name, Args: call.Args})
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.store.AppendEventsFenced(context.Background(), fixture.fence, event.Seq, []core.SessionEvent{event}); err != nil {
		t.Fatal(err)
	}
	principal := fixture.session.Principal()
	invocation, err := core.NewToolInvocation(core.RunInfo{RunID: runID, SessionID: sessionID, Principal: principal}, call, true)
	if err != nil {
		t.Fatal(err)
	}
	journal, err := NewSQLToolInvocationJournal(store.db, store.dialect)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := journal.BeginToolInvocation(context.Background(), invocation); err != nil {
		t.Fatal(err)
	}
	result := core.CapabilityResult{Content: "canonical completed result", OK: true, Metadata: map[string]any{"origin": "journal"}}
	if _, err := journal.CompleteToolInvocation(context.Background(), invocation, result); err != nil {
		t.Fatal(err)
	}
	digest, err := CanonicalCapabilityResultDigest(result)
	if err != nil {
		t.Fatal(err)
	}
	return &completedToolResultFixture{fencedSQLFixture: fixture, invocation: invocation, result: result, digest: digest}
}

func assertCompletedResultDurable(t *testing.T, fixture *completedToolResultFixture, wantVersion int64) core.SessionEvent {
	t.Helper()
	loaded := assertFencedSessionVersion(t, fixture.fencedSQLFixture, wantVersion)
	var matched []core.SessionEvent
	for _, event := range loaded.Events() {
		if event.RunID == fixture.fence.RunID && event.Type == core.EvToolResult {
			var data core.ToolResultData
			if err := json.Unmarshal(event.Data, &data); err != nil {
				t.Fatal(err)
			}
			if data.CallID == fixture.invocation.CallID {
				matched = append(matched, event)
				if data.Content != fixture.result.Content || data.OK != fixture.result.OK || !reflect.DeepEqual(data.Metadata, fixture.result.Metadata) {
					t.Fatalf("durable canonical result=%+v want=%+v", data, fixture.result)
				}
			}
		}
	}
	if len(matched) != 1 {
		t.Fatalf("durable canonical result events=%d want=1", len(matched))
	}
	return matched[0]
}

func assertCompletedResultNoWrite(t *testing.T, fixture *completedToolResultFixture, wantVersion int64) {
	t.Helper()
	assertFencedSessionVersion(t, fixture.fencedSQLFixture, wantVersion)
	var results int
	if err := fixture.store.db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM event_chunks WHERE session_id = ?`, fixture.fence.SessionID).Scan(&results); err != nil {
		t.Fatal(err)
	}
	if results != int(wantVersion) {
		t.Fatalf("event chunks=%d want one-event chunks for durable version %d", results, wantVersion)
	}
}

func assertCompletedJournalUnchanged(t *testing.T, fixture *completedToolResultFixture) {
	t.Helper()
	journal, err := NewSQLToolInvocationJournal(fixture.store.db, fixture.store.dialect)
	if err != nil {
		t.Fatal(err)
	}
	record, found, err := journal.GetToolInvocation(context.Background(), fixture.invocation)
	if err != nil || !found {
		t.Fatalf("read completed journal after rollback: found=%t err=%v", found, err)
	}
	if record.State != core.ToolInvocationCompleted || record.Result == nil || !reflect.DeepEqual(*record.Result, fixture.result) {
		t.Fatalf("journal after rollback=%+v want completed canonical result=%+v", record, fixture.result)
	}
	digest, err := CanonicalCapabilityResultDigest(*record.Result)
	if err != nil || digest != fixture.digest {
		t.Fatalf("journal digest after rollback=%q err=%v want=%q", digest, err, fixture.digest)
	}
}

func TestSQLSessionStoreAppendCompletedToolResultFencedSQLite(t *testing.T) {
	t.Run("success_indexes_evidence", func(t *testing.T) {
		fixture := newSQLiteCompletedToolResultFixture(t)
		event, appended, err := fixture.store.AppendCompletedToolResultFenced(context.Background(), fixture.fence, 2, fixture.invocation, fixture.digest)
		if err != nil || !appended {
			t.Fatalf("append canonical result: appended=%t err=%v", appended, err)
		}
		if event.Seq != 2 || event.Time.IsZero() || event.RunID != fixture.fence.RunID || event.Type != core.EvToolResult {
			t.Fatalf("appended event=%+v", event)
		}
		assertCompletedResultDurable(t, fixture, 3)
		var status string
		if err := fixture.store.db.QueryRowContext(context.Background(), `SELECT status FROM run_evidence WHERE session_id = ? AND run_id = ?`, fixture.fence.SessionID, fixture.fence.RunID).Scan(&status); err != nil {
			t.Fatal(err)
		}
		if status != RunStatusRunning {
			t.Fatalf("run evidence status=%q want=%q", status, RunStatusRunning)
		}
	})

	t.Run("response_lost_retry_is_exact_noop", func(t *testing.T) {
		fixture := newSQLiteCompletedToolResultFixture(t)
		first, appended, err := fixture.store.AppendCompletedToolResultFenced(context.Background(), fixture.fence, 2, fixture.invocation, fixture.digest)
		if err != nil || !appended {
			t.Fatalf("first append: appended=%t err=%v", appended, err)
		}
		second, appended, err := fixture.store.AppendCompletedToolResultFenced(context.Background(), fixture.fence, 2, fixture.invocation, fixture.digest)
		if err != nil || appended {
			t.Fatalf("response-lost retry: appended=%t err=%v", appended, err)
		}
		if !reflect.DeepEqual(second, first) {
			t.Fatalf("response-lost event=%+v want=%+v", second, first)
		}
		assertCompletedResultDurable(t, fixture, 3)
	})

	t.Run("response_lost_retry_requires_a_live_fence", func(t *testing.T) {
		fixture := newSQLiteCompletedToolResultFixture(t)
		if _, appended, err := fixture.store.AppendCompletedToolResultFenced(context.Background(), fixture.fence, 2, fixture.invocation, fixture.digest); err != nil || !appended {
			t.Fatalf("initial append: appended=%t err=%v", appended, err)
		}
		if _, err := fixture.store.db.ExecContext(context.Background(), `UPDATE run_queue SET worker_id = 'worker-successor' WHERE run_id = ?`, fixture.fence.RunID); err != nil {
			t.Fatal(err)
		}
		_, appended, err := fixture.store.AppendCompletedToolResultFenced(context.Background(), fixture.fence, 2, fixture.invocation, fixture.digest)
		if appended || !errors.Is(err, ErrSessionWriteFenceLost) || errors.Is(err, ErrCompletedToolResultProofInvalid) {
			t.Fatalf("stale response-lost retry: appended=%t err=%v", appended, err)
		}
		assertCompletedResultDurable(t, fixture, 3)
		assertCompletedJournalUnchanged(t, fixture)
	})

	t.Run("identity_and_digest_proof_fail_closed", func(t *testing.T) {
		fixtures := []struct {
			name   string
			mutate func(*completedToolResultFixture) (core.ToolInvocation, string)
		}{
			{"capability", func(f *completedToolResultFixture) (core.ToolInvocation, string) {
				i := f.invocation
				i.CapabilityID = "test.other"
				return i, f.digest
			}},
			{"args", func(f *completedToolResultFixture) (core.ToolInvocation, string) {
				i := f.invocation
				i.ArgsDigest = strings.Repeat("a", 64)
				return i, f.digest
			}},
			{"idempotent", func(f *completedToolResultFixture) (core.ToolInvocation, string) {
				i := f.invocation
				i.Idempotent = false
				return i, f.digest
			}},
			{"fence_subject", func(f *completedToolResultFixture) (core.ToolInvocation, string) {
				i := f.invocation
				i.SubjectID = "other-subject"
				return i, f.digest
			}},
			{"result_digest", func(f *completedToolResultFixture) (core.ToolInvocation, string) {
				return f.invocation, strings.Repeat("b", 64)
			}},
		}
		for _, test := range fixtures {
			t.Run(test.name, func(t *testing.T) {
				fixture := newSQLiteCompletedToolResultFixture(t)
				invocation, digest := test.mutate(fixture)
				_, appended, err := fixture.store.AppendCompletedToolResultFenced(context.Background(), fixture.fence, 2, invocation, digest)
				if appended || !errors.Is(err, ErrCompletedToolResultProofInvalid) {
					t.Fatalf("proof failure: appended=%t err=%v", appended, err)
				}
				assertCompletedResultNoWrite(t, fixture, 2)
			})
		}
	})

	t.Run("missing_started_and_uncertain_are_indistinguishable_proof_failures", func(t *testing.T) {
		for _, state := range []string{"missing", "started", "uncertain"} {
			t.Run(state, func(t *testing.T) {
				fixture := newSQLiteCompletedToolResultFixture(t)
				if state == "missing" {
					if _, err := fixture.store.db.ExecContext(context.Background(), `DELETE FROM tool_invocations WHERE session_id = ? AND run_id = ? AND call_id = ?`, fixture.invocation.SessionID, fixture.invocation.RunID, fixture.invocation.CallID); err != nil {
						t.Fatal(err)
					}
				} else if state == "started" {
					if _, err := fixture.store.db.ExecContext(context.Background(), `UPDATE tool_invocations SET state = 'started', result_json = '', completed_at = 0 WHERE session_id = ? AND run_id = ? AND call_id = ?`, fixture.invocation.SessionID, fixture.invocation.RunID, fixture.invocation.CallID); err != nil {
						t.Fatal(err)
					}
				} else {
					if _, err := fixture.store.db.ExecContext(context.Background(), `UPDATE tool_invocations SET state = 'uncertain', result_json = '', completed_at = 0 WHERE session_id = ? AND run_id = ? AND call_id = ?`, fixture.invocation.SessionID, fixture.invocation.RunID, fixture.invocation.CallID); err != nil {
						t.Fatal(err)
					}
				}
				_, appended, err := fixture.store.AppendCompletedToolResultFenced(context.Background(), fixture.fence, 2, fixture.invocation, fixture.digest)
				if appended || !errors.Is(err, ErrCompletedToolResultProofInvalid) {
					t.Fatalf("state %s: appended=%t err=%v", state, appended, err)
				}
				assertCompletedResultNoWrite(t, fixture, 2)
			})
		}
	})

	t.Run("corrupt_durable_result_is_not_proof_failure", func(t *testing.T) {
		fixture := newSQLiteCompletedToolResultFixture(t)
		if _, err := fixture.store.db.ExecContext(context.Background(), `UPDATE tool_invocations SET result_json = 'null' WHERE session_id = ? AND run_id = ? AND call_id = ?`, fixture.invocation.SessionID, fixture.invocation.RunID, fixture.invocation.CallID); err != nil {
			t.Fatal(err)
		}
		_, appended, err := fixture.store.AppendCompletedToolResultFenced(context.Background(), fixture.fence, 2, fixture.invocation, fixture.digest)
		if appended || err == nil || errors.Is(err, ErrCompletedToolResultProofInvalid) || errors.Is(err, core.ErrSessionConflict) || errors.Is(err, ErrSessionWriteFenceLost) {
			t.Fatalf("corrupt journal classification: appended=%t err=%v", appended, err)
		}
		assertCompletedResultNoWrite(t, fixture, 2)
	})

	t.Run("wrong_durable_tool_call_is_proof_failure", func(t *testing.T) {
		for _, data := range []core.ToolCallData{
			{CallID: "call-completed-result", Name: "test.other", Args: map[string]any{"input": "value"}},
			{CallID: "call-completed-result", Name: "test.completed", Args: map[string]any{"input": "other"}},
		} {
			t.Run(data.Name+"/"+data.Args["input"].(string), func(t *testing.T) {
				fixture := newSQLiteCompletedToolResultFixture(t)
				data.CallID = fixture.invocation.CallID
				encoded, err := json.Marshal(data)
				if err != nil {
					t.Fatal(err)
				}
				payload, err := json.Marshal(core.SessionEvent{Seq: 1, Time: time.Now().UTC(), RunID: fixture.fence.RunID, Type: core.EvToolCall, Data: encoded})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := fixture.store.db.ExecContext(context.Background(), `UPDATE event_chunks SET payload = ? WHERE session_id = ? AND start_seq = 1`, string(payload)+"\n", fixture.fence.SessionID); err != nil {
					t.Fatal(err)
				}
				_, appended, err := fixture.store.AppendCompletedToolResultFenced(context.Background(), fixture.fence, 2, fixture.invocation, fixture.digest)
				if appended || !errors.Is(err, ErrCompletedToolResultProofInvalid) {
					t.Fatalf("wrong durable call: appended=%t err=%v", appended, err)
				}
				assertCompletedResultNoWrite(t, fixture, 2)
			})
		}
	})

	t.Run("duplicate_matching_durable_tool_call_is_proof_failure", func(t *testing.T) {
		fixture := newSQLiteCompletedToolResultFixture(t)
		loaded, err := fixture.store.Load(context.Background(), fixture.fence.SessionID)
		if err != nil {
			t.Fatal(err)
		}
		duplicate, err := loaded.Append(fixture.fence.RunID, core.EvToolCall, core.ToolCallData{
			CallID: fixture.invocation.CallID, Name: fixture.invocation.CapabilityID, Args: map[string]any{"input": "value"},
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := fixture.store.AppendEvents(context.Background(), fixture.fence.SessionID, 2, []core.SessionEvent{duplicate}); err != nil {
			t.Fatal(err)
		}
		_, appended, err := fixture.store.AppendCompletedToolResultFenced(context.Background(), fixture.fence, 3, fixture.invocation, fixture.digest)
		if appended || !errors.Is(err, ErrCompletedToolResultProofInvalid) {
			t.Fatalf("duplicate durable call: appended=%t err=%v", appended, err)
		}
		assertCompletedResultNoWrite(t, fixture, 3)
	})

	t.Run("fence_and_version_failures_do_not_write", func(t *testing.T) {
		cases := []struct {
			name    string
			mutate  func(*completedToolResultFixture)
			wantIs  error
			version int64
		}{
			{"worker", func(f *completedToolResultFixture) {
				_, _ = f.store.db.ExecContext(context.Background(), `UPDATE run_queue SET worker_id = 'worker-other' WHERE run_id = ?`, f.fence.RunID)
			}, ErrSessionWriteFenceLost, 2},
			{"generation", func(f *completedToolResultFixture) {
				_, _ = f.store.db.ExecContext(context.Background(), `UPDATE run_queue SET generation = generation + 1 WHERE run_id = ?`, f.fence.RunID)
			}, ErrSessionWriteFenceLost, 2},
			{"queue_lease", func(f *completedToolResultFixture) {
				_, _ = f.store.db.ExecContext(context.Background(), `UPDATE run_queue SET lease_expires_at = 0 WHERE run_id = ?`, f.fence.RunID)
			}, ErrSessionWriteFenceLost, 2},
			{"cancel", func(f *completedToolResultFixture) {
				_, _ = f.store.db.ExecContext(context.Background(), `UPDATE run_control SET cancel_requested = 1 WHERE run_id = ?`, f.fence.RunID)
			}, ErrSessionWriteFenceLost, 2},
			{"lease_holder", func(f *completedToolResultFixture) {
				_, _ = f.store.db.ExecContext(context.Background(), `UPDATE session_leases SET holder = 'other-holder' WHERE session_id = ?`, f.fence.SessionID)
			}, ErrSessionWriteFenceLost, 2},
			{"session_lease", func(f *completedToolResultFixture) {
				_, _ = f.store.db.ExecContext(context.Background(), `UPDATE session_leases SET expires_at = 0 WHERE session_id = ?`, f.fence.SessionID)
			}, ErrSessionWriteFenceLost, 2},
			{"version", func(f *completedToolResultFixture) {
				event := rawFencedEvent(t, 2, f.fence.RunID, "unrelated")
				if err := f.store.AppendEvents(context.Background(), f.fence.SessionID, 2, []core.SessionEvent{event}); err != nil {
					t.Fatal(err)
				}
			}, core.ErrSessionConflict, 3},
		}
		for _, test := range cases {
			t.Run(test.name, func(t *testing.T) {
				fixture := newSQLiteCompletedToolResultFixture(t)
				test.mutate(fixture)
				_, appended, err := fixture.store.AppendCompletedToolResultFenced(context.Background(), fixture.fence, 2, fixture.invocation, fixture.digest)
				if appended || !errors.Is(err, test.wantIs) {
					t.Fatalf("%s: appended=%t err=%v", test.name, appended, err)
				}
				assertCompletedResultNoWrite(t, fixture, test.version)
			})
		}
	})

	t.Run("final_fence_recheck_rolls_back_chunk_and_evidence", func(t *testing.T) {
		fixture := newSQLiteCompletedToolResultFixture(t)
		trigger := "completed_result_change_owner_after_chunk"
		if _, err := fixture.store.db.ExecContext(context.Background(), `CREATE TRIGGER `+trigger+`
			AFTER INSERT ON event_chunks WHEN NEW.session_id = '`+fixture.fence.SessionID+`'
			BEGIN
				UPDATE run_queue SET worker_id = 'worker-triggered' WHERE run_id = '`+fixture.fence.RunID+`';
			END`); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _, _ = fixture.store.db.ExecContext(context.Background(), "DROP TRIGGER IF EXISTS "+trigger) })
		_, appended, err := fixture.store.AppendCompletedToolResultFenced(context.Background(), fixture.fence, 2, fixture.invocation, fixture.digest)
		if appended || !errors.Is(err, ErrSessionWriteFenceLost) {
			t.Fatalf("triggered fence loss: appended=%t err=%v", appended, err)
		}
		assertCompletedResultNoWrite(t, fixture, 2)
		var worker, status string
		if err := fixture.store.db.QueryRowContext(context.Background(), `SELECT worker_id FROM run_queue WHERE run_id = ?`, fixture.fence.RunID).Scan(&worker); err != nil {
			t.Fatal(err)
		}
		if err := fixture.store.db.QueryRowContext(context.Background(), `SELECT status FROM run_evidence WHERE session_id = ? AND run_id = ?`, fixture.fence.SessionID, fixture.fence.RunID).Scan(&status); err != nil {
			t.Fatal(err)
		}
		if worker != fixture.fence.WorkerID || status != RunStatusRunning {
			t.Fatalf("rollback worker=%q status=%q", worker, status)
		}
	})

	t.Run("final_journal_recheck_rolls_back_chunk_evidence_tip_and_trigger", func(t *testing.T) {
		cases := []struct {
			name      string
			update    string
			proofFail bool
		}{
			{
				name:      "state_becomes_uncertain",
				update:    "UPDATE tool_invocations SET state = 'uncertain', result_json = '', completed_at = 0",
				proofFail: true,
			},
			{
				name:      "canonical_result_changes",
				update:    `UPDATE tool_invocations SET result_json = '{"content":"trigger replacement","ok":true,"metadata":{"origin":"trigger"}}'`,
				proofFail: true,
			},
			{
				name:   "canonical_result_becomes_corrupt",
				update: "UPDATE tool_invocations SET result_json = 'null'",
			},
		}
		for _, test := range cases {
			t.Run(test.name, func(t *testing.T) {
				fixture := newSQLiteCompletedToolResultFixture(t)
				trigger := "completed_result_change_journal_after_chunk"
				statement := `CREATE TRIGGER ` + trigger + ` AFTER INSERT ON event_chunks
					WHEN NEW.session_id = '` + fixture.fence.SessionID + `'
					BEGIN ` + test.update + ` WHERE session_id = '` + fixture.invocation.SessionID + `'
						AND run_id = '` + fixture.invocation.RunID + `' AND call_id = '` + fixture.invocation.CallID + `'; END`
				if _, err := fixture.store.db.ExecContext(context.Background(), statement); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _, _ = fixture.store.db.ExecContext(context.Background(), "DROP TRIGGER IF EXISTS "+trigger) })
				_, appended, err := fixture.store.AppendCompletedToolResultFenced(context.Background(), fixture.fence, 2, fixture.invocation, fixture.digest)
				if test.proofFail {
					if appended || !errors.Is(err, ErrCompletedToolResultProofInvalid) {
						t.Fatalf("triggered journal proof change: appended=%t err=%v", appended, err)
					}
				} else if appended || err == nil || errors.Is(err, ErrCompletedToolResultProofInvalid) || errors.Is(err, ErrSessionWriteFenceLost) || errors.Is(err, core.ErrSessionConflict) {
					t.Fatalf("triggered journal corruption classification: appended=%t err=%v", appended, err)
				}
				assertCompletedResultNoWrite(t, fixture, 2)
				assertCompletedJournalUnchanged(t, fixture)
				var status string
				if err := fixture.store.db.QueryRowContext(context.Background(), `SELECT status FROM run_evidence WHERE session_id = ? AND run_id = ?`, fixture.fence.SessionID, fixture.fence.RunID).Scan(&status); err != nil {
					t.Fatal(err)
				}
				if status != RunStatusRunning {
					t.Fatalf("run evidence status after rollback=%q want=%q", status, RunStatusRunning)
				}
			})
		}
	})

	t.Run("duplicate_or_different_durable_result_fails_closed", func(t *testing.T) {
		for _, content := range []string{"canonical completed result", "different result"} {
			t.Run(content, func(t *testing.T) {
				fixture := newSQLiteCompletedToolResultFixture(t)
				if _, appended, err := fixture.store.AppendCompletedToolResultFenced(context.Background(), fixture.fence, 2, fixture.invocation, fixture.digest); err != nil || !appended {
					t.Fatalf("initial append: appended=%t err=%v", appended, err)
				}
				duplicate, err := fixture.store.Load(context.Background(), fixture.fence.SessionID)
				if err != nil {
					t.Fatal(err)
				}
				event, err := duplicate.Append(fixture.fence.RunID, core.EvToolResult, core.ToolResultData{CallID: fixture.invocation.CallID, Content: content, OK: true, Metadata: map[string]any{"origin": "journal"}})
				if err != nil {
					t.Fatal(err)
				}
				if err := fixture.store.AppendEvents(context.Background(), fixture.fence.SessionID, 3, []core.SessionEvent{event}); err != nil {
					t.Fatal(err)
				}
				_, appended, err := fixture.store.AppendCompletedToolResultFenced(context.Background(), fixture.fence, 2, fixture.invocation, fixture.digest)
				if appended || !errors.Is(err, ErrCompletedToolResultProofInvalid) {
					t.Fatalf("duplicate/different retry: appended=%t err=%v", appended, err)
				}
				assertFencedSessionVersion(t, fixture.fencedSQLFixture, 4)
			})
		}
	})
}

func TestFencedCompletedToolResultPostgresBinding(t *testing.T) {
	query := sqlFencePostgresLockToolInvocation.bind(SQLDialectPostgres)
	if !strings.Contains(query, "FOR UPDATE") {
		t.Fatalf("postgres journal lock query lacks FOR UPDATE: %s", query)
	}
	for index := 1; index <= 8; index++ {
		if !strings.Contains(query, "$"+string(rune('0'+index))) {
			t.Fatalf("postgres journal lock query misses placeholder $%d: %s", index, query)
		}
	}
	if !strings.Contains(fencedSessionTipUpdate(SQLDialectPostgres), "clock_timestamp()") {
		t.Fatal("postgres final fence recheck does not use database time")
	}
}

func TestPostgresSQLSessionStoreAppendCompletedToolResultFenced(t *testing.T) {
	ctx := newPostgresFenceTestContext(t)
	db := newPostgresTestDB(t)
	store, err := OpenSQLSessionStore(ctx, db, SQLDialectPostgres)
	if err != nil {
		t.Fatal(err)
	}
	fixture := newCompletedToolResultFixture(t, store, "session-pg-completed-result", "run-pg-completed-result")
	event, appended, err := fixture.store.AppendCompletedToolResultFenced(ctx, fixture.fence, 2, fixture.invocation, fixture.digest)
	if err != nil || !appended || event.Type != core.EvToolResult {
		t.Fatalf("postgres completed result: event=%+v appended=%t err=%v", event, appended, err)
	}
	assertCompletedResultDurable(t, fixture, 3)
}
