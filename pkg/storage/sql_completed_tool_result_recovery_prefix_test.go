package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
)

type completedToolResultRecoveryFixture struct {
	*fencedSQLFixture
	invocation core.ToolInvocation
	result     core.CapabilityResult
	prefix     CompletedToolResultRecoveryPrefix
	version    int64
}

func newSQLiteCompletedToolResultRecoveryFixture(t *testing.T) *completedToolResultRecoveryFixture {
	t.Helper()
	return newCompletedToolResultRecoveryFixture(t, newTestSQLStore(t), "session-completed-result-recovery", "run-completed-result-recovery")
}

func newCompletedToolResultRecoveryFixture(t *testing.T, store *SQLSessionStore, sessionID, runID string) *completedToolResultRecoveryFixture {
	t.Helper()
	fixture := newFencedSQLFixture(t, store, sessionID, runID)
	appendDurable := func(kind core.SessionEventType, data any) core.SessionEvent {
		event, err := fixture.session.Append(runID, kind, data)
		if err != nil {
			t.Fatal(err)
		}
		if err := fixture.store.AppendEventsFenced(context.Background(), fixture.fence, event.Seq, []core.SessionEvent{event}); err != nil {
			t.Fatal(err)
		}
		return event
	}
	call := core.ToolCall{ID: "call-completed-result-recovery", Name: "test.completed.recovery", Args: map[string]any{"input": "value"}}
	step := appendDurable(core.EvStepStart, core.StepData{Index: 0})
	appendDurable(core.EvAssistantMessage, core.AssistantMessageData{Text: "tools", ToolCall: &call, ToolCalls: []core.ToolCall{call}})
	appendDurable(core.EvRunUsage, core.RunUsageData{InvocationID: fmt.Sprintf("model:%d", step.Seq)})
	appendDurable(core.EvToolCall, core.ToolCallData{CallID: call.ID, Name: call.Name, Args: call.Args})
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
	result := core.CapabilityResult{Content: "canonical completed recovery result", OK: true, Metadata: map[string]any{"origin": "journal"}}
	if _, err := journal.CompleteToolInvocation(context.Background(), invocation, result); err != nil {
		t.Fatal(err)
	}
	digest, err := CanonicalCapabilityResultDigest(result)
	if err != nil {
		t.Fatal(err)
	}
	epoch, err := fixture.store.AuthorizationEpoch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	metadata := map[string]string{
		recoveryProtocolMetadataKey: recoveryProtocolV1,
		recoveryCallIDMetadataKey:   call.ID,
		recoveryResultMetadataKey:   digest,
		recoveryStepMetadataKey:     fmt.Sprintf("%d", step.Seq),
		recoveryEpochMetadataKey:    fmt.Sprintf("%d", epoch),
	}
	composition := &core.RunCompositionData{Profile: core.AgentProfileSnapshot{ID: "profile-completed-result-recovery", ProfileID: fixture.session.ProfileID(), Scope: fixture.session.Scope()}, Metadata: metadata}
	compositionRevision, err := core.CompositionRevision(composition)
	if err != nil {
		t.Fatal(err)
	}
	assignmentRevision, err := core.CompositionMetadataRevision(metadata)
	if err != nil {
		t.Fatal(err)
	}
	prefix := CompletedToolResultRecoveryPrefix{Resume: core.RunResumeData{
		ProfileSnapshotID: composition.Profile.ID, CapabilitySnapshotID: "capabilities-completed-result-recovery",
		CompositionRevision: compositionRevision, AssignmentRevision: assignmentRevision, Composition: composition,
	}, Invocation: invocation, ExpectedResultDigest: digest, ExpectedAuthorizationEpoch: epoch}
	return &completedToolResultRecoveryFixture{fencedSQLFixture: fixture, invocation: invocation, result: result, prefix: prefix, version: fixture.session.Version()}
}

func assertCompletedToolResultRecoveryPrefix(t *testing.T, fixture *completedToolResultRecoveryFixture) {
	t.Helper()
	loaded := assertFencedSessionVersion(t, fixture.fencedSQLFixture, fixture.version+2)
	events := loaded.Events()
	resume, result := events[fixture.version], events[fixture.version+1]
	if resume.Type != core.EvRunResume || result.Type != core.EvToolResult || resume.RunID != fixture.fence.RunID || result.RunID != fixture.fence.RunID {
		t.Fatalf("recovery prefix events=(%+v, %+v)", resume, result)
	}
	var resumeData core.RunResumeData
	var resultData core.ToolResultData
	if err := json.Unmarshal(resume.Data, &resumeData); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(result.Data, &resultData); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(resumeData, fixture.prefix.Resume) || resultData.CallID != fixture.invocation.CallID || resultData.Content != fixture.result.Content || resultData.OK != fixture.result.OK || !reflect.DeepEqual(resultData.Metadata, fixture.result.Metadata) {
		t.Fatalf("durable recovery prefix resume=%+v result=%+v", resumeData, resultData)
	}
	if resumeData.Composition == nil || resumeData.Composition.Metadata[recoveryEpochMetadataKey] != fmt.Sprintf("%d", fixture.prefix.ExpectedAuthorizationEpoch) {
		t.Fatalf("durable recovery prefix authorization epoch=%q want=%d", resumeData.Composition.Metadata[recoveryEpochMetadataKey], fixture.prefix.ExpectedAuthorizationEpoch)
	}
	var payload string
	if err := fixture.store.db.QueryRowContext(context.Background(), `SELECT payload FROM event_chunks WHERE session_id = ? AND start_seq = ?`, fixture.fence.SessionID, fixture.version).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	if lines := strings.Count(strings.TrimSuffix(payload, "\n"), "\n") + 1; lines != 2 {
		t.Fatalf("recovery prefix chunk lines=%d want=2 payload=%q", lines, payload)
	}
}

func assertCompletedToolResultRecoveryNoWrite(t *testing.T, fixture *completedToolResultRecoveryFixture) {
	t.Helper()
	assertFencedSessionVersion(t, fixture.fencedSQLFixture, fixture.version)
	var chunks int
	if err := fixture.store.db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM event_chunks WHERE session_id = ?`, fixture.fence.SessionID).Scan(&chunks); err != nil {
		t.Fatal(err)
	}
	if chunks != int(fixture.version) {
		t.Fatalf("event chunks=%d want=%d", chunks, fixture.version)
	}
	var evidenceCount int
	if err := fixture.store.db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM run_evidence WHERE session_id = ?`, fixture.fence.SessionID).Scan(&evidenceCount); err != nil {
		t.Fatal(err)
	}
	if evidenceCount != 1 {
		t.Fatalf("run evidence records=%d want=1", evidenceCount)
	}
	var status string
	if err := fixture.store.db.QueryRowContext(context.Background(), `SELECT status FROM run_evidence WHERE session_id = ? AND run_id = ? AND segment_seq = 0`, fixture.fence.SessionID, fixture.fence.RunID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != RunStatusRunning {
		t.Fatalf("run evidence status=%q want=%q", status, RunStatusRunning)
	}
}

func cloneCompletedToolResultRecoveryPrefix(prefix CompletedToolResultRecoveryPrefix) CompletedToolResultRecoveryPrefix {
	clone := prefix
	if prefix.Resume.Composition == nil {
		return clone
	}
	composition := *prefix.Resume.Composition
	composition.Metadata = make(map[string]string, len(prefix.Resume.Composition.Metadata))
	for key, value := range prefix.Resume.Composition.Metadata {
		composition.Metadata[key] = value
	}
	clone.Resume.Composition = &composition
	return clone
}

func refreshCompletedToolResultRecoveryPrefixRevisions(t *testing.T, prefix *CompletedToolResultRecoveryPrefix) {
	t.Helper()
	compositionRevision, err := core.CompositionRevision(prefix.Resume.Composition)
	if err != nil {
		t.Fatal(err)
	}
	assignmentRevision, err := core.CompositionMetadataRevision(prefix.Resume.Composition.Metadata)
	if err != nil {
		t.Fatal(err)
	}
	prefix.Resume.CompositionRevision = compositionRevision
	prefix.Resume.AssignmentRevision = assignmentRevision
}

func TestSQLSessionStoreAppendCompletedToolResultRecoveryPrefixFencedSQLite(t *testing.T) {
	t.Run("success_and_response_lost", func(t *testing.T) {
		fixture := newSQLiteCompletedToolResultRecoveryFixture(t)
		appended, err := fixture.store.AppendCompletedToolResultRecoveryPrefixFenced(context.Background(), fixture.fence, fixture.version, fixture.prefix)
		if err != nil || !appended {
			t.Fatalf("append recovery prefix: appended=%t err=%v", appended, err)
		}
		assertCompletedToolResultRecoveryPrefix(t, fixture)
		appended, err = fixture.store.AppendCompletedToolResultRecoveryPrefixFenced(context.Background(), fixture.fence, fixture.version, fixture.prefix)
		if err != nil || appended {
			t.Fatalf("response-lost retry: appended=%t err=%v", appended, err)
		}
		assertCompletedToolResultRecoveryPrefix(t, fixture)

		if _, err := fixture.store.db.ExecContext(context.Background(), `UPDATE run_queue SET generation = generation + 1 WHERE run_id = ?`, fixture.fence.RunID); err != nil {
			t.Fatal(err)
		}
		appended, err = fixture.store.AppendCompletedToolResultRecoveryPrefixFenced(context.Background(), fixture.fence, fixture.version, fixture.prefix)
		if appended || !errors.Is(err, ErrSessionWriteFenceLost) {
			t.Fatalf("response-lost retry with stale fence: appended=%t err=%v", appended, err)
		}
		assertCompletedToolResultRecoveryPrefix(t, fixture)
	})

	t.Run("authorization_epoch_fences_initial_and_response_lost", func(t *testing.T) {
		t.Run("initial_mismatch", func(t *testing.T) {
			fixture := newSQLiteCompletedToolResultRecoveryFixture(t)
			bindings, err := NewSQLBindingJournal(fixture.store.db, fixture.store.dialect)
			if err != nil {
				t.Fatal(err)
			}
			if err := bindings.Record(context.Background(), BindingRecord{ID: "recovery-prefix-epoch-initial", Kind: "policy", Payload: []byte(`{"scope":"global"}`)}); err != nil {
				t.Fatal(err)
			}
			appended, err := fixture.store.AppendCompletedToolResultRecoveryPrefixFenced(context.Background(), fixture.fence, fixture.version, fixture.prefix)
			if appended || !errors.Is(err, ErrAuthorizationEpochChanged) {
				t.Fatalf("epoch mismatch: appended=%t err=%v", appended, err)
			}
			assertCompletedToolResultRecoveryNoWrite(t, fixture)
		})

		t.Run("response_lost_requires_live_epoch", func(t *testing.T) {
			fixture := newSQLiteCompletedToolResultRecoveryFixture(t)
			appended, err := fixture.store.AppendCompletedToolResultRecoveryPrefixFenced(context.Background(), fixture.fence, fixture.version, fixture.prefix)
			if err != nil || !appended {
				t.Fatalf("initial prefix: appended=%t err=%v", appended, err)
			}
			bindings, err := NewSQLBindingJournal(fixture.store.db, fixture.store.dialect)
			if err != nil {
				t.Fatal(err)
			}
			if err := bindings.Record(context.Background(), BindingRecord{ID: "recovery-prefix-epoch-response-lost", Kind: "policy", Payload: []byte(`{"scope":"global"}`)}); err != nil {
				t.Fatal(err)
			}
			appended, err = fixture.store.AppendCompletedToolResultRecoveryPrefixFenced(context.Background(), fixture.fence, fixture.version, fixture.prefix)
			if appended || !errors.Is(err, ErrAuthorizationEpochChanged) {
				t.Fatalf("response-lost epoch mismatch: appended=%t err=%v", appended, err)
			}
			assertCompletedToolResultRecoveryPrefix(t, fixture)
		})
	})

	t.Run("marker_digest_step_epoch_and_version_fail_closed", func(t *testing.T) {
		cases := []struct {
			name    string
			refresh bool
			mutate  func(*CompletedToolResultRecoveryPrefix)
		}{
			{"unknown_marker", true, func(prefix *CompletedToolResultRecoveryPrefix) {
				prefix.Resume.Composition.Metadata[recoveryMetadataPrefix+"unknown"] = "x"
			}},
			{"wrong_call", true, func(prefix *CompletedToolResultRecoveryPrefix) {
				prefix.Resume.Composition.Metadata[recoveryCallIDMetadataKey] = "other-call"
			}},
			{"wrong_digest", true, func(prefix *CompletedToolResultRecoveryPrefix) {
				prefix.Resume.Composition.Metadata[recoveryResultMetadataKey] = strings.Repeat("a", 64)
			}},
			{"wrong_step", true, func(prefix *CompletedToolResultRecoveryPrefix) {
				prefix.Resume.Composition.Metadata[recoveryStepMetadataKey] = "2"
			}},
			{"noncanonical_epoch", true, func(prefix *CompletedToolResultRecoveryPrefix) {
				prefix.Resume.Composition.Metadata[recoveryEpochMetadataKey] = "01"
			}},
			{"missing_epoch", true, func(prefix *CompletedToolResultRecoveryPrefix) {
				delete(prefix.Resume.Composition.Metadata, recoveryEpochMetadataKey)
			}},
			{"request_epoch_mismatch", false, func(prefix *CompletedToolResultRecoveryPrefix) { prefix.ExpectedAuthorizationEpoch++ }},
			{"negative_request_epoch", false, func(prefix *CompletedToolResultRecoveryPrefix) { prefix.ExpectedAuthorizationEpoch = -1 }},
			{"digest_argument", false, func(prefix *CompletedToolResultRecoveryPrefix) { prefix.ExpectedResultDigest = strings.Repeat("a", 64) }},
		}
		for _, test := range cases {
			t.Run(test.name, func(t *testing.T) {
				fixture := newSQLiteCompletedToolResultRecoveryFixture(t)
				prefix := cloneCompletedToolResultRecoveryPrefix(fixture.prefix)
				test.mutate(&prefix)
				if test.refresh {
					refreshCompletedToolResultRecoveryPrefixRevisions(t, &prefix)
				}
				appended, err := fixture.store.AppendCompletedToolResultRecoveryPrefixFenced(context.Background(), fixture.fence, fixture.version, prefix)
				if appended || !errors.Is(err, ErrCompletedToolResultProofInvalid) {
					t.Fatalf("%s: appended=%t err=%v", test.name, appended, err)
				}
				assertCompletedToolResultRecoveryNoWrite(t, fixture)
			})
		}
		fixture := newSQLiteCompletedToolResultRecoveryFixture(t)
		appended, err := fixture.store.AppendCompletedToolResultRecoveryPrefixFenced(context.Background(), fixture.fence, fixture.version+3, fixture.prefix)
		if appended || !errors.Is(err, core.ErrSessionConflict) {
			t.Fatalf("version conflict: appended=%t err=%v", appended, err)
		}
		assertCompletedToolResultRecoveryNoWrite(t, fixture)
	})

	t.Run("missing_invalid_and_exhausted_epoch_do_not_misclassify", func(t *testing.T) {
		for _, test := range []struct {
			name    string
			prepare func(t *testing.T, fixture *completedToolResultRecoveryFixture)
		}{
			{name: "missing", prepare: func(t *testing.T, fixture *completedToolResultRecoveryFixture) {
				t.Helper()
				if _, err := fixture.store.db.ExecContext(context.Background(), "DELETE FROM store_meta WHERE key = 'authorization_epoch'"); err != nil {
					t.Fatal(err)
				}
			}},
			{name: "corrupt", prepare: func(t *testing.T, fixture *completedToolResultRecoveryFixture) {
				t.Helper()
				if _, err := fixture.store.db.ExecContext(context.Background(), "UPDATE store_meta SET value = 'not-an-epoch' WHERE key = 'authorization_epoch'"); err != nil {
					t.Fatal(err)
				}
			}},
			{name: "exhausted", prepare: func(t *testing.T, fixture *completedToolResultRecoveryFixture) {
				t.Helper()
				fixture.prefix.ExpectedAuthorizationEpoch = maxAuthorizationEpoch
				fixture.prefix.Resume.Composition.Metadata[recoveryEpochMetadataKey] = fmt.Sprintf("%d", maxAuthorizationEpoch)
				refreshCompletedToolResultRecoveryPrefixRevisions(t, &fixture.prefix)
				if _, err := fixture.store.db.ExecContext(context.Background(), "UPDATE store_meta SET value = ? WHERE key = 'authorization_epoch'", fmt.Sprintf("%d", maxAuthorizationEpoch)); err != nil {
					t.Fatal(err)
				}
			}},
		} {
			t.Run(test.name, func(t *testing.T) {
				fixture := newSQLiteCompletedToolResultRecoveryFixture(t)
				test.prepare(t, fixture)
				appended, err := fixture.store.AppendCompletedToolResultRecoveryPrefixFenced(context.Background(), fixture.fence, fixture.version, fixture.prefix)
				if appended || err == nil || errors.Is(err, ErrAuthorizationEpochChanged) || errors.Is(err, ErrCompletedToolResultProofInvalid) || errors.Is(err, ErrSessionWriteFenceLost) || errors.Is(err, core.ErrSessionConflict) {
					t.Fatalf("%s epoch error: appended=%t err=%v", test.name, appended, err)
				}
				assertCompletedToolResultRecoveryNoWrite(t, fixture)
			})
		}
	})

	t.Run("resume_evidence_is_self_consistent", func(t *testing.T) {
		for _, test := range []struct {
			name   string
			mutate func(*CompletedToolResultRecoveryPrefix)
		}{
			{"profile_snapshot", func(prefix *CompletedToolResultRecoveryPrefix) { prefix.Resume.ProfileSnapshotID = "other-profile" }},
			{"capability_snapshot", func(prefix *CompletedToolResultRecoveryPrefix) { prefix.Resume.CapabilitySnapshotID = "" }},
			{"composition_revision", func(prefix *CompletedToolResultRecoveryPrefix) {
				prefix.Resume.CompositionRevision = strings.Repeat("0", 64)
			}},
			{"assignment_revision", func(prefix *CompletedToolResultRecoveryPrefix) {
				prefix.Resume.AssignmentRevision = strings.Repeat("0", 64)
			}},
		} {
			t.Run(test.name, func(t *testing.T) {
				fixture := newSQLiteCompletedToolResultRecoveryFixture(t)
				prefix := cloneCompletedToolResultRecoveryPrefix(fixture.prefix)
				test.mutate(&prefix)
				appended, err := fixture.store.AppendCompletedToolResultRecoveryPrefixFenced(context.Background(), fixture.fence, fixture.version, prefix)
				if appended || !errors.Is(err, ErrCompletedToolResultProofInvalid) {
					t.Fatalf("%s: appended=%t err=%v", test.name, appended, err)
				}
				assertCompletedToolResultRecoveryNoWrite(t, fixture)
			})
		}
	})

	t.Run("nonzero_epoch_is_durable_evidence", func(t *testing.T) {
		fixture := newSQLiteCompletedToolResultRecoveryFixture(t)
		prefix := cloneCompletedToolResultRecoveryPrefix(fixture.prefix)
		originalAssignmentRevision := prefix.Resume.AssignmentRevision
		prefix.Resume.Composition.Metadata[recoveryEpochMetadataKey] = "7"
		prefix.ExpectedAuthorizationEpoch = 7
		refreshCompletedToolResultRecoveryPrefixRevisions(t, &prefix)
		if prefix.Resume.AssignmentRevision == originalAssignmentRevision {
			t.Fatal("authorization epoch marker was not included in assignment revision")
		}
		if _, err := fixture.store.db.ExecContext(context.Background(), "UPDATE store_meta SET value = '7' WHERE key = 'authorization_epoch'"); err != nil {
			t.Fatal(err)
		}
		appended, err := fixture.store.AppendCompletedToolResultRecoveryPrefixFenced(context.Background(), fixture.fence, fixture.version, prefix)
		if err != nil || !appended {
			t.Fatalf("append nonzero epoch evidence: appended=%t err=%v", appended, err)
		}
		fixture.prefix = prefix
		assertCompletedToolResultRecoveryPrefix(t, fixture)
	})

	t.Run("result_only_and_fence_loss_do_not_converge", func(t *testing.T) {
		fixture := newSQLiteCompletedToolResultRecoveryFixture(t)
		if _, appended, err := fixture.store.AppendCompletedToolResultFenced(context.Background(), fixture.fence, fixture.version, fixture.invocation, fixture.prefix.ExpectedResultDigest); err != nil || !appended {
			t.Fatalf("write V1 result: appended=%t err=%v", appended, err)
		}
		appended, err := fixture.store.AppendCompletedToolResultRecoveryPrefixFenced(context.Background(), fixture.fence, fixture.version, fixture.prefix)
		if appended || !errors.Is(err, ErrCompletedToolResultProofInvalid) {
			t.Fatalf("result-only response-lost: appended=%t err=%v", appended, err)
		}
		fenceFixture := newSQLiteCompletedToolResultRecoveryFixture(t)
		fence := fenceFixture.fence
		fence.QueueGeneration++
		appended, err = fenceFixture.store.AppendCompletedToolResultRecoveryPrefixFenced(context.Background(), fence, fenceFixture.version, fenceFixture.prefix)
		if appended || !errors.Is(err, ErrSessionWriteFenceLost) {
			t.Fatalf("fence loss: appended=%t err=%v", appended, err)
		}
		assertCompletedToolResultRecoveryNoWrite(t, fenceFixture)
	})

	t.Run("chunk_trigger_mutations_roll_back", func(t *testing.T) {
		cases := []struct {
			name    string
			body    string
			want    error
			corrupt bool
		}{
			{name: "journal", body: `UPDATE tool_invocations SET state = 'uncertain', result_json = '', completed_at = 0 WHERE session_id = '` + "SESSION" + `' AND run_id = '` + "RUN" + `' AND call_id = '` + "CALL" + `';`, want: ErrCompletedToolResultProofInvalid},
			{name: "journal_corrupt", body: `UPDATE tool_invocations SET result_json = 'null' WHERE session_id = '` + "SESSION" + `' AND run_id = '` + "RUN" + `' AND call_id = '` + "CALL" + `';`, corrupt: true},
			{name: "fence", body: `UPDATE run_queue SET worker_id = 'worker-triggered' WHERE run_id = '` + "RUN" + `';`, want: ErrSessionWriteFenceLost},
			{name: "resume", body: `UPDATE event_chunks SET payload = REPLACE(payload, '"type":"run/resume"', '"type":"run/error"') WHERE session_id = NEW.session_id AND start_seq = NEW.start_seq;`, want: ErrCompletedToolResultProofInvalid},
			{name: "result", body: `UPDATE event_chunks SET payload = REPLACE(payload, 'canonical completed recovery result', 'trigger replacement') WHERE session_id = NEW.session_id AND start_seq = NEW.start_seq;`, want: ErrCompletedToolResultProofInvalid},
			{name: "authorization_epoch", body: `UPDATE store_meta SET value = CAST(CAST(value AS BIGINT) + 1 AS TEXT) WHERE key = 'authorization_epoch';`, want: ErrAuthorizationEpochChanged},
			{name: "authorization_epoch_corrupt", body: `UPDATE store_meta SET value = 'not-an-epoch' WHERE key = 'authorization_epoch';`, corrupt: true},
		}
		for _, test := range cases {
			t.Run(test.name, func(t *testing.T) {
				fixture := newSQLiteCompletedToolResultRecoveryFixture(t)
				body := strings.NewReplacer("SESSION", fixture.invocation.SessionID, "RUN", fixture.invocation.RunID, "CALL", fixture.invocation.CallID).Replace(test.body)
				trigger := "completed_result_recovery_change_after_chunk"
				statement := `CREATE TRIGGER ` + trigger + ` AFTER INSERT ON event_chunks WHEN NEW.session_id = '` + fixture.fence.SessionID + `' BEGIN ` + body + ` END`
				if _, err := fixture.store.db.ExecContext(context.Background(), statement); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _, _ = fixture.store.db.ExecContext(context.Background(), "DROP TRIGGER IF EXISTS "+trigger) })
				appended, err := fixture.store.AppendCompletedToolResultRecoveryPrefixFenced(context.Background(), fixture.fence, fixture.version, fixture.prefix)
				if test.corrupt {
					if appended || err == nil || errors.Is(err, ErrCompletedToolResultProofInvalid) || errors.Is(err, ErrSessionWriteFenceLost) || errors.Is(err, core.ErrSessionConflict) {
						t.Fatalf("%s corruption: appended=%t err=%v", test.name, appended, err)
					}
				} else if appended || !errors.Is(err, test.want) {
					t.Fatalf("%s mutation: appended=%t err=%v", test.name, appended, err)
				}
				assertCompletedToolResultRecoveryNoWrite(t, fixture)
				if test.name == "authorization_epoch" || test.name == "authorization_epoch_corrupt" {
					assertAuthorizationEpoch(t, fixture.store, fixture.prefix.ExpectedAuthorizationEpoch)
				}
				if test.name == "journal" || test.name == "journal_corrupt" {
					assertCompletedJournalUnchanged(t, &completedToolResultFixture{fencedSQLFixture: fixture.fencedSQLFixture, invocation: fixture.invocation, result: fixture.result, digest: fixture.prefix.ExpectedResultDigest})
				}
			})
		}
	})

	t.Run("evidence_trigger_mutation_rolls_back", func(t *testing.T) {
		fixture := newSQLiteCompletedToolResultRecoveryFixture(t)
		trigger := "completed_result_recovery_change_after_evidence"
		statement := `CREATE TRIGGER ` + trigger + ` AFTER UPDATE OF status ON run_evidence
			BEGIN UPDATE run_evidence SET status = 'failed' WHERE session_id = NEW.session_id AND run_id = NEW.run_id AND segment_seq = NEW.segment_seq; END`
		if _, err := fixture.store.db.ExecContext(context.Background(), statement); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _, _ = fixture.store.db.ExecContext(context.Background(), "DROP TRIGGER IF EXISTS "+trigger) })
		appended, err := fixture.store.AppendCompletedToolResultRecoveryPrefixFenced(context.Background(), fixture.fence, fixture.version, fixture.prefix)
		if appended || err == nil || errors.Is(err, ErrCompletedToolResultProofInvalid) || errors.Is(err, ErrSessionWriteFenceLost) || errors.Is(err, core.ErrSessionConflict) {
			t.Fatalf("triggered evidence corruption: appended=%t err=%v", appended, err)
		}
		assertCompletedToolResultRecoveryNoWrite(t, fixture)
	})
}

func TestSQLSessionStoreRecoveryPrefixSQLiteAuthorizationMutationLinearizes(t *testing.T) {
	path := t.TempDir() + "/recovery-prefix-epoch.db"
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
	first.SetMaxOpenConns(1)
	second.SetMaxOpenConns(1)
	store, err := OpenSQLSessionStore(context.Background(), first, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := second.Exec(`PRAGMA busy_timeout=1000`); err != nil {
		t.Fatal(err)
	}
	fixture := newCompletedToolResultRecoveryFixture(t, store, "session-completed-result-recovery-linearized", "run-completed-result-recovery-linearized")
	bindings, err := NewSQLBindingJournal(second, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	type prefixOutcome struct {
		appended bool
		err      error
	}
	start := make(chan struct{})
	ready := make(chan struct{}, 2)
	prefixDone := make(chan prefixOutcome, 1)
	bindingDone := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		ready <- struct{}{}
		<-start
		appended, err := fixture.store.AppendCompletedToolResultRecoveryPrefixFenced(context.Background(), fixture.fence, fixture.version, fixture.prefix)
		prefixDone <- prefixOutcome{appended: appended, err: err}
	}()
	go func() {
		defer wg.Done()
		ready <- struct{}{}
		<-start
		bindingDone <- bindings.Record(context.Background(), BindingRecord{ID: "recovery-prefix-linearized-binding", Kind: "policy", Payload: []byte(`{"scope":"global"}`)})
	}()
	<-ready
	<-ready
	close(start)
	select {
	case outcome := <-prefixDone:
		var bindingErr error
		select {
		case bindingErr = <-bindingDone:
		case <-time.After(5 * time.Second):
			t.Fatal("parallel authorization mutation did not complete")
		}
		busy := bindingErr != nil && (strings.Contains(strings.ToLower(bindingErr.Error()), "busy") || strings.Contains(strings.ToLower(bindingErr.Error()), "locked"))
		if outcome.err == nil && outcome.appended {
			assertCompletedToolResultRecoveryPrefix(t, fixture)
			if busy {
				if err := bindings.Record(context.Background(), BindingRecord{ID: "recovery-prefix-linearized-binding-retry", Kind: "policy", Payload: []byte(`{"scope":"global"}`)}); err != nil {
					t.Fatalf("authorization mutation retry: %v", err)
				}
			} else if bindingErr != nil {
				t.Fatalf("authorization mutation: %v", bindingErr)
			}
		} else if errors.Is(outcome.err, ErrAuthorizationEpochChanged) && !outcome.appended {
			if bindingErr != nil {
				t.Fatalf("authorization mutation before epoch mismatch: %v", bindingErr)
			}
			assertCompletedToolResultRecoveryNoWrite(t, fixture)
		} else {
			t.Fatalf("parallel recovery prefix outcome: appended=%t err=%v", outcome.appended, outcome.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("parallel recovery prefix did not complete")
	}
	wg.Wait()
	assertAuthorizationEpoch(t, store, 1)
}

func TestFencedCompletedToolResultRecoveryPrefixPostgresBinding(t *testing.T) {
	query := sqlFencePostgresLockToolInvocation.bind(SQLDialectPostgres)
	epoch := sqlLockAuthorizationEpoch.bind(SQLDialectPostgres)
	if !strings.Contains(query, "FOR UPDATE") || !strings.Contains(epoch, "$1") || !strings.Contains(fencedSessionTipUpdate(SQLDialectPostgres), "clock_timestamp()") {
		t.Fatalf("postgres completed-result recovery binding is incomplete")
	}
	for index := 1; index <= 8; index++ {
		if !strings.Contains(query, fmt.Sprintf("$%d", index)) {
			t.Fatalf("postgres completed-result recovery binding misses placeholder $%d: %s", index, query)
		}
	}
}

func TestPostgresSQLSessionStoreAppendCompletedToolResultRecoveryPrefixFenced(t *testing.T) {
	ctx := newPostgresFenceTestContext(t)
	db := newPostgresTestDB(t)
	store, err := OpenSQLSessionStore(ctx, db, SQLDialectPostgres)
	if err != nil {
		t.Fatal(err)
	}
	t.Run("success_and_epoch_mismatch", func(t *testing.T) {
		fixture := newCompletedToolResultRecoveryFixture(t, store, "session-pg-completed-result-recovery", "run-pg-completed-result-recovery")
		appended, err := fixture.store.AppendCompletedToolResultRecoveryPrefixFenced(ctx, fixture.fence, fixture.version, fixture.prefix)
		if err != nil || !appended {
			t.Fatalf("postgres recovery prefix: appended=%t err=%v", appended, err)
		}
		assertCompletedToolResultRecoveryPrefix(t, fixture)
		bindings, err := NewSQLBindingJournal(store.db, store.dialect)
		if err != nil {
			t.Fatal(err)
		}
		if err := bindings.Record(ctx, BindingRecord{ID: "pg-recovery-prefix-epoch", Kind: "policy", Payload: []byte(`{"scope":"global"}`)}); err != nil {
			t.Fatal(err)
		}
		appended, err = fixture.store.AppendCompletedToolResultRecoveryPrefixFenced(ctx, fixture.fence, fixture.version, fixture.prefix)
		if appended || !errors.Is(err, ErrAuthorizationEpochChanged) {
			t.Fatalf("postgres response-lost epoch mismatch: appended=%t err=%v", appended, err)
		}
	})

	t.Run("authorization_binding_serializes_behind_recovery_epoch", func(t *testing.T) {
		fixture := newCompletedToolResultRecoveryFixture(t, store, "session-pg-completed-result-recovery-epoch-lock", "run-pg-completed-result-recovery-epoch-lock")
		if _, err := store.db.ExecContext(ctx, `CREATE TABLE recovery_prefix_epoch_gate (id INTEGER PRIMARY KEY)`); err != nil {
			t.Fatal(err)
		}
		if _, err := store.db.ExecContext(ctx, `INSERT INTO recovery_prefix_epoch_gate (id) VALUES (1)`); err != nil {
			t.Fatal(err)
		}
		if _, err := store.db.ExecContext(ctx, `CREATE OR REPLACE FUNCTION recovery_prefix_epoch_gate_lock() RETURNS trigger AS $$
			BEGIN
				PERFORM 1 FROM recovery_prefix_epoch_gate WHERE id = 1 FOR UPDATE;
				RETURN NEW;
			END;
		$$ LANGUAGE plpgsql`); err != nil {
			t.Fatal(err)
		}
		if _, err := store.db.ExecContext(ctx, `CREATE TRIGGER recovery_prefix_epoch_gate_trigger
			BEFORE INSERT ON event_chunks FOR EACH ROW EXECUTE FUNCTION recovery_prefix_epoch_gate_lock()`); err != nil {
			t.Fatal(err)
		}
		gate, err := store.db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		gateReleased := false
		t.Cleanup(func() {
			if !gateReleased {
				_ = gate.Rollback()
			}
		})
		var gateID int
		if err := gate.QueryRowContext(ctx, `SELECT id FROM recovery_prefix_epoch_gate WHERE id = 1 FOR UPDATE`).Scan(&gateID); err != nil {
			t.Fatal(err)
		}
		prefixDone := make(chan struct {
			appended bool
			err      error
		}, 1)
		go func() {
			appended, err := store.AppendCompletedToolResultRecoveryPrefixFenced(ctx, fixture.fence, fixture.version, fixture.prefix)
			prefixDone <- struct {
				appended bool
				err      error
			}{appended: appended, err: err}
		}()
		deadline := time.Now().Add(5 * time.Second)
		for {
			var waiters int
			err := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM pg_stat_activity
				WHERE datname = current_database() AND wait_event_type = 'Lock' AND query LIKE 'INSERT INTO event_chunks%'`).Scan(&waiters)
			if err != nil {
				t.Fatal(err)
			}
			if waiters > 0 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("recovery prefix did not reach the event-chunk gate")
			}
			time.Sleep(10 * time.Millisecond)
		}
		bindings, err := NewSQLBindingJournal(store.db, store.dialect)
		if err != nil {
			t.Fatal(err)
		}
		bindingDone := make(chan error, 1)
		go func() {
			bindingDone <- bindings.Record(ctx, BindingRecord{ID: "pg-recovery-prefix-epoch-lock", Kind: "policy", Payload: []byte(`{"scope":"global"}`)})
		}()
		select {
		case err := <-bindingDone:
			t.Fatalf("authorization binding completed before recovery epoch lock released: %v", err)
		case <-time.After(100 * time.Millisecond):
		}
		if err := gate.Commit(); err != nil {
			t.Fatal(err)
		}
		gateReleased = true
		select {
		case outcome := <-prefixDone:
			if outcome.err != nil || !outcome.appended {
				t.Fatalf("postgres gated recovery prefix: appended=%t err=%v", outcome.appended, outcome.err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("recovery prefix did not finish after gate release")
		}
		select {
		case err := <-bindingDone:
			if err != nil {
				t.Fatalf("authorization binding after recovery prefix: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("authorization binding did not finish after recovery prefix")
		}
		assertCompletedToolResultRecoveryPrefix(t, fixture)
		assertAuthorizationEpoch(t, store, 1)
	})
}
