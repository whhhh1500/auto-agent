package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
)

type completedToolResultRecoverySidecarFixture struct {
	*fencedSQLFixture
	invocation core.ToolInvocation
	result     core.CapabilityResult
	input      CompletedToolResultRecoverySidecarInput
	version    int64
}

func newSQLiteCompletedToolResultRecoverySidecarFixture(t *testing.T) *completedToolResultRecoverySidecarFixture {
	t.Helper()
	return newCompletedToolResultRecoverySidecarFixture(t, newTestSQLStore(t), "session-completed-result-sidecar", "run-completed-result-sidecar")
}

func newCompletedToolResultRecoverySidecarFixture(t *testing.T, store *SQLSessionStore, sessionID, runID string) *completedToolResultRecoverySidecarFixture {
	t.Helper()
	base := newCompletedToolResultRecoveryFixture(t, store, sessionID, runID)
	metadata := map[string]string{"harness.assignment.id": "completed-tool-result-sidecar"}
	composition := &core.RunCompositionData{
		Profile: core.AgentProfileSnapshot{
			ID: "profile-completed-result-sidecar", ProfileID: base.session.ProfileID(), Scope: base.session.Scope(),
		},
		Metadata: metadata,
	}
	compositionRevision, err := core.CompositionRevision(composition)
	if err != nil {
		t.Fatal(err)
	}
	assignmentRevision, err := core.CompositionMetadataRevision(metadata)
	if err != nil {
		t.Fatal(err)
	}
	return &completedToolResultRecoverySidecarFixture{
		fencedSQLFixture: base.fencedSQLFixture, invocation: base.invocation, result: base.result, version: base.version,
		input: CompletedToolResultRecoverySidecarInput{
			Composition: composition, ProfileSnapshotID: composition.Profile.ID, CapabilitySnapshotID: "capabilities-completed-result-sidecar",
			CompositionRevision: compositionRevision, AssignmentRevision: assignmentRevision, Invocation: base.invocation,
			ResultDigest: base.prefix.ExpectedResultDigest, AuthorizationEpoch: base.prefix.ExpectedAuthorizationEpoch,
			OriginStepSeq: 1,
		},
	}
}

func refreshCompletedToolResultRecoverySidecarInput(t *testing.T, input *CompletedToolResultRecoverySidecarInput) {
	t.Helper()
	compositionRevision, err := core.CompositionRevision(input.Composition)
	if err != nil {
		t.Fatal(err)
	}
	assignmentRevision, err := core.CompositionMetadataRevision(input.Composition.Metadata)
	if err != nil {
		t.Fatal(err)
	}
	input.CompositionRevision = compositionRevision
	input.AssignmentRevision = assignmentRevision
}

func cloneCompletedToolResultRecoverySidecarComposition(t *testing.T, composition *core.RunCompositionData) *core.RunCompositionData {
	t.Helper()
	encoded, err := json.Marshal(composition)
	if err != nil {
		t.Fatal(err)
	}
	var copyOf core.RunCompositionData
	if err := json.Unmarshal(encoded, &copyOf); err != nil {
		t.Fatal(err)
	}
	return &copyOf
}

func completedToolResultRecoverySidecarEvidenceCount(t *testing.T, fixture *completedToolResultRecoverySidecarFixture) int {
	t.Helper()
	var count int
	if err := fixture.store.db.QueryRowContext(context.Background(), (sqlQuery{"SELECT COUNT(*) FROM run_evidence WHERE session_id = ? AND run_id = ?"}).bind(fixture.store.dialect), fixture.fence.SessionID, fixture.fence.RunID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func assertCompletedToolResultRecoverySidecar(t *testing.T, fixture *completedToolResultRecoverySidecarFixture) {
	t.Helper()
	loaded := assertFencedSessionVersion(t, fixture.fencedSQLFixture, fixture.version+1)
	events := loaded.Events()
	if events[fixture.version].Type != core.EvToolResult || events[fixture.version].RunID != fixture.fence.RunID {
		t.Fatalf("durable V3 event=%+v", events[fixture.version])
	}
	for _, event := range events {
		if event.Type == core.EvRunResume {
			t.Fatalf("V3 wrote forbidden run/resume event: %+v", event)
		}
	}
	var result core.ToolResultData
	if err := json.Unmarshal(events[fixture.version].Data, &result); err != nil {
		t.Fatal(err)
	}
	if result.CallID != fixture.invocation.CallID || result.Content != fixture.result.Content || result.OK != fixture.result.OK || !reflect.DeepEqual(result.Metadata, fixture.result.Metadata) {
		t.Fatalf("durable V3 result=%+v", result)
	}
	epoch, err := fixture.store.AuthorizationEpoch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	session, sidecar, found, err := fixture.store.LoadCompletedToolResultRecoverySidecarFenced(context.Background(), fixture.fence, epoch, loaded.Version(), fixture.invocation)
	if err != nil || !found || session.Version() != loaded.Version() {
		t.Fatalf("strict V3 readback session=%v sidecar=%+v found=%t err=%v", session, sidecar, found, err)
	}
	if sidecar.Protocol != completedToolResultRecoverySidecarProtocol || sidecar.CallEventSeq != fixture.version-1 || sidecar.ResultEventSeq != fixture.version || sidecar.CompositionSHA256 == "" || !reflect.DeepEqual(sidecar.Input, fixture.input) {
		t.Fatalf("strict V3 sidecar=%+v want input=%+v", sidecar, fixture.input)
	}
	var rows int
	if err := fixture.store.db.QueryRowContext(context.Background(), (sqlQuery{"SELECT COUNT(*) FROM completed_tool_result_recovery_sidecars WHERE session_id = ? AND result_event_seq = ?"}).bind(fixture.store.dialect), fixture.fence.SessionID, fixture.version).Scan(&rows); err != nil || rows != 1 {
		t.Fatalf("sidecar rows=%d err=%v", rows, err)
	}
}

func TestSQLSessionStoreAppendCompletedToolResultRecoverySidecarFenced(t *testing.T) {
	t.Run("append_readback_and_response_lost", func(t *testing.T) {
		fixture := newSQLiteCompletedToolResultRecoverySidecarFixture(t)
		beforeEvidence := completedToolResultRecoverySidecarEvidenceCount(t, fixture)
		if session, _, found, err := fixture.store.LoadCompletedToolResultRecoverySidecarFenced(context.Background(), fixture.fence, fixture.input.AuthorizationEpoch, fixture.version, fixture.invocation); err != nil || found || session == nil || session.Version() != fixture.version {
			t.Fatalf("pre-delivery readback session=%v found=%t err=%v", session, found, err)
		}
		appended, err := fixture.store.AppendCompletedToolResultRecoverySidecarFenced(context.Background(), fixture.fence, fixture.version, fixture.input)
		if err != nil || !appended {
			t.Fatalf("V3 append appended=%t err=%v", appended, err)
		}
		if afterEvidence := completedToolResultRecoverySidecarEvidenceCount(t, fixture); afterEvidence != beforeEvidence {
			t.Fatalf("V3 append changed run_evidence from %d to %d", beforeEvidence, afterEvidence)
		}
		assertCompletedToolResultRecoverySidecar(t, fixture)
		appended, err = fixture.store.AppendCompletedToolResultRecoverySidecarFenced(context.Background(), fixture.fence, fixture.version, fixture.input)
		if err != nil || appended {
			t.Fatalf("V3 response-lost convergence appended=%t err=%v", appended, err)
		}
		mismatch := fixture.input
		mismatch.OriginStepSeq++
		if appended, err := fixture.store.AppendCompletedToolResultRecoverySidecarFenced(context.Background(), fixture.fence, fixture.version, mismatch); appended || !errors.Is(err, ErrCompletedToolResultProofInvalid) {
			t.Fatalf("V3 response-lost origin mismatch appended=%t err=%v", appended, err)
		}
		mismatch = fixture.input
		mismatch.Composition = cloneCompletedToolResultRecoverySidecarComposition(t, fixture.input.Composition)
		mismatch.Composition.Metadata["harness.assignment.id"] = "different"
		refreshCompletedToolResultRecoverySidecarInput(t, &mismatch)
		if appended, err := fixture.store.AppendCompletedToolResultRecoverySidecarFenced(context.Background(), fixture.fence, fixture.version, mismatch); appended || !errors.Is(err, ErrCompletedToolResultProofInvalid) {
			t.Fatalf("V3 response-lost composition mismatch appended=%t err=%v", appended, err)
		}
	})

	t.Run("rejects_reserved_metadata_before_write", func(t *testing.T) {
		fixture := newSQLiteCompletedToolResultRecoverySidecarFixture(t)
		fixture.input.Composition.Metadata[recoveryProtocolMetadataKey] = recoveryProtocolV1
		refreshCompletedToolResultRecoverySidecarInput(t, &fixture.input)
		if appended, err := fixture.store.AppendCompletedToolResultRecoverySidecarFenced(context.Background(), fixture.fence, fixture.version, fixture.input); appended || !errors.Is(err, ErrCompletedToolResultProofInvalid) {
			t.Fatalf("reserved metadata appended=%t err=%v", appended, err)
		}
		assertFencedSessionVersion(t, fixture.fencedSQLFixture, fixture.version)
	})

	t.Run("requires_sidecar_for_existing_result", func(t *testing.T) {
		fixture := newSQLiteCompletedToolResultRecoverySidecarFixture(t)
		if _, appended, err := fixture.store.AppendCompletedToolResultFenced(context.Background(), fixture.fence, fixture.version, fixture.invocation, fixture.input.ResultDigest); err != nil || !appended {
			t.Fatalf("V1 result append appended=%t err=%v", appended, err)
		}
		if _, _, found, err := fixture.store.LoadCompletedToolResultRecoverySidecarFenced(context.Background(), fixture.fence, fixture.input.AuthorizationEpoch, fixture.version+1, fixture.invocation); found || !errors.Is(err, ErrCompletedToolResultProofInvalid) {
			t.Fatalf("V1 tail sidecar read found=%t err=%v", found, err)
		}
		if appended, err := fixture.store.AppendCompletedToolResultRecoverySidecarFenced(context.Background(), fixture.fence, fixture.version, fixture.input); appended || !errors.Is(err, ErrCompletedToolResultProofInvalid) {
			t.Fatalf("V1 tail V3 append appended=%t err=%v", appended, err)
		}
	})

	t.Run("fence_journal_and_epoch_fail_closed", func(t *testing.T) {
		fixture := newSQLiteCompletedToolResultRecoverySidecarFixture(t)
		lost := fixture.fence
		lost.WorkerID = "worker-other"
		if appended, err := fixture.store.AppendCompletedToolResultRecoverySidecarFenced(context.Background(), lost, fixture.version, fixture.input); appended || !errors.Is(err, ErrSessionWriteFenceLost) {
			t.Fatalf("lost fence appended=%t err=%v", appended, err)
		}
		if _, err := fixture.store.db.ExecContext(context.Background(), (sqlQuery{"DELETE FROM tool_invocations WHERE session_id = ? AND call_id = ?"}).bind(fixture.store.dialect), fixture.fence.SessionID, fixture.invocation.CallID); err != nil {
			t.Fatal(err)
		}
		if appended, err := fixture.store.AppendCompletedToolResultRecoverySidecarFenced(context.Background(), fixture.fence, fixture.version, fixture.input); appended || !errors.Is(err, ErrCompletedToolResultProofInvalid) {
			t.Fatalf("missing journal appended=%t err=%v", appended, err)
		}
		assertFencedSessionVersion(t, fixture.fencedSQLFixture, fixture.version)

		fixture = newSQLiteCompletedToolResultRecoverySidecarFixture(t)
		if appended, err := fixture.store.AppendCompletedToolResultRecoverySidecarFenced(context.Background(), fixture.fence, fixture.version, fixture.input); err != nil || !appended {
			t.Fatalf("epoch fixture append appended=%t err=%v", appended, err)
		}
		bindings, err := NewSQLBindingJournal(fixture.store.db, fixture.store.dialect)
		if err != nil {
			t.Fatal(err)
		}
		if err := bindings.Record(context.Background(), BindingRecord{ID: "sidecar-epoch", Kind: "policy", Payload: []byte(`{"scope":"global"}`)}); err != nil {
			t.Fatal(err)
		}
		if appended, err := fixture.store.AppendCompletedToolResultRecoverySidecarFenced(context.Background(), fixture.fence, fixture.version, fixture.input); appended || !errors.Is(err, ErrAuthorizationEpochChanged) {
			t.Fatalf("epoch response-lost appended=%t err=%v", appended, err)
		}
	})

	t.Run("tamper_and_run_evidence_do_not_create_authority", func(t *testing.T) {
		fixture := newSQLiteCompletedToolResultRecoverySidecarFixture(t)
		if appended, err := fixture.store.AppendCompletedToolResultRecoverySidecarFenced(context.Background(), fixture.fence, fixture.version, fixture.input); err != nil || !appended {
			t.Fatalf("append appended=%t err=%v", appended, err)
		}
		if _, err := fixture.store.db.ExecContext(context.Background(), (sqlQuery{`INSERT INTO run_evidence
			(session_id, run_id, segment_seq, kind, tenant_id, subject_id, profile_id, composition_revision, assignment_revision, assignment_variant, status, created_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`}).bind(fixture.store.dialect),
			fixture.fence.SessionID, fixture.fence.RunID, 99, "forged", fixture.fence.TenantID, fixture.fence.SubjectID, fixture.session.ProfileID(), "forged", "forged", "forged", "running", time.Now().UnixMilli()); err != nil {
			t.Fatal(err)
		}
		epoch, err := fixture.store.AuthorizationEpoch(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if _, _, found, err := fixture.store.LoadCompletedToolResultRecoverySidecarFenced(context.Background(), fixture.fence, epoch, fixture.version+1, fixture.invocation); err != nil || !found {
			t.Fatalf("forged evidence affected strict read found=%t err=%v", found, err)
		}
		if _, err := fixture.store.db.ExecContext(context.Background(), (sqlQuery{"UPDATE completed_tool_result_recovery_sidecars SET composition_sha256 = ? WHERE session_id = ?"}).bind(fixture.store.dialect), strings.Repeat("0", 64), fixture.fence.SessionID); err != nil {
			t.Fatal(err)
		}
		if _, _, found, err := fixture.store.LoadCompletedToolResultRecoverySidecarFenced(context.Background(), fixture.fence, epoch, fixture.version+1, fixture.invocation); found || !errors.Is(err, ErrCompletedToolResultProofInvalid) {
			t.Fatalf("tampered sidecar found=%t err=%v", found, err)
		}
	})

	t.Run("response_lost_requires_every_historical_field", func(t *testing.T) {
		cases := []struct {
			name   string
			mutate func(*CompletedToolResultRecoverySidecarInput)
			want   error
		}{
			{"capability snapshot", func(input *CompletedToolResultRecoverySidecarInput) {
				input.CapabilitySnapshotID = "capabilities-different"
			}, ErrCompletedToolResultProofInvalid},
			{"origin step", func(input *CompletedToolResultRecoverySidecarInput) { input.OriginStepSeq++ }, ErrCompletedToolResultProofInvalid},
			{"composition revision", func(input *CompletedToolResultRecoverySidecarInput) {
				input.CompositionRevision = strings.Repeat("0", 64)
			}, ErrCompletedToolResultProofInvalid},
			{"assignment revision", func(input *CompletedToolResultRecoverySidecarInput) {
				input.AssignmentRevision = strings.Repeat("0", 64)
			}, ErrCompletedToolResultProofInvalid},
			{"result digest", func(input *CompletedToolResultRecoverySidecarInput) { input.ResultDigest = strings.Repeat("0", 64) }, ErrCompletedToolResultProofInvalid},
			{"historical epoch", func(input *CompletedToolResultRecoverySidecarInput) { input.AuthorizationEpoch++ }, ErrAuthorizationEpochChanged},
		}
		for _, test := range cases {
			t.Run(test.name, func(t *testing.T) {
				fixture := newSQLiteCompletedToolResultRecoverySidecarFixture(t)
				if appended, err := fixture.store.AppendCompletedToolResultRecoverySidecarFenced(context.Background(), fixture.fence, fixture.version, fixture.input); err != nil || !appended {
					t.Fatalf("initial append appended=%t err=%v", appended, err)
				}
				mismatch := fixture.input
				test.mutate(&mismatch)
				if appended, err := fixture.store.AppendCompletedToolResultRecoverySidecarFenced(context.Background(), fixture.fence, fixture.version, mismatch); appended || !errors.Is(err, test.want) {
					t.Fatalf("response-lost mismatch appended=%t err=%v want=%v", appended, err, test.want)
				}
			})
		}
	})

	t.Run("final_fence_recheck_rolls_back_chunk_and_sidecar", func(t *testing.T) {
		fixture := newSQLiteCompletedToolResultRecoverySidecarFixture(t)
		trigger := "completed_tool_result_sidecar_change_owner"
		if _, err := fixture.store.db.ExecContext(context.Background(), `CREATE TRIGGER `+trigger+` AFTER INSERT ON event_chunks
			WHEN NEW.session_id = '`+fixture.fence.SessionID+`'
			BEGIN UPDATE run_queue SET worker_id = 'worker-triggered' WHERE run_id = '`+fixture.fence.RunID+`'; END`); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _, _ = fixture.store.db.ExecContext(context.Background(), "DROP TRIGGER IF EXISTS "+trigger) })
		if appended, err := fixture.store.AppendCompletedToolResultRecoverySidecarFenced(context.Background(), fixture.fence, fixture.version, fixture.input); appended || !errors.Is(err, ErrSessionWriteFenceLost) {
			t.Fatalf("triggered fence append=%t err=%v", appended, err)
		}
		assertFencedSessionVersion(t, fixture.fencedSQLFixture, fixture.version)
		var sidecars int
		if err := fixture.store.db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM completed_tool_result_recovery_sidecars").Scan(&sidecars); err != nil || sidecars != 0 {
			t.Fatalf("rollback sidecars=%d err=%v", sidecars, err)
		}
	})

	for _, test := range []struct {
		name, trigger string
	}{
		{name: "event_chunk_mutation", trigger: `CREATE TRIGGER sidecar_mutate_chunk AFTER INSERT ON event_chunks
			BEGIN UPDATE event_chunks SET payload = '{}\n' WHERE session_id = NEW.session_id AND start_seq = NEW.start_seq; END`},
		{name: "sidecar_mutation", trigger: `CREATE TRIGGER sidecar_mutate_row AFTER INSERT ON completed_tool_result_recovery_sidecars
			BEGIN UPDATE completed_tool_result_recovery_sidecars SET composition_sha256 = '` + strings.Repeat("0", 64) + `' WHERE session_id = NEW.session_id AND result_event_seq = NEW.result_event_seq; END`},
	} {
		t.Run(test.name+"_rolls_back", func(t *testing.T) {
			fixture := newSQLiteCompletedToolResultRecoverySidecarFixture(t)
			if _, err := fixture.store.db.ExecContext(context.Background(), test.trigger); err != nil {
				t.Fatal(err)
			}
			if appended, err := fixture.store.AppendCompletedToolResultRecoverySidecarFenced(context.Background(), fixture.fence, fixture.version, fixture.input); appended || !errors.Is(err, ErrCompletedToolResultProofInvalid) {
				t.Fatalf("mutated append=%t err=%v", appended, err)
			}
			assertFencedSessionVersion(t, fixture.fencedSQLFixture, fixture.version)
			var rows int
			if err := fixture.store.db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM completed_tool_result_recovery_sidecars").Scan(&rows); err != nil || rows != 0 {
				t.Fatalf("mutated rollback sidecars=%d err=%v", rows, err)
			}
		})
	}

	t.Run("multiple_calls_have_distinct_sidecars", func(t *testing.T) {
		fixture := newSQLiteCompletedToolResultRecoverySidecarFixture(t)
		if appended, err := fixture.store.AppendCompletedToolResultRecoverySidecarFenced(context.Background(), fixture.fence, fixture.version, fixture.input); err != nil || !appended {
			t.Fatalf("first append appended=%t err=%v", appended, err)
		}
		loaded := assertFencedSessionVersion(t, fixture.fencedSQLFixture, fixture.version+1)
		staged, err := loaded.Clone()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := staged.Append(fixture.fence.RunID, core.EvStepEnd, core.StepData{Index: 0}); err != nil {
			t.Fatal(err)
		}
		step, err := staged.Append(fixture.fence.RunID, core.EvStepStart, core.StepData{Index: 1})
		if err != nil {
			t.Fatal(err)
		}
		call := core.ToolCall{ID: "call-completed-result-sidecar-second", Name: "test.completed.sidecar.second", Args: map[string]any{"input": "second"}}
		if _, err := staged.Append(fixture.fence.RunID, core.EvAssistantMessage, core.AssistantMessageData{Text: "second tools", ToolCall: &call, ToolCalls: []core.ToolCall{call}}); err != nil {
			t.Fatal(err)
		}
		if _, err := staged.Append(fixture.fence.RunID, core.EvRunUsage, core.RunUsageData{InvocationID: fmt.Sprintf("model:%d", step.Seq)}); err != nil {
			t.Fatal(err)
		}
		if _, err := staged.Append(fixture.fence.RunID, core.EvToolCall, core.ToolCallData{CallID: call.ID, Name: call.Name, Args: call.Args}); err != nil {
			t.Fatal(err)
		}
		if err := fixture.store.AppendEventsFenced(context.Background(), fixture.fence, loaded.Version(), staged.EventsFrom(loaded.Version())); err != nil {
			t.Fatal(err)
		}
		invocation, err := core.NewToolInvocation(core.RunInfo{RunID: fixture.fence.RunID, SessionID: fixture.fence.SessionID, Principal: fixture.session.Principal()}, call, true)
		if err != nil {
			t.Fatal(err)
		}
		journal, err := NewSQLToolInvocationJournal(fixture.store.db, fixture.store.dialect)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := journal.BeginToolInvocation(context.Background(), invocation); err != nil {
			t.Fatal(err)
		}
		result := core.CapabilityResult{Content: "second canonical result", OK: true}
		if _, err := journal.CompleteToolInvocation(context.Background(), invocation, result); err != nil {
			t.Fatal(err)
		}
		digest, err := CanonicalCapabilityResultDigest(result)
		if err != nil {
			t.Fatal(err)
		}
		secondInput := fixture.input
		secondInput.Composition = cloneCompletedToolResultRecoverySidecarComposition(t, fixture.input.Composition)
		secondInput.Invocation, secondInput.ResultDigest, secondInput.OriginStepSeq = invocation, digest, step.Seq
		current, err := fixture.store.Load(context.Background(), fixture.fence.SessionID)
		if err != nil {
			t.Fatal(err)
		}
		if appended, err := fixture.store.AppendCompletedToolResultRecoverySidecarFenced(context.Background(), fixture.fence, current.Version(), secondInput); err != nil || !appended {
			t.Fatalf("second append appended=%t err=%v", appended, err)
		}
		var rows int
		if err := fixture.store.db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM completed_tool_result_recovery_sidecars WHERE session_id = ?", fixture.fence.SessionID).Scan(&rows); err != nil || rows != 2 {
			t.Fatalf("multiple sidecars rows=%d err=%v", rows, err)
		}
		epoch, err := fixture.store.AuthorizationEpoch(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if session, sidecar, found, err := fixture.store.LoadCompletedToolResultRecoverySidecarFenced(context.Background(), fixture.fence, epoch, current.Version()+1, invocation); err != nil || !found || session.Version() != current.Version()+1 || sidecar.Input.Invocation.CallID != call.ID {
			t.Fatalf("second strict read session=%v sidecar=%+v found=%t err=%v", session, sidecar, found, err)
		}
	})
}

func TestSQLSessionStoreCompletedToolResultRecoverySidecarMigrationAndPrune(t *testing.T) {
	t.Run("fresh_and_v42_upgrade_have_no_backfill", func(t *testing.T) {
		store := newTestSQLStore(t)
		ctx := context.Background()
		exists, err := sqlTableExists(ctx, store.db, store.dialect, "completed_tool_result_recovery_sidecars")
		if err != nil || !exists {
			t.Fatalf("fresh v43 sidecar table exists=%t err=%v", exists, err)
		}
		if _, err := store.db.ExecContext(ctx, "DROP TABLE completed_tool_result_recovery_sidecars"); err != nil {
			t.Fatal(err)
		}
		if _, err := store.db.ExecContext(ctx, sqlUpdateMetaRow.bind(store.dialect), "42"); err != nil {
			t.Fatal(err)
		}
		reopened, err := OpenSQLSessionStore(ctx, store.db, store.dialect)
		if err != nil {
			t.Fatal(err)
		}
		exists, err = sqlTableExists(ctx, reopened.db, reopened.dialect, "completed_tool_result_recovery_sidecars")
		if err != nil || !exists {
			t.Fatalf("v42 upgrade table exists=%t err=%v", exists, err)
		}
		var rows int
		if err := reopened.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM completed_tool_result_recovery_sidecars").Scan(&rows); err != nil || rows != 0 {
			t.Fatalf("v42 upgrade backfilled sidecars=%d err=%v", rows, err)
		}
	})

	t.Run("prune_retains_any_sidecar_journal_proof", func(t *testing.T) {
		fixture := newSQLiteCompletedToolResultRecoverySidecarFixture(t)
		if appended, err := fixture.store.AppendCompletedToolResultRecoverySidecarFenced(context.Background(), fixture.fence, fixture.version, fixture.input); err != nil || !appended {
			t.Fatalf("append appended=%t err=%v", appended, err)
		}
		if _, err := fixture.store.db.ExecContext(context.Background(), (sqlQuery{"UPDATE tool_invocations SET completed_at = 1, updated_at = 1 WHERE session_id = ? AND call_id = ?"}).bind(fixture.store.dialect), fixture.fence.SessionID, fixture.invocation.CallID); err != nil {
			t.Fatal(err)
		}
		deleted, err := fixture.store.PruneToolInvocations(context.Background(), time.Now().UTC().Add(-time.Minute))
		if err != nil || deleted != 0 {
			t.Fatalf("sidecar prune deleted=%d err=%v", deleted, err)
		}
		journal, err := NewSQLToolInvocationJournal(fixture.store.db, fixture.store.dialect)
		if err != nil {
			t.Fatal(err)
		}
		if record, found, err := journal.GetToolInvocation(context.Background(), fixture.invocation); err != nil || !found || record.Result == nil {
			t.Fatalf("sidecar journal after prune record=%+v found=%t err=%v", record, found, err)
		}
	})
}

func TestCompletedToolResultRecoverySidecarOriginStateMachine(t *testing.T) {
	call := func(id string) core.ToolCall {
		return core.ToolCall{ID: id, Name: "test.origin", Args: map[string]any{"id": id}}
	}
	event := func(seq int64, kind core.SessionEventType, data any) core.SessionEvent {
		raw, err := json.Marshal(data)
		if err != nil {
			t.Fatal(err)
		}
		return core.SessionEvent{Seq: seq, RunID: "run-origin", Type: kind, Data: raw}
	}
	build := func(calls []core.ToolCall) []core.SessionEvent {
		events := []core.SessionEvent{
			event(0, core.EvStepStart, core.StepData{Index: 0}),
			event(1, core.EvAssistantMessage, core.AssistantMessageData{ToolCall: &calls[0], ToolCalls: calls}),
			event(2, core.EvRunUsage, core.RunUsageData{InvocationID: "model:0"}),
		}
		for index, toolCall := range calls {
			events = append(events, event(int64(len(events)), core.EvToolCall, core.ToolCallData{CallID: toolCall.ID, Name: toolCall.Name, Args: toolCall.Args}))
			if index+1 < len(calls) {
				events = append(events, event(int64(len(events)), core.EvToolResult, core.ToolResultData{CallID: toolCall.ID, Content: "ok", OK: true}))
			}
		}
		return events
	}
	t.Run("summary_artifacts_are_allowed_before_assistant", func(t *testing.T) {
		calls := []core.ToolCall{call("call-summary")}
		events := build(calls)
		events = append([]core.SessionEvent{events[0], event(1, core.EvAssistantChunk, core.AssistantChunkData{Text: "draft"}), event(2, core.EvContextSummary, core.ContextSummaryData{Op: "replace", Start: 0, End: 0, Summary: "summary"}), event(3, core.EvRunUsage, core.RunUsageData{InvocationID: "summary:0:0:1"})}, events[1:]...)
		for index := range events {
			events[index].Seq = int64(index)
		}
		var usage core.RunUsageData
		_ = json.Unmarshal(events[5].Data, &usage)
		usage.InvocationID = "model:0"
		events[5].Data, _ = json.Marshal(usage)
		if err := validateCompletedToolResultRecoverySidecarOrigin(events, "run-origin", calls[0].ID, 0, int64(len(events)-1)); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("usage_before_assistant_is_rejected", func(t *testing.T) {
		calls := []core.ToolCall{call("call-before")}
		events := build(calls)
		events[1], events[2] = events[2], events[1]
		events[1].Seq, events[2].Seq = 1, 2
		if err := validateCompletedToolResultRecoverySidecarOrigin(events, "run-origin", calls[0].ID, 0, int64(len(events)-1)); err == nil {
			t.Fatal("usage before assistant was accepted")
		}
	})
	t.Run("invalid_summary_usage_is_rejected", func(t *testing.T) {
		calls := []core.ToolCall{call("call-bad-summary")}
		events := build(calls)
		events = append([]core.SessionEvent{events[0], event(1, core.EvRunUsage, core.RunUsageData{InvocationID: "summary:"})}, events[1:]...)
		for index := range events {
			events[index].Seq = int64(index)
		}
		if _, err := core.RestoreSession(core.SessionOptions{ID: "origin-invalid-summary", ProfileID: "profile", Principal: core.Principal{SubjectID: "subject", TenantID: "tenant", Scope: core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})}, Scope: core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})}, events); err == nil {
			t.Fatal("invalid summary usage was restorable")
		}
	})
	t.Run("chunk_after_assistant_is_rejected", func(t *testing.T) {
		calls := []core.ToolCall{call("call-chunk")}
		events := build(calls)
		events = append(events[:2], append([]core.SessionEvent{event(2, core.EvAssistantChunk, core.AssistantChunkData{Text: "late"})}, events[2:]...)...)
		for index := range events {
			events[index].Seq = int64(index)
		}
		if err := validateCompletedToolResultRecoverySidecarOrigin(events, "run-origin", calls[0].ID, 0, int64(len(events)-1)); err == nil {
			t.Fatal("assistant chunk after message was accepted")
		}
	})
	t.Run("later_call_can_be_the_tail", func(t *testing.T) {
		calls := []core.ToolCall{call("call-one"), call("call-two"), call("call-three")}
		events := build(calls)
		if err := validateCompletedToolResultRecoverySidecarOrigin(events, "run-origin", calls[2].ID, 0, int64(len(events)-1)); err != nil {
			t.Fatal(err)
		}
	})
}

func TestPostgresSQLSessionStoreCompletedToolResultRecoverySidecarFenced(t *testing.T) {
	ctx := newPostgresFenceTestContext(t)
	db := newPostgresTestDB(t)
	store, err := OpenSQLSessionStore(ctx, db, SQLDialectPostgres)
	if err != nil {
		t.Fatal(err)
	}
	t.Run("append_readback_response_lost_and_v42_upgrade", func(t *testing.T) {
		fixture := newCompletedToolResultRecoverySidecarFixture(t, store, "session-pg-completed-result-sidecar", "run-pg-completed-result-sidecar")
		if appended, err := store.AppendCompletedToolResultRecoverySidecarFenced(ctx, fixture.fence, fixture.version, fixture.input); err != nil || !appended {
			t.Fatalf("postgres V3 append appended=%t err=%v", appended, err)
		}
		assertCompletedToolResultRecoverySidecar(t, fixture)
		if appended, err := store.AppendCompletedToolResultRecoverySidecarFenced(ctx, fixture.fence, fixture.version, fixture.input); err != nil || appended {
			t.Fatalf("postgres V3 response-lost appended=%t err=%v", appended, err)
		}
		if _, err := db.ExecContext(ctx, "DROP TABLE completed_tool_result_recovery_sidecars"); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(ctx, sqlUpdateMetaRow.bind(SQLDialectPostgres), "42"); err != nil {
			t.Fatal(err)
		}
		reopened, err := OpenSQLSessionStore(ctx, db, SQLDialectPostgres)
		if err != nil {
			t.Fatal(err)
		}
		var rows int
		if err := reopened.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM completed_tool_result_recovery_sidecars").Scan(&rows); err != nil || rows != 0 {
			t.Fatalf("postgres v42 upgrade rows=%d err=%v", rows, err)
		}
	})

	t.Run("authorization_binding_serializes_behind_epoch", func(t *testing.T) {
		fixture := newCompletedToolResultRecoverySidecarFixture(t, store, "session-pg-sidecar-epoch-lock", "run-pg-sidecar-epoch-lock")
		if _, err := db.ExecContext(ctx, `CREATE TABLE sidecar_epoch_gate (id INTEGER PRIMARY KEY)`); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(ctx, `INSERT INTO sidecar_epoch_gate (id) VALUES (1)`); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(ctx, `CREATE OR REPLACE FUNCTION sidecar_epoch_gate_lock() RETURNS trigger AS $$
			BEGIN PERFORM 1 FROM sidecar_epoch_gate WHERE id = 1 FOR UPDATE; RETURN NEW; END;
		$$ LANGUAGE plpgsql`); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(ctx, `CREATE TRIGGER sidecar_epoch_gate_trigger
			BEFORE INSERT ON event_chunks FOR EACH ROW EXECUTE FUNCTION sidecar_epoch_gate_lock()`); err != nil {
			t.Fatal(err)
		}
		gate, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		gateReleased := false
		t.Cleanup(func() {
			if !gateReleased {
				_ = gate.Rollback()
			}
		})
		if _, err := gate.ExecContext(ctx, `SELECT id FROM sidecar_epoch_gate WHERE id = 1 FOR UPDATE`); err != nil {
			t.Fatal(err)
		}
		appendDone := make(chan error, 1)
		go func() {
			_, err := store.AppendCompletedToolResultRecoverySidecarFenced(ctx, fixture.fence, fixture.version, fixture.input)
			appendDone <- err
		}()
		deadline := time.Now().Add(5 * time.Second)
		for {
			var waiters int
			err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM pg_stat_activity WHERE datname = current_database()
				AND wait_event_type = 'Lock' AND query LIKE 'INSERT INTO event_chunks%'`).Scan(&waiters)
			if err != nil {
				t.Fatal(err)
			}
			if waiters > 0 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("V3 append did not reach event chunk gate")
			}
			time.Sleep(10 * time.Millisecond)
		}
		bindings, err := NewSQLBindingJournal(db, SQLDialectPostgres)
		if err != nil {
			t.Fatal(err)
		}
		bindingDone := make(chan error, 1)
		go func() {
			bindingDone <- bindings.Record(ctx, BindingRecord{ID: "pg-sidecar-epoch-lock", Kind: "policy", Payload: []byte(`{"scope":"global"}`)})
		}()
		select {
		case err := <-bindingDone:
			t.Fatalf("binding completed before V3 epoch lock release: %v", err)
		case <-time.After(100 * time.Millisecond):
		}
		if err := gate.Commit(); err != nil {
			t.Fatal(err)
		}
		gateReleased = true
		select {
		case err := <-appendDone:
			if err != nil {
				t.Fatalf("gated V3 append: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("V3 append did not finish")
		}
		select {
		case err := <-bindingDone:
			if err != nil {
				t.Fatalf("binding after V3 append: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("binding did not finish")
		}
		assertCompletedToolResultRecoverySidecar(t, fixture)
	})

	t.Run("append_serializes_prune_and_preserves_journal_proof", func(t *testing.T) {
		fixture := newCompletedToolResultRecoverySidecarFixture(t, store, "session-pg-sidecar-prune", "run-pg-sidecar-prune")
		if _, err := db.ExecContext(ctx, `CREATE TABLE sidecar_prune_gate (id INTEGER PRIMARY KEY)`); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(ctx, `INSERT INTO sidecar_prune_gate (id) VALUES (1)`); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(ctx, `CREATE OR REPLACE FUNCTION sidecar_prune_gate_lock() RETURNS trigger AS $$
			BEGIN PERFORM 1 FROM sidecar_prune_gate WHERE id = 1 FOR UPDATE; RETURN NEW; END;
		$$ LANGUAGE plpgsql`); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(ctx, `CREATE TRIGGER sidecar_prune_gate_trigger
			BEFORE INSERT ON event_chunks FOR EACH ROW EXECUTE FUNCTION sidecar_prune_gate_lock()`); err != nil {
			t.Fatal(err)
		}
		gate, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := gate.ExecContext(ctx, `SELECT id FROM sidecar_prune_gate WHERE id = 1 FOR UPDATE`); err != nil {
			t.Fatal(err)
		}
		appendDone := make(chan error, 1)
		go func() {
			_, err := store.AppendCompletedToolResultRecoverySidecarFenced(ctx, fixture.fence, fixture.version, fixture.input)
			appendDone <- err
		}()
		deadline := time.Now().Add(5 * time.Second)
		for {
			var waiters int
			if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM pg_stat_activity WHERE datname = current_database()
				AND wait_event_type = 'Lock' AND query LIKE 'INSERT INTO event_chunks%'`).Scan(&waiters); err != nil {
				t.Fatal(err)
			}
			if waiters > 0 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("V3 append did not reach prune gate")
			}
			time.Sleep(10 * time.Millisecond)
		}
		pruneDone := make(chan struct {
			deleted int64
			err     error
		}, 1)
		go func() {
			deleted, err := store.PruneToolInvocations(ctx, time.Now().UTC().Add(time.Hour))
			pruneDone <- struct {
				deleted int64
				err     error
			}{deleted: deleted, err: err}
		}()
		select {
		case outcome := <-pruneDone:
			t.Fatalf("prune completed before append released epoch: %+v", outcome)
		case <-time.After(100 * time.Millisecond):
		}
		if err := gate.Commit(); err != nil {
			t.Fatal(err)
		}
		if err := <-appendDone; err != nil {
			t.Fatal(err)
		}
		outcome := <-pruneDone
		if outcome.err != nil {
			t.Fatalf("prune outcome=%+v", outcome)
		}
		assertCompletedToolResultRecoverySidecar(t, fixture)
	})

	for _, test := range []struct {
		name, table, column, value string
	}{
		{name: "event_chunk_mutation", table: "event_chunks", column: "payload", value: "{}\\n"},
		{name: "sidecar_mutation", table: "completed_tool_result_recovery_sidecars", column: "composition_sha256", value: strings.Repeat("0", 64)},
	} {
		t.Run(test.name+"_rolls_back", func(t *testing.T) {
			fixture := newCompletedToolResultRecoverySidecarFixture(t, store, "session-pg-"+test.name, "run-pg-"+test.name)
			function := "mutate_" + test.name
			trigger := function + "_trigger"
			if _, err := db.ExecContext(ctx, fmt.Sprintf(`CREATE OR REPLACE FUNCTION %s() RETURNS trigger AS $$
				BEGIN NEW.%s := %s; RETURN NEW; END;
			$$ LANGUAGE plpgsql`, function, test.column, postgresStringLiteral(test.value))); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, fmt.Sprintf("CREATE TRIGGER %s BEFORE INSERT ON %s FOR EACH ROW EXECUTE FUNCTION %s()", trigger, test.table, function)); err != nil {
				t.Fatal(err)
			}
			if appended, err := store.AppendCompletedToolResultRecoverySidecarFenced(ctx, fixture.fence, fixture.version, fixture.input); appended || !errors.Is(err, ErrCompletedToolResultProofInvalid) {
				t.Fatalf("postgres mutated append=%t err=%v", appended, err)
			}
			assertFencedSessionVersion(t, fixture.fencedSQLFixture, fixture.version)
			if _, err := db.ExecContext(ctx, fmt.Sprintf("DROP TRIGGER %s ON %s", trigger, test.table)); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, fmt.Sprintf("DROP FUNCTION %s()", function)); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func postgresStringLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func TestPostgresCompletedToolResultRecoverySidecarFreshV43(t *testing.T) {
	db := newPostgresTestDB(t)
	store, err := OpenSQLSessionStore(context.Background(), db, SQLDialectPostgres)
	if err != nil {
		t.Fatal(err)
	}
	exists, err := sqlTableExists(context.Background(), store.db, store.dialect, "completed_tool_result_recovery_sidecars")
	if err != nil || !exists {
		t.Fatalf("postgres fresh v43 table exists=%t err=%v", exists, err)
	}
}

func ExampleCompletedToolResultRecoverySidecarInput() {
	input := CompletedToolResultRecoverySidecarInput{ResultDigest: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}
	fmt.Println(len(input.ResultDigest))
	// Output: 64
}
