package storage

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
)

func TestNormalizeNativeQueuedWitnessCapability(t *testing.T) {
	base := core.SnapshotCapability{Manifest: core.CapabilityManifest{ID: "normalize.tool", Version: "v1", Name: "Normalize", Kind: core.KindTool, Tool: &core.ToolExposure{}}, Source: core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})}
	empty := base
	empty.Manifest.RequiredPermissions = []core.Permission{}
	empty.Manifest.RequiredCredentials = []core.CredentialRef{}
	empty.Manifest.InputSchema = map[string]any{}
	empty.Manifest.OutputSchema = map[string]any{}
	empty.Manifest.Metadata = map[string]string{}
	empty.Manifest.Tool = &core.ToolExposure{Parameters: map[string]any{}}
	if !reflect.DeepEqual(normalizeNativeQueuedWitnessCapability(base), normalizeNativeQueuedWitnessCapability(empty)) {
		t.Fatal("nil and empty capability collections did not normalize equally")
	}
	for _, test := range []struct {
		name   string
		mutate func(*core.SnapshotCapability)
	}{
		{"permission", func(c *core.SnapshotCapability) { c.Manifest.RequiredPermissions = []core.Permission{core.PermWrite} }},
		{"credential", func(c *core.SnapshotCapability) {
			c.Manifest.RequiredCredentials = []core.CredentialRef{"secret"}
		}},
		{"schema", func(c *core.SnapshotCapability) { c.Manifest.InputSchema = map[string]any{"type": "object"} }},
		{"metadata", func(c *core.SnapshotCapability) { c.Manifest.Metadata = map[string]string{"mode": "strict"} }},
		{"tool", func(c *core.SnapshotCapability) {
			c.Manifest.Tool = &core.ToolExposure{Parameters: map[string]any{"type": "object"}}
		}},
		{"source", func(c *core.SnapshotCapability) {
			c.Source = core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "other"})
		}},
		{"provider", func(c *core.SnapshotCapability) { c.ProviderRevision = "v2" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			changed := base
			test.mutate(&changed)
			if reflect.DeepEqual(normalizeNativeQueuedWitnessCapability(base), normalizeNativeQueuedWitnessCapability(changed)) {
				t.Fatal("non-empty contract drift was normalized away")
			}
		})
	}
}

type nativeQueuedToolEffectFixture struct {
	*fencedSQLFixture
	input   NativeQueuedToolEffectWitnessInput
	version int64
}

func newSQLiteNativeQueuedToolEffectFixture(t *testing.T) *nativeQueuedToolEffectFixture {
	t.Helper()
	return newSQLiteNativeQueuedToolEffectFixtureWithIdempotence(t, true)
}

func newSQLiteNativeQueuedToolEffectFixtureWithIdempotence(t *testing.T, idempotent bool) *nativeQueuedToolEffectFixture {
	t.Helper()
	return newNativeQueuedToolEffectFixture(t, newTestSQLStore(t), "session-native-effect", "run-native-effect", idempotent)
}

func newNativeQueuedToolEffectFixture(t *testing.T, store *SQLSessionStore, sessionID, runID string, idempotent bool) *nativeQueuedToolEffectFixture {
	t.Helper()
	return newNativeQueuedToolEffectFixtureWithMetadata(t, store, sessionID, runID, idempotent, map[string]string{"harness.assignment.id": "native-effect"})
}

func newNativeQueuedToolEffectFixtureWithMetadata(t *testing.T, store *SQLSessionStore, sessionID, runID string, idempotent bool, metadata map[string]string) *nativeQueuedToolEffectFixture {
	t.Helper()
	session := mustNamedSession(t, sessionID)
	capability := core.SnapshotCapability{Manifest: core.CapabilityManifest{
		ID: "test.native.effect", Version: "v1", Name: "Native effect", Kind: core.KindTool,
		Tool: &core.ToolExposure{Parameters: map[string]any{"type": "object"}}, Idempotent: idempotent,
	}, Source: session.Scope(), ProviderRevision: "provider-v1"}
	return newNativeQueuedToolEffectFixtureWithCapability(t, store, session, runID, capability, nil, metadata)
}

func newNativeQueuedToolEffectFixtureWithCapability(t *testing.T, store *SQLSessionStore, session *core.Session, runID string, capability core.SnapshotCapability, permissions core.PermissionSet, metadata map[string]string) *nativeQueuedToolEffectFixture {
	t.Helper()
	composition := &core.RunCompositionData{
		Profile: core.AgentProfileSnapshot{
			ID: "profile-native-effect-snapshot", ProfileID: session.ProfileID(), Scope: session.Scope(),
			Capabilities: []string{capability.Manifest.ID},
		},
		Capabilities:         []core.SnapshotCapability{capability},
		EffectivePermissions: permissions,
		Metadata:             metadata,
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
		ProfileSnapshotID: composition.Profile.ID, CapabilitySnapshotID: "capabilities-native-effect",
		CompositionRevision: compositionRevision, AssignmentRevision: assignmentRevision, Composition: composition,
	}); err != nil {
		t.Fatal(err)
	}
	step, err := session.Append(runID, core.EvStepStart, core.StepData{Index: 0})
	if err != nil {
		t.Fatal(err)
	}
	call := core.ToolCall{ID: "call-native-effect", Name: capability.Manifest.ID, Args: map[string]any{"value": "x"}}
	if _, err := session.Append(runID, core.EvAssistantMessage, core.AssistantMessageData{ToolCall: &call, ToolCalls: []core.ToolCall{call}}); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Append(runID, core.EvRunUsage, core.RunUsageData{InvocationID: "model:1"}); err != nil {
		t.Fatal(err)
	}
	if step.Seq != 1 {
		t.Fatalf("step seq=%d want=1", step.Seq)
	}
	if _, err := session.Append(runID, core.EvToolCall, core.ToolCallData{CallID: call.ID, Name: call.Name, Args: call.Args}); err != nil {
		t.Fatal(err)
	}
	if err := store.Create(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	queue, err := NewSQLRunControlStore(store.db, store.dialect)
	if err != nil {
		t.Fatal(err)
	}
	principal := session.Principal()
	if err := queue.EnqueueRun(context.Background(), QueuedRun{RunRecord: RunRecord{
		RunID: runID, SessionID: session.ID(), TenantID: principal.TenantID, SubjectID: principal.SubjectID,
	}, Message: "native effect", MaxAttempts: 3}); err != nil {
		t.Fatal(err)
	}
	claim, ok, err := queue.ClaimRun(context.Background(), "worker-native-effect", time.Hour)
	if err != nil || !ok {
		t.Fatalf("claim ok=%t err=%v", ok, err)
	}
	holder := "lease-native-effect"
	if acquired, err := store.AcquireSessionLease(context.Background(), session.ID(), holder, time.Hour); err != nil || !acquired {
		t.Fatalf("lease acquired=%t err=%v", acquired, err)
	}
	invocation, err := core.NewToolInvocation(core.RunInfo{RunID: runID, SessionID: session.ID(), Principal: principal}, call, capability.Manifest.Idempotent)
	if err != nil {
		t.Fatal(err)
	}
	epoch, err := store.AuthorizationEpoch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	base := &fencedSQLFixture{store: store, queue: queue, session: session, claim: claim, fence: SessionWriteFence{
		SessionID: session.ID(), RunID: runID, TenantID: principal.TenantID, SubjectID: principal.SubjectID,
		WorkerID: claim.WorkerID, QueueGeneration: claim.Generation, LeaseHolder: holder,
	}}
	return &nativeQueuedToolEffectFixture{fencedSQLFixture: base, version: session.Version(), input: NativeQueuedToolEffectWitnessInput{
		Invocation: invocation, AuthorizationEpoch: epoch, BootstrapRevision: "bootstrap-v1", ExpectedCapability: capability,
	}}
}

func advanceNativeQueuedToolEffectGeneration(t *testing.T, fixture *nativeQueuedToolEffectFixture) SessionWriteFence {
	t.Helper()
	next := fixture.fence
	next.WorkerID = "worker-native-effect-next"
	next.QueueGeneration++
	next.LeaseHolder = "lease-native-effect-next"
	expires := time.Now().UTC().Add(time.Hour).UnixMilli()
	if _, err := fixture.store.db.ExecContext(context.Background(), (sqlQuery{"UPDATE run_queue SET worker_id = ?, generation = ?, lease_expires_at = ? WHERE run_id = ?"}).bind(fixture.store.dialect), next.WorkerID, next.QueueGeneration, expires, next.RunID); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.db.ExecContext(context.Background(), (sqlQuery{"UPDATE session_leases SET holder = ?, expires_at = ? WHERE session_id = ?"}).bind(fixture.store.dialect), next.LeaseHolder, expires, next.SessionID); err != nil {
		t.Fatal(err)
	}
	return next
}

func TestSQLSessionStoreBeginNativeQueuedToolEffectFenced(t *testing.T) {
	fixture := newSQLiteNativeQueuedToolEffectFixture(t)
	record, decision, err := fixture.store.BeginNativeQueuedToolEffectFenced(context.Background(), fixture.fence, fixture.version, fixture.input)
	if err != nil || decision != core.ToolInvocationExecuteNew || record.State != core.ToolInvocationStarted {
		t.Fatalf("first begin record=%+v decision=%q err=%v", record, decision, err)
	}
	record, decision, err = fixture.store.BeginNativeQueuedToolEffectFenced(context.Background(), fixture.fence, fixture.version, fixture.input)
	if err != nil || decision != core.ToolInvocationExecuteRetry || record.State != core.ToolInvocationStarted {
		t.Fatalf("response-lost begin record=%+v decision=%q err=%v", record, decision, err)
	}
	var witnesses int
	if err := fixture.store.db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM native_queued_tool_effect_witnesses").Scan(&witnesses); err != nil || witnesses != 1 {
		t.Fatalf("witnesses=%d err=%v", witnesses, err)
	}
}

func TestSQLSessionStoreBeginNativeQueuedToolEffectRejectsJournalWithoutWitness(t *testing.T) {
	fixture := newSQLiteNativeQueuedToolEffectFixture(t)
	journal, err := NewSQLToolInvocationJournal(fixture.store.db, fixture.store.dialect)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := journal.BeginToolInvocation(context.Background(), fixture.input.Invocation); err != nil {
		t.Fatal(err)
	}
	if _, _, err := fixture.store.BeginNativeQueuedToolEffectFenced(context.Background(), fixture.fence, fixture.version, fixture.input); !errors.Is(err, ErrCompletedToolResultProofInvalid) {
		t.Fatalf("journal without witness error=%v", err)
	}
	var witnesses int
	if err := fixture.store.db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM native_queued_tool_effect_witnesses").Scan(&witnesses); err != nil || witnesses != 0 {
		t.Fatalf("witnesses=%d err=%v", witnesses, err)
	}
}

func TestSQLSessionStorePruneRetainsNativeQueuedToolEffectWitness(t *testing.T) {
	fixture := newSQLiteNativeQueuedToolEffectFixture(t)
	if _, _, err := fixture.store.BeginNativeQueuedToolEffectFenced(context.Background(), fixture.fence, fixture.version, fixture.input); err != nil {
		t.Fatal(err)
	}
	journal, err := NewSQLToolInvocationJournal(fixture.store.db, fixture.store.dialect)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := journal.CompleteToolInvocation(context.Background(), fixture.input.Invocation, core.CapabilityResult{Content: "done", OK: true}); err != nil {
		t.Fatal(err)
	}
	if deleted, err := fixture.store.PruneToolInvocations(context.Background(), time.Now().UTC().Add(time.Hour)); err != nil || deleted != 0 {
		t.Fatalf("prune deleted=%d err=%v", deleted, err)
	}
	if record, found, err := journal.GetToolInvocation(context.Background(), fixture.input.Invocation); err != nil || !found || record.State != core.ToolInvocationCompleted {
		t.Fatalf("journal record=%+v found=%t err=%v", record, found, err)
	}
}

func TestSQLSessionStoreNativeQueuedToolEffectDecisionsAcrossGenerations(t *testing.T) {
	t.Run("idempotent_started_gets_new_admission", func(t *testing.T) {
		fixture := newSQLiteNativeQueuedToolEffectFixture(t)
		if _, _, err := fixture.store.BeginNativeQueuedToolEffectFenced(context.Background(), fixture.fence, fixture.version, fixture.input); err != nil {
			t.Fatal(err)
		}
		next := advanceNativeQueuedToolEffectGeneration(t, fixture)
		_, decision, err := fixture.store.BeginNativeQueuedToolEffectFenced(context.Background(), next, fixture.version, fixture.input)
		if err != nil || decision != core.ToolInvocationExecuteRetry {
			t.Fatalf("decision=%q err=%v", decision, err)
		}
		var witnesses int
		if err := fixture.store.db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM native_queued_tool_effect_witnesses").Scan(&witnesses); err != nil || witnesses != 2 {
			t.Fatalf("witnesses=%d err=%v", witnesses, err)
		}
	})
	t.Run("completed_replays_without_new_effect_admission", func(t *testing.T) {
		fixture := newSQLiteNativeQueuedToolEffectFixture(t)
		if _, _, err := fixture.store.BeginNativeQueuedToolEffectFenced(context.Background(), fixture.fence, fixture.version, fixture.input); err != nil {
			t.Fatal(err)
		}
		journal, _ := NewSQLToolInvocationJournal(fixture.store.db, fixture.store.dialect)
		if _, err := journal.CompleteToolInvocation(context.Background(), fixture.input.Invocation, core.CapabilityResult{Content: "done", OK: true}); err != nil {
			t.Fatal(err)
		}
		next := advanceNativeQueuedToolEffectGeneration(t, fixture)
		_, decision, err := fixture.store.BeginNativeQueuedToolEffectFenced(context.Background(), next, fixture.version, fixture.input)
		if err != nil || decision != core.ToolInvocationReplay {
			t.Fatalf("decision=%q err=%v", decision, err)
		}
		var witnesses int
		_ = fixture.store.db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM native_queued_tool_effect_witnesses").Scan(&witnesses)
		if witnesses != 1 {
			t.Fatalf("witnesses=%d want=1", witnesses)
		}
	})
	t.Run("non_idempotent_started_stays_unknown", func(t *testing.T) {
		fixture := newSQLiteNativeQueuedToolEffectFixtureWithIdempotence(t, false)
		if _, _, err := fixture.store.BeginNativeQueuedToolEffectFenced(context.Background(), fixture.fence, fixture.version, fixture.input); err != nil {
			t.Fatal(err)
		}
		next := advanceNativeQueuedToolEffectGeneration(t, fixture)
		_, decision, err := fixture.store.BeginNativeQueuedToolEffectFenced(context.Background(), next, fixture.version, fixture.input)
		if err != nil || decision != core.ToolInvocationUnknown {
			t.Fatalf("decision=%q err=%v", decision, err)
		}
		var witnesses int
		_ = fixture.store.db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM native_queued_tool_effect_witnesses").Scan(&witnesses)
		if witnesses != 1 {
			t.Fatalf("witnesses=%d want=1", witnesses)
		}
	})
}

func TestSQLSessionStoreNativeQueuedToolEffectFailuresRollback(t *testing.T) {
	t.Run("empty_assignment_metadata", func(t *testing.T) {
		fixture := newNativeQueuedToolEffectFixtureWithMetadata(t, newTestSQLStore(t), "session-native-empty-metadata", "run-native-empty-metadata", true, nil)
		if _, decision, err := fixture.store.BeginNativeQueuedToolEffectFenced(context.Background(), fixture.fence, fixture.version, fixture.input); err != nil || decision != core.ToolInvocationExecuteNew {
			t.Fatalf("decision=%q err=%v", decision, err)
		}
	})
	t.Run("created_at_mutation", func(t *testing.T) {
		fixture := newSQLiteNativeQueuedToolEffectFixture(t)
		if _, err := fixture.store.db.ExecContext(context.Background(), `CREATE TRIGGER mutate_native_effect_created AFTER INSERT ON native_queued_tool_effect_witnesses
			BEGIN UPDATE native_queued_tool_effect_witnesses SET created_at = created_at + 1 WHERE session_id = NEW.session_id AND run_id = NEW.run_id AND call_id = NEW.call_id AND queue_generation = NEW.queue_generation; END`); err != nil {
			t.Fatal(err)
		}
		if _, _, err := fixture.store.BeginNativeQueuedToolEffectFenced(context.Background(), fixture.fence, fixture.version, fixture.input); !errors.Is(err, ErrCompletedToolResultProofInvalid) {
			t.Fatalf("created_at mutation error=%v", err)
		}
		assertNativeQueuedToolEffectRollback(t, fixture)
	})
	t.Run("final_fence_loss", func(t *testing.T) {
		fixture := newSQLiteNativeQueuedToolEffectFixture(t)
		if _, err := fixture.store.db.ExecContext(context.Background(), `CREATE TRIGGER mutate_native_effect_fence AFTER INSERT ON native_queued_tool_effect_witnesses
			BEGIN UPDATE run_queue SET worker_id = 'worker-replaced' WHERE run_id = NEW.run_id; END`); err != nil {
			t.Fatal(err)
		}
		if _, _, err := fixture.store.BeginNativeQueuedToolEffectFenced(context.Background(), fixture.fence, fixture.version, fixture.input); !errors.Is(err, ErrSessionWriteFenceLost) {
			t.Fatalf("fence mutation error=%v", err)
		}
		assertNativeQueuedToolEffectRollback(t, fixture)
	})
	t.Run("response_lost_contract_mismatch", func(t *testing.T) {
		fixture := newSQLiteNativeQueuedToolEffectFixture(t)
		if _, _, err := fixture.store.BeginNativeQueuedToolEffectFenced(context.Background(), fixture.fence, fixture.version, fixture.input); err != nil {
			t.Fatal(err)
		}
		mismatch := fixture.input
		mismatch.BootstrapRevision = "bootstrap-v2"
		if _, _, err := fixture.store.BeginNativeQueuedToolEffectFenced(context.Background(), fixture.fence, fixture.version, mismatch); !errors.Is(err, ErrCompletedToolResultProofInvalid) {
			t.Fatalf("contract mismatch error=%v", err)
		}
	})
	t.Run("epoch_change", func(t *testing.T) {
		fixture := newSQLiteNativeQueuedToolEffectFixture(t)
		fixture.input.AuthorizationEpoch++
		if _, _, err := fixture.store.BeginNativeQueuedToolEffectFenced(context.Background(), fixture.fence, fixture.version, fixture.input); !errors.Is(err, ErrAuthorizationEpochChanged) {
			t.Fatalf("epoch error=%v", err)
		}
		assertNativeQueuedToolEffectRollback(t, fixture)
	})
}

func TestSQLSchemaV43MigratesNativeQueuedToolEffectWitnesses(t *testing.T) {
	store := newTestSQLStore(t)
	ctx := context.Background()
	if _, err := store.db.ExecContext(ctx, "DROP TABLE native_queued_tool_effect_witnesses"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(ctx, sqlUpdateMetaRow.bind(store.dialect), "43"); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenSQLSessionStore(ctx, store.db, store.dialect)
	if err != nil {
		t.Fatal(err)
	}
	var version string
	if err := reopened.db.QueryRowContext(ctx, sqlSelectMetaRow.bind(reopened.dialect)).Scan(&version); err != nil || version != "44" {
		t.Fatalf("schema version=%q err=%v", version, err)
	}
	var witnesses int
	if err := reopened.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM native_queued_tool_effect_witnesses").Scan(&witnesses); err != nil || witnesses != 0 {
		t.Fatalf("witnesses=%d err=%v", witnesses, err)
	}
}

func assertNativeQueuedToolEffectRollback(t *testing.T, fixture *nativeQueuedToolEffectFixture) {
	t.Helper()
	var witnesses, journal int
	if err := fixture.store.db.QueryRowContext(context.Background(), (sqlQuery{"SELECT COUNT(*) FROM native_queued_tool_effect_witnesses WHERE session_id = ? AND run_id = ? AND call_id = ?"}).bind(fixture.store.dialect), fixture.input.Invocation.SessionID, fixture.input.Invocation.RunID, fixture.input.Invocation.CallID).Scan(&witnesses); err != nil {
		t.Fatal(err)
	}
	if err := fixture.store.db.QueryRowContext(context.Background(), (sqlQuery{"SELECT COUNT(*) FROM tool_invocations WHERE session_id = ? AND run_id = ? AND call_id = ?"}).bind(fixture.store.dialect), fixture.input.Invocation.SessionID, fixture.input.Invocation.RunID, fixture.input.Invocation.CallID).Scan(&journal); err != nil {
		t.Fatal(err)
	}
	if witnesses != 0 || journal != 0 {
		t.Fatalf("rollback witnesses=%d journal=%d", witnesses, journal)
	}
}

func TestPostgresSQLSessionStoreBeginNativeQueuedToolEffectFenced(t *testing.T) {
	ctx := newPostgresFenceTestContext(t)
	db := newPostgresTestDB(t)
	store, err := OpenSQLSessionStore(ctx, db, SQLDialectPostgres)
	if err != nil {
		t.Fatal(err)
	}
	fixture := newNativeQueuedToolEffectFixture(t, store, "session-pg-native-effect", "run-pg-native-effect", true)
	if _, decision, err := store.BeginNativeQueuedToolEffectFenced(ctx, fixture.fence, fixture.version, fixture.input); err != nil || decision != core.ToolInvocationExecuteNew {
		t.Fatalf("first decision=%q err=%v", decision, err)
	}
	if _, decision, err := store.BeginNativeQueuedToolEffectFenced(ctx, fixture.fence, fixture.version, fixture.input); err != nil || decision != core.ToolInvocationExecuteRetry {
		t.Fatalf("retry decision=%q err=%v", decision, err)
	}
	next := advanceNativeQueuedToolEffectGeneration(t, fixture)
	if _, decision, err := store.BeginNativeQueuedToolEffectFenced(ctx, next, fixture.version, fixture.input); err != nil || decision != core.ToolInvocationExecuteRetry {
		t.Fatalf("next generation decision=%q err=%v", decision, err)
	}
	journal, err := NewSQLToolInvocationJournal(store.db, store.dialect)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := journal.CompleteToolInvocation(ctx, fixture.input.Invocation, core.CapabilityResult{Content: "done", OK: true}); err != nil {
		t.Fatal(err)
	}
	next.WorkerID = "worker-native-effect-replay"
	next.QueueGeneration++
	next.LeaseHolder = "lease-native-effect-replay"
	expires := time.Now().UTC().Add(time.Hour).UnixMilli()
	if _, err := store.db.ExecContext(ctx, "UPDATE run_queue SET worker_id = $1, generation = $2, lease_expires_at = $3 WHERE run_id = $4", next.WorkerID, next.QueueGeneration, expires, next.RunID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(ctx, "UPDATE session_leases SET holder = $1, expires_at = $2 WHERE session_id = $3", next.LeaseHolder, expires, next.SessionID); err != nil {
		t.Fatal(err)
	}
	if _, decision, err := store.BeginNativeQueuedToolEffectFenced(ctx, next, fixture.version, fixture.input); err != nil || decision != core.ToolInvocationReplay {
		t.Fatalf("replay decision=%q err=%v", decision, err)
	}
	if deleted, err := store.PruneToolInvocations(ctx, time.Now().UTC().Add(time.Hour)); err != nil || deleted != 0 {
		t.Fatalf("prune deleted=%d err=%v", deleted, err)
	}
	t.Run("empty_metadata", func(t *testing.T) {
		candidate := newNativeQueuedToolEffectFixtureWithMetadata(t, store, "session-pg-native-empty", "run-pg-native-empty", true, nil)
		if _, decision, err := store.BeginNativeQueuedToolEffectFenced(ctx, candidate.fence, candidate.version, candidate.input); err != nil || decision != core.ToolInvocationExecuteNew {
			t.Fatalf("decision=%q err=%v", decision, err)
		}
	})
	t.Run("journal_without_witness", func(t *testing.T) {
		candidate := newNativeQueuedToolEffectFixture(t, store, "session-pg-native-no-witness", "run-pg-native-no-witness", true)
		journal, err := NewSQLToolInvocationJournal(store.db, store.dialect)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := journal.BeginToolInvocation(ctx, candidate.input.Invocation); err != nil {
			t.Fatal(err)
		}
		if _, _, err := store.BeginNativeQueuedToolEffectFenced(ctx, candidate.fence, candidate.version, candidate.input); !errors.Is(err, ErrCompletedToolResultProofInvalid) {
			t.Fatalf("error=%v", err)
		}
	})
	t.Run("permission_mismatch", func(t *testing.T) {
		session := mustNamedSession(t, "session-pg-native-permission")
		capability := core.SnapshotCapability{Manifest: core.CapabilityManifest{
			ID: "test.native.permission", Version: "v1", Name: "Permission", Kind: core.KindTool,
			RequiredPermissions: []core.Permission{"test.effect"}, Idempotent: true,
			Tool: &core.ToolExposure{Parameters: map[string]any{"type": "object"}},
		}, Source: session.Scope(), ProviderRevision: "provider-v1"}
		candidate := newNativeQueuedToolEffectFixtureWithCapability(t, store, session, "run-pg-native-permission", capability, nil, map[string]string{"harness.assignment.id": "permission"})
		if _, _, err := store.BeginNativeQueuedToolEffectFenced(ctx, candidate.fence, candidate.version, candidate.input); !errors.Is(err, ErrCompletedToolResultProofInvalid) {
			t.Fatalf("error=%v", err)
		}
	})
	t.Run("final_fence_loss_rolls_back", func(t *testing.T) {
		candidate := newNativeQueuedToolEffectFixture(t, store, "session-pg-native-fence", "run-pg-native-fence", true)
		if _, err := db.ExecContext(ctx, `CREATE OR REPLACE FUNCTION native_effect_fence_loss() RETURNS trigger AS $$
			BEGIN UPDATE run_queue SET worker_id = 'worker-replaced' WHERE run_id = NEW.run_id; RETURN NEW; END;
		$$ LANGUAGE plpgsql`); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(ctx, `CREATE TRIGGER native_effect_fence_loss_trigger
			AFTER INSERT ON native_queued_tool_effect_witnesses FOR EACH ROW EXECUTE FUNCTION native_effect_fence_loss()`); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			_, _ = db.ExecContext(context.Background(), "DROP TRIGGER IF EXISTS native_effect_fence_loss_trigger ON native_queued_tool_effect_witnesses")
			_, _ = db.ExecContext(context.Background(), "DROP FUNCTION IF EXISTS native_effect_fence_loss()")
		})
		if _, _, err := store.BeginNativeQueuedToolEffectFenced(ctx, candidate.fence, candidate.version, candidate.input); !errors.Is(err, ErrSessionWriteFenceLost) {
			t.Fatalf("error=%v", err)
		}
		assertNativeQueuedToolEffectRollback(t, candidate)
		if _, err := db.ExecContext(ctx, "DROP TRIGGER native_effect_fence_loss_trigger ON native_queued_tool_effect_witnesses"); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(ctx, "DROP FUNCTION native_effect_fence_loss()"); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("non_idempotent_new_generation_is_unknown", func(t *testing.T) {
		candidate := newNativeQueuedToolEffectFixture(t, store, "session-pg-native-unknown", "run-pg-native-unknown", false)
		if _, decision, err := store.BeginNativeQueuedToolEffectFenced(ctx, candidate.fence, candidate.version, candidate.input); err != nil || decision != core.ToolInvocationExecuteNew {
			t.Fatalf("first decision=%q err=%v", decision, err)
		}
		next := advanceNativeQueuedToolEffectGeneration(t, candidate)
		if _, decision, err := store.BeginNativeQueuedToolEffectFenced(ctx, next, candidate.version, candidate.input); err != nil || decision != core.ToolInvocationUnknown {
			t.Fatalf("unknown decision=%q err=%v", decision, err)
		}
		var witnesses int
		if err := store.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM native_queued_tool_effect_witnesses WHERE session_id = $1", candidate.fence.SessionID).Scan(&witnesses); err != nil || witnesses != 1 {
			t.Fatalf("witnesses=%d err=%v", witnesses, err)
		}
	})
	for _, test := range []struct {
		name, body string
		want       error
	}{
		{name: "final_epoch_change", body: `UPDATE store_meta SET value = CAST(CAST(value AS BIGINT) + 1 AS TEXT) WHERE key = 'authorization_epoch';`, want: ErrAuthorizationEpochChanged},
		{name: "witness_mutation", body: `UPDATE native_queued_tool_effect_witnesses SET created_at = created_at + 1 WHERE session_id = NEW.session_id AND run_id = NEW.run_id AND call_id = NEW.call_id AND queue_generation = NEW.queue_generation;`, want: ErrCompletedToolResultProofInvalid},
	} {
		t.Run(test.name+"_rolls_back", func(t *testing.T) {
			candidate := newNativeQueuedToolEffectFixture(t, store, "session-pg-native-"+test.name, "run-pg-native-"+test.name, true)
			function := "native_effect_" + test.name
			trigger := function + "_trigger"
			if _, err := db.ExecContext(ctx, fmt.Sprintf(`CREATE OR REPLACE FUNCTION %s() RETURNS trigger AS $$
				BEGIN %s RETURN NEW; END;
			$$ LANGUAGE plpgsql`, function, test.body)); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, fmt.Sprintf("CREATE TRIGGER %s AFTER INSERT ON native_queued_tool_effect_witnesses FOR EACH ROW EXECUTE FUNCTION %s()", trigger, function)); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				_, _ = db.ExecContext(context.Background(), fmt.Sprintf("DROP TRIGGER IF EXISTS %s ON native_queued_tool_effect_witnesses", trigger))
				_, _ = db.ExecContext(context.Background(), fmt.Sprintf("DROP FUNCTION IF EXISTS %s()", function))
			})
			if _, _, err := store.BeginNativeQueuedToolEffectFenced(ctx, candidate.fence, candidate.version, candidate.input); !errors.Is(err, test.want) {
				t.Fatalf("error=%v want=%v", err, test.want)
			}
			assertNativeQueuedToolEffectRollback(t, candidate)
			if _, err := db.ExecContext(ctx, fmt.Sprintf("DROP TRIGGER %s ON native_queued_tool_effect_witnesses", trigger)); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, fmt.Sprintf("DROP FUNCTION %s()", function)); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPostgresSchemaV43MigratesNativeQueuedToolEffectWitnesses(t *testing.T) {
	ctx := newPostgresFenceTestContext(t)
	db := newPostgresTestDB(t)
	store, err := OpenSQLSessionStore(ctx, db, SQLDialectPostgres)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "DROP TABLE native_queued_tool_effect_witnesses"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, sqlUpdateMetaRow.bind(SQLDialectPostgres), "43"); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSQLSessionStore(ctx, db, SQLDialectPostgres); err != nil {
		t.Fatal(err)
	}
	var witnesses int
	if err := store.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM native_queued_tool_effect_witnesses").Scan(&witnesses); err != nil || witnesses != 0 {
		t.Fatalf("witnesses=%d err=%v", witnesses, err)
	}
}
