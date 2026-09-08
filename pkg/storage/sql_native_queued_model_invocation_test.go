package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	core "github.com/whhhh1500/auto-agent/pkg/core"
)

type nativeQueuedModelFixture struct {
	*fencedSQLFixture
	input   NativeQueuedModelInvocationInput
	version int64
}

func newNativeQueuedModelFixture(t *testing.T, store *SQLSessionStore, sessionID, runID string) *nativeQueuedModelFixture {
	t.Helper()
	session := mustNamedSession(t, sessionID)
	composition := &core.RunCompositionData{
		Profile: core.AgentProfileSnapshot{ID: "profile-model-snapshot", ProfileID: session.ProfileID(), Scope: session.Scope()},
		Model:   core.ModelSelection{Provider: "configured-provider", Model: "model-v1"}, ResolvedProvider: "resolved-provider", ModelRevision: "adapter-v1",
		Metadata: map[string]string{"harness.assignment.id": "model-attempt"},
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
		ProfileSnapshotID: composition.Profile.ID, CapabilitySnapshotID: "capabilities-model-snapshot",
		CompositionRevision: compositionRevision, AssignmentRevision: assignmentRevision, Composition: composition,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Append(runID, core.EvUserMessage, core.UserMessageData{Text: "model request"}); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Append(runID, core.EvStepStart, core.StepData{Index: 0}); err != nil {
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
	}, Message: "model", MaxAttempts: 3}); err != nil {
		t.Fatal(err)
	}
	claim, ok, err := queue.ClaimRun(context.Background(), "worker-model", time.Hour)
	if err != nil || !ok {
		t.Fatalf("claim ok=%t err=%v", ok, err)
	}
	holder := "lease-model"
	if acquired, err := store.AcquireSessionLease(context.Background(), session.ID(), holder, time.Hour); err != nil || !acquired {
		t.Fatalf("lease acquired=%t err=%v", acquired, err)
	}
	epoch, err := store.AuthorizationEpoch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	base := &fencedSQLFixture{store: store, queue: queue, session: session, claim: claim, fence: SessionWriteFence{
		SessionID: session.ID(), RunID: runID, TenantID: principal.TenantID, SubjectID: principal.SubjectID,
		WorkerID: claim.WorkerID, QueueGeneration: claim.Generation, LeaseHolder: holder,
	}}
	return &nativeQueuedModelFixture{fencedSQLFixture: base, version: session.Version(), input: NativeQueuedModelInvocationInput{
		Request:            core.ModelCallRequest{Principal: principal, Scope: session.Scope(), SessionID: session.ID(), RunID: runID, Step: 0, Provider: composition.Model.Provider, Model: composition.Model.Model},
		AuthorizationEpoch: epoch, BootstrapRevision: "bootstrap-model-v1",
	}}
}

func advanceNativeQueuedModelGeneration(t *testing.T, fixture *nativeQueuedModelFixture) SessionWriteFence {
	t.Helper()
	next := fixture.fence
	next.WorkerID = "worker-model-next"
	next.QueueGeneration++
	next.LeaseHolder = "lease-model-next"
	expires := time.Now().UTC().Add(time.Hour).UnixMilli()
	if _, err := fixture.store.db.ExecContext(context.Background(), (sqlQuery{"UPDATE run_queue SET worker_id = ?, generation = ?, lease_expires_at = ? WHERE run_id = ?"}).bind(fixture.store.dialect), next.WorkerID, next.QueueGeneration, expires, next.RunID); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.db.ExecContext(context.Background(), (sqlQuery{"UPDATE session_leases SET holder = ?, expires_at = ? WHERE session_id = ?"}).bind(fixture.store.dialect), next.LeaseHolder, expires, next.SessionID); err != nil {
		t.Fatal(err)
	}
	return next
}

func completeNativeQueuedModelStep(t *testing.T, session *core.Session, runID string, step int, usageID string, toolCalls []core.ToolCall) {
	t.Helper()
	assistant := core.AssistantMessageData{Text: "completed model step"}
	if len(toolCalls) > 0 {
		first := toolCalls[0]
		assistant.ToolCall = &first
		assistant.ToolCalls = append([]core.ToolCall(nil), toolCalls...)
	}
	if _, err := session.Append(runID, core.EvAssistantMessage, assistant); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Append(runID, core.EvRunUsage, core.RunUsageData{InputTokens: 3, OutputTokens: 2, InvocationID: usageID}); err != nil {
		t.Fatal(err)
	}
	for _, call := range toolCalls {
		if _, err := session.Append(runID, core.EvToolCall, core.ToolCallData{CallID: call.ID, Name: call.Name, Args: call.Args}); err != nil {
			t.Fatal(err)
		}
		if _, err := session.Append(runID, core.EvToolResult, core.ToolResultData{CallID: call.ID, Content: "completed tool result", OK: true}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := session.Append(runID, core.EvStepEnd, core.StepData{Index: step}); err != nil {
		t.Fatal(err)
	}
}

func advanceNativeQueuedModelFixtureToNextStep(t *testing.T, fixture *nativeQueuedModelFixture, usageID string, toolCalls []core.ToolCall) int64 {
	t.Helper()
	previousVersion := fixture.version
	completeNativeQueuedModelStep(t, fixture.session, fixture.fence.RunID, fixture.input.Request.Step, usageID, toolCalls)
	if _, err := fixture.session.Append(fixture.fence.RunID, core.EvStepStart, core.StepData{Index: fixture.input.Request.Step + 1}); err != nil {
		t.Fatal(err)
	}
	if err := fixture.store.AppendEventsFenced(context.Background(), fixture.fence, previousVersion, fixture.session.EventsFrom(previousVersion)); err != nil {
		t.Fatal(err)
	}
	fixture.version = fixture.session.Version()
	fixture.input.Request.Step++
	return fixture.version - 1
}

func TestSQLSessionStoreBeginNativeQueuedModelInvocationFenced(t *testing.T) {
	fixture := newNativeQueuedModelFixture(t, newTestSQLStore(t), "session-model-attempt", "run-model-attempt")
	admitted, err := fixture.store.BeginNativeQueuedModelInvocationFenced(context.Background(), fixture.fence, fixture.version, fixture.input)
	if err != nil || !admitted {
		t.Fatalf("first admitted=%t err=%v", admitted, err)
	}
	if admitted, err := fixture.store.BeginNativeQueuedModelInvocationFenced(context.Background(), fixture.fence, fixture.version, fixture.input); err != nil || admitted {
		t.Fatalf("response-lost admitted=%t err=%v", admitted, err)
	}
	next := advanceNativeQueuedModelGeneration(t, fixture)
	if admitted, err := fixture.store.BeginNativeQueuedModelInvocationFenced(context.Background(), next, fixture.version, fixture.input); err != nil || admitted {
		t.Fatalf("next generation admitted=%t err=%v", admitted, err)
	}
	var rows int
	if err := fixture.store.db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM native_queued_model_invocations").Scan(&rows); err != nil || rows != 1 {
		t.Fatalf("rows=%d err=%v", rows, err)
	}
}

func TestSQLSessionStoreNativeQueuedModelInvocationCanonicalizesEmptyPrincipalAttributes(t *testing.T) {
	fixture := newNativeQueuedModelFixture(t, newTestSQLStore(t), "session-model-principal-empty", "run-model-principal-empty")
	if fixture.input.Request.Principal.Attributes == nil || len(fixture.input.Request.Principal.Attributes) != 0 {
		t.Fatalf("fixture must exercise the empty map representation: %#v", fixture.input.Request.Principal.Attributes)
	}
	if admitted, err := fixture.store.BeginNativeQueuedModelInvocationFenced(context.Background(), fixture.fence, fixture.version, fixture.input); err != nil || !admitted {
		t.Fatalf("admitted=%t err=%v", admitted, err)
	}
	tx, err := fixture.store.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	attempt, found, err := loadNativeQueuedModelInvocation(context.Background(), tx, fixture.store.dialect, fixture.fence.SessionID, fixture.fence.RunID, "model:2", false)
	if err != nil || !found {
		_ = tx.Rollback()
		t.Fatalf("load admission found=%t err=%v", found, err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if attempt.input.Request.Principal.Attributes != nil {
		t.Fatalf("JSON omitempty should round-trip empty attributes as nil, got %#v", attempt.input.Request.Principal.Attributes)
	}
	loaded, err := fixture.store.Load(context.Background(), fixture.session.ID())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := deriveNativeQueuedModelInvocation(loaded, fixture.fence, fixture.version, attempt.input); err != nil {
		t.Fatalf("empty/nil attribute representations must reconstruct the same principal: %v", err)
	}
}

func TestSQLSessionStoreBeginNativeQueuedModelInvocationFencedRejectsChangedPrincipalClaims(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*core.Principal)
	}{
		{
			name: "permission_removed",
			mutate: func(principal *core.Principal) {
				principal.Grants = core.NewPermissionSet(core.PermRead)
			},
		},
		{
			name: "permission_false_value",
			mutate: func(principal *core.Principal) {
				principal.Grants = core.PermissionSet{core.PermRead: true, core.PermWrite: false}
			},
		},
		{
			name: "role_changed",
			mutate: func(principal *core.Principal) {
				principal.Attributes = map[string]string{"role": "tenant_admin"}
			},
		},
		{
			name: "attribute_changed",
			mutate: func(principal *core.Principal) {
				principal.Attributes = map[string]string{"region": "other"}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newNativeQueuedModelFixture(t, newTestSQLStore(t), "session-model-principal-"+test.name, "run-model-principal-"+test.name)
			test.mutate(&fixture.input.Request.Principal)
			if admitted, err := fixture.store.BeginNativeQueuedModelInvocationFenced(context.Background(), fixture.fence, fixture.version, fixture.input); admitted || !errors.Is(err, ErrCompletedToolResultProofInvalid) {
				t.Fatalf("admitted=%t err=%v", admitted, err)
			}
		})
	}
}

func TestSQLSessionStoreBeginNativeQueuedModelInvocationFencedAdmitsAfterCompletedStep(t *testing.T) {
	fixture := newNativeQueuedModelFixture(t, newTestSQLStore(t), "session-model-second-step", "run-model-second-step")
	oldStepStart := fixture.version - 1
	currentStepStart := advanceNativeQueuedModelFixtureToNextStep(t, fixture, fmt.Sprintf("model:%d", oldStepStart), []core.ToolCall{{ID: "call-step-0", Name: "native.echo"}})

	admitted, err := fixture.store.BeginNativeQueuedModelInvocationFenced(context.Background(), fixture.fence, fixture.version, fixture.input)
	if err != nil || !admitted {
		t.Fatalf("admitted=%t err=%v", admitted, err)
	}
	var invocationID string
	var stepIndex int
	var stepStartSeq int64
	if err := fixture.store.db.QueryRowContext(context.Background(), (sqlQuery{"SELECT invocation_id, step_index, step_start_seq FROM native_queued_model_invocations WHERE session_id = ? AND run_id = ?"}).bind(fixture.store.dialect), fixture.fence.SessionID, fixture.fence.RunID).Scan(&invocationID, &stepIndex, &stepStartSeq); err != nil {
		t.Fatal(err)
	}
	if invocationID != fmt.Sprintf("model:%d", currentStepStart) || stepIndex != 1 || stepStartSeq != currentStepStart {
		t.Fatalf("invocation=%q step=%d start=%d", invocationID, stepIndex, stepStartSeq)
	}
}

func TestSQLSessionStoreBeginNativeQueuedModelInvocationFencedRejectsDamagedCompletedStep(t *testing.T) {
	fixture := newNativeQueuedModelFixture(t, newTestSQLStore(t), "session-model-damaged-step", "run-model-damaged-step")
	advanceNativeQueuedModelFixtureToNextStep(t, fixture, "model:999", nil)

	if admitted, err := fixture.store.BeginNativeQueuedModelInvocationFenced(context.Background(), fixture.fence, fixture.version, fixture.input); admitted || !errors.Is(err, ErrCompletedToolResultProofInvalid) {
		t.Fatalf("admitted=%t err=%v", admitted, err)
	}
}

func TestSQLSessionStoreBeginNativeQueuedModelInvocationFencedRejectsUnclosedOldTool(t *testing.T) {
	fixture := newNativeQueuedModelFixture(t, newTestSQLStore(t), "session-model-unclosed-tool", "run-model-unclosed-tool")
	stepStart := fixture.version - 1
	staged, err := fixture.session.Clone()
	if err != nil {
		t.Fatal(err)
	}
	call := core.ToolCall{ID: "call-unclosed", Name: "native.echo"}
	if _, err := staged.Append(fixture.fence.RunID, core.EvAssistantMessage, core.AssistantMessageData{Text: "tool request", ToolCall: &call, ToolCalls: []core.ToolCall{call}}); err != nil {
		t.Fatal(err)
	}
	if _, err := staged.Append(fixture.fence.RunID, core.EvRunUsage, core.RunUsageData{InvocationID: fmt.Sprintf("model:%d", stepStart)}); err != nil {
		t.Fatal(err)
	}
	if _, err := staged.Append(fixture.fence.RunID, core.EvToolCall, core.ToolCallData{CallID: call.ID, Name: call.Name}); err != nil {
		t.Fatal(err)
	}
	if _, err := staged.Append(fixture.fence.RunID, core.EvStepStart, core.StepData{Index: 1}); err != nil {
		t.Fatal(err)
	}
	if err := fixture.store.AppendEventsFenced(context.Background(), fixture.fence, fixture.version, staged.EventsFrom(fixture.version)); err != nil {
		t.Fatal(err)
	}
	fixture.session, fixture.version = staged, staged.Version()
	fixture.input.Request.Step = 1

	if admitted, err := fixture.store.BeginNativeQueuedModelInvocationFenced(context.Background(), fixture.fence, fixture.version, fixture.input); admitted || !errors.Is(err, ErrCompletedToolResultProofInvalid) {
		t.Fatalf("admitted=%t err=%v", admitted, err)
	}
}

func TestSQLSessionStoreBeginNativeQueuedModelInvocationFencedRejectsHistoricalToolIdentityMismatch(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*core.ToolCallData)
	}{
		{
			name:   "name",
			mutate: func(data *core.ToolCallData) { data.Name = "native.other" },
		},
		{
			name:   "arguments",
			mutate: func(data *core.ToolCallData) { data.Args = map[string]any{"n": 8} },
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newNativeQueuedModelFixture(t, newTestSQLStore(t), "session-model-tool-identity-"+test.name, "run-model-tool-identity-"+test.name)
			previousVersion := fixture.version
			staged, err := fixture.session.Clone()
			if err != nil {
				t.Fatal(err)
			}
			call := core.ToolCall{ID: "call-tool-identity", Name: "native.echo", Args: map[string]any{"n": 7}}
			if _, err := staged.Append(fixture.fence.RunID, core.EvAssistantMessage, core.AssistantMessageData{Text: "tool request", ToolCall: &call, ToolCalls: []core.ToolCall{call}}); err != nil {
				t.Fatal(err)
			}
			if _, err := staged.Append(fixture.fence.RunID, core.EvRunUsage, core.RunUsageData{InvocationID: fmt.Sprintf("model:%d", previousVersion-1)}); err != nil {
				t.Fatal(err)
			}
			toolCall := core.ToolCallData{CallID: call.ID, Name: call.Name, Args: call.Args}
			test.mutate(&toolCall)
			if _, err := staged.Append(fixture.fence.RunID, core.EvToolCall, toolCall); err != nil {
				t.Fatal(err)
			}
			if _, err := staged.Append(fixture.fence.RunID, core.EvToolResult, core.ToolResultData{CallID: call.ID, Content: "completed tool result", OK: true}); err != nil {
				t.Fatal(err)
			}
			if _, err := staged.Append(fixture.fence.RunID, core.EvStepEnd, core.StepData{Index: 0}); err != nil {
				t.Fatal(err)
			}
			if _, err := staged.Append(fixture.fence.RunID, core.EvStepStart, core.StepData{Index: 1}); err != nil {
				t.Fatal(err)
			}
			if err := fixture.store.AppendEventsFenced(context.Background(), fixture.fence, previousVersion, staged.EventsFrom(previousVersion)); err != nil {
				t.Fatal(err)
			}
			fixture.session, fixture.version = staged, staged.Version()
			fixture.input.Request.Step = 1

			if admitted, err := fixture.store.BeginNativeQueuedModelInvocationFenced(context.Background(), fixture.fence, fixture.version, fixture.input); admitted || !errors.Is(err, ErrCompletedToolResultProofInvalid) {
				t.Fatalf("admitted=%t err=%v", admitted, err)
			}
		})
	}
}

func TestSQLSessionStoreBeginNativeQueuedModelInvocationFencedBindsLatestOpenStep(t *testing.T) {
	fixture := newNativeQueuedModelFixture(t, newTestSQLStore(t), "session-model-latest-step", "run-model-latest-step")
	firstStepStart := fixture.version - 1
	secondStepStart := advanceNativeQueuedModelFixtureToNextStep(t, fixture, fmt.Sprintf("model:%d", firstStepStart), nil)
	thirdStepStart := advanceNativeQueuedModelFixtureToNextStep(t, fixture, fmt.Sprintf("model:%d", secondStepStart), nil)

	if admitted, err := fixture.store.BeginNativeQueuedModelInvocationFenced(context.Background(), fixture.fence, fixture.version, fixture.input); err != nil || !admitted {
		t.Fatalf("admitted=%t err=%v", admitted, err)
	}
	var stepIndex int
	var stepStartSeq int64
	if err := fixture.store.db.QueryRowContext(context.Background(), (sqlQuery{"SELECT step_index, step_start_seq FROM native_queued_model_invocations WHERE session_id = ? AND run_id = ?"}).bind(fixture.store.dialect), fixture.fence.SessionID, fixture.fence.RunID).Scan(&stepIndex, &stepStartSeq); err != nil {
		t.Fatal(err)
	}
	if stepIndex != 2 || stepStartSeq != thirdStepStart {
		t.Fatalf("step=%d start=%d want step=2 start=%d", stepIndex, stepStartSeq, thirdStepStart)
	}
}

func TestSQLSessionStoreNativeQueuedModelInvocationFailsClosed(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*nativeQueuedModelFixture)
		want   error
	}{
		{"epoch", func(f *nativeQueuedModelFixture) { f.input.AuthorizationEpoch++ }, ErrAuthorizationEpochChanged},
		{"version", func(f *nativeQueuedModelFixture) { f.version++ }, core.ErrSessionConflict},
		{"provider", func(f *nativeQueuedModelFixture) { f.input.Request.Provider = "other" }, ErrCompletedToolResultProofInvalid},
		{"model", func(f *nativeQueuedModelFixture) { f.input.Request.Model = "other" }, ErrCompletedToolResultProofInvalid},
		{"step", func(f *nativeQueuedModelFixture) { f.input.Request.Step++ }, ErrCompletedToolResultProofInvalid},
		{"bootstrap", func(f *nativeQueuedModelFixture) { f.input.BootstrapRevision = "" }, ErrCompletedToolResultProofInvalid},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newNativeQueuedModelFixture(t, newTestSQLStore(t), "session-model-fail-"+test.name, "run-model-fail-"+test.name)
			test.mutate(fixture)
			if admitted, err := fixture.store.BeginNativeQueuedModelInvocationFenced(context.Background(), fixture.fence, fixture.version, fixture.input); admitted || !errors.Is(err, test.want) {
				t.Fatalf("admitted=%t err=%v want=%v", admitted, err, test.want)
			}
		})
	}
	t.Run("row_mutation_rolls_back", func(t *testing.T) {
		fixture := newNativeQueuedModelFixture(t, newTestSQLStore(t), "session-model-mutation", "run-model-mutation")
		if _, err := fixture.store.db.ExecContext(context.Background(), `CREATE TRIGGER mutate_model_attempt AFTER INSERT ON native_queued_model_invocations
			BEGIN UPDATE native_queued_model_invocations SET request_sha256 = '`+strings.Repeat("0", 64)+`' WHERE session_id = NEW.session_id AND run_id = NEW.run_id AND invocation_id = NEW.invocation_id; END`); err != nil {
			t.Fatal(err)
		}
		if admitted, err := fixture.store.BeginNativeQueuedModelInvocationFenced(context.Background(), fixture.fence, fixture.version, fixture.input); admitted || !errors.Is(err, ErrCompletedToolResultProofInvalid) {
			t.Fatalf("admitted=%t err=%v", admitted, err)
		}
		var rows int
		if err := fixture.store.db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM native_queued_model_invocations").Scan(&rows); err != nil || rows != 0 {
			t.Fatalf("rows=%d err=%v", rows, err)
		}
	})
	t.Run("existing_request_mismatch", func(t *testing.T) {
		fixture := newNativeQueuedModelFixture(t, newTestSQLStore(t), "session-model-request-mismatch", "run-model-request-mismatch")
		if admitted, err := fixture.store.BeginNativeQueuedModelInvocationFenced(context.Background(), fixture.fence, fixture.version, fixture.input); err != nil || !admitted {
			t.Fatalf("first admitted=%t err=%v", admitted, err)
		}
		mismatch := fixture.input
		mismatch.Request.Deadline = time.Now().UTC().Add(time.Minute)
		if admitted, err := fixture.store.BeginNativeQueuedModelInvocationFenced(context.Background(), advanceNativeQueuedModelGeneration(t, fixture), fixture.version, mismatch); admitted || !errors.Is(err, ErrCompletedToolResultProofInvalid) {
			t.Fatalf("mismatch admitted=%t err=%v", admitted, err)
		}
	})
	t.Run("final_fence_loss_rolls_back", func(t *testing.T) {
		fixture := newNativeQueuedModelFixture(t, newTestSQLStore(t), "session-model-fence", "run-model-fence")
		if _, err := fixture.store.db.ExecContext(context.Background(), `CREATE TRIGGER mutate_model_fence AFTER INSERT ON native_queued_model_invocations
			BEGIN UPDATE run_queue SET worker_id = 'worker-replaced' WHERE run_id = NEW.run_id; END`); err != nil {
			t.Fatal(err)
		}
		if admitted, err := fixture.store.BeginNativeQueuedModelInvocationFenced(context.Background(), fixture.fence, fixture.version, fixture.input); admitted || !errors.Is(err, ErrSessionWriteFenceLost) {
			t.Fatalf("admitted=%t err=%v", admitted, err)
		}
		var rows int
		if err := fixture.store.db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM native_queued_model_invocations").Scan(&rows); err != nil || rows != 0 {
			t.Fatalf("rows=%d err=%v", rows, err)
		}
	})
}

func TestSQLSessionStoreNativeQueuedModelInvocationStepGrammar(t *testing.T) {
	t.Run("summary_artifacts_before_model_are_allowed", func(t *testing.T) {
		fixture := newNativeQueuedModelFixture(t, newTestSQLStore(t), "session-model-summary", "run-model-summary")
		staged, err := fixture.session.Clone()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := staged.Append(fixture.fence.RunID, core.EvRunUsage, core.RunUsageData{InvocationID: "summary:0:0:3"}); err != nil {
			t.Fatal(err)
		}
		if _, err := staged.Append(fixture.fence.RunID, core.EvContextSummary, core.ContextSummaryData{Op: "replace", Start: 0, End: 0, Summary: "summary"}); err != nil {
			t.Fatal(err)
		}
		if err := fixture.store.AppendEventsFenced(context.Background(), fixture.fence, fixture.version, staged.EventsFrom(fixture.version)); err != nil {
			t.Fatal(err)
		}
		fixture.session = staged
		fixture.version = staged.Version()
		if admitted, err := fixture.store.BeginNativeQueuedModelInvocationFenced(context.Background(), fixture.fence, fixture.version, fixture.input); err != nil || !admitted {
			t.Fatalf("admitted=%t err=%v", admitted, err)
		}
	})
	t.Run("invalid_summary_usage_is_rejected", func(t *testing.T) {
		fixture := newNativeQueuedModelFixture(t, newTestSQLStore(t), "session-model-bad-summary", "run-model-bad-summary")
		staged, err := fixture.session.Clone()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := staged.Append(fixture.fence.RunID, core.EvRunUsage, core.RunUsageData{InvocationID: "summary:"}); err == nil {
			t.Fatal("invalid summary usage was appendable")
		}
	})
	for _, kind := range []core.SessionEventType{core.EvAssistantMessage, core.EvRunResume} {
		t.Run(string(kind)+"_is_rejected", func(t *testing.T) {
			fixture := newNativeQueuedModelFixture(t, newTestSQLStore(t), "session-model-grammar-"+strings.ReplaceAll(string(kind), "/", "-"), "run-model-grammar-"+strings.ReplaceAll(string(kind), "/", "-"))
			staged, err := fixture.session.Clone()
			if err != nil {
				t.Fatal(err)
			}
			var data any = core.AssistantMessageData{Text: "already returned"}
			if kind == core.EvRunResume {
				data = core.RunResumeData{}
			}
			if _, err := staged.Append(fixture.fence.RunID, kind, data); err != nil {
				t.Fatal(err)
			}
			if err := fixture.store.AppendEventsFenced(context.Background(), fixture.fence, fixture.version, staged.EventsFrom(fixture.version)); err != nil {
				t.Fatal(err)
			}
			fixture.session, fixture.version = staged, staged.Version()
			if admitted, err := fixture.store.BeginNativeQueuedModelInvocationFenced(context.Background(), fixture.fence, fixture.version, fixture.input); admitted || !errors.Is(err, ErrCompletedToolResultProofInvalid) {
				t.Fatalf("admitted=%t err=%v", admitted, err)
			}
		})
	}
}

func TestSQLSchemaV44MigratesNativeQueuedModelInvocations(t *testing.T) {
	store := newTestSQLStore(t)
	ctx := context.Background()
	if _, err := store.db.ExecContext(ctx, "DROP TABLE native_queued_model_invocations"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(ctx, sqlUpdateMetaRow.bind(store.dialect), "44"); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSQLSessionStore(ctx, store.db, store.dialect); err != nil {
		t.Fatal(err)
	}
	var rows int
	if err := store.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM native_queued_model_invocations").Scan(&rows); err != nil || rows != 0 {
		t.Fatalf("rows=%d err=%v", rows, err)
	}
}

func TestPostgresSQLSessionStoreBeginNativeQueuedModelInvocationFenced(t *testing.T) {
	ctx := newPostgresFenceTestContext(t)
	db := newPostgresTestDB(t)
	store, err := OpenSQLSessionStore(ctx, db, SQLDialectPostgres)
	if err != nil {
		t.Fatal(err)
	}
	fixture := newNativeQueuedModelFixture(t, store, "session-pg-model-attempt", "run-pg-model-attempt")
	if admitted, err := store.BeginNativeQueuedModelInvocationFenced(ctx, fixture.fence, fixture.version, fixture.input); err != nil || !admitted {
		t.Fatalf("admitted=%t err=%v", admitted, err)
	}
	if admitted, err := store.BeginNativeQueuedModelInvocationFenced(ctx, advanceNativeQueuedModelGeneration(t, fixture), fixture.version, fixture.input); err != nil || admitted {
		t.Fatalf("retry admitted=%t err=%v", admitted, err)
	}
	for _, test := range []struct {
		name, body string
		want       error
	}{
		{name: "epoch_change", body: `UPDATE store_meta SET value = CAST(CAST(value AS BIGINT) + 1 AS TEXT) WHERE key = 'authorization_epoch';`, want: ErrAuthorizationEpochChanged},
		{name: "request_mutation", body: `UPDATE native_queued_model_invocations SET request_sha256 = '` + strings.Repeat("0", 64) + `' WHERE session_id = NEW.session_id AND run_id = NEW.run_id AND invocation_id = NEW.invocation_id;`, want: ErrCompletedToolResultProofInvalid},
		{name: "fence_loss", body: `UPDATE run_queue SET worker_id = 'worker-replaced' WHERE run_id = NEW.run_id;`, want: ErrSessionWriteFenceLost},
	} {
		t.Run(test.name+"_rolls_back", func(t *testing.T) {
			candidate := newNativeQueuedModelFixture(t, store, "session-pg-model-"+test.name, "run-pg-model-"+test.name)
			function := "native_model_" + test.name
			trigger := function + "_trigger"
			if _, err := db.ExecContext(ctx, fmt.Sprintf(`CREATE OR REPLACE FUNCTION %s() RETURNS trigger AS $$
				BEGIN %s RETURN NEW; END;
			$$ LANGUAGE plpgsql`, function, test.body)); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, fmt.Sprintf("CREATE TRIGGER %s AFTER INSERT ON native_queued_model_invocations FOR EACH ROW EXECUTE FUNCTION %s()", trigger, function)); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				_, _ = db.ExecContext(context.Background(), fmt.Sprintf("DROP TRIGGER IF EXISTS %s ON native_queued_model_invocations", trigger))
				_, _ = db.ExecContext(context.Background(), fmt.Sprintf("DROP FUNCTION IF EXISTS %s()", function))
			})
			if admitted, err := store.BeginNativeQueuedModelInvocationFenced(ctx, candidate.fence, candidate.version, candidate.input); admitted || !errors.Is(err, test.want) {
				t.Fatalf("admitted=%t err=%v want=%v", admitted, err, test.want)
			}
			var rows int
			if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM native_queued_model_invocations WHERE session_id = $1", candidate.fence.SessionID).Scan(&rows); err != nil || rows != 0 {
				t.Fatalf("rows=%d err=%v", rows, err)
			}
		})
	}
}

func TestPostgresSchemaV44MigratesNativeQueuedModelInvocations(t *testing.T) {
	ctx := newPostgresFenceTestContext(t)
	db := newPostgresTestDB(t)
	if _, err := OpenSQLSessionStore(ctx, db, SQLDialectPostgres); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "DROP TABLE native_queued_model_invocations"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, sqlUpdateMetaRow.bind(SQLDialectPostgres), "44"); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSQLSessionStore(ctx, db, SQLDialectPostgres); err != nil {
		t.Fatal(err)
	}
}

func ExampleNativeQueuedModelInvocationInput() {
	input := NativeQueuedModelInvocationInput{BootstrapRevision: "bootstrap-v1"}
	fmt.Println(input.BootstrapRevision)
	// Output: bootstrap-v1
}
