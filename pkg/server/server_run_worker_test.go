package server

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cc-auto-agent/harness-core/pkg/control"
	core "github.com/cc-auto-agent/harness-core/pkg/core"
	"github.com/cc-auto-agent/harness-core/pkg/evaluation"
	"github.com/cc-auto-agent/harness-core/pkg/storage"

	_ "modernc.org/sqlite"
)

func TestNextWorkerPollBacksOffOnlyToBoundedIdleCeiling(t *testing.T) {
	base := 250 * time.Millisecond
	max := time.Second
	want := []time.Duration{500 * time.Millisecond, time.Second, time.Second}
	current := base
	for _, expected := range want {
		current = nextWorkerPoll(current, base, max)
		if current != expected {
			t.Fatalf("next worker poll = %s, want %s", current, expected)
		}
	}
	if got := nextWorkerPoll(time.Second, base, max); got != time.Second {
		t.Fatalf("worker poll exceeded idle ceiling: %s", got)
	}
}

func TestRunWorkerLoopFirstIdleWaitUsesConfiguredPoll(t *testing.T) {
	server := &Server{runWorkerPoll: 40 * time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan time.Time, 2)
	done := make(chan struct{})
	go func() {
		server.runWorkerLoopWith(ctx, ctx, "worker-test", func(context.Context, context.Context, string) (bool, error) {
			started <- time.Now()
			if len(started) == cap(started) {
				cancel()
			}
			return false, nil
		})
		close(done)
	}()
	var first, second time.Time
	select {
	case first = <-started:
	case <-time.After(time.Second):
		t.Fatal("worker loop did not perform its first claim")
	}
	select {
	case second = <-started:
	case <-time.After(time.Second):
		t.Fatal("worker loop did not perform its second claim")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("worker loop did not stop after cancellation")
	}
	elapsed := second.Sub(first)
	if elapsed < 30*time.Millisecond || elapsed >= 80*time.Millisecond {
		t.Fatalf("first idle wait = %s, want approximately configured 40ms (not doubled)", elapsed)
	}
}

type countingRunWorkerModel struct{ calls *atomic.Int32 }

func (m countingRunWorkerModel) Provider() string { return "worker-test" }
func (m countingRunWorkerModel) Stream(ctx context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	m.calls.Add(1)
	return (core.MockLlmAdapter{}).Stream(ctx, options, emit)
}

type selectionEchoWorkerModel struct{ text string }

func (selectionEchoWorkerModel) Provider() string { return "worker-test" }
func (m selectionEchoWorkerModel) Stream(_ context.Context, _ core.GenerateOptions, emit func(core.StreamChunk)) error {
	emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: m.text})
	emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop})
	return nil
}

type blockingRunWorkerModel struct {
	started chan<- struct{}
	release <-chan struct{}
}

type approvalWorkerTool struct{ calls *atomic.Int32 }

func (approvalWorkerTool) Manifest() core.CapabilityManifest {
	return core.CapabilityManifest{
		ID: "payment.release", Version: "1.0.0", Name: "Release", Kind: core.KindTool,
		Contract: "harness.tool/v1", RequiredPermissions: []core.Permission{core.PermWrite},
		RequiresApproval: true,
		Tool: &core.ToolExposure{Parameters: map[string]any{
			"type": "object", "properties": map[string]any{"amount": map[string]any{"type": "number"}},
		}},
	}
}

func (p approvalWorkerTool) Execute(context.Context, core.CapabilityRequest) (core.CapabilityResult, error) {
	p.calls.Add(1)
	return core.CapabilityResult{Content: `{"released":true}`, OK: true}, nil
}

type approvalWorkerModel struct{}

func (approvalWorkerModel) Provider() string { return "approval-worker" }
func (approvalWorkerModel) Stream(_ context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	last := options.Messages[len(options.Messages)-1]
	if last.Role == core.RoleTool {
		emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: "released"})
		emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop})
		return nil
	}
	call := core.ToolCall{ID: "call-release", Name: "payment.release", Args: map[string]any{"amount": 10}}
	emit(core.StreamChunk{Kind: core.StreamKindAssistant, ToolCall: &call})
	emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishToolCalls})
	return nil
}

func (blockingRunWorkerModel) Provider() string { return "worker-blocking" }
func (m blockingRunWorkerModel) Stream(ctx context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	select {
	case m.started <- struct{}{}:
	default:
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-m.release:
		return (core.MockLlmAdapter{}).Stream(ctx, options, emit)
	}
}

type runWorkerFixture struct {
	server    *Server
	sessions  *storage.SQLSessionStore
	queue     *storage.SQLRunControlStore
	approvals *storage.SQLApprovalStore
	db        *sql.DB
	session   *core.Session
	principal core.Principal
	modelRuns atomic.Int32
}

type leaseOrderedSessionStore struct {
	*storage.SQLSessionStore
	leaseHeld      *atomic.Bool
	loads          atomic.Int32
	loadedUnleased atomic.Bool
}

func (s *leaseOrderedSessionStore) Load(ctx context.Context, id string) (*core.Session, error) {
	s.loads.Add(1)
	if !s.leaseHeld.Load() {
		s.loadedUnleased.Store(true)
	}
	return s.SQLSessionStore.Load(ctx, id)
}

type leaseOrderObserver struct {
	storage.SessionLeaser
	held *atomic.Bool
}

type recordingSQLSessionLeaser struct {
	*storage.SQLSessionStore
	mu      sync.Mutex
	holders []string
}

func (l *recordingSQLSessionLeaser) AcquireSessionLease(ctx context.Context, sessionID, holder string, ttl time.Duration) (bool, error) {
	acquired, err := l.SQLSessionStore.AcquireSessionLease(ctx, sessionID, holder, ttl)
	if acquired && err == nil {
		l.mu.Lock()
		l.holders = append(l.holders, holder)
		l.mu.Unlock()
	}
	return acquired, err
}

func (l *recordingSQLSessionLeaser) acquiredHolders() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.holders...)
}

// expireBeforeFirstFinishQueue models the final hand-off race where the
// terminal Session suffix is already durable, but the queue claim expires
// immediately before FinishRunClaim can commit. It delegates every operation
// except the first finish attempt to the real SQL queue.
type expireBeforeFirstFinishQueue struct {
	*storage.SQLRunControlStore
	once         sync.Once
	beforeFinish func(string)
}

func (q *expireBeforeFirstFinishQueue) FinishRunClaim(ctx context.Context, runID, workerID string, generation int64, status core.RunStatus, errorCode string) (bool, error) {
	q.once.Do(func() { q.beforeFinish(runID) })
	return q.SQLRunControlStore.FinishRunClaim(ctx, runID, workerID, generation, status, errorCode)
}

// claimLossDuringPreparationQueue makes the claim monitor observe ownership
// loss while principal preparation is still in progress. Its terminal methods
// remain observable so the test can prove that the stale worker does not
// finish, retry, or pause the claim after that signal.
type claimLossDuringPreparationQueue struct {
	*storage.SQLRunControlStore
	renewed     chan struct{}
	renewOnce   sync.Once
	finishCalls atomic.Int32
	retryCalls  atomic.Int32
	pauseCalls  atomic.Int32
}

func (q *claimLossDuringPreparationQueue) RenewRunClaim(context.Context, string, string, int64, time.Duration) (bool, error) {
	q.renewOnce.Do(func() { close(q.renewed) })
	return false, nil
}

func (q *claimLossDuringPreparationQueue) FinishRunClaim(ctx context.Context, runID, workerID string, generation int64, status core.RunStatus, errorCode string) (bool, error) {
	q.finishCalls.Add(1)
	return q.SQLRunControlStore.FinishRunClaim(ctx, runID, workerID, generation, status, errorCode)
}

func (q *claimLossDuringPreparationQueue) RetryRunClaim(ctx context.Context, runID, workerID string, generation int64, availableAt time.Time, errorCode string) (bool, error) {
	q.retryCalls.Add(1)
	return q.SQLRunControlStore.RetryRunClaim(ctx, runID, workerID, generation, availableAt, errorCode)
}

func (q *claimLossDuringPreparationQueue) PauseRunClaim(ctx context.Context, runID, workerID string, generation int64) (bool, error) {
	q.pauseCalls.Add(1)
	return q.SQLRunControlStore.PauseRunClaim(ctx, runID, workerID, generation)
}

type mutatingAcquireLeaser struct {
	storage.SessionLeaser
	once   sync.Once
	mutate func()
}

func (l *mutatingAcquireLeaser) AcquireSessionLease(ctx context.Context, sessionID, holder string, ttl time.Duration) (bool, error) {
	l.once.Do(l.mutate)
	return l.SessionLeaser.AcquireSessionLease(ctx, sessionID, holder, ttl)
}

func (l leaseOrderObserver) AcquireSessionLease(ctx context.Context, sessionID, holder string, ttl time.Duration) (bool, error) {
	acquired, err := l.SessionLeaser.AcquireSessionLease(ctx, sessionID, holder, ttl)
	if err == nil && acquired {
		l.held.Store(true)
	}
	return acquired, err
}

func (l leaseOrderObserver) ReleaseSessionLease(ctx context.Context, sessionID, holder string) error {
	err := l.SessionLeaser.ReleaseSessionLease(ctx, sessionID, holder)
	if err == nil {
		l.held.Store(false)
	}
	return err
}

func newRunWorkerFixture(t *testing.T) *runWorkerFixture {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "worker.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	sessions, err := storage.OpenSQLSessionStore(context.Background(), db, storage.SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	queue, err := storage.NewSQLRunControlStore(db, storage.SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	approvals, err := storage.NewSQLApprovalStore(db, storage.SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	product, _ := global.Child(core.ScopeRef{Kind: core.ScopeProduct, ID: "product"})
	tenant, _ := product.Child(core.ScopeRef{Kind: core.ScopeTenant, ID: "acme"})
	user, _ := tenant.Child(core.ScopeRef{Kind: core.ScopeUser, ID: "alice"})
	principal := core.Principal{
		SubjectID: "alice", TenantID: "acme", Scope: user,
		Grants: core.NewPermissionSet(core.PermRead, core.PermWrite),
	}
	profiles := core.NewAgentProfileRegistry()
	name := "Worker Agent"
	model := core.ModelSelection{Provider: "worker-test", Model: "worker-test"}
	if err := profiles.Bind(core.AgentProfileLayer{Scope: product, ProfileID: "product.agent", Name: &name, Model: &model}); err != nil {
		t.Fatal(err)
	}
	fixture := &runWorkerFixture{sessions: sessions, queue: queue, approvals: approvals, db: db, principal: principal}
	runtime := &core.Runtime{
		Capabilities: core.NewCapabilityRegistry(), Profiles: profiles,
		Approver: approvals,
		Models: core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) {
			return countingRunWorkerModel{calls: &fixture.modelRuns}, nil
		}),
	}
	sessionScope, _ := user.Child(core.ScopeRef{Kind: core.ScopeSession, ID: "session-worker"})
	session, err := core.NewSession(core.SessionOptions{
		ID: "session-worker", ProfileID: "product.agent", Principal: principal, Scope: sessionScope,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := sessions.Create(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	fixture.session = session
	fixture.server, err = New(Config{
		Runtime: runtime, Sessions: sessions, Authenticator: AuthenticatorFunc(func(*http.Request) (core.Principal, error) {
			return principal, nil
		}),
		Leaser: sessions, LeaseTTL: 300 * time.Millisecond,
		RunControl: queue, RunQueue: queue, Approvals: approvals,
		RunPrincipalResolver: RunPrincipalResolverFunc(func(context.Context, string, string) (core.Principal, error) {
			return principal, nil
		}),
		RunCancelPollInterval: 5 * time.Millisecond, RunStaleAfter: time.Second,
		RunWorkerPollInterval: 5 * time.Millisecond, RunWorkerClaimTTL: 300 * time.Millisecond,
		RunWorkerConcurrency: 1, RunWorkerMaxAttempts: 3, MaxWriteDelay: -1,
	})
	if err != nil {
		t.Fatal(err)
	}
	return fixture
}

func enqueueRunHTTP(t *testing.T, fixture *runWorkerFixture, message string) storage.RunRecord {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, "/v1/sessions/"+fixture.session.ID()+"/runs/async", bytes.NewBufferString(`{"message":"`+message+`"}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	fixture.server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusAccepted {
		t.Fatalf("enqueue status=%d body=%s", response.Code, response.Body.String())
	}
	var record storage.RunRecord
	if err := json.Unmarshal(response.Body.Bytes(), &record); err != nil {
		t.Fatal(err)
	}
	if record.Status != storage.RunStatusQueued {
		t.Fatalf("enqueue did not return queued status: %#v", record)
	}
	return record
}

func TestQueuedWorkerLoadsSessionOnlyAfterAcquiringExecutionLease(t *testing.T) {
	fixture := newRunWorkerFixture(t)
	record := enqueueRunHTTP(t, fixture, "lease ordered load")
	held := &atomic.Bool{}
	orderedStore := &leaseOrderedSessionStore{SQLSessionStore: fixture.sessions, leaseHeld: held}
	fixture.server.sessions = orderedStore
	fixture.server.leaser = leaseOrderObserver{SessionLeaser: fixture.sessions, held: held}
	claimed, err := fixture.server.RunWorkerOnce(context.Background(), "worker-lease-order")
	if err != nil || !claimed {
		t.Fatalf("worker failed: claimed=%t err=%v", claimed, err)
	}
	if orderedStore.loads.Load() == 0 || orderedStore.loadedUnleased.Load() {
		t.Fatalf("worker load ordering: loads=%d loaded_without_lease=%t", orderedStore.loads.Load(), orderedStore.loadedUnleased.Load())
	}
	terminal, err := fixture.queue.GetRun(context.Background(), record.RunID)
	if err != nil || terminal.Status != string(core.RunCompleted) {
		t.Fatalf("queued run did not complete: %#v err=%v", terminal, err)
	}
}

func TestQueuedWorkerRepairsPredecessorThroughCurrentFence(t *testing.T) {
	fixture := newRunWorkerFixture(t)
	predecessor := "run-fenced-predecessor"
	if _, err := fixture.session.Append(predecessor, core.EvRunStart, core.RunStartData{}); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.session.Append(predecessor, core.EvToolCall, core.ToolCallData{CallID: "call-fenced-predecessor", Name: "test.write"}); err != nil {
		t.Fatal(err)
	}
	if err := fixture.sessions.Save(context.Background(), fixture.session, 0); err != nil {
		t.Fatal(err)
	}
	record := enqueueRunHTTP(t, fixture, "repair predecessor")
	if claimed, err := fixture.server.RunWorkerOnce(context.Background(), "worker-fenced-repair"); err != nil || !claimed {
		t.Fatalf("worker claimed=%t err=%v", claimed, err)
	}
	loaded, err := fixture.sessions.Load(context.Background(), fixture.session.ID())
	if err != nil {
		t.Fatal(err)
	}
	if status, exists := loaded.RunStatus(predecessor); !exists || status != core.RunFailed {
		t.Fatalf("predecessor was not fenced-repaired: exists=%t status=%q", exists, status)
	}
	if status, exists := loaded.RunStatus(record.RunID); !exists || status != core.RunCompleted {
		t.Fatalf("current run did not complete after fenced repair: exists=%t status=%q", exists, status)
	}
}

func TestQueuedClaimLossDuringPreparationDoesNotSettleOldGeneration(t *testing.T) {
	fixture := newRunWorkerFixture(t)
	queue := &claimLossDuringPreparationQueue{
		SQLRunControlStore: fixture.queue,
		renewed:            make(chan struct{}),
	}
	fixture.server.runQueue = queue
	fixture.server.runControl = queue
	fixture.server.runWorkerClaimTTL = 15 * time.Millisecond
	fixture.server.runPrincipal = RunPrincipalResolverFunc(func(ctx context.Context, tenantID, subjectID string) (core.Principal, error) {
		select {
		case <-queue.renewed:
			return core.Principal{}, PermanentRunFailure(errors.New("forced preparation failure after claim loss"))
		case <-ctx.Done():
			return core.Principal{}, ctx.Err()
		}
	})
	record := enqueueRunHTTP(t, fixture, "claim loss during preparation")
	claimed, err := fixture.server.RunWorkerOnce(context.Background(), "worker-preparation-claim-loss")
	if !claimed || !errors.Is(err, errRunClaimLost) {
		t.Fatalf("claim-loss preparation result: claimed=%t err=%v", claimed, err)
	}
	if queue.finishCalls.Load() != 0 || queue.retryCalls.Load() != 0 || queue.pauseCalls.Load() != 0 {
		t.Fatalf("stale preparation settled queue: finish=%d retry=%d pause=%d", queue.finishCalls.Load(), queue.retryCalls.Load(), queue.pauseCalls.Load())
	}
	current, err := fixture.queue.GetRun(context.Background(), record.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Status != storage.RunStatusRunning {
		t.Fatalf("claim-loss preparation mutated queue record: %#v", current)
	}
	loaded, err := fixture.sessions.Load(context.Background(), fixture.session.ID())
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := loaded.RunStatus(record.RunID); exists {
		t.Fatal("claim-loss preparation appended a Session terminal suffix")
	}
}

func TestQueuedFenceLossFromBackgroundFlushStopsOldWorkerWithoutTerminalPollution(t *testing.T) {
	for _, test := range []struct {
		name       string
		invalidate func(t *testing.T, fixture *runWorkerFixture, runID string)
	}{
		{
			name: "queue_lease_expired",
			invalidate: func(t *testing.T, fixture *runWorkerFixture, runID string) {
				t.Helper()
				if _, err := fixture.db.ExecContext(context.Background(), "UPDATE run_queue SET lease_expires_at = 1 WHERE run_id = ?", runID); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "cancel_requested",
			invalidate: func(t *testing.T, fixture *runWorkerFixture, runID string) {
				t.Helper()
				if _, err := fixture.db.ExecContext(context.Background(), "UPDATE run_control SET cancel_requested = 1 WHERE run_id = ?", runID); err != nil {
					t.Fatal(err)
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newRunWorkerFixture(t)
			fixture.server.maxWriteDelay = 100 * time.Millisecond
			fixture.server.runWorkerClaimTTL = 5 * time.Second
			started := make(chan struct{}, 1)
			release := make(chan struct{})
			fixture.server.runtime.Models = core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) {
				return blockingRunWorkerModel{started: started, release: release}, nil
			})
			record := enqueueRunHTTP(t, fixture, "background fence loss")
			result := make(chan error, 1)
			go func() {
				_, err := fixture.server.RunWorkerOnce(context.Background(), "worker-background-fence")
				result <- err
			}()
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("worker did not begin model execution")
			}
			test.invalidate(t, fixture, record.RunID)
			select {
			case err := <-result:
				if !errors.Is(err, errRunClaimLost) || !errors.Is(err, storage.ErrSessionWriteFenceLost) {
					t.Fatalf("background fence loss error=%v", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("background fence loss did not stop the worker")
			}
			close(release)
			control, err := fixture.queue.GetRun(context.Background(), record.RunID)
			if err != nil || control.Status != storage.RunStatusRunning || control.ErrorCode != "" || !control.CompletedAt.IsZero() {
				t.Fatalf("stale worker wrote terminal control state: %#v err=%v", control, err)
			}
			loaded, err := fixture.sessions.Load(context.Background(), fixture.session.ID())
			if err != nil {
				t.Fatal(err)
			}
			for _, event := range loaded.Events() {
				if event.RunID == record.RunID && (event.Type == core.EvRunError || event.Type == core.EvRunEnd) {
					t.Fatalf("stale worker persisted terminal event after fence loss: %#v", event)
				}
			}
		})
	}
}

func TestQueuedSuccessorCompletesAfterOldGenerationFenceLoss(t *testing.T) {
	fixture := newRunWorkerFixture(t)
	fixture.server.maxWriteDelay = 100 * time.Millisecond
	fixture.server.runWorkerClaimTTL = 5 * time.Second
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	fixture.server.runtime.Models = core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) {
		return blockingRunWorkerModel{started: started, release: release}, nil
	})
	record := enqueueRunHTTP(t, fixture, "successor after fence loss")
	oldResult := make(chan error, 1)
	go func() {
		_, err := fixture.server.RunWorkerOnce(context.Background(), "worker-old-generation")
		oldResult <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("old worker did not begin model execution")
	}
	var oldGeneration int64
	if err := fixture.db.QueryRowContext(context.Background(), "SELECT generation FROM run_queue WHERE run_id = ?", record.RunID).Scan(&oldGeneration); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.db.ExecContext(context.Background(), "UPDATE run_queue SET lease_expires_at = 1 WHERE run_id = ?", record.RunID); err != nil {
		t.Fatal(err)
	}
	if requeued, failed, err := fixture.queue.RecoverExpiredRunClaims(context.Background(), time.Now().UTC()); err != nil || requeued != 1 || failed != 0 {
		t.Fatalf("requeue old claim: requeued=%d failed=%d err=%v", requeued, failed, err)
	}
	select {
	case err := <-oldResult:
		if !errors.Is(err, errRunClaimLost) || !errors.Is(err, storage.ErrSessionWriteFenceLost) {
			t.Fatalf("old worker error=%v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("old worker did not stop after generation replacement")
	}
	close(release)
	fixture.server.runtime.Models = core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) {
		return countingRunWorkerModel{calls: &fixture.modelRuns}, nil
	})
	if claimed, err := fixture.server.RunWorkerOnce(context.Background(), "worker-successor-generation"); err != nil || !claimed {
		t.Fatalf("successor claimed=%t err=%v", claimed, err)
	}
	terminal, err := fixture.queue.GetRun(context.Background(), record.RunID)
	if err != nil || terminal.Status != string(core.RunCompleted) {
		t.Fatalf("successor did not complete run: %#v err=%v", terminal, err)
	}
	loaded, err := fixture.sessions.Load(context.Background(), fixture.session.ID())
	if err != nil {
		t.Fatal(err)
	}
	if status, exists := loaded.RunStatus(record.RunID); !exists || status != core.RunCompleted {
		t.Fatalf("successor Session result: exists=%t status=%q", exists, status)
	}
	if oldGeneration < 1 {
		t.Fatalf("invalid original generation %d", oldGeneration)
	}
}

func TestQueuedTerminalDurabilitySurvivesClaimLossBeforeFinish(t *testing.T) {
	fixture := newRunWorkerFixture(t)
	queue := &expireBeforeFirstFinishQueue{SQLRunControlStore: fixture.queue}
	queue.beforeFinish = func(runID string) {
		t.Helper()
		loaded, err := fixture.sessions.Load(context.Background(), fixture.session.ID())
		if err != nil {
			t.Fatal(err)
		}
		if status, exists := loaded.RunStatus(runID); !exists || status != core.RunCompleted {
			t.Fatalf("terminal Session suffix was not durable before FinishRunClaim: exists=%t status=%q", exists, status)
		}
		if _, err := fixture.db.ExecContext(context.Background(), "UPDATE run_queue SET lease_expires_at = 1 WHERE run_id = ?", runID); err != nil {
			t.Fatal(err)
		}
	}
	fixture.server.runQueue = queue

	record := enqueueRunHTTP(t, fixture, "terminal then claim loss")
	claimed, err := fixture.server.RunWorkerOnce(context.Background(), "worker-terminal-old")
	if !claimed || !errors.Is(err, errRunClaimLost) {
		t.Fatalf("old worker result: claimed=%t err=%v", claimed, err)
	}
	if errors.Is(err, storage.ErrSessionWriteFenceLost) {
		t.Fatalf("post-flush Finish loss must not be misreported as a Session write fence loss: %v", err)
	}

	loaded, err := fixture.sessions.Load(context.Background(), fixture.session.ID())
	if err != nil {
		t.Fatal(err)
	}
	if status, exists := loaded.RunStatus(record.RunID); !exists || status != core.RunCompleted {
		t.Fatalf("old worker did not leave its terminal Session suffix durable: exists=%t status=%q", exists, status)
	}
	control, err := fixture.queue.GetRun(context.Background(), record.RunID)
	if err != nil || control.Status != storage.RunStatusRunning || !control.CompletedAt.IsZero() {
		t.Fatalf("expired Finish attempt mutated queue terminal state: %#v err=%v", control, err)
	}
	if requeued, failed, err := fixture.queue.RecoverExpiredRunClaims(context.Background(), time.Now().UTC()); err != nil || requeued != 1 || failed != 0 {
		t.Fatalf("recover expired terminal claim: requeued=%d failed=%d err=%v", requeued, failed, err)
	}
	if claimed, err := fixture.server.RunWorkerOnce(context.Background(), "worker-terminal-successor"); err != nil || !claimed {
		t.Fatalf("successor claim=%t err=%v", claimed, err)
	}
	terminal, err := fixture.queue.GetRun(context.Background(), record.RunID)
	if err != nil || terminal.Status != string(core.RunCompleted) {
		t.Fatalf("successor did not converge durable terminal run: %#v err=%v", terminal, err)
	}
}

func TestQueuedWorkerLostClaimBeforeLeaseDoesNotLoadOrRepairSession(t *testing.T) {
	fixture := newRunWorkerFixture(t)
	interruptedRunID := "run-claim-lost-predecessor"
	if _, err := fixture.session.Append(interruptedRunID, core.EvRunStart, core.RunStartData{}); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.session.Append(interruptedRunID, core.EvToolCall, core.ToolCallData{CallID: "call-claim-lost", Name: "test.tool"}); err != nil {
		t.Fatal(err)
	}
	if err := fixture.sessions.Save(context.Background(), fixture.session, 0); err != nil {
		t.Fatal(err)
	}
	before := fixture.session.Version()
	enqueueRunHTTP(t, fixture, "claim loss must not repair")
	if err := fixture.server.liveness.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	fixture.server.liveness = nil
	loadObserver := &leaseOrderedSessionStore{SQLSessionStore: fixture.sessions, leaseHeld: &atomic.Bool{}}
	fixture.server.sessions = loadObserver
	claimed, err := fixture.server.RunWorkerOnce(context.Background(), "worker-claim-loss-load")
	if !claimed || !errors.Is(err, errRunClaimLost) {
		t.Fatalf("claim loss: claimed=%t err=%v", claimed, err)
	}
	if loads := loadObserver.loads.Load(); loads != 0 {
		t.Fatalf("claim-lost worker loaded session %d times", loads)
	}

	persisted, err := fixture.sessions.Load(context.Background(), fixture.session.ID())
	if err != nil {
		t.Fatal(err)
	}
	if persisted.Version() != before {
		t.Fatalf("claim-lost worker changed session version: got=%d want=%d", persisted.Version(), before)
	}
	if status, exists := persisted.RunStatus(interruptedRunID); !exists || status != "" {
		t.Fatalf("claim-lost worker repaired predecessor: exists=%t status=%q", exists, status)
	}
}

func TestHistoryReadDoesNotRepairActiveCheckpoint(t *testing.T) {
	fixture := newRunWorkerFixture(t)
	runID := "run-active-history"
	if _, err := fixture.session.Append(runID, core.EvRunStart, core.RunStartData{}); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.session.Append(runID, core.EvToolCall, core.ToolCallData{CallID: "call-active-history", Name: "test.tool"}); err != nil {
		t.Fatal(err)
	}
	if err := fixture.sessions.Save(context.Background(), fixture.session, 0); err != nil {
		t.Fatal(err)
	}
	before := fixture.session.Version()
	request := httptest.NewRequest(http.MethodGet, "/v1/sessions/"+fixture.session.ID()+"/events", nil)
	response := httptest.NewRecorder()
	fixture.server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("history status=%d body=%s", response.Code, response.Body.String())
	}
	var history struct {
		Version int64               `json:"version"`
		Events  []core.SessionEvent `json:"events"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &history); err != nil {
		t.Fatal(err)
	}
	if history.Version != before || int64(len(history.Events)) != before {
		t.Fatalf("history read changed checkpoint: version=%d events=%d want=%d", history.Version, len(history.Events), before)
	}
	persisted, err := fixture.sessions.Load(context.Background(), fixture.session.ID())
	if err != nil {
		t.Fatal(err)
	}
	if persisted.Version() != before {
		t.Fatalf("history read persisted repair: version=%d want=%d", persisted.Version(), before)
	}
	if status, exists := persisted.RunStatus(runID); !exists || status != "" {
		t.Fatalf("history read terminated active run: exists=%t status=%q", exists, status)
	}
}

func TestSyncRunExplicitlyRepairsInterruptedPredecessor(t *testing.T) {
	fixture := newRunWorkerFixture(t)
	interruptedRunID := "run-sync-predecessor"
	if _, err := fixture.session.Append(interruptedRunID, core.EvRunStart, core.RunStartData{}); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.session.Append(interruptedRunID, core.EvToolCall, core.ToolCallData{CallID: "call-sync-predecessor", Name: "test.tool"}); err != nil {
		t.Fatal(err)
	}
	if err := fixture.sessions.Save(context.Background(), fixture.session, 0); err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/v1/sessions/"+fixture.session.ID()+"/runs", bytes.NewBufferString(`{"message":"continue after repair"}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	fixture.server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("sync run status=%d body=%s", response.Code, response.Body.String())
	}
	persisted, err := fixture.sessions.Load(context.Background(), fixture.session.ID())
	if err != nil {
		t.Fatal(err)
	}
	if status, exists := persisted.RunStatus(interruptedRunID); !exists || status != core.RunFailed {
		t.Fatalf("predecessor was not explicitly repaired: exists=%t status=%q", exists, status)
	}
	if code := runTerminalErrorCode(persisted, interruptedRunID); code != core.CodeRunInterrupted {
		t.Fatalf("predecessor repair code=%q", code)
	}
}

func TestSyncRunReloadsAfterLeaseBeforeRepair(t *testing.T) {
	fixture := newRunWorkerFixture(t)
	interruptedRunID := "run-sync-lease-predecessor"
	fixture.server.leaser = &mutatingAcquireLeaser{
		SessionLeaser: fixture.sessions,
		mutate: func() {
			latest, err := fixture.sessions.Load(context.Background(), fixture.session.ID())
			if err != nil {
				t.Fatal(err)
			}
			expectedVersion := latest.Version()
			if _, err := latest.Append(interruptedRunID, core.EvRunStart, core.RunStartData{}); err != nil {
				t.Fatal(err)
			}
			if _, err := latest.Append(interruptedRunID, core.EvToolCall, core.ToolCallData{CallID: "call-sync-lease-predecessor", Name: "test.tool"}); err != nil {
				t.Fatal(err)
			}
			if err := fixture.sessions.Save(context.Background(), latest, expectedVersion); err != nil {
				t.Fatal(err)
			}
		},
	}
	request := httptest.NewRequest(http.MethodPost, "/v1/sessions/"+fixture.session.ID()+"/runs", bytes.NewBufferString(`{"message":"continue after lease reload"}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	fixture.server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("sync run status=%d body=%s", response.Code, response.Body.String())
	}
	persisted, err := fixture.sessions.Load(context.Background(), fixture.session.ID())
	if err != nil {
		t.Fatal(err)
	}
	if status, exists := persisted.RunStatus(interruptedRunID); !exists || status != core.RunFailed {
		t.Fatalf("lease-reloaded predecessor was not repaired: exists=%t status=%q", exists, status)
	}
	if code := runTerminalErrorCode(persisted, interruptedRunID); code != core.CodeRunInterrupted {
		t.Fatalf("lease-reloaded predecessor repair code=%q", code)
	}
}

func enqueueRunRecorder(t *testing.T, fixture *runWorkerFixture, message, key string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, "/v1/sessions/"+fixture.session.ID()+"/runs/async", bytes.NewBufferString(`{"message":"`+message+`"}`))
	request.Header.Set("Content-Type", "application/json")
	if key != "" {
		request.Header.Set("Idempotency-Key", key)
	}
	response := httptest.NewRecorder()
	fixture.server.Handler().ServeHTTP(response, request)
	return response
}

func enableApprovalWorker(t *testing.T, fixture *runWorkerFixture) *atomic.Int32 {
	t.Helper()
	segments := fixture.principal.Scope.Segments()
	product := core.MustScopePath(segments[:2]...)
	var calls atomic.Int32
	if err := fixture.server.runtime.Capabilities.Register(product, approvalWorkerTool{calls: &calls}); err != nil {
		t.Fatal(err)
	}
	if err := fixture.server.runtime.Profiles.Bind(core.AgentProfileLayer{
		Scope: product, ProfileID: "product.agent", AddCapabilities: []string{"payment.release"},
	}); err != nil {
		t.Fatal(err)
	}
	fixture.server.runtime.Models = core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) {
		return approvalWorkerModel{}, nil
	})
	return &calls
}

func TestAsyncRunSubmitAndWorkerOnceCompletesDurably(t *testing.T) {
	fixture := newRunWorkerFixture(t)
	record := enqueueRunHTTP(t, fixture, "hello")
	claimed, err := fixture.server.RunWorkerOnce(context.Background(), "worker-e2e")
	if err != nil || !claimed {
		t.Fatalf("worker once failed: claimed=%t err=%v", claimed, err)
	}
	terminal, err := fixture.queue.GetRun(context.Background(), record.RunID)
	if err != nil || terminal.Status != string(core.RunCompleted) || terminal.CompletedAt.IsZero() {
		t.Fatalf("run did not complete: %#v err=%v", terminal, err)
	}
	loaded, err := fixture.sessions.Load(context.Background(), fixture.session.ID())
	if err != nil {
		t.Fatal(err)
	}
	if status, exists := loaded.RunStatus(record.RunID); !exists || status != core.RunCompleted {
		t.Fatalf("session run is incomplete: status=%q exists=%t", status, exists)
	}
	if fixture.modelRuns.Load() != 1 {
		t.Fatalf("model calls=%d, want 1", fixture.modelRuns.Load())
	}
}

func TestAsyncWorkerUsesCanaryRuntimeSelection(t *testing.T) {
	fixture := newRunWorkerFixture(t)
	ctx := context.Background()
	segments := fixture.principal.Scope.Segments()
	product := core.MustScopePath(segments[:2]...)
	releases, err := control.NewReleaseManager(fixture.server.runtime.Profiles)
	if err != nil {
		t.Fatal(err)
	}
	store, err := storage.NewSQLCanaryStore(fixture.db, storage.SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	canaries, err := control.NewCanaryManager(releases, store)
	if err != nil {
		t.Fatal(err)
	}
	if err := canaries.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	candidateModel := core.ModelSelection{Provider: "worker-test", Model: "candidate"}
	layer := core.AgentProfileLayer{Scope: product, ProfileID: "product.agent", Model: &candidateModel}
	revision, err := core.ProfileLayerRevision(layer)
	if err != nil {
		t.Fatal(err)
	}
	record, err := canaries.Stage(ctx, control.CanaryRecord{
		ID: "canary-async-worker", ProfileID: layer.ProfileID, Scope: product, Layer: &layer,
		Revision: revision, BasisPoints: 10000, CandidateEvaluationRunID: "evaluation-async-worker",
		Gate: evaluation.GateResult{Passed: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	fixture.server.canaries = canaries
	fixture.server.runtime.Models = core.ModelResolverFunc(func(_ context.Context, selection core.ModelSelection) (core.LlmAdapter, error) {
		return selectionEchoWorkerModel{text: selection.Model}, nil
	})

	first := enqueueRunHTTP(t, fixture, "candidate route")
	if claimed, err := fixture.server.RunWorkerOnce(ctx, "worker-canary"); err != nil || !claimed {
		t.Fatalf("candidate worker failed: claimed=%t err=%v", claimed, err)
	}
	if terminal, err := fixture.queue.GetRun(ctx, first.RunID); err != nil || terminal.Status != string(core.RunCompleted) {
		t.Fatalf("candidate run incomplete: %#v err=%v", terminal, err)
	}
	loaded, err := fixture.sessions.Load(ctx, fixture.session.ID())
	if err != nil {
		t.Fatal(err)
	}
	messages, err := loaded.DeriveMessages()
	if err != nil || len(messages) == 0 || messages[len(messages)-1].Content != "candidate" {
		t.Fatalf("async canary runtime not used: %#v err=%v", messages, err)
	}

	if _, err := canaries.Pause(ctx, record.ID); err != nil {
		t.Fatal(err)
	}
	second := enqueueRunHTTP(t, fixture, "stable route")
	if claimed, err := fixture.server.RunWorkerOnce(ctx, "worker-stable"); err != nil || !claimed {
		t.Fatalf("stable worker failed: claimed=%t err=%v", claimed, err)
	}
	if terminal, err := fixture.queue.GetRun(ctx, second.RunID); err != nil || terminal.Status != string(core.RunCompleted) {
		t.Fatalf("stable run incomplete: %#v err=%v", terminal, err)
	}
	loaded, err = fixture.sessions.Load(ctx, fixture.session.ID())
	if err != nil {
		t.Fatal(err)
	}
	messages, err = loaded.DeriveMessages()
	if err != nil || len(messages) == 0 || messages[len(messages)-1].Content != "worker-test" {
		t.Fatalf("paused async canary did not use live runtime: %#v err=%v", messages, err)
	}
}

func TestRuntimeForRefreshesPeerControlPlane(t *testing.T) {
	fixture := newRunWorkerFixture(t)
	ctx := context.Background()
	segments := fixture.principal.Scope.Segments()
	product := core.MustScopePath(segments[:2]...)
	canaryStore, err := storage.NewSQLCanaryStore(fixture.db, storage.SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	newPeerProfiles := func() *core.AgentProfileRegistry {
		profiles := core.NewAgentProfileRegistry()
		name := "Worker Agent"
		model := core.ModelSelection{Provider: "worker-test", Model: "worker-test"}
		if err := profiles.Bind(core.AgentProfileLayer{
			Scope: product, ProfileID: "product.agent", Name: &name, Model: &model,
		}); err != nil {
			t.Fatal(err)
		}
		return profiles
	}
	newControl := func(profiles *core.AgentProfileRegistry) (*control.ReleaseManager, *control.CanaryManager) {
		releases, err := control.NewReleaseManager(profiles)
		if err != nil {
			t.Fatal(err)
		}
		releases.Journal = fixture.sessions
		if err := releases.Restore(ctx); err != nil {
			t.Fatal(err)
		}
		canaries, err := control.NewCanaryManager(releases, canaryStore)
		if err != nil {
			t.Fatal(err)
		}
		if err := canaries.Restore(ctx); err != nil {
			t.Fatal(err)
		}
		return releases, canaries
	}
	releasesA, canariesA := newControl(newPeerProfiles())
	releasesB, canariesB := newControl(fixture.server.runtime.Profiles)
	fixture.server.Releases = releasesB
	fixture.server.canaries = canariesB

	candidateName := "Peer Candidate"
	layer := core.AgentProfileLayer{Scope: product, ProfileID: "product.agent", Name: &candidateName}
	revision, err := core.ProfileLayerRevision(layer)
	if err != nil {
		t.Fatal(err)
	}
	staged, err := canariesA.Stage(ctx, control.CanaryRecord{
		ID: "canary-peer-runtime", ProfileID: layer.ProfileID, Scope: product, Layer: &layer,
		Revision: revision, BasisPoints: 10000, CandidateEvaluationRunID: "evaluation-peer-runtime",
		Gate: evaluation.GateResult{Passed: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	resolveName := func(runtime *core.Runtime) string {
		profile, err := runtime.Profiles.Resolve(fixture.principal, fixture.principal.Scope, layer.ProfileID)
		if err != nil {
			t.Fatal(err)
		}
		return profile.Name
	}

	runtime, selected, err := fixture.server.runtimeFor(ctx, fixture.principal, layer.ProfileID)
	if err != nil || selected == nil || selected.ID != staged.ID || resolveName(runtime) != candidateName {
		t.Fatalf("server did not refresh peer canary: selected=%#v name=%q err=%v", selected, resolveName(runtime), err)
	}
	if _, err := canariesA.Pause(ctx, staged.ID); err != nil {
		t.Fatal(err)
	}
	runtime, selected, err = fixture.server.runtimeFor(ctx, fixture.principal, layer.ProfileID)
	if err != nil || selected == nil || selected.Candidate || selected.Status != control.CanaryPaused || resolveName(runtime) != "Worker Agent" {
		t.Fatalf("server did not refresh peer pause: selected=%#v name=%q err=%v", selected, resolveName(runtime), err)
	}
	if _, err := canariesA.Resume(ctx, staged.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := canariesA.Promote(ctx, staged.ID); err != nil {
		t.Fatal(err)
	}
	runtime, selected, err = fixture.server.runtimeFor(ctx, fixture.principal, layer.ProfileID)
	if err != nil || selected != nil || resolveName(runtime) != candidateName {
		t.Fatalf("server did not refresh peer promotion: selected=%#v name=%q err=%v", selected, resolveName(runtime), err)
	}
	if rolledBack, err := releasesA.Rollback(ctx, layer.ProfileID, 0); err != nil || len(rolledBack) != 1 {
		t.Fatalf("peer rollback failed: %#v err=%v", rolledBack, err)
	}
	runtime, selected, err = fixture.server.runtimeFor(ctx, fixture.principal, layer.ProfileID)
	if err != nil || selected != nil || resolveName(runtime) != "Worker Agent" {
		t.Fatalf("server did not refresh peer rollback: selected=%#v name=%q err=%v", selected, resolveName(runtime), err)
	}
}

func TestAsyncSubmitIdempotencyReplaysCanonicalRun(t *testing.T) {
	fixture := newRunWorkerFixture(t)
	firstResponse := enqueueRunRecorder(t, fixture, "idempotent work", "client-op-42")
	if firstResponse.Code != http.StatusAccepted || firstResponse.Header().Get("Idempotency-Replayed") != "false" {
		t.Fatalf("first submit status=%d replay=%q body=%s", firstResponse.Code, firstResponse.Header().Get("Idempotency-Replayed"), firstResponse.Body.String())
	}
	var first storage.RunRecord
	if err := json.Unmarshal(firstResponse.Body.Bytes(), &first); err != nil {
		t.Fatal(err)
	}
	if location := firstResponse.Header().Get("Location"); location != "/v1/sessions/"+fixture.session.ID()+"/runs/"+first.RunID {
		t.Fatalf("location=%q", location)
	}
	replayResponse := enqueueRunRecorder(t, fixture, "idempotent work", "client-op-42")
	if replayResponse.Code != http.StatusAccepted || replayResponse.Header().Get("Idempotency-Replayed") != "true" {
		t.Fatalf("replay status=%d replay=%q body=%s", replayResponse.Code, replayResponse.Header().Get("Idempotency-Replayed"), replayResponse.Body.String())
	}
	var replay storage.RunRecord
	if err := json.Unmarshal(replayResponse.Body.Bytes(), &replay); err != nil {
		t.Fatal(err)
	}
	if replay.RunID != first.RunID {
		t.Fatalf("replay generated another run: first=%s replay=%s", first.RunID, replay.RunID)
	}
	conflict := enqueueRunRecorder(t, fixture, "different work", "client-op-42")
	if conflict.Code != http.StatusConflict {
		t.Fatalf("different request reused key: status=%d body=%s", conflict.Code, conflict.Body.String())
	}
	var controlRows, queueRows int
	if err := fixture.db.QueryRow("SELECT COUNT(*) FROM run_control").Scan(&controlRows); err != nil {
		t.Fatal(err)
	}
	if err := fixture.db.QueryRow("SELECT COUNT(*) FROM run_queue").Scan(&queueRows); err != nil {
		t.Fatal(err)
	}
	if controlRows != 1 || queueRows != 1 {
		t.Fatalf("duplicate submission rows: controls=%d queue=%d", controlRows, queueRows)
	}
	if claimed, err := fixture.server.RunWorkerOnce(context.Background(), "worker-idempotent-submit"); err != nil || !claimed {
		t.Fatalf("worker failed: claimed=%t err=%v", claimed, err)
	}
	terminalResponse := enqueueRunRecorder(t, fixture, "idempotent work", "client-op-42")
	if terminalResponse.Code != http.StatusAccepted || terminalResponse.Header().Get("Idempotency-Replayed") != "true" {
		t.Fatalf("terminal replay status=%d body=%s", terminalResponse.Code, terminalResponse.Body.String())
	}
	var terminal storage.RunRecord
	if err := json.Unmarshal(terminalResponse.Body.Bytes(), &terminal); err != nil {
		t.Fatal(err)
	}
	if terminal.RunID != first.RunID || terminal.Status != string(core.RunCompleted) {
		t.Fatalf("terminal replay wrong: %#v", terminal)
	}
}

func TestAsyncSubmitRejectsInvalidIdempotencyKey(t *testing.T) {
	fixture := newRunWorkerFixture(t)
	response := enqueueRunRecorder(t, fixture, "invalid key", strings.Repeat("k", 257))
	if response.Code != http.StatusBadRequest {
		t.Fatalf("invalid key status=%d body=%s", response.Code, response.Body.String())
	}
	var count int
	if err := fixture.db.QueryRow("SELECT COUNT(*) FROM run_control").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("invalid idempotency request created %d runs", count)
	}
}

func TestAsyncQueuedRunCanBeCancelledWithoutWorker(t *testing.T) {
	fixture := newRunWorkerFixture(t)
	record := enqueueRunHTTP(t, fixture, "cancel me")
	request := httptest.NewRequest(http.MethodPost, "/v1/sessions/"+fixture.session.ID()+"/runs/"+record.RunID+"/cancel", nil)
	response := httptest.NewRecorder()
	fixture.server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusAccepted {
		t.Fatalf("cancel status=%d body=%s", response.Code, response.Body.String())
	}
	terminal, err := fixture.queue.GetRun(context.Background(), record.RunID)
	if err != nil || terminal.Status != string(core.RunCancelled) {
		t.Fatalf("queued run was not cancelled: %#v err=%v", terminal, err)
	}
	if claimed, err := fixture.server.RunWorkerOnce(context.Background(), "worker-after-cancel"); err != nil || claimed {
		t.Fatalf("cancelled run remained claimable: claimed=%t err=%v", claimed, err)
	}
}

func TestAsyncPermanentPrincipalFailureDoesNotInvokeModel(t *testing.T) {
	fixture := newRunWorkerFixture(t)
	record := enqueueRunHTTP(t, fixture, "disabled account")
	fixture.server.runPrincipal = RunPrincipalResolverFunc(func(context.Context, string, string) (core.Principal, error) {
		return core.Principal{}, PermanentRunFailure(errors.New("account disabled"))
	})
	claimed, err := fixture.server.RunWorkerOnce(context.Background(), "worker-disabled")
	if err == nil || !claimed {
		t.Fatalf("permanent principal failure was not surfaced: claimed=%t err=%v", claimed, err)
	}
	terminal, getErr := fixture.queue.GetRun(context.Background(), record.RunID)
	if getErr != nil || terminal.Status != string(core.RunFailed) || terminal.ErrorCode != "principal_resolution_failed" {
		t.Fatalf("principal failure state is wrong: %#v err=%v", terminal, getErr)
	}
	if fixture.modelRuns.Load() != 0 {
		t.Fatalf("disabled principal reached the model: calls=%d", fixture.modelRuns.Load())
	}
}

func TestAsyncTransientPrincipalFailureReturnsRunToQueue(t *testing.T) {
	fixture := newRunWorkerFixture(t)
	record := enqueueRunHTTP(t, fixture, "temporary account lookup failure")
	fixture.server.runPrincipal = RunPrincipalResolverFunc(func(context.Context, string, string) (core.Principal, error) {
		return core.Principal{}, errors.New("account database temporarily unavailable")
	})
	claimed, err := fixture.server.RunWorkerOnce(context.Background(), "worker-transient")
	if err == nil || !claimed {
		t.Fatalf("transient principal failure was not surfaced: claimed=%t err=%v", claimed, err)
	}
	requeued, getErr := fixture.queue.GetRun(context.Background(), record.RunID)
	if getErr != nil || requeued.Status != storage.RunStatusQueued || requeued.ErrorCode != "principal_resolution_failed" || !requeued.CompletedAt.IsZero() {
		t.Fatalf("transient failure was not requeued: %#v err=%v", requeued, getErr)
	}
	if fixture.modelRuns.Load() != 0 {
		t.Fatalf("transient resolver failure reached the model: calls=%d", fixture.modelRuns.Load())
	}
}

func TestAsyncExistingRunIDIsReconciledWithoutReplay(t *testing.T) {
	fixture := newRunWorkerFixture(t)
	runID := "run-existing-worker"
	if _, err := fixture.session.Append(runID, core.EvRunStart, core.RunStartData{}); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.session.Append(runID, core.EvRunEnd, core.RunEndData{Status: core.RunCompleted}); err != nil {
		t.Fatal(err)
	}
	if err := fixture.sessions.Save(context.Background(), fixture.session, 0); err != nil {
		t.Fatal(err)
	}
	if err := fixture.queue.EnqueueRun(context.Background(), storage.QueuedRun{
		RunRecord: storage.RunRecord{RunID: runID, SessionID: fixture.session.ID(), TenantID: "acme", SubjectID: "alice"},
		Message:   "must not replay", MaxAttempts: 3,
	}); err != nil {
		t.Fatal(err)
	}
	claimed, err := fixture.server.RunWorkerOnce(context.Background(), "worker-reconcile")
	if err != nil || !claimed {
		t.Fatalf("reconciliation failed: claimed=%t err=%v", claimed, err)
	}
	terminal, err := fixture.queue.GetRun(context.Background(), runID)
	if err != nil || terminal.Status != string(core.RunCompleted) {
		t.Fatalf("existing run was not reconciled: %#v err=%v", terminal, err)
	}
	if fixture.modelRuns.Load() != 0 {
		t.Fatalf("existing run replayed the model: calls=%d", fixture.modelRuns.Load())
	}
}

func TestRunWorkersShutdownDrainsClaimedRunBeforeReturning(t *testing.T) {
	fixture := newRunWorkerFixture(t)
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	fixture.server.runtime.Models = core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) {
		return blockingRunWorkerModel{started: started, release: release}, nil
	})
	record := enqueueRunHTTP(t, fixture, "drain me")
	serviceCtx, stopService := context.WithCancel(context.Background())
	defer stopService()
	if err := fixture.server.StartRunWorkers(serviceCtx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("worker did not start the queued run")
	}
	shutdownDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		shutdownDone <- fixture.server.Shutdown(ctx)
	}()
	select {
	case err := <-shutdownDone:
		t.Fatalf("shutdown returned before the active run drained: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-shutdownDone; err != nil {
		t.Fatal(err)
	}
	terminal, err := fixture.queue.GetRun(context.Background(), record.RunID)
	if err != nil || terminal.Status != string(core.RunCompleted) {
		t.Fatalf("drained run did not complete: %#v err=%v", terminal, err)
	}
	if err := fixture.server.StartRunWorkers(serviceCtx); err != nil {
		t.Fatalf("workers did not restart after clean shutdown: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := fixture.server.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestRunWorkersForcedShutdownLeavesClaimRecoverable(t *testing.T) {
	fixture := newRunWorkerFixture(t)
	fixture.server.runWorkerClaimTTL = 30 * time.Millisecond
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	fixture.server.runtime.Models = core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) {
		return blockingRunWorkerModel{started: started, release: release}, nil
	})
	record := enqueueRunHTTP(t, fixture, "force stop")
	serviceCtx, stopService := context.WithCancel(context.Background())
	defer stopService()
	if err := fixture.server.StartRunWorkers(serviceCtx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("worker did not start the queued run")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	err := fixture.server.Shutdown(ctx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("forced shutdown returned %v", err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		fixture.server.workersMu.Lock()
		running := fixture.server.workersRunning
		fixture.server.workersMu.Unlock()
		if !running {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("forced worker did not stop")
		}
		time.Sleep(time.Millisecond)
	}
	current, err := fixture.queue.GetRun(context.Background(), record.RunID)
	if err != nil || current.Status != storage.RunStatusRunning || !current.CompletedAt.IsZero() {
		t.Fatalf("forced shutdown wrote a false terminal state: %#v err=%v", current, err)
	}
	if _, err := fixture.db.ExecContext(context.Background(), "UPDATE run_queue SET lease_expires_at = 1 WHERE run_id = ?", record.RunID); err != nil {
		t.Fatal(err)
	}
	if requeued, failed, err := fixture.queue.RecoverExpiredRunClaims(context.Background(), time.Now().UTC()); err != nil || requeued != 1 || failed != 0 {
		t.Fatalf("forced claim was not recoverable: requeued=%d failed=%d err=%v", requeued, failed, err)
	}
	close(release)
}

func TestRunWorkerClaimLossCancelsExecutionWithoutTerminalOverwrite(t *testing.T) {
	fixture := newRunWorkerFixture(t)
	fixture.server.runWorkerClaimTTL = 30 * time.Millisecond
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	fixture.server.runtime.Models = core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) {
		return blockingRunWorkerModel{started: started, release: release}, nil
	})
	record := enqueueRunHTTP(t, fixture, "lose claim")
	result := make(chan error, 1)
	go func() {
		_, err := fixture.server.RunWorkerOnce(context.Background(), "worker-old")
		result <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("worker did not enter model execution")
	}
	if _, err := fixture.db.ExecContext(context.Background(),
		"UPDATE run_queue SET worker_id = ?, lease_expires_at = ? WHERE run_id = ?",
		"worker-new", time.Now().UTC().Add(time.Minute).UnixMilli(), record.RunID,
	); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if !errors.Is(err, errRunClaimLost) {
			t.Fatalf("claim loss returned wrong error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("claim loss did not cancel execution")
	}
	current, err := fixture.queue.GetRun(context.Background(), record.RunID)
	if err != nil || current.Status != storage.RunStatusRunning || !current.CompletedAt.IsZero() {
		t.Fatalf("old worker overwrote the live claim: %#v err=%v", current, err)
	}
	close(release)
}

func TestRunClaimRecoveryLoopRequeuesExpiredWorkerWithoutRestart(t *testing.T) {
	fixture := newRunWorkerFixture(t)
	fixture.server.runWorkerClaimTTL = 300 * time.Millisecond
	record := enqueueRunHTTP(t, fixture, "recover me")
	claimedRun, claimed, err := fixture.queue.ClaimRun(context.Background(), "worker-dead", time.Minute)
	if err != nil || !claimed {
		t.Fatalf("setup claim failed: %#v claimed=%t err=%v", claimedRun, claimed, err)
	}
	serviceCtx, stopService := context.WithCancel(context.Background())
	defer stopService()
	if err := fixture.server.StartRunWorkers(serviceCtx); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.db.ExecContext(context.Background(), "UPDATE run_queue SET lease_expires_at = 1 WHERE run_id = ?", record.RunID); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		terminal, getErr := fixture.queue.GetRun(context.Background(), record.RunID)
		if getErr == nil && terminal.Status == string(core.RunCompleted) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("runtime claim recovery did not complete the run: %#v err=%v", terminal, getErr)
		}
		time.Sleep(5 * time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := fixture.server.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestRunQueryHidesOtherUsersRun(t *testing.T) {
	fixture := newRunWorkerFixture(t)
	record := enqueueRunHTTP(t, fixture, "private")
	segments := fixture.principal.Scope.Segments()
	segments[len(segments)-1] = core.ScopeRef{Kind: core.ScopeUser, ID: "bob"}
	bobScope, err := core.NewScopePath(segments...)
	if err != nil {
		t.Fatal(err)
	}
	fixture.server.authenticator = AuthenticatorFunc(func(r *http.Request) (core.Principal, error) {
		if r.Header.Get("X-Harness-Subject") == "bob" {
			return core.Principal{SubjectID: "bob", TenantID: "acme", Scope: bobScope, Grants: core.NewPermissionSet(core.PermRead)}, nil
		}
		return fixture.principal, nil
	})
	request := httptest.NewRequest(http.MethodGet, "/v1/sessions/"+fixture.session.ID()+"/runs/"+record.RunID, nil)
	request.Header.Set("X-Harness-Subject", "bob")
	response := httptest.NewRecorder()
	fixture.server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusNotFound {
		t.Fatalf("cross-user run query status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestAsyncApprovalReleasesWorkerAndResumesSameRun(t *testing.T) {
	fixture := newRunWorkerFixture(t)
	providerCalls := enableApprovalWorker(t, fixture)
	record := enqueueRunHTTP(t, fixture, "release payment")
	claimed, err := fixture.server.RunWorkerOnce(context.Background(), "worker-approval-first")
	if err != nil || !claimed {
		t.Fatalf("first worker failed: claimed=%t err=%v", claimed, err)
	}
	waiting, err := fixture.queue.GetRun(context.Background(), record.RunID)
	if err != nil || waiting.Status != storage.RunStatusWaitingApproval || !waiting.CompletedAt.IsZero() {
		t.Fatalf("run did not pause durably: %#v err=%v", waiting, err)
	}
	if providerCalls.Load() != 0 {
		t.Fatal("provider executed before approval")
	}
	approvals, err := fixture.approvals.ListApprovals(context.Background(), storage.ApprovalFilter{
		TenantID: "acme", Status: core.ApprovalPending,
	})
	if err != nil || len(approvals) != 1 || approvals[0].RunID != record.RunID {
		t.Fatalf("pending approval missing: %#v err=%v", approvals, err)
	}
	if _, changed, err := fixture.approvals.DecideApproval(context.Background(), approvals[0].ID, core.ApprovalApproved, "admin@acme"); err != nil || !changed {
		t.Fatalf("approval decision failed: changed=%t err=%v", changed, err)
	}
	claimed, err = fixture.server.RunWorkerOnce(context.Background(), "worker-approval-resume")
	if err != nil || !claimed {
		t.Fatalf("resume worker failed: claimed=%t err=%v", claimed, err)
	}
	terminal, err := fixture.queue.GetRun(context.Background(), record.RunID)
	if err != nil || terminal.Status != string(core.RunCompleted) {
		t.Fatalf("approved run did not complete: %#v err=%v", terminal, err)
	}
	if providerCalls.Load() != 1 {
		t.Fatalf("provider calls=%d, want 1", providerCalls.Load())
	}
	loaded, err := fixture.sessions.Load(context.Background(), fixture.session.ID())
	if err != nil {
		t.Fatal(err)
	}
	if status, exists := loaded.RunStatus(record.RunID); !exists || status != core.RunCompleted {
		t.Fatalf("resumed session status=%q exists=%t", status, exists)
	}
}

func TestQueuedApprovalResumeUsesDistinctGenerationBoundLeaseHolders(t *testing.T) {
	fixture := newRunWorkerFixture(t)
	leaser := &recordingSQLSessionLeaser{SQLSessionStore: fixture.sessions}
	fixture.server.leaser = leaser
	enableApprovalWorker(t, fixture)
	record := enqueueRunHTTP(t, fixture, "approval holder generations")
	if claimed, err := fixture.server.RunWorkerOnce(context.Background(), "worker-holder-first"); err != nil || !claimed {
		t.Fatalf("first worker claimed=%t err=%v", claimed, err)
	}
	var firstGeneration int64
	if err := fixture.db.QueryRowContext(context.Background(), "SELECT generation FROM run_queue WHERE run_id = ?", record.RunID).Scan(&firstGeneration); err != nil {
		t.Fatal(err)
	}
	approvals, err := fixture.approvals.ListApprovals(context.Background(), storage.ApprovalFilter{TenantID: fixture.principal.TenantID, Status: core.ApprovalPending})
	if err != nil || len(approvals) != 1 {
		t.Fatalf("approval pause: approvals=%#v err=%v", approvals, err)
	}
	if _, changed, err := fixture.approvals.DecideApproval(context.Background(), approvals[0].ID, core.ApprovalApproved, "admin@acme"); err != nil || !changed {
		t.Fatalf("approve: changed=%t err=%v", changed, err)
	}
	if claimed, err := fixture.server.RunWorkerOnce(context.Background(), "worker-holder-second"); err != nil || !claimed {
		t.Fatalf("resume worker claimed=%t err=%v", claimed, err)
	}
	holders := leaser.acquiredHolders()
	if len(holders) != 2 || holders[0] == holders[1] {
		t.Fatalf("lease acquisition holders=%q", holders)
	}
	if !strings.HasPrefix(holders[0], fixture.server.instanceID+":r") ||
		!strings.Contains(holders[0], ":g"+strconv.FormatInt(firstGeneration, 10)+":") ||
		strings.Contains(holders[0], record.RunID) {
		t.Fatalf("first holder does not bind the first generation without exposing the raw run ID: %q", holders[0])
	}
	if !strings.HasPrefix(holders[1], fixture.server.instanceID+":r") ||
		!strings.Contains(holders[1], ":g"+strconv.FormatInt(firstGeneration+1, 10)+":") ||
		strings.Contains(holders[1], record.RunID) {
		t.Fatalf("second holder does not bind the successor generation without exposing the raw run ID: %q", holders[1])
	}
}

func TestApprovalDecisionRoutesAreTenantScoped(t *testing.T) {
	fixture := newRunWorkerFixture(t)
	enableApprovalWorker(t, fixture)
	record := enqueueRunHTTP(t, fixture, "release payment")
	if _, err := fixture.server.RunWorkerOnce(context.Background(), "worker-route-pause"); err != nil {
		t.Fatal(err)
	}
	approvals, err := fixture.approvals.ListApprovals(context.Background(), storage.ApprovalFilter{TenantID: "acme"})
	if err != nil || len(approvals) != 1 {
		t.Fatalf("approval setup failed: %#v err=%v", approvals, err)
	}
	admin := fixture.principal
	admin.SubjectID = "tenant-admin@acme"
	admin.Attributes = map[string]string{"role": storage.RoleAccountTenantAdmin}
	fixture.server.authenticator = AuthenticatorFunc(func(r *http.Request) (core.Principal, error) {
		if r.Header.Get("X-Test-Tenant") == "other" {
			other := admin
			other.TenantID = "other"
			return other, nil
		}
		return admin, nil
	})
	decisionBody := bytes.NewBufferString(`{"decision":"approved"}`)
	request := httptest.NewRequest(http.MethodPost, "/v1/admin/approvals/"+approvals[0].ID+"/decision", decisionBody)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Test-Tenant", "other")
	response := httptest.NewRecorder()
	fixture.server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusNotFound {
		t.Fatalf("other tenant decision status=%d body=%s", response.Code, response.Body.String())
	}
	request = httptest.NewRequest(http.MethodPost, "/v1/admin/approvals/"+approvals[0].ID+"/decision", bytes.NewBufferString(`{"decision":"approved"}`))
	request.Header.Set("Content-Type", "application/json")
	response = httptest.NewRecorder()
	fixture.server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("tenant decision status=%d body=%s", response.Code, response.Body.String())
	}
	if _, err := fixture.server.RunWorkerOnce(context.Background(), "worker-route-resume"); err != nil {
		t.Fatal(err)
	}
	terminal, err := fixture.queue.GetRun(context.Background(), record.RunID)
	if err != nil || terminal.Status != string(core.RunCompleted) {
		t.Fatalf("route-approved run did not complete: %#v err=%v", terminal, err)
	}
}

func TestApprovalDecisionRouteIsIdempotentAndResumesOnlyOnce(t *testing.T) {
	fixture := newRunWorkerFixture(t)
	providerCalls := enableApprovalWorker(t, fixture)
	record := enqueueRunHTTP(t, fixture, "release payment")
	if claimed, err := fixture.server.RunWorkerOnce(context.Background(), "worker-route-pause"); err != nil || !claimed {
		t.Fatalf("pause worker failed: claimed=%t err=%v", claimed, err)
	}
	approvals, err := fixture.approvals.ListApprovals(context.Background(), storage.ApprovalFilter{TenantID: "acme", Status: core.ApprovalPending})
	if err != nil || len(approvals) != 1 {
		t.Fatalf("approval setup failed: %#v err=%v", approvals, err)
	}
	admin := fixture.principal
	admin.SubjectID = "tenant-admin@acme"
	admin.Attributes = map[string]string{"role": storage.RoleAccountTenantAdmin}
	fixture.server.authenticator = AuthenticatorFunc(func(*http.Request) (core.Principal, error) {
		return admin, nil
	})

	decide := func() storage.ApprovalRecord {
		t.Helper()
		request := httptest.NewRequest(http.MethodPost, "/v1/admin/approvals/"+approvals[0].ID+"/decision", bytes.NewBufferString(`{"decision":"approved"}`))
		request.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		fixture.server.Handler().ServeHTTP(response, request)
		if response.Code != http.StatusOK {
			t.Fatalf("decision status=%d body=%s", response.Code, response.Body.String())
		}
		var decided storage.ApprovalRecord
		if err := json.Unmarshal(response.Body.Bytes(), &decided); err != nil {
			t.Fatal(err)
		}
		return decided
	}

	first := decide()
	second := decide()
	if first.Status != core.ApprovalApproved || second.Status != core.ApprovalApproved || second.DecidedAt.UnixMilli() != first.DecidedAt.UnixMilli() {
		t.Fatalf("duplicate decision changed durable approval: first=%#v second=%#v", first, second)
	}
	if claimed, err := fixture.server.RunWorkerOnce(context.Background(), "worker-route-resume"); err != nil || !claimed {
		t.Fatalf("resume worker failed: claimed=%t err=%v", claimed, err)
	}
	if claimed, err := fixture.server.RunWorkerOnce(context.Background(), "worker-route-duplicate"); err != nil || claimed {
		t.Fatalf("duplicate decision left another run claimable: claimed=%t err=%v", claimed, err)
	}
	terminal, err := fixture.queue.GetRun(context.Background(), record.RunID)
	if err != nil || terminal.Status != string(core.RunCompleted) {
		t.Fatalf("approved run terminal state wrong: %#v err=%v", terminal, err)
	}
	if providerCalls.Load() != 1 {
		t.Fatalf("approved continuation executed provider %d times, want exactly 1", providerCalls.Load())
	}
	loaded, err := fixture.sessions.Load(context.Background(), fixture.session.ID())
	if err != nil {
		t.Fatal(err)
	}
	resumes := 0
	for _, event := range loaded.Events() {
		if event.RunID == record.RunID && event.Type == core.EvRunResume {
			resumes++
		}
	}
	if resumes != 1 {
		t.Fatalf("duplicate approval created %d run/resume events, want 1", resumes)
	}
}

func TestSynchronousRunCanPauseAndResumeOnDurableWorker(t *testing.T) {
	fixture := newRunWorkerFixture(t)
	providerCalls := enableApprovalWorker(t, fixture)
	request := httptest.NewRequest(http.MethodPost, "/v1/sessions/"+fixture.session.ID()+"/runs", bytes.NewBufferString(`{"message":"release payment"}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	fixture.server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK || !bytes.Contains(response.Body.Bytes(), []byte("approval/requested")) {
		t.Fatalf("sync pause status=%d body=%s", response.Code, response.Body.String())
	}
	approvals, err := fixture.approvals.ListApprovals(context.Background(), storage.ApprovalFilter{TenantID: "acme", Status: core.ApprovalPending})
	if err != nil || len(approvals) != 1 {
		t.Fatalf("sync approval missing: %#v err=%v", approvals, err)
	}
	run, err := fixture.queue.GetRun(context.Background(), approvals[0].RunID)
	if err != nil || run.Status != storage.RunStatusWaitingApproval {
		t.Fatalf("sync run did not move to durable waiting state: %#v err=%v", run, err)
	}
	if _, changed, err := fixture.approvals.DecideApproval(context.Background(), approvals[0].ID, core.ApprovalApproved, "admin@acme"); err != nil || !changed {
		t.Fatalf("sync approval decision failed: changed=%t err=%v", changed, err)
	}
	if _, err := fixture.server.RunWorkerOnce(context.Background(), "worker-sync-resume"); err != nil {
		t.Fatal(err)
	}
	terminal, err := fixture.queue.GetRun(context.Background(), approvals[0].RunID)
	if err != nil || terminal.Status != string(core.RunCompleted) || providerCalls.Load() != 1 {
		t.Fatalf("sync continuation failed: %#v calls=%d err=%v", terminal, providerCalls.Load(), err)
	}
}

func TestWaitingApprovalCancellationClosesControlAndSession(t *testing.T) {
	fixture := newRunWorkerFixture(t)
	enableApprovalWorker(t, fixture)
	record := enqueueRunHTTP(t, fixture, "cancel pending approval")
	if _, err := fixture.server.RunWorkerOnce(context.Background(), "worker-cancel-approval"); err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/v1/sessions/"+fixture.session.ID()+"/runs/"+record.RunID+"/cancel", nil)
	response := httptest.NewRecorder()
	fixture.server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusAccepted {
		t.Fatalf("cancel waiting approval status=%d body=%s", response.Code, response.Body.String())
	}
	terminal, err := fixture.queue.GetRun(context.Background(), record.RunID)
	if err != nil || terminal.Status != string(core.RunCancelled) {
		t.Fatalf("run control not cancelled: %#v err=%v", terminal, err)
	}
	loaded, err := fixture.sessions.Load(context.Background(), fixture.session.ID())
	if err != nil {
		t.Fatal(err)
	}
	if status, exists := loaded.RunStatus(record.RunID); !exists || status != core.RunCancelled {
		t.Fatalf("session not closed after cancellation: status=%q exists=%t", status, exists)
	}
	approvals, err := fixture.approvals.ListApprovals(context.Background(), storage.ApprovalFilter{TenantID: "acme"})
	if err != nil || len(approvals) != 1 || approvals[0].Status != core.ApprovalDenied {
		t.Fatalf("pending approval not closed: %#v err=%v", approvals, err)
	}
	if claimed, err := fixture.server.RunWorkerOnce(context.Background(), "worker-after-approval-cancel"); err != nil || claimed {
		t.Fatalf("cancelled approval run remained claimable: claimed=%t err=%v", claimed, err)
	}
}

func TestApprovalExpiryLoopResumesDeniedPath(t *testing.T) {
	fixture := newRunWorkerFixture(t)
	providerCalls := enableApprovalWorker(t, fixture)
	record := enqueueRunHTTP(t, fixture, "expire pending approval")
	if _, err := fixture.server.RunWorkerOnce(context.Background(), "worker-expiry-pause"); err != nil {
		t.Fatal(err)
	}
	approvals, err := fixture.approvals.ListApprovals(context.Background(), storage.ApprovalFilter{TenantID: "acme"})
	if err != nil || len(approvals) != 1 {
		t.Fatalf("approval setup failed: %#v err=%v", approvals, err)
	}
	if _, err := fixture.db.ExecContext(context.Background(), "UPDATE approval_requests SET expires_at = 1 WHERE id = ?", approvals[0].ID); err != nil {
		t.Fatal(err)
	}
	serviceCtx, stopService := context.WithCancel(context.Background())
	defer stopService()
	if err := fixture.server.StartRunWorkers(serviceCtx); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		terminal, getErr := fixture.queue.GetRun(context.Background(), record.RunID)
		if getErr == nil && terminal.Status == string(core.RunCompleted) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expired approval did not resume: %#v err=%v", terminal, getErr)
		}
		time.Sleep(5 * time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := fixture.server.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if providerCalls.Load() != 0 {
		t.Fatalf("expired approval executed provider %d times", providerCalls.Load())
	}
	expired, err := fixture.approvals.GetApproval(context.Background(), approvals[0].ID)
	if err != nil || expired.Status != core.ApprovalExpired {
		t.Fatalf("approval did not reach expired state: %#v err=%v", expired, err)
	}
}

func TestApprovedRunRevokedBeforeResumeFailsBothLedgers(t *testing.T) {
	fixture := newRunWorkerFixture(t)
	providerCalls := enableApprovalWorker(t, fixture)
	record := enqueueRunHTTP(t, fixture, "revoke before resume")
	if _, err := fixture.server.RunWorkerOnce(context.Background(), "worker-revoke-pause"); err != nil {
		t.Fatal(err)
	}
	approvals, err := fixture.approvals.ListApprovals(context.Background(), storage.ApprovalFilter{TenantID: "acme"})
	if err != nil || len(approvals) != 1 {
		t.Fatalf("approval setup failed: %#v err=%v", approvals, err)
	}
	if _, changed, err := fixture.approvals.DecideApproval(context.Background(), approvals[0].ID, core.ApprovalApproved, "admin@acme"); err != nil || !changed {
		t.Fatalf("approval failed: changed=%t err=%v", changed, err)
	}
	fixture.server.runPrincipal = RunPrincipalResolverFunc(func(context.Context, string, string) (core.Principal, error) {
		return core.Principal{}, PermanentRunFailure(errors.New("account disabled"))
	})
	claimed, err := fixture.server.RunWorkerOnce(context.Background(), "worker-revoke-resume")
	if err == nil || !claimed {
		t.Fatalf("revoked resume did not fail: claimed=%t err=%v", claimed, err)
	}
	terminal, err := fixture.queue.GetRun(context.Background(), record.RunID)
	if err != nil || terminal.Status != string(core.RunFailed) || terminal.ErrorCode != "principal_resolution_failed" {
		t.Fatalf("run control failure wrong: %#v err=%v", terminal, err)
	}
	loaded, err := fixture.sessions.Load(context.Background(), fixture.session.ID())
	if err != nil {
		t.Fatal(err)
	}
	if status, exists := loaded.RunStatus(record.RunID); !exists || status != core.RunFailed {
		t.Fatalf("session remained suspended after revocation: status=%q exists=%t", status, exists)
	}
	if providerCalls.Load() != 0 {
		t.Fatalf("revoked principal reached provider %d times", providerCalls.Load())
	}
}
