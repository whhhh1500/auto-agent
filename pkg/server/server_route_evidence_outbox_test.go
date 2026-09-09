package server

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/whhhh1500/auto-agent/internal/executionroute"
	"github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/storage"
	_ "modernc.org/sqlite"
)

type recordingRouteEvidenceOutbox struct {
	mu       sync.Mutex
	inputs   []storage.RouteEvidenceReceiptInput
	claim    storage.RouteEvidenceOutboxClaim
	claimOK  bool
	ackOK    bool
	ackCalls int
}

func (o *recordingRouteEvidenceOutbox) CreateOrLoadRouteEvidenceReceipt(_ context.Context, input storage.RouteEvidenceReceiptInput) (storage.RouteEvidenceReceipt, bool, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	input.Payload = append([]byte(nil), input.Payload...)
	o.inputs = append(o.inputs, input)
	return storage.RouteEvidenceReceipt{ReceiptID: storage.RouteEvidenceReceiptID(input), Protocol: input.Protocol, SessionID: input.SessionID, RunID: input.RunID, TerminalEventSeq: input.TerminalEventSeq, SourceSessionVersion: input.SourceSessionVersion, TerminalStatus: input.TerminalStatus, Payload: input.Payload, PayloadSHA256: input.PayloadSHA256}, true, nil
}

func (o *recordingRouteEvidenceOutbox) ClaimRouteEvidenceReceipt(context.Context, string, time.Duration) (storage.RouteEvidenceOutboxClaim, bool, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.claim, o.claimOK, nil
}

func (o *recordingRouteEvidenceOutbox) AckRouteEvidenceReceipt(context.Context, string, string, int64) (bool, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.ackCalls++
	return o.ackOK, nil
}

func (*recordingRouteEvidenceOutbox) RetryRouteEvidenceReceipt(context.Context, string, string, int64, time.Time, string) (bool, error) {
	return true, nil
}

func (o *recordingRouteEvidenceOutbox) snapshotInputs() []storage.RouteEvidenceReceiptInput {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]storage.RouteEvidenceReceiptInput(nil), o.inputs...)
}

type staticRouteEvidenceCandidates struct {
	candidates []storage.RouteEvidenceTerminalCandidate
}

func (c staticRouteEvidenceCandidates) ListTerminalRouteEvidenceCandidates(context.Context, storage.RouteEvidenceTerminalCursor, int) ([]storage.RouteEvidenceTerminalCandidate, storage.RouteEvidenceTerminalCursor, error) {
	return append([]storage.RouteEvidenceTerminalCandidate(nil), c.candidates...), storage.RouteEvidenceTerminalCursor{}, nil
}

type transientRouteEvidenceSessionStore struct{ core.SessionStore }

func (transientRouteEvidenceSessionStore) Load(context.Context, string) (*core.Session, error) {
	return nil, errors.New("temporary session load failure")
}

type recordingRouteEvidenceDelivery struct {
	mu       sync.Mutex
	receipts []storage.RouteEvidenceReceipt
	notify   chan struct{}
}

func (d *recordingRouteEvidenceDelivery) Deliver(_ context.Context, receipt storage.RouteEvidenceReceipt) error {
	d.mu.Lock()
	d.receipts = append(d.receipts, receipt)
	d.mu.Unlock()
	if d.notify != nil {
		select {
		case d.notify <- struct{}{}:
		default:
		}
	}
	return nil
}

func (d *recordingRouteEvidenceDelivery) snapshot() []storage.RouteEvidenceReceipt {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]storage.RouteEvidenceReceipt(nil), d.receipts...)
}

func terminalRouteEvidenceFixture(t *testing.T, status core.RunStatus) (*core.Session, core.Principal, string) {
	t.Helper()
	session, principal, runID := routeEvidenceProbeReceiptFixture(t)
	if _, err := session.Append(runID, core.EvRunEnd, core.RunEndData{Status: status}); err != nil {
		t.Fatal(err)
	}
	return session, principal, runID
}

func malformedExpectedV2TerminalFixture(t *testing.T, status core.RunStatus) (*core.Session, core.Principal, string) {
	t.Helper()
	original, principal, runID := routeEvidenceProbeReceiptFixture(t)
	events := original.Events()
	var start core.RunStartData
	if err := json.Unmarshal(events[0].Data, &start); err != nil || start.Composition == nil {
		t.Fatalf("decode fixture run start: %v", err)
	}
	start.Composition.Metadata[executionroute.RouteModeKey] = "malformed-route-mode"
	compositionRevision, err := core.CompositionRevision(start.Composition)
	if err != nil {
		t.Fatal(err)
	}
	assignmentRevision, err := core.CompositionMetadataRevision(start.Composition.Metadata)
	if err != nil {
		t.Fatal(err)
	}
	start.CompositionRevision = compositionRevision
	start.AssignmentRevision = assignmentRevision
	sessionID := "route-evidence-malformed-session"
	scope, err := principal.Scope.Child(core.ScopeRef{Kind: core.ScopeSession, ID: sessionID})
	if err != nil {
		t.Fatal(err)
	}
	session, err := core.NewSession(core.SessionOptions{ID: sessionID, ProfileID: original.ProfileID(), Scope: scope, Principal: principal})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Append(runID, core.EvRunStart, start); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Append(runID, core.EvRunEnd, core.RunEndData{Status: status}); err != nil {
		t.Fatal(err)
	}
	return session, principal, runID
}

func TestRouteEvidenceMaterializerBuildsReceiptsForDirectAndQueuedTerminals(t *testing.T) {
	for _, status := range []core.RunStatus{core.RunCompleted, core.RunFailed} {
		t.Run(string(status), func(t *testing.T) {
			session, principal, runID := terminalRouteEvidenceFixture(t, status)
			outbox := &recordingRouteEvidenceOutbox{}
			server := &Server{routeEvidenceOutbox: outbox}
			if err := server.materializeTerminalRouteEvidence(context.Background(), &core.Runtime{}, session, principal, runID, status); err != nil {
				t.Fatal(err)
			}
			inputs := outbox.snapshotInputs()
			if len(inputs) != 1 {
				t.Fatalf("receipt inputs=%d want=1", len(inputs))
			}
			input := inputs[0]
			if input.TerminalEventSeq != session.Version()-1 || input.SourceSessionVersion != session.Version() || input.TerminalStatus != string(status) {
				t.Fatalf("terminal binding=%+v session version=%d", input, session.Version())
			}
			var payload map[string]string
			if err := json.Unmarshal(input.Payload, &payload); err != nil {
				t.Fatal(err)
			}
			if payload["programmatic.route.identity.status"] != string(executionroute.EvidenceStatusVerified) || payload["run.status"] != string(status) {
				t.Fatalf("receipt payload=%v", payload)
			}
		})
	}
}

func TestRouteEvidenceMaterializerStoresExpectedV2UnavailableProjection(t *testing.T) {
	session, principal, runID := malformedExpectedV2TerminalFixture(t, core.RunFailed)
	outbox := &recordingRouteEvidenceOutbox{}
	server := &Server{routeEvidenceOutbox: outbox}
	if err := server.materializeTerminalRouteEvidence(context.Background(), &core.Runtime{}, session, principal, runID, core.RunFailed); err != nil {
		t.Fatal(err)
	}
	inputs := outbox.snapshotInputs()
	if len(inputs) != 1 {
		t.Fatalf("receipt inputs=%d want=1", len(inputs))
	}
	var payload map[string]string
	if err := json.Unmarshal(inputs[0].Payload, &payload); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"run.id": runID, "session.id": session.ID(), "run.status": string(core.RunFailed), "programmatic.route.identity.status": string(executionroute.EvidenceStatusUnavailable)}
	if !reflect.DeepEqual(payload, want) {
		t.Fatalf("unavailable receipt payload=%v want=%v", payload, want)
	}
}

func TestRouteEvidenceMaterializerSkipsNonV2Terminal(t *testing.T) {
	session, principal, runID := routeEvidenceProbeReceiptFixture(t)
	plainScope, err := principal.Scope.Child(core.ScopeRef{Kind: core.ScopeSession, ID: "route-evidence-non-v2"})
	if err != nil {
		t.Fatal(err)
	}
	plain, err := core.NewSession(core.SessionOptions{ID: "route-evidence-non-v2", ProfileID: session.ProfileID(), Scope: plainScope, Principal: principal})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := plain.Append(runID, core.EvRunStart, core.RunStartData{}); err != nil {
		t.Fatal(err)
	}
	if _, err := plain.Append(runID, core.EvRunEnd, core.RunEndData{Status: core.RunCompleted}); err != nil {
		t.Fatal(err)
	}
	outbox := &recordingRouteEvidenceOutbox{}
	server := &Server{routeEvidenceOutbox: outbox}
	if err := server.materializeTerminalRouteEvidence(context.Background(), &core.Runtime{}, plain, principal, runID, core.RunCompleted); err != nil {
		t.Fatal(err)
	}
	if got := len(outbox.snapshotInputs()); got != 0 {
		t.Fatalf("non-v2 receipt inputs=%d", got)
	}
}

func TestRouteEvidenceReconcilerReloadsAndMaterializesCanonicalTerminal(t *testing.T) {
	session, principal, runID := terminalRouteEvidenceFixture(t, core.RunCompleted)
	sessions := core.NewMemorySessionStore()
	if err := sessions.Create(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	outbox := &recordingRouteEvidenceOutbox{}
	server := &Server{
		routeEvidenceOutbox:     outbox,
		routeEvidenceCandidates: staticRouteEvidenceCandidates{candidates: []storage.RouteEvidenceTerminalCandidate{{RunID: runID, SessionID: session.ID(), TenantID: principal.TenantID, SubjectID: principal.SubjectID, Status: string(core.RunCompleted)}}},
		sessions:                sessions,
		runtime:                 &core.Runtime{},
	}
	server.reconcileRouteEvidenceOnce(context.Background())
	if got := len(outbox.snapshotInputs()); got != 1 {
		t.Fatalf("reconciled receipt inputs=%d want=1", got)
	}
}

func TestRouteEvidenceReconcilerKeepsCursorBeforeTransientFailure(t *testing.T) {
	cursor := storage.RouteEvidenceTerminalCursor{CompletedAt: time.Unix(1700000000, 0).UTC(), RunID: "before-failure"}
	server := &Server{
		routeEvidenceOutbox:     &recordingRouteEvidenceOutbox{},
		routeEvidenceCandidates: staticRouteEvidenceCandidates{candidates: []storage.RouteEvidenceTerminalCandidate{{RunID: "route-evidence-run", SessionID: "route-evidence-session", TenantID: "tenant", SubjectID: "subject", Status: string(core.RunCompleted)}}},
		sessions:                transientRouteEvidenceSessionStore{SessionStore: core.NewMemorySessionStore()},
		runtime:                 &core.Runtime{},
		routeEvidenceCursor:     cursor,
	}
	server.reconcileRouteEvidenceOnce(context.Background())
	if !reflect.DeepEqual(server.routeEvidenceCursor, cursor) {
		t.Fatalf("cursor advanced past transient failure: got=%+v want=%+v", server.routeEvidenceCursor, cursor)
	}
}

func TestRouteEvidenceDeliveryIsAtLeastOnceWhenAckIsLost(t *testing.T) {
	receipt := storage.RouteEvidenceReceipt{ReceiptID: "re_test", Protocol: storage.RouteEvidenceReceiptProtocol, SessionID: "session", RunID: "run", TerminalEventSeq: 1, SourceSessionVersion: 2, TerminalStatus: string(core.RunCompleted), Payload: json.RawMessage(`{"run.id":"run"}`), PayloadSHA256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}
	outbox := &recordingRouteEvidenceOutbox{claim: storage.RouteEvidenceOutboxClaim{Receipt: receipt, LeaseGeneration: 1, Attempts: 1}, claimOK: true, ackOK: false}
	delivery := &recordingRouteEvidenceDelivery{}
	server := &Server{instanceID: "instance", routeEvidenceOutbox: outbox, routeEvidenceDelivery: delivery, runWorkerClaimTTL: time.Second}
	if _, err := server.dispatchRouteEvidenceOnce(context.Background()); err == nil {
		t.Fatal("first delivery should surface the lost acknowledgement")
	}
	if _, err := server.dispatchRouteEvidenceOnce(context.Background()); err == nil {
		t.Fatal("repeated delivery should surface the lost acknowledgement")
	}
	receipts := delivery.snapshot()
	if len(receipts) != 2 || receipts[0].ReceiptID != receipt.ReceiptID || receipts[1].ReceiptID != receipt.ReceiptID {
		t.Fatalf("at-least-once receipts=%+v", receipts)
	}
}

func TestRouteEvidenceDeliveryWorkerStopsWithLifecycleContext(t *testing.T) {
	sessions := core.NewMemorySessionStore()
	outbox := &recordingRouteEvidenceOutbox{}
	delivery := &recordingRouteEvidenceDelivery{}
	server, err := New(Config{Runtime: &core.Runtime{}, Sessions: sessions, Authenticator: AuthenticatorFunc(func(*http.Request) (core.Principal, error) { return core.Principal{}, nil }), RouteEvidenceOutbox: outbox, RouteEvidenceCandidateReader: staticRouteEvidenceCandidates{}, RouteEvidenceDelivery: delivery, RunWorkerPollInterval: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := server.StartRunWorkers(ctx); err != nil {
		t.Fatal(err)
	}
	cancel()
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), time.Second)
	defer cancelShutdown()
	if err := server.Shutdown(shutdownCtx); err != nil {
		t.Fatal(err)
	}
}

func TestRouteEvidenceConfigRequiresOutboxAndCreatesDeliveryWorkerIdentity(t *testing.T) {
	base := func() Config {
		return Config{
			Runtime:       &core.Runtime{},
			Sessions:      core.NewMemorySessionStore(),
			Authenticator: AuthenticatorFunc(func(*http.Request) (core.Principal, error) { return core.Principal{}, nil }),
		}
	}
	candidateOnly := base()
	candidateOnly.RouteEvidenceCandidateReader = staticRouteEvidenceCandidates{}
	if _, err := New(candidateOnly); err == nil {
		t.Fatal("candidate reader without an outbox was accepted")
	}
	deliveryOnly := base()
	deliveryOnly.RouteEvidenceDelivery = &recordingRouteEvidenceDelivery{}
	if _, err := New(deliveryOnly); err == nil {
		t.Fatal("delivery without an outbox was accepted")
	}
	outboxOnly := base()
	outboxOnly.RouteEvidenceOutbox = &recordingRouteEvidenceOutbox{}
	if _, err := New(outboxOnly); err == nil {
		t.Fatal("outbox without a candidate reader was accepted")
	}
	configured := base()
	configured.RouteEvidenceOutbox = &recordingRouteEvidenceOutbox{}
	configured.RouteEvidenceCandidateReader = staticRouteEvidenceCandidates{}
	configured.RouteEvidenceDelivery = &recordingRouteEvidenceDelivery{}
	server, err := New(configured)
	if err != nil {
		t.Fatal(err)
	}
	if server.instanceID == "" {
		t.Fatal("route evidence delivery server did not receive an instance identity")
	}
}

const routeEvidenceCrashScenarioEnv = "HARNESS_ROUTE_EVIDENCE_CRASH_SCENARIO"

// TestRouteEvidenceDurableOutboxCrashHelper is deliberately re-executed as a
// separate test binary. Each scenario exits after a durable boundary so the
// parent test can prove recovery from a real process loss rather than a mock.
func TestRouteEvidenceDurableOutboxCrashHelper(t *testing.T) {
	scenario := os.Getenv(routeEvidenceCrashScenarioEnv)
	if scenario == "" {
		return
	}
	databasePath := os.Getenv("HARNESS_ROUTE_EVIDENCE_CRASH_DB")
	markerPath := os.Getenv("HARNESS_ROUTE_EVIDENCE_CRASH_MARKER")
	if databasePath == "" || markerPath == "" {
		t.Fatal("crash helper paths are required")
	}
	db, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatal(err)
	}
	sessions, err := storage.OpenSQLSessionStore(context.Background(), db, storage.SQLDialectSQLite)
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	switch scenario {
	case "terminal_before_receipt":
		control, err := storage.NewSQLRunControlStore(db, storage.SQLDialectSQLite)
		if err != nil {
			t.Fatal(err)
		}
		session, principal, runID := terminalRouteEvidenceFixture(t, core.RunCompleted)
		if err := sessions.Create(context.Background(), session); err != nil {
			t.Fatal(err)
		}
		if err := control.CreateRun(context.Background(), storage.RunRecord{RunID: runID, SessionID: session.ID(), TenantID: principal.TenantID, SubjectID: principal.SubjectID}); err != nil {
			t.Fatal(err)
		}
		if err := control.FinishRun(context.Background(), runID, core.RunCompleted, ""); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(markerPath, []byte("terminal-persisted-before-receipt"), 0o600); err != nil {
			t.Fatal(err)
		}
		os.Exit(85)
	case "delivery_before_ack":
		input := routeEvidenceCrashReceiptInput()
		receipt, created, err := sessions.CreateOrLoadRouteEvidenceReceipt(context.Background(), input)
		if err != nil || !created {
			t.Fatalf("create route receipt=%+v created=%t err=%v", receipt, created, err)
		}
		server := &Server{instanceID: "crash-child", routeEvidenceOutbox: sessions, routeEvidenceDelivery: routeEvidenceCrashDelivery{markerPath: markerPath}, runWorkerClaimTTL: time.Millisecond}
		if _, err := server.dispatchRouteEvidenceOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
		t.Fatal("crash delivery returned instead of terminating before acknowledgement")
	default:
		t.Fatalf("unknown crash scenario %q", scenario)
	}
}

type routeEvidenceCrashDelivery struct{ markerPath string }

func (d routeEvidenceCrashDelivery) Deliver(_ context.Context, receipt storage.RouteEvidenceReceipt) error {
	if err := os.WriteFile(d.markerPath, []byte(receipt.ReceiptID), 0o600); err != nil {
		return err
	}
	os.Exit(86)
	return nil
}

func routeEvidenceCrashReceiptInput() storage.RouteEvidenceReceiptInput {
	payload := []byte(`{"programmatic.route.identity.status":"unavailable","run.id":"route-crash-delivery-run","run.status":"completed","session.id":"route-crash-delivery-session"}`)
	sum := sha256.Sum256(payload)
	return storage.RouteEvidenceReceiptInput{
		Protocol: storage.RouteEvidenceReceiptProtocol, SessionID: "route-crash-delivery-session", RunID: "route-crash-delivery-run",
		TerminalEventSeq: 1, SourceSessionVersion: 2, TerminalStatus: string(core.RunCompleted), Payload: payload, PayloadSHA256: fmt.Sprintf("%x", sum),
	}
}

func runRouteEvidenceCrashHelper(t *testing.T, scenario, databasePath, markerPath string, wantExit int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestRouteEvidenceDurableOutboxCrashHelper$")
	cmd.Env = append(os.Environ(), routeEvidenceCrashScenarioEnv+"="+scenario, "HARNESS_ROUTE_EVIDENCE_CRASH_DB="+databasePath, "HARNESS_ROUTE_EVIDENCE_CRASH_MARKER="+markerPath)
	result, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("crash helper scenario=%s timed out: %v output=%s", scenario, ctx.Err(), result)
	}
	exitErr, ok := err.(*exec.ExitError)
	if !ok || exitErr.ExitCode() != wantExit {
		t.Fatalf("crash helper scenario=%s exit=%v output=%s", scenario, err, result)
	}
}

func TestRouteEvidenceReconcilesAfterProcessExitBeforeReceipt(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "terminal-before-receipt.db")
	markerPath := filepath.Join(t.TempDir(), "terminal-before-receipt.marker")
	runRouteEvidenceCrashHelper(t, "terminal_before_receipt", databasePath, markerPath, 85)
	marker, err := os.ReadFile(markerPath)
	if err != nil || string(marker) != "terminal-persisted-before-receipt" {
		t.Fatalf("crash boundary marker=%q err=%v", marker, err)
	}
	db, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	sessions, err := storage.OpenSQLSessionStore(context.Background(), db, storage.SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	server := &Server{routeEvidenceOutbox: sessions, routeEvidenceCandidates: sessions, sessions: sessions, runtime: &core.Runtime{}}
	server.reconcileRouteEvidenceOnce(context.Background())
	server.reconcileRouteEvidenceOnce(context.Background())
	var receipts, pending int
	if err := db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM route_evidence_receipts").Scan(&receipts); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM route_evidence_outbox WHERE state = 'pending'").Scan(&pending); err != nil {
		t.Fatal(err)
	}
	if receipts != 1 || pending != 1 {
		t.Fatalf("reconciled receipts=%d pending=%d want one", receipts, pending)
	}
}

func TestRouteEvidenceRedeliversSameReceiptAfterProcessExitBeforeAck(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "delivery-before-ack.db")
	markerPath := filepath.Join(t.TempDir(), "delivery-before-ack.marker")
	runRouteEvidenceCrashHelper(t, "delivery_before_ack", databasePath, markerPath, 86)
	firstReceiptID, err := os.ReadFile(markerPath)
	if err != nil || len(firstReceiptID) == 0 {
		t.Fatalf("delivery boundary marker=%q err=%v", firstReceiptID, err)
	}
	db, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	sessions, err := storage.OpenSQLSessionStore(context.Background(), db, storage.SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	// The crash left the worker lease durable. Wait for its real expiration;
	// this is intentionally not an SQL mutation so the test covers the actual
	// redelivery path on process restart.
	time.Sleep(routeEvidenceLeaseTTL(time.Millisecond) + 100*time.Millisecond)
	delivery := &recordingRouteEvidenceDelivery{}
	server := &Server{instanceID: "crash-parent", routeEvidenceOutbox: sessions, routeEvidenceDelivery: delivery, runWorkerClaimTTL: time.Millisecond}
	claimed, err := server.dispatchRouteEvidenceOnce(context.Background())
	if err != nil || !claimed {
		t.Fatalf("restart delivery claimed=%t err=%v", claimed, err)
	}
	redelivered := delivery.snapshot()
	if len(redelivered) != 1 || redelivered[0].ReceiptID != string(firstReceiptID) {
		t.Fatalf("redelivery receipts=%+v first receipt=%q", redelivered, firstReceiptID)
	}
	var state, receiptID string
	if err := db.QueryRowContext(context.Background(), "SELECT state, receipt_id FROM route_evidence_outbox").Scan(&state, &receiptID); err != nil {
		t.Fatal(err)
	}
	if state != "delivered" || receiptID != string(firstReceiptID) {
		t.Fatalf("final outbox state=%q receipt=%q first=%q", state, receiptID, firstReceiptID)
	}
}
