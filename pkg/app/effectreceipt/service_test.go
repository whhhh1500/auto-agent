package effectreceipt

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/whhhh1500/auto-agent/pkg/core"
)

func TestExecutePersistsDispatchingBeforeProviderAndDoesNotConfirmAcceptance(t *testing.T) {
	request := testRequest(t, "first payload")
	store := newFakeStore()
	driver := &fakeDriver{ref: request.Intent.Driver}
	driver.dispatchFn = func(_ context.Context, request DispatchRequest) (Submission, error) {
		record := store.mustRecord(t, request.Intent)
		if record.State != StateDispatching {
			t.Fatalf("provider called from %q, want dispatching", record.State)
		}
		return acceptedSubmission(request.Intent), nil
	}
	service := testService(t, store, driver)

	record, err := service.Execute(context.Background(), request)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if record.State != StateAccepted || record.EvidenceDigest != "" {
		t.Fatalf("accepted dispatch became %q with evidence %q", record.State, record.EvidenceDigest)
	}
	if driver.dispatches != 1 || driver.readBacks != 0 {
		t.Fatalf("calls dispatch=%d read-back=%d, want 1 and 0", driver.dispatches, driver.readBacks)
	}
	if got, want := strings.Join(store.operations, ","), "ensure,dispatching,accepted"; got != want {
		t.Fatalf("operation order = %q, want %q", got, want)
	}
}

func TestCrashAfterProviderDispatchRecoversWithReadBackWithoutRedispatch(t *testing.T) {
	request := testRequest(t, "crash window")
	store := newFakeStore()
	store.markAcceptedErr = errors.New("provider receipt containing a secret")
	driver := &fakeDriver{ref: request.Intent.Driver}
	service := testService(t, store, driver)

	if _, err := service.Execute(context.Background(), request); !errors.Is(err, ErrStore) {
		t.Fatalf("execute error = %v, want ErrStore", err)
	}
	if record := store.mustRecord(t, request.Intent); record.State != StateDispatching {
		t.Fatalf("crash-window record state = %q, want dispatching", record.State)
	}
	if driver.dispatches != 1 {
		t.Fatalf("dispatches = %d, want 1", driver.dispatches)
	}

	store.markAcceptedErr = nil
	driver.readBackFn = func(_ context.Context, intent Intent) (Observation, error) {
		return confirmedObservation(intent), nil
	}
	record, err := service.Reconcile(context.Background(), request.Intent)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if record.State != StateConfirmed {
		t.Fatalf("recovered state = %q, want confirmed", record.State)
	}
	if driver.dispatches != 1 || driver.readBacks != 1 {
		t.Fatalf("calls dispatch=%d read-back=%d, want 1 and 1", driver.dispatches, driver.readBacks)
	}
}

func TestConcurrentExecuteOnlyDispatchesForTransitionWinner(t *testing.T) {
	request := testRequest(t, "concurrent")
	store := newFakeStore()
	driver := &fakeDriver{ref: request.Intent.Driver}
	firstBegin := make(chan struct{})
	releaseFirstBegin := make(chan struct{})
	var gateMu sync.Mutex
	firstBeginCall := true
	store.beginGate = func() {
		gateMu.Lock()
		shouldWait := firstBeginCall
		firstBeginCall = false
		gateMu.Unlock()
		if shouldWait {
			close(firstBegin)
			<-releaseFirstBegin
		}
	}
	dispatchStarted := make(chan struct{})
	releaseDispatch := make(chan struct{})
	readBackStarted := make(chan struct{})
	driver.dispatchFn = func(_ context.Context, intentRequest DispatchRequest) (Submission, error) {
		close(dispatchStarted)
		<-releaseDispatch
		return acceptedSubmission(intentRequest.Intent), nil
	}
	driver.readBackFn = func(_ context.Context, intent Intent) (Observation, error) {
		close(readBackStarted)
		return observationFor(intent, ObservationPending), nil
	}
	service := testService(t, store, driver)

	errs := make(chan error, 2)
	go func() { _, err := service.Execute(context.Background(), request); errs <- err }()
	<-firstBegin
	go func() { _, err := service.Execute(context.Background(), request); errs <- err }()
	<-dispatchStarted
	close(releaseFirstBegin)
	select {
	case <-readBackStarted:
	case <-time.After(time.Second):
		t.Fatal("losing Execute did not read back after BeginDispatch declined ownership")
	}
	close(releaseDispatch)
	for range 2 {
		<-errs
	}
	if got := driver.dispatchCount(); got != 1 {
		t.Fatalf("dispatches = %d, want exactly one transition winner", got)
	}
}

func TestAcceptedOnlyBecomesConfirmedFromBoundReadBack(t *testing.T) {
	request := testRequest(t, "acknowledged")
	store := newFakeStore()
	driver := &fakeDriver{ref: request.Intent.Driver}
	service := testService(t, store, driver)

	accepted, err := service.Execute(context.Background(), request)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if accepted.State != StateAccepted {
		t.Fatalf("initial state = %q, want accepted", accepted.State)
	}
	driver.readBackFn = func(_ context.Context, intent Intent) (Observation, error) {
		return confirmedObservation(intent), nil
	}
	confirmed, err := service.Reconcile(context.Background(), request.Intent)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if confirmed.State != StateConfirmed || confirmed.EvidenceDigest == "" {
		t.Fatalf("state = %#v, want confirmed bound evidence", confirmed)
	}
	if driver.dispatches != 1 || driver.readBacks != 1 {
		t.Fatalf("calls dispatch=%d read-back=%d, want 1 and 1", driver.dispatches, driver.readBacks)
	}
}

func TestRecoveryStatesOnlyReadBack(t *testing.T) {
	for _, test := range []struct {
		name        string
		initial     State
		observation ObservationState
		want        State
	}{
		{name: "dispatching confirms", initial: StateDispatching, observation: ObservationConfirmed, want: StateConfirmed},
		{name: "accepted remains accepted when pending", initial: StateAccepted, observation: ObservationPending, want: StateAccepted},
		{name: "unknown confirms", initial: StateUnknown, observation: ObservationConfirmed, want: StateConfirmed},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := testRequest(t, test.name)
			store := newFakeStore()
			store.seed(t, recordInState(request.Intent, test.initial))
			driver := &fakeDriver{ref: request.Intent.Driver}
			driver.readBackFn = func(_ context.Context, intent Intent) (Observation, error) {
				return observationFor(intent, test.observation), nil
			}
			service := testService(t, store, driver)

			record, err := service.Execute(context.Background(), request)
			if err != nil {
				t.Fatalf("execute recovery: %v", err)
			}
			if record.State != test.want {
				t.Fatalf("state = %q, want %q", record.State, test.want)
			}
			if driver.dispatches != 0 || driver.readBacks != 1 {
				t.Fatalf("calls dispatch=%d read-back=%d, want 0 and 1", driver.dispatches, driver.readBacks)
			}
		})
	}
}

func TestConflictingIntentDoesNotDispatch(t *testing.T) {
	first := testRequest(t, "original")
	changed := testRequest(t, "changed")
	store := newFakeStore()
	driver := &fakeDriver{ref: first.Intent.Driver}
	service := testService(t, store, driver)

	if _, err := service.Execute(context.Background(), first); err != nil {
		t.Fatalf("first execute: %v", err)
	}
	if _, err := service.Execute(context.Background(), changed); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflicting execute error = %v, want ErrConflict", err)
	}
	if driver.dispatches != 1 {
		t.Fatalf("dispatches = %d, want 1", driver.dispatches)
	}
}

func TestStoreCannotSubstituteAnotherValidIntent(t *testing.T) {
	request := testRequest(t, "expected")
	foreign := testRequest(t, "foreign")
	base := newFakeStore()
	store := wrongEnsureStore{Store: base, record: Record{Intent: foreign.Intent, State: StatePrepared}}
	driver := &fakeDriver{ref: request.Intent.Driver}
	service, err := NewService(store, driver)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}

	if _, err := service.Execute(context.Background(), request); !errors.Is(err, ErrStore) {
		t.Fatalf("execute error = %v, want ErrStore", err)
	}
	if driver.dispatchCount() != 0 {
		t.Fatalf("dispatches = %d, want 0", driver.dispatchCount())
	}
}

func TestReadBackMismatchBecomesUnknownWithoutRedispatch(t *testing.T) {
	request := testRequest(t, "mismatch")
	store := newFakeStore()
	store.seed(t, recordInState(request.Intent, StateAccepted))
	driver := &fakeDriver{ref: request.Intent.Driver}
	driver.readBackFn = func(_ context.Context, intent Intent) (Observation, error) {
		return Observation{
			State: ObservationConfirmed, OperationKey: SHA256Digest([]byte("wrong operation")),
			IntentDigest: intent.IntentDigest, EvidenceDigest: SHA256Digest([]byte("provider record")),
		}, nil
	}
	service := testService(t, store, driver)

	record, err := service.Reconcile(context.Background(), request.Intent)
	if !errors.Is(err, ErrReadBackMismatch) {
		t.Fatalf("reconcile error = %v, want ErrReadBackMismatch", err)
	}
	if record.State != StateUnknown || record.ErrorCode != CodeReadBackMismatch {
		t.Fatalf("record = %#v, want unknown read-back mismatch", record)
	}
	if driver.dispatches != 0 || driver.readBacks != 1 {
		t.Fatalf("calls dispatch=%d read-back=%d, want 0 and 1", driver.dispatches, driver.readBacks)
	}
}

func TestProviderAndStoreFailuresAreSanitized(t *testing.T) {
	t.Run("store error", func(t *testing.T) {
		request := testRequest(t, "store error")
		store := newFakeStore()
		store.ensureErr = errors.New("TOP_SECRET_STORE_DETAIL")
		service := testService(t, store, &fakeDriver{ref: request.Intent.Driver})

		_, err := service.Execute(context.Background(), request)
		assertFixedError(t, err, ErrStore, "TOP_SECRET_STORE_DETAIL")
	})

	t.Run("dispatch panic", func(t *testing.T) {
		request := testRequest(t, "dispatch panic")
		store := newFakeStore()
		driver := &fakeDriver{ref: request.Intent.Driver}
		driver.dispatchFn = func(context.Context, DispatchRequest) (Submission, error) {
			panic("TOP_SECRET_PANIC")
		}
		service := testService(t, store, driver)

		record, err := service.Execute(context.Background(), request)
		assertFixedError(t, err, ErrDriverPanic, "TOP_SECRET_PANIC")
		if record.State != StateUnknown || record.ErrorCode != CodeDriverPanic {
			t.Fatalf("record = %#v, want unknown driver panic", record)
		}
	})

	t.Run("read-back error", func(t *testing.T) {
		request := testRequest(t, "read-back error")
		store := newFakeStore()
		store.seed(t, recordInState(request.Intent, StateAccepted))
		driver := &fakeDriver{ref: request.Intent.Driver}
		driver.readBackFn = func(context.Context, Intent) (Observation, error) {
			return Observation{}, errors.New("TOP_SECRET_READ_BACK_DETAIL")
		}
		service := testService(t, store, driver)

		record, err := service.Reconcile(context.Background(), request.Intent)
		assertFixedError(t, err, ErrReadBack, "TOP_SECRET_READ_BACK_DETAIL")
		if record.State != StateUnknown || record.ErrorCode != CodeReadBackFailure {
			t.Fatalf("record = %#v, want unknown read-back failure", record)
		}
	})
}

func TestIntentIsDeterministicBoundedAndDoesNotRetainDispatchBytes(t *testing.T) {
	invocation := testInvocation(t, "same")
	driver := DriverRef{ID: "webhook", Version: "v1"}
	target := []byte("https://example.test/hook")
	payload := []byte("payload")
	first, err := NewIntent(invocation, driver, target, payload)
	if err != nil {
		t.Fatalf("first intent: %v", err)
	}
	second, err := NewIntent(invocation, driver, target, payload)
	if err != nil {
		t.Fatalf("second intent: %v", err)
	}
	if first != second {
		t.Fatalf("same input made unstable identities:\n%#v\n%#v", first, second)
	}
	target[0] = 'X'
	payload[0] = 'X'
	if first.TargetDigest != SHA256Digest([]byte("https://example.test/hook")) || first.PayloadDigest != SHA256Digest([]byte("payload")) {
		t.Fatalf("intent changed after input mutation: %#v", first)
	}
	if err := ValidateDispatchRequest(DispatchRequest{Intent: first, Target: target, Payload: payload}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("mutated request validation = %v, want ErrInvalidRequest", err)
	}
	if _, err := NewIntent(invocation, driver, []byte("target"), make([]byte, MaxPayloadBytes+1)); !errors.Is(err, ErrInvalidIntent) {
		t.Fatalf("oversized payload error = %v, want ErrInvalidIntent", err)
	}
}

func TestNilContextDoesNotReachPorts(t *testing.T) {
	request := testRequest(t, "nil context")
	store := newFakeStore()
	driver := &fakeDriver{ref: request.Intent.Driver}
	service := testService(t, store, driver)

	if _, err := service.Execute(nil, request); !errors.Is(err, ErrInvalidContext) {
		t.Fatalf("nil execute context error = %v, want ErrInvalidContext", err)
	}
	if _, err := service.Reconcile(nil, request.Intent); !errors.Is(err, ErrInvalidContext) {
		t.Fatalf("nil reconcile context error = %v, want ErrInvalidContext", err)
	}
	if len(store.operations) != 0 || driver.dispatchCount() != 0 || driver.readBackCount() != 0 {
		t.Fatalf("nil context reached a port: operations=%v dispatch=%d read-back=%d", store.operations, driver.dispatchCount(), driver.readBackCount())
	}
}

func assertFixedError(t *testing.T, got, want error, forbidden string) {
	t.Helper()
	if !errors.Is(got, want) {
		t.Fatalf("error = %v, want %v", got, want)
	}
	if strings.Contains(got.Error(), forbidden) {
		t.Fatalf("error leaks provider/store detail %q: %v", forbidden, got)
	}
}

func testService(t *testing.T, store *fakeStore, driver *fakeDriver) *Service {
	t.Helper()
	service, err := NewService(store, driver)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	return service
}

func testRequest(t *testing.T, payload string) DispatchRequest {
	t.Helper()
	target := []byte("effect target")
	body := []byte(payload)
	intent, err := NewIntent(testInvocation(t, payload), DriverRef{ID: "fake", Version: "v1"}, target, body)
	if err != nil {
		t.Fatalf("new intent: %v", err)
	}
	return DispatchRequest{Intent: intent, Target: target, Payload: body}
}

func testInvocation(t *testing.T, argument string) core.ToolInvocation {
	t.Helper()
	invocation, err := core.NewToolInvocation(
		core.RunInfo{RunID: "run-1", SessionID: "session-1", Principal: core.Principal{TenantID: "tenant-1", SubjectID: "subject-1"}},
		core.ToolCall{ID: "call-1", Name: "notify.send", Args: map[string]any{"body": argument}},
		false,
	)
	if err != nil {
		t.Fatalf("new tool invocation: %v", err)
	}
	return invocation
}

func acceptedSubmission(intent Intent) Submission {
	return Submission{OperationKey: intent.OperationKey, IntentDigest: intent.IntentDigest, ReceiptDigest: SHA256Digest([]byte("provider accepted"))}
}

func confirmedObservation(intent Intent) Observation {
	return observationFor(intent, ObservationConfirmed)
}

func observationFor(intent Intent, state ObservationState) Observation {
	observation := Observation{State: state, OperationKey: intent.OperationKey, IntentDigest: intent.IntentDigest}
	if state == ObservationConfirmed || state == ObservationRejected {
		observation.EvidenceDigest = SHA256Digest([]byte("provider read-back " + string(state)))
	}
	return observation
}

func recordInState(intent Intent, state State) Record {
	record := Record{Intent: intent, State: state, DispatchAttempts: 1}
	switch state {
	case StateAccepted:
		record.ReceiptDigest = SHA256Digest([]byte("provider accepted"))
	case StateConfirmed, StateRejected:
		record.EvidenceDigest = SHA256Digest([]byte("provider evidence"))
	case StateUnknown:
		record.ErrorCode = CodeDriverFailure
	}
	return record
}

type fakeDriver struct {
	mu         sync.Mutex
	ref        DriverRef
	dispatches int
	readBacks  int
	dispatchFn func(context.Context, DispatchRequest) (Submission, error)
	readBackFn func(context.Context, Intent) (Observation, error)
}

func (d *fakeDriver) Ref() DriverRef {
	return d.ref
}

func (d *fakeDriver) Dispatch(ctx context.Context, request DispatchRequest) (Submission, error) {
	d.mu.Lock()
	d.dispatches++
	fn := d.dispatchFn
	d.mu.Unlock()
	if fn != nil {
		return fn(ctx, request)
	}
	return acceptedSubmission(request.Intent), nil
}

func (d *fakeDriver) ReadBack(ctx context.Context, intent Intent) (Observation, error) {
	d.mu.Lock()
	d.readBacks++
	fn := d.readBackFn
	d.mu.Unlock()
	if fn != nil {
		return fn(ctx, intent)
	}
	return observationFor(intent, ObservationPending), nil
}

func (d *fakeDriver) dispatchCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.dispatches
}

func (d *fakeDriver) readBackCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.readBacks
}

type fakeStore struct {
	mu              sync.Mutex
	records         map[string]Record
	logical         map[string]string
	operations      []string
	ensureErr       error
	markAcceptedErr error
	beginGate       func()
}

type wrongEnsureStore struct {
	Store
	record Record
}

func (s wrongEnsureStore) Ensure(context.Context, Intent) (Record, error) {
	return s.record, nil
}

func newFakeStore() *fakeStore {
	return &fakeStore{records: make(map[string]Record), logical: make(map[string]string)}
}

func (s *fakeStore) Ensure(_ context.Context, intent Intent) (Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.operations = append(s.operations, "ensure")
	if s.ensureErr != nil {
		return Record{}, s.ensureErr
	}
	logicalKey := invocationKey(intent)
	if existing, ok := s.logical[logicalKey]; ok && existing != intent.IntentDigest {
		return Record{}, ErrConflict
	}
	if record, ok := s.records[intent.IntentDigest]; ok {
		return record, nil
	}
	record := Record{Intent: intent, State: StatePrepared}
	s.records[intent.IntentDigest] = record
	s.logical[logicalKey] = intent.IntentDigest
	return record, nil
}

func (s *fakeStore) Get(_ context.Context, intent Intent) (Record, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.records[intent.IntentDigest]
	return record, ok, nil
}

func (s *fakeStore) BeginDispatch(_ context.Context, intent Intent) (Record, bool, error) {
	if s.beginGate != nil {
		s.beginGate()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.operations = append(s.operations, "dispatching")
	record, ok := s.records[intent.IntentDigest]
	if !ok {
		return Record{}, false, ErrNotFound
	}
	if record.Intent != intent {
		return Record{}, false, ErrConflict
	}
	if record.State != StatePrepared {
		return record, false, nil
	}
	record.State = StateDispatching
	record.DispatchAttempts++
	s.records[intent.IntentDigest] = record
	return record, true, nil
}

func (s *fakeStore) MarkAccepted(_ context.Context, intent Intent, submission Submission) (Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.operations = append(s.operations, "accepted")
	if s.markAcceptedErr != nil {
		return Record{}, s.markAcceptedErr
	}
	record, err := s.transition(intent, StateAccepted)
	if err != nil {
		return Record{}, err
	}
	record.ReceiptDigest = submission.ReceiptDigest
	s.records[intent.IntentDigest] = record
	return record, nil
}

func (s *fakeStore) MarkConfirmed(_ context.Context, intent Intent, observation Observation) (Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.operations = append(s.operations, "confirmed")
	record, err := s.transition(intent, StateConfirmed)
	if err != nil {
		return Record{}, err
	}
	record.EvidenceDigest = observation.EvidenceDigest
	record.ErrorCode = ""
	s.records[intent.IntentDigest] = record
	return record, nil
}

func (s *fakeStore) MarkRejected(_ context.Context, intent Intent, observation Observation) (Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.operations = append(s.operations, "rejected")
	record, err := s.transition(intent, StateRejected)
	if err != nil {
		return Record{}, err
	}
	record.EvidenceDigest = observation.EvidenceDigest
	record.ErrorCode = ""
	s.records[intent.IntentDigest] = record
	return record, nil
}

func (s *fakeStore) MarkUnknown(_ context.Context, intent Intent, code string) (Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.operations = append(s.operations, "unknown")
	record, err := s.transition(intent, StateUnknown)
	if err != nil {
		return Record{}, err
	}
	record.EvidenceDigest = ""
	record.ErrorCode = code
	s.records[intent.IntentDigest] = record
	return record, nil
}

func (s *fakeStore) transition(intent Intent, next State) (Record, error) {
	record, ok := s.records[intent.IntentDigest]
	if !ok {
		return Record{}, ErrNotFound
	}
	if record.Intent != intent || !StateTransitionAllowed(record.State, next) {
		return Record{}, ErrConflict
	}
	record.State = next
	return record, nil
}

func (s *fakeStore) seed(t *testing.T, record Record) {
	t.Helper()
	if err := ValidateRecord(record); err != nil {
		t.Fatalf("seed invalid record: %v", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records[record.Intent.IntentDigest] = record
	s.logical[invocationKey(record.Intent)] = record.Intent.IntentDigest
}

func (s *fakeStore) mustRecord(t *testing.T, intent Intent) Record {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.records[intent.IntentDigest]
	if !ok {
		t.Fatalf("missing record for %s", intent.IntentDigest)
	}
	return record
}

func invocationKey(intent Intent) string {
	invocation := intent.Invocation
	return strings.Join([]string{invocation.TenantID, invocation.SubjectID, invocation.SessionID, invocation.RunID, invocation.CallID}, "\x00")
}
