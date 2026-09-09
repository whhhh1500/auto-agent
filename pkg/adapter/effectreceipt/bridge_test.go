package effectreceipt

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	appreceipt "github.com/whhhh1500/auto-agent/pkg/app/effectreceipt"
	"github.com/whhhh1500/auto-agent/pkg/core"
)

func TestBridgeRegistersDynamicallyAndConfirmsByReadBack(t *testing.T) {
	objects := newMemoryObjects()
	bridge := newTestBridge(t, objects, &testBinder{target: []byte("effects/test"), payload: []byte(`{"message":"hello"}`)})
	result, puts, binderCalls := runBridgeCapability(t, bridge, true)
	state, code := decodeEffectResult(t, result)
	if !result.OK || state != appreceipt.StateConfirmed || code != string(appreceipt.StateConfirmed) {
		t.Fatalf("bridge result=%#v state=%q code=%q", result, state, code)
	}
	if puts != 1 || binderCalls != 1 {
		t.Fatalf("duplicate core call crossed bridge: puts=%d binder_calls=%d", puts, binderCalls)
	}
}

func TestBridgeAcceptedDoesNotClaimConfirmation(t *testing.T) {
	objects := newMemoryObjects()
	objects.hideReads = true
	bridge := newTestBridge(t, objects, &testBinder{target: []byte("effects/test"), payload: []byte("eventual")})
	result, puts, _ := runBridgeCapability(t, bridge, false)
	state, code := decodeEffectResult(t, result)
	if result.OK || state != appreceipt.StateAccepted || code != string(appreceipt.StateAccepted) {
		t.Fatalf("accepted was represented as confirmed: result=%#v state=%q code=%q", result, state, code)
	}
	if puts != 1 {
		t.Fatalf("puts=%d, want one conditional write", puts)
	}
}

func TestObjectStoreDriverBindsIntentTargetToItsNamespace(t *testing.T) {
	objects := newMemoryObjects()
	driver, err := NewObjectStoreDriver(appreceipt.DriverRef{ID: "object.effect", Version: "v1"}, "effects/test", objects)
	if err != nil {
		t.Fatal(err)
	}
	request := testDispatchRequest(t, driver.Ref(), "target-call", []byte("payload"))
	wrongIntent, err := appreceipt.NewIntent(request.Intent.Invocation, driver.Ref(), []byte("other-namespace"), request.Payload)
	if err != nil {
		t.Fatal(err)
	}
	wrongRequest := appreceipt.DispatchRequest{Intent: wrongIntent, Target: []byte("other-namespace"), Payload: request.Payload}
	if _, err := driver.Dispatch(context.Background(), wrongRequest); !errors.Is(err, ErrObjectRequest) {
		t.Fatalf("mismatched target dispatch error=%v", err)
	}
	if _, err := driver.ReadBack(context.Background(), wrongIntent); !errors.Is(err, ErrObjectRequest) {
		t.Fatalf("mismatched target read-back error=%v", err)
	}
	if objects.putCount() != 0 {
		t.Fatalf("wrong durable target reached object store: puts=%d", objects.putCount())
	}

	bridge := newTestBridge(t, objects, &testBinder{target: []byte("other-namespace"), payload: []byte("payload")})
	result, puts, _ := runBridgeCapability(t, bridge, false)
	state, code := decodeEffectResult(t, result)
	if result.OK || state != appreceipt.StateUnknown || code != CodeBindingFailed || puts != 0 {
		t.Fatalf("bridge accepted an unbound target: result=%#v puts=%d", result, puts)
	}
}

func TestBridgeRequiresAcceptedInvocation(t *testing.T) {
	objects := newMemoryObjects()
	binder := &testBinder{target: []byte("effects/test"), payload: []byte("payload")}
	bridge := newTestBridge(t, objects, binder)
	result, err := bridge.Execute(context.Background(), core.CapabilityRequest{})
	if err != nil {
		t.Fatal(err)
	}
	state, code := decodeEffectResult(t, result)
	if result.OK || state != appreceipt.StateUnknown || code != CodeAcceptedInvocationRequired || binder.calls.Load() != 0 || objects.putCount() != 0 {
		t.Fatalf("unprotected call reached effect path: result=%#v state=%q code=%q binder=%d puts=%d", result, state, code, binder.calls.Load(), objects.putCount())
	}
}

func TestBridgeClassifiesDispatchAdmissionFailureWithoutCallingProvider(t *testing.T) {
	for _, test := range []struct {
		name     string
		admitter appreceipt.DispatchAdmitter
	}{
		{name: "error", admitter: bridgeDispatchAdmitterFunc(func(context.Context, appreceipt.Intent) (appreceipt.Record, bool, error) {
			return appreceipt.Record{}, false, appreceipt.ErrDispatchAdmission
		})},
		{name: "panic", admitter: bridgeDispatchAdmitterFunc(func(context.Context, appreceipt.Intent) (appreceipt.Record, bool, error) {
			panic("admission internals must remain private")
		})},
	} {
		t.Run(test.name, func(t *testing.T) {
			objects := newMemoryObjects()
			bridge := newTestBridge(t, objects, &testBinder{target: []byte("effects/test"), payload: []byte("payload")})
			ctx := appreceipt.WithDispatchAdmitter(context.Background(), test.admitter)
			result, puts, _ := runBridgeCapabilityContext(t, ctx, bridge, false)
			state, code := decodeEffectResult(t, result)
			if result.OK || state != appreceipt.StateUnknown || code != CodeDispatchAdmissionFailed || puts != 0 {
				t.Fatalf("admission failure result=%#v puts=%d", result, puts)
			}
		})
	}
}

func TestBridgeArtifactRevisionBindsManifestAndDriverWithoutLeakingThem(t *testing.T) {
	objects := newMemoryObjects()
	manifest := core.CapabilityManifest{
		ID: "effect.deliver", Version: "v1", Name: "Dynamic external effect", Kind: core.KindTool, Idempotent: true,
		Tool: &core.ToolExposure{Parameters: map[string]any{"type": "object"}},
	}
	firstDriver, err := NewObjectStoreDriver(appreceipt.DriverRef{ID: "provider.alpha", Version: "v1"}, "effects/test", objects)
	if err != nil {
		t.Fatal(err)
	}
	secondDriver, err := NewObjectStoreDriver(appreceipt.DriverRef{ID: "provider.beta", Version: "v1"}, "effects/test", objects)
	if err != nil {
		t.Fatal(err)
	}
	first, err := NewBridge(manifest, newMemoryEffectStore(), firstDriver, &testBinder{target: []byte("effects/test"), payload: []byte("payload")})
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewBridge(manifest, newMemoryEffectStore(), secondDriver, &testBinder{target: []byte("effects/test"), payload: []byte("payload")})
	if err != nil {
		t.Fatal(err)
	}
	if first.ArtifactRevision() == "" || first.ArtifactRevision() == second.ArtifactRevision() || strings.Contains(first.ArtifactRevision(), manifest.ID) || strings.Contains(first.ArtifactRevision(), firstDriver.Ref().ID) {
		t.Fatalf("unexpected opaque revisions first=%q second=%q", first.ArtifactRevision(), second.ArtifactRevision())
	}
	manifest.Tool.Parameters["type"] = "string"
	if first.Manifest().Tool.Parameters["type"] != "object" {
		t.Fatal("bridge retained caller-owned mutable manifest data")
	}
}

func TestBridgeSanitizesBinderAndObjectStoreFailures(t *testing.T) {
	t.Run("binder_error", func(t *testing.T) {
		secret := "binder-secret://not-for-result"
		objects := newMemoryObjects()
		bridge := newTestBridge(t, objects, &testBinder{err: errors.New(secret)})
		result, _, _ := runBridgeCapability(t, bridge, false)
		state, code := decodeEffectResult(t, result)
		if result.OK || state != appreceipt.StateUnknown || code != CodeBindingFailed || strings.Contains(result.Content, secret) || strings.Contains(result.Metadata["code"].(string), secret) {
			t.Fatalf("binder failure leaked: result=%#v", result)
		}
	})
	t.Run("binder_panic", func(t *testing.T) {
		secret := "binder-panic://not-for-result"
		objects := newMemoryObjects()
		bridge := newTestBridge(t, objects, &testBinder{panic: secret})
		result, _, _ := runBridgeCapability(t, bridge, false)
		state, code := decodeEffectResult(t, result)
		if result.OK || state != appreceipt.StateUnknown || code != CodeBindingFailed || strings.Contains(result.Content, secret) || strings.Contains(result.Metadata["code"].(string), secret) {
			t.Fatalf("binder panic leaked: result=%#v", result)
		}
	})
	t.Run("object_store_panic", func(t *testing.T) {
		secret := "object-store-secret://not-for-result"
		objects := newMemoryObjects()
		objects.putPanic = secret
		bridge := newTestBridge(t, objects, &testBinder{target: []byte("effects/test"), payload: []byte("payload")})
		result, _, _ := runBridgeCapability(t, bridge, false)
		state, code := decodeEffectResult(t, result)
		if result.OK || state != appreceipt.StateUnknown || code != appreceipt.CodeDriverFailure || strings.Contains(result.Content, secret) || strings.Contains(result.Metadata["code"].(string), secret) {
			t.Fatalf("object store panic leaked: result=%#v", result)
		}
	})
}

func TestObjectStoreDriverReadBackAndRecoveryNeverRedispatches(t *testing.T) {
	objects := newMemoryObjects()
	driver, err := NewObjectStoreDriver(appreceipt.DriverRef{ID: "object.effect", Version: "v1"}, "effects/test", objects)
	if err != nil {
		t.Fatal(err)
	}
	request := testDispatchRequest(t, driver.Ref(), "recover-call", []byte("payload-one"))
	store := newMemoryEffectStore()
	service, err := appreceipt.NewService(store, driver)
	if err != nil {
		t.Fatal(err)
	}
	first, err := service.Execute(context.Background(), request)
	if err != nil || first.State != appreceipt.StateAccepted {
		t.Fatalf("first result=%#v err=%v", first, err)
	}
	if objects.putCount() != 1 {
		t.Fatalf("first dispatch puts=%d", objects.putCount())
	}
	second, err := service.Execute(context.Background(), request)
	if err != nil || second.State != appreceipt.StateConfirmed {
		t.Fatalf("recovered result=%#v err=%v", second, err)
	}
	if objects.putCount() != 1 {
		t.Fatalf("recovery repeated dispatch: puts=%d", objects.putCount())
	}

	objects.replace(driver.key(request.Intent.OperationKey), []byte("different-payload"))
	observation, err := driver.ReadBack(context.Background(), request.Intent)
	if err != nil || observation.State != appreceipt.ObservationRejected || observation.EvidenceDigest == "" {
		t.Fatalf("mismatch observation=%#v err=%v", observation, err)
	}
	objects.remove(driver.key(request.Intent.OperationKey))
	observation, err = driver.ReadBack(context.Background(), request.Intent)
	if err != nil || observation.State != appreceipt.ObservationPending || observation.EvidenceDigest != "" {
		t.Fatalf("missing observation=%#v err=%v", observation, err)
	}
}

func TestObjectStoreDriverSanitizesErrorsAndPanics(t *testing.T) {
	secret := "provider-response=secret"
	objects := newMemoryObjects()
	driver, err := NewObjectStoreDriver(appreceipt.DriverRef{ID: "object.effect", Version: "v1"}, "effects/test", objects)
	if err != nil {
		t.Fatal(err)
	}
	request := testDispatchRequest(t, driver.Ref(), "error-call", []byte("payload"))
	objects.putErr = errors.New(secret)
	if _, err := driver.Dispatch(context.Background(), request); !errors.Is(err, ErrObjectStore) || strings.Contains(err.Error(), secret) {
		t.Fatalf("store error was not fixed: %v", err)
	}
	objects.putErr = nil
	objects.putPanic = secret
	if _, err := driver.Dispatch(context.Background(), request); !errors.Is(err, ErrObjectPanic) || strings.Contains(err.Error(), secret) {
		t.Fatalf("store panic was not fixed: %v", err)
	}
	objects.putPanic = nil
	if _, err := driver.Dispatch(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	objects.getErr = errors.New(secret)
	if _, err := driver.ReadBack(context.Background(), request.Intent); !errors.Is(err, ErrObjectStore) || strings.Contains(err.Error(), secret) {
		t.Fatalf("read-back error was not fixed: %v", err)
	}
	objects.getErr = nil
	objects.getPanic = secret
	if _, err := driver.ReadBack(context.Background(), request.Intent); !errors.Is(err, ErrObjectPanic) || strings.Contains(err.Error(), secret) {
		t.Fatalf("read-back panic was not fixed: %v", err)
	}
}

func newTestBridge(t *testing.T, objects *memoryObjects, binder Binder) *Bridge {
	t.Helper()
	driver, err := NewObjectStoreDriver(appreceipt.DriverRef{ID: "object.effect", Version: "v1"}, "effects/test", objects)
	if err != nil {
		t.Fatal(err)
	}
	bridge, err := NewBridge(core.CapabilityManifest{
		ID: "effect.deliver", Version: "v1", Name: "Dynamic external effect", Kind: core.KindTool, Idempotent: true,
		Tool: &core.ToolExposure{Parameters: map[string]any{"type": "object", "additionalProperties": false}},
	}, newMemoryEffectStore(), driver, binder)
	if err != nil {
		t.Fatal(err)
	}
	return bridge
}

func runBridgeCapability(t *testing.T, bridge *Bridge, duplicate bool) (core.CapabilityResult, int, int32) {
	return runBridgeCapabilityContext(t, context.Background(), bridge, duplicate)
}

func runBridgeCapabilityContext(t *testing.T, ctx context.Context, bridge *Bridge, duplicate bool) (core.CapabilityResult, int, int32) {
	t.Helper()
	registry := core.NewCapabilityRegistry()
	product := core.MustScopePath(
		core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"},
		core.ScopeRef{Kind: core.ScopeProduct, ID: "effects"},
	)
	user := core.MustScopePath(
		core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"},
		core.ScopeRef{Kind: core.ScopeProduct, ID: "effects"},
		core.ScopeRef{Kind: core.ScopeUser, ID: "user"},
	)
	principal := core.Principal{TenantID: "tenant", SubjectID: "subject", Scope: user, Grants: core.NewPermissionSet(core.PermRead)}
	if err := registry.Register(product, bridge); err != nil {
		t.Fatalf("dynamic registration failed: %v", err)
	}
	sessionScope := core.MustScopePath(
		core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"},
		core.ScopeRef{Kind: core.ScopeProduct, ID: "effects"},
		core.ScopeRef{Kind: core.ScopeUser, ID: "user"},
		core.ScopeRef{Kind: core.ScopeSession, ID: "effect-session"},
	)
	snapshot, err := (core.CapabilityResolver{Registry: registry}).Resolve(principal, sessionScope)
	if err != nil {
		t.Fatal(err)
	}
	session, err := core.NewSession(core.SessionOptions{ID: "effect-session", ProfileID: "effect-profile", Principal: principal, Scope: sessionScope})
	if err != nil {
		t.Fatal(err)
	}
	model := &effectModel{duplicate: duplicate}
	agent, err := core.NewAgent(core.AgentOptions{LLM: model, Tools: snapshot, Session: session, ToolJournal: newEffectJournal(), MaxSteps: 5, MaxToolCalls: 5})
	if err != nil {
		t.Fatal(err)
	}
	turn, err := agent.RunTurn(ctx, core.TurnInput{RunID: "effect-run", Text: "perform effect"})
	if err != nil || turn.Status != core.RunCompleted {
		t.Fatalf("run=%#v err=%v", turn, err)
	}
	result, found, err := session.ToolResult("effect-run", "effect-call")
	if err != nil || !found {
		t.Fatalf("tool result found=%t err=%v", found, err)
	}
	objects, ok := bridge.driver.(*ObjectStoreDriver)
	if !ok {
		t.Fatal("test bridge driver is not an ObjectStoreDriver")
	}
	memory, ok := objects.objects.(*memoryObjects)
	if !ok {
		t.Fatal("test object store type")
	}
	binder, _ := bridge.binder.(*testBinder)
	if binder == nil {
		t.Fatal("test binder type")
	}
	return result, memory.putCount(), binder.calls.Load()
}

type bridgeDispatchAdmitterFunc func(context.Context, appreceipt.Intent) (appreceipt.Record, bool, error)

func (f bridgeDispatchAdmitterFunc) BeginDispatch(ctx context.Context, intent appreceipt.Intent) (appreceipt.Record, bool, error) {
	return f(ctx, intent)
}

func decodeEffectResult(t *testing.T, result core.CapabilityResult) (appreceipt.State, string) {
	t.Helper()
	var wire struct {
		State appreceipt.State `json:"state"`
		Code  string           `json:"code"`
	}
	if err := json.Unmarshal([]byte(result.Content), &wire); err != nil {
		t.Fatalf("invalid fixed result %q: %v", result.Content, err)
	}
	if len(result.Metadata) != 2 || result.Metadata["state"] != string(wire.State) || result.Metadata["code"] != wire.Code {
		t.Fatalf("unexpected result metadata=%#v for %#v", result.Metadata, wire)
	}
	return wire.State, wire.Code
}

type testBinder struct {
	target  []byte
	payload []byte
	err     error
	panic   any
	calls   atomic.Int32
}

func (b *testBinder) Bind(_ context.Context, _ core.CapabilityRequest) (Binding, error) {
	b.calls.Add(1)
	if b.panic != nil {
		panic(b.panic)
	}
	if b.err != nil {
		return Binding{}, b.err
	}
	return Binding{Target: append([]byte(nil), b.target...), Payload: append([]byte(nil), b.payload...)}, nil
}

type memoryObjects struct {
	mu        sync.Mutex
	values    map[string][]byte
	puts      int
	hideReads bool
	putErr    error
	getErr    error
	putPanic  any
	getPanic  any
}

func newMemoryObjects() *memoryObjects { return &memoryObjects{values: map[string][]byte{}} }

func (s *memoryObjects) PutIfAbsent(_ context.Context, key string, data []byte) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.puts++
	if s.putPanic != nil {
		panic(s.putPanic)
	}
	if s.putErr != nil {
		return false, s.putErr
	}
	if _, found := s.values[key]; found {
		return false, nil
	}
	s.values[key] = append([]byte(nil), data...)
	return true, nil
}

func (s *memoryObjects) Get(_ context.Context, key string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.getPanic != nil {
		panic(s.getPanic)
	}
	if s.getErr != nil {
		return nil, s.getErr
	}
	if s.hideReads {
		return nil, ErrObjectNotFound
	}
	value, found := s.values[key]
	if !found {
		return nil, ErrObjectNotFound
	}
	return append([]byte(nil), value...), nil
}

func (s *memoryObjects) putCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.puts
}

func (s *memoryObjects) replace(key string, data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.values[key] = append([]byte(nil), data...)
}

func (s *memoryObjects) remove(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.values, key)
}

func testDispatchRequest(t *testing.T, ref appreceipt.DriverRef, callID string, payload []byte) appreceipt.DispatchRequest {
	t.Helper()
	invocation, err := core.NewToolInvocation(core.RunInfo{
		RunID: "effect-run", SessionID: "effect-session",
		Principal: core.Principal{TenantID: "tenant", SubjectID: "subject"},
	}, core.ToolCall{ID: callID, Name: "effect.deliver", Args: map[string]any{"call": callID}}, true)
	if err != nil {
		t.Fatal(err)
	}
	target := []byte("effects/test")
	intent, err := appreceipt.NewIntent(invocation, ref, target, payload)
	if err != nil {
		t.Fatal(err)
	}
	return appreceipt.DispatchRequest{Intent: intent, Target: target, Payload: append([]byte(nil), payload...)}
}

type memoryEffectStore struct {
	mu      sync.Mutex
	records map[string]appreceipt.Record
}

func newMemoryEffectStore() *memoryEffectStore {
	return &memoryEffectStore{records: map[string]appreceipt.Record{}}
}

func (s *memoryEffectStore) Ensure(_ context.Context, intent appreceipt.Intent) (appreceipt.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := effectRecordKey(intent)
	if record, found := s.records[key]; found {
		if record.Intent != intent {
			return appreceipt.Record{}, appreceipt.ErrConflict
		}
		return record, nil
	}
	record := appreceipt.Record{Intent: intent, State: appreceipt.StatePrepared}
	s.records[key] = record
	return record, nil
}

func (s *memoryEffectStore) Get(_ context.Context, intent appreceipt.Intent) (appreceipt.Record, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, found := s.records[effectRecordKey(intent)]
	if found && record.Intent != intent {
		return appreceipt.Record{}, false, appreceipt.ErrConflict
	}
	return record, found, nil
}

func (s *memoryEffectStore) BeginDispatch(_ context.Context, intent appreceipt.Intent) (appreceipt.Record, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := effectRecordKey(intent)
	record, found := s.records[key]
	if !found {
		return appreceipt.Record{}, false, appreceipt.ErrNotFound
	}
	if record.Intent != intent {
		return appreceipt.Record{}, false, appreceipt.ErrConflict
	}
	if record.State != appreceipt.StatePrepared {
		return record, false, nil
	}
	record.State, record.DispatchAttempts = appreceipt.StateDispatching, record.DispatchAttempts+1
	s.records[key] = record
	return record, true, nil
}

func (s *memoryEffectStore) MarkAccepted(_ context.Context, intent appreceipt.Intent, submission appreceipt.Submission) (appreceipt.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.markAcceptedLocked(intent, submission)
}

func (s *memoryEffectStore) markAcceptedLocked(intent appreceipt.Intent, submission appreceipt.Submission) (appreceipt.Record, error) {
	record, ok := s.records[effectRecordKey(intent)]
	if !ok {
		return appreceipt.Record{}, appreceipt.ErrNotFound
	}
	if record.Intent != intent || submission.OperationKey != intent.OperationKey || submission.IntentDigest != intent.IntentDigest {
		return appreceipt.Record{}, appreceipt.ErrConflict
	}
	if record.State == appreceipt.StateAccepted && record.ReceiptDigest == submission.ReceiptDigest {
		return record, nil
	}
	if record.State != appreceipt.StateDispatching {
		return appreceipt.Record{}, appreceipt.ErrConflict
	}
	record.State, record.ReceiptDigest = appreceipt.StateAccepted, submission.ReceiptDigest
	s.records[effectRecordKey(intent)] = record
	return record, nil
}

func (s *memoryEffectStore) MarkConfirmed(_ context.Context, intent appreceipt.Intent, observation appreceipt.Observation) (appreceipt.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.markObservationLocked(intent, observation, appreceipt.StateConfirmed)
}

func (s *memoryEffectStore) MarkRejected(_ context.Context, intent appreceipt.Intent, observation appreceipt.Observation) (appreceipt.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.markObservationLocked(intent, observation, appreceipt.StateRejected)
}

func (s *memoryEffectStore) markObservationLocked(intent appreceipt.Intent, observation appreceipt.Observation, state appreceipt.State) (appreceipt.Record, error) {
	record, ok := s.records[effectRecordKey(intent)]
	if !ok {
		return appreceipt.Record{}, appreceipt.ErrNotFound
	}
	if record.Intent != intent || observation.OperationKey != intent.OperationKey || observation.IntentDigest != intent.IntentDigest {
		return appreceipt.Record{}, appreceipt.ErrConflict
	}
	if record.State == state && record.EvidenceDigest == observation.EvidenceDigest {
		return record, nil
	}
	if !appreceipt.StateTransitionAllowed(record.State, state) {
		return appreceipt.Record{}, appreceipt.ErrConflict
	}
	record.State, record.EvidenceDigest, record.ErrorCode = state, observation.EvidenceDigest, ""
	s.records[effectRecordKey(intent)] = record
	return record, nil
}

func (s *memoryEffectStore) MarkUnknown(_ context.Context, intent appreceipt.Intent, code string) (appreceipt.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.records[effectRecordKey(intent)]
	if !ok {
		return appreceipt.Record{}, appreceipt.ErrNotFound
	}
	if record.Intent != intent {
		return appreceipt.Record{}, appreceipt.ErrConflict
	}
	if record.State == appreceipt.StateUnknown && record.ErrorCode == code {
		return record, nil
	}
	if !appreceipt.StateTransitionAllowed(record.State, appreceipt.StateUnknown) {
		return appreceipt.Record{}, appreceipt.ErrConflict
	}
	record.State, record.ErrorCode, record.EvidenceDigest = appreceipt.StateUnknown, code, ""
	s.records[effectRecordKey(intent)] = record
	return record, nil
}

func effectRecordKey(intent appreceipt.Intent) string {
	invocation := intent.Invocation
	return invocation.TenantID + "\x00" + invocation.SubjectID + "\x00" + invocation.SessionID + "\x00" + invocation.RunID + "\x00" + invocation.CallID
}

type effectModel struct {
	duplicate bool
	steps     atomic.Int32
}

func (m *effectModel) Provider() string { return "effectreceipt-test" }

func (m *effectModel) Stream(_ context.Context, _ core.GenerateOptions, emit func(core.StreamChunk)) error {
	step := m.steps.Add(1)
	if step == 1 || (step == 2 && m.duplicate) {
		call := core.ToolCall{ID: "effect-call", Name: "effect.deliver", Args: map[string]any{}}
		emit(core.StreamChunk{Kind: core.StreamKindAssistant, ToolCall: &call})
		emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishToolCalls})
		return nil
	}
	emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: "done"})
	emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop})
	return nil
}

type effectJournal struct {
	mu      sync.Mutex
	records map[string]core.ToolInvocationRecord
}

func newEffectJournal() *effectJournal {
	return &effectJournal{records: map[string]core.ToolInvocationRecord{}}
}

func (j *effectJournal) BeginToolInvocation(_ context.Context, invocation core.ToolInvocation) (core.ToolInvocationRecord, core.ToolInvocationDecision, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	key := effectJournalKey(invocation)
	if record, exists := j.records[key]; exists {
		if record.ToolInvocation != invocation {
			return record, core.ToolInvocationConflict, nil
		}
		if record.State == core.ToolInvocationCompleted {
			return record, core.ToolInvocationReplay, nil
		}
		return record, core.ToolInvocationExecuteRetry, nil
	}
	now := time.Now().UTC()
	record := core.ToolInvocationRecord{ToolInvocation: invocation, State: core.ToolInvocationStarted, StartedAt: now, UpdatedAt: now}
	j.records[key] = record
	return record, core.ToolInvocationExecuteNew, nil
}

func (j *effectJournal) CompleteToolInvocation(_ context.Context, invocation core.ToolInvocation, result core.CapabilityResult) (core.ToolInvocationRecord, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	key := effectJournalKey(invocation)
	record, found := j.records[key]
	if !found || record.ToolInvocation != invocation {
		return core.ToolInvocationRecord{}, appreceipt.ErrConflict
	}
	copyResult := result
	now := time.Now().UTC()
	record.State, record.Result, record.UpdatedAt, record.CompletedAt = core.ToolInvocationCompleted, &copyResult, now, now
	j.records[key] = record
	return record, nil
}

func (j *effectJournal) MarkToolInvocationUncertain(_ context.Context, invocation core.ToolInvocation, code string) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	key := effectJournalKey(invocation)
	record, found := j.records[key]
	if !found || record.ToolInvocation != invocation {
		return appreceipt.ErrConflict
	}
	record.State, record.ErrorCode, record.UpdatedAt = core.ToolInvocationUncertain, code, time.Now().UTC()
	j.records[key] = record
	return nil
}

func effectJournalKey(invocation core.ToolInvocation) string {
	return invocation.SessionID + "\x00" + invocation.RunID + "\x00" + invocation.CallID
}
