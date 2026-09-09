package effectreceipt

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestDriverRegistryRegistersDynamicStrictRefs(t *testing.T) {
	registry := NewDriverRegistry()
	first := &fakeDriver{ref: DriverRef{ID: "alpha-provider", Version: "v1"}}
	second := &fakeDriver{ref: DriverRef{ID: "beta-provider", Version: "v2"}}
	if err := registry.Register(second); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(first); err != nil {
		t.Fatal(err)
	}
	refs := registry.Refs()
	if len(refs) != 2 || refs[0] != first.ref || refs[1] != second.ref {
		t.Fatalf("registered refs=%+v", refs)
	}
	if driver, found, err := registry.Lookup(first.ref); err != nil || !found || driver != first {
		t.Fatalf("lookup driver=%T found=%t err=%v", driver, found, err)
	}
	if err := registry.Register(first); !errors.Is(err, ErrConflict) {
		t.Fatalf("same driver duplicate error=%v, want ErrConflict", err)
	}
	if err := registry.Register(&fakeDriver{ref: first.ref}); !errors.Is(err, ErrConflict) {
		t.Fatalf("same ref conflict error=%v, want ErrConflict", err)
	}
	if _, found, err := registry.Lookup(DriverRef{ID: "unknown", Version: "v1"}); err != nil || found {
		t.Fatalf("unknown driver found=%t err=%v", found, err)
	}
	if err := registry.Register(panicRefDriver{}); !errors.Is(err, ErrDriverPanic) {
		t.Fatalf("panic ref registration error=%v, want ErrDriverPanic", err)
	}

	concurrent := NewDriverRegistry()
	const registrations = 24
	errs := make(chan error, registrations)
	var wg sync.WaitGroup
	for index := 0; index < registrations; index++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			errs <- concurrent.Register(&fakeDriver{ref: DriverRef{ID: fmt.Sprintf("dynamic-%02d", index), Version: "v1"}})
		}(index)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Errorf("dynamic registration: %v", err)
		}
	}
	if got := len(concurrent.Refs()); got != registrations {
		t.Fatalf("concurrent registrations=%d, want %d", got, registrations)
	}
}

func TestRecoveryCoordinatorAuthorizationAndReadBackOnly(t *testing.T) {
	t.Run("denied does not read back", func(t *testing.T) {
		intent, record, store, driver := seededRecovery(t, "recovery-denied", StateDispatching)
		reader := &scriptedRecoveryReader{records: []RecoveryRecord{record}}
		authorizer := recoveryAuthorizerFunc(func(context.Context, RecoveryRecord) (bool, error) { return false, nil })
		coordinator := newRecoveryCoordinator(t, reader, store, driver, authorizer)

		result, err := coordinator.RecoverPage(context.Background(), RecoveryQuery{Driver: intent.Driver}, RecoveryCursor{}, 1)
		if err != nil || result.Examined != 1 || result.Unauthorized != 1 || result.Reconciled != 0 || result.Next.CallID != intent.Invocation.CallID {
			t.Fatalf("denied recovery result=%+v err=%v", result, err)
		}
		if driver.readBackCount() != 0 || driver.dispatchCount() != 0 {
			t.Fatalf("denied recovery calls dispatch=%d read-back=%d", driver.dispatchCount(), driver.readBackCount())
		}
	})

	t.Run("authorized only read backs", func(t *testing.T) {
		intent, record, store, driver := seededRecovery(t, "recovery-allowed", StateDispatching)
		driver.readBackFn = func(_ context.Context, intent Intent) (Observation, error) {
			return confirmedObservation(intent), nil
		}
		reader := &scriptedRecoveryReader{records: []RecoveryRecord{record}}
		coordinator := newRecoveryCoordinator(t, reader, store, driver, recoveryAuthorizerFunc(func(context.Context, RecoveryRecord) (bool, error) {
			return true, nil
		}))

		result, err := coordinator.RecoverPage(context.Background(), RecoveryQuery{Driver: intent.Driver}, RecoveryCursor{}, 1)
		if err != nil || result.Reconciled != 1 || result.Next.CallID != intent.Invocation.CallID {
			t.Fatalf("authorized recovery result=%+v err=%v", result, err)
		}
		if driver.dispatchCount() != 0 || driver.readBackCount() != 1 {
			t.Fatalf("recovery calls dispatch=%d read-back=%d", driver.dispatchCount(), driver.readBackCount())
		}
		if current := store.mustRecord(t, intent); current.State != StateConfirmed {
			t.Fatalf("read-back did not confirm: %+v", current)
		}
	})
}

func TestRecoveryCoordinatorPagesByCallerCursor(t *testing.T) {
	firstIntent, first, store, driver := seededRecovery(t, "recovery-page-a", StateDispatching)
	secondIntent := testRequest(t, "recovery-page-b").Intent
	second := recoveryRecord(secondIntent, StateDispatching, first.UpdatedAt.Add(time.Millisecond))
	store.seed(t, second.Record)
	driver.readBackFn = func(_ context.Context, intent Intent) (Observation, error) { return confirmedObservation(intent), nil }
	reader := &scriptedRecoveryReader{}
	reader.listFn = func(_ context.Context, _ RecoveryQuery, cursor RecoveryCursor, _ int) ([]RecoveryRecord, RecoveryCursor, error) {
		if cursor.UpdatedAt.IsZero() {
			return []RecoveryRecord{first}, recoveryCursorMust(first), nil
		}
		if cursor.CallID == firstIntent.Invocation.CallID {
			return []RecoveryRecord{second}, recoveryCursorMust(second), nil
		}
		return nil, cursor, nil
	}
	coordinator := newRecoveryCoordinator(t, reader, store, driver, allowRecovery())

	pageOne, err := coordinator.RecoverPage(context.Background(), RecoveryQuery{Driver: firstIntent.Driver}, RecoveryCursor{}, 1)
	if err != nil || pageOne.Reconciled != 1 || pageOne.Next.CallID != firstIntent.Invocation.CallID {
		t.Fatalf("page one=%+v err=%v", pageOne, err)
	}
	pageTwo, err := coordinator.RecoverPage(context.Background(), RecoveryQuery{Driver: secondIntent.Driver}, pageOne.Next, 1)
	if err != nil || pageTwo.Reconciled != 1 || pageTwo.Next.CallID != secondIntent.Invocation.CallID {
		t.Fatalf("page two=%+v err=%v", pageTwo, err)
	}
	if driver.dispatchCount() != 0 || driver.readBackCount() != 2 {
		t.Fatalf("page recovery calls dispatch=%d read-back=%d", driver.dispatchCount(), driver.readBackCount())
	}
}

func TestRecoveryCoordinatorFailuresDoNotAdvanceCorrespondingCursor(t *testing.T) {
	t.Run("reader error", func(t *testing.T) {
		intent, _, store, driver := seededRecovery(t, "recovery-reader-error", StateDispatching)
		input := RecoveryCursor{}
		coordinator := newRecoveryCoordinator(t, &scriptedRecoveryReader{err: errors.New("reader secret")}, store, driver, allowRecovery())
		result, err := coordinator.RecoverPage(context.Background(), RecoveryQuery{Driver: intent.Driver}, input, 1)
		assertRecoveryError(t, err, ErrRecoveryReader, "reader secret")
		assertRecoveryCursor(t, result.Next, input)
	})

	t.Run("authorizer error after one record", func(t *testing.T) {
		firstIntent, first, store, driver := seededRecovery(t, "recovery-auth-a", StateDispatching)
		secondIntent := testRequest(t, "recovery-auth-b").Intent
		second := recoveryRecord(secondIntent, StateDispatching, first.UpdatedAt.Add(time.Millisecond))
		store.seed(t, second.Record)
		driver.readBackFn = func(_ context.Context, intent Intent) (Observation, error) { return confirmedObservation(intent), nil }
		authorizer := recoveryAuthorizerFunc(func(_ context.Context, record RecoveryRecord) (bool, error) {
			if record.Intent.Invocation.CallID == secondIntent.Invocation.CallID {
				return false, errors.New("authorizer secret")
			}
			return true, nil
		})
		coordinator := newRecoveryCoordinator(t, &scriptedRecoveryReader{records: []RecoveryRecord{first, second}}, store, driver, authorizer)
		result, err := coordinator.RecoverPage(context.Background(), RecoveryQuery{Driver: firstIntent.Driver}, RecoveryCursor{}, 2)
		assertRecoveryError(t, err, ErrRecoveryAuthorizer, "authorizer secret")
		if result.Reconciled != 1 || result.Next.CallID != firstIntent.Invocation.CallID || driver.readBackCount() != 1 {
			t.Fatalf("authorizer failure advanced past record: result=%+v readbacks=%d", result, driver.readBackCount())
		}
	})

	t.Run("driver error", func(t *testing.T) {
		intent, record, store, driver := seededRecovery(t, "recovery-driver-error", StateDispatching)
		driver.readBackFn = func(context.Context, Intent) (Observation, error) {
			return Observation{}, errors.New("provider secret")
		}
		coordinator := newRecoveryCoordinator(t, &scriptedRecoveryReader{records: []RecoveryRecord{record}}, store, driver, allowRecovery())
		result, err := coordinator.RecoverPage(context.Background(), RecoveryQuery{Driver: intent.Driver}, RecoveryCursor{}, 1)
		assertRecoveryError(t, err, ErrRecoveryDriver, "provider secret")
		assertRecoveryCursor(t, result.Next, RecoveryCursor{})
		if driver.dispatchCount() != 0 {
			t.Fatalf("driver failure dispatched=%d", driver.dispatchCount())
		}
	})

	t.Run("store error", func(t *testing.T) {
		intent, record, base, driver := seededRecovery(t, "recovery-store-error", StateDispatching)
		driver.readBackFn = func(_ context.Context, intent Intent) (Observation, error) { return confirmedObservation(intent), nil }
		store := markConfirmedStore{Store: base, err: errors.New("store secret")}
		coordinator := newRecoveryCoordinator(t, &scriptedRecoveryReader{records: []RecoveryRecord{record}}, store, driver, allowRecovery())
		result, err := coordinator.RecoverPage(context.Background(), RecoveryQuery{Driver: intent.Driver}, RecoveryCursor{}, 1)
		assertRecoveryError(t, err, ErrRecoveryStore, "store secret")
		assertRecoveryCursor(t, result.Next, RecoveryCursor{})
	})
}

func TestRecoveryCoordinatorSanitizesPanics(t *testing.T) {
	t.Run("reader panic", func(t *testing.T) {
		intent, _, store, driver := seededRecovery(t, "recovery-reader-panic", StateDispatching)
		coordinator := newRecoveryCoordinator(t, &scriptedRecoveryReader{panicValue: "reader secret"}, store, driver, allowRecovery())
		_, err := coordinator.RecoverPage(context.Background(), RecoveryQuery{Driver: intent.Driver}, RecoveryCursor{}, 1)
		assertRecoveryError(t, err, ErrRecoveryReaderPanic, "reader secret")
	})

	t.Run("authorizer panic", func(t *testing.T) {
		intent, record, store, driver := seededRecovery(t, "recovery-auth-panic", StateDispatching)
		coordinator := newRecoveryCoordinator(t, &scriptedRecoveryReader{records: []RecoveryRecord{record}}, store, driver, recoveryAuthorizerFunc(func(context.Context, RecoveryRecord) (bool, error) {
			panic("authorizer secret")
		}))
		_, err := coordinator.RecoverPage(context.Background(), RecoveryQuery{Driver: intent.Driver}, RecoveryCursor{}, 1)
		assertRecoveryError(t, err, ErrRecoveryAuthorizerPanic, "authorizer secret")
	})

	t.Run("driver panic", func(t *testing.T) {
		intent, record, store, driver := seededRecovery(t, "recovery-driver-panic", StateDispatching)
		driver.readBackFn = func(context.Context, Intent) (Observation, error) { panic("driver secret") }
		coordinator := newRecoveryCoordinator(t, &scriptedRecoveryReader{records: []RecoveryRecord{record}}, store, driver, allowRecovery())
		_, err := coordinator.RecoverPage(context.Background(), RecoveryQuery{Driver: intent.Driver}, RecoveryCursor{}, 1)
		assertRecoveryError(t, err, ErrRecoveryDriverPanic, "driver secret")
	})

	t.Run("store panic", func(t *testing.T) {
		intent, record, base, driver := seededRecovery(t, "recovery-store-panic", StateDispatching)
		driver.readBackFn = func(_ context.Context, intent Intent) (Observation, error) { return confirmedObservation(intent), nil }
		coordinator := newRecoveryCoordinator(t, &scriptedRecoveryReader{records: []RecoveryRecord{record}}, markConfirmedStore{Store: base, panicValue: "store secret"}, driver, allowRecovery())
		_, err := coordinator.RecoverPage(context.Background(), RecoveryQuery{Driver: intent.Driver}, RecoveryCursor{}, 1)
		assertRecoveryError(t, err, ErrRecoveryStorePanic, "store secret")
	})
}

func seededRecovery(t *testing.T, callID string, state State) (Intent, RecoveryRecord, *fakeStore, *fakeDriver) {
	t.Helper()
	request := testRequest(t, callID)
	request.Intent.Invocation.CallID = callID
	request.Intent.IntentDigest = ""
	request.Intent.OperationKey = ""
	// Rebuild after choosing the distinct call id so all digests remain bound.
	invocation := testInvocation(t, callID)
	invocation.CallID = callID
	intent, err := NewIntent(invocation, request.Intent.Driver, request.Target, request.Payload)
	if err != nil {
		t.Fatal(err)
	}
	store := newFakeStore()
	store.seed(t, recordInState(intent, state))
	driver := &fakeDriver{ref: intent.Driver}
	return intent, recoveryRecord(intent, state, time.Unix(1700000000, 0).UTC()), store, driver
}

func recoveryRecord(intent Intent, state State, updatedAt time.Time) RecoveryRecord {
	return RecoveryRecord{Record: recordInState(intent, state), CreatedAt: updatedAt.Add(-time.Second), UpdatedAt: updatedAt}
}

func newRecoveryCoordinator(t *testing.T, reader RecoveryReader, store Store, driver Driver, authorizer RecoveryAuthorizer) *RecoveryCoordinator {
	t.Helper()
	registry := NewDriverRegistry()
	if err := registry.Register(driver); err != nil {
		t.Fatalf("register recovery driver: %v", err)
	}
	coordinator, err := NewRecoveryCoordinator(reader, store, registry, authorizer)
	if err != nil {
		t.Fatalf("new recovery coordinator: %v", err)
	}
	return coordinator
}

func allowRecovery() RecoveryAuthorizer {
	return recoveryAuthorizerFunc(func(context.Context, RecoveryRecord) (bool, error) { return true, nil })
}

func recoveryCursorMust(record RecoveryRecord) RecoveryCursor {
	cursor, valid := recoveryCursorFromRecord(record)
	if !valid {
		panic("test recovery record has invalid cursor")
	}
	return cursor
}

func assertRecoveryError(t *testing.T, got, want error, forbidden string) {
	t.Helper()
	if !errors.Is(got, want) {
		t.Fatalf("error=%v, want %v", got, want)
	}
	if strings.Contains(got.Error(), forbidden) {
		t.Fatalf("error leaked %q: %v", forbidden, got)
	}
}

func assertRecoveryCursor(t *testing.T, got, want RecoveryCursor) {
	t.Helper()
	if got.TenantID != want.TenantID || got.SubjectID != want.SubjectID || got.SessionID != want.SessionID ||
		got.RunID != want.RunID || got.CallID != want.CallID || !got.UpdatedAt.Equal(want.UpdatedAt) {
		t.Fatalf("cursor=%+v, want %+v", got, want)
	}
}

type recoveryAuthorizerFunc func(context.Context, RecoveryRecord) (bool, error)

func (f recoveryAuthorizerFunc) AuthorizeEffectRecovery(ctx context.Context, record RecoveryRecord) (bool, error) {
	return f(ctx, record)
}

type scriptedRecoveryReader struct {
	records    []RecoveryRecord
	err        error
	panicValue any
	listFn     func(context.Context, RecoveryQuery, RecoveryCursor, int) ([]RecoveryRecord, RecoveryCursor, error)
}

func (r *scriptedRecoveryReader) ListUnresolved(ctx context.Context, query RecoveryQuery, cursor RecoveryCursor, limit int) ([]RecoveryRecord, RecoveryCursor, error) {
	if r.panicValue != nil {
		panic(r.panicValue)
	}
	if r.listFn != nil {
		return r.listFn(ctx, query, cursor, limit)
	}
	if r.err != nil {
		return nil, RecoveryCursor{}, r.err
	}
	return r.records, cursor, nil
}

type markConfirmedStore struct {
	Store
	err        error
	panicValue any
}

func (s markConfirmedStore) MarkConfirmed(context.Context, Intent, Observation) (Record, error) {
	if s.panicValue != nil {
		panic(s.panicValue)
	}
	return Record{}, s.err
}

type panicRefDriver struct{}

func (panicRefDriver) Ref() DriverRef { panic("driver ref secret") }

func (panicRefDriver) Dispatch(context.Context, DispatchRequest) (Submission, error) {
	return Submission{}, nil
}

func (panicRefDriver) ReadBack(context.Context, Intent) (Observation, error) {
	return Observation{}, nil
}
