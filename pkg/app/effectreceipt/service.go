package effectreceipt

import (
	"context"
	"errors"
)

// Service coordinates the durable evidence state machine. It never calls
// Dispatch before BeginDispatch is durable, and it never calls Dispatch for a
// record that has reached dispatching, accepted, or unknown.
type Service struct {
	store  Store
	driver Driver
}

// NewService binds the two provider-neutral ports used by the state machine.
func NewService(store Store, driver Driver) (*Service, error) {
	if store == nil || driver == nil {
		return nil, ErrInvalidService
	}
	return &Service{store: store, driver: driver}, nil
}

// Execute first durably ensures the immutable prepared intent, then dispatches
// once only from prepared. A context DispatchAdmitter owns only the atomic
// prepared-to-dispatching transition; it never needs to create an intent.
// Existing in-flight records are reconciled by read-back.
func (s *Service) Execute(ctx context.Context, request DispatchRequest) (Record, error) {
	if ctx == nil {
		return Record{}, ErrInvalidContext
	}
	if s == nil || ValidateDispatchRequest(request) != nil {
		return Record{}, ErrInvalidRequest
	}
	if err := s.validateDriver(request.Intent); err != nil {
		return Record{}, err
	}
	record, err := s.storeRecord(func() (Record, error) {
		return s.store.Ensure(ctx, request.Intent)
	})
	if err != nil {
		return Record{}, err
	}
	if recordForIntent(record, request.Intent) != nil {
		return Record{}, ErrStore
	}
	switch record.State {
	case StatePrepared:
		return s.dispatch(ctx, cloneDispatchRequest(request))
	case StateDispatching, StateAccepted, StateUnknown:
		return s.reconcileRecord(ctx, record)
	case StateConfirmed, StateRejected:
		return record, nil
	default:
		return Record{}, ErrInvalidState
	}
}

// Reconcile performs recovery without creating a record and without dispatch.
// Callers use it after a process crash or whenever a non-terminal record is
// found by their durable scheduler.
func (s *Service) Reconcile(ctx context.Context, intent Intent) (Record, error) {
	if ctx == nil {
		return Record{}, ErrInvalidContext
	}
	if s == nil || ValidateIntent(intent) != nil {
		return Record{}, ErrInvalidIntent
	}
	record, found, err := s.storeGet(ctx, intent)
	if err != nil {
		return Record{}, err
	}
	if !found {
		return Record{}, ErrNotFound
	}
	switch record.State {
	case StatePrepared, StateConfirmed, StateRejected:
		return record, nil
	case StateDispatching, StateAccepted, StateUnknown:
		if err := s.validateDriver(intent); err != nil {
			return Record{}, err
		}
		return s.reconcileRecord(ctx, record)
	default:
		return Record{}, ErrInvalidState
	}
}

func (s *Service) dispatch(ctx context.Context, request DispatchRequest) (Record, error) {
	intent := request.Intent
	record, begun, err := s.beginDispatch(ctx, intent)
	if err != nil {
		return Record{}, err
	}
	if !begun {
		switch record.State {
		case StateDispatching, StateAccepted, StateUnknown:
			return s.reconcileRecord(ctx, record)
		case StateConfirmed, StateRejected:
			return record, nil
		default:
			return Record{}, ErrStore
		}
	}
	if record.State != StateDispatching {
		return Record{}, ErrStore
	}

	submission, dispatchErr := s.dispatchDriver(ctx, request)
	if dispatchErr != nil {
		return s.unknownAfterFailure(ctx, intent, codeForDispatchError(dispatchErr), dispatchErr)
	}
	if err := validateSubmission(intent, submission); err != nil {
		return s.unknownAfterFailure(ctx, intent, CodeDriverInvalidSubmission, err)
	}
	record, err = s.storeRecord(func() (Record, error) {
		return s.store.MarkAccepted(ctx, intent, submission)
	})
	if err != nil {
		// The provider may have accepted the effect, but only durable
		// dispatching is known. A later Execute/Reconcile performs read-back.
		return Record{}, err
	}
	if recordForIntent(record, intent) != nil || record.State != StateAccepted || record.ReceiptDigest != submission.ReceiptDigest {
		return Record{}, ErrStore
	}
	return record, nil
}

func (s *Service) beginDispatch(ctx context.Context, intent Intent) (record Record, begun bool, err error) {
	if admitter, ok := dispatchAdmitterFromContext(ctx); ok {
		return s.admitDispatch(ctx, admitter, intent)
	}
	defer func() {
		if recover() != nil {
			record = Record{}
			begun = false
			err = ErrStorePanic
		}
	}()
	record, begun, err = s.store.BeginDispatch(ctx, intent)
	if err != nil {
		return Record{}, false, fixedError(err, ErrStore)
	}
	if recordForIntent(record, intent) != nil {
		return Record{}, false, ErrStore
	}
	return record, begun, nil
}

func (s *Service) admitDispatch(ctx context.Context, admitter DispatchAdmitter, intent Intent) (record Record, begun bool, err error) {
	defer func() {
		if recover() != nil {
			record = Record{}
			begun = false
			err = ErrDispatchAdmissionPanic
		}
	}()
	record, begun, err = admitter.BeginDispatch(ctx, intent)
	if err != nil {
		return Record{}, false, fixedAdmissionError(err)
	}
	if recordForIntent(record, intent) != nil {
		return Record{}, false, ErrDispatchAdmission
	}
	return record, begun, nil
}

func fixedAdmissionError(err error) error {
	if errors.Is(err, ErrConflict) {
		return ErrConflict
	}
	if errors.Is(err, ErrNotFound) {
		return ErrNotFound
	}
	return ErrDispatchAdmission
}

func (s *Service) reconcileRecord(ctx context.Context, record Record) (Record, error) {
	intent := record.Intent
	observation, readBackErr := s.readBackDriver(ctx, intent)
	if readBackErr != nil {
		return s.unknownAfterFailure(ctx, intent, codeForReadBackError(readBackErr), readBackErr)
	}
	if err := validateObservation(intent, observation); err != nil {
		code := CodeReadBackInvalid
		if errors.Is(err, ErrReadBackMismatch) {
			code = CodeReadBackMismatch
		}
		return s.unknownAfterFailure(ctx, intent, code, err)
	}
	switch observation.State {
	case ObservationConfirmed:
		return s.markObservation(ctx, intent, observation, StateConfirmed)
	case ObservationRejected:
		return s.markObservation(ctx, intent, observation, StateRejected)
	case ObservationPending:
		if record.State == StateAccepted {
			return record, nil
		}
		return s.markUnknown(ctx, intent, CodeReadBackPending)
	case ObservationUnknown:
		if record.State == StateUnknown {
			return record, nil
		}
		return s.markUnknown(ctx, intent, CodeReadBackUnknown)
	default:
		return Record{}, ErrInvalidObservation
	}
}

func (s *Service) markObservation(ctx context.Context, intent Intent, observation Observation, state State) (Record, error) {
	var (
		record Record
		err    error
	)
	switch state {
	case StateConfirmed:
		record, err = s.storeRecord(func() (Record, error) { return s.store.MarkConfirmed(ctx, intent, observation) })
	case StateRejected:
		record, err = s.storeRecord(func() (Record, error) { return s.store.MarkRejected(ctx, intent, observation) })
	default:
		return Record{}, ErrInvalidState
	}
	if err != nil {
		return Record{}, err
	}
	if recordForIntent(record, intent) != nil || record.State != state || record.EvidenceDigest != observation.EvidenceDigest {
		return Record{}, ErrStore
	}
	return record, nil
}

func (s *Service) unknownAfterFailure(ctx context.Context, intent Intent, code string, cause error) (Record, error) {
	record, markErr := s.markUnknown(ctx, intent, code)
	if markErr != nil {
		return Record{}, markErr
	}
	return record, cause
}

func (s *Service) markUnknown(ctx context.Context, intent Intent, code string) (Record, error) {
	record, err := s.storeRecord(func() (Record, error) {
		return s.store.MarkUnknown(ctx, intent, code)
	})
	if err != nil {
		return Record{}, err
	}
	if recordForIntent(record, intent) != nil || record.State != StateUnknown || record.ErrorCode != code {
		return Record{}, ErrStore
	}
	return record, nil
}

func (s *Service) validateDriver(intent Intent) error {
	ref, err := s.driverRef()
	if err != nil || ref != intent.Driver {
		if err != nil {
			return err
		}
		return ErrDriver
	}
	return nil
}

func (s *Service) driverRef() (ref DriverRef, err error) {
	defer func() {
		if recover() != nil {
			err = ErrDriverPanic
		}
	}()
	ref = s.driver.Ref()
	if !validDriverRef(ref) {
		return DriverRef{}, ErrDriver
	}
	return ref, nil
}

func (s *Service) dispatchDriver(ctx context.Context, request DispatchRequest) (submission Submission, err error) {
	defer func() {
		if recover() != nil {
			err = ErrDriverPanic
		}
	}()
	submission, err = s.driver.Dispatch(ctx, request)
	if err != nil {
		return Submission{}, ErrDriver
	}
	return submission, nil
}

func (s *Service) readBackDriver(ctx context.Context, intent Intent) (observation Observation, err error) {
	defer func() {
		if recover() != nil {
			err = ErrReadBackPanic
		}
	}()
	observation, err = s.driver.ReadBack(ctx, intent)
	if err != nil {
		return Observation{}, ErrReadBack
	}
	return observation, nil
}

func (s *Service) storeRecord(call func() (Record, error)) (record Record, err error) {
	defer func() {
		if recover() != nil {
			err = ErrStorePanic
		}
	}()
	record, err = call()
	if err != nil {
		return Record{}, fixedError(err, ErrStore)
	}
	if ValidateRecord(record) != nil {
		return Record{}, ErrStore
	}
	return record, nil
}

func (s *Service) storeGet(ctx context.Context, intent Intent) (record Record, found bool, err error) {
	defer func() {
		if recover() != nil {
			err = ErrStorePanic
		}
	}()
	record, found, err = s.store.Get(ctx, intent)
	if err != nil {
		return Record{}, false, fixedError(err, ErrStore)
	}
	if found && ValidateRecord(record) != nil {
		return Record{}, false, ErrStore
	}
	if found && record.Intent != intent {
		return Record{}, false, ErrStore
	}
	return record, found, nil
}

func codeForDispatchError(err error) string {
	if errors.Is(err, ErrDriverPanic) {
		return CodeDriverPanic
	}
	return CodeDriverFailure
}

func codeForReadBackError(err error) string {
	if errors.Is(err, ErrReadBackPanic) {
		return CodeReadBackPanic
	}
	return CodeReadBackFailure
}
