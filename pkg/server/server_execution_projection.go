package server

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/cc-auto-agent/harness-core/pkg/app/runexecutor"
	core "github.com/cc-auto-agent/harness-core/pkg/core"
	"github.com/cc-auto-agent/harness-core/pkg/storage"
)

const (
	executionProjectionEpochSource = "authorization_epoch"
	profileProjectionSourcePrefix  = "profile/"
	nativeStrictControlSource      = "native_strict_control"
)

var errExecutionProjectionUnavailable = errors.New("execution projection is unavailable")

var errExecutionProjectionEpochLag = errors.New("authorization epoch lag detected")

var errDurableBindingRequired = errors.New("authorization-epoch admission requires a durable SQL binding journal")

var errBindingProjectionInvalid = errors.New("binding projection is invalid")

// executionProjectionCoordinator admits new execution only while the local
// projection has no known fault and, when configured, matches the observed
// durable authorization epoch. admissionMu is deliberately separate from
// stateMu: a projection mutation may record faults while excluding only the
// short compose-to-run-start admission interval, never a whole model run.
//
// The epoch is an optimistic lag detector here, not a control-plane replayer
// or a transaction-level authorization grant. A future reconciler must rebuild
// the projection and call markAppliedEpoch only after that rebuild succeeds.
type executionProjectionCoordinator struct {
	admissionMu sync.RWMutex
	stateMu     sync.RWMutex
	reader      storage.AuthorizationEpochReader
	desired     int64
	applied     int64
	desiredSet  bool
	appliedSet  bool
	faults      map[string]error
}

func newExecutionProjectionCoordinator(reader storage.AuthorizationEpochReader) *executionProjectionCoordinator {
	return &executionProjectionCoordinator{reader: reader, faults: map[string]error{}}
}

func (c *executionProjectionCoordinator) acquire(ctx context.Context) (*executionProjectionLease, error) {
	if c == nil || c.reader == nil {
		if c != nil {
			if err := c.fault(""); err != nil {
				return nil, err
			}
		}
		return &executionProjectionLease{}, nil
	}
	if err := c.observeEpoch(ctx); err != nil {
		return nil, err
	}
	c.admissionMu.RLock()
	if err := c.observeEpoch(ctx); err != nil {
		c.admissionMu.RUnlock()
		return nil, err
	}
	if err := c.fault(""); err != nil {
		c.admissionMu.RUnlock()
		return nil, err
	}
	return &executionProjectionLease{coordinator: c, held: true}, nil
}

func (c *executionProjectionCoordinator) requiresEpoch() bool {
	return c != nil && c.reader != nil
}

func (c *executionProjectionCoordinator) observeEpoch(ctx context.Context) error {
	epoch, err := c.reader.AuthorizationEpoch(ctx)
	if err != nil {
		c.setFault(executionProjectionEpochSource, fmt.Errorf("read authorization epoch: %w", err))
		return fmt.Errorf("read authorization epoch: %w", err)
	}
	c.stateMu.Lock()
	c.desired, c.desiredSet = epoch, true
	if !c.appliedSet || c.applied != epoch {
		if c.faults[nativeStrictControlSource] != nil {
			// A native static-control violation is the primary fail-closed
			// reason. Do not replace it with the derivative epoch-lag symptom
			// or make admission attempt another automatic reconcile.
			delete(c.faults, executionProjectionEpochSource)
		} else {
			c.faults[executionProjectionEpochSource] = fmt.Errorf("%w: epoch %d is not applied to the local execution projection", errExecutionProjectionEpochLag, epoch)
		}
	} else {
		delete(c.faults, executionProjectionEpochSource)
	}
	c.stateMu.Unlock()
	return c.fault(executionProjectionEpochSource)
}

func (c *executionProjectionCoordinator) hasOnlyEpochLag() bool {
	if c == nil {
		return false
	}
	c.stateMu.RLock()
	defer c.stateMu.RUnlock()
	if len(c.faults) != 1 {
		return false
	}
	err, ok := c.faults[executionProjectionEpochSource]
	return ok && errors.Is(err, errExecutionProjectionEpochLag)
}

// markAppliedEpoch is intentionally private. Its caller must already have
// rebuilt the complete local execution projection for epoch; it does not sync
// bindings, releases, canaries, or any external authorization source itself.
func (c *executionProjectionCoordinator) markAppliedEpoch(ctx context.Context) error {
	if c == nil || c.reader == nil {
		return nil
	}
	epoch, err := c.reader.AuthorizationEpoch(ctx)
	if err != nil {
		c.setFault(executionProjectionEpochSource, fmt.Errorf("read authorization epoch: %w", err))
		return fmt.Errorf("read authorization epoch: %w", err)
	}
	c.stateMu.Lock()
	c.desired, c.desiredSet = epoch, true
	c.applied, c.appliedSet = epoch, true
	delete(c.faults, executionProjectionEpochSource)
	c.stateMu.Unlock()
	return nil
}

// markAppliedEpochValue records an epoch already read as part of a native
// constructor's stable control snapshot. It intentionally does not read the
// authority again: another read would permit a changed epoch to be marked
// applied without re-checking the static-control preflight.
func (c *executionProjectionCoordinator) markAppliedEpochValue(epoch int64) error {
	if c == nil || c.reader == nil {
		return fmt.Errorf("authorization epoch is not configured")
	}
	if epoch < 0 {
		return fmt.Errorf("authorization epoch must not be negative")
	}
	c.stateMu.Lock()
	c.desired, c.desiredSet = epoch, true
	c.applied, c.appliedSet = epoch, true
	delete(c.faults, executionProjectionEpochSource)
	c.stateMu.Unlock()
	return nil
}

// beginDurableBindingMutation records the epoch a local durable binding
// mutation is about to advance. The caller must hold admissionMu's write lock
// until finishDurableBindingMutation or verifyFailedBindingMutation returns.
func (c *executionProjectionCoordinator) beginDurableBindingMutation(ctx context.Context) (int64, bool, error) {
	if c == nil || c.reader == nil {
		return 0, false, nil
	}
	epoch, err := c.reader.AuthorizationEpoch(ctx)
	if err != nil {
		c.setFault(executionProjectionEpochSource, fmt.Errorf("read authorization epoch: %w", err))
		return 0, true, fmt.Errorf("read authorization epoch: %w", err)
	}
	c.stateMu.Lock()
	c.desired, c.desiredSet = epoch, true
	if !c.appliedSet || c.applied != epoch {
		c.faults[executionProjectionEpochSource] = fmt.Errorf("authorization epoch %d is not applied to the execution projection", epoch)
	} else {
		delete(c.faults, executionProjectionEpochSource)
	}
	c.stateMu.Unlock()
	return epoch, true, nil
}

// finishDurableBindingMutation marks exactly one durable authorization-epoch
// advance as locally applied. A different final value means another durable
// control mutation interleaved with this local publication, so execution stays
// blocked until a complete reconciler rebuilds the projection.
func (c *executionProjectionCoordinator) finishDurableBindingMutation(ctx context.Context, before int64) error {
	if c == nil || c.reader == nil {
		return nil
	}
	after, err := c.reader.AuthorizationEpoch(ctx)
	if err != nil {
		c.setFault(executionProjectionEpochSource, fmt.Errorf("read authorization epoch: %w", err))
		return fmt.Errorf("read authorization epoch: %w", err)
	}
	if before == int64(^uint64(0)>>1) || after != before+1 {
		err := fmt.Errorf("authorization epoch changed from %d to %d during durable binding publication", before, after)
		c.setFault(executionProjectionEpochSource, err)
		return err
	}
	c.stateMu.Lock()
	c.desired, c.desiredSet = after, true
	if c.appliedSet && c.applied == before {
		c.applied, c.appliedSet = after, true
		delete(c.faults, executionProjectionEpochSource)
	} else {
		c.faults[executionProjectionEpochSource] = fmt.Errorf("authorization epoch %d is not applied to the execution projection", after)
	}
	c.stateMu.Unlock()
	return nil
}

// verifyFailedBindingMutation permits the old projection to remain admitted
// only when the failed durable mutation demonstrably left the epoch unchanged.
// A response-lost or cross-instance interleave is treated as a projection
// fault rather than guessed at from the journal error.
func (c *executionProjectionCoordinator) verifyFailedBindingMutation(ctx context.Context, expected int64) error {
	if c == nil || c.reader == nil {
		return nil
	}
	actual, err := c.reader.AuthorizationEpoch(ctx)
	if err != nil {
		c.setFault(executionProjectionEpochSource, fmt.Errorf("read authorization epoch: %w", err))
		return fmt.Errorf("read authorization epoch: %w", err)
	}
	if actual != expected {
		err := fmt.Errorf("authorization epoch changed from %d to %d after durable binding failure", expected, actual)
		c.setFault(executionProjectionEpochSource, err)
		return err
	}
	c.stateMu.Lock()
	c.desired, c.desiredSet = actual, true
	if c.appliedSet && c.applied == actual {
		delete(c.faults, executionProjectionEpochSource)
	}
	c.stateMu.Unlock()
	return nil
}

// markEpochStale records that a local mutation or restore changed projection
// inputs. It intentionally does not read or apply an epoch; only a future
// complete reconciler may make that claim.
func (c *executionProjectionCoordinator) markEpochStale() {
	if c == nil || c.reader == nil {
		return
	}
	c.stateMu.Lock()
	c.appliedSet = false
	c.faults[executionProjectionEpochSource] = errors.New("execution projection requires authorization epoch reconciliation")
	c.stateMu.Unlock()
}

func (c *executionProjectionCoordinator) lockMutation() func() {
	if c == nil {
		return func() {}
	}
	c.admissionMu.Lock()
	return c.admissionMu.Unlock
}

func (c *executionProjectionCoordinator) setFault(source string, err error) {
	if c == nil || source == "" {
		return
	}
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if err == nil {
		delete(c.faults, source)
		return
	}
	c.faults[source] = err
}

func (c *executionProjectionCoordinator) fault(prefix string) error {
	if c == nil {
		return nil
	}
	c.stateMu.RLock()
	defer c.stateMu.RUnlock()
	keys := make([]string, 0, len(c.faults))
	for source := range c.faults {
		if prefix == "" || len(source) >= len(prefix) && source[:len(prefix)] == prefix {
			keys = append(keys, source)
		}
	}
	if len(keys) == 0 {
		return nil
	}
	sort.Strings(keys)
	return c.faults[keys[0]]
}

type executionProjectionLease struct {
	coordinator *executionProjectionCoordinator
	once        sync.Once
	held        bool
}

func (l *executionProjectionLease) releaseOnEvent(event core.SessionEvent) {
	if event.Type == core.EvRunStart || event.Type == core.EvRunResume {
		l.Release()
	}
}

func (l *executionProjectionLease) requiresStartEvent() bool {
	return l != nil && l.coordinator != nil && l.coordinator.reader != nil
}

func (l *executionProjectionLease) appliedEpoch() (int64, error) {
	if l == nil || !l.held || l.coordinator == nil || l.coordinator.reader == nil {
		return 0, fmt.Errorf("execution projection epoch lease is unavailable")
	}
	l.coordinator.stateMu.RLock()
	defer l.coordinator.stateMu.RUnlock()
	if !l.coordinator.appliedSet {
		return 0, fmt.Errorf("execution projection epoch is not applied")
	}
	return l.coordinator.applied, nil
}

func (l *executionProjectionLease) Release() {
	if l == nil || !l.held || l.coordinator == nil {
		return
	}
	l.once.Do(func() { l.coordinator.admissionMu.RUnlock() })
}

func (s *Server) executionProjectionCoordinator() *executionProjectionCoordinator {
	s.readyMu.Lock()
	defer s.readyMu.Unlock()
	if s.executionProjection == nil {
		s.executionProjection = newExecutionProjectionCoordinator(nil)
	}
	return s.executionProjection
}

func (s *Server) acquireExecutionProjection(ctx context.Context) (*executionProjectionLease, error) {
	return s.acquireExecutionProjectionWithNativePreflight(ctx, nil)
}

func (s *Server) acquireExecutionProjectionWithNativePreflight(ctx context.Context, preflight func(context.Context) error) (*executionProjectionLease, error) {
	coordinator := s.executionProjectionCoordinator()
	lease, err := coordinator.acquire(ctx)
	if err != nil && s.nativeStrict != nil && coordinator.hasOnlyEpochLag() {
		// acquire returned without a read lease. Serialize this static-only
		// reconcile against compose-to-run/start admission, then retry exactly
		// once. This is still a lag detector, not an SQL transaction grant.
		release := coordinator.lockMutation()
		reconcileErr := s.reconcileNativeStrictExecutionProjectionLockedWithPreflight(ctx, preflight)
		release()
		if reconcileErr == nil {
			lease, err = coordinator.acquire(ctx)
		} else {
			err = reconcileErr
		}
	}
	if err == nil {
		return lease, nil
	}
	if s.profileProjectionError() != nil {
		return nil, fmt.Errorf("%w: %w", errProfileProjectionUnavailable, err)
	}
	return nil, fmt.Errorf("%w: %w", errExecutionProjectionUnavailable, err)
}

func (s *Server) requiresExecutionProjectionEpoch() bool {
	return s.executionProjectionCoordinator().requiresEpoch()
}

// refreshExecutionProjection serializes a live Release/Canary refresh with
// new execution admission. It is intentionally a small foundation: it does
// not claim that those managers form one detached, epoch-materialized bundle.
func (s *Server) refreshExecutionProjection(ctx context.Context) error {
	release := s.lockExecutionProjectionMutation()
	defer release()
	return s.refreshControlPlane(ctx)
}

func (s *Server) lockExecutionProjectionMutation() func() {
	return s.executionProjectionCoordinator().lockMutation()
}

// MarkExecutionProjectionAppliedEpoch admits new epoch-gated executions after
// the caller has completely rebuilt and published this Server's local
// projection from its durable control source. It is a local admission marker,
// not a detached replayer or a transaction-level authorization grant.
func (s *Server) MarkExecutionProjectionAppliedEpoch(ctx context.Context) error {
	if s.nativeStrict != nil {
		return fmt.Errorf("native strict execution projection is constructor-owned")
	}
	release := s.lockExecutionProjectionMutation()
	defer release()
	return s.executionProjectionCoordinator().markAppliedEpoch(ctx)
}

// initializeNativeStrictExecutionProjection is only called after the native
// constructor observes the same authorization epoch on both sides of its
// static-control preflight. Phase 1 never marks recovery eligible.
func (s *Server) initializeNativeStrictExecutionProjection(epoch int64) error {
	if s == nil || s.nativeStrict == nil {
		return fmt.Errorf("native strict execution projection is unavailable")
	}
	if s.nativeStrict.phase != nativeStrictPhaseStaticBootstrap {
		return fmt.Errorf("native strict execution projection phase is invalid")
	}
	return s.executionProjectionCoordinator().markAppliedEpochValue(epoch)
}

// reconcileNativeStrictExecutionProjectionLocked is the only native Phase 1 path
// that advances an already-published applied epoch. Account lifecycle writes
// change the SQL principal authority but not the static Core projection; the
// same double-read/static-control check used at construction proves that
// distinction before admission reopens. Dynamic artifacts always fail closed
// rather than being replayed here.
// The caller must hold executionProjectionCoordinator.admissionMu for write.
// The stable preflight window does not extend through a later run/start SQL
// transaction and therefore is not an authorization grant.
func (s *Server) reconcileNativeStrictExecutionProjectionLocked(ctx context.Context) error {
	return s.reconcileNativeStrictExecutionProjectionLockedWithPreflight(ctx, nil)
}

func (s *Server) reconcileNativeStrictExecutionProjectionLockedWithPreflight(ctx context.Context, preflight func(context.Context) error) error {
	if s == nil || s.nativeStrict == nil || s.nativeStrict.phase != nativeStrictPhaseStaticBootstrap {
		return fmt.Errorf("native strict execution projection is unavailable")
	}
	coordinator := s.executionProjectionCoordinator()
	if coordinator.reader == nil || s.nativeStrict.db == nil {
		return fmt.Errorf("native strict authorization authority is unavailable")
	}
	if preflight == nil {
		preflight = func(ctx context.Context) error {
			return storage.VerifyNativeStrictStaticControl(ctx, s.nativeStrict.db, s.nativeStrict.dialect)
		}
	}
	var last error
	for attempt := 0; attempt < nativeStrictBootstrapAttempts; attempt++ {
		before, err := coordinator.reader.AuthorizationEpoch(ctx)
		if err != nil {
			last = fmt.Errorf("read authorization epoch: %w", err)
			break
		}
		if err := preflight(ctx); err != nil {
			if errors.Is(err, storage.ErrNativeStrictDynamicControl) {
				coordinator.setFault(nativeStrictControlSource, err)
			}
			return err
		}
		after, err := coordinator.reader.AuthorizationEpoch(ctx)
		if err != nil {
			last = fmt.Errorf("re-read authorization epoch: %w", err)
			break
		}
		if before == after {
			coordinator.setFault(nativeStrictControlSource, nil)
			return coordinator.markAppliedEpochValue(after)
		}
		last = fmt.Errorf("authorization control changed during native strict reconcile")
	}
	if last == nil {
		last = fmt.Errorf("authorization control changed during native strict reconcile")
	}
	coordinator.setFault(executionProjectionEpochSource, last)
	return last
}

// nativeStrictRecoveryEligible remains false until native ownership is paired
// with a detached control projection, V2 re-entry, and a model journal.
func (s *Server) nativeStrictRecoveryEligible() bool {
	return false
}

func (s *Server) markExecutionProjectionEpochStale() {
	s.executionProjectionCoordinator().markEpochStale()
}

func (s *Server) setProfileProjectionFault(bindingID string, err error) {
	s.executionProjectionCoordinator().setFault(profileProjectionSourcePrefix+bindingID, err)
}

func (s *Server) clearProfileProjectionFault(bindingID string) {
	s.executionProjectionCoordinator().setFault(profileProjectionSourcePrefix+bindingID, nil)
}

func requireProjectionAwareExecutor(lease *executionProjectionLease, executor runexecutor.RunExecutor) error {
	if !lease.requiresStartEvent() {
		return nil
	}
	if _, ok := executor.(*runexecutor.Sequential); !ok {
		return fmt.Errorf("%w: authorization-epoch admission requires the built-in sequential executor", errExecutionProjectionUnavailable)
	}
	return nil
}
