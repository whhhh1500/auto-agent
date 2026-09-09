package server

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/whhhh1500/auto-agent/pkg/app/effectreceipt"
	"github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/storage"
)

const (
	effectRecoveryEvery = time.Second
	effectRecoveryLimit = 64
)

// effectRecoveryRetrySweep is a private, bounded rescan cursor. It is kept
// apart from the forward cursor because an authorization denial deliberately
// advances a recovery page; an unbounded stream of newer rows must not make
// that older denied row unreachable when authority is later restored.
type effectRecoveryRetrySweep struct {
	cursor    effectreceipt.RecoveryCursor
	watermark effectreceipt.RecoveryCursor
	active    bool
	repeat    bool
}

// nativeStrictEffectRecoveryAuthorizer reconstructs current authority before
// a provider read-back. It is intentionally server-owned: a durable intent is
// insufficient authority after an account or capability has changed.
type nativeStrictEffectRecoveryAuthorizer struct {
	runtime  *core.Runtime
	resolver *storage.SQLQueuedPrincipalResolver
	sessions *storage.SQLSessionStore
}

func (a nativeStrictEffectRecoveryAuthorizer) AuthorizeEffectRecovery(ctx context.Context, record effectreceipt.RecoveryRecord) (bool, error) {
	if a.runtime == nil || a.runtime.Capabilities == nil || a.resolver == nil || a.sessions == nil {
		return false, fmt.Errorf("effect recovery authority is unavailable")
	}
	invocation := record.Intent.Invocation
	principal, err := a.resolver.ResolveRunPrincipal(ctx, invocation.TenantID, invocation.SubjectID)
	if err != nil {
		if permanentQueuedPrincipalError(err) {
			return false, nil
		}
		return false, err
	}
	session, err := a.sessions.Load(ctx, invocation.SessionID)
	if err != nil {
		return false, err
	}
	if !sameNativeQueuedPrincipal(session.Principal(), principal) {
		return false, nil
	}
	if _, found := session.RunStatus(invocation.RunID); !found {
		return false, nil
	}
	snapshot, err := (core.CapabilityResolver{Registry: a.runtime.Capabilities}).Resolve(principal, session.Scope())
	if err != nil {
		return false, err
	}
	if a.runtime.Profiles == nil {
		return false, fmt.Errorf("effect recovery profile authority is unavailable")
	}
	profile, err := a.runtime.Profiles.Resolve(principal, session.Scope(), session.ProfileID())
	if err != nil {
		return false, err
	}
	profileAllows := false
	for _, capabilityID := range profile.Capabilities {
		if capabilityID == invocation.CapabilityID {
			profileAllows = true
			break
		}
	}
	if !profileAllows {
		return false, nil
	}
	for _, capability := range snapshot.Capabilities() {
		if capability.Manifest.ID == invocation.CapabilityID && capability.Manifest.Idempotent == invocation.Idempotent && principal.Grants.Allows(capability.Manifest.RequiredPermissions) {
			return true, nil
		}
	}
	return false, nil
}

func (s *Server) runEffectReceiptRecoveryOnce(ctx context.Context) {
	if s == nil || s.effectReceiptRecovery == nil {
		return
	}
	for _, driver := range s.effectReceiptRecovery.Drivers() {
		s.effectRecoveryMu.Lock()
		cursor := s.effectRecoveryCursors[driver]
		s.effectRecoveryMu.Unlock()
		result, err := s.effectReceiptRecovery.RecoverPage(ctx, effectreceipt.RecoveryQuery{Driver: driver}, cursor, effectRecoveryLimit)
		if err != nil {
			s.logEffectRecoveryFailure(driver)
		} else {
			s.effectRecoveryMu.Lock()
			if result.Examined == 0 {
				s.effectRecoveryCursors[driver] = effectreceipt.RecoveryCursor{}
			} else {
				s.effectRecoveryCursors[driver] = result.Next
			}
			s.effectRecoveryMu.Unlock()
			s.scheduleEffectReceiptRetrySweep(driver, cursor, result)
		}
		s.runEffectReceiptRetrySweep(ctx, driver)
	}
}

// runEffectReceiptRetrySweep performs one extra bounded read-back page for a
// driver. Its fixed watermark bounds the retry to the known-denied prefix,
// rather than waiting for the forward cursor to find an empty page; a
// perpetual tail of new unresolved records therefore cannot starve that prefix.
func (s *Server) runEffectReceiptRetrySweep(ctx context.Context, driver effectreceipt.DriverRef) {
	s.effectRecoveryMu.Lock()
	if s.effectRecoveryRetrySweeps == nil {
		s.effectRecoveryRetrySweeps = make(map[effectreceipt.DriverRef]effectRecoveryRetrySweep)
	}
	sweep := s.effectRecoveryRetrySweeps[driver]
	s.effectRecoveryMu.Unlock()
	if !sweep.active {
		return
	}

	result, err := s.effectReceiptRecovery.RecoverPage(ctx, effectreceipt.RecoveryQuery{Driver: driver}, sweep.cursor, effectRecoveryLimit)
	if err != nil {
		s.logEffectRecoveryFailure(driver)
		return
	}

	s.effectRecoveryMu.Lock()
	defer s.effectRecoveryMu.Unlock()
	sweep.repeat = sweep.repeat || result.Unauthorized > 0
	if result.Examined == 0 || effectRecoveryCursorAtOrAfter(result.Next, sweep.watermark) {
		if sweep.repeat {
			sweep.cursor = effectreceipt.RecoveryCursor{}
			sweep.repeat = false
		} else {
			sweep = effectRecoveryRetrySweep{}
		}
	} else {
		sweep.cursor = result.Next
	}
	s.effectRecoveryRetrySweeps[driver] = sweep
}

func (s *Server) scheduleEffectReceiptRetrySweep(driver effectreceipt.DriverRef, start effectreceipt.RecoveryCursor, result effectreceipt.RecoveryPageResult) {
	if result.Unauthorized == 0 {
		return
	}
	s.effectRecoveryMu.Lock()
	defer s.effectRecoveryMu.Unlock()
	if s.effectRecoveryRetrySweeps == nil {
		s.effectRecoveryRetrySweeps = make(map[effectreceipt.DriverRef]effectRecoveryRetrySweep)
	}
	sweep := s.effectRecoveryRetrySweeps[driver]
	if sweep.active {
		if effectRecoveryCursorAtOrAfter(result.Next, sweep.watermark) {
			sweep.watermark = result.Next
			s.effectRecoveryRetrySweeps[driver] = sweep
		}
		return
	}
	s.effectRecoveryRetrySweeps[driver] = effectRecoveryRetrySweep{cursor: start, watermark: result.Next, active: true}
}

func effectRecoveryCursorAtOrAfter(left, right effectreceipt.RecoveryCursor) bool {
	if right.UpdatedAt.IsZero() || left.UpdatedAt.After(right.UpdatedAt) {
		return true
	}
	if left.UpdatedAt.Before(right.UpdatedAt) {
		return false
	}
	leftValues := []string{left.TenantID, left.SubjectID, left.SessionID, left.RunID, left.CallID}
	rightValues := []string{right.TenantID, right.SubjectID, right.SessionID, right.RunID, right.CallID}
	for index := range leftValues {
		if leftValues[index] == rightValues[index] {
			continue
		}
		return leftValues[index] > rightValues[index]
	}
	return true
}

func (s *Server) runEffectReceiptRecoveryLoop(ctx context.Context) {
	ticker := time.NewTicker(effectRecoveryEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.runEffectReceiptRecoveryOnce(ctx)
		}
	}
}

func (s *Server) logEffectRecoveryFailure(driver effectreceipt.DriverRef) {
	if s == nil || s.logger == nil {
		return
	}
	s.logger.LogAttrs(context.Background(), slog.LevelWarn, "external effect recovery failed",
		slog.String("driver.id", boundedEffectRecoveryLogValue(driver.ID)),
		slog.String("driver.version", boundedEffectRecoveryLogValue(driver.Version)),
	)
}

func boundedEffectRecoveryLogValue(value string) string {
	const limit = 128
	if len(value) > limit {
		return value[:limit]
	}
	return value
}
