package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/whhhh1500/auto-agent/internal/executionroute"
	core "github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/storage"
)

const (
	routeEvidenceDeliveryTimeout    = 5 * time.Second
	routeEvidenceMaterializeTimeout = 5 * time.Second
	routeEvidenceReconcileEvery     = time.Second
	routeEvidenceCandidateLimit     = 64
)

// RouteEvidenceDelivery is the synchronous acknowledgement boundary for a
// content-free route receipt. A nil error means the receiver accepted the
// immutable ReceiptID for deduplication. OpenTelemetry spans remain a separate
// best-effort observation and never acknowledge this durable outbox.
type RouteEvidenceDelivery interface {
	Deliver(context.Context, storage.RouteEvidenceReceipt) error
}

func (s *Server) materializeTerminalRouteEvidence(ctx context.Context, runtime *core.Runtime, session *core.Session, principal core.Principal, runID string, status core.RunStatus) error {
	if s == nil || s.routeEvidenceOutbox == nil || runtime == nil || session == nil || !routeEvidenceTerminalStatus(status) {
		return nil
	}
	terminalSeq, sourceVersion, ok := terminalRouteEvidenceSource(session, runID, status)
	if !ok {
		return nil
	}
	base := context.Background()
	if ctx != nil {
		base = context.WithoutCancel(ctx)
	}
	observeCtx, cancelObserve := context.WithTimeout(base, routeEvidenceObservationTimeout)
	reader, _ := runtime.ToolJournal.(core.ToolInvocationReader)
	evidence := executionroute.ObserveRouteEvidence(observeCtx, session, principal, reader, runID)
	cancelObserve()
	payload, ok, err := routeEvidenceReceiptPayload(session.ID(), runID, status, evidence, executionroute.ExpectsRouteEvidence(session, runID))
	if err != nil || !ok {
		return err
	}
	sum := sha256.Sum256(payload)
	persistCtx, cancelPersist := context.WithTimeout(base, routeEvidenceMaterializeTimeout)
	defer cancelPersist()
	_, _, err = s.routeEvidenceOutbox.CreateOrLoadRouteEvidenceReceipt(persistCtx, storage.RouteEvidenceReceiptInput{
		Protocol: storage.RouteEvidenceReceiptProtocol, SessionID: session.ID(), RunID: runID,
		TerminalEventSeq: terminalSeq, SourceSessionVersion: sourceVersion, TerminalStatus: string(status),
		Payload: payload, PayloadSHA256: hex.EncodeToString(sum[:]),
	})
	return err
}

func routeEvidenceReceiptPayload(sessionID, runID string, status core.RunStatus, evidence executionroute.RouteEvidenceProjection, expected bool) ([]byte, bool, error) {
	var attributes core.TelemetryAttributes
	if evidence.Identity.Status == executionroute.EvidenceStatusVerified {
		attributes = routeEvidenceAttributes(sessionID, runID, status, evidence)
	} else if expected {
		attributes = core.TelemetryAttributes{
			"run.id":                             runID,
			"session.id":                         sessionID,
			"run.status":                         string(status),
			"programmatic.route.identity.status": string(executionroute.EvidenceStatusUnavailable),
		}
	} else {
		return nil, false, nil
	}
	payload, err := json.Marshal(attributes)
	if err != nil {
		return nil, false, fmt.Errorf("encode route evidence receipt payload: %w", err)
	}
	return payload, true, nil
}

func terminalRouteEvidenceSource(session *core.Session, runID string, status core.RunStatus) (terminalSeq, sourceVersion int64, ok bool) {
	if session == nil || core.ValidateRunID(runID) != nil || !routeEvidenceTerminalStatus(status) {
		return 0, 0, false
	}
	events := session.Events()
	for index := len(events) - 1; index >= 0; index-- {
		event := events[index]
		if event.RunID != runID || event.Type != core.EvRunEnd {
			continue
		}
		var end core.RunEndData
		if json.Unmarshal(event.Data, &end) != nil || end.Status != status || event.Seq < 0 || session.Version() <= event.Seq {
			return 0, 0, false
		}
		return event.Seq, session.Version(), true
	}
	return 0, 0, false
}

func routeEvidenceTerminalStatus(status core.RunStatus) bool {
	switch status {
	case core.RunCompleted, core.RunLimited, core.RunFailed, core.RunCancelled:
		return true
	default:
		return false
	}
}

// reconcileRouteEvidenceOnce reads one bounded sidecar page. Every candidate
// is reloaded from the canonical Session and validated against its current
// principal and terminal state before it can materialize a receipt.
func (s *Server) reconcileRouteEvidenceOnce(ctx context.Context) {
	if s == nil || s.routeEvidenceOutbox == nil || s.routeEvidenceCandidates == nil || s.sessions == nil || s.runtime == nil {
		return
	}
	s.routeEvidenceReconcileMu.Lock()
	cursor := s.routeEvidenceCursor
	s.routeEvidenceReconcileMu.Unlock()
	candidates, next, err := s.routeEvidenceCandidates.ListTerminalRouteEvidenceCandidates(ctx, cursor, routeEvidenceCandidateLimit)
	if err != nil {
		s.logRouteEvidenceFailure("route evidence reconciliation query failed", "")
		return
	}
	for _, candidate := range candidates {
		status := core.RunStatus(candidate.Status)
		if !routeEvidenceTerminalStatus(status) {
			continue
		}
		session, loadErr := s.sessions.Load(ctx, candidate.SessionID)
		if loadErr != nil {
			s.logRouteEvidenceFailure("route evidence reconciliation session load failed", candidate.RunID)
			return
		}
		if session.Principal().TenantID != candidate.TenantID || session.Principal().SubjectID != candidate.SubjectID {
			s.logRouteEvidenceFailure("route evidence reconciliation candidate rejected", candidate.RunID)
			continue
		}
		observed, found := session.RunStatus(candidate.RunID)
		if !found || observed != status {
			s.logRouteEvidenceFailure("route evidence reconciliation terminal state rejected", candidate.RunID)
			continue
		}
		if err := s.materializeTerminalRouteEvidence(ctx, s.runtime, session, session.Principal(), candidate.RunID, status); err != nil {
			s.logRouteEvidenceFailure("route evidence reconciliation materialization failed", candidate.RunID)
			return
		}
	}
	s.routeEvidenceReconcileMu.Lock()
	if len(candidates) == 0 {
		s.routeEvidenceCursor = storage.RouteEvidenceTerminalCursor{}
	} else {
		s.routeEvidenceCursor = next
	}
	s.routeEvidenceReconcileMu.Unlock()
}

func (s *Server) runRouteEvidenceReconcileLoop(ctx context.Context) {
	ticker := time.NewTicker(routeEvidenceReconcileEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.reconcileRouteEvidenceOnce(ctx)
		}
	}
}

func (s *Server) runRouteEvidenceDeliveryLoop(ctx context.Context) {
	interval := s.runWorkerPoll
	if interval < 50*time.Millisecond {
		interval = 50 * time.Millisecond
	}
	for {
		_, err := s.dispatchRouteEvidenceOnce(ctx)
		if err != nil {
			s.logRouteEvidenceFailure("route evidence delivery failed", "")
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (s *Server) dispatchRouteEvidenceOnce(ctx context.Context) (bool, error) {
	if s == nil || s.routeEvidenceOutbox == nil || s.routeEvidenceDelivery == nil {
		return false, nil
	}
	workerID := s.instanceID + ":route-evidence-outbox"
	claimCtx, cancelClaim := context.WithTimeout(ctx, routeEvidenceDeliveryTimeout)
	claim, claimed, err := s.routeEvidenceOutbox.ClaimRouteEvidenceReceipt(claimCtx, workerID, routeEvidenceLeaseTTL(s.runWorkerClaimTTL))
	cancelClaim()
	if err != nil || !claimed {
		return false, err
	}
	deliveryCtx, cancelDelivery := context.WithTimeout(ctx, routeEvidenceDeliveryTimeout)
	err = deliverRouteEvidenceReceipt(deliveryCtx, s.routeEvidenceDelivery, claim.Receipt)
	cancelDelivery()
	if err != nil {
		retryCtx, cancelRetry := context.WithTimeout(ctx, routeEvidenceDeliveryTimeout)
		_, retryErr := s.routeEvidenceOutbox.RetryRouteEvidenceReceipt(retryCtx, claim.Receipt.ReceiptID, workerID, claim.LeaseGeneration,
			time.Now().UTC().Add(routeEvidenceRetryDelay(claim.Attempts)), "delivery_failed")
		cancelRetry()
		if retryErr != nil {
			return true, retryErr
		}
		return true, fmt.Errorf("route evidence delivery failed")
	}
	ackCtx, cancelAck := context.WithTimeout(ctx, routeEvidenceDeliveryTimeout)
	acked, ackErr := s.routeEvidenceOutbox.AckRouteEvidenceReceipt(ackCtx, claim.Receipt.ReceiptID, workerID, claim.LeaseGeneration)
	cancelAck()
	if ackErr != nil {
		return true, ackErr
	}
	if !acked {
		return true, fmt.Errorf("route evidence delivery acknowledgement lost")
	}
	return true, nil
}

// routeEvidenceLeaseTTL leaves a full delivery deadline plus acknowledgement
// budget before a second worker may reclaim a receipt. Hard failures can still
// deliver twice, so receivers deduplicate by the immutable ReceiptID.
func routeEvidenceLeaseTTL(configured time.Duration) time.Duration {
	minimum := 2 * routeEvidenceDeliveryTimeout
	if configured < minimum {
		return minimum
	}
	return configured
}

func deliverRouteEvidenceReceipt(ctx context.Context, delivery RouteEvidenceDelivery, receipt storage.RouteEvidenceReceipt) (err error) {
	if delivery == nil {
		return fmt.Errorf("route evidence delivery is unavailable")
	}
	copyOf := receipt
	copyOf.Payload = append([]byte(nil), receipt.Payload...)
	defer func() {
		if recover() != nil {
			err = fmt.Errorf("route evidence delivery panicked")
		}
	}()
	return delivery.Deliver(ctx, copyOf)
}

func routeEvidenceRetryDelay(attempts int64) time.Duration {
	if attempts < 1 {
		return time.Second
	}
	if attempts > 6 {
		attempts = 6
	}
	return time.Second * time.Duration(1<<uint(attempts-1))
}

func (s *Server) logRouteEvidenceFailure(message, runID string) {
	if s == nil || s.logger == nil {
		return
	}
	attributes := []slog.Attr{}
	if runID != "" {
		attributes = append(attributes, slog.String("run", runID))
	}
	s.logger.LogAttrs(context.Background(), slog.LevelWarn, message, attributes...)
}
