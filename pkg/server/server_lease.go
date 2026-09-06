package server

import (
	"context"
	"crypto/rand"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// leaseTicker is kept behind a tiny interface so lease lifecycle behavior can
// be tested deterministically without sleeping or running stress tests.
type leaseTicker interface {
	C() <-chan time.Time
	Stop()
}

type leaseTickerFactory func(time.Duration) leaseTicker

type realLeaseTicker struct{ *time.Ticker }

func (t realLeaseTicker) C() <-chan time.Time { return t.Ticker.C }

func realLeaseTickerFactory(interval time.Duration) leaseTicker {
	return realLeaseTicker{Ticker: time.NewTicker(interval)}
}

func newInstanceUUID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	value[6] = (value[6] & 0x0f) | 0x40
	value[8] = (value[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		value[0:4], value[4:6], value[6:8], value[8:10], value[10:16]), nil
}

func leaseRenewInterval(ttl time.Duration) time.Duration {
	interval := ttl / 3
	if interval <= 0 {
		return time.Nanosecond
	}
	return interval
}

func defaultLeaseOperationTimeout(ttl time.Duration) time.Duration {
	timeout := leaseRenewInterval(ttl)
	if timeout > 5*time.Second {
		return 5 * time.Second
	}
	return timeout
}

func (s *Server) leaseHolder(runID string) string {
	return s.instanceID + ":" + runID
}

// acquireRunLease obtains one run-scoped lease and registers its renewal with
// the server-wide liveness scheduler.
// The returned cleanup first stops renewal, then releases ownership with a
// bounded context. Renewal failure cancels the run context immediately.
func (s *Server) acquireRunLease(runCtx context.Context, cancelRun context.CancelFunc, sessionID, runID string) (cleanup func(), acquired bool, err error) {
	holder := s.leaseHolder(runID)
	acquired, err = s.leaser.AcquireSessionLease(runCtx, sessionID, holder, s.leaseTTL)
	if err != nil || !acquired {
		return func() {}, acquired, err
	}

	if s.liveness == nil {
		releaseCtx, cancelRelease := context.WithTimeout(context.WithoutCancel(runCtx), s.leaseOpTimeout)
		_ = s.leaser.ReleaseSessionLease(releaseCtx, sessionID, holder)
		cancelRelease()
		return func() {}, false, fmt.Errorf("run liveness scheduler is not configured")
	}
	stopRenewal, err := s.liveness.Register(runCtx, "session-lease:"+holder, leaseRenewInterval(s.leaseTTL), func(ctx context.Context) error {
		renewCtx, cancelRenew := context.WithTimeout(ctx, s.leaseOpTimeout)
		renewed, renewErr := s.leaser.RenewSessionLease(renewCtx, sessionID, holder, s.leaseTTL)
		cancelRenew()
		if ctx.Err() != nil {
			return nil
		}
		if renewErr == nil && renewed {
			return nil
		}
		if s.logger != nil {
			attrs := []any{slog.String("session", sessionID), slog.String("holder", holder)}
			if renewErr != nil {
				attrs = append(attrs, slog.String("error", renewErr.Error()))
			} else {
				attrs = append(attrs, slog.String("error", "lease ownership lost"))
			}
			s.logger.ErrorContext(ctx, "session lease renewal failed", attrs...)
		}
		return fmt.Errorf("session lease renewal failed")
	}, func() { cancelRun() })
	if err != nil {
		releaseCtx, cancelRelease := context.WithTimeout(context.WithoutCancel(runCtx), s.leaseOpTimeout)
		_ = s.leaser.ReleaseSessionLease(releaseCtx, sessionID, holder)
		cancelRelease()
		return func() {}, false, err
	}

	var cleanupOnce sync.Once
	cleanup = func() {
		cleanupOnce.Do(func() {
			stopRenewal()

			// Lease release remains bounded after cancellation while preserving
			// the originating request or worker SpanContext for its error log.
			releaseCtx := context.WithoutCancel(runCtx)
			releaseOpCtx, cancelRelease := context.WithTimeout(releaseCtx, s.leaseOpTimeout)
			defer cancelRelease()
			if releaseErr := s.leaser.ReleaseSessionLease(releaseOpCtx, sessionID, holder); releaseErr != nil && s.logger != nil {
				s.logger.ErrorContext(releaseCtx, "session lease release failed",
					slog.String("session", sessionID),
					slog.String("holder", holder),
					slog.String("error", releaseErr.Error()),
				)
			}
		})
	}
	return cleanup, true, nil
}
