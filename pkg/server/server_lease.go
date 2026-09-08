package server

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	core "github.com/whhhh1500/auto-agent/pkg/core"
)

const maxSessionLeaseHolderRunes = 128

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

// runLeaseHandle identifies exactly one successful session lease acquisition.
// A handle is intentionally not reusable: its random nonce prevents a delayed
// renewal or release from an old acquisition of the same instance/run from
// matching a later acquisition.
type runLeaseHandle struct {
	holder  string
	release func()
}

func (h runLeaseHandle) Holder() string { return h.holder }

func (h runLeaseHandle) Release() {
	if h.release != nil {
		h.release()
	}
}

func (s *Server) leaseHolder(runID string, queueGeneration int64) (string, error) {
	if err := core.ValidateRunID(runID); err != nil {
		return "", fmt.Errorf("session lease holder run id: %w", err)
	}
	if queueGeneration < 0 {
		return "", fmt.Errorf("session lease holder queue generation must not be negative")
	}
	nonce, err := newInstanceUUID()
	if err != nil {
		return "", fmt.Errorf("generate session lease acquisition nonce: %w", err)
	}
	// Run IDs are valid at up to 128 characters while SQL lease holders are
	// bounded to 128 runes. A fixed-size digest keeps the holder valid and avoids
	// carrying a caller-controlled run identifier into operational logs. The
	// actual RunID remains separately available on every lease operation and
	// telemetry record.
	runDigest := sha256.Sum256([]byte(runID))
	holder := s.instanceID + ":r" + fmt.Sprintf("%x", runDigest[:16]) + ":g" + strconv.FormatInt(queueGeneration, 10) + ":" + nonce
	if len(holder) > maxSessionLeaseHolderRunes {
		return "", fmt.Errorf("generated session lease holder exceeds %d runes", maxSessionLeaseHolderRunes)
	}
	return holder, nil
}

// acquireRunLease obtains one run-scoped lease and registers its renewal with
// the server-wide liveness scheduler.
// The returned cleanup first stops renewal, then releases ownership with a
// bounded context. Renewal failure cancels the run context immediately.
func (s *Server) acquireRunLease(runCtx context.Context, cancelRun context.CancelFunc, sessionID, runID string, queueGeneration int64) (handle runLeaseHandle, acquired bool, err error) {
	holder, err := s.leaseHolder(runID, queueGeneration)
	if err != nil {
		return runLeaseHandle{}, false, err
	}
	handle.holder = holder
	handle.release = func() {}
	acquired, err = s.leaser.AcquireSessionLease(runCtx, sessionID, holder, s.leaseTTL)
	if err != nil {
		// A well-behaved leaser returns acquired=false with an error. Defend
		// against an adapter that has committed acquisition but then fails while
		// reporting it: do not leak an otherwise live holder.
		if acquired {
			s.releaseRunLease(runCtx, sessionID, holder)
		}
		return handle, false, err
	}
	if !acquired {
		return handle, false, nil
	}

	if s.liveness == nil {
		s.releaseRunLease(runCtx, sessionID, holder)
		return handle, false, fmt.Errorf("run liveness scheduler is not configured")
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
		s.releaseRunLease(runCtx, sessionID, holder)
		return handle, false, err
	}

	var cleanupOnce sync.Once
	handle.release = func() {
		cleanupOnce.Do(func() {
			stopRenewal()

			s.releaseRunLease(runCtx, sessionID, holder)
		})
	}
	return handle, true, nil
}

func (s *Server) releaseRunLease(runCtx context.Context, sessionID, holder string) {
	// Lease release remains bounded after cancellation while preserving the
	// originating request or worker SpanContext for its error log.
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
}
