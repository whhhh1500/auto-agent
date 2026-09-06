package server

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
	"github.com/cc-auto-agent/harness-core/pkg/extensions/runner"
)

const (
	minRunnerRecoveryInterval = time.Second
	maxRunnerRecoveryInterval = 30 * time.Second
)

// DefaultRunnerRecoveryInterval returns the bounded recovery cadence for hub.
// It returns zero when no private-runner hub is configured.
func DefaultRunnerRecoveryInterval(hub *runner.Hub) time.Duration {
	if hub == nil {
		return 0
	}
	interval := hub.LeaseTTL / 2
	if interval < minRunnerRecoveryInterval {
		return minRunnerRecoveryInterval
	}
	if interval > maxRunnerRecoveryInterval {
		return maxRunnerRecoveryInterval
	}
	return interval
}

// StartRunnerRecoveryLoop starts durable expired-claim recovery for private
// runner tasks. It is idempotent while the loop is already running.
func (s *Server) StartRunnerRecoveryLoop(ctx context.Context) error {
	if s.runners == nil {
		return nil
	}
	if ctx == nil {
		return fmt.Errorf("runner recovery context is nil")
	}

	s.runnerRecoveryMu.Lock()
	defer s.runnerRecoveryMu.Unlock()
	if s.runnerRecoveryRunning {
		return nil
	}
	if err := s.recoverExpiredRunnerTasks(ctx); err != nil {
		return err
	}

	recoveryCtx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	s.runnerRecoveryRunning = true
	s.runnerRecoveryStop = stop
	s.runnerRecoveryDone = done
	go s.runRunnerRecoveryLoop(recoveryCtx, done)
	return nil
}

func (s *Server) runRunnerRecoveryLoop(ctx context.Context, done chan struct{}) {
	newTicker := s.newRunnerRecoveryTicker
	if newTicker == nil {
		newTicker = realLeaseTickerFactory
	}
	ticker := newTicker(s.runnerRecoveryInterval)
	defer ticker.Stop()
	defer func() {
		s.runnerRecoveryMu.Lock()
		if s.runnerRecoveryDone == done {
			s.runnerRecoveryRunning = false
			s.runnerRecoveryStop = nil
			s.runnerRecoveryDone = nil
		}
		s.runnerRecoveryMu.Unlock()
		close(done)
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C():
			if err := s.recoverExpiredRunnerTasks(ctx); err != nil && ctx.Err() == nil && s.logger != nil {
				s.logger.ErrorContext(ctx, "expired runner task recovery failed", slog.String("error", err.Error()))
			}
		}
	}
}

func (s *Server) recoverExpiredRunnerTasks(ctx context.Context) error {
	if s.runners == nil {
		return nil
	}
	opCtx, cancel := context.WithTimeout(ctx, runControlOperationTimeout)
	defer cancel()
	requeued, terminal, err := s.runners.RecoverExpired(opCtx, time.Now().UTC())
	if err != nil {
		core.AddTelemetryCounter(s.telemetry, ctx, core.MetricQueueRecoveries, 1,
			core.TelemetryAttributes{"queue.kind": "private_runner", "recovery.outcome": "error"})
		return err
	}
	if requeued > 0 {
		core.AddTelemetryCounter(s.telemetry, ctx, core.MetricQueueRecoveries, requeued,
			core.TelemetryAttributes{"queue.kind": "private_runner", "recovery.outcome": "requeued"})
	}
	if terminal > 0 {
		core.AddTelemetryCounter(s.telemetry, ctx, core.MetricQueueRecoveries, terminal,
			core.TelemetryAttributes{"queue.kind": "private_runner", "recovery.outcome": "terminal"})
	}
	if (requeued > 0 || terminal > 0) && s.logger != nil {
		s.logger.InfoContext(ctx, "recovered expired runner tasks", slog.Int64("requeued", requeued), slog.Int64("terminal", terminal))
	}
	return nil
}
