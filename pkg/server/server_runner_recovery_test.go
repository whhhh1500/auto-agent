package server

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"testing"
	"time"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
	"github.com/cc-auto-agent/harness-core/pkg/extensions/runner"
)

type runnerRecoveryResult struct {
	requeued int64
	terminal int64
	err      error
}

type runnerRecoveryStore struct {
	runner.Store

	mu        sync.Mutex
	calls     int
	nows      []time.Time
	deadlines []bool
	results   []runnerRecoveryResult
	called    chan struct{}
}

func (s *runnerRecoveryStore) RecoverExpiredTasks(ctx context.Context, now time.Time) (int64, int64, error) {
	s.mu.Lock()
	index := s.calls
	s.calls++
	s.nows = append(s.nows, now)
	_, bounded := ctx.Deadline()
	s.deadlines = append(s.deadlines, bounded)
	result := runnerRecoveryResult{}
	if index < len(s.results) {
		result = s.results[index]
	}
	s.mu.Unlock()
	if s.called != nil {
		select {
		case s.called <- struct{}{}:
		default:
		}
	}
	return result.requeued, result.terminal, result.err
}

func (s *runnerRecoveryStore) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

type capturedRunnerRecoveryLog struct {
	level   slog.Level
	message string
	attrs   map[string]any
}

type runnerRecoveryLogHandler struct {
	mu      sync.Mutex
	records []capturedRunnerRecoveryLog
}

func (*runnerRecoveryLogHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *runnerRecoveryLogHandler) Handle(_ context.Context, record slog.Record) error {
	attrs := map[string]any{}
	record.Attrs(func(attr slog.Attr) bool {
		attrs[attr.Key] = attr.Value.Any()
		return true
	})
	h.mu.Lock()
	h.records = append(h.records, capturedRunnerRecoveryLog{level: record.Level, message: record.Message, attrs: attrs})
	h.mu.Unlock()
	return nil
}

func (h *runnerRecoveryLogHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *runnerRecoveryLogHandler) WithGroup(string) slog.Handler      { return h }

func newRunnerRecoveryTestServer(t *testing.T, results []runnerRecoveryResult, logger *slog.Logger, telemetry core.Telemetry) (*Server, *runnerRecoveryStore) {
	t.Helper()
	store := &runnerRecoveryStore{Store: runner.NewMemoryStore(), results: results, called: make(chan struct{}, 8)}
	hub := runner.NewHubWithStore(store)
	hub.LeaseTTL = 8 * time.Second
	server, err := New(Config{
		Runtime: &core.Runtime{}, Sessions: core.NewMemorySessionStore(), Runners: hub,
		Authenticator: AuthenticatorFunc(func(*http.Request) (core.Principal, error) { return core.Principal{}, nil }),
		Logger:        logger, Telemetry: telemetry,
	})
	if err != nil {
		t.Fatal(err)
	}
	return server, store
}

func waitForRunnerRecovery(t *testing.T, store *runnerRecoveryStore) {
	t.Helper()
	select {
	case <-store.called:
	case <-time.After(time.Second):
		t.Fatal("runner recovery did not run")
	}
}

func waitForRunnerRecoveryStop(t *testing.T, ticker *manualLeaseTicker) {
	t.Helper()
	select {
	case <-ticker.stop:
	case <-time.After(time.Second):
		t.Fatal("runner recovery ticker did not stop")
	}
}

func TestStartRunnerRecoveryLoop(t *testing.T) {
	t.Run("no runner is disabled", func(t *testing.T) {
		server, err := New(Config{
			Runtime: &core.Runtime{}, Sessions: core.NewMemorySessionStore(),
			Authenticator: AuthenticatorFunc(func(*http.Request) (core.Principal, error) { return core.Principal{}, nil }),
		})
		if err != nil {
			t.Fatal(err)
		}
		var ctx context.Context
		if err := server.StartRunnerRecoveryLoop(ctx); err != nil {
			t.Fatalf("disabled runner recovery returned %v", err)
		}
	})

	t.Run("nil context is rejected", func(t *testing.T) {
		server, _ := newRunnerRecoveryTestServer(t, nil, nil, nil)
		var ctx context.Context
		if err := server.StartRunnerRecoveryLoop(ctx); err == nil {
			t.Fatal("nil runner recovery context was accepted")
		}
	})

	t.Run("startup recovery runs before the loop", func(t *testing.T) {
		server, store := newRunnerRecoveryTestServer(t, nil, nil, nil)
		ticker := &manualLeaseTicker{ticks: make(chan time.Time, 1), stop: make(chan struct{})}
		server.newRunnerRecoveryTicker = func(interval time.Duration) leaseTicker {
			if interval != 4*time.Second {
				t.Fatalf("recovery interval = %s, want 4s", interval)
			}
			return ticker
		}
		ctx, cancel := context.WithCancel(context.Background())
		if err := server.StartRunnerRecoveryLoop(ctx); err != nil {
			cancel()
			t.Fatal(err)
		}
		if store.callCount() != 1 {
			cancel()
			t.Fatalf("startup recovery calls = %d, want 1", store.callCount())
		}
		waitForRunnerRecovery(t, store)
		cancel()
		waitForRunnerRecoveryStop(t, ticker)
	})

	t.Run("startup error does not mark the loop running", func(t *testing.T) {
		server, store := newRunnerRecoveryTestServer(t, []runnerRecoveryResult{{err: errors.New("store unavailable")}}, nil, nil)
		server.newRunnerRecoveryTicker = func(time.Duration) leaseTicker {
			t.Fatal("startup error started a runner recovery ticker")
			return nil
		}
		if err := server.StartRunnerRecoveryLoop(context.Background()); err == nil {
			t.Fatal("startup recovery error was ignored")
		}
		if store.callCount() != 1 {
			t.Fatalf("startup recovery calls = %d, want 1", store.callCount())
		}
		server.runnerRecoveryMu.Lock()
		running, done := server.runnerRecoveryRunning, server.runnerRecoveryDone
		server.runnerRecoveryMu.Unlock()
		if running || done != nil {
			t.Fatalf("startup failure left recovery running=%t done=%v", running, done)
		}
	})

	t.Run("ticker triggers a second recovery", func(t *testing.T) {
		server, store := newRunnerRecoveryTestServer(t, nil, nil, nil)
		ticker := &manualLeaseTicker{ticks: make(chan time.Time, 1), stop: make(chan struct{})}
		server.newRunnerRecoveryTicker = func(time.Duration) leaseTicker { return ticker }
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		if err := server.StartRunnerRecoveryLoop(ctx); err != nil {
			t.Fatal(err)
		}
		waitForRunnerRecovery(t, store)
		ticker.ticks <- time.Now()
		waitForRunnerRecovery(t, store)
		if store.callCount() != 2 {
			t.Fatalf("recovery calls = %d, want 2", store.callCount())
		}
		cancel()
		waitForRunnerRecoveryStop(t, ticker)
	})

	t.Run("context cancellation clears loop state", func(t *testing.T) {
		server, store := newRunnerRecoveryTestServer(t, nil, nil, nil)
		ticker := &manualLeaseTicker{ticks: make(chan time.Time, 1), stop: make(chan struct{})}
		server.newRunnerRecoveryTicker = func(time.Duration) leaseTicker { return ticker }
		ctx, cancel := context.WithCancel(context.Background())
		if err := server.StartRunnerRecoveryLoop(ctx); err != nil {
			cancel()
			t.Fatal(err)
		}
		waitForRunnerRecovery(t, store)
		server.runnerRecoveryMu.Lock()
		done := server.runnerRecoveryDone
		server.runnerRecoveryMu.Unlock()
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("runner recovery loop did not stop after context cancellation")
		}
		waitForRunnerRecoveryStop(t, ticker)
		server.runnerRecoveryMu.Lock()
		running, currentDone := server.runnerRecoveryRunning, server.runnerRecoveryDone
		server.runnerRecoveryMu.Unlock()
		if running || currentDone != nil {
			t.Fatalf("cancelled recovery left running=%t done=%v", running, currentDone)
		}
	})

	t.Run("duplicate start does not duplicate recovery loops", func(t *testing.T) {
		server, store := newRunnerRecoveryTestServer(t, nil, nil, nil)
		ticker := &manualLeaseTicker{ticks: make(chan time.Time, 1), stop: make(chan struct{})}
		server.newRunnerRecoveryTicker = func(time.Duration) leaseTicker { return ticker }
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		if err := server.StartRunnerRecoveryLoop(ctx); err != nil {
			t.Fatal(err)
		}
		waitForRunnerRecovery(t, store)
		if err := server.StartRunnerRecoveryLoop(ctx); err != nil {
			t.Fatal(err)
		}
		if store.callCount() != 1 {
			t.Fatalf("duplicate start made %d startup recoveries, want 1", store.callCount())
		}
		ticker.ticks <- time.Now()
		waitForRunnerRecovery(t, store)
		if store.callCount() != 2 {
			t.Fatalf("one tick made %d recoveries, want 2 total", store.callCount())
		}
		cancel()
		waitForRunnerRecoveryStop(t, ticker)
	})
}

func TestRecoverExpiredRunnerTasksRecordsLogsAndTelemetry(t *testing.T) {
	logs := &runnerRecoveryLogHandler{}
	telemetry := newServerTelemetryRecorder()
	server, store := newRunnerRecoveryTestServer(t, []runnerRecoveryResult{
		{requeued: 2, terminal: 3},
		{},
		{err: errors.New("recover failed")},
	}, slog.New(logs), telemetry)

	if err := server.recoverExpiredRunnerTasks(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := server.recoverExpiredRunnerTasks(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := server.recoverExpiredRunnerTasks(context.Background()); err == nil {
		t.Fatal("recovery error was ignored")
	}

	store.mu.Lock()
	if store.calls != 3 || len(store.nows) != 3 || len(store.deadlines) != 3 {
		store.mu.Unlock()
		t.Fatalf("recovery calls=%d nows=%d deadlines=%d", store.calls, len(store.nows), len(store.deadlines))
	}
	for index, now := range store.nows {
		if now.IsZero() || now.Location() != time.UTC || !store.deadlines[index] {
			store.mu.Unlock()
			t.Fatalf("recovery call %d now=%s bounded=%t", index, now, store.deadlines[index])
		}
	}
	store.mu.Unlock()

	telemetry.mu.Lock()
	count := telemetry.counters[core.MetricQueueRecoveries]
	attrs := append([]core.TelemetryAttributes(nil), telemetry.metricAttributes[core.MetricQueueRecoveries]...)
	telemetry.mu.Unlock()
	if count != 6 {
		t.Fatalf("queue recovery counter = %d, want 6", count)
	}
	if len(attrs) != 3 {
		t.Fatalf("queue recovery attributes = %#v", attrs)
	}
	wantOutcomes := []string{"requeued", "terminal", "error"}
	for index, outcome := range wantOutcomes {
		if attrs[index]["queue.kind"] != "private_runner" || attrs[index]["recovery.outcome"] != outcome {
			t.Fatalf("recovery attributes[%d] = %#v", index, attrs[index])
		}
	}

	logs.mu.Lock()
	records := append([]capturedRunnerRecoveryLog(nil), logs.records...)
	logs.mu.Unlock()
	if len(records) != 1 {
		t.Fatalf("recovery logs = %#v, want exactly one change log", records)
	}
	if records[0].level != slog.LevelInfo || records[0].message != "recovered expired runner tasks" ||
		records[0].attrs["requeued"] != int64(2) || records[0].attrs["terminal"] != int64(3) || len(records[0].attrs) != 2 {
		t.Fatalf("recovery log = %#v", records[0])
	}
}
