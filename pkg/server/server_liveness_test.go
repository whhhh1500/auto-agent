package server

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cc-auto-agent/harness-core/pkg/app/runliveness"
	"github.com/cc-auto-agent/harness-core/pkg/storage"
)

// manualRunLivenessClock makes server lifecycle tests drive the shared
// scheduler without wall-clock lease waits.
type manualRunLivenessClock struct {
	mu      sync.Mutex
	now     time.Time
	timers  []*manualRunLivenessTimer
	changed chan struct{}
}

type manualRunLivenessTimer struct {
	clock  *manualRunLivenessClock
	at     time.Time
	active bool
	ch     chan time.Time
}

// manualLeaseTicker remains the test clock for runner-recovery's independent
// maintenance loop. Run liveness itself uses manualRunLivenessClock above.
type manualLeaseTicker struct {
	ticks chan time.Time
	once  sync.Once
	stop  chan struct{}
}

func (t *manualLeaseTicker) C() <-chan time.Time { return t.ticks }
func (t *manualLeaseTicker) Stop()               { t.once.Do(func() { close(t.stop) }) }

func newManualRunLivenessClock() *manualRunLivenessClock {
	return &manualRunLivenessClock{now: time.Unix(1, 0).UTC(), changed: make(chan struct{}, 32)}
}

func (c *manualRunLivenessClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *manualRunLivenessClock) NewTimer(duration time.Duration) runliveness.Timer {
	c.mu.Lock()
	timer := &manualRunLivenessTimer{clock: c, at: c.now.Add(duration), active: true, ch: make(chan time.Time, 1)}
	c.timers = append(c.timers, timer)
	c.mu.Unlock()
	select {
	case c.changed <- struct{}{}:
	default:
	}
	return timer
}

func (c *manualRunLivenessClock) Advance(duration time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(duration)
	now := c.now
	for _, timer := range c.timers {
		if timer.active && !timer.at.After(now) {
			timer.active = false
			select {
			case timer.ch <- now:
			default:
			}
		}
	}
	c.mu.Unlock()
}

func (t *manualRunLivenessTimer) C() <-chan time.Time { return t.ch }

func (t *manualRunLivenessTimer) Stop() bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	wasActive := t.active
	t.active = false
	return wasActive
}

func (t *manualRunLivenessTimer) Reset(duration time.Duration) bool {
	t.clock.mu.Lock()
	wasActive := t.active
	t.at = t.clock.now.Add(duration)
	now := t.clock.now
	t.active = true
	t.clock.mu.Unlock()
	select {
	case t.clock.changed <- struct{}{}:
	default:
	}
	if duration <= 0 {
		select {
		case t.ch <- now:
		default:
		}
	}
	return wasActive
}

func (c *manualRunLivenessClock) waitTimerAt(t *testing.T, duration time.Duration) {
	t.Helper()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for {
		c.mu.Lock()
		expected := c.now.Add(duration)
		armed := false
		for _, timer := range c.timers {
			if timer.active && timer.at.Equal(expected) {
				armed = true
				break
			}
		}
		c.mu.Unlock()
		if armed {
			return
		}
		select {
		case <-c.changed:
		case <-deadline.C:
			t.Fatal("liveness scheduler did not arm or reset timer")
		}
	}
}

func replaceServerLiveness(t *testing.T, server *Server) *manualRunLivenessClock {
	t.Helper()
	if err := server.liveness.Close(context.Background()); err != nil {
		t.Fatalf("close default liveness scheduler: %v", err)
	}
	clock := newManualRunLivenessClock()
	scheduler, err := runliveness.New(runliveness.Config{Clock: clock, Workers: 2})
	if err != nil {
		t.Fatal(err)
	}
	server.liveness = scheduler
	t.Cleanup(func() {
		if err := scheduler.Close(context.Background()); err != nil {
			t.Errorf("close test liveness scheduler: %v", err)
		}
	})
	return clock
}

type observingRunQueue struct {
	storage.RunQueueStore
	renewed chan struct{}
}

func (q observingRunQueue) RenewRunClaim(ctx context.Context, runID, workerID string, generation int64, leaseTTL time.Duration) (bool, error) {
	select {
	case q.renewed <- struct{}{}:
	default:
	}
	return q.RunQueueStore.RenewRunClaim(ctx, runID, workerID, generation, leaseTTL)
}

func TestShutdownKeepsClaimRenewalAliveUntilDrainCompletes(t *testing.T) {
	fixture := newRunWorkerFixture(t)
	fixture.server.runWorkerClaimTTL = 3 * time.Millisecond
	queue := observingRunQueue{RunQueueStore: fixture.queue, renewed: make(chan struct{}, 2)}
	fixture.server.runQueue = queue
	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	claim := fixture.server.monitorQueuedRunClaim(runCtx, cancelRun, storage.QueuedRun{
		RunRecord: storage.RunRecord{RunID: "run-draining"}, Generation: 1,
	}, "worker-draining")
	defer claim.stop()
	drainDone := make(chan struct{})
	_, cancelActive := context.WithCancel(context.Background())
	defer cancelActive()
	fixture.server.workersMu.Lock()
	fixture.server.workersRunning = true
	fixture.server.workersStopClaims = func() {}
	fixture.server.workersCancelActive = cancelActive
	fixture.server.workersDone = drainDone
	fixture.server.workersMu.Unlock()
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- fixture.server.Shutdown(context.Background()) }()
	select {
	case <-queue.renewed:
	case <-time.After(time.Second):
		t.Fatalf("claim renewal stopped while active worker was draining: run=%v reason=%d", runCtx.Err(), claim.reason.Load())
	}
	close(drainDone)
	select {
	case err := <-shutdownDone:
		if err != nil {
			t.Fatalf("shutdown: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown did not finish after drain")
	}
	select {
	case <-queue.renewed:
		t.Fatal("claim renewal ran after scheduler shutdown")
	case <-time.After(20 * time.Millisecond):
	}
}
