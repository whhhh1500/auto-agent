package runliveness

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"
)

type manualClock struct {
	mu      sync.Mutex
	now     time.Time
	timers  []*manualTimer
	changed chan struct{}
}

type manualTimer struct {
	clock  *manualClock
	at     time.Time
	active bool
	ch     chan time.Time
}

func newManualClock() *manualClock {
	return &manualClock{now: time.Unix(1, 0).UTC(), changed: make(chan struct{}, 32)}
}

func (c *manualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *manualClock) NewTimer(duration time.Duration) Timer {
	c.mu.Lock()
	timer := &manualTimer{clock: c, at: c.now.Add(duration), active: true, ch: make(chan time.Time, 1)}
	c.timers = append(c.timers, timer)
	c.mu.Unlock()
	c.signal()
	return timer
}

func (c *manualClock) Advance(duration time.Duration) {
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

func (c *manualClock) signal() {
	select {
	case c.changed <- struct{}{}:
	default:
	}
}

func (t *manualTimer) C() <-chan time.Time { return t.ch }

func (t *manualTimer) Stop() bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	wasActive := t.active
	t.active = false
	return wasActive
}

func (t *manualTimer) Reset(duration time.Duration) bool {
	t.clock.mu.Lock()
	wasActive := t.active
	t.at = t.clock.now.Add(duration)
	now := t.clock.now
	t.active = true
	t.clock.mu.Unlock()
	t.clock.signal()
	if duration <= 0 {
		select {
		case t.ch <- now:
		default:
		}
	}
	return wasActive
}

func (c *manualClock) waitTimerAt(t *testing.T, duration time.Duration) {
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
			t.Fatal("scheduler did not arm or reset its timer")
		}
	}
}

func newManualScheduler(t *testing.T, workers int) (*Scheduler, *manualClock) {
	t.Helper()
	clock := newManualClock()
	scheduler, err := New(Config{Clock: clock, Workers: workers})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := scheduler.Close(context.Background()); err != nil {
			t.Errorf("close scheduler: %v", err)
		}
	})
	return scheduler, clock
}

func TestSchedulerRunsOneCallbackPerKeyAndReschedules(t *testing.T) {
	scheduler, clock := newManualScheduler(t, 1)
	runs := make(chan struct{}, 2)
	stop, err := scheduler.Register(context.Background(), "cancel:run_1", time.Second, func(context.Context) error { runs <- struct{}{}; return nil }, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	clock.waitTimerAt(t, time.Second)
	clock.Advance(time.Second)
	select {
	case <-runs:
	case <-time.After(time.Second):
		t.Fatal("first callback did not run")
	}
	clock.waitTimerAt(t, time.Second)
	clock.Advance(time.Second)
	select {
	case <-runs:
	case <-time.After(time.Second):
		t.Fatal("second callback did not run")
	}
}

func TestSchedulerRejectsDuplicateAndUnregisterPreventsCallback(t *testing.T) {
	scheduler, clock := newManualScheduler(t, 1)
	runs := make(chan struct{}, 1)
	stop, err := scheduler.Register(context.Background(), "lease:run_1", time.Second, func(context.Context) error { runs <- struct{}{}; return nil }, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := scheduler.Register(context.Background(), "lease:run_1", time.Second, func(context.Context) error { return nil }, nil); err != ErrDuplicateKey {
		t.Fatalf("duplicate registration error = %v, want %v", err, ErrDuplicateKey)
	}
	stop()
	clock.Advance(10 * time.Second)
	select {
	case <-runs:
		t.Fatal("unregistered callback ran")
	case <-time.After(20 * time.Millisecond):
	}
}

func TestSchedulerSlowAndPanickingCallbacksDoNotBlockDeadlineLoop(t *testing.T) {
	scheduler, clock := newManualScheduler(t, 2)
	slowStarted := make(chan struct{}, 1)
	releaseSlow := make(chan struct{})
	fastRuns := make(chan struct{}, 1)
	for key, callback := range map[string]Callback{
		"slow": func(context.Context) error { slowStarted <- struct{}{}; <-releaseSlow; return nil },
		"fast": func(context.Context) error { fastRuns <- struct{}{}; return nil },
	} {
		if _, err := scheduler.Register(context.Background(), key, time.Second, callback, nil); err != nil {
			t.Fatal(err)
		}
	}
	clock.waitTimerAt(t, time.Second)
	clock.Advance(time.Second)
	select {
	case <-slowStarted:
	case <-time.After(time.Second):
		t.Fatal("slow callback did not begin")
	}
	select {
	case <-fastRuns:
	case <-time.After(time.Second):
		t.Fatal("slow callback blocked another due callback")
	}
	close(releaseSlow)
}

func TestSchedulerSaturationWaitsForWorkerThenContinues(t *testing.T) {
	scheduler, clock := newManualScheduler(t, 1)
	slowStarted := make(chan struct{}, 1)
	releaseSlow := make(chan struct{})
	laterRan := make(chan struct{}, 1)
	if _, err := scheduler.Register(context.Background(), "a_slow", time.Second, func(context.Context) error {
		slowStarted <- struct{}{}
		<-releaseSlow
		return nil
	}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := scheduler.Register(context.Background(), "b_queued", time.Second, func(context.Context) error { return nil }, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := scheduler.Register(context.Background(), "c_later", time.Second, func(context.Context) error {
		laterRan <- struct{}{}
		return nil
	}, nil); err != nil {
		t.Fatal(err)
	}
	clock.waitTimerAt(t, time.Second)
	clock.Advance(time.Second)
	select {
	case <-slowStarted:
	case <-time.After(time.Second):
		t.Fatal("slow callback did not start")
	}
	select {
	case <-laterRan:
		t.Fatal("scheduler re-entered a saturated work queue")
	default:
	}
	close(releaseSlow)
	select {
	case <-laterRan:
	case <-time.After(time.Second):
		t.Fatal("scheduler did not resume due work after worker became available")
	}
}

func TestSchedulerRecoversCallbackPanic(t *testing.T) {
	scheduler, clock := newManualScheduler(t, 1)
	panicRuns := make(chan struct{}, 1)
	failures := make(chan struct{}, 1)
	if _, err := scheduler.Register(context.Background(), "panic", time.Second, func(context.Context) error {
		panicRuns <- struct{}{}
		panic("plugin failure")
	}, func() { failures <- struct{}{} }); err != nil {
		t.Fatal(err)
	}
	clock.waitTimerAt(t, time.Second)
	clock.Advance(time.Second)
	select {
	case <-panicRuns:
	case <-time.After(time.Second):
		t.Fatal("panicking callback did not run")
	}
	select {
	case <-failures:
	case <-time.After(time.Second):
		t.Fatal("panic did not invoke failure hook")
	}
	clock.Advance(10 * time.Second)
	select {
	case <-panicRuns:
		t.Fatal("failed callback was rescheduled")
	case <-time.After(20 * time.Millisecond):
	}
}

func TestSchedulerCallbackErrorFailsOnceAndDoesNotReschedule(t *testing.T) {
	scheduler, clock := newManualScheduler(t, 1)
	runs := make(chan struct{}, 2)
	failures := make(chan struct{}, 2)
	if _, err := scheduler.Register(context.Background(), "error", time.Second, func(context.Context) error {
		runs <- struct{}{}
		return errors.New("storage unavailable")
	}, func() { failures <- struct{}{} }); err != nil {
		t.Fatal(err)
	}
	clock.waitTimerAt(t, time.Second)
	clock.Advance(time.Second)
	select {
	case <-runs:
	case <-time.After(time.Second):
		t.Fatal("error callback did not run")
	}
	select {
	case <-failures:
	case <-time.After(time.Second):
		t.Fatal("error callback did not invoke failure hook")
	}
	clock.Advance(10 * time.Second)
	select {
	case <-runs:
		t.Fatal("error callback was rescheduled")
	case <-time.After(20 * time.Millisecond):
	}
	select {
	case <-failures:
		t.Fatal("error callback invoked failure more than once")
	case <-time.After(20 * time.Millisecond):
	}
}

func TestSchedulerCloseCancelsInFlightCallback(t *testing.T) {
	scheduler, clock := newManualScheduler(t, 1)
	started := make(chan struct{}, 1)
	cancelled := make(chan struct{}, 1)
	if _, err := scheduler.Register(context.Background(), "lease:run_1", time.Second, func(ctx context.Context) error {
		started <- struct{}{}
		<-ctx.Done()
		cancelled <- struct{}{}
		return nil
	}, nil); err != nil {
		t.Fatal(err)
	}
	clock.waitTimerAt(t, time.Second)
	clock.Advance(time.Second)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("callback did not start")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := scheduler.Close(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("close did not cancel in-flight callback")
	}
}

func TestSchedulerKeepsOneTimerForThousandActiveTasks(t *testing.T) {
	scheduler, clock := newManualScheduler(t, DefaultWorkers)
	for index := 0; index < 1000; index++ {
		if _, err := scheduler.Register(context.Background(), "run:"+strconv.Itoa(index), time.Minute, func(context.Context) error { return nil }, nil); err != nil {
			t.Fatalf("register %d: %v", index, err)
		}
	}
	clock.waitTimerAt(t, time.Minute)
	clock.mu.Lock()
	timerCount := len(clock.timers)
	clock.mu.Unlock()
	if timerCount != 1 {
		t.Fatalf("timer count = %d, want one shared deadline timer", timerCount)
	}
}

func TestSchedulerAllowsMoreThanLegacyTaskLimit(t *testing.T) {
	scheduler, _ := newManualScheduler(t, DefaultWorkers)
	for index := 0; index < 5000; index++ {
		if _, err := scheduler.Register(context.Background(), "run:"+strconv.Itoa(index), time.Hour, func(context.Context) error { return nil }, nil); err != nil {
			t.Fatalf("registration %d was implicitly limited: %v", index, err)
		}
	}
}

func TestSchedulerDoesNotInvokeQueuedCallbackAfterParentCancellation(t *testing.T) {
	scheduler, clock := newManualScheduler(t, 1)
	parent, cancel := context.WithCancel(context.Background())
	runs := make(chan struct{}, 1)
	if _, err := scheduler.Register(parent, "cancelled", time.Second, func(context.Context) error {
		runs <- struct{}{}
		return nil
	}, nil); err != nil {
		t.Fatal(err)
	}
	cancel()
	clock.waitTimerAt(t, time.Second)
	clock.Advance(time.Second)
	select {
	case <-runs:
		t.Fatal("cancelled queued callback ran")
	case <-time.After(20 * time.Millisecond):
	}
}

func TestSchedulerCloseIsRepeatable(t *testing.T) {
	scheduler, _ := newManualScheduler(t, 1)
	for index := 0; index < 3; index++ {
		if err := scheduler.Close(context.Background()); err != nil {
			t.Fatalf("close %d: %v", index, err)
		}
	}
	if _, err := scheduler.Register(context.Background(), "after-close", time.Second, func(context.Context) error { return nil }, nil); err != ErrClosed {
		t.Fatalf("register after close = %v, want %v", err, ErrClosed)
	}
}

func BenchmarkSchedulerRegister1000Active(b *testing.B) {
	for index := 0; index < b.N; index++ {
		clock := newManualClock()
		scheduler, err := New(Config{Clock: clock, Workers: DefaultWorkers})
		if err != nil {
			b.Fatal(err)
		}
		for task := 0; task < 1000; task++ {
			key := "run:" + strconv.Itoa(task)
			if _, err := scheduler.Register(context.Background(), key, time.Minute, func(context.Context) error { return nil }, nil); err != nil {
				b.Fatal(err)
			}
		}
		if err := scheduler.Close(context.Background()); err != nil {
			b.Fatal(err)
		}
	}
}
