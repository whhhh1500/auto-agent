// Package runliveness schedules bounded liveness callbacks for active runs.
// It owns one deadline timer and a small worker pool rather than one ticker
// and goroutine per run. Callbacks must honor their context and perform their
// own operation-level timeouts around storage calls.
package runliveness

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

const (
	DefaultWorkers = 4
	MaxWorkers     = 32
	maxKeyBytes    = 512
)

var (
	ErrClosed       = errors.New("run liveness scheduler is closed")
	ErrDuplicateKey = errors.New("run liveness task key is already registered")
)

type Timer interface {
	C() <-chan time.Time
	Stop() bool
	Reset(time.Duration) bool
}

type Clock interface {
	Now() time.Time
	NewTimer(time.Duration) Timer
}

// Callback performs one bounded liveness operation. A non-nil error, or a
// panic, permanently deactivates the task and invokes its failure hook. The
// hook must move the owning run into its fail-safe state (normally cancel).
type Callback func(context.Context) error

// Failure is invoked once when Callback fails or panics. It must not block and
// must not expose callback error or panic values.
type Failure func()

type Config struct {
	Clock   Clock
	Workers int
}

type Scheduler struct {
	clock Clock

	mu      sync.Mutex
	tasks   map[string]*task
	heap    taskHeap
	closed  bool
	wake    chan struct{}
	stop    chan struct{}
	done    chan struct{}
	work    chan *task
	finish  chan *task
	workers sync.WaitGroup
}

type task struct {
	key       string
	interval  time.Duration
	next      time.Time
	callback  Callback
	ctx       context.Context
	cancel    context.CancelFunc
	active    bool
	running   bool
	index     int
	done      chan struct{}
	failed    bool
	onFailure Failure
}

type taskHeap []*task

func (h taskHeap) Len() int { return len(h) }
func (h taskHeap) Less(i, j int) bool {
	if h[i].next.Equal(h[j].next) {
		return h[i].key < h[j].key
	}
	return h[i].next.Before(h[j].next)
}
func (h taskHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i]; h[i].index, h[j].index = i, j }
func (h *taskHeap) Push(value any) {
	item := value.(*task)
	item.index = len(*h)
	*h = append(*h, item)
}
func (h *taskHeap) Pop() any {
	old := *h
	item := old[len(old)-1]
	old[len(old)-1] = nil
	item.index = -1
	*h = old[:len(old)-1]
	return item
}

type realClock struct{}
type realTimer struct{ *time.Timer }

func (realClock) Now() time.Time { return time.Now() }
func (realClock) NewTimer(duration time.Duration) Timer {
	return realTimer{Timer: time.NewTimer(duration)}
}
func (t realTimer) C() <-chan time.Time { return t.Timer.C }

func New(config Config) (*Scheduler, error) {
	if config.Clock == nil {
		config.Clock = realClock{}
	}
	if config.Workers == 0 {
		config.Workers = DefaultWorkers
	}
	if config.Workers < 1 || config.Workers > MaxWorkers {
		return nil, fmt.Errorf("run liveness workers must be between 1 and %d", MaxWorkers)
	}
	s := &Scheduler{clock: config.Clock, tasks: make(map[string]*task), wake: make(chan struct{}, 1), stop: make(chan struct{}), done: make(chan struct{}), work: make(chan *task, config.Workers), finish: make(chan *task, config.Workers)}
	for index := 0; index < config.Workers; index++ {
		s.workers.Add(1)
		go s.worker()
	}
	go s.loop()
	return s, nil
}

// Register adds one periodic callback. Duplicate keys are rejected rather
// than silently replacing a live lease/cancel monitor. The returned stop
// function is idempotent, cancels any in-flight callback, and waits for it so
// callers can safely release the resource the callback renews.
func (s *Scheduler) Register(parent context.Context, key string, interval time.Duration, callback Callback, onFailure Failure) (func(), error) {
	if s == nil || parent == nil || parent.Err() != nil || key == "" || len(key) > maxKeyBytes || interval <= 0 || callback == nil {
		return nil, fmt.Errorf("invalid run liveness registration")
	}
	ctx, cancel := context.WithCancel(parent)
	item := &task{key: key, interval: interval, callback: callback, ctx: ctx, cancel: cancel, active: true, index: -1}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		cancel()
		return nil, ErrClosed
	}
	if _, exists := s.tasks[key]; exists {
		s.mu.Unlock()
		cancel()
		return nil, ErrDuplicateKey
	}
	item.next = s.clock.Now().Add(interval)
	item.onFailure = onFailure
	s.tasks[key] = item
	heap.Push(&s.heap, item)
	s.mu.Unlock()
	s.signal()
	var once sync.Once
	return func() { once.Do(func() { s.unregister(item) }) }, nil
}

func (s *Scheduler) unregister(item *task) {
	s.mu.Lock()
	if !item.active {
		s.mu.Unlock()
		return
	}
	item.active = false
	delete(s.tasks, item.key)
	if item.index >= 0 {
		heap.Remove(&s.heap, item.index)
	}
	running := item.running
	done := item.done
	item.cancel()
	s.mu.Unlock()
	s.signal()
	if running {
		if done != nil {
			<-done
		}
	}
}

func (s *Scheduler) signal() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *Scheduler) loop() {
	defer func() {
		s.closeWork()
		s.workers.Wait()
		close(s.done)
	}()
	var timer Timer
	var timerDeadline time.Time
	saturated := false
	for {
		deadline, hasTask := s.next()
		if !hasTask {
			if timer != nil {
				stopTimer(timer)
				timer = nil
				timerDeadline = time.Time{}
			}
			select {
			case <-s.stop:
				return
			case <-s.wake:
			case item := <-s.finish:
				s.complete(item)
			}
			continue
		}
		if saturated {
			select {
			case <-s.stop:
				return
			case <-s.wake:
				saturated = false
			case item := <-s.finish:
				s.complete(item)
				saturated = false
			}
			continue
		}
		if timer == nil {
			timer = s.armTimer(nil, deadline)
			timerDeadline = deadline
		} else if !timerDeadline.Equal(deadline) {
			// A wake can be left over from Register after this deadline was
			// already armed. Resetting that timer with an old relative duration
			// can postpone an elapsed deadline by a full interval, so only rearm
			// when the earliest absolute deadline actually changed.
			timer = s.armTimer(timer, deadline)
			timerDeadline = deadline
		}
		select {
		case <-s.stop:
			if timer != nil {
				stopTimer(timer)
			}
			return
		case <-s.wake:
		case item := <-s.finish:
			s.complete(item)
		case <-timer.C():
			timerDeadline = time.Time{}
			saturated = s.dispatchDue(s.clock.Now())
		}
	}
}

func stopTimer(timer Timer) {
	if timer == nil || timer.Stop() {
		return
	}
	select {
	case <-timer.C():
	default:
	}
}

func (s *Scheduler) timerWait(deadline time.Time) time.Duration {
	wait := deadline.Sub(s.clock.Now())
	if wait < 0 {
		return 0
	}
	return wait
}

func (s *Scheduler) armTimer(timer Timer, deadline time.Time) Timer {
	wait := s.timerWait(deadline)
	if timer == nil {
		timer = s.clock.NewTimer(wait)
	} else {
		stopTimer(timer)
		timer.Reset(wait)
	}
	// A clock can pass deadline between calculating a relative wait and arming
	// the timer. Recheck immediately so a manual clock advance, or a long
	// scheduling pause, cannot defer an already elapsed deadline by wait.
	if wait > 0 && !s.clock.Now().Before(deadline) {
		stopTimer(timer)
		timer.Reset(0)
	}
	return timer
}

func (s *Scheduler) next() (time.Time, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.heap) == 0 {
		return time.Time{}, false
	}
	return s.heap[0].next, true
}

func (s *Scheduler) dispatchDue(now time.Time) bool {
	for {
		s.mu.Lock()
		if len(s.heap) == 0 || s.heap[0].next.After(now) {
			s.mu.Unlock()
			return false
		}
		item := s.heap[0]
		if !item.active || item.running || item.ctx.Err() != nil {
			heap.Pop(&s.heap)
			if item.active && item.ctx.Err() != nil {
				item.active = false
				delete(s.tasks, item.key)
			}
			s.mu.Unlock()
			continue
		}
		if len(s.work) == cap(s.work) {
			s.mu.Unlock()
			return true
		}
		heap.Pop(&s.heap)
		item.running = true
		item.done = make(chan struct{})
		s.mu.Unlock()
		s.work <- item
	}
}

func (s *Scheduler) worker() {
	defer s.workers.Done()
	for item := range s.work {
		failed := false
		if item.ctx.Err() == nil {
			func() {
				defer func() {
					if recover() != nil {
						failed = true
					}
				}()
				if item.callback(item.ctx) != nil {
					failed = true
				}
			}()
		}
		if failed {
			s.mu.Lock()
			item.failed = true
			s.mu.Unlock()
			if item.onFailure != nil {
				func() {
					defer func() { _ = recover() }()
					item.onFailure()
				}()
			}
		}
		close(item.done)
		select {
		case s.finish <- item:
		case <-s.stop:
		}
	}
}

func (s *Scheduler) complete(item *task) {
	s.mu.Lock()
	defer s.mu.Unlock()
	item.running = false
	if item.failed || !item.active || item.ctx.Err() != nil || s.closed {
		item.active = false
		delete(s.tasks, item.key)
		return
	}
	item.next = s.clock.Now().Add(item.interval)
	heap.Push(&s.heap, item)
}

func (s *Scheduler) closeWork() {
	s.mu.Lock()
	for _, item := range s.tasks {
		item.active = false
		item.cancel()
	}
	s.tasks = map[string]*task{}
	s.heap = nil
	s.mu.Unlock()
	close(s.work)
}

// Close prevents new registrations, cancels all callbacks, and waits for the
// scheduler and its bounded workers to exit. It is safe to call more than
// once and does not start any shutdown goroutines.
func (s *Scheduler) Close(ctx context.Context) error {
	if s == nil {
		return nil
	}
	if ctx == nil {
		return fmt.Errorf("run liveness shutdown context is nil")
	}
	s.mu.Lock()
	if !s.closed {
		s.closed = true
		close(s.stop)
	}
	s.mu.Unlock()
	select {
	case <-s.done:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
