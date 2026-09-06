package sandbox

import (
	"context"
	"sync"
	"time"
)

// processLifecycle contains the provider-independent ordering contract around
// an external process. A run is registered before Start so Close can cancel a
// not-yet-started command, but the process handle is published only after
// Start succeeds. This prevents Close from observing a nil Process and then
// missing a late-started child.
type processLifecycle struct {
	mu     sync.Mutex
	closed bool
	active *processRun
}

type processRun struct {
	cancel  func()
	process any
	done    chan struct{}
}

// localRunContext is kept as a small policy seam so the wall-time contract is
// testable without starting Linux-only bwrap machinery. context.WithTimeout
// naturally selects the earlier parent cancellation or local wall deadline.
func localRunContext(parent context.Context, wall time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, wall)
}

func (l *processLifecycle) begin(cancel func()) (*processRun, error) {
	if l == nil {
		return nil, ErrLeaseTerminated
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil, ErrLeaseTerminated
	}
	var cancelOnce sync.Once
	run := &processRun{
		cancel: func() {
			cancelOnce.Do(func() {
				if cancel != nil {
					cancel()
				}
			})
		},
		done: make(chan struct{}),
	}
	l.active = run
	return run, nil
}

// publish returns false when Close won the race with Start. The caller must
// terminate and wait for the just-started process before returning.
func (l *processLifecycle) publish(run *processRun, process any) bool {
	if l == nil || run == nil {
		return false
	}
	l.mu.Lock()
	if l.active != run || l.closed {
		l.mu.Unlock()
		if run.cancel != nil {
			run.cancel()
		}
		return false
	}
	run.process = process
	l.mu.Unlock()
	return true
}

func (l *processLifecycle) finish(run *processRun) bool {
	if l == nil || run == nil {
		return true
	}
	l.mu.Lock()
	closed := l.closed
	if l.active == run {
		l.active = nil
		close(run.done)
	}
	l.mu.Unlock()
	return closed
}

func (l *processLifecycle) isClosed() bool {
	if l == nil {
		return true
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.closed
}

func (l *processLifecycle) close(ctx context.Context, kill func(any)) error {
	if ctx == nil {
		return context.Canceled
	}
	if l == nil {
		return ErrLeaseTerminated
	}
	l.mu.Lock()
	l.closed = true
	run := l.active
	var process any
	if run != nil {
		process = run.process
	}
	l.mu.Unlock()

	if run != nil {
		if run.cancel != nil {
			run.cancel()
		}
		if process != nil && kill != nil {
			kill(process)
		}
		closeCtx, cancel := context.WithTimeout(ctx, sessionCloseTimeout)
		defer cancel()
		select {
		case <-run.done:
		case <-closeCtx.Done():
			return closeCtx.Err()
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}
