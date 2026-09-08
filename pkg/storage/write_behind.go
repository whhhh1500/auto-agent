package storage

import (
	"context"
	"errors"
	"fmt"
	core "github.com/whhhh1500/auto-agent/pkg/core"
	"sync"
	"time"
)

// WriteBehind bounds the durability window of a live run: events land in the
// session immediately, and a background flusher persists a stable ordered
// prefix to the core.SessionStore at most one batching delay behind the producer.
// A crash loses at most maxDelay of events instead of the whole run, while
// the hot path never blocks on storage.
//
// Single-writer discipline applies: the caller must already serialize runs
// per session (the HTTP adapter holds a per-session lock). Conflicting
// external writers surface as ErrSessionConflict on Flush.
type WriteBehind struct {
	store              core.SessionStore
	fencedAppender     FencedSessionAppender
	fence              SessionWriteFence
	session            *core.Session
	maxDelay           time.Duration
	backgroundDisabled bool

	mu            sync.Mutex
	savedVersion  int64
	dirty         bool
	timer         *time.Timer
	flushing      bool
	lastErr       error
	errorObserver func(error)
	closed        bool
	changed       chan struct{}
}

var backgroundFlushTimeout = 30 * time.Second

// NewWriteBehind starts batching for one run. startVersion is the store
// version the session was loaded at; everything after it is pending. A
// negative maxDelay disables background batching so only Flush writes.
func NewWriteBehind(store core.SessionStore, session *core.Session, startVersion int64, maxDelay time.Duration) *WriteBehind {
	backgroundDisabled := maxDelay < 0
	if maxDelay == 0 {
		maxDelay = 200 * time.Millisecond
	}
	return &WriteBehind{
		store: store, session: session, maxDelay: maxDelay,
		backgroundDisabled: backgroundDisabled,
		savedVersion:       startVersion,
		changed:            make(chan struct{}),
	}
}

// NewFencedWriteBehind creates a WriteBehind whose every non-empty incremental
// append is authorized by one durable queued-run fence. The store must
// implement the optional FencedSessionAppender contract; there is no
// best-effort fallback. A fence-loss error is retained as the writer's terminal
// persistence error: it stops future background scheduling, and Checkpoint,
// Flush, and Abort report it to the owner that must stop the stale worker.
func NewFencedWriteBehind(store core.SessionStore, fence SessionWriteFence, session *core.Session, startVersion int64, maxDelay time.Duration) (*WriteBehind, error) {
	if store == nil || session == nil {
		return nil, fmt.Errorf("fenced write-behind requires a store and session")
	}
	if err := validateSessionWriteFence(fence); err != nil {
		return nil, err
	}
	if session.ID() != fence.SessionID {
		return nil, fmt.Errorf("write-behind session %q does not match fenced session %q", session.ID(), fence.SessionID)
	}
	if startVersion < 0 {
		return nil, fmt.Errorf("fenced write-behind start version must not be negative")
	}
	if currentVersion := session.Version(); startVersion > currentVersion {
		return nil, fmt.Errorf("fenced write-behind start version %d exceeds session version %d", startVersion, currentVersion)
	}
	appender, ok := store.(FencedSessionAppender)
	if !ok {
		return nil, fmt.Errorf("session store %T does not implement fenced append", store)
	}
	writer := NewWriteBehind(store, session, startVersion, maxDelay)
	writer.fencedAppender = appender
	writer.fence = fence
	return writer, nil
}

// MarkDirty schedules a background flush within maxDelay. It is safe to call
// for every emitted event; scheduling is idempotent while a flush is pending.
func (w *WriteBehind) MarkDirty() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return
	}
	w.dirty = true
	w.scheduleLocked()
}

// Checkpoint persists everything pending synchronously without closing the
// writer. Later MarkDirty calls continue background batching.
func (w *WriteBehind) Checkpoint(ctx context.Context) error {
	return w.flush(ctx, false)
}

// SetErrorObserver installs one best-effort notification for the first
// persistence error retained by this writer. The callback runs after the
// writer releases its internal lock, so it may cancel the owning run or call
// other infrastructure without deadlocking the flusher. Replacing an observer
// after an error has already occurred immediately notifies the replacement.
//
// The observer is intentionally notification-only: Checkpoint, Flush, and
// Abort remain the authoritative ways to observe and return persistence errors.
func (w *WriteBehind) SetErrorObserver(observer func(error)) {
	if w == nil {
		return
	}
	w.mu.Lock()
	w.errorObserver = observer
	err := w.lastErr
	w.mu.Unlock()
	if observer != nil && err != nil {
		observer(err)
	}
}

// Flush persists everything pending synchronously and stops background
// flushing. It must be called before the run's transport response completes
// so a durable prefix is on disk before the client stops listening.
func (w *WriteBehind) Flush(ctx context.Context) error {
	return w.flush(ctx, true)
}

func (w *WriteBehind) flush(ctx context.Context, closeWriter bool) error {
	w.mu.Lock()
	if w.closed && !closeWriter {
		err := w.lastErr
		w.mu.Unlock()
		return err
	}
	if closeWriter {
		w.closed = true
	}
	if w.session != nil && w.session.Version() > w.savedVersion {
		w.dirty = true
	}
	if w.timer != nil {
		w.timer.Stop()
		w.timer = nil
	}
	for {
		if w.lastErr != nil {
			err := w.lastErr
			w.mu.Unlock()
			return err
		}
		if !w.dirty {
			w.mu.Unlock()
			return nil
		}
		if w.flushing {
			changed := w.changed
			w.mu.Unlock()
			select {
			case <-changed:
			case <-ctx.Done():
				return ctx.Err()
			}
			w.mu.Lock()
			continue
		}
		w.mu.Unlock()
		w.flushOnce(ctx)
		w.mu.Lock()
	}
}

// Abort stops future background flushes without persisting the remaining
// dirty suffix. It waits for an already-running flush to return so callers do
// not release a session lease while this writer can still mutate the store.
func (w *WriteBehind) Abort(ctx context.Context) error {
	w.mu.Lock()
	w.closed = true
	if w.timer != nil {
		w.timer.Stop()
		w.timer = nil
	}
	for w.flushing {
		changed := w.changed
		w.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
		w.mu.Lock()
	}
	err := w.lastErr
	w.mu.Unlock()
	return err
}

func (w *WriteBehind) flushOnce(ctx context.Context) {
	w.mu.Lock()
	if !w.dirty || w.flushing {
		w.mu.Unlock()
		return
	}
	w.flushing = true
	session, savedVersion := w.session, w.savedVersion
	w.mu.Unlock()

	events := session.EventsFrom(savedVersion)
	targetVersion := savedVersion + int64(len(events))
	var err error
	if w.fencedAppender != nil {
		err = w.fencedAppender.AppendEventsFenced(ctx, w.fence, savedVersion, events)
	} else if appender, ok := w.store.(core.SessionAppender); ok {
		err = appender.AppendEvents(ctx, session.ID(), savedVersion, events)
	} else {
		var snapshot *core.Session
		snapshot, err = session.Clone()
		if err == nil {
			targetVersion = snapshot.Version()
			err = w.store.Save(ctx, snapshot, savedVersion)
		}
	}

	var notify func(error)
	w.mu.Lock()
	w.flushing = false
	if err != nil {
		// Retain the failure: Flush reports it instead of silently dropping.
		if w.lastErr == nil {
			w.lastErr = err
			notify = w.errorObserver
		}
	} else {
		w.savedVersion = targetVersion
		w.dirty = session.Version() > targetVersion
		if w.timer != nil {
			w.timer.Stop()
			w.timer = nil
		}
		w.scheduleLocked()
	}
	w.signalChangedLocked()
	w.mu.Unlock()
	if notify != nil {
		notify(err)
	}
}

func (w *WriteBehind) scheduleLocked() {
	if w.backgroundDisabled || w.closed || !w.dirty || w.flushing || w.timer != nil || w.lastErr != nil {
		return
	}
	var timer *time.Timer
	timer = time.AfterFunc(w.maxDelay, func() {
		w.mu.Lock()
		if w.timer == timer {
			w.timer = nil
		}
		w.mu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), backgroundFlushTimeout)
		defer cancel()
		w.flushOnce(ctx)
	})
	w.timer = timer
}

func (w *WriteBehind) signalChangedLocked() {
	close(w.changed)
	w.changed = make(chan struct{})
}

// SavedVersion reports the version known to be durable.
func (w *WriteBehind) SavedVersion() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.savedVersion
}

// IsConflict reports whether an error from Flush is a persistence conflict
// rather than an infrastructure failure.
func IsConflict(err error) bool {
	return errors.Is(err, core.ErrSessionConflict)
}
