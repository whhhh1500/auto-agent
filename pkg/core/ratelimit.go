package core

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// AdjustBacktestCutoff moves a truncation point that would split a summary's
// shadow range back before the affected range. Without this, a backtest cut
// inside an archived range would include raw events the source projection
// had summarized away, making the replayed context diverge from the source.
// It iterates to a fixpoint because moving the cutoff earlier can expose
// further excluded summaries. A cutoff of 0 (or one that would strip the
// entire archived prefix) is rejected.
func AdjustBacktestCutoff(events []SessionEvent, cutoff int64) (int64, error) {
	if cutoff <= 0 || cutoff > int64(len(events)) {
		return int64(len(events)), nil
	}
	newCutoff := cutoff
	for changed := true; changed; {
		changed = false
		for _, event := range events {
			if event.Type != EvContextSummary || event.Seq < newCutoff {
				continue // summary inside the copied prefix: fine
			}
			var data ContextSummaryData
			if err := json.Unmarshal(event.Data, &data); err != nil {
				continue
			}
			// The excluded summary shadows events inside the copy: move the
			// cutoff before its range so shadowing stays self-contained.
			if data.Start < newCutoff {
				newCutoff = data.Start
				changed = true
			}
		}
	}
	if newCutoff <= 0 {
		return 0, fmt.Errorf("cutoff would strip the summarized context prefix; no consistent truncation point exists")
	}
	return newCutoff, nil
}

// CallRateLimiter bounds how often one tenant may invoke one capability
// across runs — the cross-run complement to PerTurnBudget. Denials surface
// to the model as a stable-rate-limited refusal, not a run failure.
type CallRateLimiter interface {
	AllowCall(ctx context.Context, tenantID, capabilityID string) bool
}

// WindowRateLimiter is the in-process reference implementation: a fixed
// one-minute sliding count per (tenant, capability) pair. Multi-instance
// deployments enforce per-instance limits unless backed by shared state.
// The in-process map is bounded; unknown identities are denied once the
// cap is full of buckets still inside the active window.
type WindowRateLimiter struct {
	mu         sync.Mutex
	perMinute  int
	maxBuckets int
	now        func() time.Time
	buckets    map[string]*rateBucket
}

const maxWindowRateLimiterBuckets = 4096

type rateBucket struct {
	slots map[int64]int
	last  int64
}

// NewWindowRateLimiter allows at most perMinute calls per tenant per
// capability in any 60-second window.
func NewWindowRateLimiter(perMinute int) *WindowRateLimiter {
	if perMinute <= 0 {
		perMinute = 1
	}
	return &WindowRateLimiter{
		perMinute: perMinute, maxBuckets: maxWindowRateLimiterBuckets,
		buckets: map[string]*rateBucket{},
	}
}

func (w *WindowRateLimiter) clock() time.Time {
	if w != nil && w.now != nil {
		return w.now()
	}
	return time.Now().UTC()
}

func (w *WindowRateLimiter) AllowCall(_ context.Context, tenantID, capabilityID string) bool {
	slot := w.clock().Unix() / 60
	key := tenantID + "\x00" + capabilityID
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.maxBuckets <= 0 {
		w.maxBuckets = maxWindowRateLimiterBuckets
	}
	w.evictStaleLocked(slot)
	bucket := w.buckets[key]
	if bucket == nil {
		if len(w.buckets) >= w.maxBuckets {
			return false
		}
		bucket = &rateBucket{slots: map[int64]int{}}
		w.buckets[key] = bucket
	}
	// Lazy prune: drop slots older than the previous minute so the current
	// one-minute window still overlaps its predecessor.
	for existing := range bucket.slots {
		if existing < slot-1 {
			delete(bucket.slots, existing)
		}
	}
	if bucket.slots[slot] >= w.perMinute {
		return false
	}
	bucket.slots[slot]++
	bucket.last = slot
	return true
}

func (w *WindowRateLimiter) evictStaleLocked(slot int64) {
	for key, bucket := range w.buckets {
		if bucket.last < slot-1 {
			delete(w.buckets, key)
		}
	}
}
