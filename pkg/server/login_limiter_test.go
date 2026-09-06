package server

import (
	"testing"
	"time"
)

func TestLoginLimiterDoesNotFlushLiveLockouts(t *testing.T) {
	limiter := newLoginLimiter()
	limiter.maxEntries = 2
	limiter.max = 2
	limiter.recordFailure("admin@x.com")
	limiter.recordFailure("admin@x.com")
	if !limiter.lockedOut("admin@x.com") {
		t.Fatal("admin was not locked out")
	}
	limiter.recordFailure("a@x.com")
	limiter.recordFailure("b@x.com")
	if !limiter.lockedOut("admin@x.com") {
		t.Fatal("lockout table was flushed to admit an unseen email")
	}
	if limiter.lockedOut("b@x.com") {
		t.Fatal("unseen email replaced a live lockout")
	}
}

func TestLoginLimiterEvictsUnlockedEmails(t *testing.T) {
	limiter := newLoginLimiter()
	limiter.maxEntries = 2
	limiter.max = 3
	current := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	limiter.now = func() time.Time { return current }
	limiter.recordFailure("old@x.com")
	current = current.Add(time.Second)
	limiter.recordFailure("kept@x.com")
	limiter.recordFailure("kept@x.com")
	current = current.Add(time.Second)
	limiter.recordFailure("new@x.com")
	if _, exists := limiter.failures["old@x.com"]; exists {
		t.Fatal("unlocked stale email should have been evicted")
	}
	if _, exists := limiter.failures["kept@x.com"]; !exists {
		t.Fatal("email with more recent failures was evicted")
	}
}

func TestLoginLimiterDropsExpiredAttempts(t *testing.T) {
	limiter := newLoginLimiter()
	limiter.window = 0
	limiter.recordFailure("a@x.com")
	if limiter.lockedOut("a@x.com") {
		t.Fatal("expired attempts still locked the email")
	}
	if _, exists := limiter.failures["a@x.com"]; exists {
		t.Fatal("expired email was retained")
	}
}
