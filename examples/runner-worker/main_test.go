package main

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestWaitForRetryReturnsPromptlyWhenCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- waitForRetry(ctx, time.Second) }()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("wait error = %v, want context cancellation", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("retry wait did not stop promptly after cancellation")
	}
}

func TestWaitForRetryCompletesConfiguredDelay(t *testing.T) {
	start := time.Now()
	if err := waitForRetry(context.Background(), 10*time.Millisecond); err != nil {
		t.Fatalf("wait error = %v", err)
	}
	if elapsed := time.Since(start); elapsed < 8*time.Millisecond {
		t.Fatalf("wait returned too early: %s", elapsed)
	}
}
