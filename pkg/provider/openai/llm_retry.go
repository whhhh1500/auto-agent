package openai

import (
	"context"
	"errors"
	"fmt"
	core "github.com/whhhh1500/auto-agent/pkg/core"
	"time"
)

// RetryLlmAdapter wraps another adapter with bounded retries for transient
// failures: HTTP-send transport candidates and retryable HTTP statuses (429,
// 5xx). Cancellations, deadlines, protocol failures, and non-retryable statuses
// surface immediately. A retry can repeat a provider request, so it neither
// proves non-execution nor makes failed-attempt billing observable; it only
// prevents duplicate delivery after the consumer has received a chunk.
type RetryLlmAdapter struct {
	Next       core.LlmAdapter
	MaxRetries int
	// Backoff is the delay before retry n (1-based); defaults to 500ms * n.
	Backoff func(attempt int) time.Duration
}

const (
	HardMaxRetries = 10
	HardMaxBackoff = 5 * time.Minute
)

func (a *RetryLlmAdapter) Provider() string {
	if a == nil || a.Next == nil {
		return "retry-unconfigured"
	}
	return a.Next.Provider()
}

func (a *RetryLlmAdapter) ArtifactRevision() (revision string) {
	defer func() {
		if recover() != nil {
			revision = "retry/v2/revision-unavailable"
		}
	}()
	if a == nil || a.Next == nil {
		return "retry/unconfigured"
	}
	if revisioner, ok := a.Next.(core.ArtifactRevisioner); ok {
		return fmt.Sprintf("retry/v2/%s", revisioner.ArtifactRevision())
	}
	return "retry/v2/" + a.Next.Provider()
}

func (a *RetryLlmAdapter) Stream(ctx context.Context, opts core.GenerateOptions, emit func(core.StreamChunk)) error {
	if a == nil || a.Next == nil {
		return fmt.Errorf("retry adapter requires a next model adapter")
	}
	if a.MaxRetries < 0 {
		return fmt.Errorf("retry adapter max retries must not be negative")
	}
	if a.MaxRetries > HardMaxRetries {
		return fmt.Errorf("retry adapter max retries exceeds hard limit %d", HardMaxRetries)
	}
	var lastErr error
	for attempt := 0; attempt <= a.MaxRetries; attempt++ {
		if attempt > 0 {
			delay := time.Duration(0)
			if a.Backoff != nil {
				delay = a.Backoff(attempt)
			} else {
				delay = 500 * time.Millisecond * time.Duration(attempt)
			}
			if delay < 0 {
				return fmt.Errorf("retry adapter backoff must not be negative")
			}
			if delay > HardMaxBackoff {
				return fmt.Errorf("retry adapter backoff exceeds hard limit %s", HardMaxBackoff)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
		}
		emitted := false
		wrappedEmit := func(chunk core.StreamChunk) {
			emitted = true
			emit(chunk)
		}
		err := a.Next.Stream(ctx, opts, wrappedEmit)
		if err == nil {
			return nil
		}
		// Once bytes reached the consumer, retrying would duplicate output.
		if emitted || !retryableLlmError(err) {
			return err
		}
		lastErr = err
	}
	return lastErr
}

func retryableLlmError(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var status *HTTPStatusError
	if errors.As(err, &status) {
		return status.Retryable()
	}
	var retryable interface{ Retryable() bool }
	return errors.As(err, &retryable) && retryable.Retryable()
}
