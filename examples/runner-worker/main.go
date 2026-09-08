// runner-worker is a copyable private-runner worker. It deliberately logs no
// task payload, result, idempotency key, or trace carrier.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

type claim struct {
	ID              string `json:"id"`
	Capability      string `json:"capability"`
	Generation      int64  `json:"generation"`
	CancelRequested bool   `json:"cancel_requested"`
}
type worker struct {
	base, token  string
	capabilities []string
	client       *http.Client
}

func main() {
	base, token := strings.TrimRight(os.Getenv("HARNESS_RUNNER_URL"), "/"), os.Getenv("HARNESS_RUNNER_TOKEN")
	if base == "" || token == "" {
		panic("HARNESS_RUNNER_URL and HARNESS_RUNNER_TOKEN are required")
	}
	w := worker{base: base, token: token, capabilities: strings.FieldsFunc(os.Getenv("HARNESS_RUNNER_CAPABILITIES"), func(r rune) bool { return r == ',' }), client: &http.Client{Timeout: 15 * time.Second}}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	for delay := time.Second; ctx.Err() == nil; delay = min(delay*2, 15*time.Second) {
		got, err := w.claim(ctx)
		if err != nil {
			if err := waitForRetry(ctx, delay); err != nil {
				return
			}
			continue
		}
		delay = time.Second
		if got == nil {
			if err := waitForRetry(ctx, time.Second); err != nil {
				return
			}
			continue
		}
		if got.CancelRequested {
			_ = w.complete(ctx, *got, false, "cancelled before execution")
			continue
		}
		// Replace this deterministic safe stub with a capability-specific executor.
		if err := w.renew(ctx, *got); err != nil {
			continue
		}
		if err := w.complete(ctx, *got, true, fmt.Sprintf("completed %s", got.Capability)); err != nil {
			continue
		}
	}
}
func min(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

// waitForRetry keeps the worker's bounded idle/backoff cadence while allowing
// signal cancellation to stop an otherwise idle worker promptly.
func waitForRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
func (w worker) claim(ctx context.Context) (*claim, error) {
	var out claim
	status, err := w.post(ctx, "/v1/runners/claim", map[string]any{"capabilities": w.capabilities}, &out)
	if err != nil {
		return nil, err
	}
	if status == http.StatusNoContent {
		return nil, nil
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("claim status %d", status)
	}
	return &out, nil
}
func (w worker) renew(ctx context.Context, task claim) error {
	status, err := w.post(ctx, "/v1/runners/tasks/"+task.ID+"/renew", map[string]any{"generation": task.Generation}, nil)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("renew status %d", status)
	}
	return nil
}
func (w worker) complete(ctx context.Context, task claim, ok bool, content string) error {
	status, err := w.post(ctx, "/v1/runners/tasks/"+task.ID+"/complete", map[string]any{"generation": task.Generation, "ok": ok, "content": content}, nil)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("complete status %d", status)
	}
	return nil
}
func (w worker) post(ctx context.Context, path string, body any, out any) (int, error) {
	raw, err := json.Marshal(body)
	if err != nil {
		return 0, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.base+path, bytes.NewReader(raw))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Authorization", "Bearer "+w.token)
	req.Header.Set("Content-Type", "application/json")
	res, err := w.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer res.Body.Close()
	if out != nil && res.StatusCode == http.StatusOK {
		err = json.NewDecoder(res.Body).Decode(out)
	}
	return res.StatusCode, err
}
