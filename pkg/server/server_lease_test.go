package server

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	core "github.com/whhhh1500/auto-agent/pkg/core"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
)

type leaseResult struct {
	renewed bool
	err     error
}

type fakeSessionLeaser struct {
	mu sync.Mutex

	acquireResult  bool
	acquireErr     error
	acquireCalls   []leaseCall
	renewCalls     chan leaseCall
	renewResults   chan leaseResult
	releaseCalls   []leaseCall
	releaseBounded []bool
}

type leaseCall struct {
	sessionID string
	holder    string
	ttl       time.Duration
}

func (f *fakeSessionLeaser) AcquireSessionLease(_ context.Context, sessionID, holder string, ttl time.Duration) (bool, error) {
	f.mu.Lock()
	f.acquireCalls = append(f.acquireCalls, leaseCall{sessionID: sessionID, holder: holder, ttl: ttl})
	f.mu.Unlock()
	return f.acquireResult, f.acquireErr
}

func (f *fakeSessionLeaser) RenewSessionLease(ctx context.Context, sessionID, holder string, ttl time.Duration) (bool, error) {
	call := leaseCall{sessionID: sessionID, holder: holder, ttl: ttl}
	select {
	case f.renewCalls <- call:
	case <-ctx.Done():
		return false, ctx.Err()
	}
	select {
	case result := <-f.renewResults:
		return result.renewed, result.err
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

func (f *fakeSessionLeaser) ReleaseSessionLease(ctx context.Context, sessionID, holder string) error {
	_, bounded := ctx.Deadline()
	f.mu.Lock()
	f.releaseCalls = append(f.releaseCalls, leaseCall{sessionID: sessionID, holder: holder})
	f.releaseBounded = append(f.releaseBounded, bounded)
	f.mu.Unlock()
	return nil
}

func newLeaseTestServer(t *testing.T, leaser *fakeSessionLeaser) *Server {
	t.Helper()
	server, err := New(Config{
		Runtime:       &core.Runtime{},
		Sessions:      core.NewMemorySessionStore(),
		Authenticator: AuthenticatorFunc(func(*http.Request) (core.Principal, error) { return core.Principal{}, nil }),
		Leaser:        leaser,
		LeaseTTL:      9 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	return server
}

func waitForRenewCall(t *testing.T, server *Server, leaser *fakeSessionLeaser, clock *manualRunLivenessClock, phase string) leaseCall {
	t.Helper()
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	select {
	case call := <-leaser.renewCalls:
		return call
	case <-timer.C:
		leaser.mu.Lock()
		acquires := len(leaser.acquireCalls)
		renews := len(leaser.renewCalls)
		releases := len(leaser.releaseCalls)
		leaser.mu.Unlock()
		clock.mu.Lock()
		activeTimers := 0
		for _, timer := range clock.timers {
			if timer.active {
				activeTimers++
			}
		}
		now := clock.now
		timerCount := len(clock.timers)
		clock.mu.Unlock()
		t.Fatalf("timed out waiting for %s renewal: acquires=%d renew_channel_len=%d/%d releases=%d clock_now=%s timers=%d active_timers=%d scheduler=%T", phase, acquires, renews, cap(leaser.renewCalls), releases, now.Format(time.RFC3339Nano), timerCount, activeTimers, server.liveness)
		return leaseCall{}
	}
}

func TestServerLeaseIdentityIsStableRandomUUIDAndRunScoped(t *testing.T) {
	leaserA := &fakeSessionLeaser{}
	leaserB := &fakeSessionLeaser{}
	serverA := newLeaseTestServer(t, leaserA)
	serverB := newLeaseTestServer(t, leaserB)

	uuidV4 := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	if !uuidV4.MatchString(serverA.instanceID) || !uuidV4.MatchString(serverB.instanceID) {
		t.Fatalf("server instance IDs are not UUIDv4 values: %q %q", serverA.instanceID, serverB.instanceID)
	}
	if serverA.instanceID == serverB.instanceID {
		t.Fatalf("distinct server instances reused ID %q", serverA.instanceID)
	}

	runID := "run_one"
	holderOne, err := serverA.leaseHolder(runID, 7)
	if err != nil {
		t.Fatal(err)
	}
	holderTwo, err := serverA.leaseHolder(runID, 7)
	if err != nil {
		t.Fatal(err)
	}
	runDigest := sha256.Sum256([]byte(runID))
	wantPrefix := serverA.instanceID + ":r" + fmt.Sprintf("%x", runDigest[:16]) + ":g7:"
	if holderOne == holderTwo || !strings.HasPrefix(holderOne, wantPrefix) || !strings.HasPrefix(holderTwo, wantPrefix) {
		t.Fatalf("lease holders must bind instance/run/generation with a unique acquisition nonce: %q %q", holderOne, holderTwo)
	}
	if strings.Contains(holderOne, runID) || strings.Contains(holderTwo, runID) {
		t.Fatalf("lease holder exposes raw run ID: %q %q", holderOne, holderTwo)
	}
	for _, holder := range []string{holderOne, holderTwo} {
		parts := strings.Split(holder, ":")
		if len(parts) != 4 || !uuidV4.MatchString(parts[3]) {
			t.Fatalf("lease holder has no UUIDv4 acquisition nonce: %q", holder)
		}
	}
}

func TestServerLeaseHolderBoundsAndValidatesIdentity(t *testing.T) {
	server := newLeaseTestServer(t, &fakeSessionLeaser{})
	maxRunID := strings.Repeat("r", 128)
	holder, err := server.leaseHolder(maxRunID, 9223372036854775807)
	if err != nil {
		t.Fatalf("max-length valid run ID rejected: %v", err)
	}
	if len(holder) > 128 {
		t.Fatalf("lease holder length=%d exceeds SQL holder limit: %q", len(holder), holder)
	}
	for _, input := range []struct {
		runID      string
		generation int64
	}{
		{runID: "invalid run id", generation: 0},
		{runID: "run-valid", generation: -1},
	} {
		if _, err := server.leaseHolder(input.runID, input.generation); err == nil {
			t.Fatalf("invalid holder input run=%q generation=%d was accepted", input.runID, input.generation)
		}
	}
}

func TestRunLeaseRenewsAndCancelsRunWhenOwnershipIsLost(t *testing.T) {
	leaser := &fakeSessionLeaser{
		acquireResult: true,
		renewCalls:    make(chan leaseCall, 2),
		renewResults:  make(chan leaseResult, 2),
	}
	server := newLeaseTestServer(t, leaser)
	clock := replaceServerLiveness(t, server)
	server.leaseOpTimeout = time.Second

	runCtx, cancelRun := context.WithCancel(context.Background())
	lease, acquired, err := server.acquireRunLease(runCtx, cancelRun, "session-1", "run-1", 11)
	if err != nil || !acquired {
		t.Fatalf("acquire run lease: acquired=%v err=%v", acquired, err)
	}

	clock.waitTimerAt(t, 3*time.Second)
	leaser.renewResults <- leaseResult{renewed: true}
	clock.Advance(3 * time.Second)
	firstRenew := waitForRenewCall(t, server, leaser, clock, "successful")
	if firstRenew.holder != lease.Holder() || firstRenew.sessionID != "session-1" {
		t.Fatalf("renew used wrong lease identity: %#v", firstRenew)
	}
	select {
	case <-runCtx.Done():
		t.Fatal("successful renewal cancelled the run")
	default:
	}

	clock.waitTimerAt(t, 3*time.Second)
	leaser.renewResults <- leaseResult{renewed: false}
	clock.Advance(3 * time.Second)
	waitForRenewCall(t, server, leaser, clock, "ownership-loss")
	select {
	case <-runCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("lease ownership loss did not cancel the run")
	}

	lease.Release()

	leaser.mu.Lock()
	defer leaser.mu.Unlock()
	if len(leaser.acquireCalls) != 1 || leaser.acquireCalls[0].holder != lease.Holder() || !strings.HasPrefix(lease.Holder(), server.instanceID+":r") || !strings.Contains(lease.Holder(), ":g11:") {
		t.Fatalf("acquire used wrong holder: %#v", leaser.acquireCalls)
	}
	if len(leaser.releaseCalls) != 1 || leaser.releaseCalls[0].holder != lease.Holder() {
		t.Fatalf("release used wrong holder: %#v", leaser.releaseCalls)
	}
	if len(leaser.releaseBounded) != 1 || !leaser.releaseBounded[0] {
		t.Fatalf("release context was not bounded: %#v", leaser.releaseBounded)
	}
}

func TestRunLeaseRenewalErrorCancelsRun(t *testing.T) {
	leaser := &fakeSessionLeaser{
		acquireResult: true,
		renewCalls:    make(chan leaseCall, 1),
		renewResults:  make(chan leaseResult, 1),
	}
	server := newLeaseTestServer(t, leaser)
	clock := replaceServerLiveness(t, server)

	runCtx, cancelRun := context.WithCancel(context.Background())
	lease, acquired, err := server.acquireRunLease(runCtx, cancelRun, "session-2", "run-2", 0)
	if err != nil || !acquired {
		t.Fatalf("acquire run lease: acquired=%v err=%v", acquired, err)
	}
	clock.waitTimerAt(t, 3*time.Second)
	leaser.renewResults <- leaseResult{err: errors.New("database unavailable")}
	clock.Advance(3 * time.Second)
	waitForRenewCall(t, server, leaser, clock, "renewal-error")
	select {
	case <-runCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("renewal error did not cancel the run")
	}
	lease.Release()
}

func TestRunLeaseConflictDoesNotStartRenewal(t *testing.T) {
	leaser := &fakeSessionLeaser{acquireResult: false}
	server := newLeaseTestServer(t, leaser)

	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	lease, acquired, err := server.acquireRunLease(runCtx, cancelRun, "session-3", "run-3", 0)
	if err != nil || acquired {
		t.Fatalf("conflict result: acquired=%v err=%v", acquired, err)
	}
	lease.Release()
	if holder := lease.Holder(); !strings.HasPrefix(holder, server.instanceID+":r") || !strings.Contains(holder, ":g0:") {
		t.Fatalf("malformed lease holder %q", holder)
	}
}

func TestRunLeaseReleasesAnomalousSuccessfulAcquireError(t *testing.T) {
	leaser := &fakeSessionLeaser{acquireResult: true, acquireErr: errors.New("acquire response lost")}
	server := newLeaseTestServer(t, leaser)
	lease, acquired, err := server.acquireRunLease(context.Background(), func() {}, "session-4", "run-4", 2)
	if err == nil || acquired {
		t.Fatalf("anomalous acquire result: acquired=%t err=%v", acquired, err)
	}
	if lease.Holder() == "" {
		t.Fatal("acquire cleanup lost the generated holder")
	}
	leaser.mu.Lock()
	defer leaser.mu.Unlock()
	if len(leaser.acquireCalls) != 1 || len(leaser.releaseCalls) != 1 || leaser.releaseCalls[0].holder != lease.Holder() {
		t.Fatalf("anomalous acquire was not released: acquires=%#v releases=%#v", leaser.acquireCalls, leaser.releaseCalls)
	}
	if !leaser.releaseBounded[0] {
		t.Fatal("anomalous acquire release did not use a bounded context")
	}
}

func TestRunLeaseReleasesWhenRenewalRegistrationIsUnavailable(t *testing.T) {
	leaser := &fakeSessionLeaser{acquireResult: true}
	server := newLeaseTestServer(t, leaser)
	server.liveness = nil
	lease, acquired, err := server.acquireRunLease(context.Background(), func() {}, "session-5", "run-5", 3)
	if err == nil || acquired {
		t.Fatalf("missing scheduler result: acquired=%t err=%v", acquired, err)
	}
	leaser.mu.Lock()
	defer leaser.mu.Unlock()
	if len(leaser.acquireCalls) != 1 || len(leaser.releaseCalls) != 1 || leaser.releaseCalls[0].holder != lease.Holder() {
		t.Fatalf("lease was not released after registration failure: acquires=%#v releases=%#v", leaser.acquireCalls, leaser.releaseCalls)
	}
}
