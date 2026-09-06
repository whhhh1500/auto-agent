package server

import (
	"context"
	"errors"
	core "github.com/cc-auto-agent/harness-core/pkg/core"
	"net/http"
	"regexp"
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

	holderOne := serverA.leaseHolder("run_one")
	holderTwo := serverA.leaseHolder("run_two")
	if holderOne != serverA.instanceID+":run_one" || holderTwo != serverA.instanceID+":run_two" {
		t.Fatalf("run holders do not contain stable instance and run IDs: %q %q", holderOne, holderTwo)
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
	cleanup, acquired, err := server.acquireRunLease(runCtx, cancelRun, "session-1", "run-1")
	if err != nil || !acquired {
		t.Fatalf("acquire run lease: acquired=%v err=%v", acquired, err)
	}

	clock.waitTimerAt(t, 3*time.Second)
	leaser.renewResults <- leaseResult{renewed: true}
	clock.Advance(3 * time.Second)
	firstRenew := waitForRenewCall(t, server, leaser, clock, "successful")
	if firstRenew.holder != server.instanceID+":run-1" || firstRenew.sessionID != "session-1" {
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

	cleanup()

	leaser.mu.Lock()
	defer leaser.mu.Unlock()
	if len(leaser.acquireCalls) != 1 || leaser.acquireCalls[0].holder != server.instanceID+":run-1" {
		t.Fatalf("acquire used wrong holder: %#v", leaser.acquireCalls)
	}
	if len(leaser.releaseCalls) != 1 || leaser.releaseCalls[0].holder != server.instanceID+":run-1" {
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
	cleanup, acquired, err := server.acquireRunLease(runCtx, cancelRun, "session-2", "run-2")
	if err != nil || !acquired {
		t.Fatalf("acquire run lease: acquired=%v err=%v", acquired, err)
	}
	clock.waitTimerAt(t, 3*time.Second)
	leaser.renewResults <- leaseResult{err: errors.New("database unavailable")}
	clock.Advance(3 * time.Second)
	<-leaser.renewCalls
	select {
	case <-runCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("renewal error did not cancel the run")
	}
	cleanup()
}

func TestRunLeaseConflictDoesNotStartRenewal(t *testing.T) {
	leaser := &fakeSessionLeaser{acquireResult: false}
	server := newLeaseTestServer(t, leaser)

	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	cleanup, acquired, err := server.acquireRunLease(runCtx, cancelRun, "session-3", "run-3")
	if err != nil || acquired {
		t.Fatalf("conflict result: acquired=%v err=%v", acquired, err)
	}
	cleanup()
	if holder := server.leaseHolder("run-3"); holder != server.instanceID+":run-3" {
		t.Fatalf("malformed lease holder %q", holder)
	}
}
