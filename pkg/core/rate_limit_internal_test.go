package core

import (
	"context"
	"testing"
	"time"
)

func TestGuardedFunnelEnforcesRateLimit(t *testing.T) {
	_, product, _, user := testScopes()
	principal := testPrincipal(user)
	registry := NewCapabilityRegistry()
	if err := registry.Register(product, staticTool{manifest: toolManifest("market.quote", "1.0.0"), content: "42"}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := (CapabilityResolver{Registry: registry}).Resolve(principal, user)
	if err != nil {
		t.Fatal(err)
	}
	session := mustSession(t, user, principal)
	limiter := NewWindowRateLimiter(1)
	agent, err := NewAgent(AgentOptions{LLM: MockLlmAdapter{}, Tools: snapshot, Session: session, RateLimiter: limiter})
	if err != nil {
		t.Fatal(err)
	}
	agent.runID = "run-a"
	first, _ := agent.tools.Execute(context.Background(), ToolCall{ID: "c1", Name: "market.quote"})
	second, _ := agent.tools.Execute(context.Background(), ToolCall{ID: "c2", Name: "market.quote"})
	if !first.OK {
		t.Fatalf("first call must pass: %#v", first)
	}
	if second.OK || second.Metadata["code"] != CodeRateLimited {
		t.Fatalf("second call must be rate limited: %#v", second)
	}
}

func TestWindowRateLimiterBoundsBuckets(t *testing.T) {
	limiter := NewWindowRateLimiter(10)
	limiter.maxBuckets = 2
	ctx := context.Background()
	if !limiter.AllowCall(ctx, "t1", "c") || !limiter.AllowCall(ctx, "t2", "c") {
		t.Fatal("first two identities must be admitted")
	}
	if limiter.AllowCall(ctx, "t3", "c") {
		t.Fatal("third identity was admitted past the bucket cap")
	}
	if !limiter.AllowCall(ctx, "t1", "c") {
		t.Fatal("existing identity was evicted by a new one")
	}
}

func TestWindowRateLimiterEvictsStaleBuckets(t *testing.T) {
	limiter := NewWindowRateLimiter(10)
	limiter.maxBuckets = 1
	current := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	limiter.now = func() time.Time { return current }
	ctx := context.Background()
	if !limiter.AllowCall(ctx, "old", "c") {
		t.Fatal("seed identity must be admitted")
	}
	current = current.Add(2 * time.Minute)
	if !limiter.AllowCall(ctx, "new", "c") {
		t.Fatal("stale bucket was not evicted")
	}
	if limiter.AllowCall(ctx, "old", "c") {
		t.Fatal("evicted identity remained in the limiter")
	}
}
