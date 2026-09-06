package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	. "github.com/cc-auto-agent/harness-core/pkg/core"
	"github.com/cc-auto-agent/harness-core/pkg/execution"
	"os"
	"strings"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

// --- P0-1: settings encryption ---

func TestSettingsEncryptedAtRest(t *testing.T) {
	db, err := sql.Open("sqlite", t.TempDir()+"/enc.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	if _, err = OpenSQLSessionStore(ctx, db, SQLDialectSQLite); err != nil {
		t.Fatal(err)
	}
	masterKey, err := EnsureMasterKey(ctx, db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	accounts, err := NewSQLAccountStore(db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	accounts.Cipher, err = NewCipher(masterKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := accounts.SetSetting(ctx, "llm", `{"api_key":"sk-secret-123"}`); err != nil {
		t.Fatal(err)
	}

	// Raw row must not contain the plaintext secret.
	var raw string
	if err := db.QueryRow("SELECT value FROM settings WHERE key='llm'").Scan(&raw); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(raw, "sk-secret-123") {
		t.Fatalf("secret stored in plaintext: %s", raw)
	}
	if !strings.HasPrefix(raw, SecretPrefix) {
		t.Fatalf("value not sealed: %s", raw)
	}

	// Read path decrypts transparently.
	value, found, err := accounts.GetSetting(ctx, "llm")
	if err != nil || !found || !strings.Contains(value, "sk-secret-123") {
		t.Fatalf("decrypted read failed: %q %v %v", value, found, err)
	}

	// Master key is stable across calls.
	again, err := EnsureMasterKey(ctx, db, SQLDialectSQLite)
	if err != nil || again != masterKey {
		t.Fatalf("master key not stable: %q vs %q", again, masterKey)
	}
}

// --- P0-2: SSRF ---

func TestValidatePublicHTTPURL(t *testing.T) {
	for _, bad := range []string{
		"http://127.0.0.1/x", "http://localhost/x", "http://10.0.0.5/x",
		"http://192.168.1.1/x", "http://169.254.169.254/meta", "http://100.64.0.1/x",
		"file:///etc/passwd", "http:///nohost", "",
	} {
		if err := execution.ValidatePublicHTTPURL(bad); err == nil {
			t.Fatalf("must refuse %q", bad)
		}
	}
	if err := execution.ValidatePublicHTTPURL("https://93.184.216.34/api"); err != nil {
		t.Fatalf("public IP must pass: %v", err)
	}
}

// --- P1: login rate limiting / token revocation / password invalidation ---

func TestTokenRevocationAndRotation(t *testing.T) {
	db, err := sql.Open("sqlite", t.TempDir()+"/rev.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	if _, err = OpenSQLSessionStore(ctx, db, SQLDialectSQLite); err != nil {
		t.Fatal(err)
	}
	accounts, err := NewSQLAccountStore(db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	if err := accounts.CreateAccount(ctx, Account{
		Email: "u@sabot.com", Role: RoleAccountUser, TenantID: "default", Status: AccountActive,
	}, "password-123"); err != nil {
		t.Fatal(err)
	}
	token, err := accounts.CreateToken(ctx, "u@sabot.com", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := accounts.ResolveToken(ctx, token); err != nil {
		t.Fatalf("token should resolve: %v", err)
	}
	// Logout revokes the presented token.
	if err := accounts.RevokeToken(ctx, token); err != nil {
		t.Fatal(err)
	}
	if _, err := accounts.ResolveToken(ctx, token); err == nil {
		t.Fatal("revoked token must not resolve")
	}
	// Password rotation invalidates all outstanding tokens.
	token2, _ := accounts.CreateToken(ctx, "u@sabot.com", time.Hour)
	token3, _ := accounts.CreateToken(ctx, "u@sabot.com", time.Hour)
	if err := accounts.SetAccountPassword(ctx, "u@sabot.com", "new-password-9"); err != nil {
		t.Fatal(err)
	}
	if _, err := accounts.ResolveToken(ctx, token2); err == nil {
		t.Fatal("token must be invalid after password rotation")
	}
	if _, err := accounts.ResolveToken(ctx, token3); err == nil {
		t.Fatal("token must be invalid after password rotation")
	}
	// Disabled accounts fail closed: outstanding tokens stop resolving, and
	// new tokens are not issued.
	token4, err := accounts.CreateToken(ctx, "u@sabot.com", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err := accounts.SetAccountStatus(ctx, "u@sabot.com", AccountDisabled); err != nil {
		t.Fatal(err)
	}
	if _, err := accounts.ResolveToken(ctx, token4); err == nil {
		t.Fatal("disabled account token must not resolve")
	}
	if _, err := accounts.CreateToken(ctx, "u@sabot.com", time.Hour); err == nil {
		t.Fatal("disabled account was issued a token")
	}
}

// --- P1: retention pruning ---

func TestRetentionPrunesBoundedTables(t *testing.T) {
	store := newSQLBundle(t)
	ctx := context.Background()

	accounts, err := NewSQLAccountStore(store.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	if err := accounts.CreateAccount(ctx, Account{Email: "a@sabot.com", Role: RoleAccountAdmin, TenantID: "t"}, "password-1"); err != nil {
		t.Fatal(err)
	}
	// Seed an audit event, a hit, and an expired lease directly with old timestamps.
	if _, err := store.db.Exec("INSERT INTO audit_events (id, time, actor, action) VALUES ('aud_old', 1, 'x', 'test')"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec("INSERT INTO obs_hits (id, time, rule_id, kind, session_id) VALUES ('hit_old', 1, 'r', 'keyword', 's')"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec("INSERT INTO session_leases (session_id, holder, expires_at) VALUES ('s', 'h', 1)"); err != nil {
		t.Fatal(err)
	}

	deleted, err := store.PruneAudit(ctx, time.Now().Add(-time.Minute))
	if err != nil || deleted != 1 {
		t.Fatalf("audit prune wrong: %d %v", deleted, err)
	}
	if deleted, err = store.PruneHits(ctx, time.Now().Add(-time.Minute)); err != nil || deleted != 1 {
		t.Fatalf("hits prune wrong: %d %v", deleted, err)
	}
	if deleted, err = store.PruneExpiredLeases(ctx); err != nil || deleted != 1 {
		t.Fatalf("lease prune wrong: %d %v", deleted, err)
	}
}

// --- G-1: capability rate limiting ---

func TestWindowRateLimiterBoundsCalls(t *testing.T) {
	limiter := NewWindowRateLimiter(2)
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if !limiter.AllowCall(ctx, "tenant-a", "crm.send") {
			t.Fatalf("call %d must pass", i+1)
		}
	}
	if limiter.AllowCall(ctx, "tenant-a", "crm.send") {
		t.Fatal("third call in window must be denied")
	}
	// Different tenant is independent.
	if !limiter.AllowCall(ctx, "tenant-b", "crm.send") {
		t.Fatal("other tenant must be independent")
	}
}

// --- G-2: backtest cutoff adjustment ---

func TestAdjustBacktestCutoffAvoidsSplittingSummaryRanges(t *testing.T) {
	events := []SessionEvent{
		{Seq: 0, Data: mustJSON(t, UserMessageData{Text: "a"}), Type: EvUserMessage},
		{Seq: 1, Data: mustJSON(t, UserMessageData{Text: "b"}), Type: EvUserMessage},
		{Seq: 2, Type: EvContextSummary},
	}
	// Give the summary its range payload [1,2].
	raw, _ := json.Marshal(ContextSummaryData{Op: "replace", Start: 1, End: 2, Summary: "s"})
	events[2].Data = raw

	// Cutoff 2 splits the range: adjusted back to 1.
	adjusted, err := AdjustBacktestCutoff(events, 2)
	if err != nil || adjusted != 1 {
		t.Fatalf("cutoff should move to 1: %d %v", adjusted, err)
	}
	// Cutoff inside the range start also adjusts to 1... 1 is the range start.
	adjusted, err = AdjustBacktestCutoff(events, 3)
	if err != nil || adjusted != 3 {
		t.Fatalf("cutoff after the summary is fine: %d %v", adjusted, err)
	}
	// A summary covering seq 0 with summary at seq 2 and cutoff 1: range [0,2]
	// straddles cutoff 1 → moves to 0 → rejected (would strip everything).
	raw2, _ := json.Marshal(ContextSummaryData{Op: "replace", Start: 0, End: 2, Summary: "s"})
	events[2].Data = raw2
	if _, err := AdjustBacktestCutoff(events, 1); err == nil {
		t.Fatal("cutoff that cannot be adjusted must be rejected")
	}
	// No cutoff → full copy.
	if adjusted, err := AdjustBacktestCutoff(events, 0); err != nil || adjusted != int64(len(events)) {
		t.Fatalf("full copy default: %d %v", adjusted, err)
	}
}

// --- P2: run metrics ---

func TestRunStatsMetrics(t *testing.T) {
	store := newSQLBundle(t)
	stats, err := NewSQLRunStatsStore(store.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := stats.RecordRunStat(ctx, RunStat{RunID: "r1", SessionID: "s", TenantID: "acme", Status: "completed", InputTokens: 100, OutputTokens: 50, DurationMS: 800}); err != nil {
		t.Fatal(err)
	}
	if err := stats.RecordRunStat(ctx, RunStat{RunID: "r2", SessionID: "s", TenantID: "acme", Status: "failed", InputTokens: 10, OutputTokens: 1, DurationMS: 200}); err != nil {
		t.Fatal(err)
	}
	metrics, err := stats.Metrics(ctx, "acme")
	if err != nil {
		t.Fatal(err)
	}
	if metrics.TotalRuns != 2 || metrics.CompletedRuns != 1 || metrics.FailedRuns != 1 || metrics.TokensIn != 110 {
		t.Fatalf("metrics wrong: %#v", metrics)
	}
	if other, err := stats.Metrics(ctx, "other"); err != nil || other.TotalRuns != 0 {
		t.Fatalf("metrics must be tenant-scoped: %#v %v", other, err)
	}
}

// --- P2: cold sweep ---

func TestColdSweepCompressesAndReadsTransparently(t *testing.T) {
	store, _, session := newCommittedColdSession(t)
	ctx := context.Background()
	key := chunkKey(session.ID(), 0)
	// Negative OlderThan makes even fresh objects count as cold.
	sweeper := &ColdSweeper{Store: store, OlderThan: -time.Minute, Prefixes: []string{"sessions/"}}
	swept, err := sweeper.Sweep(ctx)
	if err != nil || swept != 1 {
		t.Fatalf("sweep wrong: %d %v", swept, err)
	}
	// The plain object is gone, the ".zst" sibling exists.
	if _, err := os.Stat(objectPath(t, store, key)); !os.IsNotExist(err) {
		t.Fatalf("plain object should have been removed after sweep (err=%v)", err)
	}
	if _, _, err := store.Get(ctx, key); err != nil {
		t.Fatalf("transparent read failed: %v", err)
	}
	zstData, err := os.ReadFile(objectPath(t, store, key) + ".zst")
	if err != nil {
		t.Fatalf(".zst sibling missing: %v", err)
	}
	if len(zstData) == 0 {
		t.Fatal("zst payload empty")
	}
	// Second sweep is a no-op (sibling skipped, original gone).
	swept, err = sweeper.Sweep(ctx)
	if err != nil || swept != 0 {
		t.Fatalf("second sweep must be a no-op: %d %v", swept, err)
	}
}
