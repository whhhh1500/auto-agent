package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"github.com/whhhh1500/auto-agent/pkg/control"
	. "github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/extensions/memory"
	"github.com/whhhh1500/auto-agent/pkg/extensions/runner"
	"github.com/whhhh1500/auto-agent/pkg/extensions/subagent"
	"strconv"
	"strings"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

func newSQLBundle(t *testing.T) *SQLSessionStore {
	t.Helper()
	db, err := sql.Open("sqlite", t.TempDir()+"/bundle.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	store, err := OpenSQLSessionStore(context.Background(), db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

// --- Distributed lease ---

func TestSessionLeaseAcquireConflictExpireRelease(t *testing.T) {
	store := newSQLBundle(t)
	ctx := context.Background()

	acquired, err := store.AcquireSessionLease(ctx, "sess-1", "instance-a", time.Minute)
	if err != nil || !acquired {
		t.Fatalf("first acquire must succeed: %v %v", acquired, err)
	}
	// Another instance cannot take a live lease.
	acquired, err = store.AcquireSessionLease(ctx, "sess-1", "instance-b", time.Minute)
	if err != nil || acquired {
		t.Fatalf("live lease must block other holders: %v %v", acquired, err)
	}
	renewed, err := store.RenewSessionLease(ctx, "sess-1", "instance-b", time.Minute)
	if err != nil || renewed {
		t.Fatalf("non-owner must not renew a live lease: %v %v", renewed, err)
	}
	// Acquisition never doubles as renewal, even for the same holder.
	acquired, err = store.AcquireSessionLease(ctx, "sess-1", "instance-a", time.Minute)
	if err != nil || acquired {
		t.Fatalf("live lease must require explicit renewal: %v %v", acquired, err)
	}
	// The current holder can explicitly renew a live lease.
	renewed, err = store.RenewSessionLease(ctx, "sess-1", "instance-a", time.Minute)
	if err != nil || !renewed {
		t.Fatalf("holder renewal must succeed: %v %v", renewed, err)
	}
	// After expiry another instance takes over.
	if _, err := store.db.Exec("UPDATE session_leases SET expires_at = 1 WHERE session_id = 'sess-1'"); err != nil {
		t.Fatal(err)
	}
	renewed, err = store.RenewSessionLease(ctx, "sess-1", "instance-a", time.Minute)
	if err != nil || renewed {
		t.Fatalf("expired lease must not be revivable: %v %v", renewed, err)
	}
	acquired, err = store.AcquireSessionLease(ctx, "sess-1", "instance-b", time.Minute)
	if err != nil || !acquired {
		t.Fatalf("expired lease must be takeable: %v %v", acquired, err)
	}
	// The stale holder cannot release the new holder's lease.
	if err := store.ReleaseSessionLease(ctx, "sess-1", "instance-a"); err != nil {
		t.Fatal(err)
	}
	acquired, _ = store.AcquireSessionLease(ctx, "sess-1", "instance-a", time.Minute)
	if acquired {
		t.Fatal("stale holder must not steal a live lease by releasing")
	}
	if err := store.ReleaseSessionLease(ctx, "sess-1", "instance-b"); err != nil {
		t.Fatal(err)
	}
	acquired, _ = store.AcquireSessionLease(ctx, "sess-1", "instance-a", time.Minute)
	if !acquired {
		t.Fatal("release must free the lease")
	}
	if _, err := store.AcquireSessionLease(ctx, "sess-2", "a", 0); err == nil {
		t.Fatal("non-positive ttl must be refused")
	}
	if _, err := store.RenewSessionLease(ctx, "sess-2", "a", 0); err == nil {
		t.Fatal("non-positive renewal ttl must be refused")
	}
}

func TestTenantAndAccountScopeIdentifiersAreValidated(t *testing.T) {
	store := newSQLBundle(t)
	accounts, err := NewSQLAccountStore(store.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	if err := accounts.CreateTenant(context.Background(), "bad/tenant", "Bad"); err == nil {
		t.Fatal("tenant id containing the scope delimiter was accepted")
	}
	if err := accounts.CreateAccount(context.Background(), Account{
		Email: "bad/user@example.com", Role: RoleAccountUser, TenantID: "acme", Status: AccountActive,
	}, "password-123"); err == nil {
		t.Fatal("account id containing the scope delimiter was accepted")
	}
}

// --- Release journal ---

func TestReleaseJournalSurvivesAcrossManagers(t *testing.T) {
	store := newSQLBundle(t)
	_, product, _, user := testScopes()
	profiles := NewAgentProfileRegistry()

	first, err := control.NewReleaseManager(profiles)
	if err != nil {
		t.Fatal(err)
	}
	first.Journal = store
	ctx := context.Background()
	if err := first.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	name := "Agent"
	model := ModelSelection{Provider: "mock", Model: "mock-1"}
	if _, err := first.Publish(ctx, product, AgentProfileLayer{ProfileID: "product.agent", Name: &name, Model: &model}); err != nil {
		t.Fatal(err)
	}
	if _, err := first.Publish(ctx, product, AgentProfileLayer{ProfileID: "product.agent", Name: &name, Model: &model}); err != nil {
		t.Fatal(err)
	}
	if _, err := first.Rollback(ctx, "product.agent", 1); err != nil {
		t.Fatal(err)
	}

	// A fresh manager with the same journal reads the durable history.
	restoredProfiles := NewAgentProfileRegistry()
	second, err := control.NewReleaseManager(restoredProfiles)
	if err != nil {
		t.Fatal(err)
	}
	second.Journal = store
	if err := second.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	history := second.History(ctx, "product.agent")
	if len(history) != 2 || history[0].Version != 1 || history[1].Version != 2 {
		t.Fatalf("durable history wrong: %#v", history)
	}
	if !history[1].RolledBack || history[0].RolledBack {
		t.Fatalf("rollback not recorded: %#v", history)
	}
	if history[0].Scope.String() != product.String() {
		t.Fatalf("scope round trip failed: %q", history[0].Scope.String())
	}
	if snapshot, err := restoredProfiles.Resolve(testPrincipal(user), user, "product.agent"); err != nil || snapshot.Name != name {
		t.Fatalf("live release artifact was not restored: %#v err=%v", snapshot, err)
	}
	if _, err := second.Rollback(ctx, "product.agent", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := restoredProfiles.Resolve(testPrincipal(user), user, "product.agent"); err == nil {
		t.Fatal("restored release remained live after rollback")
	}
}

func TestSchemaV1DatabaseUpgradesInPlace(t *testing.T) {
	db, err := sql.Open("sqlite", t.TempDir()+"/legacy.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()

	// Build a genuine v1 database with the previous DDL and version.
	v1 := `
	CREATE TABLE IF NOT EXISTS store_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
	CREATE TABLE IF NOT EXISTS sessions (id TEXT PRIMARY KEY, version BIGINT NOT NULL, tenant_id TEXT NOT NULL,
		user_id TEXT NOT NULL, profile_id TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'active',
		event_count BIGINT NOT NULL, header TEXT NOT NULL, updated_at BIGINT NOT NULL);
	CREATE TABLE IF NOT EXISTS event_chunks (session_id TEXT NOT NULL, start_seq BIGINT NOT NULL,
		payload TEXT NOT NULL, PRIMARY KEY (session_id, start_seq));
	INSERT INTO store_meta VALUES ('schema_version', '1');
	`
	if _, err := db.ExecContext(ctx, v1); err != nil {
		t.Fatal(err)
	}
	// Opening upgrades in place and the new subsystems work.
	store, err := OpenSQLSessionStore(ctx, db, SQLDialectSQLite)
	if err != nil {
		t.Fatalf("v1 upgrade failed: %v", err)
	}
	var version string
	if err := db.QueryRow("SELECT value FROM store_meta WHERE key='schema_version'").Scan(&version); err != nil {
		t.Fatal(err)
	}
	if parsed, convErr := strconv.Atoi(version); convErr != nil || parsed < SQLSchemaVersion {
		t.Fatalf("schema version not advanced: %q %v", version, convErr)
	}
	if acquired, err := store.AcquireSessionLease(ctx, "s", "h", time.Minute); err != nil || !acquired {
		t.Fatalf("v3 lease table missing after upgrade: %v %v", acquired, err)
	}
	journal, err := NewSQLToolInvocationJournal(db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	invocation, err := NewToolInvocation(RunInfo{
		RunID: "run-upgrade-journal", SessionID: "session-upgrade-journal",
		Principal: Principal{TenantID: "tenant-a", SubjectID: "user-a"},
	}, ToolCall{ID: "call-upgrade", Name: "upgrade.tool"}, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, decision, err := journal.BeginToolInvocation(ctx, invocation); err != nil || decision != ToolInvocationExecuteNew {
		t.Fatalf("v9 tool journal missing after upgrade: decision=%q err=%v", decision, err)
	}
}

// TestSchemaV1DatabasePreservesSessionRowsAcrossRepeatOpen uses the original
// v1 tables instead of a current schema with a rewound version marker. It
// proves that the oldest supported session and event-chunk rows remain
// readable after the complete upgrade and a subsequent idempotent open.
func TestSchemaV1DatabasePreservesSessionRowsAcrossRepeatOpen(t *testing.T) {
	db, err := sql.Open("sqlite", t.TempDir()+"/legacy-session.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()

	_, _, _, user := testScopes()
	principal := testPrincipal(user)
	legacy := mustSession(t, user, principal)
	event, err := legacy.Append("run-v1", EvUserMessage, UserMessageData{Text: "preserve this v1 event"})
	if err != nil {
		t.Fatal(err)
	}
	header, err := json.Marshal(SessionOptions{
		ID: legacy.ID(), ProfileID: legacy.ProfileID(), Principal: legacy.Principal(),
		Scope: legacy.Scope(), Metadata: legacy.Metadata(),
	})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := db.ExecContext(ctx, `
		CREATE TABLE store_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
		CREATE TABLE sessions (id TEXT PRIMARY KEY, version BIGINT NOT NULL, tenant_id TEXT NOT NULL,
			user_id TEXT NOT NULL, profile_id TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'active',
			event_count BIGINT NOT NULL, header TEXT NOT NULL, updated_at BIGINT NOT NULL);
		CREATE TABLE event_chunks (session_id TEXT NOT NULL, start_seq BIGINT NOT NULL,
			payload TEXT NOT NULL, PRIMARY KEY (session_id, start_seq));
		INSERT INTO store_meta VALUES ('schema_version', '1');
	`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO sessions
		(id, version, tenant_id, user_id, profile_id, event_count, header, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		legacy.ID(), 1, principal.TenantID, principal.SubjectID, legacy.ProfileID(), 1, string(header), int64(1234)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO event_chunks (session_id, start_seq, payload) VALUES (?, ?, ?)", legacy.ID(), 0, string(payload)+"\n"); err != nil {
		t.Fatal(err)
	}

	store, err := OpenSQLSessionStore(ctx, db, SQLDialectSQLite)
	if err != nil {
		t.Fatalf("v1 upgrade failed: %v", err)
	}
	loaded, err := store.Load(ctx, legacy.ID())
	if err != nil {
		t.Fatalf("load preserved v1 session: %v", err)
	}
	if loaded.ID() != legacy.ID() || loaded.ProfileID() != legacy.ProfileID() || len(loaded.Events()) != 1 ||
		string(loaded.Events()[0].Data) != string(event.Data) {
		t.Fatalf("v1 session row was not preserved: %#v", loaded)
	}

	if _, err := OpenSQLSessionStore(ctx, db, SQLDialectSQLite); err != nil {
		t.Fatalf("repeat open after v1 upgrade: %v", err)
	}
	var version string
	if err := db.QueryRowContext(ctx, sqlSelectMetaRow.bind(SQLDialectSQLite)).Scan(&version); err != nil || version != strconv.Itoa(SQLSchemaVersion) {
		t.Fatalf("v1 upgrade version = %q, %v; want %d", version, err, SQLSchemaVersion)
	}
	var sessionRows, chunkRows int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM sessions WHERE id = ?", legacy.ID()).Scan(&sessionRows); err != nil || sessionRows != 1 {
		t.Fatalf("preserved session rows = %d, %v; want 1", sessionRows, err)
	}
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM event_chunks WHERE session_id = ?", legacy.ID()).Scan(&chunkRows); err != nil || chunkRows != 1 {
		t.Fatalf("preserved event chunk rows = %d, %v; want 1", chunkRows, err)
	}
}

// --- SQL memory + RAG ---

func TestSQLMemoryStoreMatchesSliceSemantics(t *testing.T) {
	db := newSQLMemoryRagV26DB(t)
	sqlMemory, err := NewSQLMemoryStore(db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	sliceMemory := memory.NewSliceStore()
	_, _, _, user := testScopes()
	ctx := context.Background()

	for _, backend := range []memory.Store{sqlMemory, sliceMemory} {
		if _, err := backend.Remember(ctx, user, MemoryEntry{Key: "watchlist", Content: "Alice watches SOL", Tags: []string{"portfolio"}}); err != nil {
			t.Fatal(err)
		}
		if _, err := backend.Remember(ctx, user, MemoryEntry{Key: "watchlist", Content: "Alice watches ETH", Tags: []string{"portfolio"}}); err != nil {
			t.Fatal(err)
		}
		entries, err := backend.Recall(ctx, user, "eth", nil, 10)
		if err != nil || len(entries) != 1 || !strings.Contains(entries[0].Content, "ETH") {
			t.Fatalf("%T recall wrong: %#v %v", backend, entries, err)
		}
		if err := backend.Forget(ctx, user, entries[0].ID); err != nil {
			t.Fatal(err)
		}
		empty, err := backend.Recall(ctx, user, "", nil, 10)
		if err != nil || len(empty) != 0 {
			t.Fatalf("%T forget failed: %#v %v", backend, empty, err)
		}
		if err := backend.Forget(ctx, user, "mem_missing"); err == nil {
			t.Fatalf("%T forget of missing id must fail", backend)
		}
	}
}

func TestSQLRagIndexMatchesKeywordSemantics(t *testing.T) {
	db := newSQLMemoryRagV26DB(t)
	sqlIndex, err := NewSQLRagIndex(db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	global, _, tenant, user := testScopes()
	ctx := context.Background()

	// Ingest at tenant level: visible to users, not to other tenants.
	if err := sqlIndex.Ingest(ctx, tenant, RagDocument{
		ID: "sol-doc", Source: "docs/sol.md",
		Content: "Solana is a high throughput blockchain for parallel execution.",
		Tags:    []string{"research"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := sqlIndex.Ingest(ctx, global, RagDocument{
		ID: "glossary", Content: "Throughput means transactions per second on a blockchain.",
	}); err != nil {
		t.Fatal(err)
	}
	chunks, err := sqlIndex.Search(ctx, user, RagQuery{Query: "solana throughput", TopK: 5})
	if err != nil || len(chunks) != 2 {
		t.Fatalf("search wrong: %#v %v", chunks, err)
	}
	if chunks[0].ID != "sol-doc" {
		t.Fatalf("ranking wrong: %#v", chunks)
	}
	tagged, err := sqlIndex.Search(ctx, user, RagQuery{Query: "solana", Tags: []string{"research"}})
	if err != nil || len(tagged) != 1 || tagged[0].ID != "sol-doc" {
		t.Fatalf("tag filter wrong: %#v %v", tagged, err)
	}
	// Same-document id replaces at the same scope.
	if err := sqlIndex.Ingest(ctx, tenant, RagDocument{ID: "sol-doc", Content: "Solana v2 overview."}); err != nil {
		t.Fatal(err)
	}
	updated, err := sqlIndex.Search(ctx, user, RagQuery{Query: "solana"})
	if err != nil || len(updated) != 1 || !strings.Contains(updated[0].Content, "v2") {
		t.Fatalf("replace-by-id failed: %#v %v", updated, err)
	}
	if err := sqlIndex.Ingest(ctx, tenant, RagDocument{ID: "empty", Content: "   "}); err == nil {
		t.Fatal("empty content must be refused")
	}
}

// --- Runner hub ---

func TestRunnerHubSubmitClaimComplete(t *testing.T) {
	hub := runner.NewHub()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	done := make(chan CapabilityResult, 1)
	go func() {
		result, err := hub.SubmitWithKey(ctx, "runner.acme", "call-render", map[string]any{"job": "render"})
		if err != nil {
			done <- CapabilityResult{Content: err.Error(), OK: false}
			return
		}
		done <- result
	}()

	deadline := time.Now().Add(2 * time.Second)
	var task runner.Task
	for {
		if claimedTask, ok, err := hub.Claim(ctx, runner.ClaimOptions{WorkerID: "runner-worker", LeaseTTL: time.Second}); err != nil {
			t.Fatal(err)
		} else if ok {
			task = claimedTask
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("task never appeared in the queue")
		}
		time.Sleep(time.Millisecond)
	}
	if task.Capability != "runner.acme" || task.IdempotencyKey != "call-render" || task.Args["job"] != "render" {
		t.Fatalf("task payload wrong: %#v", task)
	}
	if _, completed, err := hub.Complete(ctx, task.ID, "runner-worker", task.Generation, CapabilityResult{Content: "rendered", OK: true}); err != nil || !completed {
		t.Fatal("complete reported false for a live task")
	}
	result := <-done
	if !result.OK || result.Content != "rendered" {
		t.Fatalf("submit did not receive the completion: %#v", result)
	}
	if _, completed, err := hub.Complete(ctx, "rtask_missing", "runner-worker", 1, CapabilityResult{}); !errors.Is(err, runner.ErrTaskNotFound) || completed {
		t.Fatal("completing an unknown task must report false")
	}
}

func TestRunnerProviderTimesOutWhenRunnerSilent(t *testing.T) {
	hub := runner.NewHub()
	provider := runner.Provider{Hub: hub, Capability: "runner.acme", Timeout: 50 * time.Millisecond}
	principal := testPrincipal(mustUserScope(t))
	result, _ := provider.Execute(context.Background(), CapabilityRequest{
		CallID: "c1", Args: map[string]any{},
		Context: CapabilityContext{Principal: principal, Scope: principal.Scope},
	})
	if result.OK || result.Metadata["code"] != "runner_timeout" {
		t.Fatalf("silent runner must time out: %#v", result)
	}
}

// --- Continuable subagents ---

func TestSubagentContinuationAppendsToChildSession(t *testing.T) {
	_, product, _, user := testScopes()
	principal := testPrincipal(user)
	capabilities := NewCapabilityRegistry()
	profiles := NewAgentProfileRegistry()
	name := "Specialist"
	model := ModelSelection{Provider: "mock", Model: "mock-1"}
	if err := profiles.Bind(AgentProfileLayer{
		Scope: product, ProfileID: "product.child", Name: &name, Model: &model,
	}); err != nil {
		t.Fatal(err)
	}
	runtime := &Runtime{
		Capabilities: capabilities, Profiles: profiles,
		Models: ModelResolverFunc(func(context.Context, ModelSelection) (LlmAdapter, error) {
			return MockLlmAdapter{}, nil
		}),
	}
	subagentCapability, err := subagent.NewCapability(runtime, "agent.helper", subagent.Options{ProfileID: "product.child"})
	if err != nil {
		t.Fatal(err)
	}
	if err := capabilities.Register(product, subagentCapability); err != nil {
		t.Fatal(err)
	}
	snapshot, err := (CapabilityResolver{Registry: capabilities}).Resolve(principal, user)
	if err != nil {
		t.Fatal(err)
	}

	first, err := snapshot.Execute(context.Background(), ToolCall{
		ID: "c1", Name: "agent.helper", Args: map[string]any{"prompt": "start"},
	})
	if err != nil || !first.OK {
		t.Fatalf("first delegation failed: %#v %v", first, err)
	}
	childID, _ := first.Metadata["child_session_id"].(string)

	// Continue the same child: the mock answers with the tool-result prefix,
	// proving the followup landed on the existing session.
	second, err := snapshot.Execute(context.Background(), ToolCall{
		ID: "c2", Name: "agent.helper",
		Args: map[string]any{"session_id": childID, "followup": "continue"},
	})
	if err != nil || !second.OK {
		t.Fatalf("continuation failed: %#v %v", second, err)
	}
	if second.Metadata["child_session_id"] != childID {
		t.Fatalf("continuation created a new child: %#v", second.Metadata)
	}

	// Only the delegating subject may continue.
	other := principal
	other.Grants = principal.Grants.Clone()
	other.SubjectID = "mallory"
	otherSnapshot, err := (CapabilityResolver{Registry: capabilities}).Resolve(other, user)
	if err != nil {
		t.Fatal(err)
	}
	hijacked, _ := otherSnapshot.Execute(context.Background(), ToolCall{
		ID: "c3", Name: "agent.helper",
		Args: map[string]any{"session_id": childID, "followup": "hijack"},
	})
	if hijacked.OK || hijacked.Metadata["code"] != "child_session_forbidden" {
		t.Fatalf("cross-subject continuation must be forbidden: %#v", hijacked)
	}

	// Unknown child sessions fail cleanly.
	if result, _ := snapshot.Execute(context.Background(), ToolCall{
		ID: "c4", Name: "agent.helper",
		Args: map[string]any{"session_id": "sess_missing", "followup": "x"},
	}); result.OK || result.Metadata["code"] != "child_session_unavailable" {
		t.Fatalf("missing child must be unavailable: %#v", result)
	}

	// A new prompt plus an existing session_id must not replace the child.
	replaced, _ := snapshot.Execute(context.Background(), ToolCall{
		ID: "c5", Name: "agent.helper",
		Args: map[string]any{"session_id": childID, "prompt": "replace"},
	})
	if replaced.OK || replaced.Metadata["code"] != CodeInvalidArgs {
		t.Fatalf("session_id without followup must not start a new child: %#v", replaced)
	}
	continued, err := snapshot.Execute(context.Background(), ToolCall{
		ID: "c6", Name: "agent.helper",
		Args: map[string]any{"session_id": childID, "followup": "still-there"},
	})
	if err != nil || !continued.OK || continued.Metadata["child_session_id"] != childID {
		t.Fatalf("original child was replaced: %#v err=%v", continued, err)
	}
}
