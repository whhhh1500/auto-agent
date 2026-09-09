package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"strconv"
	"strings"
	"testing"

	core "github.com/whhhh1500/auto-agent/pkg/core"
)

func TestSQLObsStoreListHitsRejectsInvalidFiltersBeforeSQL(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	store, err := NewSQLObsStore(db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	assertBeforeSQL := func(name string, err error) {
		t.Helper()
		if err == nil {
			t.Fatalf("%s was accepted", name)
		}
		if strings.Contains(err.Error(), "no such table") {
			t.Fatalf("%s reached SQL before validation: %v", name, err)
		}
	}
	assertBeforeSQL("tenant NUL", func() error {
		_, _, err := store.ListHits(ctx, ObsHitFilter{TenantID: "acme\x00"})
		return err
	}())
	assertBeforeSQL("session id", func() error {
		_, _, err := store.ListHits(ctx, ObsHitFilter{SessionID: "bad id"})
		return err
	}())
	assertBeforeSQL("rule control", func() error {
		_, _, err := store.ListHits(ctx, ObsHitFilter{RuleID: "rule\n1"})
		return err
	}())
}

func TestSQLObsStoreRejectsInvalidWritesBeforeSQL(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	store, err := NewSQLObsStore(db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	assertBeforeSQL := func(name string, err error) {
		t.Helper()
		if err == nil {
			t.Fatalf("%s was accepted", name)
		}
		if strings.Contains(err.Error(), "no such table") {
			t.Fatalf("%s reached SQL before validation: %v", name, err)
		}
	}
	assertBeforeSQL("rule NUL name", func() error {
		_, err := store.CreateRule(ctx, ObsRule{Name: "bad\x00", Kind: "keyword", Pattern: "sol"})
		return err
	}())
	assertBeforeSQL("rule NUL pattern", func() error {
		_, err := store.CreateRule(ctx, ObsRule{Name: "watch", Kind: "keyword", Pattern: "sol\x00"})
		return err
	}())
	assertBeforeSQL("delete empty", store.DeleteRule(ctx, ""))
	assertBeforeSQL("hit session", store.RecordHit(ctx, ObsHit{RuleID: "obs_1", SessionID: "bad id"}))
	assertBeforeSQL("hit snippet NUL", store.RecordHit(ctx, ObsHit{RuleID: "obs_1", Snippet: "x\x00y"}))
}

func TestSQLObsStoreRejectsRuleOverflow(t *testing.T) {
	sessions := newTestSQLStore(t)
	store, err := NewSQLObsStore(sessions.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	store.maxRules = 2
	ctx := context.Background()
	if _, err := store.CreateRule(ctx, ObsRule{Name: "one", Kind: "keyword", Pattern: "alpha"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateRule(ctx, ObsRule{Name: "two", Kind: "keyword", Pattern: "beta"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateRule(ctx, ObsRule{Name: "three", Kind: "keyword", Pattern: "gamma"}); err == nil {
		t.Fatal("obs rule overflow was accepted")
	}
}

func TestSQLObsStoreRejectsHitOverflow(t *testing.T) {
	sessions := newTestSQLStore(t)
	store, err := NewSQLObsStore(sessions.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	store.maxHits = 2
	ctx := context.Background()
	if err := store.RecordHit(ctx, ObsHit{RuleID: "obs_1", Kind: "keyword", Snippet: "one"}); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordHit(ctx, ObsHit{RuleID: "obs_1", Kind: "keyword", Snippet: "two"}); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordHit(ctx, ObsHit{RuleID: "obs_1", Kind: "keyword", Snippet: "three"}); err == nil {
		t.Fatal("obs hit overflow was accepted")
	} else if !strings.Contains(err.Error(), "observability hits exceed maximum of 2") {
		t.Fatalf("unexpected overflow error: %v", err)
	}
	if err := store.RecordHit(ctx, ObsHit{RuleID: "obs_1", Kind: "keyword", Snippet: strings.Repeat("x", MaxObsSnippetBytes+1)}); err == nil {
		t.Fatal("oversized obs snippet was accepted")
	}
}

func TestMatchEventUsesNonSensitiveSummariesWithoutChangingMatches(t *testing.T) {
	toolSecret := "tool-secret-should-not-persist"
	messageSecret := "message-secret-should-not-persist"
	rules := []ObsRule{
		{ID: "tool-rule", Name: "payment tool", Kind: "tool", Pattern: "payments.refund"},
		{ID: "keyword-rule", Name: "message keyword", Kind: "keyword", Pattern: "escalate"},
	}
	encode := func(value any) json.RawMessage {
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	message := "Please ESCALATE this case with " + messageSecret
	tests := []struct {
		name            string
		event           core.SessionEvent
		wantRuleID      string
		wantSummary     string
		forbiddenValues []string
	}{
		{
			name: "tool capability without arguments",
			event: core.SessionEvent{RunID: "run-tool", Type: core.EvToolCall, Data: encode(core.ToolCallData{
				Name: "payments.refund", Args: map[string]any{"api_key": toolSecret},
			})},
			wantRuleID: "tool-rule", wantSummary: obsToolSummaryPrefix + "payments.refund",
			forbiddenValues: []string{toolSecret, "api_key"},
		},
		{
			name:       "user message category without text",
			event:      core.SessionEvent{RunID: "run-user", Type: core.EvUserMessage, Data: encode(core.UserMessageData{Text: message})},
			wantRuleID: "keyword-rule", wantSummary: obsMessageSummary,
			forbiddenValues: []string{message, messageSecret, "ESCALATE"},
		},
		{
			name:       "assistant message category without text",
			event:      core.SessionEvent{RunID: "run-assistant", Type: core.EvAssistantMessage, Data: encode(core.AssistantMessageData{Text: message})},
			wantRuleID: "keyword-rule", wantSummary: obsMessageSummary,
			forbiddenValues: []string{message, messageSecret, "ESCALATE"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			hit := MatchEvent(rules, test.event)
			if hit == nil || hit.RuleID != test.wantRuleID || hit.RunID != test.event.RunID || hit.Snippet != test.wantSummary {
				t.Fatalf("hit=%#v, want rule=%q run=%q summary=%q", hit, test.wantRuleID, test.event.RunID, test.wantSummary)
			}
			for _, forbidden := range test.forbiddenValues {
				if strings.Contains(hit.Snippet, forbidden) {
					t.Fatalf("summary leaked %q: %q", forbidden, hit.Snippet)
				}
			}
		})
	}
}

func TestSQLObsStoreDropsNonCanonicalSnippetBeforePersistence(t *testing.T) {
	sessions := newTestSQLStore(t)
	store, err := NewSQLObsStore(sessions.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	secret := "do-not-write-this-tool-argument"
	if err := store.RecordHit(ctx, ObsHit{ID: "hit-secret", RuleID: "obs_1", Kind: "tool", Snippet: secret}); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordHit(ctx, ObsHit{ID: "hit-safe", RuleID: "obs_2", Kind: "tool", Snippet: obsToolSummaryPrefix + "payments.refund"}); err != nil {
		t.Fatal(err)
	}
	hits, total, err := store.ListHits(ctx, ObsHitFilter{Limit: 10})
	if err != nil || total != 2 || len(hits) != 2 {
		t.Fatalf("hits=%#v total=%d err=%v", hits, total, err)
	}
	byID := map[string]ObsHit{}
	for _, hit := range hits {
		byID[hit.ID] = hit
	}
	if got := byID["hit-secret"].Snippet; got != "" {
		t.Fatalf("non-canonical snippet persisted as %q", got)
	}
	if got := byID["hit-safe"].Snippet; got != obsToolSummaryPrefix+"payments.refund" {
		t.Fatalf("safe capability summary=%q", got)
	}
}

func TestSQLSchemaV48ClearsLegacyObsHitSnippets(t *testing.T) {
	db, err := sql.Open("sqlite", t.TempDir()+"/legacy-observability.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()
	if _, err := OpenSQLSessionStore(ctx, db, SQLDialectSQLite); err != nil {
		t.Fatal(err)
	}
	secret := "legacy-message-and-tool-argument-secret"
	if _, err := db.ExecContext(ctx, "INSERT INTO obs_hits (id, time, rule_id, kind, session_id, snippet) VALUES (?, ?, ?, ?, ?, ?)", "hit-legacy", 1, "obs_legacy", "keyword", "sess_legacy", secret); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "UPDATE store_meta SET value = '47' WHERE key = 'schema_version'"); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSQLSessionStore(ctx, db, SQLDialectSQLite); err != nil {
		t.Fatal(err)
	}
	var snippet, version string
	if err := db.QueryRowContext(ctx, "SELECT snippet FROM obs_hits WHERE id = 'hit-legacy'").Scan(&snippet); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, "SELECT value FROM store_meta WHERE key = 'schema_version'").Scan(&version); err != nil {
		t.Fatal(err)
	}
	if snippet != "" || version != strconv.Itoa(SQLSchemaVersion) {
		t.Fatalf("legacy snippet=%q schema version=%q", snippet, version)
	}
}
