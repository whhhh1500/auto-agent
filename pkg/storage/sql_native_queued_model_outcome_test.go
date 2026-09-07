package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
)

type nativeQueuedModelOutcomeFixture struct {
	*nativeQueuedModelFixture
	events []core.SessionEvent
}

func newNativeQueuedModelOutcomeFixture(t *testing.T, store *SQLSessionStore, sessionID, runID string) *nativeQueuedModelOutcomeFixture {
	t.Helper()
	model := newNativeQueuedModelFixture(t, store, sessionID, runID)
	if admitted, err := store.BeginNativeQueuedModelInvocationFenced(context.Background(), model.fence, model.version, model.input); err != nil || !admitted {
		t.Fatalf("attempt admitted=%t err=%v", admitted, err)
	}
	staged, err := model.session.Clone()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := staged.Append(runID, core.EvAssistantMessage, core.AssistantMessageData{Text: "model outcome"}); err != nil {
		t.Fatal(err)
	}
	if _, err := staged.Append(runID, core.EvRunUsage, core.RunUsageData{InputTokens: 4, OutputTokens: 2, InvocationID: "model:2"}); err != nil {
		t.Fatal(err)
	}
	return &nativeQueuedModelOutcomeFixture{nativeQueuedModelFixture: model, events: staged.EventsFrom(model.version)}
}

func assertNativeQueuedModelOutcome(t *testing.T, fixture *nativeQueuedModelOutcomeFixture) {
	t.Helper()
	loaded, err := fixture.store.Load(context.Background(), fixture.session.ID())
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Version() != fixture.version+int64(len(fixture.events)) {
		t.Fatalf("version=%d", loaded.Version())
	}
	actual := loaded.EventsFrom(fixture.version)
	if len(actual) != 2 || actual[0].Type != core.EvAssistantMessage || actual[1].Type != core.EvRunUsage {
		t.Fatalf("outcome events=%+v", actual)
	}
	var rows int
	if err := fixture.store.db.QueryRowContext(context.Background(), (sqlQuery{"SELECT COUNT(*) FROM native_queued_model_invocation_outcomes WHERE session_id = ? AND run_id = ?"}).bind(fixture.store.dialect), fixture.session.ID(), fixture.fence.RunID).Scan(&rows); err != nil || rows != 1 {
		t.Fatalf("outcome rows=%d err=%v", rows, err)
	}
}

func TestSQLSessionStoreAppendNativeQueuedModelOutcomeFenced(t *testing.T) {
	fixture := newNativeQueuedModelOutcomeFixture(t, newTestSQLStore(t), "session-model-outcome", "run-model-outcome")
	if appended, err := fixture.store.AppendNativeQueuedModelOutcomeFenced(context.Background(), fixture.fence, fixture.version, fixture.events); err != nil || !appended {
		t.Fatalf("append=%t err=%v", appended, err)
	}
	assertNativeQueuedModelOutcome(t, fixture)
	if appended, err := fixture.store.AppendNativeQueuedModelOutcomeFenced(context.Background(), fixture.fence, fixture.version, fixture.events); err != nil || appended {
		t.Fatalf("response-lost append=%t err=%v", appended, err)
	}
	if appended, err := fixture.store.AppendNativeQueuedModelOutcomeFenced(context.Background(), advanceNativeQueuedModelGeneration(t, fixture.nativeQueuedModelFixture), fixture.version, fixture.events); err != nil || appended {
		t.Fatalf("new generation convergence append=%t err=%v", appended, err)
	}
}

func TestSQLSessionStoreNativeQueuedModelOutcomeFailsClosed(t *testing.T) {
	t.Run("missing_attempt", func(t *testing.T) {
		fixture := newNativeQueuedModelFixture(t, newTestSQLStore(t), "session-model-no-attempt", "run-model-no-attempt")
		staged, _ := fixture.session.Clone()
		_, _ = staged.Append(fixture.fence.RunID, core.EvAssistantMessage, core.AssistantMessageData{Text: "outcome"})
		_, _ = staged.Append(fixture.fence.RunID, core.EvRunUsage, core.RunUsageData{InvocationID: "model:2"})
		if appended, err := fixture.store.AppendNativeQueuedModelOutcomeFenced(context.Background(), fixture.fence, fixture.version, staged.EventsFrom(fixture.version)); appended || !errors.Is(err, ErrCompletedToolResultProofInvalid) {
			t.Fatalf("append=%t err=%v", appended, err)
		}
	})
	t.Run("another_generation_cannot_first_deliver", func(t *testing.T) {
		fixture := newNativeQueuedModelOutcomeFixture(t, newTestSQLStore(t), "session-model-owner", "run-model-owner")
		if appended, err := fixture.store.AppendNativeQueuedModelOutcomeFenced(context.Background(), advanceNativeQueuedModelGeneration(t, fixture.nativeQueuedModelFixture), fixture.version, fixture.events); appended || !errors.Is(err, ErrSessionWriteFenceLost) {
			t.Fatalf("append=%t err=%v", appended, err)
		}
	})
	for _, test := range []struct {
		name   string
		mutate func(*nativeQueuedModelOutcomeFixture)
		want   error
	}{
		{"usage", func(f *nativeQueuedModelOutcomeFixture) {
			var data core.RunUsageData
			_ = json.Unmarshal(f.events[len(f.events)-1].Data, &data)
			data.InvocationID = "model:999"
			f.events[len(f.events)-1].Data, _ = json.Marshal(data)
		}, ErrCompletedToolResultProofInvalid},
		{"version", func(f *nativeQueuedModelOutcomeFixture) { f.version++ }, core.ErrSessionConflict},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newNativeQueuedModelOutcomeFixture(t, newTestSQLStore(t), "session-model-outcome-"+test.name, "run-model-outcome-"+test.name)
			test.mutate(fixture)
			if appended, err := fixture.store.AppendNativeQueuedModelOutcomeFenced(context.Background(), fixture.fence, fixture.version, fixture.events); appended || !errors.Is(err, test.want) {
				t.Fatalf("append=%t err=%v want=%v", appended, err, test.want)
			}
		})
	}
	t.Run("row_mutation_rolls_back", func(t *testing.T) {
		fixture := newNativeQueuedModelOutcomeFixture(t, newTestSQLStore(t), "session-model-outcome-mutation", "run-model-outcome-mutation")
		if _, err := fixture.store.db.ExecContext(context.Background(), `CREATE TRIGGER mutate_model_outcome AFTER INSERT ON native_queued_model_invocation_outcomes
			BEGIN UPDATE native_queued_model_invocation_outcomes SET outcome_sha256 = '`+strings.Repeat("0", 64)+`' WHERE session_id = NEW.session_id AND run_id = NEW.run_id AND invocation_id = NEW.invocation_id; END`); err != nil {
			t.Fatal(err)
		}
		if appended, err := fixture.store.AppendNativeQueuedModelOutcomeFenced(context.Background(), fixture.fence, fixture.version, fixture.events); appended || !errors.Is(err, ErrCompletedToolResultProofInvalid) {
			t.Fatalf("append=%t err=%v", appended, err)
		}
		loaded, _ := fixture.store.Load(context.Background(), fixture.session.ID())
		if loaded.Version() != fixture.version {
			t.Fatalf("version=%d", loaded.Version())
		}
	})
	for _, test := range []struct {
		name, trigger string
		want          error
	}{
		{name: "chunk_mutation", trigger: `CREATE TRIGGER mutate_model_outcome_chunk AFTER INSERT ON event_chunks
			BEGIN UPDATE event_chunks SET payload = '{}\n' WHERE session_id = NEW.session_id AND start_seq = NEW.start_seq; END`, want: ErrCompletedToolResultProofInvalid},
		{name: "attempt_mutation", trigger: `CREATE TRIGGER mutate_model_outcome_attempt AFTER INSERT ON native_queued_model_invocation_outcomes
			BEGIN UPDATE native_queued_model_invocations SET request_sha256 = '` + strings.Repeat("0", 64) + `' WHERE session_id = NEW.session_id AND run_id = NEW.run_id AND invocation_id = NEW.invocation_id; END`, want: ErrCompletedToolResultProofInvalid},
		{name: "fence_loss", trigger: `CREATE TRIGGER mutate_model_outcome_fence AFTER INSERT ON native_queued_model_invocation_outcomes
			BEGIN UPDATE run_queue SET worker_id = 'worker-replaced' WHERE run_id = NEW.run_id; END`, want: ErrSessionWriteFenceLost},
	} {
		t.Run(test.name+"_rolls_back", func(t *testing.T) {
			fixture := newNativeQueuedModelOutcomeFixture(t, newTestSQLStore(t), "session-model-"+test.name, "run-model-"+test.name)
			if _, err := fixture.store.db.ExecContext(context.Background(), test.trigger); err != nil {
				t.Fatal(err)
			}
			if appended, err := fixture.store.AppendNativeQueuedModelOutcomeFenced(context.Background(), fixture.fence, fixture.version, fixture.events); appended || !errors.Is(err, test.want) {
				t.Fatalf("append=%t err=%v want=%v", appended, err, test.want)
			}
			loaded, _ := fixture.store.Load(context.Background(), fixture.session.ID())
			if loaded.Version() != fixture.version {
				t.Fatalf("version=%d", loaded.Version())
			}
			var rows int
			if err := fixture.store.db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM native_queued_model_invocation_outcomes").Scan(&rows); err != nil || rows != 0 {
				t.Fatalf("rows=%d err=%v", rows, err)
			}
		})
	}
}

func TestSQLSchemaV45MigratesNativeQueuedModelOutcomes(t *testing.T) {
	store := newTestSQLStore(t)
	ctx := context.Background()
	if _, err := store.db.ExecContext(ctx, "DROP TABLE native_queued_model_invocation_outcomes"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(ctx, sqlUpdateMetaRow.bind(store.dialect), "45"); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSQLSessionStore(ctx, store.db, store.dialect); err != nil {
		t.Fatal(err)
	}
	var rows int
	if err := store.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM native_queued_model_invocation_outcomes").Scan(&rows); err != nil || rows != 0 {
		t.Fatalf("rows=%d err=%v", rows, err)
	}
}

func TestPostgresSQLSessionStoreAppendNativeQueuedModelOutcomeFenced(t *testing.T) {
	ctx := newPostgresFenceTestContext(t)
	db := newPostgresTestDB(t)
	store, err := OpenSQLSessionStore(ctx, db, SQLDialectPostgres)
	if err != nil {
		t.Fatal(err)
	}
	fixture := newNativeQueuedModelOutcomeFixture(t, store, "session-pg-model-outcome", "run-pg-model-outcome")
	if appended, err := store.AppendNativeQueuedModelOutcomeFenced(ctx, fixture.fence, fixture.version, fixture.events); err != nil || !appended {
		t.Fatalf("append=%t err=%v", appended, err)
	}
	assertNativeQueuedModelOutcome(t, fixture)
	if appended, err := store.AppendNativeQueuedModelOutcomeFenced(ctx, advanceNativeQueuedModelGeneration(t, fixture.nativeQueuedModelFixture), fixture.version, fixture.events); err != nil || appended {
		t.Fatalf("convergence append=%t err=%v", appended, err)
	}
	t.Run("new_generation_cannot_first_deliver", func(t *testing.T) {
		candidate := newNativeQueuedModelOutcomeFixture(t, store, "session-pg-model-other-owner", "run-pg-model-other-owner")
		if appended, err := store.AppendNativeQueuedModelOutcomeFenced(ctx, advanceNativeQueuedModelGeneration(t, candidate.nativeQueuedModelFixture), candidate.version, candidate.events); appended || !errors.Is(err, ErrSessionWriteFenceLost) {
			t.Fatalf("append=%t err=%v", appended, err)
		}
	})
	for _, test := range []struct {
		name, table, body string
		want              error
	}{
		{name: "chunk_mutation", table: "event_chunks", body: `NEW.payload := '{}\n';`, want: ErrCompletedToolResultProofInvalid},
		{name: "outcome_mutation", table: "native_queued_model_invocation_outcomes", body: `NEW.outcome_sha256 := '` + strings.Repeat("0", 64) + `';`, want: ErrCompletedToolResultProofInvalid},
		{name: "attempt_mutation", table: "native_queued_model_invocation_outcomes", body: `UPDATE native_queued_model_invocations SET request_sha256 = '` + strings.Repeat("0", 64) + `' WHERE session_id = NEW.session_id AND run_id = NEW.run_id AND invocation_id = NEW.invocation_id;`, want: ErrCompletedToolResultProofInvalid},
		{name: "fence_loss", table: "native_queued_model_invocation_outcomes", body: `UPDATE run_queue SET worker_id = 'worker-replaced' WHERE run_id = NEW.run_id;`, want: ErrSessionWriteFenceLost},
	} {
		t.Run(test.name+"_rolls_back", func(t *testing.T) {
			candidate := newNativeQueuedModelOutcomeFixture(t, store, "session-pg-model-outcome-"+test.name, "run-pg-model-outcome-"+test.name)
			function := "model_outcome_" + test.name
			trigger := function + "_trigger"
			if _, err := db.ExecContext(ctx, fmt.Sprintf(`CREATE OR REPLACE FUNCTION %s() RETURNS trigger AS $$ BEGIN %s RETURN NEW; END; $$ LANGUAGE plpgsql`, function, test.body)); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, fmt.Sprintf("CREATE TRIGGER %s BEFORE INSERT ON %s FOR EACH ROW EXECUTE FUNCTION %s()", trigger, test.table, function)); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				_, _ = db.ExecContext(context.Background(), fmt.Sprintf("DROP TRIGGER IF EXISTS %s ON %s", trigger, test.table))
				_, _ = db.ExecContext(context.Background(), fmt.Sprintf("DROP FUNCTION IF EXISTS %s()", function))
			})
			if appended, err := store.AppendNativeQueuedModelOutcomeFenced(ctx, candidate.fence, candidate.version, candidate.events); appended || !errors.Is(err, test.want) {
				t.Fatalf("append=%t err=%v want=%v", appended, err, test.want)
			}
			loaded, _ := store.Load(ctx, candidate.session.ID())
			if loaded.Version() != candidate.version {
				t.Fatalf("version=%d", loaded.Version())
			}
		})
	}
}

func TestPostgresSchemaV45MigratesNativeQueuedModelOutcomes(t *testing.T) {
	ctx := newPostgresFenceTestContext(t)
	db := newPostgresTestDB(t)
	if _, err := OpenSQLSessionStore(ctx, db, SQLDialectPostgres); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "DROP TABLE native_queued_model_invocation_outcomes"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, sqlUpdateMetaRow.bind(SQLDialectPostgres), "45"); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSQLSessionStore(ctx, db, SQLDialectPostgres); err != nil {
		t.Fatal(err)
	}
}

func ExampleSQLSessionStore_AppendNativeQueuedModelOutcomeFenced() {
	fmt.Println("historical outcome only")
	// Output: historical outcome only
}
