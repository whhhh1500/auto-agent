package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
)

func TestSQLSessionStoreRejectsDamagedCommittedPrefix(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, *sqlSessionPrefixFixture)
	}{
		{
			name: "missing last chunk",
			mutate: func(t *testing.T, fixture *sqlSessionPrefixFixture) {
				deleteSessionChunk(t, fixture, 2)
			},
		},
		{
			name: "missing all chunks",
			mutate: func(t *testing.T, fixture *sqlSessionPrefixFixture) {
				if _, err := fixture.store.db.ExecContext(context.Background(), sqlQuery{
					`DELETE FROM event_chunks WHERE session_id = ?`,
				}.bind(fixture.store.dialect), fixture.session.ID()); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "invalid JSON payload",
			mutate: func(t *testing.T, fixture *sqlSessionPrefixFixture) {
				if _, err := fixture.store.db.ExecContext(context.Background(), sqlQuery{
					`UPDATE event_chunks SET payload = '{' WHERE session_id = ? AND start_seq = ?`,
				}.bind(fixture.store.dialect), fixture.session.ID(), 1); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "empty final chunk",
			mutate: func(t *testing.T, fixture *sqlSessionPrefixFixture) {
				if _, err := fixture.store.db.ExecContext(context.Background(), sqlQuery{
					`UPDATE event_chunks SET payload = '' WHERE session_id = ? AND start_seq = ?`,
				}.bind(fixture.store.dialect), fixture.session.ID(), 2); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "chunk gap",
			mutate: func(t *testing.T, fixture *sqlSessionPrefixFixture) {
				deleteSessionChunk(t, fixture, 1)
			},
		},
		{
			name: "event sequence gap",
			mutate: func(t *testing.T, fixture *sqlSessionPrefixFixture) {
				corruptSessionChunkEventSequence(t, fixture, 1, 99)
			},
		},
		{
			name: "negative row version",
			mutate: func(t *testing.T, fixture *sqlSessionPrefixFixture) {
				if _, err := fixture.store.db.ExecContext(context.Background(), sqlQuery{
					`UPDATE sessions SET version = -1 WHERE id = ?`,
				}.bind(fixture.store.dialect), fixture.session.ID()); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "version above core event cap",
			mutate: func(t *testing.T, fixture *sqlSessionPrefixFixture) {
				if _, err := fixture.store.db.ExecContext(context.Background(), sqlQuery{
					`UPDATE sessions SET version = ? WHERE id = ?`,
				}.bind(fixture.store.dialect), int64(core.MaxSessionEvents)+1, fixture.session.ID()); err != nil {
					t.Fatal(err)
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newSQLSessionPrefixFixture(t)
			test.mutate(t, fixture)
			err, panicked := loadSessionErrorWithoutPanic(fixture.store, fixture.session.ID())
			if panicked {
				t.Fatal("Load panicked on damaged committed session prefix")
			}
			if err == nil {
				t.Fatal("Load accepted a damaged committed session prefix")
			}
		})
	}
}

func TestSQLSessionStoreAllowsZeroVersionAndSurplusPrefix(t *testing.T) {
	t.Run("zero version", func(t *testing.T) {
		store := newTestSQLStore(t)
		session := mustNamedSession(t, "session-prefix-zero")
		if err := store.Create(context.Background(), session); err != nil {
			t.Fatal(err)
		}
		loaded, err := store.Load(context.Background(), session.ID())
		if err != nil {
			t.Fatal(err)
		}
		if loaded.Version() != 0 {
			t.Fatalf("zero-version load version=%d", loaded.Version())
		}
	})
	t.Run("surplus chunk remains outside committed prefix", func(t *testing.T) {
		fixture := newSQLSessionPrefixFixture(t)
		event := core.SessionEvent{
			Seq:   fixture.session.Version(),
			RunID: "run-prefix",
			Type:  core.EvUserMessage,
			Data:  mustMarshalSessionPrefix(t, core.UserMessageData{Text: "uncommitted surplus"}),
		}
		payload, err := json.Marshal(event)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.store.db.ExecContext(context.Background(), sqlInsertChunk.bind(fixture.store.dialect), fixture.session.ID(), event.Seq, string(payload)+"\n"); err != nil {
			t.Fatal(err)
		}
		loaded, err := fixture.store.Load(context.Background(), fixture.session.ID())
		if err != nil {
			t.Fatal(err)
		}
		if loaded.Version() != fixture.session.Version() {
			t.Fatalf("committed prefix version=%d want=%d", loaded.Version(), fixture.session.Version())
		}
	})
}

func TestSQLSessionStoreRestoreAcceptsStaleCommittedPrefix(t *testing.T) {
	fixture := newSQLSessionPrefixFixture(t)
	row, err := fixture.store.loadSessionRow(context.Background(), fixture.session.ID())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.session.Append("run-prefix", core.EvUserMessage, core.UserMessageData{Text: "newly committed after row read"}); err != nil {
		t.Fatal(err)
	}
	if err := fixture.store.Save(context.Background(), fixture.session, row.Version); err != nil {
		t.Fatal(err)
	}
	loaded, restoredVersion, err := fixture.store.restore(context.Background(), fixture.session.ID(), row)
	if err != nil {
		t.Fatal(err)
	}
	if restoredVersion != row.Version || loaded.Version() != row.Version {
		t.Fatalf("stale prefix restoredVersion=%d sessionVersion=%d want=%d", restoredVersion, loaded.Version(), row.Version)
	}
}

func TestSQLSessionStoreFencedRestoreRejectsInvalidCommittedVersion(t *testing.T) {
	fixture := newSQLSessionPrefixFixture(t)
	options := core.SessionOptions{
		ID: fixture.session.ID(), ProfileID: fixture.session.ProfileID(), Principal: fixture.session.Principal(),
		Scope: fixture.session.Scope(), Metadata: fixture.session.Metadata(),
	}
	for _, committed := range []int64{-1, int64(core.MaxSessionEvents) + 1} {
		t.Run(fmt.Sprintf("version_%d", committed), func(t *testing.T) {
			tx, err := fixture.store.db.BeginTx(context.Background(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			err, panicked := restoreFencedSessionErrorWithoutPanic(fixture.store, tx, fixture.session.ID(), options, committed)
			if panicked {
				t.Fatal("restoreFencedSession panicked on invalid committed version")
			}
			if err == nil {
				t.Fatal("restoreFencedSession accepted invalid committed version")
			}
		})
	}
}

func TestPostgresSQLSessionStoreRestorePrefix(t *testing.T) {
	t.Run("damaged prefix and event sequence", func(t *testing.T) {
		fixture := newPostgresSQLSessionPrefixFixture(t)
		deleteSessionChunk(t, fixture, 2)
		if err, panicked := loadSessionErrorWithoutPanic(fixture.store, fixture.session.ID()); panicked || err == nil {
			t.Fatalf("short PostgreSQL prefix err=%v panicked=%t", err, panicked)
		}

		fixture = newPostgresSQLSessionPrefixFixture(t)
		corruptSessionChunkEventSequence(t, fixture, 1, 99)
		if err, panicked := loadSessionErrorWithoutPanic(fixture.store, fixture.session.ID()); panicked || err == nil {
			t.Fatalf("sequence-corrupt PostgreSQL prefix err=%v panicked=%t", err, panicked)
		}
	})
	t.Run("invalid versions are rejected by load and fenced restore", func(t *testing.T) {
		for _, committed := range []int64{-1, int64(core.MaxSessionEvents) + 1} {
			t.Run(fmt.Sprintf("version_%d", committed), func(t *testing.T) {
				fixture := newPostgresSQLSessionPrefixFixture(t)
				if _, err := fixture.store.db.ExecContext(context.Background(), sqlQuery{
					`UPDATE sessions SET version = ? WHERE id = ?`,
				}.bind(fixture.store.dialect), committed, fixture.session.ID()); err != nil {
					t.Fatal(err)
				}
				if err, panicked := loadSessionErrorWithoutPanic(fixture.store, fixture.session.ID()); panicked || err == nil {
					t.Fatalf("PostgreSQL Load invalid version err=%v panicked=%t", err, panicked)
				}

				tx, err := fixture.store.db.BeginTx(context.Background(), nil)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback()
				options := sessionPrefixOptions(fixture.session)
				if err, panicked := restoreFencedSessionErrorWithoutPanic(fixture.store, tx, fixture.session.ID(), options, committed); panicked || err == nil {
					t.Fatalf("PostgreSQL fenced invalid version err=%v panicked=%t", err, panicked)
				}
			})
		}
	})
	t.Run("zero surplus and stale row prefix", func(t *testing.T) {
		store, db := newPostgresSQLSessionPrefixStore(t)
		zero := mustNamedSession(t, "session-prefix-pg-zero")
		if err := store.Create(context.Background(), zero); err != nil {
			t.Fatal(err)
		}
		loaded, err := store.Load(context.Background(), zero.ID())
		if err != nil {
			t.Fatal(err)
		}
		if loaded.Version() != 0 {
			t.Fatalf("zero-version PostgreSQL load version=%d", loaded.Version())
		}

		fixture := newSQLSessionPrefixFixtureForStore(t, store, "session-prefix-pg-prefix")
		row, err := fixture.store.loadSessionRow(context.Background(), fixture.session.ID())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.session.Append("run-prefix", core.EvUserMessage, core.UserMessageData{Text: "later append"}); err != nil {
			t.Fatal(err)
		}
		if err := fixture.store.Save(context.Background(), fixture.session, row.Version); err != nil {
			t.Fatal(err)
		}
		stale, restoredVersion, err := fixture.store.restore(context.Background(), fixture.session.ID(), row)
		if err != nil {
			t.Fatal(err)
		}
		if restoredVersion != row.Version || stale.Version() != row.Version {
			t.Fatalf("stale PostgreSQL prefix restored=%d version=%d want=%d", restoredVersion, stale.Version(), row.Version)
		}

		surplus := core.SessionEvent{Seq: fixture.session.Version(), RunID: "run-prefix", Type: core.EvUserMessage, Data: mustMarshalSessionPrefix(t, core.UserMessageData{Text: "surplus"})}
		payload, err := json.Marshal(surplus)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(context.Background(), sqlInsertChunk.bind(SQLDialectPostgres), fixture.session.ID(), surplus.Seq, string(payload)+"\n"); err != nil {
			t.Fatal(err)
		}
		loaded, err = store.Load(context.Background(), fixture.session.ID())
		if err != nil {
			t.Fatal(err)
		}
		if loaded.Version() != fixture.session.Version() {
			t.Fatalf("surplus PostgreSQL prefix version=%d want=%d", loaded.Version(), fixture.session.Version())
		}
	})
}

type sqlSessionPrefixFixture struct {
	store   *SQLSessionStore
	session *core.Session
}

func newSQLSessionPrefixFixture(t *testing.T) *sqlSessionPrefixFixture {
	t.Helper()
	return newSQLSessionPrefixFixtureForStore(t, newTestSQLStore(t), "session-prefix")
}

func newSQLSessionPrefixFixtureForStore(t *testing.T, store *SQLSessionStore, sessionID string) *sqlSessionPrefixFixture {
	t.Helper()
	session := mustNamedSession(t, sessionID)
	ctx := context.Background()
	if err := store.Create(ctx, session); err != nil {
		t.Fatal(err)
	}
	for index, text := range []string{"one", "two", "three"} {
		if _, err := session.Append("run-prefix", core.EvUserMessage, core.UserMessageData{Text: text}); err != nil {
			t.Fatal(err)
		}
		if err := store.Save(ctx, session, int64(index)); err != nil {
			t.Fatal(err)
		}
	}
	return &sqlSessionPrefixFixture{store: store, session: session}
}

func newPostgresSQLSessionPrefixStore(t *testing.T) (*SQLSessionStore, *sql.DB) {
	t.Helper()
	db := newPostgresTestDB(t)
	store, err := OpenSQLSessionStore(context.Background(), db, SQLDialectPostgres)
	if err != nil {
		t.Fatal(err)
	}
	return store, db
}

func newPostgresSQLSessionPrefixFixture(t *testing.T) *sqlSessionPrefixFixture {
	t.Helper()
	store, _ := newPostgresSQLSessionPrefixStore(t)
	return newSQLSessionPrefixFixtureForStore(t, store, "session-prefix-pg")
}

func sessionPrefixOptions(session *core.Session) core.SessionOptions {
	return core.SessionOptions{
		ID: session.ID(), ProfileID: session.ProfileID(), Principal: session.Principal(),
		Scope: session.Scope(), Metadata: session.Metadata(),
	}
}

func deleteSessionChunk(t *testing.T, fixture *sqlSessionPrefixFixture, startSeq int64) {
	t.Helper()
	if _, err := fixture.store.db.ExecContext(context.Background(), sqlQuery{
		`DELETE FROM event_chunks WHERE session_id = ? AND start_seq = ?`,
	}.bind(fixture.store.dialect), fixture.session.ID(), startSeq); err != nil {
		t.Fatal(err)
	}
}

func corruptSessionChunkEventSequence(t *testing.T, fixture *sqlSessionPrefixFixture, startSeq, sequence int64) {
	t.Helper()
	var payload string
	if err := fixture.store.db.QueryRowContext(context.Background(), sqlQuery{
		`SELECT payload FROM event_chunks WHERE session_id = ? AND start_seq = ?`,
	}.bind(fixture.store.dialect), fixture.session.ID(), startSeq).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	var event core.SessionEvent
	if err := json.Unmarshal([]byte(payload), &event); err != nil {
		t.Fatal(err)
	}
	event.Seq = sequence
	encoded, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.db.ExecContext(context.Background(), sqlQuery{
		`UPDATE event_chunks SET payload = ? WHERE session_id = ? AND start_seq = ?`,
	}.bind(fixture.store.dialect), string(encoded)+"\n", fixture.session.ID(), startSeq); err != nil {
		t.Fatal(err)
	}
}

func loadSessionErrorWithoutPanic(store *SQLSessionStore, id string) (err error, panicked bool) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("Load panicked: %v", recovered)
			panicked = true
		}
	}()
	_, err = store.Load(context.Background(), id)
	return err, false
}

func restoreFencedSessionErrorWithoutPanic(store *SQLSessionStore, tx *sql.Tx, id string, options core.SessionOptions, committed int64) (err error, panicked bool) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("restoreFencedSession panicked: %v", recovered)
			panicked = true
		}
	}()
	_, err = store.restoreFencedSession(context.Background(), tx, id, options, committed)
	return err, false
}

func mustMarshalSessionPrefix(t *testing.T, value any) json.RawMessage {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}
