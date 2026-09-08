package storage

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/whhhh1500/auto-agent/pkg/evaluation"
	moderncsqlite "modernc.org/sqlite"
)

var evaluationSQLiteBarrierDriverSequence atomic.Int64

func newEvaluationIntegrityFixture(t *testing.T, cases int) (*SQLEvaluationStore, evaluation.Dataset, evaluation.RunResult) {
	t.Helper()
	sessions := newTestSQLStore(t)
	store, err := NewSQLEvaluationStore(sessions.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	dataset := sqlEvaluationDataset()
	for index := 2; index <= cases; index++ {
		dataset.Cases = append(dataset.Cases, evaluation.Case{
			ID: "case-sql-" + string(rune('0'+index)), Input: "case input",
			Assertions: dataset.Cases[0].Assertions,
		})
	}
	stored, _, err := store.PutDataset(context.Background(), dataset)
	if err != nil {
		t.Fatal(err)
	}
	return store, stored, sqlEvaluationRun(stored)
}

func completeEvaluationRun(t *testing.T, run evaluation.RunResult, result evaluation.CaseResult, includeCases bool) evaluation.RunResult {
	t.Helper()
	run.Status = evaluation.RunCompleted
	run.Score, run.Passed, run.PassedCases = result.Score, result.Passed, 1
	run.CompletedAt = time.Now().UTC()
	if includeCases {
		run.Cases = []evaluation.CaseResult{result}
	}
	return run
}

func TestSQLEvaluationStoreCreateRunBindsImmutableDataset(t *testing.T) {
	ctx := context.Background()
	t.Run("revision and full case count", func(t *testing.T) {
		store, dataset, run := newEvaluationIntegrityFixture(t, 2)
		wrongRevision := run
		wrongRevision.ID = "eval_sql_wrong_dataset_revision"
		wrongRevision.DatasetRevision = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		if wrongRevision.DatasetRevision == dataset.Revision {
			t.Fatal("fixture revision unexpectedly matches")
		}
		if err := store.CreateRun(ctx, wrongRevision); err == nil {
			t.Fatal("CreateRun accepted a dataset revision that is not durably stored")
		}

		subset := run
		subset.ID, subset.TotalCases = "eval_sql_subset", 1
		if err := store.CreateRun(ctx, subset); err == nil {
			t.Fatal("CreateRun accepted a subset total case count")
		}
	})
	t.Run("run profile override remains valid", func(t *testing.T) {
		store, _, run := newEvaluationIntegrityFixture(t, 1)
		run.ID, run.ProfileID = "eval_sql_profile_override", "evaluation.override"
		if err := store.CreateRun(ctx, run); err != nil {
			t.Fatalf("CreateRun rejected an explicit valid run profile override: %v", err)
		}
	})
}

func TestSQLEvaluationStoreCaseResultsRequireDatasetMember(t *testing.T) {
	ctx := context.Background()
	store, _, run := newEvaluationIntegrityFixture(t, 1)
	if err := store.CreateRun(ctx, run); err != nil {
		t.Fatal(err)
	}
	outside := sqlEvaluationCaseResult()
	outside.CaseID = "case-outside"
	if err := store.RecordCaseResult(ctx, run.ID, outside); err == nil {
		t.Fatal("RecordCaseResult accepted a case outside the immutable dataset")
	}
	loaded, err := store.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.Cases) != 0 || loaded.Status != evaluation.RunRunning {
		t.Fatalf("nonmember result changed run: %#v", loaded)
	}
}

func TestSQLEvaluationStoreFinishRunBindsImmutableHeaderAndCases(t *testing.T) {
	ctx := context.Background()
	for name, mutate := range map[string]func(*evaluation.RunResult, evaluation.CaseResult){
		"tenant header": func(run *evaluation.RunResult, _ evaluation.CaseResult) { run.TenantID = "other" },
		"case payload": func(run *evaluation.RunResult, result evaluation.CaseResult) {
			run.Cases = []evaluation.CaseResult{result}
			run.Cases[0].Answer = "different persisted result"
		},
	} {
		t.Run(name, func(t *testing.T) {
			store, _, run := newEvaluationIntegrityFixture(t, 1)
			if err := store.CreateRun(ctx, run); err != nil {
				t.Fatal(err)
			}
			result := sqlEvaluationCaseResult()
			if err := store.RecordCaseResult(ctx, run.ID, result); err != nil {
				t.Fatal(err)
			}
			finished := completeEvaluationRun(t, run, result, true)
			mutate(&finished, result)
			if err := store.FinishRun(ctx, finished); err == nil {
				t.Fatal("FinishRun accepted a header or case payload different from durable state")
			}
			loaded, err := store.GetRun(ctx, run.ID)
			if err != nil {
				t.Fatal(err)
			}
			if loaded.Status != evaluation.RunRunning || len(loaded.Cases) != 1 {
				t.Fatalf("rejected finish changed durable run: %#v", loaded)
			}
		})
	}
}

func TestSQLEvaluationStoreFinishRunAllowsEmptyCasesButBindsTerminalReplay(t *testing.T) {
	ctx := context.Background()
	store, _, run := newEvaluationIntegrityFixture(t, 1)
	if err := store.CreateRun(ctx, run); err != nil {
		t.Fatal(err)
	}
	result := sqlEvaluationCaseResult()
	if err := store.RecordCaseResult(ctx, run.ID, result); err != nil {
		t.Fatal(err)
	}
	finished := completeEvaluationRun(t, run, result, false)
	if err := store.FinishRun(ctx, finished); err != nil {
		t.Fatalf("empty FinishRun cases must use durable rows: %v", err)
	}
	persisted, err := store.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	replay := finished
	replay.CreatedAt = replay.CreatedAt.Add(time.Millisecond)
	replay.CompletedAt = replay.CompletedAt.Add(time.Millisecond)
	if err := store.FinishRun(ctx, replay); err != nil {
		t.Fatalf("terminal replay with fresh request timestamps must be a no-op: %v", err)
	}
	afterReplay, err := store.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !afterReplay.CreatedAt.Equal(persisted.CreatedAt) || !afterReplay.CompletedAt.Equal(persisted.CompletedAt) {
		t.Fatalf("terminal replay rewrote durable timestamps: before=%#v after=%#v", persisted, afterReplay)
	}
}

func TestSQLEvaluationStoreGetRunRejectsInvalidPersistedEvidence(t *testing.T) {
	ctx := context.Background()
	t.Run("foreign case", func(t *testing.T) {
		sessions := newTestSQLStore(t)
		store, err := NewSQLEvaluationStore(sessions.db, SQLDialectSQLite)
		if err != nil {
			t.Fatal(err)
		}
		dataset, _, err := store.PutDataset(ctx, sqlEvaluationDataset())
		if err != nil {
			t.Fatal(err)
		}
		run := sqlEvaluationRun(dataset)
		if err := store.CreateRun(ctx, run); err != nil {
			t.Fatal(err)
		}
		result := sqlEvaluationCaseResult()
		result.CaseID = "case-foreign"
		raw, err := json.Marshal(result)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := sessions.db.ExecContext(ctx, `INSERT INTO evaluation_case_results
			(run_id, case_id, result_json, score, passed, composition_revision, assignment_revision, completed_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, run.ID, result.CaseID, string(raw), result.Score, boolInt(result.Passed),
			result.Artifacts.CompositionRevision, result.Artifacts.AssignmentRevision, result.CompletedAt.UnixMilli()); err != nil {
			t.Fatal(err)
		}
		if _, err := store.GetRun(ctx, run.ID); err == nil {
			t.Fatal("GetRun accepted a persisted foreign case result")
		}
	})
	t.Run("completed partial result set", func(t *testing.T) {
		sessions := newTestSQLStore(t)
		store, err := NewSQLEvaluationStore(sessions.db, SQLDialectSQLite)
		if err != nil {
			t.Fatal(err)
		}
		dataset := sqlEvaluationDataset()
		dataset.Cases = append(dataset.Cases, evaluation.Case{ID: "case-sql-2", Input: "two", Assertions: dataset.Cases[0].Assertions})
		dataset, _, err = store.PutDataset(ctx, dataset)
		if err != nil {
			t.Fatal(err)
		}
		run := sqlEvaluationRun(dataset)
		if err := store.CreateRun(ctx, run); err != nil {
			t.Fatal(err)
		}
		if _, err := sessions.db.ExecContext(ctx, `UPDATE evaluation_runs
			SET status = 'completed', completed_at = ? WHERE id = ?`, time.Now().UTC().UnixMilli(), run.ID); err != nil {
			t.Fatal(err)
		}
		if _, err := store.GetRun(ctx, run.ID); err == nil {
			t.Fatal("GetRun accepted a completed run without its full durable dataset membership")
		}
	})
}

func TestSQLEvaluationStoreRecordAndFinishSerialize(t *testing.T) {
	ctx := context.Background()
	t.Run("finish lock blocks late record", func(t *testing.T) {
		entered, release := make(chan struct{}), make(chan struct{})
		released := false
		defer func() {
			if !released {
				close(release)
			}
		}()
		store, db, run := newSQLiteEvaluationBarrierFixture(t, "finish", entered, release)
		installSQLiteEvaluationBarrier(t, db, `CREATE TRIGGER evaluation_finish_gate
			BEFORE UPDATE OF status ON evaluation_runs WHEN NEW.status = 'failed'
			BEGIN SELECT evaluation_barrier(); END`)
		finished := run
		finished.Status, finished.Error, finished.CompletedAt = evaluation.RunFailed, "stopped", time.Now().UTC()
		finishDone := make(chan error, 1)
		go func() { finishDone <- store.FinishRun(ctx, finished) }()
		awaitEvaluationBarrier(t, entered, "FinishRun did not acquire the run lock")
		recordDone := make(chan error, 1)
		go func() { recordDone <- store.RecordCaseResult(ctx, run.ID, sqlEvaluationCaseResult()) }()
		recordBusy := observeEvaluationBarrierContender(t, recordDone, "late RecordCaseResult bypassed a locked FinishRun")
		close(release)
		released = true
		if err := <-finishDone; err != nil {
			t.Fatal(err)
		}
		if recordBusy {
			err := store.RecordCaseResult(ctx, run.ID, sqlEvaluationCaseResult())
			if err == nil {
				t.Fatal("late RecordCaseResult retry was accepted after failed terminal FinishRun")
			}
		} else if err := <-recordDone; err == nil {
			t.Fatal("late RecordCaseResult was accepted after failed terminal FinishRun")
		}
		loaded, err := store.GetRun(ctx, run.ID)
		if err != nil || loaded.Status != evaluation.RunFailed || len(loaded.Cases) != 0 {
			t.Fatalf("finish-first serialization wrong: %#v err=%v", loaded, err)
		}
	})
	t.Run("record lock commits before finish", func(t *testing.T) {
		entered, release := make(chan struct{}), make(chan struct{})
		released := false
		defer func() {
			if !released {
				close(release)
			}
		}()
		store, db, run := newSQLiteEvaluationBarrierFixture(t, "record", entered, release)
		installSQLiteEvaluationBarrier(t, db, `CREATE TRIGGER evaluation_record_gate
			BEFORE INSERT ON evaluation_case_results BEGIN SELECT evaluation_barrier(); END`)
		recordDone := make(chan error, 1)
		go func() { recordDone <- store.RecordCaseResult(ctx, run.ID, sqlEvaluationCaseResult()) }()
		awaitEvaluationBarrier(t, entered, "RecordCaseResult did not acquire the run lock")
		finished := run
		finished.Status, finished.Error, finished.CompletedAt = evaluation.RunFailed, "stopped", time.Now().UTC()
		finishDone := make(chan error, 1)
		go func() { finishDone <- store.FinishRun(ctx, finished) }()
		finishBusy := observeEvaluationBarrierContender(t, finishDone, "FinishRun bypassed a locked RecordCaseResult")
		close(release)
		released = true
		if err := <-recordDone; err != nil {
			t.Fatal(err)
		}
		if finishBusy {
			err := store.FinishRun(ctx, finished)
			if err != nil {
				t.Fatal(err)
			}
		} else if err := <-finishDone; err != nil {
			t.Fatal(err)
		}
		loaded, err := store.GetRun(ctx, run.ID)
		if err != nil || loaded.Status != evaluation.RunFailed || len(loaded.Cases) != 1 {
			t.Fatalf("record-first serialization wrong: %#v err=%v", loaded, err)
		}
	})
}

func TestPostgresSQLEvaluationStoreRecordAndFinishSerialize(t *testing.T) {
	ctx := context.Background()
	t.Run("finish lock blocks late record", func(t *testing.T) {
		store, db, run, gatePID, releaseGate := newPostgresEvaluationBarrierFixture(t, "finish")
		finished := run
		finished.Status, finished.Error, finished.CompletedAt = evaluation.RunFailed, "stopped", time.Now().UTC()
		finishDone := make(chan error, 1)
		go func() { finishDone <- store.FinishRun(ctx, finished) }()
		awaitPostgresEvaluationGate(t, ctx, db, gatePID, "FinishRun did not reach its in-transaction gate")

		recordDone := make(chan error, 1)
		go func() { recordDone <- store.RecordCaseResult(ctx, run.ID, sqlEvaluationCaseResult()) }()
		awaitPostgresEvaluationContender(t, ctx, db, gatePID, "RecordCaseResult did not wait for FinishRun's run-row transaction")
		if err := releaseGate(); err != nil {
			t.Fatal(err)
		}
		if err := awaitEvaluationResult(finishDone); err != nil {
			t.Fatal(err)
		}
		if err := awaitEvaluationResult(recordDone); err == nil {
			t.Fatal("late RecordCaseResult was accepted after failed terminal FinishRun")
		}
		loaded, err := store.GetRun(ctx, run.ID)
		if err != nil || loaded.Status != evaluation.RunFailed || len(loaded.Cases) != 0 {
			t.Fatalf("finish-first serialization wrong: %#v err=%v", loaded, err)
		}
	})
	t.Run("record lock commits before finish", func(t *testing.T) {
		store, db, run, gatePID, releaseGate := newPostgresEvaluationBarrierFixture(t, "record")
		recordDone := make(chan error, 1)
		go func() { recordDone <- store.RecordCaseResult(ctx, run.ID, sqlEvaluationCaseResult()) }()
		awaitPostgresEvaluationGate(t, ctx, db, gatePID, "RecordCaseResult did not reach its in-transaction gate")

		finished := run
		finished.Status, finished.Error, finished.CompletedAt = evaluation.RunFailed, "stopped", time.Now().UTC()
		finishDone := make(chan error, 1)
		go func() { finishDone <- store.FinishRun(ctx, finished) }()
		awaitPostgresEvaluationContender(t, ctx, db, gatePID, "FinishRun did not wait for RecordCaseResult's run-row transaction")
		if err := releaseGate(); err != nil {
			t.Fatal(err)
		}
		if err := awaitEvaluationResult(recordDone); err != nil {
			t.Fatal(err)
		}
		if err := awaitEvaluationResult(finishDone); err != nil {
			t.Fatal(err)
		}
		loaded, err := store.GetRun(ctx, run.ID)
		if err != nil || loaded.Status != evaluation.RunFailed || len(loaded.Cases) != 1 {
			t.Fatalf("record-first serialization wrong: %#v err=%v", loaded, err)
		}
	})
}

const evaluationPostgresBarrierKey = 39039

func newPostgresEvaluationBarrierFixture(t *testing.T, kind string) (*SQLEvaluationStore, *sql.DB, evaluation.RunResult, int, func() error) {
	t.Helper()
	ctx := context.Background()
	db := newPostgresTestDB(t)
	if _, err := OpenSQLSessionStore(ctx, db, SQLDialectPostgres); err != nil {
		t.Fatal(err)
	}
	store, err := NewSQLEvaluationStore(db, SQLDialectPostgres)
	if err != nil {
		t.Fatal(err)
	}
	dataset, _, err := store.PutDataset(ctx, sqlEvaluationDataset())
	if err != nil {
		t.Fatal(err)
	}
	run := sqlEvaluationRun(dataset)
	run.ID = "eval_postgres_barrier_" + kind
	if err := store.CreateRun(ctx, run); err != nil {
		t.Fatal(err)
	}

	trigger := `CREATE TRIGGER evaluation_record_gate BEFORE INSERT ON evaluation_case_results
		FOR EACH ROW EXECUTE FUNCTION evaluation_barrier_gate()`
	if kind == "finish" {
		trigger = `CREATE TRIGGER evaluation_finish_gate BEFORE UPDATE OF status ON evaluation_runs
			FOR EACH ROW WHEN (NEW.status = 'failed') EXECUTE FUNCTION evaluation_barrier_gate()`
	}
	if _, err := db.ExecContext(ctx, `CREATE FUNCTION evaluation_barrier_gate() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN PERFORM pg_advisory_lock(hashtext(current_schema()), 39039); RETURN NEW; END; $$;`+trigger); err != nil {
		t.Fatal(err)
	}
	gate, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var held bool
	releaseGate := func() error {
		if !held {
			return nil
		}
		held = false
		_, err := gate.ExecContext(context.Background(), "SELECT pg_advisory_unlock(hashtext(current_schema()), $1)", evaluationPostgresBarrierKey)
		return err
	}
	t.Cleanup(func() {
		_ = releaseGate()
		_ = gate.Close()
	})
	var gatePID int
	if err := gate.QueryRowContext(ctx, "SELECT pg_backend_pid()").Scan(&gatePID); err != nil {
		t.Fatal(err)
	}
	if _, err := gate.ExecContext(ctx, "SELECT pg_advisory_lock(hashtext(current_schema()), $1)", evaluationPostgresBarrierKey); err != nil {
		t.Fatal(err)
	}
	held = true
	return store, db, run, gatePID, releaseGate
}

func awaitPostgresEvaluationGate(t *testing.T, ctx context.Context, db *sql.DB, gatePID int, message string) {
	t.Helper()
	awaitPostgresEvaluationLock(t, ctx, db, message, `SELECT COUNT(*) FROM pg_locks AS waiting
		WHERE waiting.locktype = 'advisory' AND NOT waiting.granted
			AND waiting.database = (SELECT oid FROM pg_database WHERE datname = current_database())
			AND waiting.classid = hashtext(current_schema())::oid
			AND waiting.objid = 39039::oid AND waiting.objsubid = 2
			AND $1 = ANY(pg_blocking_pids(waiting.pid))`, gatePID)
}

func awaitPostgresEvaluationContender(t *testing.T, ctx context.Context, db *sql.DB, gatePID int, message string) {
	t.Helper()
	awaitPostgresEvaluationLock(t, ctx, db, message, `WITH gated AS (
			SELECT waiting.pid FROM pg_locks AS waiting
			WHERE waiting.locktype = 'advisory' AND NOT waiting.granted
				AND waiting.database = (SELECT oid FROM pg_database WHERE datname = current_database())
				AND waiting.classid = hashtext(current_schema())::oid
				AND waiting.objid = 39039::oid AND waiting.objsubid = 2
				AND $1 = ANY(pg_blocking_pids(waiting.pid))
		)
		SELECT COUNT(*) FROM pg_locks AS contender CROSS JOIN gated
		WHERE contender.locktype = 'transactionid' AND NOT contender.granted
			AND contender.pid <> gated.pid
			AND gated.pid = ANY(pg_blocking_pids(contender.pid))`, gatePID)
}

func awaitPostgresEvaluationLock(t *testing.T, ctx context.Context, db *sql.DB, message, query string, args ...any) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-deadline.C:
			t.Fatal(message)
		case <-ticker.C:
			var waiting int
			if err := db.QueryRowContext(ctx, query, args...).Scan(&waiting); err != nil {
				t.Fatal(err)
			}
			if waiting > 0 {
				return
			}
		}
	}
}

func awaitEvaluationResult(done <-chan error) error {
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		return errors.New("evaluation operation did not return after releasing its barrier")
	}
}

func newSQLiteEvaluationBarrierFixture(t *testing.T, suffix string, entered chan<- struct{}, release <-chan struct{}) (*SQLEvaluationStore, *sql.DB, evaluation.RunResult) {
	t.Helper()
	driverName := "evaluation_barrier_" + strconv.FormatInt(evaluationSQLiteBarrierDriverSequence.Add(1), 10)
	driverInstance := &moderncsqlite.Driver{}
	var once sync.Once
	driverInstance.MustRegisterScalarFunction("evaluation_barrier", 0, func(*moderncsqlite.FunctionContext, []driver.Value) (driver.Value, error) {
		once.Do(func() { close(entered) })
		<-release
		return nil, nil
	})
	sql.Register(driverName, driverInstance)
	db, err := sql.Open(driverName, t.TempDir()+"/evaluation-"+suffix+".db?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := OpenSQLSessionStore(context.Background(), db, SQLDialectSQLite); err != nil {
		t.Fatal(err)
	}
	store, err := NewSQLEvaluationStore(db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	dataset, _, err := store.PutDataset(context.Background(), sqlEvaluationDataset())
	if err != nil {
		t.Fatal(err)
	}
	run := sqlEvaluationRun(dataset)
	run.ID = "eval_sql_barrier_" + suffix
	if err := store.CreateRun(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	return store, db, run
}

func installSQLiteEvaluationBarrier(t *testing.T, db *sql.DB, trigger string) {
	t.Helper()
	if _, err := db.Exec(trigger); err != nil {
		t.Fatal(err)
	}
}

func awaitEvaluationBarrier(t *testing.T, entered <-chan struct{}, message string) {
	t.Helper()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal(message)
	}
}

func observeEvaluationBarrierContender(t *testing.T, done <-chan error, message string) bool {
	t.Helper()
	select {
	case err := <-done:
		if err != nil && strings.Contains(err.Error(), "database is locked") {
			return true
		}
		t.Fatalf("%s: %v", message, err)
	case <-time.After(100 * time.Millisecond):
		return false
	}
	return false
}
