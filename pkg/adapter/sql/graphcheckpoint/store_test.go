package graphcheckpoint

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/whhhh1500/auto-agent/pkg/adapter/sql/sqlkit"
	"github.com/whhhh1500/auto-agent/pkg/extensions/graph"
	"github.com/whhhh1500/auto-agent/pkg/storage"
	_ "modernc.org/sqlite"
)

func TestSQLiteCreateCASReplayAndBoundedListing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "graph.db")
	firstDB, first := newSQLiteStore(t, path)
	defer firstDB.Close()
	secondDB, second := newSQLiteStore(t, path)
	defer secondDB.Close()
	initial, created := checkpointAndTransition(1, graph.CheckpointReady)
	createdCheckpoint, disposition, err := first.Create(context.Background(), initial, created)
	if err != nil || disposition != graph.CommitApplied || createdCheckpoint.Revision != 1 {
		t.Fatalf("create=%#v err=%v", createdCheckpoint, err)
	}
	if _, _, err := second.Create(context.Background(), initial, mutatedTransition(initial)); !errors.Is(err, graph.ErrCheckpointConflict) {
		t.Fatalf("mutated replay error=%v", err)
	}
	if _, disposition, err := second.Create(context.Background(), initial, created); err != nil || disposition != graph.CommitReplayed {
		t.Fatalf("exact create replay=%v", err)
	}
	if _, _, err := first.CompareAndSwap(context.Background(), initial.Key, 0, initial, created); !errors.Is(err, graph.ErrCheckpointConflict) {
		t.Fatalf("zero CAS error=%v", err)
	}
	next, transition := checkpointAndTransition(2, graph.CheckpointExecuting)
	transition.SourceSegmentID, transition.SourceHostGeneration = initial.SegmentID, initial.HostGeneration
	transition.SourceNodeID, transition.SourceAttemptID = initial.CurrentNodeID, initial.AttemptID
	wrongSource := graph.CloneTransition(transition)
	wrongSource.SourceNodeID = "other-node"
	if _, _, err := first.CompareAndSwap(context.Background(), initial.Key, 1, next, wrongSource); !errors.Is(err, graph.ErrInvalidTransition) {
		t.Fatalf("CAS source mismatch error=%v", err)
	}
	if got, err := first.ListTransitions(context.Background(), initial.Key, 0, 2); err != nil || len(got) != 1 {
		t.Fatalf("source mismatch mutated history=%#v err=%v", got, err)
	}
	if _, disposition, err := first.CompareAndSwap(context.Background(), initial.Key, 1, next, transition); err != nil || disposition != graph.CommitApplied {
		t.Fatalf("cas=%v", err)
	}
	if _, disposition, err := second.CompareAndSwap(context.Background(), initial.Key, 1, next, transition); err != nil || disposition != graph.CommitReplayed {
		t.Fatalf("exact cas replay=%v", err)
	}
	mutated := graph.CloneCheckpoint(next)
	mutated.State["value"] = []byte(`"changed"`)
	if _, _, err := second.CompareAndSwap(context.Background(), initial.Key, 1, mutated, transition); !errors.Is(err, graph.ErrCheckpointConflict) {
		t.Fatalf("stale CAS error=%v", err)
	}
	transitions, err := first.ListTransitions(context.Background(), initial.Key, 0, 2)
	if err != nil || len(transitions) != 2 || transitions[0].Revision != 1 || transitions[1].Revision != 2 {
		t.Fatalf("transitions=%#v err=%v", transitions, err)
	}
	if _, err := first.ListTransitions(context.Background(), initial.Key, 0, graph.MaxTransitionPageSize+1); !errors.Is(err, graph.ErrInvalidTransition) {
		t.Fatalf("unbounded list error=%v", err)
	}
}

func TestDocumentBoundsRejectOversizeBeforeDecode(t *testing.T) {
	if _, err := decodeCheckpoint(bytes.Repeat([]byte("x"), maxCheckpointDocumentBytes+1)); !errors.Is(err, graph.ErrInvalidCheckpoint) {
		t.Fatalf("oversize checkpoint error=%v", err)
	}
	if _, err := decodeTransition(bytes.Repeat([]byte("x"), maxTransitionDocumentBytes+1)); !errors.Is(err, graph.ErrInvalidTransition) {
		t.Fatalf("oversize transition error=%v", err)
	}
}

func TestSQLiteCheckpointVersionsValidateAndListMetadataOnly(t *testing.T) {
	db, store := newSQLiteStore(t, filepath.Join(t.TempDir(), "versions.db"))
	defer db.Close()
	checkpoint, transition := checkpointAndTransition(1, graph.CheckpointReady)
	if _, _, err := store.Create(context.Background(), checkpoint, transition); err != nil {
		t.Fatal(err)
	}
	next, nextTransition := checkpointAndTransition(2, graph.CheckpointExecuting)
	nextTransition.SourceSegmentID, nextTransition.SourceHostGeneration = checkpoint.SegmentID, checkpoint.HostGeneration
	nextTransition.SourceNodeID, nextTransition.SourceAttemptID = checkpoint.CurrentNodeID, checkpoint.AttemptID
	if _, _, err := store.CompareAndSwap(context.Background(), checkpoint.Key, 1, next, nextTransition); err != nil {
		t.Fatal(err)
	}
	versions, err := store.ListVersions(context.Background(), checkpoint.Key, 0, 2)
	if err != nil || len(versions) != 2 || versions[0].Revision != 1 || versions[0].Origin != graph.CheckpointVersionOriginCommit || versions[0].ParentID != "" ||
		versions[1].Revision != 2 || versions[1].Origin != graph.CheckpointVersionOriginCommit || versions[1].ParentID != graph.CheckpointVersionID(checkpoint.Key, 1) {
		t.Fatalf("versions=%#v err=%v", versions, err)
	}
	loaded, err := store.LoadVersion(context.Background(), checkpoint.Key, 2)
	if err != nil || !reflect.DeepEqual(loaded.Checkpoint, next) || !reflect.DeepEqual(loaded.Info, versions[1]) {
		t.Fatalf("version=%#v err=%v", loaded, err)
	}
	if _, err := store.ListVersions(context.Background(), checkpoint.Key, 0, graph.MaxCheckpointVersionPageSize+1); !errors.Is(err, graph.ErrInvalidCheckpoint) {
		t.Fatalf("unbounded version list error=%v", err)
	}
	if _, err := store.LoadVersion(context.Background(), checkpoint.Key, 0); !errors.Is(err, graph.ErrInvalidCheckpoint) {
		t.Fatalf("zero version error=%v", err)
	}
	if _, err := store.LoadVersion(context.Background(), checkpoint.Key, 3); !errors.Is(err, graph.ErrCheckpointNotFound) {
		t.Fatalf("missing version error=%v", err)
	}

	if _, err := db.Exec(`UPDATE graph_checkpoint_versions SET version_id = ? WHERE revision = 1`, strings.Repeat("a", 64)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.LoadVersion(context.Background(), checkpoint.Key, 1); !errors.Is(err, graph.ErrInvalidCheckpoint) {
		t.Fatalf("corrupt version ID error=%v", err)
	}
	if _, err := db.Exec(`UPDATE graph_checkpoint_versions SET version_id = ? WHERE revision = 1`, versions[0].ID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE graph_checkpoint_versions SET checkpoint_hash = ? WHERE revision = 1`, strings.Repeat("g", 64)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ListVersions(context.Background(), checkpoint.Key, 0, 2); !errors.Is(err, graph.ErrInvalidCheckpoint) {
		t.Fatalf("corrupt version hash list error=%v", err)
	}
	if _, err := db.Exec(`UPDATE graph_checkpoint_versions SET checkpoint_hash = ? WHERE revision = 1`, versions[0].CheckpointHash); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE graph_checkpoint_versions SET checkpoint_json = '{"unknown_field":true}' WHERE revision = 1`); err != nil {
		t.Fatal(err)
	}
	if listed, err := store.ListVersions(context.Background(), checkpoint.Key, 0, 2); err != nil || len(listed) != 2 {
		t.Fatalf("metadata-only versions=%#v err=%v", listed, err)
	}
	if _, err := store.LoadVersion(context.Background(), checkpoint.Key, 1); !errors.Is(err, graph.ErrInvalidCheckpoint) {
		t.Fatalf("corrupt version JSON error=%v", err)
	}
}

func TestSQLiteCheckpointVersionWriteRollback(t *testing.T) {
	db, store := newSQLiteStore(t, filepath.Join(t.TempDir(), "version-rollback.db"))
	defer db.Close()
	checkpoint, transition := checkpointAndTransition(1, graph.CheckpointReady)
	if _, err := db.Exec(`CREATE TRIGGER reject_checkpoint_version BEFORE INSERT ON graph_checkpoint_versions
		BEGIN SELECT RAISE(ABORT, 'reject checkpoint version'); END`); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Create(context.Background(), checkpoint, transition); err == nil {
		t.Fatal("version insertion failure accepted create")
	}
	assertSQLiteGraphFactCounts(t, db, 0, 0, 0)
	if _, err := db.Exec(`DROP TRIGGER reject_checkpoint_version`); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Create(context.Background(), checkpoint, transition); err != nil {
		t.Fatal(err)
	}
	next, nextTransition := checkpointAndTransition(2, graph.CheckpointExecuting)
	nextTransition.SourceSegmentID, nextTransition.SourceHostGeneration = checkpoint.SegmentID, checkpoint.HostGeneration
	nextTransition.SourceNodeID, nextTransition.SourceAttemptID = checkpoint.CurrentNodeID, checkpoint.AttemptID
	if _, err := db.Exec(`CREATE TRIGGER reject_checkpoint_transition BEFORE INSERT ON graph_transitions
		BEGIN SELECT RAISE(ABORT, 'reject checkpoint transition'); END`); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.CompareAndSwap(context.Background(), checkpoint.Key, 1, next, nextTransition); err == nil {
		t.Fatal("transition insertion failure accepted CAS")
	}
	assertSQLiteGraphFactCounts(t, db, 1, 1, 1)
	head, err := store.Load(context.Background(), checkpoint.Key)
	if err != nil || head.Revision != 1 {
		t.Fatalf("rolled-back head=%#v err=%v", head, err)
	}
	if _, err := store.LoadVersion(context.Background(), checkpoint.Key, 2); !errors.Is(err, graph.ErrCheckpointNotFound) {
		t.Fatalf("rolled-back version error=%v", err)
	}
}

func TestSQLiteCheckpointHistorySurvivesHeadLoss(t *testing.T) {
	db, store := newSQLiteStore(t, filepath.Join(t.TempDir(), "history-survives-head-loss.db"))
	defer db.Close()
	checkpoint, transition := checkpointAndTransition(1, graph.CheckpointReady)
	if _, _, err := store.Create(context.Background(), checkpoint, transition); err != nil {
		t.Fatal(err)
	}
	next, nextTransition := checkpointAndTransition(2, graph.CheckpointExecuting)
	nextTransition.SourceSegmentID, nextTransition.SourceHostGeneration = checkpoint.SegmentID, checkpoint.HostGeneration
	nextTransition.SourceNodeID, nextTransition.SourceAttemptID = checkpoint.CurrentNodeID, checkpoint.AttemptID
	if _, _, err := store.CompareAndSwap(context.Background(), checkpoint.Key, 1, next, nextTransition); err != nil {
		t.Fatal(err)
	}
	disableSQLiteHistoryHeadFence(t, db)
	if _, err := db.Exec(`UPDATE graph_checkpoints SET checkpoint_json = '{"unknown_field":true}'`); err != nil {
		t.Fatal(err)
	}
	if version, err := store.LoadVersion(context.Background(), checkpoint.Key, 1); err != nil || !reflect.DeepEqual(version.Checkpoint, checkpoint) {
		t.Fatalf("version with corrupt head=%#v err=%v", version, err)
	}
	if versions, err := store.ListVersions(context.Background(), checkpoint.Key, 0, 2); err != nil || len(versions) != 2 {
		t.Fatalf("versions with corrupt head=%#v err=%v", versions, err)
	}
	if _, err := db.Exec(`DELETE FROM graph_checkpoints`); err != nil {
		t.Fatal(err)
	}
	if version, err := store.LoadVersion(context.Background(), checkpoint.Key, 2); err != nil || !reflect.DeepEqual(version.Checkpoint, next) {
		t.Fatalf("version with missing head=%#v err=%v", version, err)
	}
	if versions, err := store.ListVersions(context.Background(), checkpoint.Key, 2, 2); err != nil || len(versions) != 0 {
		t.Fatalf("after final revision versions=%#v err=%v", versions, err)
	}
	missing := graph.CheckpointKey{TenantID: "tenant", SessionID: "session", RunID: "missing"}
	if _, err := store.LoadVersion(context.Background(), missing, 1); !errors.Is(err, graph.ErrCheckpointNotFound) {
		t.Fatalf("unknown version error=%v", err)
	}
	if _, err := store.ListVersions(context.Background(), missing, 0, 1); !errors.Is(err, graph.ErrCheckpointNotFound) {
		t.Fatalf("unknown versions error=%v", err)
	}
}

func TestSQLiteTwoHandleCompetingCASAppendsOneVersion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "competing-cas.db")
	firstDB, first := newSQLiteStore(t, path)
	defer firstDB.Close()
	secondDB, second := newSQLiteStore(t, path)
	defer secondDB.Close()
	assertCompetingCASOneWinner(t, first, second, first)
}

func assertSQLiteGraphFactCounts(t *testing.T, db *sql.DB, heads, versions, transitions int) {
	t.Helper()
	for table, want := range map[string]int{"graph_checkpoints": heads, "graph_checkpoint_versions": versions, "graph_transitions": transitions} {
		var got int
		if err := db.QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&got); err != nil || got != want {
			t.Fatalf("%s count=%d err=%v want=%d", table, got, err, want)
		}
	}
}

func TestSQLiteTwoHandleCreateSingleWinnerAndCorruptDocumentsFailClosed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "graph-race.db")
	firstDB, first := newSQLiteStore(t, path)
	defer firstDB.Close()
	secondDB, second := newSQLiteStore(t, path)
	defer secondDB.Close()
	checkpoint, transition := checkpointAndTransition(1, graph.CheckpointReady)
	start := make(chan struct{})
	errs := make(chan error, 2)
	var group sync.WaitGroup
	for _, store := range []*Store{first, second} {
		group.Add(1)
		go func(store *Store) {
			defer group.Done()
			<-start
			_, _, err := store.Create(context.Background(), checkpoint, transition)
			errs <- err
		}(store)
	}
	close(start)
	group.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("same create should be idempotent, got %v", err)
		}
	}
	disableSQLiteHistoryHeadFence(t, firstDB)
	if _, err := firstDB.Exec(`UPDATE graph_checkpoints SET checkpoint_json = '{"unknown_secret":"not-allowed"}'`); err != nil {
		t.Fatal(err)
	}
	if _, err := first.Load(context.Background(), checkpoint.Key); !errors.Is(err, graph.ErrInvalidCheckpoint) {
		t.Fatalf("corrupt/unknown document error=%v", err)
	}
}

func TestSQLiteRejectsNilContextAndTamperedSplitColumns(t *testing.T) {
	db, store := newSQLiteStore(t, filepath.Join(t.TempDir(), "split.db"))
	defer db.Close()
	checkpoint, transition := checkpointAndTransition(1, graph.CheckpointReady)
	if _, _, err := store.Create(context.Background(), checkpoint, transition); err != nil {
		t.Fatal(err)
	}
	var nilContext context.Context
	if _, err := store.Load(nilContext, checkpoint.Key); !errors.Is(err, graph.ErrInvalidCheckpoint) {
		t.Fatalf("nil load error=%v", err)
	}
	disableSQLiteHistoryHeadFence(t, db)
	if _, err := db.Exec(`UPDATE graph_checkpoints SET revision = 2`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Load(context.Background(), checkpoint.Key); !errors.Is(err, graph.ErrInvalidCheckpoint) {
		t.Fatalf("checkpoint split column error=%v", err)
	}
	if _, err := db.Exec(`UPDATE graph_checkpoints SET revision = 1`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE graph_transitions SET transition_id = ?`, strings.Repeat("a", 64)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ListTransitions(context.Background(), checkpoint.Key, 0, 1); !errors.Is(err, graph.ErrInvalidTransition) {
		t.Fatalf("transition split column error=%v", err)
	}
	if _, err := store.ListTransitions(context.Background(), graph.CheckpointKey{TenantID: "tenant", SessionID: "session", RunID: "missing"}, 0, 1); !errors.Is(err, graph.ErrCheckpointNotFound) {
		t.Fatalf("missing list error=%v", err)
	}
}

func disableSQLiteHistoryHeadFence(t *testing.T, db *sql.DB) {
	t.Helper()
	for _, statement := range []string{
		`DROP TRIGGER IF EXISTS graph_checkpoint_history_head_insert`,
		`DROP TRIGGER IF EXISTS graph_checkpoint_history_head_update`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
}

func TestConstraintRecognitionIsUniqueOnly(t *testing.T) {
	for _, test := range []struct {
		err  error
		want bool
	}{
		{errors.New("UNIQUE constraint failed: graph_checkpoints"), true},
		{errors.New("PRIMARY KEY must be unique"), true},
		{errors.New("duplicate key value violates unique constraint"), true},
		{errors.New("CHECK constraint failed"), false},
		{errors.New("foreign key constraint failed"), false},
		{errors.New("not null constraint failed"), false},
		{errors.New("duplicate key value violates exclusion constraint"), false},
		{sqlStateTestError("23505"), true},
		{sqlStateTestError("23514"), false},
	} {
		if got := isConstraint(test.err); got != test.want {
			t.Fatalf("isConstraint(%v)=%t want %t", test.err, got, test.want)
		}
	}
}

func TestPostgresCreateCASAndReplay(t *testing.T) {
	db := newPostgresDB(t)
	store, err := New(db, sqlkit.Postgres)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint, transition := checkpointAndTransition(1, graph.CheckpointReady)
	if _, _, err := store.Create(context.Background(), checkpoint, transition); err != nil {
		t.Fatal(err)
	}
	next, nextTransition := checkpointAndTransition(2, graph.CheckpointExecuting)
	nextTransition.SourceSegmentID, nextTransition.SourceHostGeneration = checkpoint.SegmentID, checkpoint.HostGeneration
	nextTransition.SourceNodeID, nextTransition.SourceAttemptID = checkpoint.CurrentNodeID, checkpoint.AttemptID
	if _, _, err := store.CompareAndSwap(context.Background(), checkpoint.Key, 1, next, nextTransition); err != nil {
		t.Fatal(err)
	}
	if _, disposition, err := store.CompareAndSwap(context.Background(), checkpoint.Key, 1, next, nextTransition); err != nil || disposition != graph.CommitReplayed {
		t.Fatalf("postgres exact replay=%v", err)
	}
	versions, err := store.ListVersions(context.Background(), checkpoint.Key, 0, 2)
	if err != nil || len(versions) != 2 || versions[0].Origin != graph.CheckpointVersionOriginCommit || versions[1].ParentID != graph.CheckpointVersionID(checkpoint.Key, 1) || versions[1].Origin != graph.CheckpointVersionOriginCommit {
		t.Fatalf("postgres versions=%#v err=%v", versions, err)
	}
	if version, err := store.LoadVersion(context.Background(), checkpoint.Key, 2); err != nil || !reflect.DeepEqual(version.Checkpoint, next) {
		t.Fatalf("postgres version=%#v err=%v", version, err)
	}
}

func TestPostgresCheckpointVersionTransitionFailureRollsBack(t *testing.T) {
	db := newPostgresDB(t)
	store, err := New(db, sqlkit.Postgres)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint, transition := checkpointAndTransition(1, graph.CheckpointReady)
	if _, _, err := store.Create(context.Background(), checkpoint, transition); err != nil {
		t.Fatal(err)
	}
	next, nextTransition := checkpointAndTransition(2, graph.CheckpointExecuting)
	nextTransition.SourceSegmentID, nextTransition.SourceHostGeneration = checkpoint.SegmentID, checkpoint.HostGeneration
	nextTransition.SourceNodeID, nextTransition.SourceAttemptID = checkpoint.CurrentNodeID, checkpoint.AttemptID
	if _, err := db.Exec(`CREATE FUNCTION reject_graph_checkpoint_transition() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN RAISE EXCEPTION 'reject checkpoint transition'; END; $$`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TRIGGER reject_graph_checkpoint_transition BEFORE INSERT ON graph_transitions
		FOR EACH ROW EXECUTE FUNCTION reject_graph_checkpoint_transition()`); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.CompareAndSwap(context.Background(), checkpoint.Key, 1, next, nextTransition); err == nil {
		t.Fatal("transition insertion failure accepted CAS")
	}
	for table, want := range map[string]int{"graph_checkpoints": 1, "graph_checkpoint_versions": 1, "graph_transitions": 1} {
		var got int
		if err := db.QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&got); err != nil || got != want {
			t.Fatalf("%s count=%d err=%v want=%d", table, got, err, want)
		}
	}
}

func TestPostgresCompetingCASAppendsOneVersion(t *testing.T) {
	db := newPostgresDB(t)
	store, err := New(db, sqlkit.Postgres)
	if err != nil {
		t.Fatal(err)
	}
	assertCompetingCASOneWinner(t, store, store, store)
}

func assertCompetingCASOneWinner(t *testing.T, first, second, reader *Store) {
	t.Helper()
	checkpoint, transition := checkpointAndTransition(1, graph.CheckpointReady)
	if _, disposition, err := first.Create(context.Background(), checkpoint, transition); err != nil || disposition != graph.CommitApplied {
		t.Fatalf("create disposition=%v err=%v", disposition, err)
	}
	candidates := make([]graph.Checkpoint, 2)
	transitions := make([]graph.Transition, 2)
	for index, value := range []string{`"first"`, `"second"`} {
		next, nextTransition := checkpointAndTransition(2, graph.CheckpointExecuting)
		next.State["value"] = []byte(value)
		nextTransition.SourceSegmentID, nextTransition.SourceHostGeneration = checkpoint.SegmentID, checkpoint.HostGeneration
		nextTransition.SourceNodeID, nextTransition.SourceAttemptID = checkpoint.CurrentNodeID, checkpoint.AttemptID
		candidates[index], transitions[index] = next, nextTransition
	}
	type result struct {
		disposition graph.CommitDisposition
		err         error
	}
	start := make(chan struct{})
	results := make(chan result, 2)
	for index, store := range []*Store{first, second} {
		go func(store *Store, checkpoint graph.Checkpoint, transition graph.Transition) {
			<-start
			_, disposition, err := store.CompareAndSwap(context.Background(), checkpoint.Key, 1, checkpoint, transition)
			results <- result{disposition: disposition, err: err}
		}(store, candidates[index], transitions[index])
	}
	close(start)
	applied, conflict := 0, 0
	for range 2 {
		result := <-results
		switch {
		case result.err == nil && result.disposition == graph.CommitApplied:
			applied++
		case errors.Is(result.err, graph.ErrCheckpointConflict):
			conflict++
		default:
			t.Fatalf("competing CAS disposition=%v err=%v", result.disposition, result.err)
		}
	}
	if applied != 1 || conflict != 1 {
		t.Fatalf("competing CAS applied=%d conflict=%d", applied, conflict)
	}
	head, err := reader.Load(context.Background(), checkpoint.Key)
	if err != nil || head.Revision != 2 || (string(head.State["value"]) != `"first"` && string(head.State["value"]) != `"second"`) {
		t.Fatalf("competing CAS head=%#v err=%v", head, err)
	}
	versions, err := reader.ListVersions(context.Background(), checkpoint.Key, 0, 3)
	if err != nil || len(versions) != 2 || versions[0].Revision != 1 || versions[0].Origin != graph.CheckpointVersionOriginCommit || versions[1].Revision != 2 || versions[1].Origin != graph.CheckpointVersionOriginCommit {
		t.Fatalf("competing CAS versions=%#v err=%v", versions, err)
	}
	storedTransitions, err := reader.ListTransitions(context.Background(), checkpoint.Key, 0, 3)
	if err != nil || len(storedTransitions) != 2 || storedTransitions[0].Revision != 1 || storedTransitions[1].Revision != 2 {
		t.Fatalf("competing CAS transitions=%#v err=%v", storedTransitions, err)
	}
}

func TestPostgresDivergentCreateAndStaleCASHaveZeroSideEffects(t *testing.T) {
	db := newPostgresDB(t)
	store, err := New(db, sqlkit.Postgres)
	if err != nil {
		t.Fatal(err)
	}
	first, transition := checkpointAndTransition(1, graph.CheckpointReady)
	second := graph.CloneCheckpoint(first)
	second.State["value"] = []byte(`"other"`)
	start, results := make(chan struct{}), make(chan error, 2)
	for _, checkpoint := range []graph.Checkpoint{first, second} {
		go func(checkpoint graph.Checkpoint) {
			<-start
			_, _, err := store.Create(context.Background(), checkpoint, transition)
			results <- err
		}(checkpoint)
	}
	close(start)
	success, conflict := 0, 0
	for range 2 {
		if err := <-results; err == nil {
			success++
		} else if errors.Is(err, graph.ErrCheckpointConflict) {
			conflict++
		} else {
			t.Fatal(err)
		}
	}
	if success != 1 || conflict != 1 {
		t.Fatalf("create success=%d conflict=%d", success, conflict)
	}
	current, err := store.Load(context.Background(), first.Key)
	if err != nil {
		t.Fatal(err)
	}
	next, nextTransition := checkpointAndTransition(2, graph.CheckpointExecuting)
	nextTransition.SourceSegmentID, nextTransition.SourceHostGeneration = current.SegmentID, current.HostGeneration
	nextTransition.SourceNodeID, nextTransition.SourceAttemptID = current.CurrentNodeID, current.AttemptID
	if _, _, err := store.CompareAndSwap(context.Background(), current.Key, 1, next, nextTransition); err != nil {
		t.Fatal(err)
	}
	mutated := graph.CloneCheckpoint(next)
	mutated.State["value"] = []byte(`"mutated"`)
	if _, _, err := store.CompareAndSwap(context.Background(), current.Key, 1, mutated, nextTransition); !errors.Is(err, graph.ErrCheckpointConflict) {
		t.Fatalf("mutated replay=%v", err)
	}
	transitions, err := store.ListTransitions(context.Background(), current.Key, 0, 2)
	if err != nil || len(transitions) != 2 {
		t.Fatalf("stale CAS history=%#v err=%v", transitions, err)
	}
}

func checkpointAndTransition(revision uint64, status graph.CheckpointStatus) (graph.Checkpoint, graph.Transition) {
	checkpoint := graph.Checkpoint{Key: graph.CheckpointKey{TenantID: "tenant", SessionID: "session", RunID: "run"}, SegmentID: "segment", GraphID: "graph", DefinitionRevision: "definition", CompositionRevision: "composition", ImplementationRevision: "implementation", CurrentNodeID: "node", Revision: revision, Status: status, Visits: map[string]int{"node": 1}, State: graph.State{"value": []byte(`"ok"`)}}
	from := graph.CheckpointStatus("")
	if revision > 1 {
		from = graph.CheckpointReady
	}
	if status == graph.CheckpointExecuting {
		checkpoint.Attempt, checkpoint.AttemptID = 1, "attempt"
	}
	transition := graph.Transition{ID: graph.TransitionID(checkpoint.Key, revision), Key: checkpoint.Key, Revision: revision, From: from, To: status, SegmentID: checkpoint.SegmentID, CurrentNodeID: checkpoint.CurrentNodeID, AttemptID: checkpoint.AttemptID}
	return checkpoint, transition
}

func mutatedTransition(checkpoint graph.Checkpoint) graph.Transition {
	return graph.Transition{ID: graph.TransitionID(checkpoint.Key, 1), Key: checkpoint.Key, Revision: 1, To: graph.CheckpointReady, SegmentID: checkpoint.SegmentID, CurrentNodeID: checkpoint.CurrentNodeID, OutcomeCode: "mutated"}
}

func newSQLiteStore(t *testing.T, path string) (*sql.DB, *Store) {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout%285000%29&_pragma=journal_mode%28WAL%29")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := storage.OpenSQLSessionStore(context.Background(), db, storage.SQLDialectSQLite); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	store, err := New(db, sqlkit.SQLite)
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	return db, store
}

func newPostgresDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("HARNESS_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("HARNESS_TEST_PG_DSN is not configured")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	admin := stdlib.OpenDB(*config)
	if err := admin.PingContext(ctx); err != nil {
		_ = admin.Close()
		t.Fatal(err)
	}
	schema := fmt.Sprintf("graphcheckpoint_test_%d", time.Now().UnixNano())
	if _, err := admin.ExecContext(ctx, "CREATE SCHEMA "+schema); err != nil {
		_ = admin.Close()
		t.Fatal(err)
	}
	testConfig, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	testConfig.RuntimeParams["search_path"] = schema
	db := stdlib.OpenDB(*testConfig)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := storage.OpenSQLSessionStore(ctx, db, storage.SQLDialectPostgres); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = db.Close()
		cleanup, stop := context.WithTimeout(context.Background(), 15*time.Second)
		defer stop()
		_, _ = admin.ExecContext(cleanup, "DROP SCHEMA "+schema+" CASCADE")
		_ = admin.Close()
	})
	return db
}

type sqlStateTestError string

func (err sqlStateTestError) Error() string    { return "sql state " + string(err) }
func (err sqlStateTestError) SQLState() string { return string(err) }
