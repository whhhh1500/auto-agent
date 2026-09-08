package graphcheckpoint

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	graph "github.com/whhhh1500/auto-agent/pkg/extensions/graph"
)

func TestStoreCASReplayAndDefensiveCopies(t *testing.T) {
	t.Parallel()
	store, err := New(2, 8)
	if err != nil {
		t.Fatal(err)
	}
	initial := checkpoint(1, graph.CheckpointReady)
	if _, disposition, err := store.Create(context.Background(), initial, transition(graph.Checkpoint{}, initial, "created")); err != nil || disposition != graph.CommitApplied {
		t.Fatalf("initial create disposition=%v err=%v", disposition, err)
	}
	if _, disposition, err := store.Create(context.Background(), initial, transition(graph.Checkpoint{}, initial, "created")); err != nil || disposition != graph.CommitReplayed {
		t.Fatalf("replayed create disposition=%v err=%v", disposition, err)
	}
	initial.State["value"] = json.RawMessage(`"mutated"`)
	loaded, err := store.Load(context.Background(), initial.Key)
	if err != nil {
		t.Fatal(err)
	}
	if string(loaded.State["value"]) != `"one"` {
		t.Fatalf("store aliased caller state: %s", loaded.State["value"])
	}

	next := loaded
	next.Status, next.Revision, next.CurrentNodeID, next.Attempt, next.AttemptID = graph.CheckpointExecuting, 2, "next", 1, "attempt-1"
	committed, disposition, err := store.CompareAndSwap(context.Background(), loaded.Key, 1, next, transition(loaded, next, "started"))
	if err != nil {
		t.Fatal(err)
	}
	if disposition != graph.CommitApplied {
		t.Fatalf("first CAS disposition = %v", disposition)
	}
	if committed.Revision != 2 {
		t.Fatalf("revision = %d", committed.Revision)
	}
	if _, disposition, err := store.CompareAndSwap(context.Background(), loaded.Key, 1, next, transition(loaded, next, "started")); err != nil || disposition != graph.CommitReplayed {
		t.Fatalf("idempotent replay disposition=%v err=%v", disposition, err)
	}
	mutated := next
	mutated.CurrentNodeID = "other"
	if _, _, err := store.CompareAndSwap(context.Background(), loaded.Key, 1, mutated, transition(loaded, mutated, "started")); !errors.Is(err, graph.ErrCheckpointConflict) {
		t.Fatalf("mutated replay error = %v", err)
	}
	if _, disposition, err := store.CompareAndSwap(context.Background(), loaded.Key, 1, next, transition(loaded, next, "started")); err != nil || disposition != graph.CommitReplayed {
		t.Fatalf("second idempotent replay disposition=%v err=%v", disposition, err)
	}
	transitions, err := store.ListTransitions(context.Background(), loaded.Key, 0, 8)
	if err != nil || len(transitions) != 2 {
		t.Fatalf("transitions = %d, %v", len(transitions), err)
	}
}

func TestStoreCASRequiresCurrentSourceEvidence(t *testing.T) {
	store, err := New(2, 8)
	if err != nil {
		t.Fatal(err)
	}
	initial := checkpoint(1, graph.CheckpointReady)
	if _, _, err := store.Create(context.Background(), initial, transition(graph.Checkpoint{}, initial, "created")); err != nil {
		t.Fatal(err)
	}
	next := initial
	next.Revision, next.Status, next.Attempt, next.AttemptID = 2, graph.CheckpointExecuting, 1, "attempt-1"
	missing := transition(initial, next, "started")
	missing.SourceSegmentID = ""
	missing.SourceHostGeneration = 0
	missing.SourceNodeID = ""
	missing.SourceAttemptID = ""
	if _, _, err := store.CompareAndSwap(context.Background(), initial.Key, initial.Revision, next, missing); !errors.Is(err, graph.ErrInvalidTransition) {
		t.Fatalf("missing source evidence error = %v", err)
	}
	valid := transition(initial, next, "started")
	valid.SourceSegmentID = initial.SegmentID
	valid.SourceHostGeneration = initial.HostGeneration
	valid.SourceNodeID = initial.CurrentNodeID
	valid.SourceAttemptID = initial.AttemptID
	if _, _, err := store.CompareAndSwap(context.Background(), initial.Key, initial.Revision, next, valid); err != nil {
		t.Fatalf("valid source evidence rejected: %v", err)
	}
}

func TestStoreStaleCASOneWinner(t *testing.T) {
	t.Parallel()
	store, err := New(2, 8)
	if err != nil {
		t.Fatal(err)
	}
	initial := checkpoint(1, graph.CheckpointReady)
	if _, _, err := store.Create(context.Background(), initial, transition(graph.Checkpoint{}, initial, "created")); err != nil {
		t.Fatal(err)
	}
	first := initial
	first.Revision, first.Status, first.Attempt, first.AttemptID = 2, graph.CheckpointExecuting, 1, "attempt-1"
	second := first
	second.CurrentNodeID = "different"
	if _, _, err := store.CompareAndSwap(context.Background(), initial.Key, 1, first, transition(initial, first, "started")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.CompareAndSwap(context.Background(), initial.Key, 1, second, transition(initial, second, "started")); !errors.Is(err, graph.ErrCheckpointConflict) {
		t.Fatalf("stale error = %v", err)
	}
}

func TestStoreHistoryIsImmutablePagedAndReplaySafe(t *testing.T) {
	store, err := New(2, 8)
	if err != nil {
		t.Fatal(err)
	}
	initial := checkpoint(1, graph.CheckpointReady)
	if _, disposition, err := store.Create(context.Background(), initial, transition(graph.Checkpoint{}, initial, "created")); err != nil || disposition != graph.CommitApplied {
		t.Fatalf("create disposition=%v err=%v", disposition, err)
	}
	next := initial
	next.Revision, next.Status, next.Attempt, next.AttemptID = 2, graph.CheckpointExecuting, 1, "attempt-1"
	committed, disposition, err := store.CompareAndSwap(context.Background(), initial.Key, 1, next, transition(initial, next, "started"))
	if err != nil || disposition != graph.CommitApplied {
		t.Fatalf("cas disposition=%v err=%v", disposition, err)
	}
	if _, disposition, err := store.CompareAndSwap(context.Background(), initial.Key, 1, next, transition(initial, next, "started")); err != nil || disposition != graph.CommitReplayed {
		t.Fatalf("replay disposition=%v err=%v", disposition, err)
	}

	infos, err := store.ListVersions(context.Background(), initial.Key, 0, 1)
	if err != nil || len(infos) != 1 || infos[0].Revision != 1 || infos[0].Origin != graph.CheckpointVersionOriginCommit || infos[0].ParentID != "" {
		t.Fatalf("first version infos=%#v err=%v", infos, err)
	}
	infos, err = store.ListVersions(context.Background(), initial.Key, 1, 8)
	if err != nil || len(infos) != 1 || infos[0].Revision != 2 || infos[0].Origin != graph.CheckpointVersionOriginCommit || infos[0].ParentID != graph.CheckpointVersionID(initial.Key, 1) {
		t.Fatalf("second version infos=%#v err=%v", infos, err)
	}
	if infos[0].CheckpointHash == "" || infos[0].CreatedAt <= 0 {
		t.Fatalf("version metadata lacks digest or timestamp: %#v", infos[0])
	}

	version, err := store.LoadVersion(context.Background(), initial.Key, 1)
	if err != nil || version.Checkpoint.Revision != 1 {
		t.Fatalf("load version=%#v err=%v", version, err)
	}
	version.Checkpoint.State["value"] = json.RawMessage(`"mutated"`)
	again, err := store.LoadVersion(context.Background(), initial.Key, 1)
	if err != nil || string(again.Checkpoint.State["value"]) != `"one"` {
		t.Fatalf("stored version was aliased: %#v err=%v", again, err)
	}
	if _, err := store.LoadVersion(context.Background(), initial.Key, 3); !errors.Is(err, graph.ErrCheckpointNotFound) {
		t.Fatalf("missing version error = %v", err)
	}
	if _, err := store.ListVersions(context.Background(), initial.Key, 0, 0); !errors.Is(err, graph.ErrInvalidCheckpoint) {
		t.Fatalf("invalid history page limit error = %v", err)
	}
	if committed.Revision != 2 {
		t.Fatalf("committed revision = %d", committed.Revision)
	}
}

func TestStoreHistoryListValidatesMetadataAndCancelledMutationDoesNotCommit(t *testing.T) {
	store, err := New(2, 8)
	if err != nil {
		t.Fatal(err)
	}
	initial := checkpoint(1, graph.CheckpointReady)
	if _, _, err := store.Create(context.Background(), initial, transition(graph.Checkpoint{}, initial, "created")); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	record := store.records[initial.Key]
	record.versions[1] = graph.CheckpointVersion{Info: graph.CheckpointVersionInfo{}}
	store.records[initial.Key] = record
	store.mu.Unlock()
	if _, err := store.ListVersions(context.Background(), initial.Key, 0, 1); !errors.Is(err, graph.ErrInvalidCheckpoint) {
		t.Fatalf("corrupt metadata list error = %v", err)
	}

	cancelled, cancel := context.WithCancel(context.Background())
	store.mu.Lock()
	result := make(chan error, 1)
	go func() {
		_, _, err := store.Create(cancelled, checkpoint(1, graph.CheckpointReady), transition(graph.Checkpoint{}, checkpoint(1, graph.CheckpointReady), "created"))
		result <- err
	}()
	time.Sleep(10 * time.Millisecond)
	cancel()
	store.mu.Unlock()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled create error = %v", err)
	}
}

func TestStoreHistoryCapacityFailureLeavesNoVersion(t *testing.T) {
	store, err := New(1, 1)
	if err != nil {
		t.Fatal(err)
	}
	initial := checkpoint(1, graph.CheckpointReady)
	if _, _, err := store.Create(context.Background(), initial, transition(graph.Checkpoint{}, initial, "created")); err != nil {
		t.Fatal(err)
	}
	next := initial
	next.Revision, next.Status, next.Attempt, next.AttemptID = 2, graph.CheckpointExecuting, 1, "attempt-1"
	if _, _, err := store.CompareAndSwap(context.Background(), initial.Key, 1, next, transition(initial, next, "started")); !errors.Is(err, graph.ErrCheckpointCapacity) {
		t.Fatalf("capacity error = %v", err)
	}
	head, err := store.Load(context.Background(), initial.Key)
	if err != nil || head.Revision != 1 {
		t.Fatalf("head after capacity failure = %#v err=%v", head, err)
	}
	versions, err := store.ListVersions(context.Background(), initial.Key, 0, 8)
	if err != nil || len(versions) != 1 || versions[0].Revision != 1 {
		t.Fatalf("versions after capacity failure = %#v err=%v", versions, err)
	}
}

func checkpoint(revision uint64, status graph.CheckpointStatus) graph.Checkpoint {
	return graph.Checkpoint{Key: graph.CheckpointKey{TenantID: "tenant", SessionID: "session", RunID: "run"}, SegmentID: "segment", GraphID: "g", DefinitionRevision: "definition", CompositionRevision: "composition", ImplementationRevision: "implementation", CurrentNodeID: "start", Revision: revision, Status: status, Visits: map[string]int{}, State: graph.State{"value": json.RawMessage(`"one"`)}}
}
func transition(previous, next graph.Checkpoint, outcome string) graph.Transition {
	result := graph.Transition{ID: graph.TransitionID(next.Key, next.Revision), Key: next.Key, Revision: next.Revision, From: previous.Status, To: next.Status, SegmentID: next.SegmentID, CurrentNodeID: next.CurrentNodeID, AttemptID: next.AttemptID, OutcomeCode: outcome}
	if previous.CurrentNodeID != "" {
		result.SourceSegmentID = previous.SegmentID
		result.SourceHostGeneration = previous.HostGeneration
		result.SourceNodeID = previous.CurrentNodeID
		result.SourceAttemptID = previous.AttemptID
	}
	return result
}
