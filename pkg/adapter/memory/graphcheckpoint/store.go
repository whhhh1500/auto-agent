// Package graphcheckpoint is a bounded in-memory reference implementation of
// the Graph checkpoint Store port. It is deliberately not durable and is
// intended for tests and local embedding only.
package graphcheckpoint

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"time"

	graph "github.com/whhhh1500/auto-agent/pkg/extensions/graph"
)

const (
	DefaultMaxCheckpoints = 1024
	DefaultMaxTransitions = graph.MaxCheckpointTransitions
)

type Store struct {
	mu             sync.RWMutex
	maxCheckpoints int
	maxTransitions int
	records        map[graph.CheckpointKey]record
	transitions    int
}

type record struct {
	checkpoint  graph.Checkpoint
	transitions map[uint64]graph.Transition
	versions    map[uint64]graph.CheckpointVersion
}

func New(maxCheckpoints, maxTransitions int) (*Store, error) {
	if maxCheckpoints <= 0 || maxCheckpoints > graph.MaxCheckpointTransitions || maxTransitions <= 0 || maxTransitions > graph.MaxCheckpointTransitions || maxTransitions < maxCheckpoints {
		return nil, fmt.Errorf("graph memory checkpoint capacities are invalid")
	}
	return &Store{
		maxCheckpoints: maxCheckpoints,
		maxTransitions: maxTransitions,
		records:        make(map[graph.CheckpointKey]record, maxCheckpoints),
	}, nil
}

func NewDefault() *Store {
	store, err := New(DefaultMaxCheckpoints, DefaultMaxTransitions)
	if err != nil {
		panic(err)
	}
	return store
}

func (s *Store) Load(ctx context.Context, key graph.CheckpointKey) (graph.Checkpoint, error) {
	if err := contextError(ctx); err != nil {
		return graph.Checkpoint{}, err
	}
	if err := graph.ValidateCheckpointKey(key); err != nil {
		return graph.Checkpoint{}, err
	}
	s.mu.RLock()
	record, ok := s.records[key]
	s.mu.RUnlock()
	if !ok {
		return graph.Checkpoint{}, graph.ErrCheckpointNotFound
	}
	return graph.CloneCheckpoint(record.checkpoint), nil
}

func (s *Store) Create(ctx context.Context, checkpoint graph.Checkpoint, transition graph.Transition) (graph.Checkpoint, graph.CommitDisposition, error) {
	if err := contextError(ctx); err != nil {
		return graph.Checkpoint{}, graph.CommitUnknown, err
	}
	checkpoint, err := graph.ValidateCheckpoint(checkpoint)
	if err != nil {
		return graph.Checkpoint{}, graph.CommitUnknown, err
	}
	if checkpoint.Revision != 1 || checkpoint.Status != graph.CheckpointReady {
		return graph.Checkpoint{}, graph.CommitUnknown, fmt.Errorf("%w: create requires revision 1 ready checkpoint", graph.ErrInvalidCheckpoint)
	}
	transition, err = graph.ValidateTransition(transition, checkpoint)
	if err != nil {
		return graph.Checkpoint{}, graph.CommitUnknown, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := contextError(ctx); err != nil {
		return graph.Checkpoint{}, graph.CommitUnknown, err
	}
	if existing, ok := s.records[checkpoint.Key]; ok {
		if sameCheckpoint(existing.checkpoint, checkpoint) && sameTransition(existing.transitions[transition.Revision], transition) {
			return graph.CloneCheckpoint(existing.checkpoint), graph.CommitReplayed, nil
		}
		return graph.Checkpoint{}, graph.CommitUnknown, graph.ErrCheckpointConflict
	}
	if len(s.records) >= s.maxCheckpoints || s.transitions >= s.maxTransitions {
		return graph.Checkpoint{}, graph.CommitUnknown, graph.ErrCheckpointCapacity
	}
	version, err := versionForCheckpoint(checkpoint, "", graph.CheckpointVersionOriginCommit)
	if err != nil {
		return graph.Checkpoint{}, graph.CommitUnknown, err
	}
	s.records[checkpoint.Key] = record{
		checkpoint:  graph.CloneCheckpoint(checkpoint),
		transitions: map[uint64]graph.Transition{transition.Revision: graph.CloneTransition(transition)},
		versions:    map[uint64]graph.CheckpointVersion{checkpoint.Revision: version},
	}
	s.transitions++
	return graph.CloneCheckpoint(checkpoint), graph.CommitApplied, nil
}

func (s *Store) CompareAndSwap(ctx context.Context, key graph.CheckpointKey, expectedRevision uint64, next graph.Checkpoint, transition graph.Transition) (graph.Checkpoint, graph.CommitDisposition, error) {
	if err := contextError(ctx); err != nil {
		return graph.Checkpoint{}, graph.CommitUnknown, err
	}
	if err := graph.ValidateCheckpointKey(key); err != nil {
		return graph.Checkpoint{}, graph.CommitUnknown, err
	}
	next, err := graph.ValidateCheckpoint(next)
	if err != nil {
		return graph.Checkpoint{}, graph.CommitUnknown, err
	}
	if next.Key != key || expectedRevision == 0 || next.Revision != expectedRevision+1 {
		return graph.Checkpoint{}, graph.CommitUnknown, fmt.Errorf("%w: CAS revision or key is invalid", graph.ErrInvalidCheckpoint)
	}
	transition, err = graph.ValidateTransition(transition, next)
	if err != nil {
		return graph.Checkpoint{}, graph.CommitUnknown, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := contextError(ctx); err != nil {
		return graph.Checkpoint{}, graph.CommitUnknown, err
	}
	record, ok := s.records[key]
	if !ok {
		return graph.Checkpoint{}, graph.CommitUnknown, graph.ErrCheckpointNotFound
	}
	if record.checkpoint.Revision != expectedRevision {
		if record.checkpoint.Revision == next.Revision && sameCheckpoint(record.checkpoint, next) && sameTransition(record.transitions[next.Revision], transition) {
			return graph.CloneCheckpoint(record.checkpoint), graph.CommitReplayed, nil
		}
		return graph.Checkpoint{}, graph.CommitUnknown, graph.ErrCheckpointConflict
	}
	if err := validateTransitionSource(transition, record.checkpoint); err != nil {
		return graph.Checkpoint{}, graph.CommitUnknown, err
	}
	if record.checkpoint.Status == graph.CheckpointWaitingApproval && next.Status == graph.CheckpointReady &&
		(next.AttemptID != record.checkpoint.AttemptID || (transition.OutcomeCode != "approval_approved" && transition.OutcomeCode != "approval_denied")) {
		return graph.Checkpoint{}, graph.CommitUnknown, fmt.Errorf("%w: approved resume must retain the suspended attempt", graph.ErrInvalidTransition)
	}
	if transition.From != record.checkpoint.Status {
		return graph.Checkpoint{}, graph.CommitUnknown, fmt.Errorf("%w: transition source does not match current checkpoint", graph.ErrInvalidTransition)
	}
	if s.transitions >= s.maxTransitions {
		return graph.Checkpoint{}, graph.CommitUnknown, graph.ErrCheckpointCapacity
	}
	if _, exists := record.transitions[next.Revision]; exists {
		return graph.Checkpoint{}, graph.CommitUnknown, graph.ErrCheckpointConflict
	}
	version, err := versionForCheckpoint(next, graph.CheckpointVersionID(key, expectedRevision), graph.CheckpointVersionOriginCommit)
	if err != nil {
		return graph.Checkpoint{}, graph.CommitUnknown, err
	}
	record.checkpoint = graph.CloneCheckpoint(next)
	record.transitions[next.Revision] = graph.CloneTransition(transition)
	record.versions[next.Revision] = version
	s.records[key] = record
	s.transitions++
	return graph.CloneCheckpoint(next), graph.CommitApplied, nil
}

func versionForCheckpoint(checkpoint graph.Checkpoint, parentID string, origin graph.CheckpointVersionOrigin) (graph.CheckpointVersion, error) {
	hash, err := graph.CheckpointDigest(checkpoint)
	if err != nil {
		return graph.CheckpointVersion{}, err
	}
	return graph.ValidateCheckpointVersion(graph.CheckpointVersion{
		Info: graph.CheckpointVersionInfo{
			ID:             graph.CheckpointVersionID(checkpoint.Key, checkpoint.Revision),
			ParentID:       parentID,
			Origin:         origin,
			Key:            checkpoint.Key,
			Revision:       checkpoint.Revision,
			CheckpointHash: hash,
			CreatedAt:      time.Now().UTC().UnixNano(),
		},
		Checkpoint: checkpoint,
	})
}

func validateTransitionSource(transition graph.Transition, previous graph.Checkpoint) error {
	if previous.CurrentNodeID == "" {
		return nil
	}
	if transition.SourceSegmentID != previous.SegmentID ||
		transition.SourceHostGeneration != previous.HostGeneration ||
		transition.SourceNodeID != previous.CurrentNodeID ||
		transition.SourceAttemptID != previous.AttemptID {
		return fmt.Errorf("%w: transition source evidence does not match current checkpoint", graph.ErrInvalidTransition)
	}
	return nil
}

func (s *Store) ListTransitions(ctx context.Context, key graph.CheckpointKey, afterRevision uint64, limit int) ([]graph.Transition, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if err := graph.ValidateCheckpointKey(key); err != nil {
		return nil, err
	}
	if limit <= 0 || limit > graph.MaxTransitionPageSize {
		return nil, fmt.Errorf("%w: transition page limit must be 1..%d", graph.ErrInvalidTransition, graph.MaxTransitionPageSize)
	}
	s.mu.RLock()
	record, ok := s.records[key]
	if !ok {
		s.mu.RUnlock()
		return nil, graph.ErrCheckpointNotFound
	}
	revisions := make([]uint64, 0, len(record.transitions))
	for revision := range record.transitions {
		if revision > afterRevision {
			revisions = append(revisions, revision)
		}
	}
	sort.Slice(revisions, func(i, j int) bool { return revisions[i] < revisions[j] })
	if len(revisions) > limit {
		revisions = revisions[:limit]
	}
	out := make([]graph.Transition, len(revisions))
	for index, revision := range revisions {
		out[index] = graph.CloneTransition(record.transitions[revision])
	}
	s.mu.RUnlock()
	return out, nil
}

func (s *Store) LoadVersion(ctx context.Context, key graph.CheckpointKey, revision uint64) (graph.CheckpointVersion, error) {
	if err := contextError(ctx); err != nil {
		return graph.CheckpointVersion{}, err
	}
	if err := graph.ValidateCheckpointKey(key); err != nil {
		return graph.CheckpointVersion{}, err
	}
	if revision == 0 {
		return graph.CheckpointVersion{}, fmt.Errorf("%w: checkpoint version revision must be positive", graph.ErrInvalidCheckpoint)
	}
	s.mu.RLock()
	record, ok := s.records[key]
	if !ok {
		s.mu.RUnlock()
		return graph.CheckpointVersion{}, graph.ErrCheckpointNotFound
	}
	version, ok := record.versions[revision]
	s.mu.RUnlock()
	if !ok {
		return graph.CheckpointVersion{}, graph.ErrCheckpointNotFound
	}
	return graph.ValidateCheckpointVersion(version)
}

func (s *Store) ListVersions(ctx context.Context, key graph.CheckpointKey, afterRevision uint64, limit int) ([]graph.CheckpointVersionInfo, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if err := graph.ValidateCheckpointKey(key); err != nil {
		return nil, err
	}
	if limit <= 0 || limit > graph.MaxCheckpointVersionPageSize {
		return nil, fmt.Errorf("%w: checkpoint version page limit must be 1..%d", graph.ErrInvalidCheckpoint, graph.MaxCheckpointVersionPageSize)
	}
	s.mu.RLock()
	record, ok := s.records[key]
	if !ok {
		s.mu.RUnlock()
		return nil, graph.ErrCheckpointNotFound
	}
	revisions := make([]uint64, 0, len(record.versions))
	for revision := range record.versions {
		if revision > afterRevision {
			revisions = append(revisions, revision)
		}
	}
	sort.Slice(revisions, func(i, j int) bool { return revisions[i] < revisions[j] })
	if len(revisions) > limit {
		revisions = revisions[:limit]
	}
	out := make([]graph.CheckpointVersionInfo, len(revisions))
	for index, revision := range revisions {
		validated, err := graph.ValidateCheckpointVersionInfo(record.versions[revision].Info)
		if err != nil {
			s.mu.RUnlock()
			return nil, err
		}
		out[index] = validated
	}
	s.mu.RUnlock()
	return out, nil
}

func contextError(ctx context.Context) error {
	if ctx == nil {
		return errors.New("nil context")
	}
	return ctx.Err()
}

func sameCheckpoint(left, right graph.Checkpoint) bool {
	return reflect.DeepEqual(graph.CloneCheckpoint(left), graph.CloneCheckpoint(right))
}

func sameTransition(left, right graph.Transition) bool {
	return reflect.DeepEqual(graph.CloneTransition(left), graph.CloneTransition(right))
}

var _ graph.Store = (*Store)(nil)
var _ graph.HistoryStore = (*Store)(nil)
