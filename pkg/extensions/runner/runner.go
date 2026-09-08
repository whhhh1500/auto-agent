// Package runner provides a durable-store-neutral private-runner queue and
// capability adapter. The default MemoryStore is useful for one process;
// production deployments can inject a Store backed by SQL or another queue.
package runner

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"

	core "github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/extensions/internal/support"
)

type TaskState string

const (
	TaskQueued    TaskState = "queued"
	TaskClaimed   TaskState = "claimed"
	TaskCompleted TaskState = "completed"
	TaskCancelled TaskState = "cancelled"
	TaskFailed    TaskState = "failed"

	DefaultMaxAttempts   = 3
	HardMaxAttempts      = 10
	MaxIdempotencyKey    = 256
	MaxWorkerID          = 256
	MaxClaimCapabilities = 64
	DefaultPollInterval  = 50 * time.Millisecond
	maxAdaptivePoll      = 250 * time.Millisecond
	DefaultCatalogLimit  = 100
	MaxCatalogLimit      = 500
	MaxCatalogOffset     = 1_000_000
	MaxCatalogStates     = 5
	MaxCatalogFilterLen  = 4096
	MaxInFlightTasks     = 1024
	MaxStoredTasks       = 4096
)

var (
	ErrTaskNotFound         = errors.New("runner task not found")
	ErrSubmissionConflict   = errors.New("runner submission conflict")
	ErrOutcomeUnknown       = errors.New("runner task outcome unknown")
	ErrCatalogUnsupported   = errors.New("runner task catalog unsupported")
	ErrRetentionUnsupported = errors.New("runner task retention unsupported")
)

type CancelDisposition string

const (
	CancelDispositionCancelled CancelDisposition = "cancelled"
	CancelDispositionRequested CancelDisposition = "requested"
	CancelDispositionTerminal  CancelDisposition = "terminal"
)

// Task is the durable private-runner protocol record. Args and Result are
// cloned at every Store boundary so callers cannot mutate canonical state.
type Task struct {
	ID              string                     `json:"id"`
	Capability      string                     `json:"capability"`
	IdempotencyKey  string                     `json:"idempotency_key,omitempty"`
	RetriedFromID   string                     `json:"retried_from_id,omitempty"`
	TenantID        string                     `json:"tenant_id,omitempty"`
	SubjectID       string                     `json:"subject_id,omitempty"`
	Scope           string                     `json:"scope,omitempty"`
	Args            map[string]any             `json:"args,omitempty"`
	ArgsDigest      string                     `json:"args_digest"`
	TraceContext    core.TelemetryTraceContext `json:"trace_context,omitempty"`
	State           TaskState                  `json:"state"`
	CancelRequested bool                       `json:"cancel_requested,omitempty"`
	WorkerID        string                     `json:"worker_id,omitempty"`
	LeaseExpiresAt  time.Time                  `json:"lease_expires_at,omitempty"`
	AvailableAt     time.Time                  `json:"available_at"`
	Attempt         int                        `json:"attempt"`
	MaxAttempts     int                        `json:"max_attempts"`
	Generation      int64                      `json:"generation"`
	Result          *core.CapabilityResult     `json:"result,omitempty"`
	ErrorCode       string                     `json:"error_code,omitempty"`
	CreatedAt       time.Time                  `json:"created_at"`
	UpdatedAt       time.Time                  `json:"updated_at"`
	CompletedAt     time.Time                  `json:"completed_at,omitempty"`
}

// TaskSummary is the metadata-only view of a durable runner task. It never
// exposes arguments, outcomes, idempotency material, argument digests, or
// trace carriers.
type TaskSummary struct {
	ID              string    `json:"id"`
	Capability      string    `json:"capability"`
	RetriedFromID   string    `json:"retried_from_id,omitempty"`
	TenantID        string    `json:"tenant_id,omitempty"`
	SubjectID       string    `json:"subject_id,omitempty"`
	Scope           string    `json:"scope,omitempty"`
	State           TaskState `json:"state"`
	CancelRequested bool      `json:"cancel_requested,omitempty"`
	WorkerID        string    `json:"worker_id,omitempty"`
	LeaseExpiresAt  time.Time `json:"lease_expires_at,omitempty"`
	AvailableAt     time.Time `json:"available_at"`
	Attempt         int       `json:"attempt"`
	MaxAttempts     int       `json:"max_attempts"`
	Generation      int64     `json:"generation"`
	ErrorCode       string    `json:"error_code,omitempty"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
	CompletedAt     time.Time `json:"completed_at,omitempty"`
	HasResult       bool      `json:"has_result"`
	HasTraceContext bool      `json:"has_trace_context"`
}

// TaskQuery selects a page from the optional durable task catalog.
type TaskQuery struct {
	ID          string
	TenantID    string
	SubjectID   string
	ScopePrefix string
	Capability  string
	WorkerID    string
	States      []TaskState
	Limit       int
	Offset      int
}

// Normalize returns the canonical, validated catalog query. Filters are
// trimmed, states are de-duplicated and sorted, and a zero limit becomes the
// default catalog page size.
func (q TaskQuery) Normalize() (TaskQuery, error) {
	q.ID = strings.TrimSpace(q.ID)
	if q.ID != "" {
		if err := validateTaskID(q.ID); err != nil {
			return TaskQuery{}, fmt.Errorf("invalid runner catalog task id %q: %w", q.ID, err)
		}
	}
	for _, filter := range []struct {
		name  string
		value *string
		limit int
	}{
		{name: "tenant", value: &q.TenantID, limit: MaxCatalogFilterLen},
		{name: "subject", value: &q.SubjectID, limit: MaxCatalogFilterLen},
		{name: "scope prefix", value: &q.ScopePrefix, limit: MaxCatalogFilterLen},
		{name: "worker", value: &q.WorkerID, limit: MaxWorkerID},
		{name: "capability", value: &q.Capability, limit: MaxCatalogFilterLen},
	} {
		*filter.value = strings.TrimSpace(*filter.value)
		if len(*filter.value) > filter.limit || hasControl(*filter.value) {
			return TaskQuery{}, fmt.Errorf("runner catalog %s filter is too long or contains control characters", filter.name)
		}
	}
	if q.Capability != "" {
		if err := core.ValidateNamespacedID(q.Capability); err != nil {
			return TaskQuery{}, fmt.Errorf("invalid runner catalog capability %q: %w", q.Capability, err)
		}
	}
	if q.Limit == 0 {
		q.Limit = DefaultCatalogLimit
	}
	if q.Limit < 1 || q.Limit > MaxCatalogLimit {
		return TaskQuery{}, fmt.Errorf("runner catalog limit must be between 1 and %d", MaxCatalogLimit)
	}
	if q.Offset < 0 || q.Offset > MaxCatalogOffset {
		return TaskQuery{}, fmt.Errorf("runner catalog offset must be between 0 and %d", MaxCatalogOffset)
	}
	uniqueStates := make(map[TaskState]struct{}, len(q.States))
	for _, state := range q.States {
		switch state {
		case TaskQueued, TaskClaimed, TaskCompleted, TaskCancelled, TaskFailed:
		default:
			return TaskQuery{}, fmt.Errorf("invalid runner catalog task state %q", state)
		}
		uniqueStates[state] = struct{}{}
	}
	if len(uniqueStates) > MaxCatalogStates {
		return TaskQuery{}, fmt.Errorf("runner catalog supports at most %d states", MaxCatalogStates)
	}
	if len(uniqueStates) == 0 {
		q.States = nil
	} else {
		q.States = make([]TaskState, 0, len(uniqueStates))
		for state := range uniqueStates {
			q.States = append(q.States, state)
		}
		sort.Slice(q.States, func(i, j int) bool { return q.States[i] < q.States[j] })
	}
	return q, nil
}

// Validate reports whether q can be normalized into a catalog query.
func (q TaskQuery) Validate() error {
	_, err := q.Normalize()
	return err
}

// ClaimOptions identifies a worker and the capabilities it can execute.
// Nil or empty Capabilities claim from all capabilities.
type ClaimOptions struct {
	WorkerID     string
	Capabilities []string
	LeaseTTL     time.Duration
}

// Store owns task identity, leases, fencing and terminal outcomes.
type Store interface {
	CreateTask(context.Context, Task) (Task, bool, error)
	GetTask(context.Context, string) (Task, error)
	ClaimTask(context.Context, ClaimOptions) (Task, bool, error)
	RenewTaskClaim(context.Context, string, string, int64, time.Duration) (bool, error)
	CompleteTask(context.Context, string, string, int64, core.CapabilityResult) (Task, bool, error)
	CancelTask(context.Context, string) (CancelDisposition, error)
	RecoverExpiredTasks(context.Context, time.Time) (requeued int64, terminal int64, err error)
	PendingTasks(context.Context) (int64, error)
}

// TaskCatalog is an optional metadata-only task listing capability. It stays
// separate from Store so queue implementations do not need to expose a
// catalog to satisfy the durable runner protocol.
type TaskCatalog interface {
	ListTasks(context.Context, TaskQuery) ([]TaskSummary, int, error)
}

// TaskRetrier is an optional atomic retry primitive for failed tasks.
type TaskRetrier interface {
	RetryTask(context.Context, string) (Task, bool, error)
}

// TaskRetention is an optional durable task cleanup capability. Stores retain
// their normal queue protocol when they do not implement it.
type TaskRetention interface {
	PruneTasks(ctx context.Context, olderThan time.Time) (int64, error)
}

// MemoryStore is the reference Store. It implements the same state machine as
// durable adapters and can be shared by replacement Hub instances in tests.
type MemoryStore struct {
	mu          sync.Mutex
	tasks       map[string]Task
	dedupe      map[string]string
	ordered     []string
	maxInFlight int
	maxStored   int
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{tasks: map[string]Task{}, dedupe: map[string]string{}}
}

func (s *MemoryStore) CreateTask(ctx context.Context, task Task) (Task, bool, error) {
	if err := ctx.Err(); err != nil {
		return Task{}, false, err
	}
	prepared, err := prepareTask(task)
	if err != nil {
		return Task{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if prepared.IdempotencyKey != "" {
		key := taskDedupeKey(prepared)
		if id := s.dedupe[key]; id != "" {
			existing := s.tasks[id]
			if existing.ArgsDigest != prepared.ArgsDigest {
				return Task{}, false, ErrSubmissionConflict
			}
			return cloneTask(existing), false, nil
		}
	}
	if _, exists := s.tasks[prepared.ID]; exists {
		return Task{}, false, ErrSubmissionConflict
	}
	if s.inFlightLocked() >= s.inFlightCap() {
		return Task{}, false, fmt.Errorf("runner in-flight tasks exceed maximum of %d", s.inFlightCap())
	}
	if len(s.tasks) >= s.storedCap() {
		return Task{}, false, fmt.Errorf("runner tasks exceed maximum of %d", s.storedCap())
	}
	s.tasks[prepared.ID] = prepared
	s.ordered = append(s.ordered, prepared.ID)
	if prepared.IdempotencyKey != "" {
		s.dedupe[taskDedupeKey(prepared)] = prepared.ID
	}
	return cloneTask(prepared), true, nil
}

func (s *MemoryStore) GetTask(ctx context.Context, id string) (Task, error) {
	if err := ctx.Err(); err != nil {
		return Task{}, err
	}
	if err := validateTaskID(id); err != nil {
		return Task{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	task, ok := s.tasks[id]
	if !ok {
		return Task{}, ErrTaskNotFound
	}
	return cloneTask(task), nil
}

func (s *MemoryStore) RetryTask(ctx context.Context, id string) (Task, bool, error) {
	if err := ctx.Err(); err != nil {
		return Task{}, false, err
	}
	if err := validateTaskID(id); err != nil {
		return Task{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	original, ok := s.tasks[id]
	if !ok {
		return Task{}, false, ErrTaskNotFound
	}
	if original.State != TaskFailed {
		return Task{}, false, fmt.Errorf("runner task %s is not failed", id)
	}
	for _, task := range s.tasks {
		if task.RetriedFromID == id {
			return cloneTask(task), false, nil
		}
	}
	child, err := prepareTask(Task{Capability: original.Capability, TenantID: original.TenantID, SubjectID: original.SubjectID, Scope: original.Scope, Args: original.Args, ArgsDigest: original.ArgsDigest, TraceContext: original.TraceContext, MaxAttempts: original.MaxAttempts, RetriedFromID: id})
	if err != nil {
		return Task{}, false, err
	}
	if s.inFlightLocked() >= s.inFlightCap() || len(s.tasks) >= s.storedCap() {
		return Task{}, false, fmt.Errorf("runner retry capacity exceeded")
	}
	s.tasks[child.ID] = child
	s.ordered = append(s.ordered, child.ID)
	return cloneTask(child), true, nil
}

func (s *MemoryStore) ClaimTask(ctx context.Context, options ClaimOptions) (Task, bool, error) {
	if err := ctx.Err(); err != nil {
		return Task{}, false, err
	}
	options, err := normalizeClaimOptions(options)
	if err != nil {
		return Task{}, false, err
	}
	now := time.Now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recoverExpiredLocked(now)
	ids := append([]string(nil), s.ordered...)
	sort.SliceStable(ids, func(i, j int) bool {
		left, right := s.tasks[ids[i]], s.tasks[ids[j]]
		if !left.AvailableAt.Equal(right.AvailableAt) {
			return left.AvailableAt.Before(right.AvailableAt)
		}
		if !left.CreatedAt.Equal(right.CreatedAt) {
			return left.CreatedAt.Before(right.CreatedAt)
		}
		return left.ID < right.ID
	})
	for _, id := range ids {
		task := s.tasks[id]
		if task.State != TaskQueued || task.AvailableAt.After(now) || !capabilitySelected(options.Capabilities, task.Capability) {
			continue
		}
		task.State = TaskClaimed
		task.WorkerID = options.WorkerID
		task.LeaseExpiresAt = now.Add(options.LeaseTTL)
		task.Attempt++
		task.Generation++
		task.UpdatedAt = now
		s.tasks[id] = task
		return cloneTask(task), true, nil
	}
	return Task{}, false, nil
}

func (s *MemoryStore) RenewTaskClaim(ctx context.Context, id, worker string, generation int64, ttl time.Duration) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := validateClaim(id, worker, generation, ttl); err != nil {
		return false, err
	}
	now := time.Now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	task, ok := s.tasks[id]
	if !ok {
		return false, ErrTaskNotFound
	}
	if !ownsLiveClaim(task, worker, generation, now) {
		return false, nil
	}
	task.LeaseExpiresAt = now.Add(ttl)
	task.UpdatedAt = now
	s.tasks[id] = task
	return true, nil
}

func (s *MemoryStore) CompleteTask(ctx context.Context, id, worker string, generation int64, result core.CapabilityResult) (Task, bool, error) {
	if err := ctx.Err(); err != nil {
		return Task{}, false, err
	}
	if err := validateClaim(id, worker, generation, time.Second); err != nil {
		return Task{}, false, err
	}
	if err := core.ValidateCapabilityResult(result); err != nil {
		return Task{}, false, err
	}
	canonical, err := cloneResult(result)
	if err != nil {
		return Task{}, false, err
	}
	now := time.Now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	task, ok := s.tasks[id]
	if !ok {
		return Task{}, false, ErrTaskNotFound
	}
	if task.State == TaskCompleted || task.State == TaskCancelled || task.State == TaskFailed {
		return cloneTask(task), false, nil
	}
	if !ownsLiveClaim(task, worker, generation, now) {
		return cloneTask(task), false, nil
	}
	task.State = TaskCompleted
	task.Result = &canonical
	task.WorkerID = ""
	task.LeaseExpiresAt = time.Time{}
	task.UpdatedAt = now
	task.CompletedAt = now
	s.tasks[id] = task
	return cloneTask(task), true, nil
}

func (s *MemoryStore) CancelTask(ctx context.Context, id string) (CancelDisposition, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := validateTaskID(id); err != nil {
		return "", err
	}
	now := time.Now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	task, ok := s.tasks[id]
	if !ok {
		return "", ErrTaskNotFound
	}
	switch task.State {
	case TaskQueued:
		task.State = TaskCancelled
		task.CancelRequested = true
		task.UpdatedAt = now
		task.CompletedAt = now
		s.tasks[id] = task
		return CancelDispositionCancelled, nil
	case TaskClaimed:
		task.CancelRequested = true
		task.UpdatedAt = now
		s.tasks[id] = task
		return CancelDispositionRequested, nil
	case TaskCompleted, TaskCancelled, TaskFailed:
		return CancelDispositionTerminal, nil
	default:
		return "", fmt.Errorf("runner task %s has invalid state %q", id, task.State)
	}
}

func (s *MemoryStore) RecoverExpiredTasks(ctx context.Context, now time.Time) (int64, int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, 0, err
	}
	if now.IsZero() {
		return 0, 0, fmt.Errorf("runner recovery time is zero")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.recoverExpiredLocked(now.UTC())
}

func (s *MemoryStore) recoverExpiredLocked(now time.Time) (int64, int64, error) {
	var requeued, terminal int64
	for id, task := range s.tasks {
		if task.State != TaskClaimed || task.LeaseExpiresAt.After(now) {
			continue
		}
		task.WorkerID = ""
		task.LeaseExpiresAt = time.Time{}
		task.UpdatedAt = now
		switch {
		case task.CancelRequested:
			task.State = TaskCancelled
			task.CompletedAt = now
			terminal++
		case task.Attempt < task.MaxAttempts:
			task.State = TaskQueued
			task.AvailableAt = now
			requeued++
		default:
			task.State = TaskFailed
			task.ErrorCode = "worker_lost"
			task.CompletedAt = now
			terminal++
		}
		s.tasks[id] = task
	}
	return requeued, terminal, nil
}

func (s *MemoryStore) inFlightCap() int {
	if s != nil && s.maxInFlight > 0 {
		return s.maxInFlight
	}
	return MaxInFlightTasks
}

func (s *MemoryStore) storedCap() int {
	if s != nil && s.maxStored > 0 {
		return s.maxStored
	}
	return MaxStoredTasks
}

func (s *MemoryStore) inFlightLocked() int {
	count := 0
	for _, task := range s.tasks {
		if task.State == TaskQueued || task.State == TaskClaimed {
			count++
		}
	}
	return count
}

func (s *MemoryStore) PendingTasks(ctx context.Context) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var count int64
	for _, task := range s.tasks {
		if task.State == TaskQueued {
			count++
		}
	}
	return count, nil
}

// PruneTasks removes non-idempotent terminal tasks completed strictly before
// olderThan. Active tasks and every task with an idempotency key are retained.
func (s *MemoryStore) PruneTasks(ctx context.Context, olderThan time.Time) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if olderThan.IsZero() {
		return 0, fmt.Errorf("runner retention cutoff is zero")
	}
	cutoff := olderThan.UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	removed := make(map[string]struct{})
	for id, task := range s.tasks {
		terminal := task.State == TaskCompleted || task.State == TaskCancelled || task.State == TaskFailed
		if !terminal || task.IdempotencyKey != "" || task.CompletedAt.IsZero() || !task.CompletedAt.Before(cutoff) {
			continue
		}
		delete(s.tasks, id)
		removed[id] = struct{}{}
	}
	if len(removed) == 0 {
		return 0, nil
	}
	ordered := s.ordered[:0]
	for _, id := range s.ordered {
		if _, deleted := removed[id]; !deleted {
			ordered = append(ordered, id)
		}
	}
	s.ordered = ordered
	for key, id := range s.dedupe {
		if _, deleted := removed[id]; deleted {
			delete(s.dedupe, key)
		}
	}
	return int64(len(removed)), nil
}

// ListTasks returns a filtered metadata-only task page in stable catalog
// order: newest update first, then task ID ascending.
func (s *MemoryStore) ListTasks(ctx context.Context, query TaskQuery) ([]TaskSummary, int, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	query, err := query.Normalize()
	if err != nil {
		return nil, 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	matched := make([]TaskSummary, 0, len(s.tasks))
	for _, task := range s.tasks {
		if taskMatchesQuery(task, query) {
			matched = append(matched, taskSummary(task))
		}
	}
	sort.Slice(matched, func(i, j int) bool {
		if !matched[i].UpdatedAt.Equal(matched[j].UpdatedAt) {
			return matched[i].UpdatedAt.After(matched[j].UpdatedAt)
		}
		return matched[i].ID < matched[j].ID
	})
	total := len(matched)
	if query.Offset >= total {
		return []TaskSummary{}, total, nil
	}
	end := query.Offset + query.Limit
	if end > total {
		end = total
	}
	page := make([]TaskSummary, end-query.Offset)
	copy(page, matched[query.Offset:end])
	return page, total, nil
}

type Hub struct {
	Store        Store
	Telemetry    core.Telemetry
	PollInterval time.Duration
	LeaseTTL     time.Duration
}

func NewHub() *Hub { return NewHubWithStore(NewMemoryStore()) }

func NewHubWithStore(store Store) *Hub {
	return &Hub{Store: store, PollInterval: DefaultPollInterval, LeaseTTL: 30 * time.Second}
}

func (h *Hub) Submit(ctx context.Context, capability string, args map[string]any) (core.CapabilityResult, error) {
	return h.SubmitWithKey(ctx, capability, "", args)
}

func (h *Hub) SubmitWithKey(ctx context.Context, capability, key string, args map[string]any) (core.CapabilityResult, error) {
	return h.SubmitScoped(ctx, core.Principal{}, core.ScopePath{}, capability, key, args, DefaultMaxAttempts)
}

func (h *Hub) SubmitScoped(ctx context.Context, principal core.Principal, scope core.ScopePath, capability, key string, args map[string]any, maxAttempts int) (core.CapabilityResult, error) {
	if h == nil || h.Store == nil {
		return core.CapabilityResult{}, fmt.Errorf("runner store is not configured")
	}
	task, _, err := h.Store.CreateTask(ctx, Task{
		Capability: capability, IdempotencyKey: key, TenantID: principal.TenantID,
		SubjectID: principal.SubjectID, Scope: scope.String(), Args: args, MaxAttempts: maxAttempts,
		TraceContext: core.InjectTelemetryTraceContext(h.Telemetry, ctx),
	})
	if err != nil {
		return core.CapabilityResult{}, err
	}
	interval := h.PollInterval
	if interval <= 0 {
		interval = DefaultPollInterval
	}
	pollInterval := interval
	adaptiveMax := maxAdaptivePoll
	if interval > adaptiveMax {
		adaptiveMax = interval
	}
	lastState := task.State
	for {
		current, err := h.Store.GetTask(ctx, task.ID)
		if err != nil {
			if ctx.Err() != nil {
				return h.cancelWait(ctx, task.ID)
			}
			return core.CapabilityResult{}, err
		}
		if result, done, err := terminalResult(current); done {
			return result, err
		}
		if current.State != lastState {
			pollInterval = interval
			lastState = current.State
		}
		timer := time.NewTimer(pollInterval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return h.cancelWait(ctx, task.ID)
		case <-timer.C:
			pollInterval = nextAdaptivePoll(pollInterval, interval, adaptiveMax)
		}
	}
}

func nextAdaptivePoll(current, base, max time.Duration) time.Duration {
	if base <= 0 {
		base = DefaultPollInterval
	}
	if max < base {
		max = base
	}
	if current < base {
		return base
	}
	if current >= max {
		return max
	}
	next := current * 2
	if next < current || next > max {
		return max
	}
	return next
}

func (h *Hub) cancelWait(ctx context.Context, taskID string) (core.CapabilityResult, error) {
	detached := context.WithoutCancel(ctx)
	disposition, cancelErr := h.Store.CancelTask(detached, taskID)
	if cancelErr != nil {
		return core.CapabilityResult{}, errors.Join(ctx.Err(), cancelErr)
	}
	latest, getErr := h.Store.GetTask(detached, taskID)
	if getErr == nil {
		if result, done, terminalErr := terminalResult(latest); done {
			if latest.State == TaskCompleted || latest.State == TaskFailed {
				return result, terminalErr
			}
		}
	}
	if disposition == CancelDispositionRequested {
		return core.CapabilityResult{}, fmt.Errorf("%w: %v", ErrOutcomeUnknown, ctx.Err())
	}
	return core.CapabilityResult{}, ctx.Err()
}

func (h *Hub) Claim(ctx context.Context, options ClaimOptions) (Task, bool, error) {
	if h == nil || h.Store == nil {
		return Task{}, false, fmt.Errorf("runner store is not configured")
	}
	if options.LeaseTTL <= 0 {
		options.LeaseTTL = h.LeaseTTL
	}
	return h.Store.ClaimTask(ctx, options)
}

func (h *Hub) Renew(ctx context.Context, id, worker string, generation int64, ttl time.Duration) (bool, error) {
	if h == nil || h.Store == nil {
		return false, fmt.Errorf("runner store is not configured")
	}
	if ttl <= 0 {
		ttl = h.LeaseTTL
	}
	return h.Store.RenewTaskClaim(ctx, id, worker, generation, ttl)
}

func (h *Hub) Complete(ctx context.Context, id, worker string, generation int64, result core.CapabilityResult) (Task, bool, error) {
	if h == nil || h.Store == nil {
		return Task{}, false, fmt.Errorf("runner store is not configured")
	}
	return h.Store.CompleteTask(ctx, id, worker, generation, result)
}

// Cancel requests cancellation of a durable runner task.
func (h *Hub) Cancel(ctx context.Context, id string) (CancelDisposition, error) {
	if h == nil || h.Store == nil {
		return "", fmt.Errorf("runner store is not configured")
	}
	return h.Store.CancelTask(ctx, id)
}

func (h *Hub) RecoverExpired(ctx context.Context, now time.Time) (int64, int64, error) {
	if h == nil || h.Store == nil {
		return 0, 0, fmt.Errorf("runner store is not configured")
	}
	return h.Store.RecoverExpiredTasks(ctx, now)
}

// PruneTasks removes old, non-idempotent terminal tasks when the configured
// Store opts into TaskRetention. Queue-only stores return ErrRetentionUnsupported.
func (h *Hub) PruneTasks(ctx context.Context, olderThan time.Time) (int64, error) {
	if h == nil || h.Store == nil {
		return 0, fmt.Errorf("runner store is not configured")
	}
	retention, ok := h.Store.(TaskRetention)
	if !ok {
		return 0, ErrRetentionUnsupported
	}
	return retention.PruneTasks(ctx, olderThan)
}

func (h *Hub) Pending(ctx context.Context) (int64, error) {
	if h == nil || h.Store == nil {
		return 0, fmt.Errorf("runner store is not configured")
	}
	return h.Store.PendingTasks(ctx)
}

func (h *Hub) Task(ctx context.Context, id string) (Task, error) {
	if h == nil || h.Store == nil {
		return Task{}, fmt.Errorf("runner store is not configured")
	}
	return h.Store.GetTask(ctx, id)
}

func (h *Hub) Retry(ctx context.Context, id string) (Task, bool, error) {
	if h == nil || h.Store == nil {
		return Task{}, false, fmt.Errorf("runner store is not configured")
	}
	retrier, ok := h.Store.(TaskRetrier)
	if !ok {
		return Task{}, false, ErrCatalogUnsupported
	}
	return retrier.RetryTask(ctx, id)
}

// ListTasks lists durable task metadata when the configured Store opts into
// TaskCatalog. Queue-only stores return ErrCatalogUnsupported.
func (h *Hub) ListTasks(ctx context.Context, query TaskQuery) ([]TaskSummary, int, error) {
	if h == nil || h.Store == nil {
		return nil, 0, fmt.Errorf("runner store is not configured")
	}
	catalog, ok := h.Store.(TaskCatalog)
	if !ok {
		return nil, 0, ErrCatalogUnsupported
	}
	return catalog.ListTasks(ctx, query)
}

type Provider struct {
	Hub         *Hub
	Capability  string
	Timeout     time.Duration
	MaxAttempts int
}

func (Provider) ArtifactRevision() string { return "private-runner-provider/v3" }

func (p Provider) Execute(ctx context.Context, request core.CapabilityRequest) (core.CapabilityResult, error) {
	if p.Hub == nil {
		return support.DeniedResult("runner_unavailable", "runner hub is not configured"), nil
	}
	timeout := p.Timeout
	if timeout <= 0 {
		timeout = 2 * time.Minute
	}
	maxAttempts := p.MaxAttempts
	if maxAttempts == 0 {
		maxAttempts = DefaultMaxAttempts
	}
	callCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	result, err := p.Hub.SubmitScoped(callCtx, request.Context.Principal, request.Context.Scope,
		p.Capability, request.IdempotencyKey, request.Args, maxAttempts)
	if err == nil {
		return result, nil
	}
	if errors.Is(err, ErrOutcomeUnknown) {
		return core.CapabilityResult{}, err
	}
	if errors.Is(err, ErrSubmissionConflict) {
		return support.DeniedResult("runner_submission_conflict", "runner idempotency key was reused with different arguments"), nil
	}
	if callCtx.Err() != nil {
		return support.DeniedResult("runner_timeout", fmt.Sprintf("runner did not complete the task within %s", timeout)), nil
	}
	return core.CapabilityResult{}, err
}

func RegisterCapability(registry *core.CapabilityRegistry, scope core.ScopePath, manifest core.CapabilityManifest, hub *Hub) (func(), error) {
	if registry == nil {
		return nil, fmt.Errorf("runner capability %q requires a registry", manifest.ID)
	}
	if hub == nil || hub.Store == nil {
		return nil, fmt.Errorf("runner capability %q requires a hub", manifest.ID)
	}
	return registry.Mount(core.CapabilityBinding{
		Scope: scope, Mode: core.BindingProvide, Manifest: manifest,
		Provider: Provider{Hub: hub, Capability: manifest.ID},
	})
}

func prepareTask(task Task) (Task, error) {
	if err := core.ValidateNamespacedID(task.Capability); err != nil {
		return Task{}, err
	}
	if task.ID == "" {
		id, err := core.NewID("rtask_")
		if err != nil {
			return Task{}, err
		}
		task.ID = id
	}
	if err := validateTaskID(task.ID); err != nil {
		return Task{}, err
	}
	if err := validateIdentity(task.TenantID, task.SubjectID, task.Scope); err != nil {
		return Task{}, err
	}
	if err := validateIdempotencyKey(task.IdempotencyKey); err != nil {
		return Task{}, err
	}
	if err := validateTraceContext(task.TraceContext); err != nil {
		return Task{}, err
	}
	args, digest, err := cloneArgs(task.Args)
	if err != nil {
		return Task{}, err
	}
	if task.ArgsDigest != "" && task.ArgsDigest != digest {
		return Task{}, ErrSubmissionConflict
	}
	maxAttempts := task.MaxAttempts
	if maxAttempts == 0 {
		maxAttempts = DefaultMaxAttempts
	}
	if maxAttempts < 1 || maxAttempts > HardMaxAttempts {
		return Task{}, fmt.Errorf("runner max attempts must be between 1 and %d", HardMaxAttempts)
	}
	now := time.Now().UTC()
	if task.CreatedAt.IsZero() {
		task.CreatedAt = now
	}
	if task.AvailableAt.IsZero() {
		task.AvailableAt = task.CreatedAt
	}
	task.UpdatedAt = task.CreatedAt
	task.Args, task.ArgsDigest = args, digest
	task.TraceContext = cloneTraceContext(task.TraceContext)
	task.State = TaskQueued
	task.MaxAttempts = maxAttempts
	task.CancelRequested, task.WorkerID, task.ErrorCode = false, "", ""
	task.LeaseExpiresAt, task.CompletedAt = time.Time{}, time.Time{}
	task.Attempt, task.Generation, task.Result = 0, 0, nil
	return task, nil
}

func validateTaskID(id string) error {
	if len(id) <= len("rtask_") || core.ValidateRunID(id) != nil {
		return fmt.Errorf("invalid runner task id %q", id)
	}
	if !strings.HasPrefix(id, "rtask_") || len(id) > 128 || hasControl(id) || strings.ContainsAny(id, "/\\") {
		return fmt.Errorf("invalid runner task id %q", id)
	}
	return nil
}

func validateIdentity(tenant, subject, scope string) error {
	allEmpty := tenant == "" && subject == "" && scope == ""
	if allEmpty {
		return nil
	}
	for name, value := range map[string]string{"tenant": tenant, "subject": subject, "scope": scope} {
		if strings.TrimSpace(value) == "" || len(value) > 4096 || hasControl(value) {
			return fmt.Errorf("runner task %s is empty, too long, or contains control characters", name)
		}
	}
	return nil
}

func validateIdempotencyKey(key string) error {
	if key == "" {
		return nil
	}
	if len(key) > MaxIdempotencyKey || hasControl(key) {
		return fmt.Errorf("runner idempotency key is too long or contains control characters")
	}
	return nil
}

func validateTraceContext(traceContext core.TelemetryTraceContext) error {
	if len(traceContext.TraceParent) > 256 || len(traceContext.TraceState) > 512 ||
		hasControl(traceContext.TraceParent) || hasControl(traceContext.TraceState) {
		return fmt.Errorf("runner durable trace context is too large or contains control characters")
	}
	return nil
}

func validateWorkerLease(worker string, ttl time.Duration) error {
	if strings.TrimSpace(worker) == "" || len(worker) > MaxWorkerID || hasControl(worker) {
		return fmt.Errorf("runner worker id is empty, too long, or contains control characters")
	}
	if ttl <= 0 || ttl > 24*time.Hour {
		return fmt.Errorf("runner claim ttl must be between 1ns and 24h")
	}
	return nil
}

func normalizeClaimOptions(options ClaimOptions) (ClaimOptions, error) {
	if err := validateWorkerLease(options.WorkerID, options.LeaseTTL); err != nil {
		return ClaimOptions{}, err
	}
	if len(options.Capabilities) == 0 {
		options.Capabilities = nil
		return options, nil
	}
	if len(options.Capabilities) > MaxClaimCapabilities {
		return ClaimOptions{}, fmt.Errorf("runner claim supports at most %d capabilities", MaxClaimCapabilities)
	}
	unique := make(map[string]struct{}, len(options.Capabilities))
	for _, capability := range options.Capabilities {
		capability = strings.TrimSpace(capability)
		if capability == "*" {
			return ClaimOptions{}, fmt.Errorf("runner claim capability selector cannot include %q", capability)
		}
		if err := core.ValidateNamespacedID(capability); err != nil {
			return ClaimOptions{}, fmt.Errorf("invalid runner claim capability %q: %w", capability, err)
		}
		unique[capability] = struct{}{}
	}
	options.Capabilities = make([]string, 0, len(unique))
	for capability := range unique {
		options.Capabilities = append(options.Capabilities, capability)
	}
	sort.Strings(options.Capabilities)
	return options, nil
}

func capabilitySelected(capabilities []string, capability string) bool {
	if len(capabilities) == 0 {
		return true
	}
	index := sort.SearchStrings(capabilities, capability)
	return index < len(capabilities) && capabilities[index] == capability
}

func validateClaim(id, worker string, generation int64, ttl time.Duration) error {
	if err := validateTaskID(id); err != nil {
		return err
	}
	if generation < 1 {
		return fmt.Errorf("runner claim generation must be positive")
	}
	return validateWorkerLease(worker, ttl)
}

func ownsLiveClaim(task Task, worker string, generation int64, now time.Time) bool {
	return task.State == TaskClaimed && task.WorkerID == worker && task.Generation == generation && task.LeaseExpiresAt.After(now)
}

func taskDedupeKey(task Task) string {
	return task.TenantID + "\x00" + task.SubjectID + "\x00" + task.Scope + "\x00" + task.Capability + "\x00" + task.IdempotencyKey
}

func cloneArgs(args map[string]any) (map[string]any, string, error) {
	if args == nil {
		args = map[string]any{}
	}
	encoded, err := json.Marshal(args)
	if err != nil {
		return nil, "", fmt.Errorf("encode runner args: %w", err)
	}
	if len(encoded) > core.MaxToolArgumentBytes {
		return nil, "", fmt.Errorf("runner args exceed %d bytes", core.MaxToolArgumentBytes)
	}
	var cloned map[string]any
	if err := json.Unmarshal(encoded, &cloned); err != nil {
		return nil, "", fmt.Errorf("decode runner args: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return cloned, hex.EncodeToString(digest[:]), nil
}

func cloneResult(result core.CapabilityResult) (core.CapabilityResult, error) {
	if err := core.ValidateCapabilityResult(result); err != nil {
		return core.CapabilityResult{}, err
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		return core.CapabilityResult{}, err
	}
	var cloned core.CapabilityResult
	if err := json.Unmarshal(encoded, &cloned); err != nil {
		return core.CapabilityResult{}, err
	}
	return cloned, nil
}

func cloneTask(task Task) Task {
	args, _, err := cloneArgs(task.Args)
	if err == nil {
		task.Args = args
	}
	task.TraceContext = cloneTraceContext(task.TraceContext)
	if task.Result != nil {
		result, err := cloneResult(*task.Result)
		if err == nil {
			task.Result = &result
		}
	}
	return task
}

func taskSummary(task Task) TaskSummary {
	return TaskSummary{
		ID: task.ID, Capability: task.Capability, TenantID: task.TenantID,
		RetriedFromID: task.RetriedFromID,
		SubjectID:     task.SubjectID, Scope: task.Scope, State: task.State,
		CancelRequested: task.CancelRequested, WorkerID: task.WorkerID,
		LeaseExpiresAt: task.LeaseExpiresAt, AvailableAt: task.AvailableAt,
		Attempt: task.Attempt, MaxAttempts: task.MaxAttempts, Generation: task.Generation,
		ErrorCode: task.ErrorCode, CreatedAt: task.CreatedAt, UpdatedAt: task.UpdatedAt,
		CompletedAt: task.CompletedAt, HasResult: task.Result != nil,
		HasTraceContext: task.TraceContext.TraceParent != "" || task.TraceContext.TraceState != "",
	}
}

func taskMatchesQuery(task Task, query TaskQuery) bool {
	if query.ID != "" && task.ID != query.ID {
		return false
	}
	if query.TenantID != "" && task.TenantID != query.TenantID {
		return false
	}
	if query.SubjectID != "" && task.SubjectID != query.SubjectID {
		return false
	}
	if query.ScopePrefix != "" && !scopeHasPrefix(task.Scope, query.ScopePrefix) {
		return false
	}
	if query.Capability != "" && task.Capability != query.Capability {
		return false
	}
	if query.WorkerID != "" && task.WorkerID != query.WorkerID {
		return false
	}
	if len(query.States) > 0 {
		index := sort.Search(len(query.States), func(i int) bool { return query.States[i] >= task.State })
		if index == len(query.States) || query.States[index] != task.State {
			return false
		}
	}
	return true
}

func scopeHasPrefix(scope, prefix string) bool {
	return scope == prefix || strings.HasPrefix(scope, prefix+"/")
}

func cloneTraceContext(traceContext core.TelemetryTraceContext) core.TelemetryTraceContext {
	return core.TelemetryTraceContext{
		TraceParent: traceContext.TraceParent,
		TraceState:  traceContext.TraceState,
	}
}

func terminalResult(task Task) (core.CapabilityResult, bool, error) {
	switch task.State {
	case TaskCompleted:
		if task.Result == nil {
			return core.CapabilityResult{}, true, fmt.Errorf("runner task %s completed without a result", task.ID)
		}
		result, err := cloneResult(*task.Result)
		return result, true, err
	case TaskCancelled:
		return core.CapabilityResult{}, true, context.Canceled
	case TaskFailed:
		code := strings.TrimSpace(task.ErrorCode)
		if code == "" {
			code = "runner_failed"
		}
		return support.DeniedResult(code, fmt.Sprintf("runner task %s failed: %s", task.ID, code)), true, nil
	default:
		return core.CapabilityResult{}, false, nil
	}
}

func hasControl(value string) bool {
	return strings.IndexFunc(value, unicode.IsControl) >= 0
}

var _ Store = (*MemoryStore)(nil)
var _ TaskCatalog = (*MemoryStore)(nil)
var _ TaskRetrier = (*MemoryStore)(nil)
