package evaluation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	core "github.com/whhhh1500/auto-agent/pkg/core"
)

var (
	ErrDatasetNotFound = errors.New("evaluation dataset not found")
	ErrRunNotFound     = errors.New("evaluation run not found")
)

type Store interface {
	PutDataset(ctx context.Context, dataset Dataset) (Dataset, bool, error)
	GetDataset(ctx context.Context, id string, version int) (Dataset, error)
	ListDatasets(ctx context.Context, id string, limit int) ([]DatasetSummary, error)
	CreateRun(ctx context.Context, run RunResult) error
	RecordRunError(ctx context.Context, runID, message string) error
	RecordCaseResult(ctx context.Context, runID string, result CaseResult) error
	FinishRun(ctx context.Context, run RunResult) error
	GetRun(ctx context.Context, id string) (RunResult, error)
	ListRuns(ctx context.Context, datasetID, tenantID string, limit int) ([]RunResult, error)
}

// RunQuery is the indexed, bounded Evaluation Run listing surface. Revision
// filters match durable Case Artifacts; AssignmentRevision also matches the
// Evaluation Run's Composition metadata revision.
type RunQuery struct {
	DatasetID           string
	TenantID            string
	CompositionRevision string
	AssignmentRevision  string
	Limit               int
}

func (q RunQuery) Validate() error {
	if q.DatasetID != "" {
		if err := core.ValidateNamespacedID(q.DatasetID); err != nil {
			return fmt.Errorf("evaluation dataset id: %w", err)
		}
	}
	if err := validateOptionalQueryText("evaluation tenant_id", q.TenantID); err != nil {
		return err
	}
	for name, value := range map[string]string{
		"composition revision": q.CompositionRevision,
		"assignment revision":  q.AssignmentRevision,
	} {
		if value == "" {
			continue
		}
		if len(value) != 64 {
			return fmt.Errorf("evaluation %s must be a SHA-256 digest", name)
		}
		for _, char := range value {
			if !((char >= '0' && char <= '9') || (char >= 'a' && char <= 'f') || (char >= 'A' && char <= 'F')) {
				return fmt.Errorf("evaluation %s must be hexadecimal", name)
			}
		}
	}
	if q.Limit < 0 || q.Limit > 500 {
		return fmt.Errorf("evaluation run limit must be between 0 and 500")
	}
	return nil
}

func validateOptionalQueryText(name, value string) error {
	if value == "" {
		return nil
	}
	if strings.ContainsRune(value, '\x00') {
		return fmt.Errorf("%s contains NUL", name)
	}
	for _, char := range value {
		if char < 0x20 || char == 0x7f {
			return fmt.Errorf("%s contains a control character", name)
		}
	}
	if len([]rune(value)) > 128 {
		return fmt.Errorf("%s exceeds 128 runes", name)
	}
	return nil
}

// QueryStore is optional so integrations that only need the legacy list
// operation do not have to implement indexed revision queries.
type QueryStore interface {
	Store
	QueryRuns(ctx context.Context, query RunQuery) ([]RunResult, error)
}

type MemoryStore struct {
	mu                 sync.Mutex
	datasets           map[string]Dataset
	runs               map[string]RunResult
	maxRunning         int
	maxDatasetVersions int
	maxDatasetIDs      int
	maxRuns            int
	maxCases           int
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{datasets: map[string]Dataset{}, runs: map[string]RunResult{}}
}

func datasetKey(id string, version int) string { return fmt.Sprintf("%s\x00%d", id, version) }

func (s *MemoryStore) PutDataset(_ context.Context, dataset Dataset) (Dataset, bool, error) {
	if err := ValidateDataset(&dataset); err != nil {
		return Dataset{}, false, err
	}
	copyOf, err := cloneJSON(dataset)
	if err != nil {
		return Dataset{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := datasetKey(copyOf.ID, copyOf.Version)
	if existing, ok := s.datasets[key]; ok {
		if existing.Revision != copyOf.Revision {
			return Dataset{}, false, fmt.Errorf("dataset %s version %d already exists with another revision", copyOf.ID, copyOf.Version)
		}
		result, _ := cloneJSON(existing)
		return result, false, nil
	}
	if s.datasetVersionLocked(copyOf.ID) == 0 && s.datasetIDLocked() >= s.datasetIDCap() {
		return Dataset{}, false, fmt.Errorf("evaluation dataset ids exceed maximum of %d", s.datasetIDCap())
	}
	if s.datasetVersionLocked(copyOf.ID) >= s.datasetVersionCap() {
		return Dataset{}, false, fmt.Errorf("dataset %s versions exceed maximum of %d", copyOf.ID, s.datasetVersionCap())
	}
	s.datasets[key] = copyOf
	result, _ := cloneJSON(copyOf)
	return result, true, nil
}

func (s *MemoryStore) GetDataset(_ context.Context, id string, version int) (Dataset, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	dataset, ok := s.datasets[datasetKey(id, version)]
	if !ok {
		return Dataset{}, ErrDatasetNotFound
	}
	return cloneJSON(dataset)
}

func (s *MemoryStore) ListDatasets(_ context.Context, id string, limit int) ([]DatasetSummary, error) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []DatasetSummary{}
	for _, dataset := range s.datasets {
		if id == "" || dataset.ID == id {
			out = append(out, DatasetSummary{
				ID: dataset.ID, Version: dataset.Version, Revision: dataset.Revision,
				Name: dataset.Name, ProfileID: dataset.ProfileID, CaseCount: len(dataset.Cases), CreatedAt: dataset.CreatedAt,
			})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ID == out[j].ID {
			return out[i].Version > out[j].Version
		}
		return out[i].ID < out[j].ID
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (s *MemoryStore) CreateRun(_ context.Context, run RunResult) error {
	if len(run.Cases) != 0 {
		return fmt.Errorf("new evaluation run already contains case results")
	}
	run.CreatedAt = truncateEvaluationTime(run.CreatedAt)
	if err := normalizeMemoryStoreRunDefinition(&run); err != nil {
		return err
	}
	if err := ValidateRunResult(run, false); err != nil {
		return err
	}
	copyOf, err := cloneJSON(run)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.boundDatasetLocked(run); err != nil {
		return err
	}
	if existing, exists := s.runs[run.ID]; exists {
		if sameMemoryStoreRunHeader(existing, run) {
			return nil
		}
		return fmt.Errorf("evaluation run %s already exists", run.ID)
	}
	if s.runningLocked() >= s.runningCap() {
		return fmt.Errorf("running evaluation runs exceed maximum of %d", s.runningCap())
	}
	if len(s.runs) >= s.runCap() {
		return fmt.Errorf("evaluation runs exceed maximum of %d", s.runCap())
	}
	s.runs[run.ID] = copyOf
	return nil
}

func (s *MemoryStore) runningCap() int {
	if s != nil && s.maxRunning > 0 {
		return s.maxRunning
	}
	return MaxRunningEvaluationRuns
}

func (s *MemoryStore) datasetVersionCap() int {
	if s != nil && s.maxDatasetVersions > 0 {
		return s.maxDatasetVersions
	}
	return MaxDatasetVersionsPerID
}

func (s *MemoryStore) datasetIDCap() int {
	if s != nil && s.maxDatasetIDs > 0 {
		return s.maxDatasetIDs
	}
	return MaxDatasetIDs
}

func (s *MemoryStore) runCap() int {
	if s != nil && s.maxRuns > 0 {
		return s.maxRuns
	}
	return MaxEvaluationRuns
}

func (s *MemoryStore) caseCap() int {
	if s != nil && s.maxCases > 0 {
		return s.maxCases
	}
	return MaxDatasetCases
}

func (s *MemoryStore) datasetIDLocked() int {
	seen := map[string]bool{}
	for _, dataset := range s.datasets {
		seen[dataset.ID] = true
	}
	return len(seen)
}

func (s *MemoryStore) datasetVersionLocked(id string) int {
	count := 0
	for _, dataset := range s.datasets {
		if dataset.ID == id {
			count++
		}
	}
	return count
}

func (s *MemoryStore) runningLocked() int {
	count := 0
	for _, run := range s.runs {
		if run.Status == RunRunning {
			count++
		}
	}
	return count
}

func (s *MemoryStore) RecordCaseResult(_ context.Context, runID string, result CaseResult) error {
	if err := core.ValidateRunID(runID); err != nil {
		return fmt.Errorf("evaluation run id: %w", err)
	}
	if err := ValidateCaseResult(result); err != nil {
		return err
	}
	copyOf, err := cloneJSON(result)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	run, ok := s.runs[runID]
	if !ok {
		return ErrRunNotFound
	}
	dataset, err := s.boundDatasetLocked(run)
	if err != nil {
		return err
	}
	if !datasetHasCase(dataset, result.CaseID) {
		return fmt.Errorf("evaluation case result %s is outside the dataset", result.CaseID)
	}
	for _, existing := range run.Cases {
		if existing.CaseID == result.CaseID {
			if sameMemoryStoreJSON(existing, result) {
				return nil
			}
			return fmt.Errorf("case result %s already exists", result.CaseID)
		}
	}
	if run.Status != RunRunning {
		return fmt.Errorf("evaluation run %s is already terminal", runID)
	}
	if len(run.Cases) >= s.caseCap() {
		return fmt.Errorf("evaluation case results exceed maximum of %d", s.caseCap())
	}
	run.Cases = append(run.Cases, copyOf)
	s.runs[runID] = run
	return nil
}

func (s *MemoryStore) RecordRunError(_ context.Context, runID, message string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	run, ok := s.runs[runID]
	if !ok {
		return ErrRunNotFound
	}
	if run.Status != RunRunning {
		return fmt.Errorf("evaluation run %s is already terminal", runID)
	}
	run.Error = boundedEvaluationMessage(message)
	s.runs[runID] = run
	return nil
}

func (s *MemoryStore) FinishRun(_ context.Context, run RunResult) error {
	run.CompletedAt = truncateEvaluationTime(run.CompletedAt)
	if err := normalizeMemoryStoreRunDefinition(&run); err != nil {
		return err
	}
	if err := ValidateRunResult(run, true); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	existing, ok := s.runs[run.ID]
	if !ok {
		return ErrRunNotFound
	}
	dataset, err := s.boundDatasetLocked(existing)
	if err != nil {
		return err
	}
	if !sameMemoryStoreRunHeader(existing, run) {
		return fmt.Errorf("evaluation run %s has another immutable definition", run.ID)
	}
	if len(run.Cases) != 0 && !sameMemoryStoreCaseResults(run.Cases, existing.Cases) {
		return fmt.Errorf("evaluation run %s supplied case results do not match persisted results", run.ID)
	}
	if !caseResultsMatchDataset(existing.Cases, dataset) {
		return fmt.Errorf("evaluation run %s has case results outside its dataset", run.ID)
	}
	if existing.Status != RunRunning {
		if sameMemoryStoreTerminalResult(existing, run) {
			return nil
		}
		return fmt.Errorf("evaluation run %s is already terminal with another result", run.ID)
	}
	if run.Status == RunCompleted && !caseResultsCoverDataset(existing.Cases, dataset) {
		return fmt.Errorf("evaluation run %s has %d of %d case results", run.ID, len(existing.Cases), len(dataset.Cases))
	}
	existing.Status = run.Status
	existing.Score = run.Score
	existing.Passed = run.Passed
	existing.PassedCases = run.PassedCases
	existing.Error = run.Error
	existing.CompletedAt = run.CompletedAt
	s.runs[run.ID] = existing
	return nil
}

func (s *MemoryStore) GetRun(_ context.Context, id string) (RunResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	run, ok := s.runs[id]
	if !ok {
		return RunResult{}, ErrRunNotFound
	}
	return cloneJSON(run)
}

func (s *MemoryStore) ListRuns(_ context.Context, datasetID, tenantID string, limit int) ([]RunResult, error) {
	return s.QueryRuns(context.Background(), RunQuery{DatasetID: datasetID, TenantID: tenantID, Limit: limit})
}

func (s *MemoryStore) QueryRuns(_ context.Context, query RunQuery) ([]RunResult, error) {
	if err := query.Validate(); err != nil {
		return nil, err
	}
	limit := query.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []RunResult{}
	for _, run := range s.runs {
		if (query.DatasetID != "" && run.DatasetID != query.DatasetID) ||
			(query.TenantID != "" && run.TenantID != query.TenantID) {
			continue
		}
		if query.AssignmentRevision != "" {
			revision := run.AssignmentRevision
			if revision == "" {
				revision, _ = core.CompositionMetadataRevision(run.CompositionMetadata)
			}
			matched := revision == query.AssignmentRevision
			for _, result := range run.Cases {
				matched = matched || result.Artifacts.AssignmentRevision == query.AssignmentRevision
			}
			if !matched {
				continue
			}
		}
		if query.CompositionRevision != "" {
			matched := false
			for _, result := range run.Cases {
				matched = matched || result.Artifacts.CompositionRevision == query.CompositionRevision
			}
			if !matched {
				continue
			}
		}
		copyOf, _ := cloneJSON(run)
		copyOf.Cases = nil
		out = append(out, copyOf)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func cloneJSON[T any](value T) (T, error) {
	var out T
	encoded, err := json.Marshal(value)
	if err != nil {
		return out, err
	}
	if err := json.Unmarshal(encoded, &out); err != nil {
		return out, err
	}
	return out, nil
}

func (s *MemoryStore) boundDatasetLocked(run RunResult) (Dataset, error) {
	dataset, ok := s.datasets[datasetKey(run.DatasetID, run.DatasetVersion)]
	if !ok {
		return Dataset{}, fmt.Errorf("evaluation run dataset %s version %d is not stored", run.DatasetID, run.DatasetVersion)
	}
	if dataset.Revision != run.DatasetRevision || len(dataset.Cases) != run.TotalCases {
		return Dataset{}, fmt.Errorf("evaluation run dataset definition does not match stored dataset")
	}
	return dataset, nil
}

func normalizeMemoryStoreRunDefinition(run *RunResult) error {
	if run.AssignmentRevision != "" {
		return nil
	}
	assignmentRevision, err := core.CompositionMetadataRevision(run.CompositionMetadata)
	if err != nil {
		return fmt.Errorf("evaluation assignment revision: %w", err)
	}
	run.AssignmentRevision = assignmentRevision
	return nil
}

func truncateEvaluationTime(value time.Time) time.Time {
	if value.IsZero() {
		return value
	}
	return time.UnixMilli(value.UnixMilli()).UTC()
}

func datasetHasCase(dataset Dataset, caseID string) bool {
	for _, evalCase := range dataset.Cases {
		if evalCase.ID == caseID {
			return true
		}
	}
	return false
}

func caseResultsMatchDataset(results []CaseResult, dataset Dataset) bool {
	datasetCaseIDs := make(map[string]bool, len(dataset.Cases))
	for _, evalCase := range dataset.Cases {
		datasetCaseIDs[evalCase.ID] = true
	}
	seen := make(map[string]bool, len(results))
	for _, result := range results {
		if seen[result.CaseID] || !datasetCaseIDs[result.CaseID] {
			return false
		}
		seen[result.CaseID] = true
	}
	return true
}

func caseResultsCoverDataset(results []CaseResult, dataset Dataset) bool {
	return len(results) == len(dataset.Cases) && caseResultsMatchDataset(results, dataset)
}

func sameMemoryStoreRunHeader(left, right RunResult) bool {
	left.Cases, right.Cases = nil, nil
	left.CreatedAt, right.CreatedAt = time.Time{}, time.Time{}
	left.CompletedAt, right.CompletedAt = time.Time{}, time.Time{}
	left.Status, right.Status = RunRunning, RunRunning
	left.Score, right.Score = 0, 0
	left.Passed, right.Passed = false, false
	left.PassedCases, right.PassedCases = 0, 0
	left.Error, right.Error = "", ""
	return sameMemoryStoreJSON(left, right)
}

func sameMemoryStoreTerminalResult(left, right RunResult) bool {
	return left.Status == right.Status && left.Score == right.Score && left.Passed == right.Passed &&
		left.PassedCases == right.PassedCases && left.Error == right.Error
}

func sameMemoryStoreCaseResults(left, right []CaseResult) bool {
	if len(left) != len(right) {
		return false
	}
	byID := make(map[string]CaseResult, len(right))
	for _, result := range right {
		if _, exists := byID[result.CaseID]; exists {
			return false
		}
		byID[result.CaseID] = result
	}
	for _, result := range left {
		existing, ok := byID[result.CaseID]
		if !ok || !sameMemoryStoreJSON(existing, result) {
			return false
		}
	}
	return true
}

func sameMemoryStoreJSON[T any](left, right T) bool {
	leftJSON, leftErr := json.Marshal(left)
	rightJSON, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && string(leftJSON) == string(rightJSON)
}

var _ Store = (*MemoryStore)(nil)
var _ QueryStore = (*MemoryStore)(nil)
