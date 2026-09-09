package control

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	core "github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/evaluation"
)

var ErrCanaryNotFound = errors.New("canary not found")

type CanaryStatus string

const (
	CanaryActive     CanaryStatus = "active"
	CanaryPaused     CanaryStatus = "paused"
	CanaryPromoting  CanaryStatus = "promoting"
	CanaryPromoted   CanaryStatus = "promoted"
	CanaryRolledBack CanaryStatus = "rolled_back"
)

type CanaryRecord struct {
	ID                       string                  `json:"id"`
	ProfileID                string                  `json:"profile_id"`
	Scope                    core.ScopePath          `json:"scope"`
	Layer                    *core.AgentProfileLayer `json:"-"`
	Revision                 string                  `json:"revision"`
	BaseReleaseRevision      string                  `json:"base_release_revision,omitempty"`
	Status                   CanaryStatus            `json:"status"`
	BasisPoints              int                     `json:"basis_points"`
	BaselineEvaluationRunID  string                  `json:"baseline_evaluation_run_id,omitempty"`
	CandidateEvaluationRunID string                  `json:"candidate_evaluation_run_id"`
	Gate                     evaluation.GateResult   `json:"gate"`
	ReleaseVersion           int                     `json:"release_version,omitempty"`
	CreatedAt                time.Time               `json:"created_at"`
	UpdatedAt                time.Time               `json:"updated_at"`
}

// CanaryAssignment is one Subject's stable decision for an applicable open
// Canary. Candidate=false means the run uses Live because the Canary is paused
// or the Subject bucket falls outside BasisPoints.
type CanaryAssignment struct {
	CanaryRecord
	Bucket    int  `json:"bucket"`
	Candidate bool `json:"candidate"`
}

type CanaryStore interface {
	CreateCanary(ctx context.Context, record CanaryRecord) error
	UpdateCanary(ctx context.Context, record CanaryRecord, expected ...CanaryStatus) (bool, error)
	GetCanary(ctx context.Context, id string) (CanaryRecord, error)
	ListOpenCanaries(ctx context.Context) ([]CanaryRecord, error)
	ListCanaries(ctx context.Context, profileID string, limit int) ([]CanaryRecord, error)
	ControlRevision(ctx context.Context) (int64, error)
}

type canaryState struct {
	record   CanaryRecord
	profiles *core.AgentProfileRegistry
}

type CanaryManager struct {
	Releases *ReleaseManager
	Store    CanaryStore

	mu             sync.RWMutex
	states         map[string]*canaryState
	syncedRevision int64
	restored       bool
}

func NewCanaryManager(releases *ReleaseManager, store CanaryStore) (*CanaryManager, error) {
	if releases == nil || releases.Profiles == nil {
		return nil, fmt.Errorf("canary manager requires a release manager")
	}
	if store == nil {
		return nil, fmt.Errorf("canary manager requires durable storage")
	}
	return &CanaryManager{Releases: releases, Store: store, states: map[string]*canaryState{}, syncedRevision: -1}, nil
}

func ValidateCanaryRecord(record CanaryRecord) error {
	if err := core.ValidateRunID(record.ID); err != nil {
		return fmt.Errorf("canary id: %w", err)
	}
	if err := core.ValidateNamespacedID(record.ProfileID); err != nil {
		return fmt.Errorf("canary profile id: %w", err)
	}
	if record.Scope.Depth() == 0 || record.Layer == nil || record.Revision == "" {
		return fmt.Errorf("canary artifact is incomplete")
	}
	if record.Layer.ProfileID != record.ProfileID {
		return fmt.Errorf("canary layer profile id mismatch")
	}
	if record.BaseReleaseRevision == "" && (record.Status == CanaryPromoted || record.Status == CanaryRolledBack) {
		// Pre-v17 terminal history can remain readable; open legacy records
		// fail closed because their evaluated Release baseline is unknowable.
	} else {
		if len(record.BaseReleaseRevision) != sha256.Size*2 {
			return fmt.Errorf("canary base release revision is invalid")
		}
		if _, err := hex.DecodeString(record.BaseReleaseRevision); err != nil {
			return fmt.Errorf("canary base release revision: %w", err)
		}
	}
	layer := core.CloneAgentProfileLayer(*record.Layer)
	layer.Scope = record.Scope
	revision, err := core.ProfileLayerRevision(layer)
	if err != nil || revision != record.Revision {
		return fmt.Errorf("canary artifact revision mismatch")
	}
	switch record.Status {
	case CanaryActive, CanaryPaused, CanaryPromoting, CanaryPromoted, CanaryRolledBack:
	default:
		return fmt.Errorf("canary status %q is invalid", record.Status)
	}
	if record.BasisPoints < 1 || record.BasisPoints > 10000 {
		return fmt.Errorf("canary basis points must be between 1 and 10000")
	}
	if !record.Gate.Passed {
		return fmt.Errorf("canary gate did not pass")
	}
	if err := evaluation.ValidateGateResult(record.Gate); err != nil {
		return fmt.Errorf("canary coverage gate: %w", err)
	}
	if record.Gate.RequiredEfficiencyContract != "" {
		if record.Gate.RequiredEfficiencyContract != evaluation.EfficiencyGateContractV1 || record.Gate.Efficiency == nil ||
			record.Gate.Efficiency.ContractID != record.Gate.RequiredEfficiencyContract ||
			record.Gate.Efficiency.Verdict != evaluation.EfficiencyGatePassed {
			return fmt.Errorf("canary required efficiency gate did not pass")
		}
	}
	if record.Status == CanaryPromoted {
		if record.ReleaseVersion < 1 {
			return fmt.Errorf("promoted canary has no release version")
		}
	} else if record.ReleaseVersion != 0 {
		return fmt.Errorf("unpromoted canary has a release version")
	}
	if err := core.ValidateRunID(record.CandidateEvaluationRunID); err != nil {
		return fmt.Errorf("candidate evaluation run id: %w", err)
	}
	if record.BaselineEvaluationRunID != "" {
		if err := core.ValidateRunID(record.BaselineEvaluationRunID); err != nil {
			return fmt.Errorf("baseline evaluation run id: %w", err)
		}
	}
	if record.CreatedAt.IsZero() || record.UpdatedAt.IsZero() {
		return fmt.Errorf("canary timestamps are incomplete")
	}
	return nil
}

func (m *CanaryManager) Restore(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.restored {
		return nil
	}
	records, err := m.Store.ListOpenCanaries(ctx)
	if err != nil {
		return err
	}
	states := map[string]*canaryState{}
	releaseReservations := func() {
		for _, state := range states {
			m.Releases.releaseReservation(state.record.ProfileID, state.record.ID)
		}
	}
	for _, record := range records {
		if err := ValidateCanaryRecord(record); err != nil {
			releaseReservations()
			return fmt.Errorf("restore canary %s: %w", record.ID, err)
		}
		if record.Status == CanaryPromoting {
			if _, err := m.reconcilePromotion(ctx, record); err != nil {
				releaseReservations()
				return fmt.Errorf("resume canary promotion %s: %w", record.ID, err)
			}
			continue
		}
		if record.Status != CanaryActive && record.Status != CanaryPaused {
			continue
		}
		state, err := m.buildState(record)
		if err != nil {
			releaseReservations()
			return fmt.Errorf("restore canary %s: %w", record.ID, err)
		}
		if conflict := activeCanaryConflict(states, record); conflict != "" {
			m.Releases.releaseReservation(record.ProfileID, record.ID)
			releaseReservations()
			return fmt.Errorf("canary %s conflicts with %s", record.ID, conflict)
		}
		states[record.ID] = state
	}
	m.states, m.restored, m.syncedRevision = states, true, -1
	return nil
}

// Refresh synchronizes Releases and open Canary records written by another
// manager instance. Existing Candidate registries remain fixed; only mutable
// rollout fields are replaced. New records rebuild their isolated registry,
// terminal records disappear from local routing, and promoting records are
// reconciled idempotently.
func (m *CanaryManager) Refresh(ctx context.Context) error {
	if err := m.Releases.Sync(ctx); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.restored {
		return fmt.Errorf("canary manager must be restored before refresh")
	}
	startRevision, err := m.Store.ControlRevision(ctx)
	if err != nil {
		return fmt.Errorf("read control revision: %w", err)
	}
	if startRevision == m.syncedRevision {
		return nil
	}
	records, err := m.Store.ListOpenCanaries(ctx)
	if err != nil {
		return err
	}
	next := map[string]*canaryState{}
	newReservations := []CanaryRecord{}
	releaseNewReservations := func() {
		for _, record := range newReservations {
			m.Releases.releaseReservation(record.ProfileID, record.ID)
		}
	}
	for _, record := range records {
		if err := ValidateCanaryRecord(record); err != nil {
			releaseNewReservations()
			return fmt.Errorf("refresh canary %s: %w", record.ID, err)
		}
		if record.Status == CanaryPromoting {
			if _, err := m.reconcilePromotion(ctx, record); err != nil {
				releaseNewReservations()
				return fmt.Errorf("refresh canary promotion %s: %w", record.ID, err)
			}
			continue
		}
		if record.Status != CanaryActive && record.Status != CanaryPaused {
			continue
		}
		if existing := m.states[record.ID]; existing != nil {
			if err := matchCanaryArtifact(existing.record, record); err != nil {
				releaseNewReservations()
				return err
			}
			state := &canaryState{record: cloneCanaryRecord(record), profiles: existing.profiles}
			if conflict := activeCanaryConflict(next, record); conflict != "" {
				releaseNewReservations()
				return fmt.Errorf("canary %s conflicts with %s", record.ID, conflict)
			}
			next[record.ID] = state
			continue
		}
		state, err := m.buildState(record)
		if err != nil {
			releaseNewReservations()
			return fmt.Errorf("refresh canary %s: %w", record.ID, err)
		}
		newReservations = append(newReservations, record)
		if conflict := activeCanaryConflict(next, record); conflict != "" {
			m.Releases.releaseReservation(record.ProfileID, record.ID)
			releaseNewReservations()
			return fmt.Errorf("canary %s conflicts with %s", record.ID, conflict)
		}
		next[record.ID] = state
	}
	for id, state := range m.states {
		if next[id] == nil {
			m.Releases.releaseReservation(state.record.ProfileID, id)
		}
	}
	m.states = next
	endRevision, err := m.Store.ControlRevision(ctx)
	if err != nil {
		m.syncedRevision = -1
		return fmt.Errorf("re-read control revision: %w", err)
	}
	if endRevision != startRevision {
		m.syncedRevision = -1
		return fmt.Errorf("control plane changed during canary refresh")
	}
	m.syncedRevision = endRevision
	return nil
}

func (m *CanaryManager) Stage(ctx context.Context, record CanaryRecord) (CanaryRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.restored {
		return CanaryRecord{}, fmt.Errorf("canary manager must be restored before staging")
	}
	if record.ID == "" {
		id, err := core.NewID("canary_")
		if err != nil {
			return CanaryRecord{}, err
		}
		record.ID = id
	}
	if record.Status == "" {
		record.Status = CanaryActive
	}
	if record.Status != CanaryActive || !record.Gate.Passed {
		return CanaryRecord{}, fmt.Errorf("new canary must be active with a passing gate")
	}
	if record.BasisPoints < 1 || record.BasisPoints > 10000 {
		return CanaryRecord{}, fmt.Errorf("canary basis points must be between 1 and 10000")
	}
	if conflict := activeCanaryConflict(m.states, record); conflict != "" {
		return CanaryRecord{}, fmt.Errorf("profile/scope already has active canary %s", conflict)
	}
	if record.BaseReleaseRevision == "" {
		baseline, err := m.Releases.BaselineRevision(record.ProfileID, record.Scope)
		if err != nil {
			return CanaryRecord{}, err
		}
		record.BaseReleaseRevision = baseline
	}
	state, err := m.buildState(record)
	if err != nil {
		return CanaryRecord{}, err
	}
	now := time.Now().UTC()
	if record.CreatedAt.IsZero() {
		record.CreatedAt = now
	}
	record.UpdatedAt = now
	state.record = cloneCanaryRecord(record)
	if err := ValidateCanaryRecord(state.record); err != nil {
		m.Releases.releaseReservation(record.ProfileID, record.ID)
		return CanaryRecord{}, err
	}
	if err := m.Store.CreateCanary(ctx, state.record); err != nil {
		m.Releases.releaseReservation(record.ProfileID, record.ID)
		return CanaryRecord{}, err
	}
	m.syncedRevision = -1
	m.states[record.ID] = state
	return cloneCanaryRecord(state.record), nil
}

func (m *CanaryManager) SetBasisPoints(ctx context.Context, id string, basisPoints int) (CanaryRecord, error) {
	if basisPoints < 1 || basisPoints > 10000 {
		return CanaryRecord{}, fmt.Errorf("canary basis points must be between 1 and 10000")
	}
	return m.update(ctx, id, []CanaryStatus{CanaryActive, CanaryPaused}, func(record *CanaryRecord) {
		record.BasisPoints = basisPoints
	})
}

func (m *CanaryManager) Pause(ctx context.Context, id string) (CanaryRecord, error) {
	return m.update(ctx, id, []CanaryStatus{CanaryActive}, func(record *CanaryRecord) { record.Status = CanaryPaused })
}

func (m *CanaryManager) Resume(ctx context.Context, id string) (CanaryRecord, error) {
	return m.update(ctx, id, []CanaryStatus{CanaryPaused}, func(record *CanaryRecord) { record.Status = CanaryActive })
}

func (m *CanaryManager) Rollback(ctx context.Context, id string) (CanaryRecord, error) {
	record, err := m.update(ctx, id, []CanaryStatus{CanaryActive, CanaryPaused}, func(record *CanaryRecord) {
		record.Status = CanaryRolledBack
	})
	if err != nil {
		return CanaryRecord{}, err
	}
	m.mu.Lock()
	delete(m.states, id)
	m.mu.Unlock()
	m.Releases.releaseReservation(record.ProfileID, record.ID)
	return record, nil
}

// Promote durably transitions an open canary through promoting and publishes
// its artifact with the canary id as an idempotency operation. A retry returns
// the same release version, including after a restart between publish and the
// final canary state update.
func (m *CanaryManager) Promote(ctx context.Context, id string) (CanaryRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	state := m.states[id]
	if state == nil {
		record, err := m.Store.GetCanary(ctx, id)
		if err != nil {
			return CanaryRecord{}, err
		}
		if record.Status == CanaryPromoted {
			return record, nil
		}
		if record.Status != CanaryPromoting {
			return CanaryRecord{}, fmt.Errorf("canary %s is not promotable", id)
		}
		state = &canaryState{record: cloneCanaryRecord(record)}
		m.states[id] = state
	}
	record := cloneCanaryRecord(state.record)
	if record.Status == CanaryActive || record.Status == CanaryPaused {
		previous := record.Status
		if err := m.Releases.verifyBaseline(record.ProfileID, record.Scope, record.BaseReleaseRevision); err != nil {
			return CanaryRecord{}, err
		}
		record.Status = CanaryPromoting
		record.UpdatedAt = time.Now().UTC()
		changed, err := m.Store.UpdateCanary(ctx, record, previous)
		if err != nil {
			return CanaryRecord{}, err
		}
		if !changed {
			return CanaryRecord{}, fmt.Errorf("canary %s state changed concurrently", id)
		}
		m.syncedRevision = -1
		state.record = cloneCanaryRecord(record)
	} else if record.Status != CanaryPromoting {
		return CanaryRecord{}, fmt.Errorf("canary %s is not promotable", id)
	}
	promoted, err := m.reconcilePromotion(ctx, record)
	if err != nil {
		return CanaryRecord{}, err
	}
	delete(m.states, id)
	m.Releases.releaseReservation(record.ProfileID, record.ID)
	return promoted, nil
}

func (m *CanaryManager) reconcilePromotion(ctx context.Context, record CanaryRecord) (CanaryRecord, error) {
	if record.Status != CanaryPromoting || record.Layer == nil {
		return CanaryRecord{}, fmt.Errorf("canary %s has no resumable promotion", record.ID)
	}
	release, err := m.Releases.PublishWithOperationAtBaseline(
		ctx, record.Scope, *record.Layer, record.ID, record.BaseReleaseRevision,
	)
	if err != nil {
		return CanaryRecord{}, fmt.Errorf("publish canary %s: %w", record.ID, err)
	}
	updated := cloneCanaryRecord(record)
	updated.Status = CanaryPromoted
	updated.ReleaseVersion = release.Version
	updated.UpdatedAt = time.Now().UTC()
	changed, err := m.Store.UpdateCanary(ctx, updated, CanaryPromoting)
	if err != nil {
		return CanaryRecord{}, err
	}
	if changed {
		m.syncedRevision = -1
		m.Releases.releaseReservation(record.ProfileID, record.ID)
		return updated, nil
	}
	current, err := m.Store.GetCanary(ctx, record.ID)
	if err == nil && current.Status == CanaryPromoted && current.ReleaseVersion == release.Version {
		m.Releases.releaseReservation(record.ProfileID, record.ID)
		return current, nil
	}
	return CanaryRecord{}, fmt.Errorf("canary %s promotion state changed concurrently", record.ID)
}

func (m *CanaryManager) Select(principal core.Principal, profileID string) (*core.AgentProfileRegistry, *CanaryRecord) {
	profiles, assignment := m.Assign(principal, profileID)
	if assignment == nil || !assignment.Candidate {
		return profiles, nil
	}
	record := cloneCanaryRecord(assignment.CanaryRecord)
	return profiles, &record
}

// Assign returns the full rollout decision, including Live bucket misses and
// paused Canaries, so adapters can persist the actual assignment evidence.
func (m *CanaryManager) Assign(principal core.Principal, profileID string) (*core.AgentProfileRegistry, *CanaryAssignment) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var selected *canaryState
	for _, state := range m.states {
		record := state.record
		if (record.Status != CanaryActive && record.Status != CanaryPaused) ||
			record.ProfileID != profileID || !record.Scope.IsAncestorOf(principal.Scope) {
			continue
		}
		if selected == nil || record.Scope.Depth() > selected.record.Scope.Depth() {
			selected = state
		}
	}
	if selected == nil {
		return m.Releases.Profiles, nil
	}
	bucket := canaryBucket(selected.record.ID, principal)
	assignment := &CanaryAssignment{
		CanaryRecord: cloneCanaryRecord(selected.record), Bucket: bucket,
		Candidate: selected.record.Status == CanaryActive && bucket < selected.record.BasisPoints,
	}
	if assignment.Candidate {
		return selected.profiles, assignment
	}
	return m.Releases.Profiles, assignment
}

func (m *CanaryManager) Get(ctx context.Context, id string) (CanaryRecord, error) {
	return m.Store.GetCanary(ctx, id)
}

func (m *CanaryManager) List(ctx context.Context, profileID string, limit int) ([]CanaryRecord, error) {
	return m.Store.ListCanaries(ctx, profileID, limit)
}

func (m *CanaryManager) update(ctx context.Context, id string, expected []CanaryStatus, mutate func(*CanaryRecord)) (CanaryRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	state := m.states[id]
	if state == nil {
		return CanaryRecord{}, fmt.Errorf("canary %s is not active in this process", id)
	}
	updated := cloneCanaryRecord(state.record)
	mutate(&updated)
	updated.UpdatedAt = time.Now().UTC()
	changed, err := m.Store.UpdateCanary(ctx, updated, expected...)
	if err != nil {
		return CanaryRecord{}, err
	}
	if !changed {
		return CanaryRecord{}, fmt.Errorf("canary %s state changed concurrently", id)
	}
	m.syncedRevision = -1
	state.record = cloneCanaryRecord(updated)
	return cloneCanaryRecord(updated), nil
}

func (m *CanaryManager) buildState(record CanaryRecord) (*canaryState, error) {
	if record.Layer == nil || record.ProfileID == "" || record.Scope.Depth() == 0 || record.Revision == "" {
		return nil, fmt.Errorf("canary artifact is incomplete")
	}
	layer := core.CloneAgentProfileLayer(*record.Layer)
	layer.Scope = record.Scope
	if layer.ProfileID != record.ProfileID {
		return nil, fmt.Errorf("canary layer profile does not match record")
	}
	revision, err := core.ProfileLayerRevision(layer)
	if err != nil {
		return nil, fmt.Errorf("calculate canary layer revision: %w", err)
	}
	if revision != record.Revision {
		return nil, fmt.Errorf("canary layer revision mismatch")
	}
	profiles, preparedRevision, err := m.Releases.prepareReservedCandidate(
		record.Scope, layer, record.ID, record.BaseReleaseRevision,
	)
	if err != nil {
		return nil, fmt.Errorf("prepare canary artifact: %w", err)
	}
	if preparedRevision != record.Revision {
		return nil, fmt.Errorf("prepared canary revision mismatch")
	}
	record.Layer = &layer
	return &canaryState{record: cloneCanaryRecord(record), profiles: profiles}, nil
}

func activeCanaryConflict(states map[string]*canaryState, record CanaryRecord) string {
	for id, state := range states {
		if state.record.ProfileID == record.ProfileID && state.record.Scope.Equal(record.Scope) &&
			(state.record.Status == CanaryActive || state.record.Status == CanaryPaused || state.record.Status == CanaryPromoting) {
			return id
		}
	}
	return ""
}

func canaryBucket(canaryID string, principal core.Principal) int {
	sum := sha256.Sum256([]byte(canaryID + "\x00" + principal.TenantID + "\x00" + principal.SubjectID))
	return int(binary.BigEndian.Uint64(sum[:8]) % 10000)
}

func cloneCanaryRecord(record CanaryRecord) CanaryRecord {
	out := record
	if record.Layer != nil {
		layer := core.CloneAgentProfileLayer(*record.Layer)
		out.Layer = &layer
	}
	out.Gate.Reasons = append([]string(nil), record.Gate.Reasons...)
	if record.Gate.Comparison != nil {
		comparison := *record.Gate.Comparison
		comparison.CaseDeltas = map[string]float64{}
		for key, value := range record.Gate.Comparison.CaseDeltas {
			comparison.CaseDeltas[key] = value
		}
		out.Gate.Comparison = &comparison
	}
	if record.Gate.CapabilityCompatibility != nil {
		compatibility := *record.Gate.CapabilityCompatibility
		compatibility.Added = append([]string(nil), compatibility.Added...)
		compatibility.Removed = append([]string(nil), compatibility.Removed...)
		compatibility.Changed = append([]string(nil), compatibility.Changed...)
		compatibility.Issues = append([]evaluation.CapabilityCompatibilityIssue(nil), compatibility.Issues...)
		out.Gate.CapabilityCompatibility = &compatibility
	}
	if record.Gate.Efficiency != nil {
		efficiency := *record.Gate.Efficiency
		efficiency.ReasonCodes = append([]evaluation.EfficiencyGateReasonCode(nil), record.Gate.Efficiency.ReasonCodes...)
		efficiency.Pairs = append([]evaluation.EfficiencyGatePair(nil), record.Gate.Efficiency.Pairs...)
		efficiency.Strata = append([]evaluation.EfficiencyGateStratum(nil), record.Gate.Efficiency.Strata...)
		out.Gate.Efficiency = &efficiency
	}
	if record.Gate.Coverage != nil {
		coverage := *record.Gate.Coverage
		coverage.ReasonCodes = append([]evaluation.CoverageReasonCode(nil), record.Gate.Coverage.ReasonCodes...)
		out.Gate.Coverage = &coverage
	}
	return out
}

func matchCanaryArtifact(local, durable CanaryRecord) error {
	if local.ID != durable.ID || local.ProfileID != durable.ProfileID || !local.Scope.Equal(durable.Scope) ||
		local.Revision != durable.Revision || local.BaseReleaseRevision != durable.BaseReleaseRevision ||
		local.CandidateEvaluationRunID != durable.CandidateEvaluationRunID ||
		local.BaselineEvaluationRunID != durable.BaselineEvaluationRunID ||
		local.Gate.RequiredCoverageRevision != durable.Gate.RequiredCoverageRevision ||
		!reflect.DeepEqual(local.Gate.Coverage, durable.Gate.Coverage) ||
		local.Gate.RequiredEfficiencyContract != durable.Gate.RequiredEfficiencyContract ||
		!reflect.DeepEqual(local.Gate.Efficiency, durable.Gate.Efficiency) {
		return fmt.Errorf("durable canary %s changed immutable fields", local.ID)
	}
	return nil
}
