package control

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
)

var (
	ErrReleaseBaselineDrift = errors.New("profile release baseline drift")
	ErrReleaseReserved      = errors.New("profile release is reserved by an open canary")
)

type releaseReservation struct {
	Owner             string
	ProfileID         string
	Scope             core.ScopePath
	BaselineRevision  string
	CandidateRevision string
}

// ProfileRelease is one published profile change at one scope. A publish
// mounts exactly the layers it carried; a rollback unmounts everything that
// came after the target version, in reverse order. Releases compose with the
// live registry: unmount is the same reversible primitive plugins use, so
// rollback never leaves a half-applied state.
type ProfileRelease struct {
	ProfileID   string
	Version     int
	OperationID string
	Scope       core.ScopePath
	CreatedAt   time.Time
	RolledBack  bool
	Layer       core.AgentProfileLayer
	Revision    string

	unmounts []func()
}

// ReleaseInfo is the audit view of a release (no unmount handles).
type ReleaseInfo struct {
	ProfileID   string                  `json:"profile_id"`
	Version     int                     `json:"version"`
	OperationID string                  `json:"operation_id,omitempty"`
	Scope       core.ScopePath          `json:"scope"`
	CreatedAt   time.Time               `json:"created_at"`
	RolledBack  bool                    `json:"rolled_back"`
	Layer       *core.AgentProfileLayer `json:"-"`
	Revision    string                  `json:"revision,omitempty"`
}

// ReleaseJournal stores publish and rollback history across process lifetimes.
type ReleaseJournal interface {
	RecordRelease(ctx context.Context, info ReleaseInfo) error
	MarkRolledBack(ctx context.Context, profileID string, versions []int) error
	LoadReleases(ctx context.Context, profileID string) ([]ReleaseInfo, error)
	ListReleaseProfiles(ctx context.Context) ([]string, error)
	FindReleaseByOperation(ctx context.Context, operationID string) (ReleaseInfo, bool, error)
	ControlRevision(ctx context.Context) (int64, error)
}

// ReleaseManager adds publish / version / rollback on top of the live
// profile registry. It is the kernel-side primitive behind a Control Plane
// release API: publish captures "which layers this act added", so rollback
// is exact rather than best-effort.
//
// When a Journal is configured, every publish and rollback is durably
// recorded and History reads from it. Restore rebuilds live mounts and
// unmount handles from the persisted layer artifacts.
type ReleaseManager struct {
	Profiles *core.AgentProfileRegistry
	// Journal durably records release history. Optional.
	Journal ReleaseJournal

	mu             sync.Mutex
	next           map[string]int
	releases       map[string][]*ProfileRelease
	reserved       map[string]releaseReservation
	syncedRevision int64
	restored       bool
}

func NewReleaseManager(profiles *core.AgentProfileRegistry) (*ReleaseManager, error) {
	if profiles == nil {
		return nil, fmt.Errorf("release manager requires a profile registry")
	}
	return &ReleaseManager{
		Profiles:       profiles,
		next:           map[string]int{},
		releases:       map[string][]*ProfileRelease{},
		reserved:       map[string]releaseReservation{},
		syncedRevision: -1,
	}, nil
}

// PrepareCandidate mounts one layer into an independent registry clone. The
// live registry, release history, version counter, and journal are untouched.
func (m *ReleaseManager) PrepareCandidate(scope core.ScopePath, layer core.AgentProfileLayer) (*core.AgentProfileRegistry, string, error) {
	candidate, revision, _, err := m.PrepareCandidateAtCurrentBaseline(scope, layer)
	return candidate, revision, err
}

// PrepareCandidateAtCurrentBaseline clones the live registry and returns the
// Release baseline revision against which the candidate was composed. A later
// gated publish or Canary Stage can require the same baseline.
func (m *ReleaseManager) PrepareCandidateAtCurrentBaseline(scope core.ScopePath, layer core.AgentProfileLayer) (*core.AgentProfileRegistry, string, string, error) {
	if m == nil || m.Profiles == nil {
		return nil, "", "", fmt.Errorf("release manager is not configured")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Journal != nil && !m.restored {
		return nil, "", "", fmt.Errorf("release journal must be restored before preparing a candidate")
	}
	candidate, revision, err := m.prepareCandidateLocked(scope, layer)
	if err != nil {
		return nil, "", "", err
	}
	baseline, err := m.baselineRevisionLocked(layer.ProfileID, scope)
	if err != nil {
		return nil, "", "", err
	}
	return candidate, revision, baseline, nil
}

func (m *ReleaseManager) prepareReservedCandidate(scope core.ScopePath, layer core.AgentProfileLayer, owner, expectedBaseline string) (*core.AgentProfileRegistry, string, error) {
	if err := core.ValidateRunID(owner); err != nil {
		return nil, "", fmt.Errorf("release reservation owner: %w", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Journal != nil && !m.restored {
		return nil, "", fmt.Errorf("release journal must be restored before reserving a candidate")
	}
	current, err := m.baselineRevisionLocked(layer.ProfileID, scope)
	if err != nil {
		return nil, "", err
	}
	if expectedBaseline != "" && current != expectedBaseline {
		return nil, "", fmt.Errorf("%w: expected %s, found %s", ErrReleaseBaselineDrift, expectedBaseline, current)
	}
	candidate, revision, err := m.prepareCandidateLocked(scope, layer)
	if err != nil {
		return nil, "", err
	}
	if existing, ok := m.reserved[owner]; ok {
		if existing.ProfileID != layer.ProfileID || !existing.Scope.Equal(scope) ||
			existing.BaselineRevision != current || existing.CandidateRevision != revision {
			return nil, "", fmt.Errorf("release reservation %s conflicts with its existing artifact", owner)
		}
	} else {
		m.reserved[owner] = releaseReservation{
			Owner: owner, ProfileID: layer.ProfileID, Scope: scope,
			BaselineRevision: current, CandidateRevision: revision,
		}
	}
	return candidate, revision, nil
}

func (m *ReleaseManager) prepareCandidateLocked(scope core.ScopePath, layer core.AgentProfileLayer) (*core.AgentProfileRegistry, string, error) {
	if scope.Depth() == 0 {
		return nil, "", fmt.Errorf("candidate requires a scope")
	}
	if layer.ProfileID == "" {
		return nil, "", fmt.Errorf("candidate requires a profile id")
	}
	layer.Scope = scope
	revision, err := core.ProfileLayerRevision(layer)
	if err != nil {
		return nil, "", err
	}
	candidate := m.Profiles.Clone()
	if _, err := candidate.Mount(layer); err != nil {
		return nil, "", fmt.Errorf("prepare candidate %s: %w", layer.ProfileID, err)
	}
	return candidate, revision, nil
}

// BaselineRevision fingerprints Release history whose scopes overlap the
// requested scope. Sibling scopes remain independent while ancestor and
// descendant changes invalidate an evaluated candidate.
func (m *ReleaseManager) BaselineRevision(profileID string, scope core.ScopePath) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Journal != nil && !m.restored {
		return "", fmt.Errorf("release journal must be restored before reading a baseline")
	}
	return m.baselineRevisionLocked(profileID, scope)
}

func (m *ReleaseManager) verifyBaseline(profileID string, scope core.ScopePath, expected string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	current, err := m.baselineRevisionLocked(profileID, scope)
	if err != nil {
		return err
	}
	if current != expected {
		return fmt.Errorf("%w: expected %s, found %s", ErrReleaseBaselineDrift, expected, current)
	}
	return nil
}

func (m *ReleaseManager) baselineRevisionLocked(profileID string, scope core.ScopePath) (string, error) {
	if err := core.ValidateNamespacedID(profileID); err != nil {
		return "", fmt.Errorf("baseline profile id: %w", err)
	}
	if scope.Depth() == 0 {
		return "", fmt.Errorf("release baseline requires a scope")
	}
	type baselineRelease struct {
		Version     int    `json:"version"`
		Scope       string `json:"scope"`
		Revision    string `json:"revision"`
		RolledBack  bool   `json:"rolled_back"`
		OperationID string `json:"operation_id,omitempty"`
	}
	items := []baselineRelease{}
	for _, release := range m.releases[profileID] {
		if !scopesOverlap(scope, release.Scope) {
			continue
		}
		items = append(items, baselineRelease{
			Version: release.Version, Scope: release.Scope.String(), Revision: release.Revision,
			RolledBack: release.RolledBack, OperationID: release.OperationID,
		})
	}
	encoded, err := json.Marshal(items)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

// Publish mounts one profile layer at one scope as a new version. Extends
// chains are resolved live, so a publish may extend a profile defined
// elsewhere; the layer itself is what gets rolled back.
func (m *ReleaseManager) Publish(ctx context.Context, scope core.ScopePath, layer core.AgentProfileLayer) (ReleaseInfo, error) {
	return m.publish(ctx, scope, layer, "", "")
}

// PublishAtBaseline publishes only if overlapping Release history still
// matches the baseline captured for an evaluation.
func (m *ReleaseManager) PublishAtBaseline(ctx context.Context, scope core.ScopePath, layer core.AgentProfileLayer, baselineRevision string) (ReleaseInfo, error) {
	if err := validateReleaseBaselineRevision(baselineRevision); err != nil {
		return ReleaseInfo{}, err
	}
	return m.publish(ctx, scope, layer, "", baselineRevision)
}

// PublishWithOperation publishes a layer exactly once for a durable operation
// id. Replaying the same operation with the same artifact returns the original
// release; reusing it for a different artifact fails closed.
func (m *ReleaseManager) PublishWithOperation(ctx context.Context, scope core.ScopePath, layer core.AgentProfileLayer, operationID string) (ReleaseInfo, error) {
	if err := core.ValidateRunID(operationID); err != nil {
		return ReleaseInfo{}, fmt.Errorf("publish operation id: %w", err)
	}
	return m.publish(ctx, scope, layer, operationID, "")
}

// PublishWithOperationAtBaseline combines durable operation idempotency with
// an evaluated Release baseline. It is the safe promotion primitive used by
// staged rollouts.
func (m *ReleaseManager) PublishWithOperationAtBaseline(ctx context.Context, scope core.ScopePath, layer core.AgentProfileLayer, operationID, baselineRevision string) (ReleaseInfo, error) {
	if err := core.ValidateRunID(operationID); err != nil {
		return ReleaseInfo{}, fmt.Errorf("publish operation id: %w", err)
	}
	if err := validateReleaseBaselineRevision(baselineRevision); err != nil {
		return ReleaseInfo{}, err
	}
	return m.publish(ctx, scope, layer, operationID, baselineRevision)
}

func validateReleaseBaselineRevision(revision string) error {
	if len(revision) != sha256.Size*2 {
		return fmt.Errorf("release baseline revision must be a SHA-256 digest")
	}
	if _, err := hex.DecodeString(revision); err != nil {
		return fmt.Errorf("release baseline revision: %w", err)
	}
	return nil
}

func (m *ReleaseManager) publish(ctx context.Context, scope core.ScopePath, layer core.AgentProfileLayer, operationID, baselineRevision string) (ReleaseInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if layer.ProfileID == "" {
		return ReleaseInfo{}, fmt.Errorf("publish requires a profile id")
	}
	if m.Journal != nil && !m.restored {
		return ReleaseInfo{}, fmt.Errorf("release journal must be restored before publish")
	}
	layer.Scope = scope
	if scope.Depth() == 0 {
		return ReleaseInfo{}, fmt.Errorf("publish requires a scope")
	}
	revision, err := core.ProfileLayerRevision(layer)
	if err != nil {
		return ReleaseInfo{}, err
	}
	if operationID != "" {
		if existing, found := m.releaseByOperationLocked(operationID); found {
			return matchReleaseOperation(existing, layer.ProfileID, scope, revision)
		}
		if m.Journal != nil {
			existing, found, err := m.Journal.FindReleaseByOperation(ctx, operationID)
			if err != nil {
				return ReleaseInfo{}, fmt.Errorf("find release operation %s: %w", operationID, err)
			}
			if found {
				return matchReleaseOperation(existing, layer.ProfileID, scope, revision)
			}
		}
	}
	currentBaseline, err := m.baselineRevisionLocked(layer.ProfileID, scope)
	if err != nil {
		return ReleaseInfo{}, err
	}
	if baselineRevision != "" && currentBaseline != baselineRevision {
		return ReleaseInfo{}, fmt.Errorf("%w: expected %s, found %s", ErrReleaseBaselineDrift, baselineRevision, currentBaseline)
	}
	for owner, reservation := range m.reserved {
		if reservation.ProfileID != layer.ProfileID || !scopesOverlap(reservation.Scope, scope) {
			continue
		}
		if owner != operationID || baselineRevision == "" || baselineRevision != reservation.BaselineRevision {
			return ReleaseInfo{}, fmt.Errorf("%w: canary %s", ErrReleaseReserved, owner)
		}
	}
	version := m.next[layer.ProfileID] + 1
	release := &ProfileRelease{
		ProfileID: layer.ProfileID, Version: version, OperationID: operationID, Scope: scope,
		CreatedAt: time.Now().UTC(), Layer: core.CloneAgentProfileLayer(layer), Revision: revision,
	}
	unmount, err := m.Profiles.Mount(layer)
	if err != nil {
		return ReleaseInfo{}, fmt.Errorf("publish %s v%d: %w", layer.ProfileID, version, err)
	}
	release.unmounts = append(release.unmounts, unmount)
	m.next[layer.ProfileID] = version
	m.releases[layer.ProfileID] = append(m.releases[layer.ProfileID], release)
	if m.Journal != nil {
		if err := m.Journal.RecordRelease(ctx, release.info()); err != nil {
			unmount()
			m.releases[layer.ProfileID] = m.releases[layer.ProfileID][:len(m.releases[layer.ProfileID])-1]
			m.next[layer.ProfileID] = version - 1
			return ReleaseInfo{}, fmt.Errorf("record release %s v%d: %w", layer.ProfileID, version, err)
		}
		m.syncedRevision = -1
	}
	return release.info(), nil
}

func (m *ReleaseManager) releaseReservation(profileID, owner string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if reservation, ok := m.reserved[owner]; ok && reservation.ProfileID == profileID {
		delete(m.reserved, owner)
	}
}

func scopesOverlap(left, right core.ScopePath) bool {
	return left.IsAncestorOf(right) || right.IsAncestorOf(left)
}

func (m *ReleaseManager) releaseByOperationLocked(operationID string) (ReleaseInfo, bool) {
	for _, releases := range m.releases {
		for _, release := range releases {
			if release.OperationID == operationID {
				return release.info(), true
			}
		}
	}
	return ReleaseInfo{}, false
}

func matchReleaseOperation(existing ReleaseInfo, profileID string, scope core.ScopePath, revision string) (ReleaseInfo, error) {
	if existing.ProfileID != profileID || !existing.Scope.Equal(scope) || existing.Revision != revision {
		return ReleaseInfo{}, fmt.Errorf("release operation %s was already used for a different artifact", existing.OperationID)
	}
	if existing.RolledBack {
		return ReleaseInfo{}, fmt.Errorf("release operation %s refers to a rolled-back release", existing.OperationID)
	}
	return existing, nil
}

// Rollback unmounts every release of the profile newer than toVersion, most
// recent first. toVersion 0 rolls the profile back to its pre-release state.
// Rolling back an already-rolled-back release is a no-op; the target version
// itself stays live.
func (m *ReleaseManager) Rollback(ctx context.Context, profileID string, toVersion int) ([]ReleaseInfo, error) {
	m.mu.Lock()
	if m.Journal != nil && !m.restored {
		m.mu.Unlock()
		return nil, fmt.Errorf("release journal must be restored before rollback")
	}
	for owner, reservation := range m.reserved {
		if reservation.ProfileID == profileID {
			m.mu.Unlock()
			return nil, fmt.Errorf("%w: canary %s", ErrReleaseReserved, owner)
		}
	}
	releases := m.releases[profileID]
	if len(releases) == 0 {
		m.mu.Unlock()
		if m.Journal != nil {
			return nil, fmt.Errorf("profile %q has no releases mounted by this process", profileID)
		}
		return nil, fmt.Errorf("profile %q has no releases", profileID)
	}
	if toVersion < 0 || toVersion > m.next[profileID] {
		m.mu.Unlock()
		return nil, fmt.Errorf("profile %q has no version %d", profileID, toVersion)
	}
	targets := []*ProfileRelease{}
	for i := len(releases) - 1; i >= 0; i-- {
		release := releases[i]
		if release.Version <= toVersion || release.RolledBack {
			continue
		}
		targets = append(targets, release)
	}
	if m.Journal != nil && len(targets) > 0 {
		versions := make([]int, 0, len(targets))
		for _, release := range targets {
			versions = append(versions, release.Version)
		}
		if err := m.Journal.MarkRolledBack(ctx, profileID, versions); err != nil {
			m.mu.Unlock()
			return nil, fmt.Errorf("record rollback of %s: %w", profileID, err)
		}
		m.syncedRevision = -1
	}
	rolledBack := []ReleaseInfo{}
	for _, release := range targets {
		for j := len(release.unmounts) - 1; j >= 0; j-- {
			release.unmounts[j]()
		}
		release.unmounts = nil
		release.RolledBack = true
		rolledBack = append(rolledBack, release.info())
	}
	m.mu.Unlock()
	return rolledBack, nil
}

// Sync applies durable Release changes written by another manager instance.
// It validates the complete journal view before mutating the local registry,
// mounts all new live artifacts before committing local history, and then
// applies durable rollbacks through the original unmount handles.
func (m *ReleaseManager) Sync(ctx context.Context) error {
	if m == nil || m.Profiles == nil {
		return fmt.Errorf("release manager is not configured")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Journal == nil {
		m.restored = true
		m.syncedRevision = 0
		return nil
	}
	if !m.restored {
		return fmt.Errorf("release journal must be restored before sync")
	}
	startRevision, err := m.Journal.ControlRevision(ctx)
	if err != nil {
		return fmt.Errorf("read control revision: %w", err)
	}
	if startRevision == m.syncedRevision {
		return nil
	}
	profileIDs, err := m.Journal.ListReleaseProfiles(ctx)
	if err != nil {
		return fmt.Errorf("list release profiles: %w", err)
	}
	sort.Strings(profileIDs)
	durable := map[string]map[int]ReleaseInfo{}
	operationOwners := map[string]string{}
	for _, profileID := range profileIDs {
		infos, err := m.Journal.LoadReleases(ctx, profileID)
		if err != nil {
			return fmt.Errorf("load releases for %s: %w", profileID, err)
		}
		sort.Slice(infos, func(i, j int) bool { return infos[i].Version < infos[j].Version })
		versions := map[int]ReleaseInfo{}
		for _, info := range infos {
			if _, err := releaseFromInfo(info, profileID); err != nil {
				return err
			}
			if _, exists := versions[info.Version]; exists {
				return fmt.Errorf("release history for %s repeats version %d", profileID, info.Version)
			}
			versions[info.Version] = info
			if info.OperationID != "" {
				if prior, exists := operationOwners[info.OperationID]; exists {
					return fmt.Errorf("release operation %s appears in both %s and %s", info.OperationID, prior, profileID)
				}
				operationOwners[info.OperationID] = profileID
			}
		}
		durable[profileID] = versions
	}
	for profileID, locals := range m.releases {
		versions, exists := durable[profileID]
		if !exists {
			return fmt.Errorf("durable release history for %s disappeared", profileID)
		}
		for _, local := range locals {
			info, exists := versions[local.Version]
			if !exists {
				return fmt.Errorf("durable release %s v%d disappeared", profileID, local.Version)
			}
			if err := matchDurableRelease(local, info); err != nil {
				return err
			}
		}
	}

	type plannedRelease struct {
		profileID string
		release   *ProfileRelease
	}
	newReleases := []plannedRelease{}
	rollbacks := []*ProfileRelease{}
	for _, profileID := range profileIDs {
		locals := map[int]*ProfileRelease{}
		for _, release := range m.releases[profileID] {
			locals[release.Version] = release
		}
		versions := durable[profileID]
		ordered := make([]int, 0, len(versions))
		for version := range versions {
			ordered = append(ordered, version)
		}
		sort.Ints(ordered)
		for _, version := range ordered {
			info := versions[version]
			if local := locals[version]; local != nil {
				if !local.RolledBack && info.RolledBack {
					if err := m.validateExternalReleaseChange(local.ProfileID, local.Scope, "", ""); err != nil {
						return err
					}
					rollbacks = append(rollbacks, local)
				}
				continue
			}
			release, err := releaseFromInfo(info, profileID)
			if err != nil {
				return err
			}
			if !release.RolledBack {
				if err := m.validateExternalReleaseChange(
					release.ProfileID, release.Scope, release.OperationID, release.Revision,
				); err != nil {
					return err
				}
			}
			newReleases = append(newReleases, plannedRelease{profileID: profileID, release: release})
		}
	}

	mounted := []*ProfileRelease{}
	for _, planned := range newReleases {
		if planned.release.RolledBack {
			continue
		}
		unmount, err := m.Profiles.Mount(planned.release.Layer)
		if err != nil {
			for index := len(mounted) - 1; index >= 0; index-- {
				mounted[index].unmounts[0]()
				mounted[index].unmounts = nil
			}
			return fmt.Errorf("sync release %s v%d: %w", planned.profileID, planned.release.Version, err)
		}
		planned.release.unmounts = []func(){unmount}
		mounted = append(mounted, planned.release)
	}
	for _, release := range rollbacks {
		for index := len(release.unmounts) - 1; index >= 0; index-- {
			release.unmounts[index]()
		}
		release.unmounts = nil
		release.RolledBack = true
	}
	for _, planned := range newReleases {
		m.releases[planned.profileID] = append(m.releases[planned.profileID], planned.release)
		if planned.release.Version > m.next[planned.profileID] {
			m.next[planned.profileID] = planned.release.Version
		}
	}
	for profileID := range m.releases {
		sort.Slice(m.releases[profileID], func(i, j int) bool {
			return m.releases[profileID][i].Version < m.releases[profileID][j].Version
		})
	}
	endRevision, err := m.Journal.ControlRevision(ctx)
	if err != nil {
		m.syncedRevision = -1
		return fmt.Errorf("re-read control revision: %w", err)
	}
	if endRevision != startRevision {
		m.syncedRevision = -1
		return fmt.Errorf("control plane changed during release sync")
	}
	m.syncedRevision = endRevision
	return nil
}

func (m *ReleaseManager) validateExternalReleaseChange(profileID string, scope core.ScopePath, operationID, revision string) error {
	for owner, reservation := range m.reserved {
		if reservation.ProfileID != profileID || !scopesOverlap(reservation.Scope, scope) {
			continue
		}
		if operationID == owner && reservation.Scope.Equal(scope) && reservation.CandidateRevision == revision {
			current, err := m.baselineRevisionLocked(profileID, scope)
			if err != nil {
				return err
			}
			if current == reservation.BaselineRevision {
				continue
			}
		}
		return fmt.Errorf("%w: canary %s", ErrReleaseReserved, owner)
	}
	return nil
}

func releaseFromInfo(info ReleaseInfo, profileID string) (*ProfileRelease, error) {
	if info.Version < 1 || info.ProfileID != profileID || info.Scope.Depth() == 0 {
		return nil, fmt.Errorf("release history for %s contains an invalid record", profileID)
	}
	if info.OperationID != "" {
		if err := core.ValidateRunID(info.OperationID); err != nil {
			return nil, fmt.Errorf("release %s v%d operation id: %w", profileID, info.Version, err)
		}
	}
	release := &ProfileRelease{
		ProfileID: info.ProfileID, Version: info.Version, OperationID: info.OperationID, Scope: info.Scope,
		CreatedAt: info.CreatedAt, RolledBack: info.RolledBack, Revision: info.Revision,
	}
	if info.Layer != nil {
		release.Layer = core.CloneAgentProfileLayer(*info.Layer)
		release.Layer.Scope = info.Scope
	}
	if !info.RolledBack {
		if info.Layer == nil || info.Revision == "" {
			return nil, fmt.Errorf("live release %s v%d has no restorable artifact", profileID, info.Version)
		}
		calculated, err := core.ProfileLayerRevision(release.Layer)
		if err != nil || calculated != info.Revision {
			return nil, fmt.Errorf("release %s v%d artifact revision mismatch", profileID, info.Version)
		}
	}
	return release, nil
}

func matchDurableRelease(local *ProfileRelease, info ReleaseInfo) error {
	if local.ProfileID != info.ProfileID || local.Version != info.Version || local.OperationID != info.OperationID ||
		!local.Scope.Equal(info.Scope) || local.Revision != info.Revision {
		return fmt.Errorf("durable release %s v%d changed immutable fields", local.ProfileID, local.Version)
	}
	if local.RolledBack && !info.RolledBack {
		return fmt.Errorf("durable release %s v%d was resurrected", local.ProfileID, local.Version)
	}
	return nil
}

// Restore rebuilds live mounts and version counters from durable artifacts.
// It is idempotent after the first successful restore.
func (m *ReleaseManager) Restore(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.restored || m.Journal == nil {
		m.restored = true
		return nil
	}
	profileIDs, err := m.Journal.ListReleaseProfiles(ctx)
	if err != nil {
		return fmt.Errorf("list release profiles: %w", err)
	}
	next := map[string]int{}
	releases := map[string][]*ProfileRelease{}
	mounted := []func(){}
	rollbackMounted := func() {
		for index := len(mounted) - 1; index >= 0; index-- {
			mounted[index]()
		}
	}
	for _, profileID := range profileIDs {
		infos, err := m.Journal.LoadReleases(ctx, profileID)
		if err != nil {
			rollbackMounted()
			return fmt.Errorf("load releases for %s: %w", profileID, err)
		}
		sort.Slice(infos, func(i, j int) bool { return infos[i].Version < infos[j].Version })
		for _, info := range infos {
			if info.Version < 1 || info.ProfileID != profileID || info.Scope.Depth() == 0 {
				rollbackMounted()
				return fmt.Errorf("release history for %s contains an invalid record", profileID)
			}
			release := &ProfileRelease{
				ProfileID: info.ProfileID, Version: info.Version, OperationID: info.OperationID, Scope: info.Scope,
				CreatedAt: info.CreatedAt, RolledBack: info.RolledBack, Revision: info.Revision,
			}
			if info.OperationID != "" {
				if err := core.ValidateRunID(info.OperationID); err != nil {
					rollbackMounted()
					return fmt.Errorf("release %s v%d operation id: %w", profileID, info.Version, err)
				}
				for _, existing := range releases {
					for _, prior := range existing {
						if prior.OperationID == info.OperationID {
							rollbackMounted()
							return fmt.Errorf("release operation %s appears more than once", info.OperationID)
						}
					}
				}
			}
			if info.Layer != nil {
				release.Layer = core.CloneAgentProfileLayer(*info.Layer)
				release.Layer.Scope = info.Scope
			}
			if !info.RolledBack {
				if info.Layer == nil || info.Revision == "" {
					rollbackMounted()
					return fmt.Errorf("live release %s v%d has no restorable artifact", profileID, info.Version)
				}
				revision, err := core.ProfileLayerRevision(release.Layer)
				if err != nil || revision != info.Revision {
					rollbackMounted()
					return fmt.Errorf("release %s v%d artifact revision mismatch", profileID, info.Version)
				}
				unmount, err := m.Profiles.Mount(release.Layer)
				if err != nil {
					rollbackMounted()
					return fmt.Errorf("restore release %s v%d: %w", profileID, info.Version, err)
				}
				release.unmounts = []func(){unmount}
				mounted = append(mounted, unmount)
			}
			releases[profileID] = append(releases[profileID], release)
			if info.Version > next[profileID] {
				next[profileID] = info.Version
			}
		}
	}
	m.next, m.releases, m.restored, m.syncedRevision = next, releases, true, -1
	return nil
}

// History lists the releases of one profile, oldest first. With a Journal
// the durable record is the source of truth, including releases from prior
// process lifetimes; without one, the in-process history.
func (m *ReleaseManager) History(ctx context.Context, profileID string) []ReleaseInfo {
	if m.Journal != nil {
		if releases, err := m.Journal.LoadReleases(ctx, profileID); err == nil {
			return releases
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]ReleaseInfo, 0, len(m.releases[profileID]))
	for _, release := range m.releases[profileID] {
		out = append(out, release.info())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Version < out[j].Version })
	return out
}

func (r *ProfileRelease) info() ReleaseInfo {
	var layer *core.AgentProfileLayer
	if r.Layer.ProfileID != "" {
		copyOf := core.CloneAgentProfileLayer(r.Layer)
		layer = &copyOf
	}
	return ReleaseInfo{
		ProfileID: r.ProfileID, Version: r.Version, OperationID: r.OperationID, Scope: r.Scope,
		CreatedAt: r.CreatedAt, RolledBack: r.RolledBack, Layer: layer, Revision: r.Revision,
	}
}
