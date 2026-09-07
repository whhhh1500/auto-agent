package core

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
)

// PromptSection gives layered persona fragments a stable rendering position.
type PromptSection string

const (
	PromptIdentity     PromptSection = "identity"
	PromptTraits       PromptSection = "traits"
	PromptValues       PromptSection = "values"
	PromptGoals        PromptSection = "goals"
	PromptStyle        PromptSection = "style"
	PromptBoundaries   PromptSection = "boundaries"
	PromptInstructions PromptSection = "instructions"
)

var promptSectionOrder = map[PromptSection]int{
	PromptIdentity: 10, PromptTraits: 20, PromptValues: 30, PromptGoals: 40,
	PromptStyle: 50, PromptBoundaries: 60, PromptInstructions: 70,
}

// PromptFragment is one independently versionable part of an agent persona.
type PromptFragment struct {
	ID       string        `json:"id"`
	Section  PromptSection `json:"section"`
	Content  string        `json:"content"`
	Priority int           `json:"priority,omitempty"`
}

// ModelSelection identifies a provider-neutral model choice.
type ModelSelection struct {
	Provider string `json:"provider"`
	Model    string `json:"model"`
}

const (
	MaxProfileCapabilities  = 256
	MaxProfileFragments     = 512
	MaxPromptFragmentBytes  = 64 << 10
	MaxSystemPromptBytes    = 1 << 20
	MaxProfileMetadataItems = 256
)

// AgentProfileLayer contributes to one profile at one ownership scope. Scalar
// fields replace inherited values; capabilities and prompt fragments merge by ID.
// The JSON tags define the publish-API wire format.
type AgentProfileLayer struct {
	Scope              ScopePath         `json:"-"`
	ProfileID          string            `json:"profile_id"`
	Extends            string            `json:"extends,omitempty"`
	Name               *string           `json:"name,omitempty"`
	Description        *string           `json:"description,omitempty"`
	Model              *ModelSelection   `json:"model,omitempty"`
	MaxSteps           *int              `json:"max_steps,omitempty"`
	MaxToolCalls       *int              `json:"max_tool_calls,omitempty"`
	AddCapabilities    []string          `json:"add_capabilities,omitempty"`
	RemoveCapabilities []string          `json:"remove_capabilities,omitempty"`
	PutFragments       []PromptFragment  `json:"put_fragments,omitempty"`
	RemoveFragments    []string          `json:"remove_fragments,omitempty"`
	Metadata           map[string]string `json:"metadata,omitempty"`
}

type storedProfileLayer struct {
	AgentProfileLayer
	order uint64
}

// AgentProfileRegistry stores deployer, tenant and user profile contributions.
type AgentProfileRegistry struct {
	mu     sync.RWMutex
	layers map[string][]storedProfileLayer
	// flat indexes every layer by mount order for catalog listing, since the
	// layers map is keyed by (scope, profileID) pairs.
	flat        []storedProfileLayer
	next        uint64
	maxBindings int
}

// NewAgentProfileRegistry creates an empty profile registry.
func NewAgentProfileRegistry() *AgentProfileRegistry {
	return &AgentProfileRegistry{layers: map[string][]storedProfileLayer{}}
}

// Clone returns an independent registry with identical mount ordering. It is
// used for invisible candidate composition: mutations on either registry do
// not affect the other.
func (r *AgentProfileRegistry) Clone() *AgentProfileRegistry {
	if r == nil {
		return NewAgentProfileRegistry()
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.cloneLocked()
}

func (r *AgentProfileRegistry) cloneLocked() *AgentProfileRegistry {
	out := &AgentProfileRegistry{layers: map[string][]storedProfileLayer{}, next: r.next, maxBindings: r.maxBindings}
	for key, layers := range r.layers {
		copies := make([]storedProfileLayer, len(layers))
		for index, layer := range layers {
			copies[index] = storedProfileLayer{AgentProfileLayer: cloneProfileLayer(layer.AgentProfileLayer), order: layer.order}
		}
		out.layers[key] = copies
	}
	out.flat = make([]storedProfileLayer, len(r.flat))
	for index, layer := range r.flat {
		out.flat[index] = storedProfileLayer{AgentProfileLayer: cloneProfileLayer(layer.AgentProfileLayer), order: layer.order}
	}
	return out
}

// ProfileLayerRevision fingerprints one complete scoped release artifact.
func ProfileLayerRevision(layer AgentProfileLayer) (string, error) {
	if layer.Scope.Depth() == 0 {
		return "", fmt.Errorf("profile layer revision requires a scope")
	}
	encoded, err := canonicalProfileLayer(layer)
	if err != nil {
		return "", fmt.Errorf("encode profile layer revision: %w", err)
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

// CloneAgentProfileLayer returns a fully detached public layer value.
func CloneAgentProfileLayer(layer AgentProfileLayer) AgentProfileLayer {
	return cloneProfileLayer(layer)
}

// Bind validates and records one profile contribution.
func (r *AgentProfileRegistry) Bind(layer AgentProfileLayer) error {
	_, err := r.Mount(layer)
	return err
}

// Mount records one profile layer and returns an idempotent unmount function.
func (r *AgentProfileRegistry) Mount(layer AgentProfileLayer) (func(), error) {
	if err := validateProfileLayer(layer); err != nil {
		return nil, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.flat) >= registryCap(r.maxBindings) {
		return nil, fmt.Errorf("registry bindings exceed maximum of %d", registryCap(r.maxBindings))
	}
	r.next++
	order := r.next
	key := profileLayerKey(layer.Scope, layer.ProfileID)
	stored := storedProfileLayer{AgentProfileLayer: cloneProfileLayer(layer), order: order}
	r.layers[key] = append(r.layers[key], stored)
	r.flat = append(r.flat, stored)
	var once sync.Once
	return func() {
		once.Do(func() {
			r.mu.Lock()
			defer r.mu.Unlock()
			current := r.layers[key]
			for i, item := range current {
				if item.order == order {
					r.layers[key] = append(current[:i], current[i+1:]...)
					break
				}
			}
			for i, item := range r.flat {
				if item.order == order {
					r.flat = append(r.flat[:i], r.flat[i+1:]...)
					break
				}
			}
		})
	}, nil
}

// ReplaceExact atomically swaps one uniquely matching mounted layer in place. The
// replacement keeps its mount order, so concurrent Resolve calls see either
// the complete old projection or the complete new projection. Existing
// unmount closures remain valid and remove the replacement at that order.
func (r *AgentProfileRegistry) ReplaceExact(oldLayer, nextLayer AgentProfileLayer) error {
	if err := validateProfileLayer(oldLayer); err != nil {
		return fmt.Errorf("replace old profile layer: %w", err)
	}
	if err := validateProfileLayer(nextLayer); err != nil {
		return fmt.Errorf("replace new profile layer: %w", err)
	}
	if oldLayer.ProfileID != nextLayer.ProfileID || !oldLayer.Scope.Equal(nextLayer.Scope) {
		return fmt.Errorf("profile replacement requires the same profile id and scope")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	key := profileLayerKey(oldLayer.Scope, oldLayer.ProfileID)
	match := -1
	for index, stored := range r.layers[key] {
		if profileLayerEqual(stored.AgentProfileLayer, oldLayer) {
			if match >= 0 {
				return fmt.Errorf("profile replacement has multiple matching layers")
			}
			match = index
		}
	}
	if match < 0 {
		return fmt.Errorf("profile replacement layer was not found")
	}
	order := r.layers[key][match].order
	flatMatch := -1
	for index, stored := range r.flat {
		if stored.order == order {
			if flatMatch >= 0 || !profileLayerEqual(stored.AgentProfileLayer, oldLayer) {
				return fmt.Errorf("profile registry projection is inconsistent")
			}
			flatMatch = index
		}
	}
	if flatMatch < 0 {
		return fmt.Errorf("profile registry projection is inconsistent")
	}

	candidate := r.cloneLocked()
	candidateStored := storedProfileLayer{AgentProfileLayer: cloneProfileLayer(nextLayer), order: order}
	candidate.layers[key][match] = candidateStored
	candidate.flat[flatMatch] = candidateStored
	if _, err := candidate.Resolve(Principal{Scope: nextLayer.Scope}, nextLayer.Scope, nextLayer.ProfileID); err != nil {
		return fmt.Errorf("replace profile layer: %w", err)
	}
	r.layers[key][match] = storedProfileLayer{AgentProfileLayer: cloneProfileLayer(nextLayer), order: order}
	r.flat[flatMatch] = storedProfileLayer{AgentProfileLayer: cloneProfileLayer(nextLayer), order: order}
	return nil
}

// profileLayerEqual compares the typed layer contract. Empty and nil
// collections are equivalent because they produce the same profile projection.
func profileLayerEqual(left, right AgentProfileLayer) bool {
	return left.Scope.Equal(right.Scope) && left.ProfileID == right.ProfileID && left.Extends == right.Extends &&
		profileOptionalEqual(left.Name, right.Name) && profileOptionalEqual(left.Description, right.Description) && profileOptionalEqual(left.Model, right.Model) && profileOptionalEqual(left.MaxSteps, right.MaxSteps) && profileOptionalEqual(left.MaxToolCalls, right.MaxToolCalls) &&
		slices.Equal(left.AddCapabilities, right.AddCapabilities) && slices.Equal(left.RemoveCapabilities, right.RemoveCapabilities) && slices.Equal(left.PutFragments, right.PutFragments) &&
		slices.Equal(left.RemoveFragments, right.RemoveFragments) && maps.Equal(left.Metadata, right.Metadata)
}

func profileOptionalEqual[T comparable](left, right *T) bool {
	return left == nil && right == nil || left != nil && right != nil && *left == *right
}

func canonicalProfileLayer(layer AgentProfileLayer) ([]byte, error) {
	copyOf := cloneProfileLayer(layer)
	return json.Marshal(struct {
		Scope ScopePath         `json:"scope"`
		Layer AgentProfileLayer `json:"layer"`
	}{Scope: copyOf.Scope, Layer: copyOf})
}

func validateProfileLayer(layer AgentProfileLayer) error {
	if layer.Scope.Depth() == 0 {
		return fmt.Errorf("agent profile %q has an empty scope", layer.ProfileID)
	}
	if err := ValidateProfileID(layer.ProfileID); err != nil {
		return err
	}
	if layer.Extends == layer.ProfileID {
		return fmt.Errorf("agent profile %q cannot extend itself", layer.ProfileID)
	}
	if len(layer.AddCapabilities)+len(layer.RemoveCapabilities) > MaxProfileCapabilities {
		return fmt.Errorf("agent profile %q layer exceeds %d capability changes", layer.ProfileID, MaxProfileCapabilities)
	}
	if len(layer.PutFragments)+len(layer.RemoveFragments) > MaxProfileFragments {
		return fmt.Errorf("agent profile %q layer exceeds %d fragment changes", layer.ProfileID, MaxProfileFragments)
	}
	if len(layer.Metadata) > MaxProfileMetadataItems {
		return fmt.Errorf("agent profile %q layer exceeds %d metadata items", layer.ProfileID, MaxProfileMetadataItems)
	}
	for _, id := range append(append([]string(nil), layer.AddCapabilities...), layer.RemoveCapabilities...) {
		if err := validateCapabilityID(id); err != nil {
			return fmt.Errorf("agent profile %q: %w", layer.ProfileID, err)
		}
	}
	for _, fragment := range layer.PutFragments {
		if fragment.ID == "" || len(fragment.ID) > 128 || containsControl(fragment.ID) || strings.TrimSpace(fragment.Content) == "" {
			return fmt.Errorf("agent profile %q has an invalid prompt fragment", layer.ProfileID)
		}
		if len(fragment.Content) > MaxPromptFragmentBytes {
			return fmt.Errorf("agent profile %q fragment %q exceeds %d bytes", layer.ProfileID, fragment.ID, MaxPromptFragmentBytes)
		}
		if _, ok := promptSectionOrder[fragment.Section]; !ok {
			return fmt.Errorf("agent profile %q fragment %q has unknown section %q", layer.ProfileID, fragment.ID, fragment.Section)
		}
	}
	for key, value := range layer.Metadata {
		if key == "" || len(key) > 128 || containsControl(key) || len(value) > MaxPromptFragmentBytes || containsControl(value) {
			return fmt.Errorf("agent profile %q has invalid metadata", layer.ProfileID)
		}
	}
	return nil
}

type profileState struct {
	id           string
	name         string
	description  string
	model        ModelSelection
	maxSteps     int
	maxToolCalls int
	capabilities map[string]bool
	fragments    map[string]ResolvedPromptFragment
	metadata     map[string]string
}

// ResolvedPromptFragment records the scope that supplied the effective fragment.
type ResolvedPromptFragment struct {
	PromptFragment
	Source ScopePath `json:"source"`
}

// AgentProfileSnapshot fixes identity, persona, model and capability selection for a run.
type AgentProfileSnapshot struct {
	ID           string                   `json:"id"`
	ProfileID    string                   `json:"profile_id"`
	CreatedAt    time.Time                `json:"created_at"`
	Scope        ScopePath                `json:"scope"`
	Name         string                   `json:"name"`
	Description  string                   `json:"description,omitempty"`
	Model        ModelSelection           `json:"model"`
	MaxSteps     int                      `json:"max_steps"`
	MaxToolCalls int                      `json:"max_tool_calls,omitempty"`
	Capabilities []string                 `json:"capabilities"`
	Fragments    []ResolvedPromptFragment `json:"fragments"`
	Metadata     map[string]string        `json:"metadata,omitempty"`
}

// Resolve creates an immutable profile snapshot for one authenticated scope.
func (r *AgentProfileRegistry) Resolve(principal Principal, target ScopePath, profileID string) (*AgentProfileSnapshot, error) {
	if !principal.Scope.IsAncestorOf(target) {
		return nil, fmt.Errorf("principal scope %q does not own target scope %q", principal.Scope, target)
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	state, err := r.resolveLocked(target, profileID, map[string]bool{})
	if err != nil {
		return nil, err
	}

	capabilities := make([]string, 0, len(state.capabilities))
	for id, enabled := range state.capabilities {
		if enabled {
			capabilities = append(capabilities, id)
		}
	}
	sort.Strings(capabilities)

	fragments := make([]ResolvedPromptFragment, 0, len(state.fragments))
	for _, fragment := range state.fragments {
		fragments = append(fragments, fragment)
	}
	sort.Slice(fragments, func(i, j int) bool {
		left, right := fragments[i], fragments[j]
		if promptSectionOrder[left.Section] != promptSectionOrder[right.Section] {
			return promptSectionOrder[left.Section] < promptSectionOrder[right.Section]
		}
		if left.Priority != right.Priority {
			return left.Priority < right.Priority
		}
		return left.ID < right.ID
	})

	snapshot := &AgentProfileSnapshot{
		ProfileID: profileID, CreatedAt: time.Now().UTC(), Scope: target,
		Name: state.name, Description: state.description, Model: state.model,
		MaxSteps: state.maxSteps, MaxToolCalls: state.maxToolCalls, Capabilities: capabilities, Fragments: fragments,
		Metadata: state.metadata,
	}
	if len(snapshot.Capabilities) > MaxProfileCapabilities {
		return nil, fmt.Errorf("agent profile %q exceeds %d capabilities", profileID, MaxProfileCapabilities)
	}
	if len(snapshot.Fragments) > MaxProfileFragments {
		return nil, fmt.Errorf("agent profile %q exceeds %d prompt fragments", profileID, MaxProfileFragments)
	}
	if len(snapshot.SystemPrompt()) > MaxSystemPromptBytes {
		return nil, fmt.Errorf("agent profile %q system prompt exceeds %d bytes", profileID, MaxSystemPromptBytes)
	}
	digest, err := profileDigest(snapshot)
	if err != nil {
		return nil, err
	}
	snapshot.ID = digest
	return snapshot, nil
}

func (r *AgentProfileRegistry) resolveLocked(target ScopePath, profileID string, visiting map[string]bool) (*profileState, error) {
	if visiting[profileID] {
		return nil, fmt.Errorf("agent profile inheritance cycle at %q", profileID)
	}
	visiting[profileID] = true
	defer delete(visiting, profileID)

	layers := r.layersForLocked(target, profileID)
	if len(layers) == 0 {
		return nil, fmt.Errorf("agent profile %q was not found for scope %q", profileID, target)
	}

	extends := ""
	for _, layer := range layers {
		if layer.Extends == "" {
			continue
		}
		if extends != "" && extends != layer.Extends {
			return nil, fmt.Errorf("agent profile %q has conflicting parents %q and %q", profileID, extends, layer.Extends)
		}
		extends = layer.Extends
	}

	state := &profileState{
		id: profileID, maxSteps: 10, maxToolCalls: 16, capabilities: map[string]bool{},
		fragments: map[string]ResolvedPromptFragment{}, metadata: map[string]string{},
	}
	if extends != "" {
		parent, err := r.resolveLocked(target, extends, visiting)
		if err != nil {
			return nil, fmt.Errorf("resolve parent of %q: %w", profileID, err)
		}
		state = cloneProfileState(parent)
		state.id = profileID
	}

	for _, layer := range layers {
		if layer.Name != nil {
			state.name = *layer.Name
		}
		if layer.Description != nil {
			state.description = *layer.Description
		}
		if layer.Model != nil {
			state.model = *layer.Model
		}
		if layer.MaxSteps != nil {
			if *layer.MaxSteps <= 0 || *layer.MaxSteps > HardMaxSteps {
				return nil, fmt.Errorf("agent profile %q max steps must be between 1 and %d", profileID, HardMaxSteps)
			}
			state.maxSteps = *layer.MaxSteps
		}
		if layer.MaxToolCalls != nil {
			if *layer.MaxToolCalls <= 0 || *layer.MaxToolCalls > HardMaxToolCalls {
				return nil, fmt.Errorf("agent profile %q max tool calls must be between 1 and %d", profileID, HardMaxToolCalls)
			}
			state.maxToolCalls = *layer.MaxToolCalls
		}
		for _, id := range layer.AddCapabilities {
			state.capabilities[id] = true
		}
		for _, id := range layer.RemoveCapabilities {
			delete(state.capabilities, id)
		}
		for _, id := range layer.RemoveFragments {
			delete(state.fragments, id)
		}
		for _, fragment := range layer.PutFragments {
			state.fragments[fragment.ID] = ResolvedPromptFragment{PromptFragment: fragment, Source: layer.Scope}
		}
		for key, value := range layer.Metadata {
			state.metadata[key] = value
		}
	}
	if state.name == "" {
		state.name = profileID
	}
	if err := validateModelSelection(state.model); err != nil {
		return nil, fmt.Errorf("agent profile %q: %w", profileID, err)
	}
	return state, nil
}

func (r *AgentProfileRegistry) layersForLocked(target ScopePath, profileID string) []storedProfileLayer {
	out := []storedProfileLayer{}
	for _, prefix := range target.Prefixes() {
		withinScope := append([]storedProfileLayer(nil), r.layers[profileLayerKey(prefix, profileID)]...)
		sort.SliceStable(withinScope, func(i, j int) bool { return withinScope[i].order < withinScope[j].order })
		out = append(out, withinScope...)
	}
	return out
}

// ListProfiles returns the profile identifiers with at least one layer visible
// at the target scope. It backs catalog APIs; resolution may still fail for a
// listed profile (for example on a broken inheritance chain).
func (r *AgentProfileRegistry) ListProfiles(target ScopePath) ([]string, error) {
	if target.Depth() == 0 {
		return nil, fmt.Errorf("target scope is empty")
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	seen := map[string]bool{}
	out := []string{}
	for _, layer := range r.flat {
		if layer.Scope.IsAncestorOf(target) && !seen[layer.ProfileID] {
			seen[layer.ProfileID] = true
			out = append(out, layer.ProfileID)
		}
	}
	sort.Strings(out)
	return out, nil
}

// SystemPrompt renders stable sections for model prefix caching and auditing.
func (s *AgentProfileSnapshot) SystemPrompt() string {
	var out strings.Builder
	current := PromptSection("")
	for _, fragment := range s.Fragments {
		if fragment.Section != current {
			if out.Len() > 0 {
				out.WriteString("\n\n")
			}
			out.WriteString("## ")
			out.WriteString(string(fragment.Section))
			out.WriteString("\n")
			current = fragment.Section
		}
		out.WriteString("- ")
		out.WriteString(fragment.Content)
		out.WriteString("\n")
	}
	return strings.TrimSpace(out.String())
}

// FilterCapabilities applies the profile allow-list to a capability snapshot.
func (s *AgentProfileSnapshot) FilterCapabilities(snapshot *CapabilitySnapshot) (*CapabilitySnapshot, error) {
	return snapshot.Filter(s.Capabilities)
}

// Filter applies an explicit capability allow-list to the snapshot. Every
// listed capability must exist; unlisted capabilities are dropped.
func (s *CapabilitySnapshot) Filter(ids []string) (*CapabilitySnapshot, error) {
	allowed := map[string]bool{}
	for _, id := range ids {
		allowed[id] = true
	}
	entries := []SnapshotCapability{}
	providers := map[string]any{}
	for _, capability := range s.capabilities {
		if !allowed[capability.Manifest.ID] {
			continue
		}
		entries = append(entries, capability)
		providers[capability.Manifest.ID] = s.providers[capability.Manifest.ID]
	}
	for id := range allowed {
		if _, ok := providers[id]; !ok {
			return nil, fmt.Errorf("agent profile selects unavailable capability %q", id)
		}
	}
	return s.rebuild(entries, providers), nil
}

// FilterByPermissions drops capabilities whose required permissions exceed the
// effective set. Unlike Filter it never errors: a narrowed policy silently
// reduces the capability set, it does not invalidate profile composition.
func (s *CapabilitySnapshot) FilterByPermissions(permissions PermissionSet) *CapabilitySnapshot {
	entries := []SnapshotCapability{}
	providers := map[string]any{}
	for _, capability := range s.capabilities {
		if !permissions.Allows(capability.Manifest.RequiredPermissions) {
			continue
		}
		entries = append(entries, capability)
		providers[capability.Manifest.ID] = s.providers[capability.Manifest.ID]
	}
	return s.rebuild(entries, providers)
}

// FilterByPredicate applies a subtractive run-local capability filter. A
// filter panic fails closed for that capability.
func (s *CapabilitySnapshot) FilterByPredicate(filter CapabilityFilter) *CapabilitySnapshot {
	if filter == nil {
		return s
	}
	entries := []SnapshotCapability{}
	providers := map[string]any{}
	for _, capability := range s.capabilities {
		allowed := false
		func() {
			defer func() { _ = recover() }()
			allowed = filter.AllowCapability(publicManifest(capability.Manifest))
		}()
		if !allowed {
			continue
		}
		entries = append(entries, capability)
		providers[capability.Manifest.ID] = s.providers[capability.Manifest.ID]
	}
	return s.rebuild(entries, providers)
}

// rebuild produces a new immutable snapshot from a subset with a fresh digest.
func (s *CapabilitySnapshot) rebuild(entries []SnapshotCapability, providers map[string]any) *CapabilitySnapshot {
	digest, err := snapshotDigest(s.Scope, s.principal, entries)
	if err != nil {
		// The source snapshot already marshaled these values; treat an
		// encode failure as an empty selection instead of panicking.
		entries = []SnapshotCapability{}
		providers = map[string]any{}
	}
	return &CapabilitySnapshot{
		ID: digest, CreatedAt: time.Now().UTC(), Scope: s.Scope,
		principal: clonePrincipal(s.principal), capabilities: entries, providers: providers,
		credentials: s.credentials,
	}
}

// ValidateProfileID validates built-in bare profile IDs such as "general" and
// namespaced profile IDs using the same safe identifier vocabulary.
func ValidateProfileID(id string) error {
	if !namespacedIDPattern.MatchString(id) {
		return fmt.Errorf("profile id %q must be a safe identifier of at most 128 characters", id)
	}
	return nil
}

func profileLayerKey(scope ScopePath, profileID string) string {
	return scope.String() + "\x00" + profileID
}

func profileDigest(snapshot *AgentProfileSnapshot) (string, error) {
	copyOf := *snapshot
	copyOf.ID = ""
	copyOf.CreatedAt = time.Time{}
	data, err := json.Marshal(copyOf)
	if err != nil {
		return "", fmt.Errorf("encode agent profile snapshot: %w", err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func cloneProfileLayer(layer AgentProfileLayer) AgentProfileLayer {
	out := layer
	if layer.Name != nil {
		value := *layer.Name
		out.Name = &value
	}
	if layer.Description != nil {
		value := *layer.Description
		out.Description = &value
	}
	if layer.Model != nil {
		value := *layer.Model
		out.Model = &value
	}
	if layer.MaxSteps != nil {
		value := *layer.MaxSteps
		out.MaxSteps = &value
	}
	if layer.MaxToolCalls != nil {
		value := *layer.MaxToolCalls
		out.MaxToolCalls = &value
	}
	out.AddCapabilities = append([]string(nil), layer.AddCapabilities...)
	out.RemoveCapabilities = append([]string(nil), layer.RemoveCapabilities...)
	out.PutFragments = append([]PromptFragment(nil), layer.PutFragments...)
	out.RemoveFragments = append([]string(nil), layer.RemoveFragments...)
	out.Metadata = map[string]string{}
	for key, value := range layer.Metadata {
		out.Metadata[key] = value
	}
	return out
}

func cloneProfileState(state *profileState) *profileState {
	out := *state
	out.capabilities = map[string]bool{}
	for id, enabled := range state.capabilities {
		out.capabilities[id] = enabled
	}
	out.fragments = map[string]ResolvedPromptFragment{}
	for id, fragment := range state.fragments {
		out.fragments[id] = fragment
	}
	out.metadata = map[string]string{}
	for key, value := range state.metadata {
		out.metadata[key] = value
	}
	return &out
}
