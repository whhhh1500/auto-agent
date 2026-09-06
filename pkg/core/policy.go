package core

import (
	"fmt"
	"sort"
	"sync"
)

// PolicyLayer is one ownership scope's contribution to the effective run
// policy. Layers can only NARROW: permissions are intersected or removed and
// budgets are capped at the strictest value. A layer has no way to widen a
// parent restriction, which makes the documented "children only narrow" rule
// structural rather than enforced.
type PolicyLayer struct {
	Scope ScopePath

	// AllowPermissions, when non-empty, intersects the effective permission
	// set with exactly these grants.
	AllowPermissions []Permission
	// DenyPermissions removes grants from the effective set.
	DenyPermissions []Permission
	// MaxSteps caps the agent loop step count for runs under this scope.
	MaxSteps *int
	// MaxToolCalls caps total tool calls per run for runs under this scope.
	MaxToolCalls *int
}

type storedPolicyLayer struct {
	PolicyLayer
	order uint64
}

// PolicyRegistry stores layered policy contributions and resolves the
// intersection for one authenticated run target.
type PolicyRegistry struct {
	mu          sync.RWMutex
	layers      map[string][]storedPolicyLayer
	next        uint64
	maxBindings int
}

func NewPolicyRegistry() *PolicyRegistry {
	return &PolicyRegistry{layers: map[string][]storedPolicyLayer{}}
}

// Bind records one policy contribution.
func (r *PolicyRegistry) Bind(layer PolicyLayer) error {
	_, err := r.Mount(layer)
	return err
}

// Mount records one policy layer and returns an idempotent unmount function.
func (r *PolicyRegistry) Mount(layer PolicyLayer) (func(), error) {
	if layer.Scope.Depth() == 0 {
		return nil, fmt.Errorf("policy layer has an empty scope")
	}
	if layer.MaxSteps != nil && (*layer.MaxSteps <= 0 || *layer.MaxSteps > HardMaxSteps) {
		return nil, fmt.Errorf("policy layer max steps must be between 1 and %d", HardMaxSteps)
	}
	if layer.MaxToolCalls != nil && (*layer.MaxToolCalls <= 0 || *layer.MaxToolCalls > HardMaxToolCalls) {
		return nil, fmt.Errorf("policy layer max tool calls must be between 1 and %d", HardMaxToolCalls)
	}
	for _, permission := range append(append([]Permission(nil), layer.AllowPermissions...), layer.DenyPermissions...) {
		if err := ValidateNamespacedID(string(permission)); err != nil {
			return nil, fmt.Errorf("policy layer permission %q is invalid: %w", permission, err)
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if scopedBindingCount(r.layers) >= registryCap(r.maxBindings) {
		return nil, fmt.Errorf("registry bindings exceed maximum of %d", registryCap(r.maxBindings))
	}
	r.next++
	order := r.next
	key := layer.Scope.String()
	r.layers[key] = append(r.layers[key], storedPolicyLayer{PolicyLayer: layer, order: order})
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
		})
	}, nil
}

// ResolvedPolicy is the intersected policy for one run target. Permissions
// start from the principal grants and can only shrink layer by layer.
type ResolvedPolicy struct {
	Permissions  PermissionSet
	MaxSteps     *int
	MaxToolCalls *int
}

// PolicyLayerView is the audit view of one stored policy layer.
type PolicyLayerView struct {
	Scope        ScopePath    `json:"scope"`
	AllowPerms   []Permission `json:"allow_permissions,omitempty"`
	DenyPerms    []Permission `json:"deny_permissions,omitempty"`
	MaxSteps     *int         `json:"max_steps,omitempty"`
	MaxToolCalls *int         `json:"max_tool_calls,omitempty"`
}

// List returns every policy layer recorded at the target scope or one of its
// ancestors, root-first. It backs catalog and admin surfaces; layers carry no
// secrets.
func (r *PolicyRegistry) List(target ScopePath) ([]PolicyLayerView, error) {
	if target.Depth() == 0 {
		return nil, fmt.Errorf("target scope is empty")
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := []PolicyLayerView{}
	for _, prefix := range target.Prefixes() {
		layers := append([]storedPolicyLayer(nil), r.layers[prefix.String()]...)
		sort.SliceStable(layers, func(i, j int) bool { return layers[i].order < layers[j].order })
		for _, stored := range layers {
			view := PolicyLayerView{
				Scope:        stored.Scope,
				AllowPerms:   append([]Permission(nil), stored.AllowPermissions...),
				DenyPerms:    append([]Permission(nil), stored.DenyPermissions...),
				MaxSteps:     stored.MaxSteps,
				MaxToolCalls: stored.MaxToolCalls,
			}
			out = append(out, view)
		}
	}
	return out, nil
}

// Resolve walks target prefixes root-to-leaf, intersecting every layer.
func (r *PolicyRegistry) Resolve(principal Principal, target ScopePath) (*ResolvedPolicy, error) {
	if !principal.Scope.IsAncestorOf(target) {
		return nil, fmt.Errorf("principal scope %q does not own policy target %q", principal.Scope, target)
	}
	r.mu.RLock()
	defer r.mu.RUnlock()

	resolved := &ResolvedPolicy{Permissions: principal.Grants.Clone()}
	for _, prefix := range target.Prefixes() {
		layers := append([]storedPolicyLayer(nil), r.layers[prefix.String()]...)
		sort.SliceStable(layers, func(i, j int) bool { return layers[i].order < layers[j].order })
		for _, stored := range layers {
			layer := stored.PolicyLayer
			if len(layer.AllowPermissions) > 0 {
				allow := NewPermissionSet(layer.AllowPermissions...)
				resolved.Permissions = resolved.Permissions.Intersect(allow)
			}
			for _, denied := range layer.DenyPermissions {
				delete(resolved.Permissions, denied)
			}
			if layer.MaxSteps != nil && (resolved.MaxSteps == nil || *layer.MaxSteps < *resolved.MaxSteps) {
				capValue := *layer.MaxSteps
				resolved.MaxSteps = &capValue
			}
			if layer.MaxToolCalls != nil && (resolved.MaxToolCalls == nil || *layer.MaxToolCalls < *resolved.MaxToolCalls) {
				capValue := *layer.MaxToolCalls
				resolved.MaxToolCalls = &capValue
			}
		}
	}
	return resolved, nil
}
