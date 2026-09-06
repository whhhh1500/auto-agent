package core

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
)

// CredentialRef is a stable reference to a secret, never the secret value.
type CredentialRef string

var ErrCredentialNotFound = errors.New("credential not found")

// CredentialValue is intentionally excluded from JSON surfaces.
type CredentialValue struct {
	Value  string `json:"-"`
	Source string `json:"-"`
}

// CredentialAccessor is the least-authority resolver passed to one capability call.
type CredentialAccessor interface {
	Resolve(ctx context.Context, ref CredentialRef) (CredentialValue, error)
}

// CredentialResolver resolves a reference for an authenticated ownership scope.
type CredentialResolver interface {
	ResolveCredential(
		ctx context.Context,
		principal Principal,
		target ScopePath,
		ref CredentialRef,
	) (CredentialValue, error)
}

// CredentialResolverFunc adapts a function to CredentialResolver.
type CredentialResolverFunc func(context.Context, Principal, ScopePath, CredentialRef) (CredentialValue, error)

func (f CredentialResolverFunc) ResolveCredential(
	ctx context.Context,
	principal Principal,
	target ScopePath,
	ref CredentialRef,
) (CredentialValue, error) {
	return f(ctx, principal, target, ref)
}

// CredentialProvider owns the value for one credential reference.
type CredentialProvider interface {
	Resolve(ctx context.Context, principal Principal) (CredentialValue, error)
}

// CredentialProviderFunc adapts a function to CredentialProvider.
type CredentialProviderFunc func(context.Context, Principal) (CredentialValue, error)

func (f CredentialProviderFunc) Resolve(ctx context.Context, principal Principal) (CredentialValue, error) {
	return f(ctx, principal)
}

// StaticCredentialProvider is useful for tests and trusted bootstrap code.
// Production systems should prefer a KMS, vault, environment, or private runner provider.
type StaticCredentialProvider struct {
	Value  string
	Source string
}

func (p StaticCredentialProvider) Resolve(context.Context, Principal) (CredentialValue, error) {
	if p.Value == "" {
		return CredentialValue{}, ErrCredentialNotFound
	}
	return CredentialValue(p), nil
}

// EnvironmentCredentialProvider reads one environment variable at call time.
type EnvironmentCredentialProvider struct {
	Name string
}

func (p EnvironmentCredentialProvider) Resolve(context.Context, Principal) (CredentialValue, error) {
	value, ok := os.LookupEnv(p.Name)
	if !ok || value == "" {
		return CredentialValue{}, fmt.Errorf("%w: environment variable %s", ErrCredentialNotFound, p.Name)
	}
	return CredentialValue{Value: value, Source: "env:" + p.Name}, nil
}

// CredentialBindingMode controls inherited credential reference resolution.
type CredentialBindingMode string

const (
	CredentialProvide CredentialBindingMode = "provide"
	CredentialReplace CredentialBindingMode = "replace"
	CredentialDisable CredentialBindingMode = "disable"
)

// CredentialBinding attaches a provider to one scope without exposing its value.
type CredentialBinding struct {
	Scope     ScopePath
	Ref       CredentialRef
	Mode      CredentialBindingMode
	Provider  CredentialProvider
	Protected bool
}

type storedCredentialBinding struct {
	CredentialBinding
	order uint64
}

// CredentialRegistry resolves layered secret references at call time.
type CredentialRegistry struct {
	mu          sync.RWMutex
	bindings    map[string][]storedCredentialBinding
	next        uint64
	maxBindings int
}

func NewCredentialRegistry() *CredentialRegistry {
	return &CredentialRegistry{bindings: map[string][]storedCredentialBinding{}}
}

// Bind records one credential reference contribution.
func (r *CredentialRegistry) Bind(binding CredentialBinding) error {
	_, err := r.Mount(binding)
	return err
}

// Mount records one contribution and returns an idempotent unmount function.
func (r *CredentialRegistry) Mount(binding CredentialBinding) (func(), error) {
	if binding.Scope.Depth() == 0 {
		return nil, fmt.Errorf("credential binding has an empty scope")
	}
	if err := validateCredentialRef(binding.Ref); err != nil {
		return nil, err
	}
	if binding.Mode == "" {
		binding.Mode = CredentialProvide
	}
	switch binding.Mode {
	case CredentialProvide, CredentialReplace:
		if binding.Provider == nil {
			return nil, fmt.Errorf("credential %q has no provider", binding.Ref)
		}
	case CredentialDisable:
		binding.Provider = nil
	default:
		return nil, fmt.Errorf("credential %q has unknown binding mode %q", binding.Ref, binding.Mode)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if scopedBindingCount(r.bindings) >= registryCap(r.maxBindings) {
		return nil, fmt.Errorf("registry bindings exceed maximum of %d", registryCap(r.maxBindings))
	}
	r.next++
	order := r.next
	key := binding.Scope.String()
	r.bindings[key] = append(r.bindings[key], storedCredentialBinding{CredentialBinding: binding, order: order})
	var once sync.Once
	return func() {
		once.Do(func() {
			r.mu.Lock()
			defer r.mu.Unlock()
			current := r.bindings[key]
			for i, item := range current {
				if item.order == order {
					r.bindings[key] = append(current[:i], current[i+1:]...)
					break
				}
			}
		})
	}, nil
}

// CredentialRefView is the audit view of one stored credential reference.
// It deliberately carries no provider internals: secret values are never
// listed, echoed, or summarized by any listing surface.
type CredentialRefView struct {
	Ref   CredentialRef         `json:"ref"`
	Scope ScopePath             `json:"scope"`
	Mode  CredentialBindingMode `json:"mode"`
}

// List returns every credential reference visible at the target scope or one
// of its ancestors, root-first. Values are not retrievable through this or
// any other listing API.
func (r *CredentialRegistry) List(target ScopePath) ([]CredentialRefView, error) {
	if target.Depth() == 0 {
		return nil, fmt.Errorf("target scope is empty")
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := []CredentialRefView{}
	for _, prefix := range target.Prefixes() {
		for _, binding := range r.bindings[prefix.String()] {
			out = append(out, CredentialRefView{
				Ref: binding.Ref, Scope: binding.Scope, Mode: binding.Mode,
			})
		}
	}
	return out, nil
}

// ResolveCredential resolves the nearest effective provider without caching secret values.
func (r *CredentialRegistry) ResolveCredential(
	ctx context.Context,
	principal Principal,
	target ScopePath,
	ref CredentialRef,
) (CredentialValue, error) {
	if !principal.Scope.IsAncestorOf(target) {
		return CredentialValue{}, fmt.Errorf("principal scope %q does not own credential target %q", principal.Scope, target)
	}
	if err := validateCredentialRef(ref); err != nil {
		return CredentialValue{}, err
	}

	type effectiveCredential struct {
		provider  CredentialProvider
		scope     ScopePath
		protected bool
	}
	var effective *effectiveCredential
	r.mu.RLock()
	for _, prefix := range target.Prefixes() {
		for _, binding := range r.bindings[prefix.String()] {
			if binding.Ref != ref {
				continue
			}
			switch binding.Mode {
			case CredentialProvide:
				if effective != nil {
					r.mu.RUnlock()
					return CredentialValue{}, fmt.Errorf("credential %q is already provided; use replace explicitly", ref)
				}
				effective = &effectiveCredential{binding.Provider, binding.Scope, binding.Protected}
			case CredentialReplace:
				if effective == nil {
					r.mu.RUnlock()
					return CredentialValue{}, fmt.Errorf("credential %q cannot replace a missing credential", ref)
				}
				if effective.protected && !effective.scope.Equal(binding.Scope) {
					r.mu.RUnlock()
					return CredentialValue{}, fmt.Errorf("credential %q is protected by scope %q", ref, effective.scope)
				}
				effective = &effectiveCredential{binding.Provider, binding.Scope, binding.Protected}
			case CredentialDisable:
				if effective == nil {
					continue
				}
				if effective.protected && !effective.scope.Equal(binding.Scope) {
					r.mu.RUnlock()
					return CredentialValue{}, fmt.Errorf("credential %q is protected by scope %q", ref, effective.scope)
				}
				effective = nil
			}
		}
	}
	r.mu.RUnlock()
	if effective == nil || effective.provider == nil {
		return CredentialValue{}, fmt.Errorf("%w: %s", ErrCredentialNotFound, ref)
	}
	value, err := safeCredentialProviderResolve(effective.provider, ctx, clonePrincipal(principal))
	if err != nil {
		return CredentialValue{}, err
	}
	if value.Value == "" {
		return CredentialValue{}, fmt.Errorf("%w: %s", ErrCredentialNotFound, ref)
	}
	return value, nil
}

func safeCredentialProviderResolve(provider CredentialProvider, ctx context.Context, principal Principal) (value CredentialValue, err error) {
	defer func() {
		if recover() != nil {
			err = fmt.Errorf("credential provider panicked")
			value = CredentialValue{}
		}
	}()
	return provider.Resolve(ctx, principal)
}

type callCredentialAccessor struct {
	values map[CredentialRef]CredentialValue
}

func (a callCredentialAccessor) Resolve(_ context.Context, ref CredentialRef) (CredentialValue, error) {
	value, ok := a.values[ref]
	if !ok {
		return CredentialValue{}, fmt.Errorf("%w: %s is not declared by this capability", ErrCredentialNotFound, ref)
	}
	return value, nil
}

func resolveCallCredentials(
	ctx context.Context,
	resolver CredentialResolver,
	principal Principal,
	target ScopePath,
	refs []CredentialRef,
) (CredentialAccessor, error) {
	values := map[CredentialRef]CredentialValue{}
	for _, ref := range refs {
		if resolver == nil {
			return nil, fmt.Errorf("%w: %s", ErrCredentialNotFound, ref)
		}
		value, err := safeCredentialResolve(resolver, ctx, principal, target, ref)
		if err != nil {
			return nil, err
		}
		values[ref] = value
	}
	return callCredentialAccessor{values: values}, nil
}

func safeCredentialResolve(resolver CredentialResolver, ctx context.Context, principal Principal, target ScopePath, ref CredentialRef) (value CredentialValue, err error) {
	defer func() {
		if recover() != nil {
			err = fmt.Errorf("credential resolver panicked")
			value = CredentialValue{}
		}
	}()
	return resolver.ResolveCredential(ctx, principal, target, ref)
}

func validateCredentialRef(ref CredentialRef) error {
	return ValidateNamespacedID(string(ref))
}
