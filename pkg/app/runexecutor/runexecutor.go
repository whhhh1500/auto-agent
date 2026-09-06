// Package runexecutor defines the application-level plug-in seam for choosing
// a run orchestrator. A Profile may select its stable ID in an upper-layer
// resolver; this package deliberately owns neither Profile persistence nor
// database resolution.
package runexecutor

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"unicode"
	"unicode/utf8"

	"github.com/cc-auto-agent/harness-core/pkg/core"
)

const (
	// SequentialID is the backward-compatible default orchestrator.
	SequentialID = "sequential"
	// SequentialVersion identifies the built-in sequential adapter contract.
	SequentialVersion = "1"
	// SequentialImplementationRevision identifies direct core.Runtime delegation.
	SequentialImplementationRevision = "core-runtime-v1"
	// DefaultMaxExecutors bounds an in-process registry.
	DefaultMaxExecutors = 64
	maxRegistryEntries  = 256
	maxMetadataBytes    = 128
)

var (
	ErrInvalidRegistration = errors.New("invalid run executor registration")
	ErrRegistryFull        = errors.New("run executor registry is full")
	ErrExecutorNotFound    = errors.New("run executor is not registered")
	ErrVersionMismatch     = errors.New("run executor version mismatch")
	ErrFactoryPanic        = errors.New("run executor factory panicked")
)

// RunExecutor is the narrow orchestration boundary. It intentionally reuses
// the existing core request/result and event types so an optional executor
// cannot create a second transport or session DTO family.
type RunExecutor interface {
	RunTurn(context.Context, core.Principal, *core.Session, core.TurnInput, func(core.SessionEvent)) (core.TurnResult, error)
	ResumeTurn(context.Context, core.Principal, *core.Session, core.ResumeInput, func(core.SessionEvent)) (core.TurnResult, error)
}

// ContinuingRunExecutor is the optional post-result continuation protocol.
// Implementations resume only facts already durable in the supplied Session.
type ContinuingRunExecutor interface {
	ContinueTurn(context.Context, core.Principal, *core.Session, core.ResumeInput, func(core.SessionEvent)) (core.TurnResult, error)
}

// Metadata is stable, bounded selection evidence. It has no provider, graph,
// server, database, or implementation-locator fields.
type Metadata struct {
	ID                     string
	Version                string
	ImplementationRevision string
}

// Factory creates one executor from a previously validated registration.
// It must be cheap and side-effect-free: Resolve may be retried and must not
// itself start runs, reserve resources, or make external calls. A panic is
// contained and returned as ErrFactoryPanic.
type Dependencies struct{ Runtime *core.Runtime }
type Factory func(Dependencies) (RunExecutor, error)

// Registration is immutable once accepted by Registry. Factory is retained as
// a function value; callers receive metadata copies rather than this record.
type Registration struct {
	Metadata Metadata
	Factory  Factory
}

// Registry keeps a bounded set of exact orchestrator registrations. It has no
// global instance and does not start goroutines or timers.
type Registry struct {
	limit   int
	entries map[string]Registration
}

// NewRegistry creates a bounded empty registry and optionally registers the
// supplied records. The input slice is never retained.
func NewRegistry(limit int, registrations ...Registration) (*Registry, error) {
	if limit <= 0 || limit > maxRegistryEntries {
		return nil, fmt.Errorf("%w: registry capacity", ErrInvalidRegistration)
	}
	registry := &Registry{limit: limit, entries: make(map[string]Registration, limit)}
	for _, registration := range registrations {
		copyOf, err := validateRegistration(registration)
		if err != nil {
			return nil, err
		}
		if _, exists := registry.entries[copyOf.Metadata.ID]; exists {
			return nil, fmt.Errorf("%w: %s", ErrInvalidRegistration, copyOf.Metadata.ID)
		}
		if len(registry.entries) >= registry.limit {
			return nil, ErrRegistryFull
		}
		registry.entries[copyOf.Metadata.ID] = copyOf
	}
	return registry, nil
}

// NewDefaultRegistry registers the injected core runtime as the only built-in
// sequential executor. Graph and third-party executors are opt-in registrations.
func NewDefaultRegistry() (*Registry, error) {
	registration, err := SequentialRegistration()
	if err != nil {
		return nil, err
	}
	return NewRegistry(DefaultMaxExecutors, registration)
}

// SequentialRegistration adapts one caller-owned core Runtime without changing
// its error or panic behavior.
func SequentialRegistration() (Registration, error) {
	return Registration{Metadata: Metadata{ID: SequentialID, Version: SequentialVersion, ImplementationRevision: SequentialImplementationRevision}, Factory: func(d Dependencies) (RunExecutor, error) { return NewSequential(d.Runtime) }}, nil
}

// Register adds exactly one registration. Existing records are immutable and
// duplicate IDs are rejected even if their metadata happens to match. One ID
// has one active version; an upgrade uses a new ID or drains then rebuilds the
// registry, never an in-place multi-version hot switch.
// Resolve selects an exact ID/version pair and creates an executor. It never
// falls back to sequential when the requested ID is explicit.
func (registry *Registry) Resolve(id, version string, dependencies Dependencies) (RunExecutor, Metadata, error) {
	if registry == nil {
		return nil, Metadata{}, fmt.Errorf("%w: nil registry", ErrExecutorNotFound)
	}
	if err := validateMetadataPart(id, "id"); err != nil {
		return nil, Metadata{}, err
	}
	if err := validateMetadataPart(version, "version"); err != nil {
		return nil, Metadata{}, err
	}
	registration, found := registry.entries[id]
	if !found {
		return nil, Metadata{}, fmt.Errorf("%w: %s", ErrExecutorNotFound, id)
	}
	if registration.Metadata.Version != version {
		return nil, Metadata{}, fmt.Errorf("%w: %s@%s", ErrVersionMismatch, id, version)
	}
	executor, err := invokeFactory(registration.Factory, dependencies)
	if err != nil {
		return nil, Metadata{}, err
	}
	if isNilExecutor(executor) {
		return nil, Metadata{}, fmt.Errorf("%w: factory returned nil executor", ErrInvalidRegistration)
	}
	return executor, copyMetadata(registration.Metadata), nil
}

// List returns sorted metadata copies for diagnostics and control-plane
// displays. It never returns factory values or the registry's backing map.
func (registry *Registry) List() []Metadata {
	if registry == nil {
		return nil
	}
	metadata := make([]Metadata, 0, len(registry.entries))
	for _, registration := range registry.entries {
		metadata = append(metadata, copyMetadata(registration.Metadata))
	}
	sort.Slice(metadata, func(left, right int) bool {
		if metadata[left].ID == metadata[right].ID {
			return metadata[left].Version < metadata[right].Version
		}
		return metadata[left].ID < metadata[right].ID
	})
	return metadata
}

// ResolveOrDefault resolves sequential only when ID is empty. A supplied
// version still has to exactly match sequential; explicit unknown IDs never
// fall back.
func (registry *Registry) ResolveOrDefault(id, version string, dependencies Dependencies) (RunExecutor, Metadata, error) {
	if id == "" {
		id = SequentialID
		if version == "" {
			version = SequentialVersion
		}
	}
	return registry.Resolve(id, version, dependencies)
}

func validateRegistration(registration Registration) (Registration, error) {
	if registration.Factory == nil {
		return Registration{}, fmt.Errorf("%w: nil factory", ErrInvalidRegistration)
	}
	if err := validateMetadata(registration.Metadata); err != nil {
		return Registration{}, err
	}
	return Registration{Metadata: copyMetadata(registration.Metadata), Factory: registration.Factory}, nil
}

func validateMetadata(metadata Metadata) error {
	for name, value := range map[string]string{"id": metadata.ID, "version": metadata.Version, "implementation revision": metadata.ImplementationRevision} {
		if err := validateMetadataPart(value, name); err != nil {
			return err
		}
	}
	return nil
}

func validateMetadataPart(value, name string) error {
	if value == "" || len(value) > maxMetadataBytes || !utf8.ValidString(value) {
		return fmt.Errorf("%w: invalid %s", ErrInvalidRegistration, name)
	}
	for _, character := range value {
		if unicode.IsSpace(character) || unicode.IsControl(character) {
			return fmt.Errorf("%w: invalid %s", ErrInvalidRegistration, name)
		}
	}
	return nil
}

func copyMetadata(metadata Metadata) Metadata { return metadata }

func invokeFactory(factory Factory, dependencies Dependencies) (executor RunExecutor, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			executor = nil
			err = fmt.Errorf("%w: %T", ErrFactoryPanic, recovered)
		}
	}()
	return factory(dependencies)
}

func isNilExecutor(executor RunExecutor) bool {
	if executor == nil {
		return true
	}
	value := reflect.ValueOf(executor)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

// Sequential directly delegates to an injected core Runtime. It deliberately
// does not recover panics or translate errors, preserving core semantics.
type Sequential struct{ runtime *core.Runtime }

func NewSequential(runtime *core.Runtime) (*Sequential, error) {
	if runtime == nil {
		return nil, fmt.Errorf("%w: sequential runtime is nil", ErrInvalidRegistration)
	}
	return &Sequential{runtime: runtime}, nil
}

func (executor *Sequential) RunTurn(ctx context.Context, principal core.Principal, session *core.Session, input core.TurnInput, emit func(core.SessionEvent)) (core.TurnResult, error) {
	return executor.runtime.RunTurn(ctx, principal, session, input, emit)
}

func (executor *Sequential) ResumeTurn(ctx context.Context, principal core.Principal, session *core.Session, input core.ResumeInput, emit func(core.SessionEvent)) (core.TurnResult, error) {
	return executor.runtime.ResumeTurn(ctx, principal, session, input, emit)
}

func (executor *Sequential) ContinueTurn(ctx context.Context, principal core.Principal, session *core.Session, input core.ResumeInput, emit func(core.SessionEvent)) (core.TurnResult, error) {
	return executor.runtime.ContinueTurn(ctx, principal, session, input, emit)
}
