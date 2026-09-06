// Package capabilityruntime defines the small, immutable extension point used
// to construct dynamic capability providers. Factories are trusted application
// code: the registry validates their identity and contains panics at the
// boundary, while each factory remains responsible for validating its input.
package capabilityruntime

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
)

var (
	ErrInvalidRegistry = errors.New("invalid capability runtime registry")
	ErrRuntimeNotFound = errors.New("capability runtime not found")
	ErrRuntimeVersion  = errors.New("capability runtime version mismatch")
	ErrFactoryPanic    = errors.New("capability runtime factory panicked")
	ErrInvalidRequest  = errors.New("invalid capability runtime request")
)

const DefaultCapacity = 32
const MaxFactories = 128

type Request struct {
	Manifest   core.CapabilityManifest
	Entrypoint string
	Workdir    string
	Sandbox    core.SandboxPolicy
	Writes     bool
	Method     string
	Headers    map[string]string
}

func (r Request) Clone() (Request, error) {
	manifest, err := cloneManifest(r.Manifest)
	if err != nil {
		return Request{}, err
	}
	r.Manifest = manifest
	if r.Headers != nil {
		headerCopy := make(map[string]string, len(r.Headers))
		for k, v := range r.Headers {
			headerCopy[k] = v
		}
		r.Headers = headerCopy
	}
	return r, nil
}

type Factory interface {
	ID() string
	Version() string
	ImplementationRevision() string
	New(context.Context, Request) (Result, error)
}
type Result struct {
	Provider               any
	Manifest               core.CapabilityManifest
	ImplementationRevision string
}

type Registration struct {
	ID, Version, ImplementationRevision string
	Factory                             Factory
}
type Registry struct {
	entries map[string]Registration
	byID    map[string]struct{}
}

func New(capacity int, factories ...Factory) (*Registry, error) {
	if capacity <= 0 {
		capacity = DefaultCapacity
	}
	if capacity > MaxFactories || len(factories) > capacity {
		return nil, ErrInvalidRegistry
	}
	entries := make(map[string]Registration, len(factories))
	byID := make(map[string]struct{})
	for _, f := range factories {
		if isNil(f) {
			return nil, ErrInvalidRegistry
		}
		id, version, err := safeIdentity(f)
		if err != nil {
			return nil, err
		}
		revision, revErr := safeRevision(f)
		if revErr != nil {
			return nil, revErr
		}
		if !validToken(id) || !validToken(version) || !validToken(revision) {
			return nil, ErrInvalidRegistry
		}
		key := id + "\x00" + version
		if _, ok := entries[key]; ok {
			return nil, ErrInvalidRegistry
		}
		entries[key] = Registration{ID: id, Version: version, ImplementationRevision: revision, Factory: f}
		byID[id] = struct{}{}
	}
	return &Registry{entries: entries, byID: byID}, nil
}

func (r *Registry) Resolve(id, version string) (Factory, error) {
	if r == nil {
		return nil, ErrRuntimeNotFound
	}
	e, ok := r.entries[id+"\x00"+version]
	if !ok {
		if _, exists := r.byID[id]; exists {
			return nil, ErrRuntimeVersion
		}
		return nil, ErrRuntimeNotFound
	}
	if e.Version != version {
		return nil, ErrRuntimeVersion
	}
	return e.Factory, nil
}
func (r *Registry) Revision(id, version string) (string, error) {
	if r == nil {
		return "", ErrRuntimeNotFound
	}
	e, ok := r.entries[id+"\x00"+version]
	if !ok {
		if _, exists := r.byID[id]; exists {
			return "", ErrRuntimeVersion
		}
		return "", ErrRuntimeNotFound
	}
	return e.ImplementationRevision, nil
}

type Metadata struct{ ID, Version, ImplementationRevision string }

func (r *Registry) List() []Metadata {
	if r == nil {
		return nil
	}
	out := make([]Metadata, 0, len(r.entries))
	for _, e := range r.entries {
		out = append(out, Metadata{ID: e.ID, Version: e.Version, ImplementationRevision: e.ImplementationRevision})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ID != out[j].ID {
			return out[i].ID < out[j].ID
		}
		if out[i].Version != out[j].Version {
			return out[i].Version < out[j].Version
		}
		return out[i].ImplementationRevision < out[j].ImplementationRevision
	})
	return out
}

func (r *Registry) Create(ctx context.Context, id, version string, req Request) (result Result, err error) {
	if ctx == nil {
		return Result{}, ErrInvalidRequest
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	f, err := r.Resolve(id, version)
	if err != nil {
		return Result{}, err
	}
	if isNil(f) {
		return Result{}, ErrRuntimeNotFound
	}
	defer func() {
		if recover() != nil {
			result, err = Result{}, ErrFactoryPanic
		}
	}()
	owned, cloneErr := req.Clone()
	if cloneErr != nil {
		return Result{}, cloneErr
	}
	if owned.Manifest.ID == "" && req.Manifest.ID != "" {
		return Result{}, ErrInvalidRequest
	}
	result, err = f.New(ctx, owned)
	if err != nil {
		return Result{}, err
	}
	if isNil(result.Provider) || result.Manifest.ID == "" || result.Manifest.Version == "" {
		return Result{}, ErrInvalidRegistry
	}
	result.ImplementationRevision, _ = r.Revision(id, version)
	ownedResult, cloneErr := cloneManifest(result.Manifest)
	if cloneErr != nil {
		return Result{}, cloneErr
	}
	result.Manifest = ownedResult
	return result, nil
}

func cloneManifest(m core.CapabilityManifest) (core.CapabilityManifest, error) {
	out := m
	var err error
	if out.InputSchema, err = cloneJSONMap(m.InputSchema); err != nil {
		return core.CapabilityManifest{}, err
	}
	if out.OutputSchema, err = cloneJSONMap(m.OutputSchema); err != nil {
		return core.CapabilityManifest{}, err
	}
	if m.Metadata != nil {
		out.Metadata = map[string]string{}
		for k, v := range m.Metadata {
			out.Metadata[k] = v
		}
	}
	if m.Tool != nil {
		out.Tool = &core.ToolExposure{Description: m.Tool.Description}
		out.Tool.Parameters, err = cloneJSONMap(m.Tool.Parameters)
		if err != nil {
			return core.CapabilityManifest{}, err
		}
	}
	if m.Execution != nil {
		e := *m.Execution
		e.Headers = map[string]string{}
		for k, v := range m.Execution.Headers {
			e.Headers[k] = v
		}
		out.Execution = &e
	}
	out.RequiredPermissions = append([]core.Permission(nil), m.RequiredPermissions...)
	out.RequiredCredentials = append([]core.CredentialRef(nil), m.RequiredCredentials...)
	return out, nil
}
func cloneJSONMap(in map[string]any) (map[string]any, error) {
	if in == nil {
		return nil, nil
	}
	v, err := cloneJSONValue(in)
	if err != nil {
		return nil, err
	}
	return v.(map[string]any), nil
}
func cloneJSONValue(v any) (any, error) {
	switch x := v.(type) {
	case nil, string, bool, json.Number:
		return x, nil
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, val := range x {
			if strings.TrimSpace(k) != k {
				return nil, ErrInvalidRequest
			}
			c, err := cloneJSONValue(val)
			if err != nil {
				return nil, err
			}
			out[k] = c
		}
		return out, nil
	case []any:
		out := make([]any, len(x))
		for i, val := range x {
			c, err := cloneJSONValue(val)
			if err != nil {
				return nil, err
			}
			out[i] = c
		}
		return out, nil
	default:
		rv := reflect.ValueOf(v)
		switch rv.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return v, nil
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return v, nil
		case reflect.Float32, reflect.Float64:
			f := rv.Float()
			if math.IsNaN(f) || math.IsInf(f, 0) {
				return nil, ErrInvalidRequest
			}
			return v, nil
		}
		return nil, ErrInvalidRequest
	}
}

func isNil(v any) bool {
	if v == nil {
		return true
	}
	x := reflect.ValueOf(v)
	switch x.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
		return x.IsNil()
	}
	return false
}

func validToken(s string) bool {
	if s == "" || len(s) > 128 || strings.TrimSpace(s) != s || !utf8.ValidString(s) {
		return false
	}
	for _, r := range s {
		if unicode.IsControl(r) || unicode.IsSpace(r) {
			return false
		}
	}
	return true
}
func safeIdentity(f Factory) (id, version string, err error) {
	defer func() {
		if recover() != nil {
			id, version, err = "", "", ErrFactoryPanic
		}
	}()
	return f.ID(), f.Version(), nil
}

func safeRevision(f Factory) (revision string, err error) {
	defer func() {
		if recover() != nil {
			revision, err = "", ErrFactoryPanic
		}
	}()
	return f.ImplementationRevision(), nil
}
