package core

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

type capabilityInvocationContextKey struct{}

type capabilityInvocationContext struct {
	invocation          Invocation
	remaining           int
	compositionRevision string
}

func withCapabilityInvocation(ctx context.Context, invocation Invocation, remaining int, compositionRevision string) context.Context {
	return context.WithValue(ctx, capabilityInvocationContextKey{}, capabilityInvocationContext{invocation: invocation, remaining: remaining, compositionRevision: compositionRevision})
}

func capabilityInvocationFromContext(ctx context.Context) (Invocation, int, string) {
	value, _ := ctx.Value(capabilityInvocationContextKey{}).(capabilityInvocationContext)
	return value.invocation, value.remaining, value.compositionRevision
}

// ToolProvider executes a capability exposed through the model tool consumer.
type ToolProvider interface {
	Execute(ctx context.Context, request CapabilityRequest) (CapabilityResult, error)
}

const (
	MaxCapabilityTimeoutMs          = 24 * 60 * 60 * 1000
	MaxCapabilityManifestBytes      = 256 << 10
	MaxCapabilitySnapshotBytes      = 8 << 20
	DefaultMaxCapabilityOutputBytes = 1 << 20
	HardMaxCapabilityOutputBytes    = 16 << 20
	MaxCapabilityMetadataBytes      = 256 << 10
	MaxRegistryBindings             = 4096
	MaxToolArgumentBytes            = 1 << 20
)

// Capability combines a manifest and an in-process provider for convenient registration.
type Capability interface {
	Manifest() CapabilityManifest
	ToolProvider
}

// Executor runs a declarative capability in a specific execution environment.
type Executor interface {
	Runtime() string
	Execute(ctx context.Context, spec ExecutionSpec, request CapabilityRequest) (CapabilityResult, error)
}

// ExecutorProvider adapts an Executor to the normal CapabilityProvider interface.
type ExecutorProvider struct {
	Executor Executor
	Spec     ExecutionSpec
}

func (p ExecutorProvider) ArtifactRevision() string {
	if p.Executor == nil {
		return ""
	}
	if revision := artifactRevisionLabel(p.Executor); revision != "" {
		return revision
	}
	return "executor/" + p.Executor.Runtime()
}

// Execute delegates one request to the configured executor.
func (p ExecutorProvider) Execute(ctx context.Context, request CapabilityRequest) (CapabilityResult, error) {
	if p.Executor == nil {
		return CapabilityResult{Content: "capability executor is not configured", OK: false}, nil
	}
	return p.Executor.Execute(ctx, p.Spec, request)
}

// BindingMode controls how a scope contributes to an inherited capability set.
type BindingMode string

const (
	BindingProvide BindingMode = "provide"
	BindingReplace BindingMode = "replace"
	BindingDisable BindingMode = "disable"
)

// CapabilityBinding attaches a versioned provider decision to one ownership scope.
type CapabilityBinding struct {
	Scope     ScopePath
	Mode      BindingMode
	Manifest  CapabilityManifest
	Provider  any
	Protected bool
}

type storedBinding struct {
	CapabilityBinding
	order uint64
}

// CapabilityRegistry stores scope-owned capability contributions. It does not
// decide visibility; Resolve creates an immutable per-run view.
type CapabilityRegistry struct {
	mu          sync.RWMutex
	bindings    map[string][]storedBinding
	next        uint64
	maxBindings int
}

// NewCapabilityRegistry creates an empty multi-scope registry.
func NewCapabilityRegistry() *CapabilityRegistry {
	return &CapabilityRegistry{bindings: map[string][]storedBinding{}}
}

// Bind validates and records one capability contribution.
func (r *CapabilityRegistry) Bind(binding CapabilityBinding) error {
	_, err := r.Mount(binding)
	return err
}

// Mount records one contribution and returns an idempotent unmount function.
func (r *CapabilityRegistry) Mount(binding CapabilityBinding) (func(), error) {
	if binding.Scope.Depth() == 0 {
		return nil, fmt.Errorf("capability binding has an empty scope")
	}
	if err := validateCapabilityID(binding.Manifest.ID); err != nil {
		return nil, err
	}
	if binding.Manifest.Version == "" {
		return nil, fmt.Errorf("capability %q has an empty version", binding.Manifest.ID)
	}
	if binding.Mode == "" {
		binding.Mode = BindingProvide
	}
	switch binding.Mode {
	case BindingProvide, BindingReplace:
		if err := validateCapabilityManifest(binding.Manifest); err != nil {
			return nil, err
		}
		encodedManifest, err := json.Marshal(binding.Manifest)
		if err != nil {
			return nil, fmt.Errorf("capability %q manifest is not JSON serializable: %w", binding.Manifest.ID, err)
		}
		if len(encodedManifest) > MaxCapabilityManifestBytes {
			return nil, fmt.Errorf("capability %q manifest exceeds %d bytes", binding.Manifest.ID, MaxCapabilityManifestBytes)
		}
		if binding.Provider == nil {
			return nil, fmt.Errorf("capability %q has no provider", binding.Manifest.ID)
		}
		if binding.Manifest.Tool != nil {
			if _, executable := binding.Provider.(ToolProvider); !executable {
				return nil, fmt.Errorf("capability %q is exposed as a tool but its provider does not implement ToolProvider", binding.Manifest.ID)
			}
		}
	case BindingDisable:
		binding.Provider = nil
	default:
		return nil, fmt.Errorf("capability %q has unknown binding mode %q", binding.Manifest.ID, binding.Mode)
	}
	// A registry contribution is immutable after Mount returns. Providers are
	// intentionally retained by reference, but every mutable manifest field is
	// copied so caller-owned maps and slices cannot alter future snapshots.
	binding.Manifest = cloneManifest(binding.Manifest)

	r.mu.Lock()
	defer r.mu.Unlock()
	if scopedBindingCount(r.bindings) >= registryCap(r.maxBindings) {
		return nil, fmt.Errorf("registry bindings exceed maximum of %d", registryCap(r.maxBindings))
	}
	r.next++
	order := r.next
	key := binding.Scope.String()
	r.bindings[key] = append(r.bindings[key], storedBinding{CapabilityBinding: binding, order: order})
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

func validateCapabilityManifest(manifest CapabilityManifest) error {
	if strings.TrimSpace(string(manifest.Kind)) == "" {
		return fmt.Errorf("capability %q has an empty kind", manifest.ID)
	}
	if manifest.PerTurnBudget < 0 || manifest.PerTurnBudget > HardMaxToolCalls {
		return fmt.Errorf("capability %q per-turn budget must be between 0 and %d", manifest.ID, HardMaxToolCalls)
	}
	if manifest.MaxOutputBytes < 0 || manifest.MaxOutputBytes > HardMaxCapabilityOutputBytes {
		return fmt.Errorf("capability %q max output bytes must be between 0 and %d", manifest.ID, HardMaxCapabilityOutputBytes)
	}
	if manifest.TimeoutMs < 0 || manifest.TimeoutMs > MaxCapabilityTimeoutMs {
		return fmt.Errorf("capability %q timeout must be between 0 and %d ms", manifest.ID, MaxCapabilityTimeoutMs)
	}
	seenPermissions := map[Permission]bool{}
	for _, permission := range manifest.RequiredPermissions {
		if err := ValidateNamespacedID(string(permission)); err != nil {
			return fmt.Errorf("capability %q has invalid required permission %q: %w", manifest.ID, permission, err)
		}
		if seenPermissions[permission] {
			return fmt.Errorf("capability %q repeats required permission %q", manifest.ID, permission)
		}
		seenPermissions[permission] = true
	}
	seenCredentials := map[CredentialRef]bool{}
	for _, ref := range manifest.RequiredCredentials {
		if err := validateCredentialRef(ref); err != nil {
			return fmt.Errorf("capability %q: %w", manifest.ID, err)
		}
		if seenCredentials[ref] {
			return fmt.Errorf("capability %q repeats required credential %q", manifest.ID, ref)
		}
		seenCredentials[ref] = true
	}
	if err := ValidateSchema(manifest.InputSchema); err != nil {
		return fmt.Errorf("capability %q input schema: %w", manifest.ID, err)
	}
	if err := ValidateSchema(manifest.OutputSchema); err != nil {
		return fmt.Errorf("capability %q output schema: %w", manifest.ID, err)
	}
	if manifest.Tool != nil {
		if err := ValidateSchema(manifest.Tool.Parameters); err != nil {
			return fmt.Errorf("capability %q tool schema: %w", manifest.ID, err)
		}
	}
	if manifest.Execution != nil {
		switch manifest.Execution.Sandbox.Mode {
		case "", SandboxReadOnly, SandboxWorkspaceWrite, SandboxDangerFullAccess:
		default:
			return fmt.Errorf("capability %q has unknown sandbox mode %q", manifest.ID, manifest.Execution.Sandbox.Mode)
		}
	}
	return nil
}

// Register binds an in-process capability at one scope.
func (r *CapabilityRegistry) Register(scope ScopePath, capability Capability) error {
	if capability == nil {
		return fmt.Errorf("capability is nil")
	}
	manifest, err := safeCapabilityManifest(capability)
	if err != nil {
		return err
	}
	return r.Bind(CapabilityBinding{
		Scope: scope, Mode: BindingProvide, Manifest: manifest, Provider: capability,
	})
}

// RegisterExecutor binds a declarative capability to an executor.
func (r *CapabilityRegistry) RegisterExecutor(scope ScopePath, manifest CapabilityManifest, executor Executor) error {
	if manifest.Execution == nil {
		return fmt.Errorf("capability %q has no execution spec", manifest.ID)
	}
	if executor == nil || executor.Runtime() != manifest.Execution.Runtime {
		return fmt.Errorf("capability %q requires runtime %q", manifest.ID, manifest.Execution.Runtime)
	}
	return r.Bind(CapabilityBinding{
		Scope: scope, Mode: BindingProvide, Manifest: manifest,
		Provider: ExecutorProvider{Executor: executor, Spec: *manifest.Execution},
	})
}

// CapabilityResolver resolves inherited bindings for one authenticated run.
type CapabilityResolver struct {
	Registry    *CapabilityRegistry
	Credentials CredentialResolver
}

type resolvedEntry struct {
	manifest  CapabilityManifest
	provider  any
	scope     ScopePath
	protected bool
	order     uint64
}

// Resolve creates an immutable, permission-filtered capability snapshot.
func (r CapabilityResolver) Resolve(principal Principal, target ScopePath) (*CapabilitySnapshot, error) {
	return r.resolve(principal, target, true)
}

// resolveForProfile is the runtime-only composition path: it keeps every
// scope-resolved capability long enough to distinguish an absent profile ID
// from one that is merely unavailable to this principal. Runtime filters the
// resulting profile snapshot by permissions immediately afterward. Public
// Resolve remains permission-filtered for all external callers.
func (r CapabilityResolver) resolveForProfile(principal Principal, target ScopePath) (*CapabilitySnapshot, error) {
	return r.resolve(principal, target, false)
}

func (r CapabilityResolver) resolve(principal Principal, target ScopePath, filterPermissions bool) (*CapabilitySnapshot, error) {
	if r.Registry == nil {
		return nil, fmt.Errorf("capability registry is nil")
	}
	if !principal.Scope.IsAncestorOf(target) {
		return nil, fmt.Errorf("principal scope %q does not own target scope %q", principal.Scope, target)
	}

	r.Registry.mu.RLock()
	effective, err := r.Registry.resolveEntriesLocked(target)
	r.Registry.mu.RUnlock()
	if err != nil {
		return nil, err
	}

	entries := make([]SnapshotCapability, 0, len(effective))
	providers := make(map[string]any, len(effective))
	for id, entry := range effective {
		if filterPermissions && !principal.Grants.Allows(entry.manifest.RequiredPermissions) {
			continue
		}
		entries = append(entries, SnapshotCapability{
			Manifest: cloneManifest(entry.manifest), Source: entry.scope,
			ProviderRevision: artifactRevision(entry.provider),
		})
		providers[id] = entry.provider
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Manifest.ID < entries[j].Manifest.ID })

	digest, err := snapshotDigest(target, principal, entries)
	if err != nil {
		return nil, err
	}
	return &CapabilitySnapshot{
		ID: digest, CreatedAt: time.Now().UTC(), Scope: target,
		principal: clonePrincipal(principal), capabilities: entries, providers: providers,
		credentials: r.Credentials,
	}, nil
}

// resolveEntriesLocked walks target prefixes and applies provide/replace/
// disable semantics. Callers must hold the registry read lock.
func (r *CapabilityRegistry) resolveEntriesLocked(target ScopePath) (map[string]resolvedEntry, error) {
	effective := map[string]resolvedEntry{}
	for _, prefix := range target.Prefixes() {
		bindings := append([]storedBinding(nil), r.bindings[prefix.String()]...)
		sort.SliceStable(bindings, func(i, j int) bool { return bindings[i].order < bindings[j].order })
		for _, binding := range bindings {
			id := binding.Manifest.ID
			current, exists := effective[id]
			switch binding.Mode {
			case BindingProvide:
				if exists {
					return nil, fmt.Errorf("capability %q is already provided; use replace explicitly", id)
				}
				effective[id] = resolvedEntry{binding.Manifest, binding.Provider, binding.Scope, binding.Protected, binding.order}
			case BindingReplace:
				if !exists {
					return nil, fmt.Errorf("capability %q cannot replace a missing capability", id)
				}
				if current.protected && !current.scope.Equal(binding.Scope) {
					return nil, fmt.Errorf("capability %q is protected by scope %q", id, current.scope)
				}
				effective[id] = resolvedEntry{binding.Manifest, binding.Provider, binding.Scope, binding.Protected, binding.order}
			case BindingDisable:
				if !exists {
					continue
				}
				if current.protected && !current.scope.Equal(binding.Scope) {
					return nil, fmt.Errorf("capability %q is protected by scope %q", id, current.scope)
				}
				delete(effective, id)
			}
		}
	}
	return effective, nil
}

// Entries lists the effective capability declarations visible at one target
// scope, without permission filtering or a principal. It backs catalog APIs;
// it never returns providers.
func (r *CapabilityRegistry) Entries(target ScopePath) ([]SnapshotCapability, error) {
	if target.Depth() == 0 {
		return nil, fmt.Errorf("target scope is empty")
	}
	r.mu.RLock()
	effective, err := r.resolveEntriesLocked(target)
	r.mu.RUnlock()
	if err != nil {
		return nil, err
	}
	entries := make([]SnapshotCapability, 0, len(effective))
	for _, entry := range effective {
		entries = append(entries, SnapshotCapability{
			Manifest: publicManifest(entry.manifest), Source: entry.scope,
			ProviderRevision: artifactRevision(entry.provider),
		})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Manifest.ID < entries[j].Manifest.ID })
	return entries, nil
}

// SnapshotCapability is one immutable capability selection and its source scope.
type SnapshotCapability struct {
	Manifest         CapabilityManifest `json:"manifest"`
	Source           ScopePath          `json:"source"`
	ProviderRevision string             `json:"provider_revision,omitempty"`
}

// CapabilitySnapshot is the complete capability set fixed for one run.
type CapabilitySnapshot struct {
	ID        string    `json:"id"`
	CreatedAt time.Time `json:"created_at"`
	Scope     ScopePath `json:"scope"`

	capabilities []SnapshotCapability
	providers    map[string]any
	credentials  CredentialResolver
	principal    Principal
}

// Principal returns a defensive copy of the identity frozen into this
// capability snapshot.
func (s *CapabilitySnapshot) Principal() Principal {
	if s == nil {
		return Principal{}
	}
	return clonePrincipal(s.principal)
}

func (s *CapabilitySnapshot) MarshalJSON() ([]byte, error) {
	if s == nil {
		return []byte("null"), nil
	}
	return json.Marshal(struct {
		ID           string               `json:"id"`
		CreatedAt    time.Time            `json:"created_at"`
		Scope        ScopePath            `json:"scope"`
		Principal    Principal            `json:"principal"`
		Capabilities []SnapshotCapability `json:"capabilities"`
	}{
		ID: s.ID, CreatedAt: s.CreatedAt, Scope: s.Scope,
		Principal: s.Principal(), Capabilities: s.Capabilities(),
	})
}

// Capabilities returns a defensive copy of the selected definitions.
func (s *CapabilitySnapshot) Capabilities() []SnapshotCapability {
	out := make([]SnapshotCapability, len(s.capabilities))
	for i, capability := range s.capabilities {
		out[i] = SnapshotCapability{
			Manifest: publicManifest(capability.Manifest), Source: capability.Source,
			ProviderRevision: capability.ProviderRevision,
		}
	}
	return out
}

// Schemas returns only capabilities explicitly exposed as model tools.
func (s *CapabilitySnapshot) Schemas() []ToolSchema {
	out := []ToolSchema{}
	for _, capability := range s.capabilities {
		tool := capability.Manifest.Tool
		if tool == nil {
			continue
		}
		if _, executable := s.providers[capability.Manifest.ID].(ToolProvider); !executable {
			continue
		}
		description := tool.Description
		if description == "" {
			description = capability.Manifest.Description
		}
		if description == "" {
			description = capability.Manifest.Name
		}
		parameters := cloneMap(tool.Parameters)
		if parameters == nil {
			parameters = cloneMap(capability.Manifest.InputSchema)
		}
		out = append(out, ToolSchema{Name: capability.Manifest.ID, Description: description, Parameters: parameters})
	}
	return out
}

// Authorized reports whether the snapshot contains a capability.
func (s *CapabilitySnapshot) Authorized(name string) bool {
	provider, ok := s.providers[name]
	if !ok {
		return false
	}
	_, executable := provider.(ToolProvider)
	return executable
}

// MaxCallBudget returns the strictest positive per-turn budget in the snapshot.
func (s *CapabilitySnapshot) MaxCallBudget() int {
	budget := 16
	for _, capability := range s.capabilities {
		if _, executable := s.providers[capability.Manifest.ID].(ToolProvider); !executable {
			continue
		}
		candidate := capability.Manifest.PerTurnBudget
		if candidate > 0 && candidate < budget {
			budget = candidate
		}
	}
	return budget
}

// BudgetFor returns the declared per-run budget for one capability, or zero
// when the capability is absent, not tool-executable, or declares no budget.
func (s *CapabilitySnapshot) BudgetFor(name string) int {
	manifest, ok := s.manifestInternal(name)
	if !ok {
		return 0
	}
	if _, executable := s.providers[name].(ToolProvider); !executable {
		return 0
	}
	return manifest.PerTurnBudget
}

// Execute invokes a capability from the fixed snapshot.
func (s *CapabilitySnapshot) Execute(ctx context.Context, call ToolCall) (CapabilityResult, error) {
	return s.executeProtected(ctx, call, nil)
}

// executeProtected invokes a capability while passing the active run's guard
// funnel to composite providers. Direct snapshot callers intentionally receive
// no invoker, so a composite capability fails closed instead of bypassing the
// Agent's run-scoped guards.
func (s *CapabilitySnapshot) executeProtected(
	ctx context.Context,
	call ToolCall,
	invoker ProtectedToolInvoker,
) (CapabilityResult, error) {
	if err := validateToolCall(call); err != nil {
		return CapabilityResult{
			Content: err.Error(), OK: false,
			Metadata: map[string]any{"code": "invalid_tool_call"},
		}, nil
	}
	providerValue, ok := s.providers[call.Name]
	if !ok {
		return CapabilityResult{Content: "capability is not available in this run", OK: false}, nil
	}
	provider, ok := providerValue.(ToolProvider)
	if !ok {
		return CapabilityResult{Content: "capability is not executable as a tool", OK: false}, nil
	}
	manifest, ok := s.manifestInternal(call.Name)
	if !ok {
		return CapabilityResult{Content: "capability manifest is missing", OK: false}, nil
	}
	if manifest.TimeoutMs > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(manifest.TimeoutMs)*time.Millisecond)
		defer cancel()
	}
	policy := SandboxPolicy{Mode: SandboxReadOnly}
	if manifest.Execution != nil {
		policy = manifest.Execution.Sandbox
	}
	credentials, err := resolveCallCredentials(
		ctx, s.credentials, s.principal, s.Scope, manifest.RequiredCredentials,
	)
	if err != nil {
		return CapabilityResult{
			Content: "required credential is unavailable", OK: false,
			Metadata: map[string]any{"code": "credential_unavailable"},
		}, nil
	}
	args, cloneErr := cloneCallArgs(call.Args)
	if cloneErr != nil {
		return CapabilityResult{
			Content: cloneErr.Error(), OK: false,
			Metadata: map[string]any{"code": CodeInvalidArgs},
		}, nil
	}
	invocation, remainingToolCalls, compositionRevision := capabilityInvocationFromContext(ctx)
	result, err := safeToolProviderExecute(provider, ctx, CapabilityRequest{
		CallID: call.ID,
		IdempotencyKey: func() string {
			if manifest.Idempotent {
				return call.ID
			}
			return ""
		}(),
		Args: args,
		Context: CapabilityContext{
			Principal: clonePrincipal(s.principal), Scope: s.Scope,
			CapabilityID:        call.Name,
			CompositionRevision: compositionRevision,
			Policy:              policy, Credentials: credentials, Invoker: invoker,
			Invocation: invocation, RemainingToolCalls: remainingToolCalls,
			Accepted: acceptedInvocationFromContext(ctx),
		},
	})
	outputLimit := manifest.MaxOutputBytes
	if outputLimit == 0 {
		outputLimit = DefaultMaxCapabilityOutputBytes
	}
	if len(result.Content) > outputLimit {
		return CapabilityResult{
			Content: fmt.Sprintf("capability %s output exceeded %d bytes", call.Name, outputLimit),
			OK:      false, Metadata: map[string]any{"code": "capability_output_too_large"},
		}, nil
	}
	if metadataErr := validateResultMetadata(result.Metadata); metadataErr != nil {
		return invalidCapabilityOutput(call.Name, metadataErr.Error()), nil
	}
	if err != nil || !result.OK || len(manifest.OutputSchema) == 0 {
		return result, err
	}
	var output any
	if decodeErr := json.Unmarshal([]byte(result.Content), &output); decodeErr != nil {
		return invalidCapabilityOutput(call.Name, fmt.Sprintf("result is not valid JSON: %v", decodeErr)), nil
	}
	if validationErr := ValidateJSONValue(manifest.OutputSchema, output); validationErr != nil {
		return invalidCapabilityOutput(call.Name, validationErr.Error()), nil
	}
	return result, nil
}

func (s *CapabilitySnapshot) manifestInternal(id string) (CapabilityManifest, bool) {
	for _, capability := range s.capabilities {
		if capability.Manifest.ID == id {
			return cloneManifest(capability.Manifest), true
		}
	}
	return CapabilityManifest{}, false
}

// ManifestFor returns the frozen manifest for one capability in this snapshot.
func (s *CapabilitySnapshot) ManifestFor(id string) (CapabilityManifest, bool) {
	manifest, ok := s.manifestInternal(id)
	if !ok {
		return CapabilityManifest{}, false
	}
	return publicManifest(manifest), true
}

// Provider returns the provider selected for a non-tool capability consumer.
// Callers must assert the interface declared by the capability contract.
func (s *CapabilitySnapshot) Provider(name string) (any, bool) {
	provider, ok := s.providers[name]
	if !ok {
		return nil, false
	}
	if _, executable := provider.(ToolProvider); executable {
		return nil, false
	}
	return provider, true
}

// Select returns capabilities matching a kind and optional contract.
func (s *CapabilitySnapshot) Select(kind CapabilityKind, contract string) []SnapshotCapability {
	out := []SnapshotCapability{}
	for _, capability := range s.capabilities {
		if capability.Manifest.Kind != kind {
			continue
		}
		if contract != "" && capability.Manifest.Contract != contract {
			continue
		}
		out = append(out, SnapshotCapability{
			Manifest: publicManifest(capability.Manifest), Source: capability.Source,
			ProviderRevision: capability.ProviderRevision,
		})
	}
	return out
}

func validateCapabilityID(id string) error {
	return ValidateNamespacedID(id)
}

func artifactRevision(value any) string {
	label := artifactRevisionLabel(value)
	if label == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(label))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func safeCapabilityManifest(capability Capability) (manifest CapabilityManifest, err error) {
	defer func() {
		if recover() != nil {
			err = fmt.Errorf("capability manifest panicked")
		}
	}()
	return capability.Manifest(), nil
}

func cloneCallArgs(args map[string]any) (map[string]any, error) {
	if args == nil {
		return nil, nil
	}
	data, err := json.Marshal(args)
	if err != nil {
		return nil, fmt.Errorf("tool arguments are not JSON serializable: %w", err)
	}
	if len(data) > MaxToolArgumentBytes {
		return nil, fmt.Errorf("tool arguments exceed %d bytes", MaxToolArgumentBytes)
	}
	if cloned, ok := cloneJSONNative(args); ok {
		return cloned.(map[string]any), nil
	}
	var out map[string]any
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, fmt.Errorf("decode cloned tool arguments: %w", err)
	}
	return out, nil
}

func validateResultMetadata(metadata map[string]any) error {
	if metadata == nil {
		return nil
	}
	encoded, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("result metadata is not JSON serializable: %w", err)
	}
	if len(encoded) > MaxCapabilityMetadataBytes {
		return fmt.Errorf("result metadata exceeds %d bytes", MaxCapabilityMetadataBytes)
	}
	return nil
}

// ValidateCapabilityResult applies the provider-neutral hard limits required
// before a result crosses a durable adapter boundary.
func ValidateCapabilityResult(result CapabilityResult) error {
	if len(result.Content) > HardMaxCapabilityOutputBytes {
		return fmt.Errorf("capability result exceeds hard output limit %d", HardMaxCapabilityOutputBytes)
	}
	return validateResultMetadata(result.Metadata)
}

func artifactRevisionLabel(value any) (revision string) {
	revisioner, ok := value.(ArtifactRevisioner)
	if !ok {
		return ""
	}
	defer func() {
		if recover() != nil {
			revision = ""
		}
	}()
	revision = strings.TrimSpace(revisioner.ArtifactRevision())
	if len(revision) > 4096 {
		revision = revision[:4096]
	}
	return revision
}

func safeToolProviderExecute(provider ToolProvider, ctx context.Context, request CapabilityRequest) (result CapabilityResult, err error) {
	defer func() {
		if recover() != nil {
			result = CapabilityResult{
				Content: "capability provider panicked", OK: false,
				Metadata: map[string]any{"code": "capability_provider_panic"},
			}
			err = nil
		}
	}()
	return provider.Execute(ctx, request)
}

func invalidCapabilityOutput(name, message string) CapabilityResult {
	return CapabilityResult{
		Content: fmt.Sprintf("capability %s returned invalid output: %s", name, message),
		OK:      false, Metadata: map[string]any{"code": "invalid_capability_output"},
	}
}

func snapshotDigest(scope ScopePath, principal Principal, capabilities []SnapshotCapability) (string, error) {
	value := struct {
		Scope        ScopePath
		Subject      string
		Capabilities []SnapshotCapability
	}{scope, principal.SubjectID, capabilities}
	data, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("encode capability snapshot: %w", err)
	}
	if len(data) > MaxCapabilitySnapshotBytes {
		return "", fmt.Errorf("capability snapshot exceeds %d bytes", MaxCapabilitySnapshotBytes)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func clonePrincipal(principal Principal) Principal {
	out := principal
	out.Grants = principal.Grants.Clone()
	out.Attributes = map[string]string{}
	for key, value := range principal.Attributes {
		out.Attributes[key] = value
	}
	return out
}

func cloneManifest(manifest CapabilityManifest) CapabilityManifest {
	out := manifest
	out.RequiredPermissions = append([]Permission(nil), manifest.RequiredPermissions...)
	out.RequiredCredentials = append([]CredentialRef(nil), manifest.RequiredCredentials...)
	out.InputSchema = cloneMap(manifest.InputSchema)
	out.OutputSchema = cloneMap(manifest.OutputSchema)
	out.Metadata = map[string]string{}
	for key, value := range manifest.Metadata {
		out.Metadata[key] = value
	}
	if manifest.Tool != nil {
		tool := *manifest.Tool
		tool.Parameters = cloneMap(manifest.Tool.Parameters)
		out.Tool = &tool
	}
	if manifest.Execution != nil {
		execution := *manifest.Execution
		if manifest.Execution.Headers != nil {
			execution.Headers = make(map[string]string, len(manifest.Execution.Headers))
			for key, value := range manifest.Execution.Headers {
				execution.Headers[key] = value
			}
		}
		out.Execution = &execution
	}
	return out
}

func publicManifest(manifest CapabilityManifest) CapabilityManifest {
	out := cloneManifest(manifest)
	out.Metadata = redactStringMetadata(out.Metadata)
	if out.Execution != nil {
		for key, value := range out.Execution.Headers {
			if !strings.HasPrefix(value, "$credential:") {
				out.Execution.Headers[key] = "[redacted]"
			}
		}
	}
	return out
}

func cloneMap(input map[string]any) map[string]any {
	if input == nil {
		return nil
	}
	if cloned, ok := cloneJSONNative(input); ok {
		return cloned.(map[string]any)
	}
	data, err := json.Marshal(input)
	if err != nil {
		return nil
	}
	var out map[string]any
	if err := json.Unmarshal(data, &out); err != nil {
		return nil
	}
	return out
}

const cloneJSONNativeMaxDepth = 128

type cloneJSONNativeVisit struct {
	kind byte
	ptr  uintptr
}

// cloneJSONNative owns only trees whose behavior is provably equivalent to the
// default JSON interface round-trip. Anything uncertain uses the compatibility
// Marshal/Unmarshal path at the caller. The active-path set and depth bound
// ensure hostile cycles/depth never recurse without a limit.
func cloneJSONNative(value any) (any, bool) {
	return cloneJSONNativeValue(value, 0, make(map[cloneJSONNativeVisit]struct{}))
}

func cloneJSONNativeValue(value any, depth int, active map[cloneJSONNativeVisit]struct{}) (any, bool) {
	if depth > cloneJSONNativeMaxDepth {
		return nil, false
	}
	switch value := value.(type) {
	case nil:
		return nil, true
	case bool:
		return value, true
	case string:
		if !utf8.ValidString(value) {
			return nil, false
		}
		return value, true
	case float64:
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return nil, false
		}
		return value, true
	case json.Number:
		if !validJSONNumberLiteral(string(value)) {
			return nil, false
		}
		var number float64
		if err := json.Unmarshal([]byte(value), &number); err != nil {
			return nil, false
		}
		return number, true
	case []any:
		if value == nil {
			return nil, false
		}
		visit := cloneJSONNativeVisit{kind: 's', ptr: reflect.ValueOf(value).Pointer()}
		if _, exists := active[visit]; exists {
			return nil, false
		}
		active[visit] = struct{}{}
		defer delete(active, visit)
		out := make([]any, len(value))
		for index, item := range value {
			cloned, ok := cloneJSONNativeValue(item, depth+1, active)
			if !ok {
				return nil, false
			}
			out[index] = cloned
		}
		return out, true
	case map[string]any:
		if value == nil {
			return nil, false
		}
		visit := cloneJSONNativeVisit{kind: 'm', ptr: reflect.ValueOf(value).Pointer()}
		if _, exists := active[visit]; exists {
			return nil, false
		}
		active[visit] = struct{}{}
		defer delete(active, visit)
		out := make(map[string]any, len(value))
		for key, item := range value {
			if !utf8.ValidString(key) {
				return nil, false
			}
			cloned, ok := cloneJSONNativeValue(item, depth+1, active)
			if !ok {
				return nil, false
			}
			out[key] = cloned
		}
		return out, true
	default:
		return nil, false
	}
}

func validJSONNumberLiteral(value string) bool {
	if value == "" {
		return false
	}
	index := 0
	if value[index] == '-' {
		index++
		if index == len(value) {
			return false
		}
	}
	if value[index] == '0' {
		index++
	} else {
		if value[index] < '1' || value[index] > '9' {
			return false
		}
		for index < len(value) && value[index] >= '0' && value[index] <= '9' {
			index++
		}
	}
	if index < len(value) && value[index] == '.' {
		index++
		fractionStart := index
		for index < len(value) && value[index] >= '0' && value[index] <= '9' {
			index++
		}
		if fractionStart == index {
			return false
		}
	}
	if index < len(value) && (value[index] == 'e' || value[index] == 'E') {
		index++
		if index < len(value) && (value[index] == '+' || value[index] == '-') {
			index++
		}
		exponentStart := index
		for index < len(value) && value[index] >= '0' && value[index] <= '9' {
			index++
		}
		if exponentStart == index {
			return false
		}
	}
	return index == len(value)
}

func registryCap(max int) int {
	if max > 0 {
		return max
	}
	return MaxRegistryBindings
}

func scopedBindingCount[T any](items map[string][]T) int {
	count := 0
	for _, list := range items {
		count += len(list)
	}
	return count
}
