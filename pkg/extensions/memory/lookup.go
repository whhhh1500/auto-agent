package memory

import (
	"context"
	"fmt"

	core "github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/extensions/internal/support"
)

const LookupCapabilityID = "memory.lookup"

// NewLookupCapability returns the opt-in exact-key memory capability. It is not
// part of NewStandardCapabilities so hosts choose whether to expose it.
func NewLookupCapability(store KeyStore) (core.Capability, error) {
	if store == nil {
		return nil, fmt.Errorf("memory lookup capability requires a key store")
	}
	return &lookupCapability{store: store}, nil
}

type lookupCapability struct {
	store KeyStore
}

func (*lookupCapability) ArtifactRevision() string { return "memory-lookup/v1" }

func (*lookupCapability) Manifest() core.CapabilityManifest {
	return core.CapabilityManifest{
		ID:                  LookupCapabilityID,
		Version:             "1.0.0",
		Name:                LookupCapabilityID,
		Description:         "Look up one remembered fact by its exact canonical key for the current principal scope. Copy the key exactly, including punctuation and case; this does not search content or normalize separators. The result always has found and entry; entry is null when found is false.",
		Kind:                core.KindMemory,
		Contract:            ContractV1,
		RequiredPermissions: []core.Permission{core.PermRead},
		Idempotent:          true,
		Tool:                &core.ToolExposure{Parameters: lookupInputSchema()},
		OutputSchema:        lookupOutputSchema(),
	}
}

func lookupInputSchema() map[string]any {
	return map[string]any{
		"type": "object", "additionalProperties": false,
		"properties": map[string]any{
			"key": map[string]any{
				"type": "string", "maxLength": MaxEntryKeyBytes,
				"description": "Canonical memory key. Copy it exactly, including punctuation and case; no content search or separator normalization is performed. The runtime also enforces a 1024-byte key limit.",
			},
		},
		"required": []any{"key"},
	}
}

// lookupOutputSchema uses a fixed nullable entry field. The core schema subset
// cannot express a found-dependent required property, so Execute and its tests
// enforce found:false with entry:null and found:true with a valid entry.
func lookupOutputSchema() map[string]any {
	entry := map[string]any{
		"type": "object", "additionalProperties": false,
		"properties": map[string]any{
			"id":         map[string]any{"type": "string"},
			"key":        map[string]any{"type": "string"},
			"content":    map[string]any{"type": "string"},
			"tags":       map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
			"created_at": map[string]any{"type": "string"},
		},
		"required": []any{"id", "key", "content", "created_at"},
	}
	return map[string]any{
		"type": "object", "additionalProperties": false,
		"properties": map[string]any{
			"found": map[string]any{"type": "boolean"},
			"entry": map[string]any{"type": []any{"object", "null"}, "properties": entry["properties"], "required": entry["required"], "additionalProperties": false},
		},
		"required": []any{"found", "entry"},
	}
}

func (m *lookupCapability) Execute(ctx context.Context, request core.CapabilityRequest) (core.CapabilityResult, error) {
	if err := core.RequireAcceptedInvocation(request); err != nil {
		return core.CapabilityResult{}, err
	}
	if m == nil || m.store == nil || request.Context.CapabilityID != LookupCapabilityID {
		return core.CapabilityResult{}, fmt.Errorf("%w: capability binding", errMemoryStore)
	}
	if err := ValidateScope(request.Context.Principal.Scope); err != nil {
		return invalidStandardArgs(), nil
	}
	key, ok := parseLookupArgs(request.Args)
	if !ok {
		return invalidStandardArgs(), nil
	}
	entry, found, err := safeLookup(m.store, ctx, request.Context.Principal.Scope, key)
	if err != nil {
		return storeFailure("memory_lookup_failed"), nil
	}
	if !found {
		return support.JSONResult(map[string]any{"found": false, "entry": nil})
	}
	if entry.Key != key || ValidateEntry(entry) != nil {
		return storeFailure("memory_lookup_failed"), nil
	}
	return support.JSONResult(map[string]any{"found": true, "entry": entry})
}

func parseLookupArgs(args map[string]any) (string, bool) {
	if !hasOnly(args, "key") {
		return "", false
	}
	key, ok := args["key"].(string)
	return key, ok && ValidateLookupKey(key) == nil
}

func safeLookup(store KeyStore, ctx context.Context, scope core.ScopePath, key string) (entry Entry, found bool, err error) {
	defer func() {
		if recover() != nil {
			entry, found, err = Entry{}, false, errMemoryStore
		}
	}()
	entry, found, err = store.Lookup(ctx, scope, key)
	if err != nil {
		return Entry{}, false, errMemoryStore
	}
	return entry, found, nil
}
