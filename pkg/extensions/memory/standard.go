package memory

import (
	"context"
	"errors"
	"fmt"
	"math"

	core "github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/extensions/internal/support"
)

const (
	RecallCapabilityID   = "memory.recall"
	RememberCapabilityID = "memory.remember"
	ForgetCapabilityID   = "memory.forget"
)

var errMemoryStore = errors.New("memory store failed")

type standardAction uint8

const (
	standardRecall standardAction = iota + 1
	standardRemember
	standardForget
)

// NewStandardCapabilities returns separately composable memory actions. Unlike
// NewCapability, each action has one permission/approval contract and one
// closed input schema. The returned values share the supplied Store only.
func NewStandardCapabilities(store Store) ([]core.Capability, error) {
	if store == nil {
		return nil, fmt.Errorf("standard memory capabilities require a store")
	}
	return []core.Capability{
		&standardCapability{store: store, id: RecallCapabilityID, action: standardRecall},
		&standardCapability{store: store, id: RememberCapabilityID, action: standardRemember},
		&standardCapability{store: store, id: ForgetCapabilityID, action: standardForget},
	}, nil
}

type standardCapability struct {
	store  Store
	id     string
	action standardAction
}

func (*standardCapability) ArtifactRevision() string { return "memory-standard-capability/v1" }

func (m *standardCapability) Manifest() core.CapabilityManifest {
	manifest := core.CapabilityManifest{
		ID: m.id, Version: "1.0.0", Name: m.id, Kind: core.KindMemory, Contract: ContractV1,
		Tool: &core.ToolExposure{Parameters: standardSchema(m.action)},
	}
	switch m.action {
	case standardRecall:
		manifest.Description = "Recall relevant remembered facts for the current principal scope."
		manifest.RequiredPermissions = []core.Permission{core.PermRead}
		manifest.Idempotent = true
	case standardRemember:
		manifest.Description = "Remember or replace one fact for the current principal scope."
		manifest.RequiredPermissions = []core.Permission{core.PermWrite}
		// Replacing by key is not invocation idempotency: a retried call can
		// receive a new entry ID and timestamp, so do not claim it is idempotent.
	case standardForget:
		manifest.Description = "Forget one remembered fact from the current principal scope."
		manifest.RequiredPermissions = []core.Permission{core.PermWrite}
		manifest.RequiresApproval = true
	}
	return manifest
}

func standardSchema(action standardAction) map[string]any {
	properties := map[string]any{}
	required := []any{}
	switch action {
	case standardRecall:
		properties["query"] = map[string]any{"type": "string", "maxLength": MaxQueryRunes}
		properties["tags"] = tagsSchema()
		properties["limit"] = map[string]any{"type": "integer", "minimum": 1, "maximum": MaxRecallLimit}
		required = append(required, "query")
	case standardRemember:
		properties["key"] = map[string]any{"type": "string", "maxLength": MaxEntryKeyBytes}
		properties["content"] = map[string]any{"type": "string", "maxLength": MaxEntryContentBytes}
		properties["tags"] = tagsSchema()
		required = append(required, "key", "content")
	case standardForget:
		properties["id"] = map[string]any{"type": "string", "maxLength": MaxEntryIDBytes}
		required = append(required, "id")
	}
	return map[string]any{"type": "object", "additionalProperties": false, "properties": properties, "required": required}
}

func tagsSchema() map[string]any {
	return map[string]any{"type": "array", "maxItems": MaxRecallTags, "items": map[string]any{"type": "string", "maxLength": MaxTagRunes}}
}

func (m *standardCapability) Execute(ctx context.Context, request core.CapabilityRequest) (core.CapabilityResult, error) {
	if err := core.RequireAcceptedInvocation(request); err != nil {
		return core.CapabilityResult{}, err
	}
	if m == nil || m.store == nil || request.Context.CapabilityID != m.id {
		return core.CapabilityResult{}, fmt.Errorf("%w: capability binding", errMemoryStore)
	}
	scope := request.Context.Principal.Scope
	if err := ValidateScope(scope); err != nil {
		return invalidStandardArgs(), nil
	}
	switch m.action {
	case standardRecall:
		query, tags, limit, ok := parseRecallArgs(request.Args)
		if !ok {
			return invalidStandardArgs(), nil
		}
		entries, err := safeRecall(m.store, ctx, scope, query, tags, limit)
		if err != nil {
			return storeFailure("memory_recall_failed"), nil
		}
		return support.JSONResult(map[string]any{"entries": entries})
	case standardRemember:
		entry, ok := parseRememberArgs(request.Args)
		if !ok {
			return invalidStandardArgs(), nil
		}
		remembered, err := safeRemember(m.store, ctx, scope, entry)
		if err != nil {
			return storeFailure("memory_remember_failed"), nil
		}
		return support.JSONResult(map[string]any{"remembered": remembered.ID, "key": remembered.Key})
	case standardForget:
		id, ok := parseForgetArgs(request.Args)
		if !ok {
			return invalidStandardArgs(), nil
		}
		if err := safeForget(m.store, ctx, scope, id); err != nil {
			return storeFailure("memory_forget_failed"), nil
		}
		return support.JSONResult(map[string]any{"forgotten": id})
	default:
		return core.CapabilityResult{}, fmt.Errorf("%w: unknown action", errMemoryStore)
	}
}

func invalidStandardArgs() core.CapabilityResult {
	return support.DeniedResult(core.CodeInvalidArgs, "invalid memory arguments")
}

func storeFailure(code string) core.CapabilityResult {
	return support.DeniedResult(code, "memory store operation failed")
}

func parseRecallArgs(args map[string]any) (string, []string, int, bool) {
	if !hasOnly(args, "query", "tags", "limit") {
		return "", nil, 0, false
	}
	query, ok := args["query"].(string)
	if !ok {
		return "", nil, 0, false
	}
	tags, ok := parseTags(args["tags"])
	if !ok {
		return "", nil, 0, false
	}
	limit := 10
	if raw, exists := args["limit"]; exists {
		value, ok := integer(raw)
		if !ok || value <= 0 {
			return "", nil, 0, false
		}
		limit = value
	}
	if ValidateRecall(query, tags, limit) != nil {
		return "", nil, 0, false
	}
	return query, tags, limit, true
}

func parseRememberArgs(args map[string]any) (Entry, bool) {
	if !hasOnly(args, "key", "content", "tags") {
		return Entry{}, false
	}
	key, keyOK := args["key"].(string)
	content, contentOK := args["content"].(string)
	tags, tagsOK := parseTags(args["tags"])
	entry := Entry{Key: key, Content: content, Tags: tags}
	return entry, keyOK && contentOK && tagsOK && ValidateEntry(entry) == nil
}

func parseForgetArgs(args map[string]any) (string, bool) {
	if !hasOnly(args, "id") {
		return "", false
	}
	id, ok := args["id"].(string)
	return id, ok && ValidateForgetID(id) == nil
}

func hasOnly(args map[string]any, allowed ...string) bool {
	if args == nil {
		return false
	}
	for key := range args {
		found := false
		for _, candidate := range allowed {
			if key == candidate {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func parseTags(value any) ([]string, bool) {
	if value == nil {
		return nil, true
	}
	values, ok := value.([]any)
	if !ok {
		return nil, false
	}
	if len(values) > MaxRecallTags {
		return nil, false
	}
	tags := make([]string, len(values))
	for i, value := range values {
		tag, ok := value.(string)
		if !ok {
			return nil, false
		}
		tags[i] = tag
	}
	return tags, ValidateTags(tags) == nil
}

func integer(value any) (int, bool) {
	switch number := value.(type) {
	case int:
		return number, true
	case float64:
		if math.IsNaN(number) || math.IsInf(number, 0) || math.Trunc(number) != number || number > math.MaxInt || number < math.MinInt {
			return 0, false
		}
		return int(number), true
	default:
		return 0, false
	}
}

func safeRemember(store Store, ctx context.Context, scope core.ScopePath, entry Entry) (out Entry, err error) {
	defer func() {
		if recover() != nil {
			out, err = Entry{}, errMemoryStore
		}
	}()
	out, err = store.Remember(ctx, scope, entry)
	if err != nil {
		return Entry{}, errMemoryStore
	}
	return out, nil
}

func safeRecall(store Store, ctx context.Context, scope core.ScopePath, query string, tags []string, limit int) (out []Entry, err error) {
	defer func() {
		if recover() != nil {
			out, err = nil, errMemoryStore
		}
	}()
	out, err = store.Recall(ctx, scope, query, tags, limit)
	if err != nil {
		return nil, errMemoryStore
	}
	return out, nil
}

func safeForget(store Store, ctx context.Context, scope core.ScopePath, id string) (err error) {
	defer func() {
		if recover() != nil {
			err = errMemoryStore
		}
	}()
	if err = store.Forget(ctx, scope, id); err != nil {
		return errMemoryStore
	}
	return nil
}
