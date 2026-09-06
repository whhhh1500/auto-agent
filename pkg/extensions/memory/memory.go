// Package memory provides an optional user-memory capability for the core
// runtime. It owns the memory contract, reference store, and tool adapter.
package memory

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
	"github.com/cc-auto-agent/harness-core/pkg/extensions/internal/support"
)

const ContractV1 = "harness.memory/v1"

const (
	MaxEntryKeyBytes     = 1024
	MaxEntryContentBytes = 64 << 10
	MaxEntryIDBytes      = 256
	MaxQueryRunes        = 4096
	MaxTagRunes          = 256
	MaxRecallTags        = 64
	MaxRecallLimit       = 100
	MaxEntryTags         = MaxRecallTags
	MaxEntriesPerScope   = 1024
	MaxScopes            = 4096
)

type Entry = core.MemoryEntry

// Store persists memory entries under one exact ownership scope.
type Store interface {
	Remember(ctx context.Context, scope core.ScopePath, entry Entry) (Entry, error)
	Recall(ctx context.Context, scope core.ScopePath, query string, tags []string, limit int) ([]Entry, error)
	Forget(ctx context.Context, scope core.ScopePath, id string) error
}

// SliceStore is the dependency-free in-process reference implementation.
type SliceStore struct {
	mu         sync.RWMutex
	scope      map[string][]Entry
	next       int64
	maxEntries int
	maxScopes  int
}

func NewSliceStore() *SliceStore {
	return &SliceStore{scope: map[string][]Entry{}}
}

// ValidateScope validates the ownership scope required by memory operations.
func ValidateScope(scope core.ScopePath) error {
	if scope.Depth() == 0 {
		return fmt.Errorf("memory scope is empty")
	}
	return nil
}

// ValidateEntryID validates an optional memory entry ID.
func ValidateEntryID(id string) error {
	if len(id) > MaxEntryIDBytes {
		return fmt.Errorf("memory entry id exceeds maximum of %d bytes", MaxEntryIDBytes)
	}
	for _, char := range id {
		if unicode.IsControl(char) {
			return fmt.Errorf("memory entry id contains a control character")
		}
	}
	return nil
}

// ValidateForgetID validates the required memory entry ID used by forget.
func ValidateForgetID(id string) error {
	if strings.TrimSpace(id) == "" {
		return fmt.Errorf("memory entry id is empty")
	}
	return ValidateEntryID(id)
}

// ValidateEntry validates a memory entry before persistence.
func ValidateEntry(entry Entry) error {
	if strings.TrimSpace(entry.Key) == "" || strings.TrimSpace(entry.Content) == "" {
		return fmt.Errorf("memory entry requires key and content")
	}
	if len(entry.Key) > MaxEntryKeyBytes || len(entry.Content) > MaxEntryContentBytes {
		return fmt.Errorf("memory entry exceeds key or content limits")
	}
	if strings.ContainsRune(entry.Key, '\x00') || strings.ContainsRune(entry.Content, '\x00') {
		return fmt.Errorf("memory entry key or content contains NUL")
	}
	if err := ValidateEntryID(entry.ID); err != nil {
		return err
	}
	return ValidateTags(entry.Tags)
}

// ValidateTags validates the bounded tag filters shared by memory writes and recalls.
func ValidateTags(tags []string) error {
	if len(tags) > MaxRecallTags {
		return fmt.Errorf("memory tags exceed maximum of %d", MaxRecallTags)
	}
	for index, tag := range tags {
		if utf8.RuneCountInString(tag) > MaxTagRunes {
			return fmt.Errorf("memory tag %d exceeds maximum of %d runes", index, MaxTagRunes)
		}
		if strings.ContainsRune(tag, '\x00') {
			return fmt.Errorf("memory tag %d contains NUL", index)
		}
	}
	return nil
}

// ValidateRecall validates the bounded query contract for memory recalls.
func ValidateRecall(query string, tags []string, limit int) error {
	if utf8.RuneCountInString(query) > MaxQueryRunes {
		return fmt.Errorf("memory query exceeds maximum of %d runes", MaxQueryRunes)
	}
	if strings.ContainsRune(query, '\x00') {
		return fmt.Errorf("memory query contains NUL")
	}
	if err := ValidateTags(tags); err != nil {
		return err
	}
	if limit < 0 || limit > MaxRecallLimit {
		return fmt.Errorf("memory recall limit must be between 0 and %d", MaxRecallLimit)
	}
	return nil
}

func (s *SliceStore) Remember(_ context.Context, scope core.ScopePath, entry Entry) (Entry, error) {
	if err := ValidateScope(scope); err != nil {
		return Entry{}, err
	}
	if err := ValidateEntry(entry); err != nil {
		return Entry{}, err
	}
	entry.Tags = append([]string(nil), entry.Tags...)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.next++
	if entry.ID == "" {
		entry.ID = fmt.Sprintf("mem_%d", s.next)
	}
	entry.CreatedAt = time.Now().UTC()
	key := scope.String()
	if _, exists := s.scope[key]; !exists && len(s.scope) >= s.scopeCap() {
		return Entry{}, fmt.Errorf("memory store exceeds maximum of %d scopes", s.scopeCap())
	}
	entries := s.scope[key]
	replaced := false
	for i, existing := range entries {
		if existing.Key == entry.Key {
			entries = append(entries[:i], entries[i+1:]...)
			replaced = true
			break
		}
	}
	if !replaced && len(entries) >= s.entryCap() {
		return Entry{}, fmt.Errorf("memory scope exceeds maximum of %d entries", s.entryCap())
	}
	s.scope[key] = append(entries, entry)
	return cloneEntry(entry), nil
}

func (s *SliceStore) entryCap() int {
	if s != nil && s.maxEntries > 0 {
		return s.maxEntries
	}
	return MaxEntriesPerScope
}

func (s *SliceStore) scopeCap() int {
	if s != nil && s.maxScopes > 0 {
		return s.maxScopes
	}
	return MaxScopes
}

func (s *SliceStore) Recall(_ context.Context, scope core.ScopePath, query string, tags []string, limit int) ([]Entry, error) {
	if err := ValidateScope(scope); err != nil {
		return nil, err
	}
	if err := ValidateRecall(query, tags, limit); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	needle := strings.ToLower(strings.TrimSpace(query))
	wantTags := map[string]bool{}
	for _, tag := range tags {
		wantTags[tag] = true
	}
	out := []Entry{}
	for _, entry := range s.scope[scope.String()] {
		if needle != "" &&
			!strings.Contains(strings.ToLower(entry.Key), needle) &&
			!strings.Contains(strings.ToLower(entry.Content), needle) {
			continue
		}
		if len(wantTags) > 0 {
			match := false
			for _, tag := range entry.Tags {
				if wantTags[tag] {
					match = true
					break
				}
			}
			if !match {
				continue
			}
		}
		out = append(out, cloneEntry(entry))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func cloneEntry(entry Entry) Entry {
	entry.Tags = append([]string(nil), entry.Tags...)
	return entry
}

func (s *SliceStore) Forget(_ context.Context, scope core.ScopePath, id string) error {
	if err := ValidateScope(scope); err != nil {
		return err
	}
	if err := ValidateForgetID(id); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := scope.String()
	for i, entry := range s.scope[key] {
		if entry.ID == id {
			s.scope[key] = append(s.scope[key][:i], s.scope[key][i+1:]...)
			if len(s.scope[key]) == 0 {
				delete(s.scope, key)
			}
			return nil
		}
	}
	return fmt.Errorf("memory entry %q not found", id)
}

// Capability exposes remember, recall, and forget as one model tool.
type Capability struct {
	store Store
	id    string
}

func (*Capability) ArtifactRevision() string { return "memory-capability/v3" }

func NewCapability(id string, store Store) (*Capability, error) {
	if err := support.ValidateCapabilityID(id); err != nil {
		return nil, err
	}
	if store == nil {
		return nil, fmt.Errorf("memory capability %q requires a store", id)
	}
	return &Capability{store: store, id: id}, nil
}

func (m *Capability) Manifest() core.CapabilityManifest {
	parameters := support.ObjectSchema(map[string]any{
		"action":  map[string]any{"type": "string", "enum": []any{"remember", "recall", "forget"}},
		"key":     map[string]any{"type": "string", "description": "Short stable label (remember)."},
		"content": map[string]any{"type": "string", "description": "Fact to remember (remember)."},
		"tags": map[string]any{
			"type": "array", "maxItems": MaxRecallTags,
			"items": map[string]any{"type": "string", "maxLength": MaxTagRunes},
		},
		"query": map[string]any{
			"type": "string", "maxLength": MaxQueryRunes,
			"description": "Recall filter over key and content.",
		},
		"id": map[string]any{"type": "string", "maxLength": MaxEntryIDBytes, "description": "Entry id (forget)."},
	})
	// Action is the only unconditional requirement: the required fields for the
	// remaining operations depend on which action the caller selected.
	parameters["required"] = []any{"action"}
	return core.CapabilityManifest{
		ID: m.id, Version: "1.0.0", Name: m.id,
		Description:         "Remember, recall, or forget facts for this user across sessions.",
		Kind:                core.KindMemory,
		Contract:            ContractV1,
		RequiredPermissions: []core.Permission{core.PermWrite},
		Tool:                &core.ToolExposure{Parameters: parameters},
	}
}

func (m *Capability) Execute(ctx context.Context, request core.CapabilityRequest) (core.CapabilityResult, error) {
	scope := request.Context.Principal.Scope
	action, _ := request.Args["action"].(string)
	switch action {
	case "remember":
		key, _ := request.Args["key"].(string)
		content, _ := request.Args["content"].(string)
		entry, err := m.store.Remember(ctx, scope, Entry{Key: key, Content: content, Tags: support.StringSlice(request.Args["tags"])})
		if err != nil {
			return support.DeniedResult(core.CodeInvalidArgs, err.Error()), nil
		}
		return support.JSONResult(map[string]any{"remembered": entry.ID, "key": entry.Key})
	case "recall":
		query, _ := request.Args["query"].(string)
		entries, err := m.store.Recall(ctx, scope, query, support.StringSlice(request.Args["tags"]), 10)
		if err != nil {
			return support.DeniedResult("memory_recall_failed", err.Error()), nil
		}
		return support.JSONResult(map[string]any{"entries": entries})
	case "forget":
		id, _ := request.Args["id"].(string)
		if err := m.store.Forget(ctx, scope, id); err != nil {
			return support.DeniedResult("memory_forget_failed", err.Error()), nil
		}
		return support.JSONResult(map[string]any{"forgotten": id})
	default:
		return support.DeniedResult(core.CodeInvalidArgs, fmt.Sprintf("unknown memory action %q", action)), nil
	}
}
