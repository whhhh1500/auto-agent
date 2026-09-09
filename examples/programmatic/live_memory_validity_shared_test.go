package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	appcontextassembly "github.com/whhhh1500/auto-agent/pkg/app/contextassembly"
	core "github.com/whhhh1500/auto-agent/pkg/core"
	extmemory "github.com/whhhh1500/auto-agent/pkg/extensions/memory"
)

// memoryValidityStore is test-only. It models a caller-owned validity ledger;
// MemoryEntry and production Stores intentionally have no expiry contract.
type memoryValidityStore struct {
	inner      extmemory.Store
	now        time.Time
	validUntil map[memoryValidityKey]time.Time
	failRecall bool

	mu          sync.Mutex
	recallCalls []memoryValidityRecall
}

type memoryValidityKey struct{ scope, id string }

type memoryValidityRecall struct {
	scope string
	query string
	limit int
}

func (s *memoryValidityStore) Remember(ctx context.Context, scope core.ScopePath, entry extmemory.Entry) (extmemory.Entry, error) {
	return s.inner.Remember(ctx, scope, entry)
}

func (s *memoryValidityStore) Recall(ctx context.Context, scope core.ScopePath, query string, tags []string, limit int) ([]extmemory.Entry, error) {
	// Keep the Store boundary valid even though the decorator internally asks
	// the bounded backing store for all matching entries before filtering.
	if err := extmemory.ValidateScope(scope); err != nil {
		return nil, err
	}
	if err := extmemory.ValidateRecall(query, tags, limit); err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.recallCalls = append(s.recallCalls, memoryValidityRecall{scope: scope.String(), query: query, limit: limit})
	fail := s.failRecall
	now := s.now
	validUntil := make(map[memoryValidityKey]time.Time, len(s.validUntil))
	for key, value := range s.validUntil {
		validUntil[key] = value
	}
	s.mu.Unlock()
	if fail {
		return nil, errors.New("fixture memory backend unavailable")
	}

	// SliceStore is bounded per scope. Recall all matching entries first, then
	// apply the private validity filter and the caller's original limit.
	entries, err := s.inner.Recall(ctx, scope, query, tags, 0)
	if err != nil {
		return nil, err
	}
	out := make([]extmemory.Entry, 0, len(entries))
	for _, entry := range entries {
		until, found := validUntil[memoryValidityKey{scope: scope.String(), id: entry.ID}]
		if !found || !until.After(now) { // expiry at now is unavailable.
			continue
		}
		out = append(out, entry)
	}
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (s *memoryValidityStore) Forget(ctx context.Context, scope core.ScopePath, id string) error {
	return s.inner.Forget(ctx, scope, id)
}

func (s *memoryValidityStore) setValidity(scope core.ScopePath, id string, until time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.validUntil == nil {
		s.validUntil = map[memoryValidityKey]time.Time{}
	}
	s.validUntil[memoryValidityKey{scope: scope.String(), id: id}] = until
}

func (s *memoryValidityStore) calls() []memoryValidityRecall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]memoryValidityRecall(nil), s.recallCalls...)
}

type memoryValidityValues struct {
	Lookup, Target, Expired, MissingValidity, Foreign, Unrelated string
}

func newMemoryValidityValues() (memoryValidityValues, error) {
	values := memoryValidityValues{}
	for _, value := range []*string{&values.Lookup, &values.Target, &values.Expired, &values.MissingValidity, &values.Foreign, &values.Unrelated} {
		nonce, err := memoryNonce()
		if err != nil {
			return memoryValidityValues{}, err
		}
		*value = nonce
	}
	return values, nil
}

type memoryValidityFixture struct {
	runtime        *core.Runtime
	principal      core.Principal
	session        *core.Session
	base           *extmemory.SliceStore
	store          *memoryValidityStore
	peerScope      core.ScopePath
	peerSnapshot   extmemory.Entry
	journal        *memoryJournal
	assemblerCalls atomic.Int64
}

func newMemoryValidityFixture(model core.LlmAdapter, modelID string, values memoryValidityValues, arm, suffix string) (*memoryValidityFixture, error) {
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	product, err := global.Child(core.ScopeRef{Kind: core.ScopeProduct, ID: "memory-validity"})
	if err != nil {
		return nil, err
	}
	tenant, err := product.Child(core.ScopeRef{Kind: core.ScopeTenant, ID: "memory-validity-tenant"})
	if err != nil {
		return nil, err
	}
	currentScope, err := tenant.Child(core.ScopeRef{Kind: core.ScopeUser, ID: "current"})
	if err != nil {
		return nil, err
	}
	peerScope, err := tenant.Child(core.ScopeRef{Kind: core.ScopeUser, ID: "peer"})
	if err != nil {
		return nil, err
	}
	principal := core.Principal{TenantID: "memory-validity-tenant", SubjectID: "current", Scope: currentScope, Grants: core.NewPermissionSet(core.PermRead)}
	sessionScope, err := currentScope.Child(core.ScopeRef{Kind: core.ScopeSession, ID: "session-" + suffix})
	if err != nil {
		return nil, err
	}
	session, err := core.NewSession(core.SessionOptions{ID: "session-" + suffix, ProfileID: "example.memory.validity", Principal: principal, Scope: sessionScope})
	if err != nil {
		return nil, err
	}

	base := extmemory.NewSliceStore()
	// Seed the valid value first so that subsequent unavailable matching values
	// sort ahead of it. Limit-after-filter must still return this old value.
	seed := []struct {
		scope core.ScopePath
		entry extmemory.Entry
	}{
		{currentScope, extmemory.Entry{ID: "memory-shared-id", Key: "valid", Content: "lookup=" + values.Lookup + "; code=" + values.Target}},
		{currentScope, extmemory.Entry{ID: "memory-expired-id", Key: "expired", Content: "lookup=" + values.Lookup + "; code=" + values.Expired}},
		{currentScope, extmemory.Entry{ID: "memory-missing-validity-id", Key: "missing-validity", Content: "lookup=" + values.Lookup + "; code=" + values.MissingValidity}},
		{currentScope, extmemory.Entry{ID: "memory-unrelated-id", Key: "unrelated", Content: "note=" + values.Unrelated}},
		// Same Entry.ID as currentScope proves the ledger key includes scope.
		{peerScope, extmemory.Entry{ID: "memory-shared-id", Key: "peer", Content: "lookup=" + values.Lookup + "; code=" + values.Foreign}},
	}
	var peerSnapshot extmemory.Entry
	for index, item := range seed {
		// SliceStore owns CreatedAt and some test hosts coalesce adjacent clock
		// reads. Separate matching records before observing the raw sort order.
		if index > 0 && index < 3 {
			time.Sleep(20 * time.Millisecond)
		}
		remembered, err := base.Remember(context.Background(), item.scope, item.entry)
		if err != nil {
			return nil, err
		}
		if index == len(seed)-1 {
			peerSnapshot = remembered
		}
	}

	now := time.Date(2026, time.September, 9, 12, 0, 0, 0, time.UTC)
	store := &memoryValidityStore{inner: base, now: now, validUntil: map[memoryValidityKey]time.Time{}, failRecall: arm == "backend_error"}
	targetUntil := now.Add(time.Hour)
	if arm == "expired" {
		targetUntil = now
	}
	store.setValidity(currentScope, "memory-shared-id", targetUntil)
	store.setValidity(currentScope, "memory-expired-id", now)
	// The missing-validity entry deliberately has no ledger row.
	// A peer expiry at now must not hide the current scope's same-ID entry.
	store.setValidity(peerScope, "memory-shared-id", now)

	capabilities, err := extmemory.NewStandardCapabilities(store)
	if err != nil {
		return nil, err
	}
	registry := core.NewCapabilityRegistry()
	for _, capability := range capabilities {
		if capability.Manifest().ID != extmemory.RecallCapabilityID {
			continue
		}
		if err := registry.Register(product, capability); err != nil {
			return nil, err
		}
	}
	profiles := core.NewAgentProfileRegistry()
	name := "Memory validity fixture"
	selection := core.ModelSelection{Provider: model.Provider(), Model: modelID}
	steps, calls := 2, 1
	instructions := core.PromptFragment{ID: "memory-validity", Section: core.PromptInstructions, Content: "Use memory.recall results as evidence before answering."}
	if err := profiles.Bind(core.AgentProfileLayer{Scope: product, ProfileID: session.ProfileID(), Name: &name, Model: &selection, MaxSteps: &steps, MaxToolCalls: &calls, AddCapabilities: []string{extmemory.RecallCapabilityID}, PutFragments: []core.PromptFragment{instructions}}); err != nil {
		return nil, err
	}
	assembler, err := appcontextassembly.NewAssembler(appcontextassembly.Config{})
	if err != nil {
		return nil, err
	}
	journal := newMemoryJournal()
	fixture := &memoryValidityFixture{principal: principal, session: session, base: base, store: store, peerScope: peerScope, peerSnapshot: peerSnapshot, journal: journal}
	fixture.runtime = &core.Runtime{
		Capabilities: registry, Profiles: profiles, ToolJournal: journal,
		Models: core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) { return model, nil }),
		ContextAssembler: func(ctx context.Context, input core.ModelContext) (core.ModelContext, error) {
			fixture.assemblerCalls.Add(1)
			return assembler.AssembleModelContext(ctx, input)
		},
	}
	return fixture, nil
}

func memoryValidityPrompt(lookup string) string {
	return "Call memory.recall exactly once with lookup token \"" + lookup + "\" and limit 1. If one entry is returned, answer only its code. Otherwise answer exactly UNKNOWN."
}

func memoryValidityPromptLeaks(prompt string, values memoryValidityValues) bool {
	return strings.Contains(prompt, values.Target) || strings.Contains(prompt, values.Expired) || strings.Contains(prompt, values.MissingValidity) || strings.Contains(prompt, values.Foreign) || strings.Contains(prompt, values.Unrelated)
}

func memoryValidityRawPeerIntact(fixture *memoryValidityFixture, values memoryValidityValues) bool {
	entries, err := fixture.base.Recall(context.Background(), fixture.peerScope, values.Lookup, nil, 0)
	if err != nil || len(entries) != 1 {
		return false
	}
	entry, snapshot := entries[0], fixture.peerSnapshot
	return entry.ID == snapshot.ID && entry.Key == snapshot.Key && entry.Content == snapshot.Content && entry.CreatedAt.Equal(snapshot.CreatedAt) && strings.Join(entry.Tags, "\x00") == strings.Join(snapshot.Tags, "\x00") && strings.Contains(entry.Content, values.Foreign)
}

func memoryValidityRawCurrentStatus(fixture *memoryValidityFixture, values memoryValidityValues) (observed, newestUnavailable, entriesIntact bool) {
	if fixture == nil {
		return false, false, false
	}
	entries, err := fixture.base.Recall(context.Background(), fixture.principal.Scope, values.Lookup, nil, 0)
	if err != nil || len(entries) != 3 {
		return false, false, false
	}
	content := entries[0].Content + entries[1].Content + entries[2].Content
	// SliceStore owns CreatedAt. Assert the observed newest raw match is one of
	// the unavailable records; the caller limit would otherwise hide the target.
	newestUnavailable = strings.Contains(entries[0].Content, values.Expired) || strings.Contains(entries[0].Content, values.MissingValidity)
	entriesIntact = strings.Contains(content, values.Target) && strings.Contains(content, values.Expired) && strings.Contains(content, values.MissingValidity)
	return true, newestUnavailable, entriesIntact
}

func memoryValidityRawCurrentEntries(fixture *memoryValidityFixture, values memoryValidityValues) bool {
	observed, newestUnavailable, entriesIntact := memoryValidityRawCurrentStatus(fixture, values)
	return observed && newestUnavailable && entriesIntact
}

func memoryValidityErrorf(format string, args ...any) error {
	return fmt.Errorf("memory validity: "+format, args...)
}
