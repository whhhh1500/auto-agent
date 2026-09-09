package main

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"

	appcontextassembly "github.com/whhhh1500/auto-agent/pkg/app/contextassembly"
	core "github.com/whhhh1500/auto-agent/pkg/core"
	extmemory "github.com/whhhh1500/auto-agent/pkg/extensions/memory"
)

const (
	authorityCurrentID     = "authority.current"
	authorityEntity        = "launch-code"
	authorityVersion       = "v1"
	authorityUnavailableID = "authority_current_unavailable"
)

type memoryAuthorityValues struct{ Entity, Historical, Current, Foreign string }

func newMemoryAuthorityValues() (memoryAuthorityValues, error) {
	return newMemoryAuthorityValuesForEntity(authorityEntity)
}

func newMemoryAuthorityValuesForEntity(entity string) (memoryAuthorityValues, error) {
	if strings.TrimSpace(entity) == "" {
		return memoryAuthorityValues{}, errors.New("fixture authority entity is empty")
	}
	values := memoryAuthorityValues{Entity: entity}
	for _, field := range []*string{&values.Historical, &values.Current, &values.Foreign} {
		value, err := memoryNonce()
		if err != nil {
			return memoryAuthorityValues{}, err
		}
		*field = value
	}
	if values.Historical == values.Current {
		return memoryAuthorityValues{}, errors.New("fixture authority values collided")
	}
	return values, nil
}

func memoryAuthorityPrompt(readBoth bool) string {
	return memoryAuthorityPromptForEntity(authorityEntity, readBoth)
}

func memoryAuthorityPromptForEntity(entity string, readBoth bool) string {
	if readBoth {
		return "Determine the current launch code for entity " + entity + ". Call memory.recall and authority.current to compare the historical clue with the current authoritative source. Answer only the current code when authority reports available; otherwise answer exactly UNKNOWN."
	}
	return "Determine the current launch code for entity " + entity + ". Historical memory may be a clue, but only authority.current can establish the current code. Answer only the authority code when it reports available; otherwise answer exactly UNKNOWN."
}

type authorityCurrentTool struct {
	available bool
	entity    string
	code      string
	mu        sync.Mutex
	calls     int
}

func (a *authorityCurrentTool) Manifest() core.CapabilityManifest {
	output := map[string]any{"type": "object", "additionalProperties": false, "required": []any{"available"}, "properties": map[string]any{"available": map[string]any{"type": "boolean"}, "entity": map[string]any{"type": "string"}, "version": map[string]any{"type": "string"}, "code": map[string]any{"type": "string"}}}
	parameters := map[string]any{"type": "object", "additionalProperties": false, "required": []any{"entity"}, "properties": map[string]any{"entity": map[string]any{"type": "string", "enum": []any{a.entity}}}}
	return core.CapabilityManifest{ID: authorityCurrentID, Version: "1.0.0", Name: authorityCurrentID, Kind: core.KindKnowledge, Contract: "example.memory-authority/v1", Description: "Read the frozen current authoritative source for the configured entity. Historical memory is not authoritative.", RequiredPermissions: []core.Permission{core.PermRead}, Idempotent: true, MaxOutputBytes: 512, OutputSchema: output, Tool: &core.ToolExposure{Parameters: parameters}}
}

func (a *authorityCurrentTool) Execute(_ context.Context, request core.CapabilityRequest) (core.CapabilityResult, error) {
	if err := core.RequireAcceptedInvocation(request); err != nil {
		return core.CapabilityResult{}, err
	}
	entity, ok := request.Args["entity"].(string)
	if !ok || entity != a.entity || len(request.Args) != 1 {
		return core.CapabilityResult{Content: "invalid authority arguments", OK: false, Metadata: map[string]any{"code": core.CodeInvalidArgs}}, nil
	}
	a.mu.Lock()
	a.calls++
	available, code := a.available, a.code
	a.mu.Unlock()
	if !available {
		content, _ := json.Marshal(map[string]any{"available": false})
		return core.CapabilityResult{Content: string(content), OK: true}, nil
	}
	content, _ := json.Marshal(map[string]any{"available": true, "entity": a.entity, "version": authorityVersion, "code": code})
	return core.CapabilityResult{Content: string(content), OK: true}, nil
}

func (a *authorityCurrentTool) callsN() int { a.mu.Lock(); defer a.mu.Unlock(); return a.calls }

type memoryAuthorityFixture struct {
	runtime        *core.Runtime
	principal      core.Principal
	session        *core.Session
	base           *extmemory.SliceStore
	peerScope      core.ScopePath
	peerSnapshot   extmemory.Entry
	authority      *authorityCurrentTool
	journal        *memoryJournal
	assemblerCalls atomic.Int64
}

func newMemoryAuthorityFixture(model core.LlmAdapter, modelID string, values memoryAuthorityValues, arm, suffix string) (*memoryAuthorityFixture, error) {
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	product, err := global.Child(core.ScopeRef{Kind: core.ScopeProduct, ID: "memory-authority"})
	if err != nil {
		return nil, err
	}
	tenant, err := product.Child(core.ScopeRef{Kind: core.ScopeTenant, ID: "memory-authority-tenant"})
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
	principal := core.Principal{TenantID: "memory-authority-tenant", SubjectID: "current", Scope: currentScope, Grants: core.NewPermissionSet(core.PermRead)}
	sessionScope, err := currentScope.Child(core.ScopeRef{Kind: core.ScopeSession, ID: "session-" + suffix})
	if err != nil {
		return nil, err
	}
	session, err := core.NewSession(core.SessionOptions{ID: "session-" + suffix, ProfileID: "example.memory.authority", Principal: principal, Scope: sessionScope})
	if err != nil {
		return nil, err
	}
	base := extmemory.NewSliceStore()
	if _, err := base.Remember(context.Background(), currentScope, extmemory.Entry{ID: "historical", Key: values.Entity, Content: "entity=" + values.Entity + "; code=" + values.Historical}); err != nil {
		return nil, err
	}
	peerSnapshot, err := base.Remember(context.Background(), peerScope, extmemory.Entry{ID: "peer", Key: values.Entity, Content: "entity=" + values.Entity + "; code=" + values.Foreign})
	if err != nil {
		return nil, err
	}
	memories, err := extmemory.NewStandardCapabilities(base)
	if err != nil {
		return nil, err
	}
	registry := core.NewCapabilityRegistry()
	for _, capability := range memories {
		if capability.Manifest().ID == extmemory.RecallCapabilityID {
			if err := registry.Register(product, capability); err != nil {
				return nil, err
			}
		}
	}
	authority := &authorityCurrentTool{available: arm != "authority_unavailable", entity: values.Entity, code: values.Current}
	if err := registry.Register(product, authority); err != nil {
		return nil, err
	}
	profiles := core.NewAgentProfileRegistry()
	name := "Memory authority fixture"
	selection := core.ModelSelection{Provider: model.Provider(), Model: modelID}
	steps, calls := 3, 2
	capabilities := []string{authorityCurrentID}
	if arm != "direct_only_conflict" {
		capabilities = append([]string{extmemory.RecallCapabilityID}, capabilities...)
	}
	instructions := core.PromptFragment{ID: "memory-authority", Section: core.PromptInstructions, Content: "authority.current is the only current source; memory.recall is historical context."}
	if err := profiles.Bind(core.AgentProfileLayer{Scope: product, ProfileID: session.ProfileID(), Name: &name, Model: &selection, MaxSteps: &steps, MaxToolCalls: &calls, AddCapabilities: capabilities, PutFragments: []core.PromptFragment{instructions}}); err != nil {
		return nil, err
	}
	assembler, err := appcontextassembly.NewAssembler(appcontextassembly.Config{})
	if err != nil {
		return nil, err
	}
	journal := newMemoryJournal()
	fixture := &memoryAuthorityFixture{principal: principal, session: session, base: base, peerScope: peerScope, peerSnapshot: peerSnapshot, authority: authority, journal: journal}
	fixture.runtime = &core.Runtime{Capabilities: registry, Profiles: profiles, ToolJournal: journal, Models: core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) { return model, nil }), ContextAssembler: func(ctx context.Context, input core.ModelContext) (core.ModelContext, error) {
		fixture.assemblerCalls.Add(1)
		return assembler.AssembleModelContext(ctx, input)
	}}
	return fixture, nil
}

func memoryAuthorityPeerIntact(f *memoryAuthorityFixture, values memoryAuthorityValues) bool {
	entries, err := f.base.Recall(context.Background(), f.peerScope, values.Entity, nil, 1)
	if err != nil || len(entries) != 1 {
		return false
	}
	entry, snapshot := entries[0], f.peerSnapshot
	return entry.ID == snapshot.ID && entry.Key == snapshot.Key && entry.Content == snapshot.Content && entry.CreatedAt.Equal(snapshot.CreatedAt) && strings.Join(entry.Tags, "\x00") == strings.Join(snapshot.Tags, "\x00") && strings.Contains(entry.Content, values.Foreign)
}
