package memory

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	core "github.com/whhhh1500/auto-agent/pkg/core"
)

func TestSliceStoreLookupIsExactScopedAndCloned(t *testing.T) {
	store := NewSliceStore()
	ctx := context.Background()
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	first, err := global.Child(core.ScopeRef{Kind: core.ScopeUser, ID: "first"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := global.Child(core.ScopeRef{Kind: core.ScopeUser, ID: "second"})
	if err != nil {
		t.Fatal(err)
	}
	key := "Billing-Policy:EU_West"
	if _, err := store.Remember(ctx, first, Entry{Key: key, Content: "first", Tags: []string{"private"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Remember(ctx, second, Entry{Key: key, Content: "second"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Remember(ctx, first, Entry{Key: "unrelated-key", Content: "content-reference"}); err != nil {
		t.Fatal(err)
	}

	entry, found, err := store.Lookup(ctx, first, key)
	if err != nil || !found || entry.Content != "first" {
		t.Fatalf("exact lookup entry=%#v found=%t err=%v", entry, found, err)
	}
	entry.Tags[0] = "mutated"
	again, found, err := store.Lookup(ctx, first, key)
	if err != nil || !found || len(again.Tags) != 1 || again.Tags[0] != "private" {
		t.Fatalf("lookup did not clone entry=%#v found=%t err=%v", again, found, err)
	}
	for _, altered := range []string{"billing-policy:eu_west", "Billing Policy EU West", "content-reference"} {
		if entry, found, err := store.Lookup(ctx, first, altered); err != nil || found {
			t.Fatalf("altered key %q entry=%#v found=%t err=%v", altered, entry, found, err)
		}
	}
	other, found, err := store.Lookup(ctx, second, key)
	if err != nil || !found || other.Content != "second" {
		t.Fatalf("scope lookup entry=%#v found=%t err=%v", other, found, err)
	}
	for _, invalid := range []string{"", "   ", "bad\x00key", strings.Repeat("x", MaxEntryKeyBytes+1)} {
		if err := ValidateLookupKey(invalid); err == nil {
			t.Fatalf("invalid lookup key accepted: %q", invalid)
		}
	}
	if _, _, err := store.Lookup(ctx, core.ScopePath{}, key); err == nil {
		t.Fatal("empty lookup scope was accepted")
	}
}

func TestLookupCapabilityManifestAndOptIn(t *testing.T) {
	store := NewSliceStore()
	capability, err := NewLookupCapability(store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewLookupCapability(nil); err == nil {
		t.Fatal("nil key store was accepted")
	}
	if _, err := capability.Execute(context.Background(), core.CapabilityRequest{}); !errors.Is(err, core.ErrAcceptedInvocationRequired) {
		t.Fatalf("accepted invocation gate error=%v", err)
	}
	revisioner, ok := capability.(core.ArtifactRevisioner)
	if !ok || revisioner.ArtifactRevision() != "memory-lookup/v1" {
		t.Fatalf("artifact revision=%v ok=%t", revisioner, ok)
	}
	manifest := capability.Manifest()
	if manifest.ID != LookupCapabilityID || manifest.Version != "1.0.0" || manifest.Kind != core.KindMemory || manifest.Contract != ContractV1 || !manifest.Idempotent || manifest.RequiresApproval || len(manifest.RequiredPermissions) != 1 || manifest.RequiredPermissions[0] != core.PermRead {
		t.Fatalf("lookup manifest=%#v", manifest)
	}
	if manifest.Tool == nil || manifest.Tool.Parameters["additionalProperties"] != false || !sameRequired(manifest.Tool.Parameters["required"], []string{"key"}) {
		t.Fatalf("lookup input schema=%#v", manifest.Tool)
	}
	properties, ok := manifest.Tool.Parameters["properties"].(map[string]any)
	key, keyOK := properties["key"].(map[string]any)
	if !ok || !keyOK || key["type"] != "string" || key["maxLength"] != MaxEntryKeyBytes || key["description"] != "Canonical memory key. Copy it exactly, including punctuation and case; no content search or separator normalization is performed. The runtime also enforces a 1024-byte key limit." {
		t.Fatalf("lookup key schema=%#v", key)
	}
	for _, output := range []map[string]any{
		{"found": false, "entry": nil},
		{"found": true, "entry": map[string]any{"id": "mem_1", "key": "release.channel/v2", "content": "value", "created_at": time.Now().UTC().Format(time.RFC3339Nano)}},
	} {
		if err := core.ValidateJSONValue(manifest.OutputSchema, output); err != nil {
			t.Fatalf("valid lookup output=%#v err=%v", output, err)
		}
	}
	for _, invalid := range []map[string]any{
		{"found": false},
		{"found": false, "entry": "not-an-entry"},
		{"found": false, "entry": nil, "extra": true},
	} {
		if err := core.ValidateJSONValue(manifest.OutputSchema, invalid); err == nil {
			t.Fatalf("lookup output schema accepted invalid value=%#v", invalid)
		}
	}
	multibyte := strings.Repeat("界", MaxEntryKeyBytes/2)
	if err := core.ValidateArgs(manifest.Tool.Parameters, map[string]any{"key": multibyte}); err != nil {
		t.Fatalf("schema unexpectedly rejected a %d-rune key: %v", len([]rune(multibyte)), err)
	}
	if err := ValidateLookupKey(multibyte); err == nil {
		t.Fatal("runtime byte validation accepted a multi-byte key above the byte limit")
	}
	standard, err := NewStandardCapabilities(store)
	if err != nil {
		t.Fatal(err)
	}
	for _, capability := range standard {
		if capability.Manifest().ID == LookupCapabilityID {
			t.Fatal("lookup capability was added to the default standard menu")
		}
	}
}

func TestLookupCapabilityUsesPrincipalScopeAndExactKey(t *testing.T) {
	store := NewSliceStore()
	fixture := newLookupFixture(t, store)
	key := "release.channel/v2"
	if _, err := store.Remember(context.Background(), fixture.principal.Scope, Entry{Key: key, Content: "current"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Remember(context.Background(), fixture.principal.Scope, Entry{Key: "unrelated-key", Content: "content-reference"}); err != nil {
		t.Fatal(err)
	}
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	peer, err := global.Child(core.ScopeRef{Kind: core.ScopeTenant, ID: "tenant"})
	if err != nil {
		t.Fatal(err)
	}
	peer, err = peer.Child(core.ScopeRef{Kind: core.ScopeUser, ID: "peer"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Remember(context.Background(), peer, Entry{Key: key, Content: "peer"}); err != nil {
		t.Fatal(err)
	}

	result := fixture.invoke(t, map[string]any{"key": key})
	var found struct {
		Found bool   `json:"found"`
		Entry *Entry `json:"entry"`
	}
	if !result.OK || json.Unmarshal([]byte(result.Content), &found) != nil || !found.Found || found.Entry == nil || found.Entry.Key != key || found.Entry.Content != "current" {
		t.Fatalf("exact lookup result=%#v decoded=%#v", result, found)
	}
	for _, altered := range []string{"release channel v2", "release.channel/V2", "content-reference"} {
		result := fixture.invoke(t, map[string]any{"key": altered})
		var absent struct {
			Found bool   `json:"found"`
			Entry *Entry `json:"entry"`
		}
		if !result.OK || json.Unmarshal([]byte(result.Content), &absent) != nil || absent.Found || absent.Entry != nil || strings.Contains(result.Content, "current") || strings.Contains(result.Content, "peer") {
			t.Fatalf("non-exact lookup %q result=%#v decoded=%#v", altered, result, absent)
		}
	}
	for _, args := range []map[string]any{
		{}, {"key": ""}, {"key": "   "}, {"key": "bad\x00key"}, {"key": strings.Repeat("界", MaxEntryKeyBytes/2)}, {"key": key, "scope": "spoof"},
	} {
		result := fixture.invoke(t, args)
		if result.OK || result.Metadata["code"] != core.CodeInvalidArgs {
			t.Fatalf("invalid lookup args=%#v result=%#v", args, result)
		}
	}
}

func TestLookupCapabilityStoreFailuresAndPanicsAreRedacted(t *testing.T) {
	for name, store := range map[string]KeyStore{
		"error":         lookupErrorStore{},
		"panic":         lookupPanicStore{},
		"invalid_entry": lookupInvalidEntryStore{},
	} {
		t.Run(name, func(t *testing.T) {
			result := newLookupFixture(t, store).invoke(t, map[string]any{"key": "private-key"})
			if result.OK || result.Metadata["code"] != "memory_lookup_failed" || strings.Contains(result.Content, "TOP-SECRET") {
				t.Fatalf("lookup failure leaked: %#v", result)
			}
		})
	}
}

type lookupErrorStore struct{}

func (lookupErrorStore) Lookup(context.Context, core.ScopePath, string) (Entry, bool, error) {
	return Entry{}, false, errors.New("TOP-SECRET credential material")
}

type lookupPanicStore struct{}

func (lookupPanicStore) Lookup(context.Context, core.ScopePath, string) (Entry, bool, error) {
	panic("TOP-SECRET credential material")
}

type lookupInvalidEntryStore struct{}

func (lookupInvalidEntryStore) Lookup(_ context.Context, _ core.ScopePath, key string) (Entry, bool, error) {
	return Entry{ID: "mem_1", Key: key, Content: ""}, true, nil
}

type lookupFixture struct {
	principal core.Principal
	registry  *core.CapabilityRegistry
}

func newLookupFixture(t *testing.T, store KeyStore) lookupFixture {
	t.Helper()
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	tenant, err := global.Child(core.ScopeRef{Kind: core.ScopeTenant, ID: "tenant"})
	if err != nil {
		t.Fatal(err)
	}
	user, err := tenant.Child(core.ScopeRef{Kind: core.ScopeUser, ID: "user"})
	if err != nil {
		t.Fatal(err)
	}
	principal := core.Principal{TenantID: "tenant", SubjectID: "user", Scope: user, Grants: core.NewPermissionSet(core.PermRead)}
	capability, err := NewLookupCapability(store)
	if err != nil {
		t.Fatal(err)
	}
	registry := core.NewCapabilityRegistry()
	if err := registry.Register(user, capability); err != nil {
		t.Fatal(err)
	}
	return lookupFixture{principal: principal, registry: registry}
}

func (f lookupFixture) invoke(t *testing.T, args map[string]any) core.CapabilityResult {
	t.Helper()
	sessionScope, err := f.principal.Scope.Child(core.ScopeRef{Kind: core.ScopeSession, ID: "lookup"})
	if err != nil {
		t.Fatal(err)
	}
	session, err := core.NewSession(core.SessionOptions{ID: "lookup", ProfileID: "memory.lookup", Principal: f.principal, Scope: sessionScope})
	if err != nil {
		t.Fatal(err)
	}
	call := core.ToolCall{ID: "call-lookup", Name: LookupCapabilityID, Args: args}
	tools, err := (core.CapabilityResolver{Registry: f.registry}).Resolve(f.principal, sessionScope)
	if err != nil {
		t.Fatal(err)
	}
	agent, err := core.NewAgent(core.AgentOptions{LLM: &standardModel{call: call}, Tools: tools, Session: session, ToolJournal: newStandardJournal(), MaxSteps: 2})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := agent.RunTurn(context.Background(), core.TurnInput{RunID: "lookup", Text: "lookup"}); err != nil {
		t.Fatal(err)
	}
	result, exists, err := session.ToolResult("lookup", call.ID)
	if err != nil || !exists {
		t.Fatalf("lookup tool result exists=%t err=%v", exists, err)
	}
	return result
}
