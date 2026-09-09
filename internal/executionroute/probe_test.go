package executionroute

import (
	"context"
	"errors"
	"testing"

	"github.com/whhhh1500/auto-agent/pkg/app/programmatic"
	"github.com/whhhh1500/auto-agent/pkg/core"
)

func TestResolveProbePlanReconstructsTrustedMemoryReceipt(t *testing.T) {
	fixture := newProbeResolverFixture(t)
	fixture.appendValidReceipt(t)

	plan, isProbe, err := ResolveProbePlan(context.Background(), fixture.session, fixture.principal, fixture.journal, fixture.runID, fixture.call.ID)
	if err != nil || !isProbe || plan.Digest() == "" {
		t.Fatalf("plan=%#v isProbe=%t err=%v", plan, isProbe, err)
	}
	facts := plan.Facts()
	if facts.FollowupCapabilityID != fixture.followup.Manifest.ID || len(facts.Candidates) != 1 || facts.Candidates[0].Args["id"] != "record-17" {
		t.Fatalf("unexpected reconstructed facts: %#v", facts)
	}
	if direct := plan.DirectToolSchema(); direct.Name != fixture.followup.Manifest.ID {
		t.Fatalf("direct schema=%#v", direct)
	}
	if choices, err := plan.ChoiceTools(); err != nil || len(choices) != 2 || choices[1].Name != programmatic.DefaultExecuteToolID {
		t.Fatalf("choice tools=%#v err=%v", choices, err)
	}
}

func TestResolveProbePlanOrdinaryFrozenCapabilityIsNotAProbe(t *testing.T) {
	fixture := newProbeResolverFixture(t)
	fixture.probe.Manifest.Metadata = nil
	fixture.appendValidReceipt(t)

	plan, isProbe, err := ResolveProbePlan(context.Background(), fixture.session, fixture.principal, fixture.journal, fixture.runID, fixture.call.ID)
	if err != nil || isProbe || plan.Digest() != "" {
		t.Fatalf("plan=%#v isProbe=%t err=%v", plan, isProbe, err)
	}
}

func TestIsNeutralProbeClassifiesFrozenProposalWithoutReceipt(t *testing.T) {
	fixture := newProbeResolverFixture(t)
	fixture.appendStart(t)

	isProbe, err := IsNeutralProbe(fixture.session, fixture.principal, fixture.runID, fixture.probe.Manifest.ID)
	if err != nil || !isProbe {
		t.Fatalf("probe=%t err=%v", isProbe, err)
	}
	isProbe, err = IsNeutralProbe(fixture.session, fixture.principal, fixture.runID, fixture.followup.Manifest.ID)
	if err != nil || isProbe {
		t.Fatalf("ordinary capability probe=%t err=%v", isProbe, err)
	}
}

func TestIsNeutralProbeFailsClosedForInvalidFrozenEligibility(t *testing.T) {
	for _, test := range []struct {
		name  string
		setup func(*probeResolverFixture) core.Principal
	}{
		{
			name: "unsupported marker version",
			setup: func(fixture *probeResolverFixture) core.Principal {
				fixture.probe.Manifest.Metadata[programmatic.ProbeManifestKey] = "2"
				fixture.appendStart(t)
				return fixture.principal
			},
		},
		{
			name: "probe is programmatically exposed",
			setup: func(fixture *probeResolverFixture) core.Principal {
				fixture.probe.Manifest.Metadata[programmatic.ExposureKey] = programmatic.ExposureVersion
				fixture.appendStart(t)
				return fixture.principal
			},
		},
		{
			name: "approval capable probe",
			setup: func(fixture *probeResolverFixture) core.Principal {
				fixture.probe.Manifest.RequiresApproval = true
				fixture.appendStart(t)
				return fixture.principal
			},
		},
		{
			name: "current grant revoked",
			setup: func(fixture *probeResolverFixture) core.Principal {
				fixture.probe.Manifest.RequiredPermissions = []core.Permission{core.PermRead}
				fixture.appendStart(t)
				revoked := fixture.principal
				revoked.Grants = core.NewPermissionSet()
				return revoked
			},
		},
		{
			name: "duplicate frozen probe capability",
			setup: func(fixture *probeResolverFixture) core.Principal {
				fixture.composition.Capabilities = []core.SnapshotCapability{fixture.probe, fixture.probe, fixture.followup, fixture.execute}
				fixture.appendComposition(t, core.EvRunStart, &fixture.composition)
				return fixture.principal
			},
		},
		{
			name: "frozen route is not v2",
			setup: func(fixture *probeResolverFixture) core.Principal {
				fixture.composition.Metadata[RouteVersionKey] = RouteVersion
				fixture.appendStart(t)
				return fixture.principal
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newProbeResolverFixture(t)
			principal := test.setup(fixture)
			isProbe, err := IsNeutralProbe(fixture.session, principal, fixture.runID, fixture.probe.Manifest.ID)
			if err == nil || !errors.Is(err, ErrProbePlanResolution) || !isProbe {
				t.Fatalf("probe=%t err=%v", isProbe, err)
			}
		})
	}
}

func TestResolveProbePlanFailsClosedForAdversarialEvidence(t *testing.T) {
	for _, test := range []struct {
		name  string
		setup func(*probeResolverFixture) *probeResolverFixture
	}{
		{
			name: "missing journal record",
			setup: func(fixture *probeResolverFixture) *probeResolverFixture {
				fixture.journal.records = nil
				return fixture
			},
		},
		{
			name: "journal uncertain",
			setup: func(fixture *probeResolverFixture) *probeResolverFixture {
				fixture.journal.mutate(func(record *core.ToolInvocationRecord) { record.State = core.ToolInvocationUncertain })
				return fixture
			},
		},
		{
			name: "journal canonical result conflicts",
			setup: func(fixture *probeResolverFixture) *probeResolverFixture {
				changed := fixture.result
				changed.Content = "changed"
				fixture.journal.mutate(func(record *core.ToolInvocationRecord) { record.Result = &changed })
				return fixture
			},
		},
		{
			name: "args digest identity conflicts",
			setup: func(fixture *probeResolverFixture) *probeResolverFixture {
				mutated := fixture.call
				mutated.Args = map[string]any{"unexpected": true}
				fixture.journal.rekey(t, fixture, mutated)
				return fixture
			},
		},
		{
			name: "journal reader returns conflicting identity",
			setup: func(fixture *probeResolverFixture) *probeResolverFixture {
				conflicting := fixture.journal.record
				conflicting.ArgsDigest = "conflicting-digest"
				fixture.journal.forced = &conflicting
				return fixture
			},
		},
		{
			name: "duplicate assistant evidence",
			setup: func(fixture *probeResolverFixture) *probeResolverFixture {
				fixture.append(t, core.EvAssistantMessage, core.AssistantMessageData{ToolCall: &fixture.call, ToolCalls: []core.ToolCall{fixture.call}})
				return fixture
			},
		},
		{
			name: "duplicate tool result evidence",
			setup: func(fixture *probeResolverFixture) *probeResolverFixture {
				fixture.append(t, core.EvToolResult, core.ToolResultData{CallID: fixture.call.ID, Content: fixture.result.Content, OK: fixture.result.OK, Metadata: fixture.result.Metadata})
				return fixture
			},
		},
		{
			name: "probe mixed with another call",
			setup: func(fixture *probeResolverFixture) *probeResolverFixture {
				// This case starts from a clean session, so replace the valid proof
				// with the mixed assistant message before the reader is populated.
				return fixture.rebuildMixed(t)
			},
		},
		{
			name: "orphan tool call",
			setup: func(fixture *probeResolverFixture) *probeResolverFixture {
				return fixture.rebuildOrphan(t)
			},
		},
		{
			name: "unpaired other assistant call",
			setup: func(fixture *probeResolverFixture) *probeResolverFixture {
				other := core.ToolCall{ID: "unpaired-call", Name: fixture.followup.Manifest.ID, Args: map[string]any{"id": "record-17"}}
				fixture.append(t, core.EvAssistantMessage, core.AssistantMessageData{ToolCall: &other, ToolCalls: []core.ToolCall{other}})
				return fixture
			},
		},
		{
			name: "receipt before frozen start",
			setup: func(fixture *probeResolverFixture) *probeResolverFixture {
				return fixture.rebuildBeforeStart(t)
			},
		},
		{
			name: "resume provider revision drift",
			setup: func(fixture *probeResolverFixture) *probeResolverFixture {
				drift := fixture.compositionCopy()
				drift.Capabilities[0].ProviderRevision = "inventory-v2"
				fixture.appendComposition(t, core.EvRunResume, &drift)
				return fixture
			},
		},
		{
			name: "probe write permission",
			setup: func(fixture *probeResolverFixture) *probeResolverFixture {
				return fixture.rebuildWithManifest(t, func(probe *core.SnapshotCapability, _ *core.SnapshotCapability) {
					probe.Manifest.RequiredPermissions = []core.Permission{core.PermWrite}
				})
			},
		},
		{
			name: "follow-up not programmatic",
			setup: func(fixture *probeResolverFixture) *probeResolverFixture {
				return fixture.rebuildWithManifest(t, func(_ *core.SnapshotCapability, followup *core.SnapshotCapability) {
					followup.Manifest.Metadata = nil
				})
			},
		},
		{
			name: "malformed trusted facts",
			setup: func(fixture *probeResolverFixture) *probeResolverFixture {
				return fixture.rebuildWithResult(t, func(result *core.CapabilityResult) {
					facts := result.Metadata[programmatic.ProbeFactsMetadataKey].(map[string]any)
					facts["unexpected"] = true
				})
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newProbeResolverFixture(t)
			fixture.appendValidReceipt(t)
			fixture = test.setup(fixture)
			plan, isProbe, err := ResolveProbePlan(context.Background(), fixture.session, fixture.principal, fixture.journal, fixture.runID, fixture.call.ID)
			if err == nil || !errors.Is(err, ErrProbePlanResolution) || !isProbe || plan.Digest() != "" {
				t.Fatalf("plan=%#v isProbe=%t err=%v", plan, isProbe, err)
			}
		})
	}
}

func TestResolveProbePlanRejectsRevokedCurrentPrincipal(t *testing.T) {
	fixture := newProbeResolverFixture(t)
	fixture.probe.Manifest.RequiredPermissions = []core.Permission{core.PermRead}
	fixture.followup.Manifest.RequiredPermissions = []core.Permission{core.PermRead}
	fixture.appendValidReceipt(t)
	revoked := fixture.principal
	revoked.Grants = core.NewPermissionSet()

	plan, isProbe, err := ResolveProbePlan(context.Background(), fixture.session, revoked, fixture.journal, fixture.runID, fixture.call.ID)
	if err == nil || !errors.Is(err, ErrProbePlanResolution) || !isProbe || plan.Digest() != "" {
		t.Fatalf("plan=%#v isProbe=%t err=%v", plan, isProbe, err)
	}
}

type probeResolverFixture struct {
	session     *core.Session
	principal   core.Principal
	runID       string
	call        core.ToolCall
	result      core.CapabilityResult
	probe       core.SnapshotCapability
	followup    core.SnapshotCapability
	execute     core.SnapshotCapability
	composition core.RunCompositionData
	journal     *memoryProbeJournal
}

func newProbeResolverFixture(t *testing.T) *probeResolverFixture {
	t.Helper()
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	product, err := global.Child(core.ScopeRef{Kind: core.ScopeProduct, ID: "product"})
	if err != nil {
		t.Fatal(err)
	}
	user, err := product.Child(core.ScopeRef{Kind: core.ScopeUser, ID: "user"})
	if err != nil {
		t.Fatal(err)
	}
	sessionScope, err := user.Child(core.ScopeRef{Kind: core.ScopeSession, ID: "probe-session"})
	if err != nil {
		t.Fatal(err)
	}
	principal := core.Principal{TenantID: "tenant", SubjectID: "user", Scope: user, Grants: core.NewPermissionSet(core.PermRead)}
	session, err := core.NewSession(core.SessionOptions{ID: "probe-session", ProfileID: "probe.agent", Principal: principal, Scope: sessionScope})
	if err != nil {
		t.Fatal(err)
	}
	probeSchema := map[string]any{"type": "object", "additionalProperties": false}
	followupSchema := map[string]any{
		"type": "object", "additionalProperties": false,
		"properties": map[string]any{"id": map[string]any{"type": "string"}}, "required": []any{"id"},
	}
	probe := core.SnapshotCapability{Manifest: core.CapabilityManifest{
		ID: "records.inventory", Version: "1", Name: "Inventory", Kind: core.KindTool,
		Idempotent: true, MaxOutputBytes: 4096, InputSchema: probeSchema,
		Tool:     &core.ToolExposure{Description: "List records", Parameters: probeSchema},
		Metadata: map[string]string{programmatic.ProbeManifestKey: programmatic.ProbeManifestVersion},
	}, Source: session.Scope(), ProviderRevision: "inventory-v1"}
	followup := core.SnapshotCapability{Manifest: core.CapabilityManifest{
		ID: "records.detail", Version: "1", Name: "Detail", Kind: core.KindTool,
		Idempotent: true, InputSchema: followupSchema,
		Tool: &core.ToolExposure{Description: "Read one record", Parameters: followupSchema},
		OutputSchema: map[string]any{"type": "object", "required": []any{"id", "name"}, "additionalProperties": false, "properties": map[string]any{
			"id": map[string]any{"type": "string", "maxLength": 16}, "name": map[string]any{"type": "string", "maxLength": 64},
		}},
		Metadata: map[string]string{programmatic.ExposureKey: programmatic.ExposureVersion},
	}, Source: session.Scope(), ProviderRevision: "detail-v1"}
	execute := core.SnapshotCapability{Manifest: core.CapabilityManifest{
		ID: programmatic.DefaultExecuteToolID, Version: "1", Name: "Execute", Kind: core.KindTool,
		Tool: &core.ToolExposure{Description: "Execute program", Parameters: map[string]any{"type": "object"}},
	}, Source: session.Scope(), ProviderRevision: "execute-v1"}
	result := core.CapabilityResult{OK: true, Content: "inventory complete", Metadata: map[string]any{
		programmatic.ProbeFactsMetadataKey: map[string]any{
			"version": programmatic.ProbeFactsVersion, "followup_capability_id": followup.Manifest.ID,
			"candidates":             []any{map[string]any{"label": "record-17", "args": map[string]any{"id": "record-17"}, "facts": map[string]any{"active": true}}},
			"max_model_return_bytes": float64(1024),
		},
	}}
	fixture := &probeResolverFixture{
		session: session, principal: principal, runID: "probe-run", call: core.ToolCall{ID: "probe-call", Name: probe.Manifest.ID, Args: map[string]any{}},
		result: result, probe: probe, followup: followup, execute: execute, journal: &memoryProbeJournal{},
	}
	fixture.composition = core.RunCompositionData{
		Profile: core.AgentProfileSnapshot{ProfileID: session.ProfileID(), Scope: session.Scope()}, EffectivePermissions: core.NewPermissionSet(core.PermRead),
		Model: core.ModelSelection{Provider: "provider", Model: "model"}, ResolvedProvider: "provider", ModelRevision: "model-v1",
		MaxSteps: 4, MaxToolCalls: 8,
		Metadata: map[string]string{
			RouteVersionKey: RouteProbeVersion, RouteModeKey: probeRouteMode,
			RouteCatalogToolIDKey: programmatic.DefaultCatalogToolID, RouteExecuteToolIDKey: programmatic.DefaultExecuteToolID,
			RouteImplementationKey: RouteProbeImplementationRevision,
		},
	}
	return fixture
}

func (fixture *probeResolverFixture) appendValidReceipt(t *testing.T) {
	t.Helper()
	fixture.appendStart(t)
	fixture.append(t, core.EvUserMessage, core.UserMessageData{Text: "find records"})
	fixture.append(t, core.EvAssistantMessage, core.AssistantMessageData{ToolCall: &fixture.call, ToolCalls: []core.ToolCall{fixture.call}})
	fixture.append(t, core.EvToolCall, core.ToolCallData{CallID: fixture.call.ID, Name: fixture.call.Name, Args: fixture.call.Args})
	fixture.append(t, core.EvToolResult, core.ToolResultData{CallID: fixture.call.ID, Content: fixture.result.Content, OK: fixture.result.OK, Metadata: fixture.result.Metadata})
	fixture.journal.store(t, fixture, fixture.call, fixture.result)
}

func (fixture *probeResolverFixture) appendStart(t *testing.T) {
	t.Helper()
	fixture.composition.Capabilities = []core.SnapshotCapability{fixture.probe, fixture.followup, fixture.execute}
	fixture.appendComposition(t, core.EvRunStart, &fixture.composition)
}

func (fixture *probeResolverFixture) appendComposition(t *testing.T, kind core.SessionEventType, composition *core.RunCompositionData) {
	t.Helper()
	compositionRevision, err := core.CompositionRevision(composition)
	if err != nil {
		t.Fatal(err)
	}
	assignmentRevision, err := core.CompositionMetadataRevision(composition.Metadata)
	if err != nil {
		t.Fatal(err)
	}
	if kind == core.EvRunStart {
		fixture.append(t, kind, core.RunStartData{Composition: composition, CompositionRevision: compositionRevision, AssignmentRevision: assignmentRevision})
		return
	}
	fixture.append(t, kind, core.RunResumeData{Composition: composition, CompositionRevision: compositionRevision, AssignmentRevision: assignmentRevision})
}

func (fixture *probeResolverFixture) append(t *testing.T, kind core.SessionEventType, data any) {
	t.Helper()
	if _, err := fixture.session.Append(fixture.runID, kind, data); err != nil {
		t.Fatal(err)
	}
}

func (fixture *probeResolverFixture) compositionCopy() core.RunCompositionData {
	copy := fixture.composition
	copy.Metadata = cloneMetadata(fixture.composition.Metadata)
	copy.Capabilities = append([]core.SnapshotCapability(nil), fixture.composition.Capabilities...)
	return copy
}

func (fixture *probeResolverFixture) rebuildWithManifest(t *testing.T, mutate func(*core.SnapshotCapability, *core.SnapshotCapability)) *probeResolverFixture {
	t.Helper()
	next := newProbeResolverFixture(t)
	mutate(&next.probe, &next.followup)
	next.appendValidReceipt(t)
	return next
}

func (fixture *probeResolverFixture) rebuildWithResult(t *testing.T, mutate func(*core.CapabilityResult)) *probeResolverFixture {
	t.Helper()
	next := newProbeResolverFixture(t)
	mutate(&next.result)
	next.appendValidReceipt(t)
	return next
}

func (fixture *probeResolverFixture) rebuildMixed(t *testing.T) *probeResolverFixture {
	t.Helper()
	next := newProbeResolverFixture(t)
	next.appendStart(t)
	mixed := core.ToolCall{ID: "other-call", Name: next.followup.Manifest.ID, Args: map[string]any{"id": "record-17"}}
	next.append(t, core.EvAssistantMessage, core.AssistantMessageData{ToolCalls: []core.ToolCall{next.call, mixed}})
	next.append(t, core.EvToolCall, core.ToolCallData{CallID: next.call.ID, Name: next.call.Name, Args: next.call.Args})
	next.append(t, core.EvToolResult, core.ToolResultData{CallID: next.call.ID, Content: next.result.Content, OK: next.result.OK, Metadata: next.result.Metadata})
	next.journal.store(t, next, next.call, next.result)
	return next
}

func (fixture *probeResolverFixture) rebuildOrphan(t *testing.T) *probeResolverFixture {
	t.Helper()
	next := newProbeResolverFixture(t)
	next.appendStart(t)
	next.append(t, core.EvToolCall, core.ToolCallData{CallID: next.call.ID, Name: next.call.Name, Args: next.call.Args})
	next.append(t, core.EvAssistantMessage, core.AssistantMessageData{ToolCall: &next.call, ToolCalls: []core.ToolCall{next.call}})
	next.append(t, core.EvToolResult, core.ToolResultData{CallID: next.call.ID, Content: next.result.Content, OK: next.result.OK, Metadata: next.result.Metadata})
	next.journal.store(t, next, next.call, next.result)
	return next
}

func (fixture *probeResolverFixture) rebuildBeforeStart(t *testing.T) *probeResolverFixture {
	t.Helper()
	next := newProbeResolverFixture(t)
	next.append(t, core.EvAssistantMessage, core.AssistantMessageData{ToolCall: &next.call, ToolCalls: []core.ToolCall{next.call}})
	next.append(t, core.EvToolCall, core.ToolCallData{CallID: next.call.ID, Name: next.call.Name, Args: next.call.Args})
	next.append(t, core.EvToolResult, core.ToolResultData{CallID: next.call.ID, Content: next.result.Content, OK: next.result.OK, Metadata: next.result.Metadata})
	next.appendStart(t)
	next.journal.store(t, next, next.call, next.result)
	return next
}

type memoryProbeJournal struct {
	records map[core.ToolInvocation]core.ToolInvocationRecord
	record  core.ToolInvocationRecord
	forced  *core.ToolInvocationRecord
	err     error
}

func (journal *memoryProbeJournal) GetToolInvocation(_ context.Context, invocation core.ToolInvocation) (core.ToolInvocationRecord, bool, error) {
	if journal.err != nil {
		return core.ToolInvocationRecord{}, false, journal.err
	}
	if journal.forced != nil {
		return *journal.forced, true, nil
	}
	record, found := journal.records[invocation]
	if found {
		journal.record = record
	}
	return record, found, nil
}

func (journal *memoryProbeJournal) store(t *testing.T, fixture *probeResolverFixture, call core.ToolCall, result core.CapabilityResult) {
	t.Helper()
	invocation, err := core.NewToolInvocation(core.RunInfo{RunID: fixture.runID, SessionID: fixture.session.ID(), ProfileID: fixture.session.ProfileID(), Principal: fixture.principal}, call, true)
	if err != nil {
		t.Fatal(err)
	}
	canonical := result
	journal.record = core.ToolInvocationRecord{ToolInvocation: invocation, State: core.ToolInvocationCompleted, Result: &canonical}
	journal.records = map[core.ToolInvocation]core.ToolInvocationRecord{invocation: journal.record}
}

func (journal *memoryProbeJournal) rekey(t *testing.T, fixture *probeResolverFixture, call core.ToolCall) {
	t.Helper()
	invocation, err := core.NewToolInvocation(core.RunInfo{RunID: fixture.runID, SessionID: fixture.session.ID(), ProfileID: fixture.session.ProfileID(), Principal: fixture.principal}, call, true)
	if err != nil {
		t.Fatal(err)
	}
	journal.records = map[core.ToolInvocation]core.ToolInvocationRecord{invocation: {ToolInvocation: invocation, State: core.ToolInvocationCompleted, Result: journal.record.Result}}
}

func (journal *memoryProbeJournal) mutate(mutate func(*core.ToolInvocationRecord)) {
	for invocation, record := range journal.records {
		mutate(&record)
		journal.record = record
		journal.records[invocation] = record
		return
	}
}
