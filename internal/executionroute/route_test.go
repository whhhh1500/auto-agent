package executionroute

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/whhhh1500/auto-agent/pkg/app/programmatic"
	"github.com/whhhh1500/auto-agent/pkg/app/runexecutor"
	"github.com/whhhh1500/auto-agent/pkg/core"
)

type routeTestExecutor struct{}

func (routeTestExecutor) RunTurn(context.Context, core.Principal, *core.Session, core.TurnInput, func(core.SessionEvent)) (core.TurnResult, error) {
	return core.TurnResult{}, nil
}
func (routeTestExecutor) ResumeTurn(context.Context, core.Principal, *core.Session, core.ResumeInput, func(core.SessionEvent)) (core.TurnResult, error) {
	return core.TurnResult{}, nil
}

func routeFixture(t *testing.T, metadata map[string]string) (*core.Runtime, core.Principal, *core.Session) {
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
	sessionScope, err := user.Child(core.ScopeRef{Kind: core.ScopeSession, ID: "route-session"})
	if err != nil {
		t.Fatal(err)
	}
	principal := core.Principal{TenantID: "tenant", SubjectID: "user", Scope: user}
	profiles := core.NewAgentProfileRegistry()
	model := core.ModelSelection{Provider: "route", Model: "route"}
	if err := profiles.Bind(core.AgentProfileLayer{Scope: product, ProfileID: "route.agent", Model: &model, Metadata: metadata}); err != nil {
		t.Fatal(err)
	}
	session, err := core.NewSession(core.SessionOptions{ID: "route-session", ProfileID: "route.agent", Principal: principal, Scope: sessionScope})
	if err != nil {
		t.Fatal(err)
	}
	return &core.Runtime{Profiles: profiles, Models: core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) {
		return core.MockLlmAdapter{}, nil
	})}, principal, session
}

func routeRegistry(t *testing.T, revision string) *runexecutor.Registry {
	t.Helper()
	registry, err := runexecutor.NewRegistry(2, runexecutor.Registration{
		Metadata: runexecutor.Metadata{ID: "route.custom", Version: "1", ImplementationRevision: revision},
		Factory:  func(runexecutor.Dependencies) (runexecutor.RunExecutor, error) { return routeTestExecutor{}, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	return registry
}

func TestResolveFreezesDefaultSequentialAndCopiesBaseMetadata(t *testing.T) {
	runtime, principal, session := routeFixture(t, nil)
	registry, err := runexecutor.NewDefaultRegistry()
	if err != nil {
		t.Fatal(err)
	}
	base := map[string]string{"harness.assignment.id": "route-test"}
	decision, err := Resolve(context.Background(), Request{
		Runtime: runtime, Registry: registry, Principal: principal, Session: session, RunID: "run_route", BaseMetadata: base,
	})
	if err != nil {
		t.Fatal(err)
	}
	if decision.Metadata.ID != runexecutor.SequentialID || decision.CompositionMetadata[ExecutorIDKey] != runexecutor.SequentialID ||
		decision.CompositionMetadata[ExecutorVersionKey] != runexecutor.SequentialVersion ||
		decision.CompositionMetadata[ExecutorImplementationKey] != runexecutor.SequentialImplementationRevision {
		t.Fatalf("default decision did not freeze sequential identity: %#v", decision)
	}
	base["harness.assignment.id"] = "mutated"
	if decision.CompositionMetadata["harness.assignment.id"] != "route-test" {
		t.Fatalf("resolver retained caller metadata: %#v", decision.CompositionMetadata)
	}
}

func TestResolveRejectsUnavailableCodePTCBeforeExecution(t *testing.T) {
	runtime, principal, session := routeFixture(t, map[string]string{
		ExecutorIDKey: runexecutor.CodePTCID, ExecutorVersionKey: runexecutor.CodePTCVersion,
	})
	registry, err := runexecutor.NewDefaultRegistry()
	if err != nil {
		t.Fatal(err)
	}
	decision, err := Resolve(context.Background(), Request{Runtime: runtime, Registry: registry, Principal: principal, Session: session, RunID: "run_codeptc"})
	if !errors.Is(err, ErrSelection) || !errors.Is(err, runexecutor.ErrExecutorNotFound) || decision.Executor != nil {
		t.Fatalf("CodePTC decision=%#v err=%v", decision, err)
	}
}

func TestResolveResumeUsesFrozenEvidenceAndFailsClosedOnDrift(t *testing.T) {
	runtime, principal, session := routeFixture(t, nil)
	metadata := map[string]string{
		ExecutorIDKey: "route.custom", ExecutorVersionKey: "1", ExecutorImplementationKey: "route-v1",
	}
	if _, err := session.Append("run_resume", core.EvRunStart, core.RunStartData{Composition: &core.RunCompositionData{
		Profile: core.AgentProfileSnapshot{ProfileID: session.ProfileID(), Scope: session.Scope()}, Metadata: metadata,
	}}); err != nil {
		t.Fatal(err)
	}
	decision, err := Resolve(context.Background(), Request{
		Runtime: runtime, Registry: routeRegistry(t, "route-v1"), Principal: principal, Session: session, RunID: "run_resume", Resume: true,
	})
	if err != nil || decision.Metadata.ImplementationRevision != "route-v1" {
		t.Fatalf("frozen decision=%#v err=%v", decision, err)
	}
	if _, err := Resolve(context.Background(), Request{
		Runtime: runtime, Registry: routeRegistry(t, "route-v2"), Principal: principal, Session: session, RunID: "run_resume", Resume: true,
	}); !errors.Is(err, ErrSelection) {
		t.Fatalf("implementation drift accepted: %v", err)
	}
}

func TestResolveResumeLegacyDefaultsToSequential(t *testing.T) {
	runtime, principal, session := routeFixture(t, nil)
	registry, err := runexecutor.NewDefaultRegistry()
	if err != nil {
		t.Fatal(err)
	}
	decision, err := Resolve(context.Background(), Request{
		Runtime: runtime, Registry: registry, Principal: principal, Session: session, RunID: "run_legacy", Resume: true,
	})
	if err != nil || decision.Metadata.ID != runexecutor.SequentialID {
		t.Fatalf("legacy decision=%#v err=%v", decision, err)
	}
}

func TestResolveFreezesTrustedRouteAndPassesWrappedRuntimeToFactory(t *testing.T) {
	runtime, principal, session := routeFixture(t, map[string]string{ExecutorIDKey: "route.capture", ExecutorVersionKey: "1"})
	var received *core.Runtime
	registry, err := runexecutor.NewRegistry(1, runexecutor.Registration{
		Metadata: runexecutor.Metadata{ID: "route.capture", Version: "1", ImplementationRevision: "capture-v1"},
		Factory: func(dependencies runexecutor.Dependencies) (runexecutor.RunExecutor, error) {
			received = dependencies.Runtime
			return routeTestExecutor{}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	base := map[string]string{
		RouteVersionKey: "attacker-version", RouteModeKey: string(programmatic.RoutePTCOnly),
		RouteCatalogToolIDKey: "attacker.catalog", RouteExecuteToolIDKey: "attacker.execute", RouteImplementationKey: "attacker-revision",
	}
	decision, err := Resolve(context.Background(), Request{
		Runtime: runtime, Registry: registry, Principal: principal, Session: session, RunID: "run_trusted_route", BaseMetadata: base,
	})
	if err != nil {
		t.Fatal(err)
	}
	if received == nil || received == runtime {
		t.Fatalf("factory did not receive a private wrapped runtime: received=%p original=%p", received, runtime)
	}
	if _, ok := received.Models.(*programmatic.ModelResolver); !ok {
		t.Fatalf("factory received unwrapped model resolver %T", received.Models)
	}
	for key, want := range map[string]string{
		RouteVersionKey: RouteVersion, RouteModeKey: string(programmatic.RouteDirectOnly),
		RouteCatalogToolIDKey: programmatic.DefaultCatalogToolID, RouteExecuteToolIDKey: programmatic.DefaultExecuteToolID,
		RouteImplementationKey: RouteImplementationRevision,
	} {
		if got := decision.CompositionMetadata[key]; got != want {
			t.Fatalf("trusted route metadata %s=%q, want %q: %#v", key, got, want, decision.CompositionMetadata)
		}
	}
}

func TestResolveProgrammaticRouteRejectsIncompleteUnknownAndUnreadableJournal(t *testing.T) {
	for _, test := range []struct {
		name     string
		metadata map[string]string
		journal  core.ToolInvocationJournal
	}{
		{name: "missing mode", metadata: map[string]string{RouteVersionKey: RouteVersion}},
		{name: "unknown mode", metadata: map[string]string{RouteVersionKey: RouteVersion, RouteModeKey: "unknown"}},
		{name: "invalid identifiers", metadata: map[string]string{RouteVersionKey: RouteVersion, RouteModeKey: string(programmatic.RoutePTCOnly), RouteCatalogToolIDKey: "same.tool", RouteExecuteToolIDKey: "same.tool"}},
		{name: "reader missing", metadata: map[string]string{RouteVersionKey: RouteVersion, RouteModeKey: string(programmatic.RouteAutoFirstAction)}, journal: writeOnlyRouteJournal{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			runtime, principal, session := routeFixture(t, test.metadata)
			runtime.ToolJournal = test.journal
			registry, err := runexecutor.NewDefaultRegistry()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := Resolve(context.Background(), Request{Runtime: runtime, Registry: registry, Principal: principal, Session: session, RunID: "run_route_reject"}); !errors.Is(err, ErrSelection) {
				t.Fatalf("Resolve err=%v, want selection failure", err)
			}
		})
	}
}

func TestResolveFreezesOptInProbeRouteV2(t *testing.T) {
	if got, want := RouteProbeImplementationRevision, "programmatic-route-projection/v2-probe-once-host-projected-ptc-choice-capacity-admission"; got != want {
		t.Fatalf("RouteProbeImplementationRevision=%q, want %q", got, want)
	}
	runtime, principal, session := routeFixture(t, map[string]string{
		RouteVersionKey: RouteProbeVersion,
		RouteModeKey:    string(programmatic.RouteAutoProbeOnce),
	})
	runtime.ToolJournal = routeReaderJournal{}
	registry, err := runexecutor.NewDefaultRegistry()
	if err != nil {
		t.Fatal(err)
	}
	decision, err := Resolve(context.Background(), Request{
		Runtime: runtime, Registry: registry, Principal: principal, Session: session, RunID: "run_probe_v2",
	})
	if err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]string{
		RouteVersionKey:        RouteProbeVersion,
		RouteModeKey:           string(programmatic.RouteAutoProbeOnce),
		RouteCatalogToolIDKey:  programmatic.DefaultCatalogToolID,
		RouteExecuteToolIDKey:  programmatic.DefaultExecuteToolID,
		RouteImplementationKey: RouteProbeImplementationRevision,
	} {
		if got := decision.CompositionMetadata[key]; got != want {
			t.Fatalf("probe route metadata %s=%q, want %q", key, got, want)
		}
	}
}

func TestSupportsRouteIdentityKeepsVersionModeAndImplementationCoupled(t *testing.T) {
	for _, test := range []struct {
		name                          string
		version, mode, implementation string
		want                          bool
	}{
		{name: "v1 direct", version: RouteVersion, mode: string(programmatic.RouteDirectOnly), implementation: RouteImplementationRevision, want: true},
		{name: "v1 ptc", version: RouteVersion, mode: string(programmatic.RoutePTCOnly), implementation: RouteImplementationRevision, want: true},
		{name: "v2 probe", version: RouteProbeVersion, mode: string(programmatic.RouteAutoProbeOnce), implementation: RouteProbeImplementationRevision, want: true},
		{name: "v2 with v1 mode", version: RouteProbeVersion, mode: string(programmatic.RouteDirectOnly), implementation: RouteProbeImplementationRevision},
		{name: "v1 with v2 implementation", version: RouteVersion, mode: string(programmatic.RouteDirectOnly), implementation: RouteProbeImplementationRevision},
		{name: "unknown version", version: "3", mode: string(programmatic.RouteAutoProbeOnce), implementation: RouteProbeImplementationRevision},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := SupportsRouteIdentity(test.version, test.mode, test.implementation); got != test.want {
				t.Fatalf("SupportsRouteIdentity(%q, %q, %q)=%t, want %t", test.version, test.mode, test.implementation, got, test.want)
			}
		})
	}
}

func TestV2RetiredImplementationRevisionFailsClosedForConfiguredAndFrozenMetadata(t *testing.T) {
	metadata := map[string]string{
		RouteVersionKey:        RouteProbeVersion,
		RouteModeKey:           string(programmatic.RouteAutoProbeOnce),
		RouteCatalogToolIDKey:  programmatic.DefaultCatalogToolID,
		RouteExecuteToolIDKey:  programmatic.DefaultExecuteToolID,
		RouteImplementationKey: "programmatic-route-projection/v2-probe-once-host-projected-ptc-choice-guidance",
	}
	if _, err := configuredRouteSelection(metadata); err == nil || !strings.Contains(err.Error(), "implementation revision is unknown") {
		t.Fatalf("retired configured v2 revision accepted: %v", err)
	}
	if _, err := frozenRouteMetadata(metadata); err == nil || !strings.Contains(err.Error(), "implementation revision is unknown") {
		t.Fatalf("retired frozen v2 revision accepted: %v", err)
	}
}

func TestResolveRejectsMixedRouteProtocolVersionAndMode(t *testing.T) {
	for _, metadata := range []map[string]string{
		{RouteVersionKey: RouteVersion, RouteModeKey: string(programmatic.RouteAutoProbeOnce)},
		{RouteVersionKey: RouteProbeVersion, RouteModeKey: string(programmatic.RouteDirectOnly)},
		{RouteVersionKey: RouteProbeVersion, RouteModeKey: string(programmatic.RouteAutoProbeOnce), RouteImplementationKey: RouteImplementationRevision},
	} {
		runtime, principal, session := routeFixture(t, metadata)
		runtime.ToolJournal = routeReaderJournal{}
		registry, err := runexecutor.NewDefaultRegistry()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := Resolve(context.Background(), Request{
			Runtime: runtime, Registry: registry, Principal: principal, Session: session, RunID: "run_mixed_protocol",
		}); !errors.Is(err, ErrSelection) {
			t.Fatalf("mixed route metadata accepted: %#v err=%v", metadata, err)
		}
	}
}

func TestResolveResumeUsesOnlyFrozenRouteWhenProfileChanges(t *testing.T) {
	metadata := map[string]string{
		RouteVersionKey: RouteVersion, RouteModeKey: string(programmatic.RoutePTCOnly),
		RouteCatalogToolIDKey: programmatic.DefaultCatalogToolID, RouteExecuteToolIDKey: programmatic.DefaultExecuteToolID,
		RouteImplementationKey: RouteImplementationRevision,
	}
	runtime, principal, session := routeFixture(t, map[string]string{RouteVersionKey: RouteVersion, RouteModeKey: string(programmatic.RouteDirectOnly)})
	runtime.ToolJournal = routeReaderJournal{}
	if _, err := session.Append("run_frozen_route", core.EvRunStart, core.RunStartData{Composition: &core.RunCompositionData{
		Profile: core.AgentProfileSnapshot{ProfileID: session.ProfileID(), Scope: session.Scope()}, Metadata: metadata,
	}}); err != nil {
		t.Fatal(err)
	}
	registry, err := runexecutor.NewDefaultRegistry()
	if err != nil {
		t.Fatal(err)
	}
	decision, err := Resolve(context.Background(), Request{
		Runtime: runtime, Registry: registry, Principal: principal, Session: session, RunID: "run_frozen_route", Resume: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if decision.CompositionMetadata[RouteModeKey] != string(programmatic.RoutePTCOnly) {
		t.Fatalf("resume route followed changed profile: %#v", decision.CompositionMetadata)
	}
}

func TestCatalogVerifierRequiresExactDurableEvidence(t *testing.T) {
	runtime, principal, session := routeFixture(t, nil)
	_ = runtime
	route := defaultRouteSelection()
	route.mode = programmatic.RoutePTCOnly
	metadata := map[string]string{}
	route.writeMetadata(metadata)
	if _, err := session.Append("run_catalog_evidence", core.EvRunStart, core.RunStartData{Composition: &core.RunCompositionData{
		Profile: core.AgentProfileSnapshot{ProfileID: session.ProfileID(), Scope: session.Scope()}, Metadata: metadata,
	}}); err != nil {
		t.Fatal(err)
	}
	call := core.ToolCall{ID: "catalog-evidence", Name: route.catalogID, Args: map[string]any{"scope": "current"}}
	if _, err := session.Append("run_catalog_evidence", core.EvAssistantMessage, core.AssistantMessageData{ToolCall: &call, ToolCalls: []core.ToolCall{call}}); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Append("run_catalog_evidence", core.EvToolCall, core.ToolCallData{CallID: call.ID, Name: call.Name, Args: call.Args}); err != nil {
		t.Fatal(err)
	}
	result := core.CapabilityResult{Content: `{"version":"ptc-ir/v1","language":"test","tools":[{}]}`, OK: true, Metadata: map[string]any{"source": "journal"}}
	if _, err := session.Append("run_catalog_evidence", core.EvToolResult, core.ToolResultData{CallID: call.ID, Content: result.Content, OK: result.OK, Metadata: result.Metadata}); err != nil {
		t.Fatal(err)
	}
	invocation, err := core.NewToolInvocation(core.RunInfo{RunID: "run_catalog_evidence", SessionID: session.ID(), Principal: principal}, call, true)
	if err != nil {
		t.Fatal(err)
	}
	journal := routeReaderJournal{records: map[core.ToolInvocation]core.ToolInvocationRecord{
		invocation: {ToolInvocation: invocation, State: core.ToolInvocationCompleted, Result: &result},
	}}
	if ok, err := verifyCatalogResult(context.Background(), session, principal, journal, "run_catalog_evidence", route.catalogID, call.ID); err != nil || !ok {
		t.Fatalf("exact catalog evidence rejected: ok=%t err=%v", ok, err)
	}
	wrong := result
	wrong.Content = `{"changed":true}`
	journal.records[invocation] = core.ToolInvocationRecord{ToolInvocation: invocation, State: core.ToolInvocationCompleted, Result: &wrong}
	if ok, err := verifyCatalogResult(context.Background(), session, principal, journal, "run_catalog_evidence", route.catalogID, call.ID); err == nil || ok {
		t.Fatalf("mismatched durable result accepted: ok=%t err=%v", ok, err)
	}
}

func TestExecuteVerifierPinsCatalogCompositionAcrossRecovery(t *testing.T) {
	_, _, session := routeFixture(t, nil)
	route := defaultRouteSelection()
	route.mode = programmatic.RoutePTCOnly
	metadata := map[string]string{}
	route.writeMetadata(metadata)
	manifest := core.CapabilityManifest{
		ID: route.executeID, Version: "1", Name: "Execute", Description: "Execute a bounded program", Kind: core.KindTool,
		Tool: &core.ToolExposure{Description: "Execute a bounded program", Parameters: map[string]any{"type": "object", "additionalProperties": false}},
	}
	composition := &core.RunCompositionData{
		Profile: core.AgentProfileSnapshot{ProfileID: session.ProfileID(), Scope: session.Scope()}, Metadata: metadata,
		Capabilities: []core.SnapshotCapability{{Manifest: manifest, Source: session.Scope(), ProviderRevision: "execute-v1"}},
	}
	if _, err := session.Append("run_execute_evidence", core.EvRunStart, core.RunStartData{Composition: composition}); err != nil {
		t.Fatal(err)
	}
	catalog := core.ToolCall{ID: "catalog-evidence", Name: route.catalogID, Args: map[string]any{"scope": "current"}}
	if _, err := session.Append("run_execute_evidence", core.EvAssistantMessage, core.AssistantMessageData{ToolCall: &catalog, ToolCalls: []core.ToolCall{catalog}}); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Append("run_execute_evidence", core.EvToolCall, core.ToolCallData{CallID: catalog.ID, Name: catalog.Name, Args: catalog.Args}); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Append("run_execute_evidence", core.EvToolResult, core.ToolResultData{CallID: catalog.ID, Content: `{"version":"ptc-ir/v1","language":"test","tools":[{}]}`, OK: true}); err != nil {
		t.Fatal(err)
	}
	schema := schemaFromManifest(manifest)
	if ok, err := verifyExecuteTool(session, "run_execute_evidence", route, schema); err != nil || !ok {
		t.Fatalf("current execute schema rejected: ok=%t err=%v", ok, err)
	}
	if _, err := session.Append("run_execute_evidence", core.EvRunResume, core.RunResumeData{Composition: &core.RunCompositionData{
		Profile: core.AgentProfileSnapshot{ProfileID: session.ProfileID(), Scope: session.Scope()}, Metadata: metadata,
		Capabilities: []core.SnapshotCapability{{Manifest: manifest, Source: session.Scope(), ProviderRevision: "execute-v2"}},
	}}); err != nil {
		t.Fatal(err)
	}
	if ok, err := verifyExecuteTool(session, "run_execute_evidence", route, schema); err == nil || ok {
		t.Fatalf("same-schema execute provider drift accepted: ok=%t err=%v", ok, err)
	}
}

// TestRecoveryProjectionFailsClosed keeps the recovery decision at the same
// boundary used by the server: the route wrapper may expose execute only after
// it can correlate the catalog result in the session with a completed durable
// journal record. A missing, conflicting, or duplicated record is therefore a
// route-selection fact, not an instruction for the model to repair.
func TestRecoveryProjectionFailsClosed(t *testing.T) {
	for _, test := range []struct {
		name      string
		mode      programmatic.RouteMode
		journal   func(core.ToolInvocation, core.CapabilityResult) core.ToolInvocationJournal
		duplicate bool
		content   string
		want      []string
		wantErr   bool
	}{
		{
			name: "auto verified catalog exposes execute", mode: programmatic.RouteAutoFirstAction,
			journal: func(invocation core.ToolInvocation, result core.CapabilityResult) core.ToolInvocationJournal {
				return routeReaderJournal{records: map[core.ToolInvocation]core.ToolInvocationRecord{invocation: {ToolInvocation: invocation, State: core.ToolInvocationCompleted, Result: &result}}}
			}, want: []string{programmatic.DefaultExecuteToolID},
		},
		{
			name: "ptc verified catalog exposes execute", mode: programmatic.RoutePTCOnly,
			journal: func(invocation core.ToolInvocation, result core.CapabilityResult) core.ToolInvocationJournal {
				return routeReaderJournal{records: map[core.ToolInvocation]core.ToolInvocationRecord{invocation: {ToolInvocation: invocation, State: core.ToolInvocationCompleted, Result: &result}}}
			}, want: []string{programmatic.DefaultExecuteToolID},
		},
		{
			name: "direct ignores catalog and remains direct", mode: programmatic.RouteDirectOnly,
			journal: func(core.ToolInvocation, core.CapabilityResult) core.ToolInvocationJournal {
				return writeOnlyRouteJournal{}
			},
			want: []string{"route.read"},
		},
		{
			name: "auto missing journal record cannot unlock execute", mode: programmatic.RouteAutoFirstAction,
			journal: func(core.ToolInvocation, core.CapabilityResult) core.ToolInvocationJournal {
				return routeReaderJournal{}
			}, wantErr: true,
		},
		{
			name: "ptc conflicting journal record cannot unlock execute", mode: programmatic.RoutePTCOnly,
			journal: func(invocation core.ToolInvocation, result core.CapabilityResult) core.ToolInvocationJournal {
				conflict := result
				conflict.Content = `{"changed":true}`
				return routeReaderJournal{records: map[core.ToolInvocation]core.ToolInvocationRecord{invocation: {ToolInvocation: invocation, State: core.ToolInvocationCompleted, Result: &conflict}}}
			}, wantErr: true,
		},
		{
			name: "auto duplicate catalog evidence cannot unlock execute", mode: programmatic.RouteAutoFirstAction,
			journal: func(invocation core.ToolInvocation, result core.CapabilityResult) core.ToolInvocationJournal {
				return routeReaderJournal{records: map[core.ToolInvocation]core.ToolInvocationRecord{invocation: {ToolInvocation: invocation, State: core.ToolInvocationCompleted, Result: &result}}}
			}, duplicate: true, wantErr: true,
		},
		{
			name: "ptc invalid catalog envelope cannot retry catalog", mode: programmatic.RoutePTCOnly,
			journal: func(invocation core.ToolInvocation, result core.CapabilityResult) core.ToolInvocationJournal {
				return routeReaderJournal{records: map[core.ToolInvocation]core.ToolInvocationRecord{invocation: {ToolInvocation: invocation, State: core.ToolInvocationCompleted, Result: &result}}}
			}, content: "not-json", wantErr: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			runtime, principal, session := routeFixture(t, nil)
			route := defaultRouteSelection()
			route.mode = test.mode
			call := core.ToolCall{ID: "catalog-recovery", Name: route.catalogID, Args: map[string]any{}}
			result := core.CapabilityResult{Content: `{"version":"ptc-ir/v1","language":"test","tools":[{}]}`, OK: true}
			if test.content != "" {
				result.Content = test.content
			}
			appendCatalogHistory(t, session, "run_recovery", route, call, result, test.duplicate)
			invocation, err := core.NewToolInvocation(core.RunInfo{RunID: "run_recovery", SessionID: session.ID(), ProfileID: session.ProfileID(), Principal: principal}, call, true)
			if err != nil {
				t.Fatal(err)
			}
			runtime.ToolJournal = test.journal(invocation, result)
			wrapped, err := runtimeWithRouteProjection(runtime, session, principal, "run_recovery", true, route)
			if err != nil {
				t.Fatal(err)
			}
			model := &routeCaptureModel{}
			wrapped.Models = core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) { return model, nil })
			// Reapply the wrapper after substituting the test-only inner adapter.
			wrapped, err = runtimeWithRouteProjection(wrapped, session, principal, "run_recovery", true, route)
			if err != nil {
				t.Fatal(err)
			}
			adapter, err := wrapped.Models.ResolveModel(context.Background(), core.ModelSelection{Provider: "route", Model: "route"})
			if err != nil {
				t.Fatal(err)
			}
			messages := []core.ChatMessage{{Role: core.RoleUser, Content: "continue"}, {Role: core.RoleAssistant, ToolCall: &call, ToolCalls: []core.ToolCall{call}}, {Role: core.RoleTool, ToolCallID: call.ID, Content: result.Content}}
			err = adapter.Stream(context.Background(), core.GenerateOptions{ModelCall: core.ModelCallRequest{RunID: "run_recovery"}, Messages: messages, Tools: recoveryToolSchemas(route)}, nil)
			if test.wantErr {
				if !errors.Is(err, programmatic.ErrInvalidRouteProjection) || model.calls != 0 {
					t.Fatalf("err=%v inner calls=%d", err, model.calls)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got := routeSchemaNames(model.tools); !reflect.DeepEqual(got, test.want) {
				t.Fatalf("visible tools=%v, want %v", got, test.want)
			}
		})
	}
}

func appendCatalogHistory(t *testing.T, session *core.Session, runID string, route routeSelection, call core.ToolCall, result core.CapabilityResult, duplicate bool) {
	t.Helper()
	metadata := map[string]string{}
	route.writeMetadata(metadata)
	execute := core.CapabilityManifest{ID: route.executeID, Version: "1", Name: "execute", Description: "execute", Kind: core.KindTool, Tool: &core.ToolExposure{Description: "execute", Parameters: map[string]any{"type": "object"}}}
	composition := &core.RunCompositionData{
		Profile: core.AgentProfileSnapshot{ProfileID: session.ProfileID(), Scope: session.Scope()}, Metadata: metadata,
		Capabilities: []core.SnapshotCapability{{Manifest: execute, Source: session.Scope(), ProviderRevision: "execute-v1"}},
	}
	for _, event := range []struct {
		kind core.SessionEventType
		data any
	}{
		{core.EvRunStart, core.RunStartData{Composition: composition}},
		{core.EvUserMessage, core.UserMessageData{Text: "continue"}},
		{core.EvAssistantMessage, core.AssistantMessageData{ToolCall: &call, ToolCalls: []core.ToolCall{call}}},
		{core.EvToolCall, core.ToolCallData{CallID: call.ID, Name: call.Name, Args: call.Args}},
		{core.EvToolResult, core.ToolResultData{CallID: call.ID, Content: result.Content, OK: result.OK, Metadata: result.Metadata}},
	} {
		if _, err := session.Append(runID, event.kind, event.data); err != nil {
			t.Fatal(err)
		}
	}
	if duplicate {
		if _, err := session.Append(runID, core.EvAssistantMessage, core.AssistantMessageData{ToolCall: &call, ToolCalls: []core.ToolCall{call}}); err != nil {
			t.Fatal(err)
		}
	}
}

type routeCaptureModel struct {
	tools []core.ToolSchema
	calls int
}

func (*routeCaptureModel) Provider() string { return "route-capture" }
func (m *routeCaptureModel) Stream(_ context.Context, options core.GenerateOptions, _ func(core.StreamChunk)) error {
	m.calls++
	m.tools = append([]core.ToolSchema(nil), options.Tools...)
	return nil
}

func recoveryToolSchemas(route routeSelection) []core.ToolSchema {
	return []core.ToolSchema{
		{Name: route.catalogID, Description: "catalog", Parameters: map[string]any{"type": "object"}},
		{Name: route.executeID, Description: "execute", Parameters: map[string]any{"type": "object"}},
		{Name: "route.read", Description: "read", Parameters: map[string]any{"type": "object"}},
	}
}

func routeSchemaNames(schemas []core.ToolSchema) []string {
	names := make([]string, 0, len(schemas))
	for _, schema := range schemas {
		names = append(names, schema.Name)
	}
	return names
}

type writeOnlyRouteJournal struct{}

func (writeOnlyRouteJournal) BeginToolInvocation(context.Context, core.ToolInvocation) (core.ToolInvocationRecord, core.ToolInvocationDecision, error) {
	return core.ToolInvocationRecord{}, core.ToolInvocationExecuteNew, nil
}
func (writeOnlyRouteJournal) CompleteToolInvocation(context.Context, core.ToolInvocation, core.CapabilityResult) (core.ToolInvocationRecord, error) {
	return core.ToolInvocationRecord{}, nil
}
func (writeOnlyRouteJournal) MarkToolInvocationUncertain(context.Context, core.ToolInvocation, string) error {
	return nil
}

type routeReaderJournal struct {
	records map[core.ToolInvocation]core.ToolInvocationRecord
}

func (j routeReaderJournal) BeginToolInvocation(context.Context, core.ToolInvocation) (core.ToolInvocationRecord, core.ToolInvocationDecision, error) {
	return core.ToolInvocationRecord{}, core.ToolInvocationExecuteNew, nil
}
func (j routeReaderJournal) CompleteToolInvocation(context.Context, core.ToolInvocation, core.CapabilityResult) (core.ToolInvocationRecord, error) {
	return core.ToolInvocationRecord{}, nil
}
func (j routeReaderJournal) MarkToolInvocationUncertain(context.Context, core.ToolInvocation, string) error {
	return nil
}
func (j routeReaderJournal) GetToolInvocation(_ context.Context, invocation core.ToolInvocation) (core.ToolInvocationRecord, bool, error) {
	record, found := j.records[invocation]
	return record, found, nil
}

var _ core.ToolInvocationReader = routeReaderJournal{}
