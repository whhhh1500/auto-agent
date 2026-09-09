package executionroute

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/whhhh1500/auto-agent/pkg/adapter/programmatic/toolcapability"
	"github.com/whhhh1500/auto-agent/pkg/app/programmatic"
	"github.com/whhhh1500/auto-agent/pkg/app/runexecutor"
	"github.com/whhhh1500/auto-agent/pkg/core"
)

func TestV2RuntimeProbeDirectBatchCompletesInThreeCalls(t *testing.T) {
	fixture := newV2RuntimeFixture(t, v2RuntimeDirect, false, false)
	result, err := fixture.run(t, "run_v2_direct")
	if err != nil || result.Status != core.RunCompleted || result.Answer != "direct final" {
		t.Fatalf("RunTurn result=%#v err=%v", result, err)
	}
	fixture.model.requireCalls(t, [][]string{
		{"records.detail", "records.inventory"},
		{"records.detail", programmatic.DefaultExecuteToolID},
		nil,
	})
	if got := fixture.probe.calls(); got != 1 {
		t.Fatalf("neutral probe calls=%d, want 1", got)
	}
	if got := fixture.detail.ids(); !reflect.DeepEqual(got, []string{"record-17", "record-18"}) {
		t.Fatalf("direct effects=%v, want planned batch", got)
	}
	if fixture.probe.readsAtExecution != 0 {
		t.Fatalf("new initial probe required durable plan evidence before execution: reads=%d", fixture.probe.readsAtExecution)
	}
}

func TestV2RuntimeProbePTCTransformsGenericExecuteAndCompletesInThreeCalls(t *testing.T) {
	fixture := newV2RuntimeFixture(t, v2RuntimePTC, false, false)
	result, err := fixture.run(t, "run_v2_ptc")
	if err != nil || result.Status != core.RunCompleted || result.Answer != "ptc final" {
		t.Fatalf("RunTurn result=%#v err=%v", result, err)
	}
	fixture.model.requireCalls(t, [][]string{
		{"records.detail", "records.inventory"},
		{"records.detail", programmatic.DefaultExecuteToolID},
		nil,
	})
	if got := fixture.probe.calls(); got != 1 {
		t.Fatalf("neutral probe calls=%d, want 1", got)
	}
	if got := fixture.detail.ids(); !reflect.DeepEqual(got, []string{"record-17", "record-18"}) {
		t.Fatalf("PTC child effects=%v, want frozen targets", got)
	}
	args := toolCallArgs(t, fixture.session, "run_v2_ptc", "execute")
	if _, exists := args["selection"]; exists {
		t.Fatalf("Core persisted route-specific selection instead of generic execute args: %#v", args)
	}
	input, ok := args["input"].(map[string]any)
	if !ok {
		t.Fatalf("generic execute input=%#v", args["input"])
	}
	targets, ok := input["targets"].([]any)
	if !ok || len(targets) != 2 {
		t.Fatalf("generic execute targets=%#v", input["targets"])
	}
	for index, want := range []string{"record-17", "record-18"} {
		target, ok := targets[index].(map[string]any)
		if !ok || target["args"].(map[string]any)["id"] != want {
			t.Fatalf("generic target %d=%#v, want %q", index, targets[index], want)
		}
	}
}

func TestV2RuntimeFailsClosedOnProbeJournalMismatchAndOversizedExecuteReturn(t *testing.T) {
	for _, test := range []struct {
		name          string
		mismatchRead  bool
		oversizedPTC  bool
		wantModelCall int
	}{
		{name: "journal mismatch", mismatchRead: true, wantModelCall: 1},
		{name: "oversized execute return", oversizedPTC: true, wantModelCall: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newV2RuntimeFixture(t, v2RuntimePTC, test.mismatchRead, test.oversizedPTC)
			result, err := fixture.run(t, "run_v2_closed")
			if err == nil || result.Status != core.RunFailed {
				t.Fatalf("fail-closed result=%#v err=%v", result, err)
			}
			if got := fixture.model.calls(); got != test.wantModelCall {
				t.Fatalf("model calls=%d, want %d", got, test.wantModelCall)
			}
			if test.mismatchRead && len(fixture.detail.ids()) != 0 {
				t.Fatalf("journal mismatch admitted follow-up effects: %v", fixture.detail.ids())
			}
		})
	}
}

func TestV2ChoiceAdmissionRejectsOverCapacityBatchesBeforeChoiceEffects(t *testing.T) {
	tests := []struct {
		name      string
		configure func(t *testing.T, fixture *v2RuntimeFixture)
		choice    []core.ToolCall
		frozenCap int
		detailCap int
	}{
		{
			name:      "direct batch exceeds plan selection and followup per-turn budget",
			detailCap: 1,
			choice: []core.ToolCall{
				{ID: "detail-17", Name: "records.detail", Args: map[string]any{"id": "record-17"}},
				{ID: "detail-18", Name: "records.detail", Args: map[string]any{"id": "record-18"}},
			},
		},
		{
			name: "probe leaves insufficient frozen global capacity for direct batch",
			configure: func(t *testing.T, fixture *v2RuntimeFixture) {
				fixture.setPolicyToolCallCap(t, 2)
			},
			choice: []core.ToolCall{
				{ID: "detail-17", Name: "records.detail", Args: map[string]any{"id": "record-17"}},
				{ID: "detail-18", Name: "records.detail", Args: map[string]any{"id": "record-18"}},
			},
			frozenCap: 2,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newV2RuntimeFixtureWithDetailBudget(t, v2RuntimeDirect, false, false, test.detailCap)
			if test.configure != nil {
				test.configure(t, fixture)
			}
			fixture.model.setChoice(test.choice...)
			const runID = "run_v2_choice_capacity_reject"
			result, err := fixture.run(t, runID)
			if err == nil || result.Status != core.RunFailed {
				t.Fatalf("over-capacity choice result=%#v err=%v", result, err)
			}
			if got := fixture.model.calls(); got != 2 {
				t.Fatalf("rejected choice model calls=%d, want probe plus rejected choice", got)
			}
			if got := fixture.detail.ids(); len(got) != 0 {
				t.Fatalf("rejected choice reached the detail provider: %v", got)
			}
			assertNoV2ChoiceJournalEffects(t, fixture, runID)
			if test.frozenCap != 0 {
				frozen, freezeErr := frozenProbeComposition(fixture.session.Events(), runID)
				if freezeErr != nil || frozen.composition.MaxToolCalls != test.frozenCap {
					t.Fatalf("frozen max tool calls=%d err=%v, want %d", frozen.composition.MaxToolCalls, freezeErr, test.frozenCap)
				}
			}
			if test.detailCap != 0 {
				frozen, freezeErr := frozenProbeComposition(fixture.session.Events(), runID)
				if freezeErr != nil {
					t.Fatal(freezeErr)
				}
				capability, capErr := frozen.capability("records.detail")
				if capErr != nil || capability.Manifest.PerTurnBudget != test.detailCap {
					t.Fatalf("frozen follow-up per-turn budget=%d err=%v, want %d", capability.Manifest.PerTurnBudget, capErr, test.detailCap)
				}
			}
			observation := ObserveSelectedRoute(context.Background(), fixture.session, fixture.principal, fixture.journal, runID)
			if observation.Route != SelectedRouteUnavailable || observation.Status != SelectionStatusUnavailable {
				t.Fatalf("rejected choice was verified as a selected route: %+v", observation)
			}
		})
	}
}

func TestV2ChoiceAdmissionReservesExecuteParentAndChildrenSeparatelyFromDirect(t *testing.T) {
	t.Run("direct one remains legal at two total calls", func(t *testing.T) {
		fixture := newV2RuntimeFixture(t, v2RuntimeDirect, false, false)
		fixture.setPolicyToolCallCap(t, 2)
		fixture.model.setChoice(core.ToolCall{ID: "detail-17", Name: "records.detail", Args: map[string]any{"id": "record-17"}})
		const runID = "run_v2_capacity_direct_one"
		result, err := fixture.run(t, runID)
		if err != nil || result.Status != core.RunCompleted || result.Answer != "direct final" {
			t.Fatalf("legal direct result=%#v err=%v", result, err)
		}
		if got := fixture.detail.ids(); !reflect.DeepEqual(got, []string{"record-17"}) {
			t.Fatalf("legal direct effects=%v", got)
		}
		observation := ObserveSelectedRoute(context.Background(), fixture.session, fixture.principal, fixture.journal, runID)
		if observation.Route != SelectedRouteDirect || observation.Status != SelectionStatusVerified {
			t.Fatalf("legal direct route observation=%+v", observation)
		}
	})

	t.Run("execute one selection needs parent plus child and is rejected", func(t *testing.T) {
		fixture := newV2RuntimeFixture(t, v2RuntimePTC, false, false)
		fixture.setPolicyToolCallCap(t, 2)
		fixture.model.setChoice(core.ToolCall{ID: "execute", Name: programmatic.DefaultExecuteToolID, Args: map[string]any{
			"projection": "name", "selection": []any{float64(0)},
		}})
		const runID = "run_v2_capacity_execute_one"
		result, err := fixture.run(t, runID)
		if err == nil || result.Status != core.RunFailed {
			t.Fatalf("over-capacity execute result=%#v err=%v", result, err)
		}
		if got := fixture.detail.ids(); len(got) != 0 {
			t.Fatalf("rejected execute reached the child provider: %v", got)
		}
		assertNoV2ChoiceJournalEffects(t, fixture, runID)
		observation := ObserveSelectedRoute(context.Background(), fixture.session, fixture.principal, fixture.journal, runID)
		if observation.Route != SelectedRouteUnavailable || observation.Status != SelectionStatusUnavailable {
			t.Fatalf("rejected execute was verified as a selected route: %+v", observation)
		}
	})
}

// The v2 Runtime route must not alter the legacy default selection. This is
// intentionally an end-to-end assertion through Resolve and Sequential.
func TestV2RuntimeDoesNotChangeV1DefaultDirectSurface(t *testing.T) {
	fixture := newV2RuntimeFixture(t, v2RuntimeV1Direct, false, false)
	result, err := fixture.run(t, "run_v1_default")
	if err != nil || result.Status != core.RunCompleted || result.Answer != "v1 final" {
		t.Fatalf("RunTurn result=%#v err=%v", result, err)
	}
	fixture.model.requireCalls(t, [][]string{{"records.detail", "records.inventory"}, {"records.detail", "records.inventory"}, {"records.detail", "records.inventory"}})
	if got := fixture.probe.calls(); got != 1 {
		t.Fatalf("v1 direct probe-shaped tool calls=%d, want 1", got)
	}
}

type v2RuntimeMode uint8

const (
	v2RuntimeDirect v2RuntimeMode = iota
	v2RuntimePTC
	v2RuntimeV1Direct
)

type v2RuntimeFixture struct {
	runtime   *core.Runtime
	principal core.Principal
	session   *core.Session
	registry  *runexecutor.Registry
	model     *v2RuntimeScriptModel
	journal   *v2RuntimeJournal
	probe     *v2RuntimeProbe
	detail    *v2RuntimeDetail
}

func (f *v2RuntimeFixture) setPolicyToolCallCap(t *testing.T, cap int) {
	t.Helper()
	policy := core.NewPolicyRegistry()
	if err := policy.Bind(core.PolicyLayer{Scope: f.session.Scope(), MaxToolCalls: &cap}); err != nil {
		t.Fatal(err)
	}
	f.runtime.Policy = policy
}

func newV2RuntimeFixture(t *testing.T, mode v2RuntimeMode, mismatchRead, oversizedPTC bool) *v2RuntimeFixture {
	return newV2RuntimeFixtureWithDetailBudget(t, mode, mismatchRead, oversizedPTC, 0)
}

func newV2RuntimeFixtureWithDetailBudget(t *testing.T, mode v2RuntimeMode, mismatchRead, oversizedPTC bool, detailBudget int) *v2RuntimeFixture {
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
	sessionScope, err := user.Child(core.ScopeRef{Kind: core.ScopeSession, ID: "v2-runtime-session"})
	if err != nil {
		t.Fatal(err)
	}
	principal := core.Principal{TenantID: "tenant", SubjectID: "user", Scope: user}
	session, err := core.NewSession(core.SessionOptions{ID: "v2-runtime-session", ProfileID: "v2.runtime", Principal: principal, Scope: sessionScope})
	if err != nil {
		t.Fatal(err)
	}

	journal := newV2RuntimeJournal(mismatchRead)
	probe := &v2RuntimeProbe{journal: journal}
	detail := &v2RuntimeDetail{oversized: oversizedPTC, perTurnBudget: detailBudget}
	execute, err := toolcapability.NewExecuteCapability(programmatic.DefaultExecuteToolID)
	if err != nil {
		t.Fatal(err)
	}
	capabilities := core.NewCapabilityRegistry()
	for _, capability := range []core.Capability{probe, detail, execute} {
		if err := capabilities.Register(product, capability); err != nil {
			t.Fatal(err)
		}
	}

	metadata := map[string]string{}
	if mode != v2RuntimeV1Direct {
		metadata[RouteVersionKey] = RouteProbeVersion
		metadata[RouteModeKey] = string(programmatic.RouteAutoProbeOnce)
	}
	steps, toolCalls := 4, 8
	profiles := core.NewAgentProfileRegistry()
	selection := core.ModelSelection{Provider: "v2-runtime", Model: "script"}
	if err := profiles.Bind(core.AgentProfileLayer{
		Scope: product, ProfileID: "v2.runtime", Model: &selection, MaxSteps: &steps, MaxToolCalls: &toolCalls,
		AddCapabilities: []string{"records.inventory", "records.detail", programmatic.DefaultExecuteToolID}, Metadata: metadata,
	}); err != nil {
		t.Fatal(err)
	}
	model := &v2RuntimeScriptModel{mode: mode, oversizedPTC: oversizedPTC}
	runtime := &core.Runtime{
		Capabilities: capabilities, Profiles: profiles, ToolJournal: journal,
		Models: core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) { return model, nil }),
	}
	registry, err := runexecutor.NewDefaultRegistry()
	if err != nil {
		t.Fatal(err)
	}
	return &v2RuntimeFixture{runtime: runtime, principal: principal, session: session, registry: registry, model: model, journal: journal, probe: probe, detail: detail}
}

func (f *v2RuntimeFixture) run(t *testing.T, runID string) (core.TurnResult, error) {
	t.Helper()
	decision, err := Resolve(context.Background(), Request{Runtime: f.runtime, Registry: f.registry, Principal: f.principal, Session: f.session, RunID: runID})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if decision.Metadata.ID != runexecutor.SequentialID {
		t.Fatalf("executor=%#v, want sequential", decision.Metadata)
	}
	return decision.Executor.RunTurn(context.Background(), f.principal, f.session, core.TurnInput{
		RunID: runID, Text: "resolve the selected records", CompositionMetadata: decision.CompositionMetadata,
	}, nil)
}

type v2RuntimeProbe struct {
	journal          *v2RuntimeJournal
	mu               sync.Mutex
	callsN           int
	readsAtExecution int
}

func (*v2RuntimeProbe) ArtifactRevision() string { return "v2-runtime-probe/v1" }
func (*v2RuntimeProbe) Manifest() core.CapabilityManifest {
	return core.CapabilityManifest{
		ID: "records.inventory", Version: "1", Name: "Neutral inventory", Kind: core.KindTool, Idempotent: true, MaxOutputBytes: 4096,
		Tool:     &core.ToolExposure{Description: "Read candidate inventory", Parameters: emptyV2RuntimeObjectSchema()},
		Metadata: map[string]string{programmatic.ProbeManifestKey: programmatic.ProbeManifestVersion},
	}
}
func (p *v2RuntimeProbe) Execute(_ context.Context, request core.CapabilityRequest) (core.CapabilityResult, error) {
	if err := core.RequireAcceptedInvocation(request); err != nil {
		return core.CapabilityResult{}, err
	}
	p.mu.Lock()
	p.callsN++
	p.readsAtExecution = p.journal.reads()
	p.mu.Unlock()
	return core.CapabilityResult{Content: "two approved records", OK: true, Metadata: map[string]any{
		programmatic.ProbeFactsMetadataKey: map[string]any{
			"version": programmatic.ProbeFactsVersion, "followup_capability_id": "records.detail", "max_model_return_bytes": float64(4096),
			"candidates": []any{
				map[string]any{"label": "record-17", "args": map[string]any{"id": "record-17"}, "facts": map[string]any{"active": true}},
				map[string]any{"label": "record-18", "args": map[string]any{"id": "record-18"}, "facts": map[string]any{"active": true}},
			},
		},
	}}, nil
}
func (p *v2RuntimeProbe) calls() int { p.mu.Lock(); defer p.mu.Unlock(); return p.callsN }

type v2RuntimeDetail struct {
	oversized     bool
	mu            sync.Mutex
	callIDs       []string
	revision      string
	perTurnBudget int
}

func (d *v2RuntimeDetail) ArtifactRevision() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.revision != "" {
		return d.revision
	}
	return "v2-runtime-detail/v1"
}

func (d *v2RuntimeDetail) setArtifactRevision(revision string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.revision = revision
}
func (d *v2RuntimeDetail) Manifest() core.CapabilityManifest {
	d.mu.Lock()
	perTurnBudget := d.perTurnBudget
	d.mu.Unlock()
	return core.CapabilityManifest{
		ID: "records.detail", Version: "1", Name: "Programmatic record detail", Kind: core.KindTool, Idempotent: true, MaxOutputBytes: 8192, PerTurnBudget: perTurnBudget,
		Tool: &core.ToolExposure{Description: "Read a selected record", Parameters: v2RuntimeDetailSchema()},
		OutputSchema: map[string]any{"type": "object", "required": []any{"id", "name"}, "additionalProperties": false, "properties": map[string]any{
			"id": map[string]any{"type": "string", "maxLength": 16}, "name": map[string]any{"type": "string", "maxLength": 32},
		}},
		Metadata: map[string]string{programmatic.ExposureKey: programmatic.ExposureVersion},
	}
}

func (d *v2RuntimeDetail) Execute(_ context.Context, request core.CapabilityRequest) (core.CapabilityResult, error) {
	if err := core.RequireAcceptedInvocation(request); err != nil {
		return core.CapabilityResult{}, err
	}
	id, _ := request.Args["id"].(string)
	d.mu.Lock()
	d.callIDs = append(d.callIDs, id)
	d.mu.Unlock()
	if d.oversized {
		return core.CapabilityResult{Content: fmt.Sprintf(`{"id":%q,"payload":%q}`, id, strings.Repeat("x", 5000)), OK: true}, nil
	}
	return core.CapabilityResult{Content: fmt.Sprintf(`{"id":%q,"name":%q}`, id, "name-"+id), OK: true}, nil
}
func (d *v2RuntimeDetail) ids() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.callIDs...)
}

type v2RuntimeScriptModel struct {
	mode         v2RuntimeMode
	oversizedPTC bool
	mu           sync.Mutex
	surfaces     [][]string
	choice       []core.ToolCall
}

func (*v2RuntimeScriptModel) Provider() string         { return "v2-runtime-script" }
func (*v2RuntimeScriptModel) ArtifactRevision() string { return "v2-runtime-script/v1" }
func (m *v2RuntimeScriptModel) Stream(_ context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	m.mu.Lock()
	phase := len(m.surfaces)
	m.surfaces = append(m.surfaces, v2RuntimeToolNames(options.Tools))
	choice := append([]core.ToolCall(nil), m.choice...)
	m.mu.Unlock()
	switch phase {
	case 0:
		return emitV2RuntimeToolCalls(emit, core.ToolCall{ID: "probe", Name: "records.inventory", Args: map[string]any{}})
	case 1:
		if choice != nil {
			return emitV2RuntimeToolCalls(emit, choice...)
		}
		switch m.mode {
		case v2RuntimeDirect:
			return emitV2RuntimeToolCalls(emit,
				core.ToolCall{ID: "detail-17", Name: "records.detail", Args: map[string]any{"id": "record-17"}},
				core.ToolCall{ID: "detail-18", Name: "records.detail", Args: map[string]any{"id": "record-18"}},
			)
		case v2RuntimePTC:
			return emitV2RuntimeToolCalls(emit, core.ToolCall{ID: "execute", Name: programmatic.DefaultExecuteToolID, Args: map[string]any{
				"projection": "name", "selection": []any{float64(0), float64(1)},
			}})
		case v2RuntimeV1Direct:
			return emitV2RuntimeToolCalls(emit, core.ToolCall{ID: "v1-detail", Name: "records.detail", Args: map[string]any{"id": "record-17"}})
		}
	case 2:
		answer := "direct final"
		if m.mode == v2RuntimePTC {
			answer = "ptc final"
		}
		if m.mode == v2RuntimeV1Direct {
			answer = "v1 final"
		}
		emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: answer})
		emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop})
		return nil
	}
	return errors.New("script model received an unexpected fourth call")
}
func (m *v2RuntimeScriptModel) calls() int { m.mu.Lock(); defer m.mu.Unlock(); return len(m.surfaces) }
func (m *v2RuntimeScriptModel) setChoice(calls ...core.ToolCall) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.choice = append([]core.ToolCall(nil), calls...)
}
func (m *v2RuntimeScriptModel) requireCalls(t *testing.T, want [][]string) {
	t.Helper()
	m.mu.Lock()
	got := append([][]string(nil), m.surfaces...)
	m.mu.Unlock()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("model tool surfaces=%#v, want %#v", got, want)
	}
}

func emitV2RuntimeToolCalls(emit func(core.StreamChunk), calls ...core.ToolCall) error {
	emit(core.StreamChunk{Kind: core.StreamKindAssistant, ToolCalls: calls})
	emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishToolCalls})
	return nil
}

type v2RuntimeJournal struct {
	mu           sync.Mutex
	records      map[string]core.ToolInvocationRecord
	mismatchRead bool
	readCalls    int
}

func newV2RuntimeJournal(mismatchRead bool) *v2RuntimeJournal {
	return &v2RuntimeJournal{records: map[string]core.ToolInvocationRecord{}, mismatchRead: mismatchRead}
}
func v2RuntimeJournalKey(invocation core.ToolInvocation) string {
	return invocation.SessionID + "\x00" + invocation.RunID + "\x00" + invocation.CallID
}
func (j *v2RuntimeJournal) BeginToolInvocation(_ context.Context, invocation core.ToolInvocation) (core.ToolInvocationRecord, core.ToolInvocationDecision, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	key := v2RuntimeJournalKey(invocation)
	if prior, found := j.records[key]; found {
		if prior.ToolInvocation != invocation {
			return core.CloneToolInvocationRecord(prior), core.ToolInvocationConflict, nil
		}
		if prior.State == core.ToolInvocationCompleted {
			return core.CloneToolInvocationRecord(prior), core.ToolInvocationReplay, nil
		}
		return core.CloneToolInvocationRecord(prior), core.ToolInvocationExecuteRetry, nil
	}
	now := time.Now().UTC()
	record := core.ToolInvocationRecord{ToolInvocation: invocation, State: core.ToolInvocationStarted, StartedAt: now, UpdatedAt: now}
	j.records[key] = record
	return record, core.ToolInvocationExecuteNew, nil
}
func (j *v2RuntimeJournal) CompleteToolInvocation(_ context.Context, invocation core.ToolInvocation, result core.CapabilityResult) (core.ToolInvocationRecord, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	key := v2RuntimeJournalKey(invocation)
	record, found := j.records[key]
	if !found || record.ToolInvocation != invocation {
		return core.ToolInvocationRecord{}, errors.New("in-memory journal invocation conflict")
	}
	resultCopy := result
	now := time.Now().UTC()
	record.State, record.Result, record.CompletedAt, record.UpdatedAt = core.ToolInvocationCompleted, &resultCopy, now, now
	j.records[key] = record
	return core.CloneToolInvocationRecord(record), nil
}
func (j *v2RuntimeJournal) MarkToolInvocationUncertain(_ context.Context, invocation core.ToolInvocation, code string) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	record, found := j.records[v2RuntimeJournalKey(invocation)]
	if !found || record.ToolInvocation != invocation {
		return errors.New("in-memory journal invocation conflict")
	}
	record.State, record.ErrorCode, record.UpdatedAt = core.ToolInvocationUncertain, code, time.Now().UTC()
	j.records[v2RuntimeJournalKey(invocation)] = record
	return nil
}
func (j *v2RuntimeJournal) GetToolInvocation(_ context.Context, invocation core.ToolInvocation) (core.ToolInvocationRecord, bool, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.readCalls++
	record, found := j.records[v2RuntimeJournalKey(invocation)]
	if !found || record.ToolInvocation != invocation {
		return core.ToolInvocationRecord{}, false, nil
	}
	if j.mismatchRead && invocation.CapabilityID == "records.inventory" {
		mismatch := core.CloneToolInvocationRecord(record)
		result := *mismatch.Result
		result.Content = "journal result was changed"
		mismatch.Result = &result
		return mismatch, true, nil
	}
	return core.CloneToolInvocationRecord(record), true, nil
}
func (j *v2RuntimeJournal) reads() int { j.mu.Lock(); defer j.mu.Unlock(); return j.readCalls }

func toolCallArgs(t *testing.T, session *core.Session, runID, callID string) map[string]any {
	t.Helper()
	for _, event := range session.Events() {
		if event.RunID != runID || event.Type != core.EvToolCall {
			continue
		}
		var data core.ToolCallData
		if err := json.Unmarshal(event.Data, &data); err != nil {
			t.Fatal(err)
		}
		if data.CallID == callID {
			return data.Args
		}
	}
	t.Fatalf("tool call %q not found", callID)
	return nil
}

func v2RuntimeToolNames(tools []core.ToolSchema) []string {
	if len(tools) == 0 {
		return nil
	}
	out := make([]string, len(tools))
	for index, tool := range tools {
		out[index] = tool.Name
	}
	return out
}
func emptyV2RuntimeObjectSchema() map[string]any {
	return map[string]any{"type": "object", "additionalProperties": false}
}
func v2RuntimeDetailSchema() map[string]any {
	return map[string]any{"type": "object", "additionalProperties": false, "required": []any{"id"}, "properties": map[string]any{"id": map[string]any{"type": "string", "minLength": 1}}}
}

func assertNoV2ChoiceJournalEffects(t *testing.T, fixture *v2RuntimeFixture, runID string) {
	t.Helper()
	for _, event := range fixture.session.Events() {
		if event.RunID != runID {
			continue
		}
		if event.Type == core.EvAssistantMessage {
			var data core.AssistantMessageData
			if err := json.Unmarshal(event.Data, &data); err != nil {
				t.Fatal(err)
			}
			calls, err := strictAssistantCalls(data)
			if err != nil {
				t.Fatal(err)
			}
			for _, call := range calls {
				if call.Name != "records.inventory" {
					t.Fatalf("rejected choice left an assistant tool-call effect: %+v", call)
				}
			}
			continue
		}
		if event.Type != core.EvToolCall && event.Type != core.EvToolResult {
			continue
		}
		var callID string
		var name string
		if event.Type == core.EvToolCall {
			var data core.ToolCallData
			if err := json.Unmarshal(event.Data, &data); err != nil {
				t.Fatal(err)
			}
			callID, name = data.CallID, data.Name
		} else {
			var data core.ToolResultData
			if err := json.Unmarshal(event.Data, &data); err != nil {
				t.Fatal(err)
			}
			callID = data.CallID
		}
		if callID != "probe" && name != "records.inventory" {
			t.Fatalf("rejected choice left a tool journal effect: type=%s call=%q tool=%q", event.Type, callID, name)
		}
	}
	fixture.journal.mu.Lock()
	defer fixture.journal.mu.Unlock()
	for _, record := range fixture.journal.records {
		if record.RunID != runID {
			continue
		}
		if record.CapabilityID != "records.inventory" {
			t.Fatalf("rejected choice left a durable invocation record: %+v", record.ToolInvocation)
		}
	}
}

var _ core.ToolInvocationReader = (*v2RuntimeJournal)(nil)
