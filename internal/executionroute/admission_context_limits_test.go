package executionroute

import (
	"context"
	"sync"
	"testing"

	"github.com/whhhh1500/auto-agent/pkg/core"
)

func TestV2RouteAdmissionAdapterModelContextLimits(t *testing.T) {
	tests := []struct {
		name    string
		adapter *v2RouteAdmissionAdapter
		window  int
		output  int
	}{
		{name: "nil admission adapter"},
		{name: "nil inner adapter", adapter: &v2RouteAdmissionAdapter{}},
		{name: "missing inner limits", adapter: &v2RouteAdmissionAdapter{inner: v2NoContextLimitsAdapter{}}},
		{name: "valid inner limits", adapter: &v2RouteAdmissionAdapter{inner: v2ContextLimitsAdapter{window: 128000, output: 4096}}, window: 128000, output: 4096},
		{name: "inner panic", adapter: &v2RouteAdmissionAdapter{inner: v2ContextLimitsAdapter{panic: true}}},
		{name: "zero window", adapter: &v2RouteAdmissionAdapter{inner: v2ContextLimitsAdapter{window: 0, output: 4096}}},
		{name: "negative window", adapter: &v2RouteAdmissionAdapter{inner: v2ContextLimitsAdapter{window: -1, output: 4096}}},
		{name: "zero output", adapter: &v2RouteAdmissionAdapter{inner: v2ContextLimitsAdapter{window: 128000, output: 0}}},
		{name: "negative output", adapter: &v2RouteAdmissionAdapter{inner: v2ContextLimitsAdapter{window: 128000, output: -1}}},
		{name: "output equals window", adapter: &v2RouteAdmissionAdapter{inner: v2ContextLimitsAdapter{window: 4096, output: 4096}}},
		{name: "output exceeds window", adapter: &v2RouteAdmissionAdapter{inner: v2ContextLimitsAdapter{window: 4095, output: 4096}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.adapter == nil {
				var nilAdapter *v2RouteAdmissionAdapter
				window, output := nilAdapter.ModelContextLimits()
				if window != 0 || output != 0 {
					t.Fatalf("nil limits=(%d,%d), want zero fallback", window, output)
				}
				return
			}
			window, output := test.adapter.ModelContextLimits()
			if window != test.window || output != test.output {
				t.Fatalf("limits=(%d,%d), want (%d,%d)", window, output, test.window, test.output)
			}
		})
	}
}

func TestV2RouteAdmissionForwardsContextLimitsToRuntimeComposition(t *testing.T) {
	fixture := newV2RuntimeFixture(t, v2RuntimeDirect, false, false)
	limited := &v2ContextLimitsAdapter{inner: fixture.model, window: 128000, output: 4096}
	fixture.runtime.Models = core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) {
		return limited, nil
	})
	assembler := &v2ContextLimitsRecorder{}
	fixture.runtime.ContextAssembler = assembler.assemble

	const runID = "run_v2_context_limits"
	result, err := fixture.run(t, runID)
	if err != nil || result.Status != core.RunCompleted {
		t.Fatalf("v2 route run result=%#v err=%v", result, err)
	}
	requests := assembler.requests()
	if len(requests) != 3 {
		t.Fatalf("context assembly calls=%d, want three model calls", len(requests))
	}
	for index, request := range requests {
		if request.ContextWindowTokens != 128000 || request.MaxOutputTokens != 4096 {
			t.Fatalf("model call %d context limits=(%d,%d), want inner (128000,4096)", index, request.ContextWindowTokens, request.MaxOutputTokens)
		}
	}
	frozen, err := frozenProbeComposition(fixture.session.Events(), runID)
	if err != nil {
		t.Fatalf("frozen composition: %v", err)
	}
	if frozen.composition.ResolvedProvider != limited.Provider() || frozen.composition.ModelRevision != v2ModelArtifactRevision(limited) {
		t.Fatalf("admission did not preserve pinned model evidence: %#v", frozen.composition)
	}
}

type v2NoContextLimitsAdapter struct{}

func (v2NoContextLimitsAdapter) Provider() string { return "limits-test" }
func (v2NoContextLimitsAdapter) Stream(context.Context, core.GenerateOptions, func(core.StreamChunk)) error {
	return nil
}

type v2ContextLimitsAdapter struct {
	inner  core.LlmAdapter
	window int
	output int
	panic  bool
}

func (a v2ContextLimitsAdapter) Provider() string {
	if a.inner != nil {
		return a.inner.Provider()
	}
	return "limits-test"
}

func (a v2ContextLimitsAdapter) ArtifactRevision() string {
	if revision, ok := a.inner.(core.ArtifactRevisioner); ok {
		return revision.ArtifactRevision()
	}
	return "limits-test/v1"
}

func (a v2ContextLimitsAdapter) ModelContextLimits() (int, int) {
	if a.panic {
		panic("context limits panic")
	}
	return a.window, a.output
}

func (a v2ContextLimitsAdapter) Stream(ctx context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	if a.inner == nil {
		return nil
	}
	return a.inner.Stream(ctx, options, emit)
}

type v2ContextLimitsRecorder struct {
	mu       sync.Mutex
	contexts []core.ModelContext
}

func (r *v2ContextLimitsRecorder) assemble(_ context.Context, request core.ModelContext) (core.ModelContext, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.contexts = append(r.contexts, request)
	return request, nil
}

func (r *v2ContextLimitsRecorder) requests() []core.ModelContext {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]core.ModelContext(nil), r.contexts...)
}
