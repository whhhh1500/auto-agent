package main

import (
	"context"
	"testing"

	core "github.com/whhhh1500/auto-agent/pkg/core"
)

func TestLiveModelArtifactRevisionForwardsSafely(t *testing.T) {
	const revision = "offline-live-model-revision/v1"
	for _, test := range []struct {
		name  string
		model core.LlmAdapter
		want  string
	}{
		{name: "supported", model: liveRevisionModel{revision: revision}, want: revision},
		{name: "unsupported", model: liveRevisionUnsupportedModel{}},
		{name: "empty", model: liveRevisionModel{revision: ""}},
		{name: "panic", model: liveRevisionPanicModel{}},
		{name: "nil_inner"},
	} {
		t.Run(test.name, func(t *testing.T) {
			model := &liveModel{inner: test.model}
			revisioner, ok := any(model).(core.ArtifactRevisioner)
			if !ok {
				t.Fatal("liveModel does not implement core.ArtifactRevisioner")
			}
			if got := revisioner.ArtifactRevision(); got != test.want {
				t.Fatalf("ArtifactRevision()=%q, want %q", got, test.want)
			}
		})
	}

	var nilModel *liveModel
	if got := nilModel.ArtifactRevision(); got != "" {
		t.Fatalf("nil liveModel ArtifactRevision()=%q, want empty", got)
	}
}

func TestLiveModelContextLimitsForwardSafely(t *testing.T) {
	const (
		contextWindow = 128_000
		maxOutput     = 4_096
	)
	for _, test := range []struct {
		name               string
		model              core.LlmAdapter
		wantContextWindow  int
		wantMaxOutputToken int
	}{
		{name: "supported", model: liveContextLimitsModel{contextWindow: contextWindow, maxOutput: maxOutput}, wantContextWindow: contextWindow, wantMaxOutputToken: maxOutput},
		{name: "unsupported", model: liveRevisionUnsupportedModel{}},
		{name: "zero_window", model: liveContextLimitsModel{maxOutput: maxOutput}},
		{name: "zero_output", model: liveContextLimitsModel{contextWindow: contextWindow}},
		{name: "output_equals_window", model: liveContextLimitsModel{contextWindow: maxOutput, maxOutput: maxOutput}},
		{name: "output_exceeds_window", model: liveContextLimitsModel{contextWindow: maxOutput, maxOutput: contextWindow}},
		{name: "panic", model: liveContextLimitsPanicModel{}},
		{name: "nil_inner"},
	} {
		t.Run(test.name, func(t *testing.T) {
			model := &liveModel{inner: test.model}
			reporter, ok := any(model).(interface{ ModelContextLimits() (int, int) })
			if !ok {
				t.Fatal("liveModel does not implement ModelContextLimits")
			}
			window, output := reporter.ModelContextLimits()
			if window != test.wantContextWindow || output != test.wantMaxOutputToken {
				t.Fatalf("ModelContextLimits()=(%d,%d), want=(%d,%d)", window, output, test.wantContextWindow, test.wantMaxOutputToken)
			}
		})
	}

	var nilModel *liveModel
	if window, output := nilModel.ModelContextLimits(); window != 0 || output != 0 {
		t.Fatalf("nil liveModel ModelContextLimits()=(%d,%d), want=(0,0)", window, output)
	}
}

type liveRevisionModel struct{ revision string }

func (liveRevisionModel) Provider() string { return "offline-live-revision" }

func (liveRevisionModel) Stream(context.Context, core.GenerateOptions, func(core.StreamChunk)) error {
	return nil
}

func (m liveRevisionModel) ArtifactRevision() string { return m.revision }

type liveRevisionUnsupportedModel struct{}

func (liveRevisionUnsupportedModel) Provider() string { return "offline-live-revision" }

func (liveRevisionUnsupportedModel) Stream(context.Context, core.GenerateOptions, func(core.StreamChunk)) error {
	return nil
}

type liveRevisionPanicModel struct{}

func (liveRevisionPanicModel) Provider() string { return "offline-live-revision" }

func (liveRevisionPanicModel) Stream(context.Context, core.GenerateOptions, func(core.StreamChunk)) error {
	return nil
}

func (liveRevisionPanicModel) ArtifactRevision() string { panic("offline revision panic") }

type liveContextLimitsModel struct{ contextWindow, maxOutput int }

func (liveContextLimitsModel) Provider() string { return "offline-live-limits" }

func (liveContextLimitsModel) Stream(context.Context, core.GenerateOptions, func(core.StreamChunk)) error {
	return nil
}

func (m liveContextLimitsModel) ModelContextLimits() (int, int) {
	return m.contextWindow, m.maxOutput
}

type liveContextLimitsPanicModel struct{}

func (liveContextLimitsPanicModel) Provider() string { return "offline-live-limits" }

func (liveContextLimitsPanicModel) Stream(context.Context, core.GenerateOptions, func(core.StreamChunk)) error {
	return nil
}

func (liveContextLimitsPanicModel) ModelContextLimits() (int, int) {
	panic("offline context limits panic")
}
