package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"testing"

	appcontextassembly "github.com/whhhh1500/auto-agent/pkg/app/contextassembly"
	core "github.com/whhhh1500/auto-agent/pkg/core"
)

func TestRepositoryBundleOracleAndReadOnlyRuntimeClosure(t *testing.T) {
	bundle, err := loadRepositoryBundle()
	if err != nil {
		t.Fatal(err)
	}
	oracle, err := repositoryLimitsOracle(bundle)
	if err != nil {
		t.Fatal(err)
	}
	model := &repositoryOfflineModel{oracle: oracle}
	recording := &repositoryLiveModel{liveModel: newLiveModel(t, model, "offline-repository-limits", 6, nil)}
	fixture, err := newRepositoryFixture(recording, bundle)
	if err != nil {
		t.Fatal(err)
	}
	if err := configureRepositoryProfile(fixture, model.Provider(), "offline-repository-limits", 6, 8); err != nil {
		t.Fatal(err)
	}
	assembler, err := appcontextassembly.NewAssembler(appcontextassembly.Config{})
	if err != nil {
		t.Fatal(err)
	}
	var observed core.ModelContext
	fixture.context = &repositoryContextRecorder{}
	fixture.runtime.ContextAssembler = func(ctx context.Context, request core.ModelContext) (core.ModelContext, error) {
		observed = request
		assembled, err := assembler.AssembleModelContext(ctx, request)
		fixture.context.observe(request, assembled, err)
		return assembled, err
	}
	result, err := fixture.runtime.RunTurn(context.Background(), fixture.principal, fixture.session, core.TurnInput{RunID: "offline-repository-limits", Text: repositoryLimitsPrompt()}, nil)
	if err != nil || result.Status != core.RunCompleted {
		t.Fatalf("repository runtime result=%#v err=%v", result, err)
	}
	if observed.ContextWindowTokens != 128000 || observed.MaxOutputTokens != 4096 {
		t.Fatalf("adapter limits were not forwarded to assembler: %#v", observed)
	}
	limits, ok := liveAdapterLimits(recording)
	if !ok {
		t.Fatal("offline adapter limits unavailable")
	}
	assertRepositoryAcceptance(t, fixture, bundle, oracle, limits, result, recording)
	answer, valid, keys := parseRepositoryAnswer(result.Answer)
	if !valid || !keys || answer.FallbackWindow != oracle.FallbackWindow || answer.CompilerOutput != oracle.CompilerOutput || !repositoryEvidencePaths(answer.Evidence) || !repositoryEvidenceSymbols(answer.Evidence, oracle.RequiredSymbols) {
		t.Fatalf("offline repository answer did not satisfy oracle: %s", result.Answer)
	}
	counts, failures := fixture.source.effect()
	if failures != 0 || len(counts) != len(repositorySourcePaths) {
		t.Fatalf("repository source effects=%v failures=%d", counts, failures)
	}
	for _, path := range repositorySourcePaths {
		if counts[path] != 1 {
			t.Fatalf("source %s calls=%d, want one", path, counts[path])
		}
	}
	if fixture.source.Manifest().MaxOutputBytes <= 0 {
		t.Fatal("repository source manifest lacks an encoded output bound")
	}
}

func TestRepositoryReaderRejectsPathsOutsideFrozenAllowlist(t *testing.T) {
	bundle, err := loadRepositoryBundle()
	if err != nil {
		t.Fatal(err)
	}
	tool, err := newRepositorySourceTool(bundle)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tool.read(".credentials/github.env"); err == nil {
		t.Fatal("reader accepted a credential path")
	}
	if _, err := tool.read("../pkg/core/model_context.go"); err == nil {
		t.Fatal("reader accepted traversal")
	}
	counts, failures := tool.effect()
	if len(counts) != 0 || failures != 2 {
		t.Fatalf("rejected reads mutated successful effects: counts=%v failures=%d", counts, failures)
	}
}

type repositoryOfflineModel struct {
	oracle repositoryLimits
	mu     sync.Mutex
	phase  int
}

func (*repositoryOfflineModel) Provider() string               { return "offline-repository-limits" }
func (*repositoryOfflineModel) ArtifactRevision() string       { return "offline-repository-limits/v1" }
func (*repositoryOfflineModel) ModelContextLimits() (int, int) { return 128000, 4096 }
func (m *repositoryOfflineModel) Stream(_ context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	m.mu.Lock()
	phase := m.phase
	m.phase++
	m.mu.Unlock()
	switch phase {
	case 0:
		calls := make([]core.ToolCall, 0, len(repositorySourcePaths))
		for index, path := range repositorySourcePaths {
			calls = append(calls, core.ToolCall{ID: fmt.Sprintf("repository-source-%d", index+1), Name: repositorySourceToolID, Args: map[string]any{"path": path}})
		}
		emit(core.StreamChunk{Kind: core.StreamKindAssistant, ToolCall: &calls[0], ToolCalls: calls})
		emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishToolCalls, Usage: &core.TokenUsage{InputTokens: 10, OutputTokens: 5}})
	case 1:
		answer, err := repositoryOfflineAnswer(m.oracle)
		if err != nil {
			return err
		}
		emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: answer})
		emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop, Usage: &core.TokenUsage{InputTokens: 20, OutputTokens: 8}})
	default:
		return fmt.Errorf("offline repository model received extra model call (%d messages)", len(options.Messages))
	}
	return nil
}

func repositoryOfflineAnswer(oracle repositoryLimits) (string, error) {
	evidence := make([]repositoryAnswerEvidence, 0)
	paths := make([]string, 0, len(oracle.RequiredSymbols))
	for path := range oracle.RequiredSymbols {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	for _, path := range paths {
		symbols := make([]string, 0, len(oracle.RequiredSymbols[path]))
		for symbol := range oracle.RequiredSymbols[path] {
			symbols = append(symbols, symbol)
		}
		sort.Strings(symbols)
		for _, symbol := range symbols {
			evidence = append(evidence, repositoryAnswerEvidence{Path: path, Symbol: symbol})
		}
	}
	encoded, err := json.Marshal(repositoryAnswer{FallbackWindow: oracle.FallbackWindow, FallbackOutput: oracle.FallbackOutput, CompilerWindow: oracle.CompilerWindow, CompilerOutput: oracle.CompilerOutput, ConfiguredOutputOverridesDefault: oracle.ConfiguredOutputOverridesDefault, Evidence: evidence})
	return string(encoded), err
}
