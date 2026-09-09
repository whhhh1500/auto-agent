package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"

	core "github.com/whhhh1500/auto-agent/pkg/core"
)

func TestRepositoryAvailabilityOfflineRuntimeClosure(t *testing.T) {
	full, err := loadRepositoryBundle()
	if err != nil {
		t.Fatal(err)
	}
	for _, arm := range []struct {
		name  string
		paths []string
	}{
		{"complete", append([]string(nil), repositorySourcePaths...)},
		{"compiler_unavailable", repositorySourcePaths[:len(repositorySourcePaths)-1]},
	} {
		t.Run(arm.name, func(t *testing.T) {
			bundle, err := repositoryBundleView(full, arm.paths)
			if err != nil {
				t.Fatal(err)
			}
			oracle, err := deriveRepositoryAvailabilityOracle(bundle)
			if err != nil {
				t.Fatal(err)
			}
			model := &repositoryAvailabilityOfflineModel{oracle: oracle, paths: repositoryAvailablePaths(bundle)}
			recording := &repositoryLiveModel{liveModel: newLiveModel(t, model, "offline-availability-"+arm.name, 6, nil)}
			fixture, err := newRepositoryFixture(recording, bundle)
			if err != nil {
				t.Fatal(err)
			}
			if err := configureRepositoryProfile(fixture, model.Provider(), "offline-repository-availability", 6, 8); err != nil {
				t.Fatal(err)
			}
			installRepositoryContextRecorder(t, fixture)
			result, err := fixture.runtime.RunTurn(context.Background(), fixture.principal, fixture.session, core.TurnInput{RunID: "offline-repository-availability-" + arm.name, Text: repositoryAvailabilityPrompt()}, nil)
			if err != nil || result.Status != core.RunCompleted {
				t.Fatalf("availability runtime result=%#v err=%v", result, err)
			}
			limits, ok := liveAdapterLimits(recording)
			if !ok {
				t.Fatal("offline availability limits unavailable")
			}
			assertRepositoryAvailabilityAcceptance(t, fixture, bundle, oracle, limits, result, recording)
			answer, valid, keys := parseRepositoryAvailabilityAnswer(result.Answer)
			if !valid || !keys || answer.Status == "" {
				t.Fatalf("availability answer malformed: %s", result.Answer)
			}
			counts, failures := fixture.source.effect()
			if failures != 0 || len(counts) != len(arm.paths) {
				t.Fatalf("availability source effects=%v failures=%d", counts, failures)
			}
			for _, path := range arm.paths {
				if counts[path] != 1 {
					t.Fatalf("source %s calls=%d", path, counts[path])
				}
			}
			if _, found := counts[repositorySourcePaths[len(repositorySourcePaths)-1]]; !oracle.Complete && found {
				t.Fatal("unavailable compiler source appeared in effects")
			}
			if strings.TrimSpace(result.Answer) != strings.TrimSpace(model.answer) || strings.TrimSpace(result.Answer) != strings.TrimSpace(fixture.session.LastAssistantText(result.RunID)) {
				t.Fatal("offline model, turn result, and durable answer diverged")
			}
			if answer.FallbackWindow != oracle.FallbackWindow || answer.FallbackOutput != oracle.FallbackOutput || !repositoryEvidencePathsFor(answer.Evidence, arm.paths) || !repositoryEvidenceSymbols(answer.Evidence, oracle.RequiredSymbols) {
				t.Fatalf("availability source-answer predicates failed: %#v", answer)
			}
			if oracle.Complete != (answer.Status == "complete") || (!oracle.Complete && (answer.CompilerWindow != nil || answer.CompilerOutput != nil || answer.ConfiguredOutputOverridesDefault != nil)) {
				t.Fatalf("availability answer did not represent available evidence: %#v", answer)
			}
		})
	}
}

func TestRepositoryAvailabilityBundleViewDoesNotExposeCompiler(t *testing.T) {
	full, err := loadRepositoryBundle()
	if err != nil {
		t.Fatal(err)
	}
	view, err := repositoryBundleView(full, repositorySourcePaths[:len(repositorySourcePaths)-1])
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := view.Sources[repositorySourcePaths[len(repositorySourcePaths)-1]]; ok {
		t.Fatal("availability view retained compiler source")
	}
	tool, err := newRepositorySourceTool(view)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tool.read(repositorySourcePaths[len(repositorySourcePaths)-1]); err == nil {
		t.Fatal("availability reader accepted unavailable compiler path")
	}
	if _, failures := tool.effect(); failures != 1 {
		t.Fatal("unavailable compiler read was not recorded as rejected")
	}
}

func TestRepositoryAvailabilityExtraEvidenceIdentifiers(t *testing.T) {
	full, err := loadRepositoryBundle()
	if err != nil {
		t.Fatal(err)
	}
	compiler := full.Sources[repositorySourcePaths[len(repositorySourcePaths)-1]]
	if !repositoryAvailabilityIdentifierExists(compiler.Content, "maxOutput") {
		t.Fatal("local compiler propagation identifier was rejected")
	}
	for _, value := range []string{"inventedIdentifier", "configuration.MaxTokens"} {
		if repositoryAvailabilityIdentifierExists(compiler.Content, value) {
			t.Fatalf("invalid additional evidence identifier %q was accepted", value)
		}
	}
}

func TestRepositoryAvailabilityRequiredEvidenceCannotBeOmitted(t *testing.T) {
	full, err := loadRepositoryBundle()
	if err != nil {
		t.Fatal(err)
	}
	oracle, err := deriveRepositoryAvailabilityOracle(full)
	if err != nil {
		t.Fatal(err)
	}
	values := make([]repositoryAnswerEvidence, 0)
	for path, symbols := range oracle.RequiredSymbols {
		for symbol := range symbols {
			if path == repositorySourcePaths[3] && symbol == "compile" {
				continue
			}
			values = append(values, repositoryAnswerEvidence{Path: path, Symbol: symbol})
		}
	}
	if repositoryEvidenceSymbols(values, oracle.RequiredSymbols) {
		t.Fatal("missing required compiler evidence was accepted")
	}
	if !repositoryAvailabilityExtraIdentifiersExist(values, oracle.RequiredSymbols, full) {
		t.Fatal("required-evidence counterexample was confounded by extra identifier validation")
	}
}

type repositoryAvailabilityOfflineModel struct {
	oracle repositoryAvailabilityOracle
	paths  []string
	mu     sync.Mutex
	phase  int
	answer string
}

func (*repositoryAvailabilityOfflineModel) Provider() string {
	return "offline-repository-availability"
}
func (*repositoryAvailabilityOfflineModel) ModelContextLimits() (int, int) { return 128000, 4096 }
func (m *repositoryAvailabilityOfflineModel) Stream(_ context.Context, _ core.GenerateOptions, emit func(core.StreamChunk)) error {
	m.mu.Lock()
	phase := m.phase
	m.phase++
	m.mu.Unlock()
	switch phase {
	case 0:
		calls := make([]core.ToolCall, 0, len(m.paths))
		for index, path := range m.paths {
			calls = append(calls, core.ToolCall{ID: fmt.Sprintf("availability-source-%d", index+1), Name: repositorySourceToolID, Args: map[string]any{"path": path}})
		}
		emit(core.StreamChunk{Kind: core.StreamKindAssistant, ToolCall: &calls[0], ToolCalls: calls})
		emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishToolCalls, Usage: &core.TokenUsage{InputTokens: 10, OutputTokens: 5}})
	case 1:
		answer, err := repositoryAvailabilityOfflineAnswer(m.oracle)
		if err != nil {
			return err
		}
		m.mu.Lock()
		m.answer = answer
		m.mu.Unlock()
		emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: answer})
		emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop, Usage: &core.TokenUsage{InputTokens: 20, OutputTokens: 8}})
	default:
		return fmt.Errorf("offline availability model received extra model call")
	}
	return nil
}
func repositoryAvailabilityOfflineAnswer(oracle repositoryAvailabilityOracle) (string, error) {
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
	answer := repositoryAvailabilityAnswer{FallbackWindow: oracle.FallbackWindow, FallbackOutput: oracle.FallbackOutput, Evidence: evidence}
	if oracle.Complete {
		answer.Status = "complete"
		window, output := oracle.CompilerWindow, oracle.CompilerOutput
		value := true
		answer.CompilerWindow = &window
		answer.CompilerOutput = &output
		answer.ConfiguredOutputOverridesDefault = &value
	} else {
		answer.Status = "insufficient_evidence"
	}
	encoded, err := json.Marshal(answer)
	return string(encoded), err
}
