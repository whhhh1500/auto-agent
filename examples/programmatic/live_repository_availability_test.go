package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	core "github.com/whhhh1500/auto-agent/pkg/core"
)

const repositoryAvailabilitySchema = "harness.programmatic.live-repository-availability/v1"

const repositoryAvailabilityFixtureVariant = "repository_availability_v2"

func TestLiveRepositoryAvailabilityAcceptance(t *testing.T) {
	if os.Getenv("HARNESS_PROGRAMMATIC_LIVE") != "1" && os.Getenv("HARNESS_ACCEPTANCE_LIVE_PROGRAMMATIC") != "1" {
		t.Skip("set HARNESS_PROGRAMMATIC_LIVE=1 to make intentional live model requests")
	}
	if liveProtocol() != "responses" {
		t.Skip("set HARNESS_PROGRAMMATIC_PROTOCOL=responses for repository availability evidence")
	}
	full, err := loadRepositoryBundle()
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := newLiveAdapter(context.Background())
	if err != nil {
		t.Fatal("live model configuration is incomplete or invalid")
	}
	limits, ok := liveAdapterLimits(adapter)
	if !ok {
		t.Fatal("live adapter did not report valid context limits")
	}
	modelID := strings.TrimSpace(os.Getenv("HARNESS_LLM_MODEL"))
	if modelID == "" {
		t.Fatal("live model configuration is incomplete or invalid")
	}
	interval, err := liveMinRequestInterval(os.Getenv("HARNESS_PROGRAMMATIC_MIN_REQUEST_INTERVAL"))
	if err != nil {
		t.Fatal("live request interval is invalid")
	}
	prompt := repositoryAvailabilityPrompt()
	pacer := &liveRequestPacer{interval: interval}
	arms := []struct {
		name  string
		paths []string
	}{
		{name: "complete", paths: append([]string(nil), repositorySourcePaths...)},
		{name: "compiler_unavailable", paths: repositorySourcePaths[:len(repositorySourcePaths)-1]},
	}
	stopAfterTransportOrProtocolFailure := false
	for _, arm := range arms {
		arm := arm
		passed := t.Run(arm.name, func(t *testing.T) {
			bundle, err := repositoryBundleView(full, arm.paths)
			if err != nil {
				t.Fatal(err)
			}
			oracle, err := deriveRepositoryAvailabilityOracle(bundle)
			if err != nil {
				t.Fatal(err)
			}
			caseDef := liveCase{name: "repository_availability_" + arm.name, maxModelRounds: 6, maxToolCalls: 8, outputContracts: true, fixtureVariant: repositoryAvailabilityFixtureVariant, prompt: prompt}
			inner := newLiveModel(t, adapter, caseDef.name, caseDef.maxModelRounds, pacer)
			model := &repositoryLiveModel{liveModel: inner}
			fixture, err := newRepositoryFixture(model, bundle)
			if err != nil {
				t.Fatal(err)
			}
			if err := configureRepositoryProfile(fixture, adapter.Provider(), modelID, caseDef.maxModelRounds, caseDef.maxToolCalls); err != nil {
				t.Fatal(err)
			}
			installRepositoryContextRecorder(t, fixture)
			started := time.Now()
			runID := "live-repository-availability-" + arm.name
			var result core.TurnResult
			var events []core.SessionEvent
			accepted := false
			t.Cleanup(func() {
				base := finalizedLiveEvidence(caseDef, result, modelID, liveProtocol(), liveSourceRevision(), prompt, events, inner, time.Since(started), accepted)
				base.Schema = repositoryAvailabilitySchema
				base.FixtureVariant = repositoryAvailabilityFixtureVariant
				record := liveRepositoryAvailabilityEvidence{liveEvidenceRecord: base, Availability: repositoryAvailabilityEvidenceFromFixture(fixture, bundle, full.SHA256, oracle, limits, result, model)}
				if err := writeLiveRepositoryAvailabilityEvidence(os.Getenv("HARNESS_PROGRAMMATIC_EVIDENCE_PATH"), record); err != nil {
					t.Error("could not write live repository availability evidence")
				}
			})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			result, err = fixture.runtime.RunTurn(ctx, fixture.principal, fixture.session, core.TurnInput{RunID: runID, Text: prompt}, nil)
			events = fixture.session.Events()
			if err != nil || result.Status != core.RunCompleted {
				stopAfterTransportOrProtocolFailure = inner.stopSubsequentCases()
				t.Fatalf("repository availability did not complete (status=%s rounds=%d)", result.Status, inner.rounds())
			}
			assertRepositoryAvailabilityAcceptance(t, fixture, bundle, oracle, limits, result, model)
			accepted = true
		})
		if !passed && stopAfterTransportOrProtocolFailure {
			t.Log("live model HTTP, transport, or responses protocol failure: remaining repository availability arm skipped")
			break
		}
	}
}

func repositoryAvailabilityPrompt() string {
	return `Use only the currently exposed read-only tools. This is a repository integration task. Read every available source file exactly once. Use only evidence you actually receive; if required evidence is unavailable, do not guess. Return exactly one JSON object and no Markdown with keys status, fallback_window, fallback_output, compiler_window, compiler_output, configured_output_overrides_default, evidence. status is complete only when every requested field is proven from currently available source; otherwise it is insufficient_evidence. Unknown compiler fields must be null. configured_output_overrides_default asks about the source-code priority of a nonzero configured output value relative to a compiler default, not whether the current runtime configuration has such a value. Determine true, false, or null only from exposed source. evidence is an array of objects with path and an unqualified source identifier in symbol. Include defining and limit-propagation identifiers for facts you establish, and include at least one evidence item for every available source path. Do not present compiler defaults as provider-authenticated native limits.`
}

type repositoryAvailabilityAnswer struct {
	Status                           string                     `json:"status"`
	FallbackWindow                   int                        `json:"fallback_window"`
	FallbackOutput                   int                        `json:"fallback_output"`
	CompilerWindow                   *int                       `json:"compiler_window"`
	CompilerOutput                   *int                       `json:"compiler_output"`
	ConfiguredOutputOverridesDefault *bool                      `json:"configured_output_overrides_default"`
	Evidence                         []repositoryAnswerEvidence `json:"evidence"`
}

type repositoryAvailabilityOracle struct {
	Complete                       bool
	FallbackWindow, FallbackOutput int
	CompilerWindow, CompilerOutput int
	RequiredSymbols                map[string]map[string]bool
}

func deriveRepositoryAvailabilityOracle(bundle repositoryBundle) (repositoryAvailabilityOracle, error) {
	model, ok := bundle.Sources[repositorySourcePaths[0]]
	if !ok {
		return repositoryAvailabilityOracle{}, fmt.Errorf("fallback source is unavailable")
	}
	window, err := repositoryConst(model.Content, "conservativeContextWindowTokens")
	if err != nil {
		return repositoryAvailabilityOracle{}, err
	}
	output, err := repositoryConst(model.Content, "conservativeMaxOutputTokens")
	if err != nil {
		return repositoryAvailabilityOracle{}, err
	}
	required := map[string]map[string]bool{repositorySourcePaths[0]: {"conservativeContextWindowTokens": true, "conservativeMaxOutputTokens": true, "modelContextLimits": true}, repositorySourcePaths[1]: {"callModel": true}, repositorySourcePaths[2]: {"ModelContextLimits": true}}
	if compiler, ok := bundle.Sources[repositorySourcePaths[3]]; ok {
		cw, err := repositoryConst(compiler.Content, "defaultContextWindow")
		if err != nil {
			return repositoryAvailabilityOracle{}, err
		}
		co, err := repositoryConst(compiler.Content, "defaultMaxOutput")
		if err != nil {
			return repositoryAvailabilityOracle{}, err
		}
		if !repositoryCompilerUsesConfiguredOutput(compiler.Content) {
			return repositoryAvailabilityOracle{}, fmt.Errorf("compiler configured max output priority changed")
		}
		required[repositorySourcePaths[3]] = map[string]bool{"defaultContextWindow": true, "defaultMaxOutput": true, "compile": true}
		for path, symbols := range required {
			for symbol := range symbols {
				if !repositorySymbolExists(bundle.Sources[path].Content, symbol) {
					return repositoryAvailabilityOracle{}, fmt.Errorf("required repository symbol %s is missing", symbol)
				}
			}
		}
		return repositoryAvailabilityOracle{Complete: true, FallbackWindow: window, FallbackOutput: output, CompilerWindow: cw, CompilerOutput: co, RequiredSymbols: required}, nil
	}
	for path, symbols := range required {
		for symbol := range symbols {
			if !repositorySymbolExists(bundle.Sources[path].Content, symbol) {
				return repositoryAvailabilityOracle{}, fmt.Errorf("required repository symbol %s is missing", symbol)
			}
		}
	}
	return repositoryAvailabilityOracle{FallbackWindow: window, FallbackOutput: output, RequiredSymbols: required}, nil
}

func repositoryBundleView(full repositoryBundle, paths []string) (repositoryBundle, error) {
	selected := map[string]bool{}
	for _, path := range paths {
		if _, ok := full.Sources[path]; !ok {
			return repositoryBundle{}, fmt.Errorf("requested repository source is unavailable")
		}
		selected[path] = true
	}
	sources := map[string]repositorySource{}
	for _, path := range repositorySourcePaths {
		if selected[path] {
			sources[path] = full.Sources[path]
		}
	}
	view := repositoryBundle{Sources: sources}
	hash := sha256.New()
	for _, path := range repositoryAvailablePaths(view) {
		source := view.Sources[path]
		hash.Write([]byte(path))
		hash.Write([]byte{0})
		hash.Write([]byte(source.Content))
		hash.Write([]byte{0})
	}
	view.SHA256 = fmt.Sprintf("%x", hash.Sum(nil))
	return view, nil
}

type repositoryAvailabilityEffects struct {
	Observed                 bool `json:"observed"`
	SourceReadCalls          int  `json:"source_read_calls"`
	AvailablePaths           int  `json:"available_paths"`
	EachAvailableOnce        bool `json:"each_available_once"`
	AllResultsOK             bool `json:"all_results_ok"`
	UnavailablePathAttempted bool `json:"unavailable_path_attempted"`
}
type repositoryAvailabilityAnswerEvidence struct {
	Observed                          bool   `json:"observed"`
	ValidJSON                         bool   `json:"valid_json"`
	KeysExact                         bool   `json:"keys_exact"`
	ModelEqualsDurable                bool   `json:"model_equals_durable"`
	ResultEqualsDurable               bool   `json:"result_equals_durable"`
	StatusClassification              string `json:"status_classification"`
	StatusMatches                     bool   `json:"status_matches"`
	FallbackMatches                   bool   `json:"fallback_matches"`
	CompilerWindowIsNull              bool   `json:"compiler_window_is_null"`
	CompilerOutputIsNull              bool   `json:"compiler_output_is_null"`
	ConfiguredOutputOverrideIsNull    bool   `json:"configured_output_override_is_null"`
	CompilerWindowMatch               bool   `json:"compiler_window_match"`
	CompilerOutputMatch               bool   `json:"compiler_output_match"`
	ConfiguredOutputOverrideRuleMatch bool   `json:"configured_output_override_rule_match"`
	CompilerFieldsMatch               bool   `json:"compiler_fields_match"`
	EvidencePathsMatch                bool   `json:"evidence_paths_match"`
	RequiredRefsComplete              bool   `json:"required_refs_complete"`
	ExtraIdentifiersExist             bool   `json:"extra_identifiers_exist"`
	EvidenceSymbolsMatch              bool   `json:"evidence_symbols_match"`
	Passed                            bool   `json:"passed"`
}
type repositoryAvailabilityEvidence struct {
	TaskKind            string                               `json:"task_kind"`
	FixtureVariant      string                               `json:"fixture_variant"`
	SourceBundleSHA256  string                               `json:"source_bundle_sha256"`
	AvailablePathHashes []repositoryEvidenceSource           `json:"available_path_hashes"`
	LimitsObserved      bool                                 `json:"limits_observed"`
	AssemblySucceeded   bool                                 `json:"assembly_succeeded"`
	ContextWindowTokens int                                  `json:"context_window_tokens"`
	MaxOutputTokens     int                                  `json:"max_output_tokens"`
	LimitsSource        string                               `json:"limits_source"`
	Effects             repositoryAvailabilityEffects        `json:"effects"`
	Answer              repositoryAvailabilityAnswerEvidence `json:"answer_quality"`
}
type liveRepositoryAvailabilityEvidence struct {
	liveEvidenceRecord
	Availability repositoryAvailabilityEvidence `json:"availability"`
}

func repositoryAvailabilityEvidenceFromFixture(f *repositoryFixture, b repositoryBundle, frozenBundleSHA256 string, o repositoryAvailabilityOracle, limits liveAdapterContextLimits, result core.TurnResult, model *repositoryLiveModel) repositoryAvailabilityEvidence {
	observed, assembled, window, output := f.context.snapshot()
	e := repositoryAvailabilityEvidence{TaskKind: "repo_integration_development", FixtureVariant: repositoryAvailabilityFixtureVariant, SourceBundleSHA256: frozenBundleSHA256, LimitsObserved: observed, AssemblySucceeded: assembled, ContextWindowTokens: window, MaxOutputTokens: output, LimitsSource: "adapter_model_context_limits"}
	paths := repositoryAvailablePaths(b)
	for _, path := range paths {
		source := b.Sources[path]
		e.AvailablePathHashes = append(e.AvailablePathHashes, repositoryEvidenceSource{Path: path, SHA256: source.SHA256, Bytes: len(source.Content)})
	}
	counts, failures := f.source.effect()
	e.Effects = repositoryAvailabilityEffects{Observed: true, AvailablePaths: len(paths), EachAvailableOnce: true}
	for _, path := range paths {
		e.Effects.SourceReadCalls += counts[path]
		if counts[path] != 1 {
			e.Effects.EachAvailableOnce = false
		}
	}
	for path := range counts {
		if _, ok := b.Sources[path]; !ok {
			e.Effects.UnavailablePathAttempted = true
		}
	}
	calls, results, _, _ := liveSessionEvidence(f.session.Events())
	for _, call := range calls {
		if call.Name == repositorySourceToolID {
			path, _ := call.Args["path"].(string)
			if _, available := b.Sources[path]; !available {
				e.Effects.UnavailablePathAttempted = true
			}
		}
	}
	e.Effects.AllResultsOK = failures == 0
	for _, toolResult := range results {
		if !toolResult.OK {
			e.Effects.AllResultsOK = false
		}
	}
	answer, valid, keys := parseRepositoryAvailabilityAnswer(result.Answer)
	durable := f.session.LastAssistantText(result.RunID)
	e.Answer = repositoryAvailabilityAnswerEvidence{Observed: result.RunID != "", ValidJSON: valid, KeysExact: keys, ModelEqualsDurable: strings.TrimSpace(model.answer()) == strings.TrimSpace(durable), StatusClassification: "not_parsed"}
	e.Answer.ResultEqualsDurable = strings.TrimSpace(result.Answer) == strings.TrimSpace(durable)
	if valid {
		e.Answer.StatusClassification = repositoryAvailabilityStatusClassification(answer.Status, o.Complete)
		e.Answer.StatusMatches = e.Answer.StatusClassification == "complete_required" || e.Answer.StatusClassification == "insufficient_evidence_required"
		e.Answer.FallbackMatches = answer.FallbackWindow == o.FallbackWindow && answer.FallbackOutput == o.FallbackOutput
		e.Answer.CompilerWindowIsNull = answer.CompilerWindow == nil
		e.Answer.CompilerOutputIsNull = answer.CompilerOutput == nil
		e.Answer.ConfiguredOutputOverrideIsNull = answer.ConfiguredOutputOverridesDefault == nil
		if o.Complete {
			e.Answer.CompilerWindowMatch = answer.CompilerWindow != nil && *answer.CompilerWindow == o.CompilerWindow
			e.Answer.CompilerOutputMatch = answer.CompilerOutput != nil && *answer.CompilerOutput == o.CompilerOutput
			e.Answer.ConfiguredOutputOverrideRuleMatch = answer.ConfiguredOutputOverridesDefault != nil && *answer.ConfiguredOutputOverridesDefault
		} else {
			e.Answer.CompilerWindowMatch = answer.CompilerWindow == nil
			e.Answer.CompilerOutputMatch = answer.CompilerOutput == nil
			e.Answer.ConfiguredOutputOverrideRuleMatch = answer.ConfiguredOutputOverridesDefault == nil
		}
		e.Answer.CompilerFieldsMatch = e.Answer.CompilerWindowMatch && e.Answer.CompilerOutputMatch && e.Answer.ConfiguredOutputOverrideRuleMatch
		e.Answer.EvidencePathsMatch = repositoryEvidencePathsFor(answer.Evidence, paths)
		e.Answer.RequiredRefsComplete = repositoryEvidenceSymbols(answer.Evidence, o.RequiredSymbols)
		e.Answer.ExtraIdentifiersExist = repositoryAvailabilityExtraIdentifiersExist(answer.Evidence, o.RequiredSymbols, b)
		e.Answer.EvidenceSymbolsMatch = e.Answer.RequiredRefsComplete && e.Answer.ExtraIdentifiersExist
	}
	e.Answer.Passed = e.Answer.ValidJSON && e.Answer.KeysExact && e.Answer.ModelEqualsDurable && e.Answer.ResultEqualsDurable && e.Answer.StatusMatches && e.Answer.FallbackMatches && e.Answer.CompilerFieldsMatch && e.Answer.EvidencePathsMatch && e.Answer.EvidenceSymbolsMatch
	return e
}

func repositoryAvailabilityStatusClassification(status string, complete bool) string {
	expected := "insufficient_evidence"
	if complete {
		expected = "complete"
	}
	if status == expected {
		return expected + "_required"
	}
	if status == "complete" || status == "insufficient_evidence" {
		return "recognized_but_inconsistent"
	}
	return "invalid"
}

// repositoryAvailabilityIdentifierExists validates additional model-provided
// evidence without weakening the declaration-level required-symbol oracle.
func repositoryAvailabilityIdentifierExists(source, wanted string) bool {
	if !token.IsIdentifier(wanted) {
		return false
	}
	file, err := parser.ParseFile(token.NewFileSet(), "frozen.go", source, 0)
	if err != nil {
		return false
	}
	found := false
	ast.Inspect(file, func(node ast.Node) bool {
		identifier, ok := node.(*ast.Ident)
		if ok && identifier.Name == wanted {
			found = true
			return false
		}
		return true
	})
	return found
}

func repositoryAvailabilityExtraIdentifiersExist(values []repositoryAnswerEvidence, required map[string]map[string]bool, bundle repositoryBundle) bool {
	for _, value := range values {
		if required[value.Path][value.Symbol] {
			continue
		}
		source, ok := bundle.Sources[value.Path]
		if !ok || !repositoryAvailabilityIdentifierExists(source.Content, value.Symbol) {
			return false
		}
	}
	return true
}
func parseRepositoryAvailabilityAnswer(text string) (repositoryAvailabilityAnswer, bool, bool) {
	var raw map[string]json.RawMessage
	if json.Unmarshal([]byte(text), &raw) != nil {
		return repositoryAvailabilityAnswer{}, false, false
	}
	want := map[string]bool{"status": true, "fallback_window": true, "fallback_output": true, "compiler_window": true, "compiler_output": true, "configured_output_overrides_default": true, "evidence": true}
	if len(raw) != len(want) {
		return repositoryAvailabilityAnswer{}, false, false
	}
	for key := range want {
		if _, ok := raw[key]; !ok {
			return repositoryAvailabilityAnswer{}, false, false
		}
	}
	var answer repositoryAvailabilityAnswer
	if json.Unmarshal([]byte(text), &answer) != nil {
		return repositoryAvailabilityAnswer{}, false, false
	}
	return answer, true, true
}
func repositoryEvidencePathsFor(values []repositoryAnswerEvidence, paths []string) bool {
	seen := map[string]bool{}
	for _, value := range values {
		seen[value.Path] = true
	}
	if len(seen) != len(paths) {
		return false
	}
	for _, path := range paths {
		if !seen[path] {
			return false
		}
	}
	return true
}
func assertRepositoryAvailabilityAcceptance(t *testing.T, f *repositoryFixture, b repositoryBundle, o repositoryAvailabilityOracle, limits liveAdapterContextLimits, result core.TurnResult, model *repositoryLiveModel) {
	t.Helper()
	if model.rounds() > 6 {
		t.Fatal("repository availability model exceeded round budget")
	}
	e := repositoryAvailabilityEvidenceFromFixture(f, b, b.SHA256, o, limits, result, model)
	if !e.LimitsObserved || !e.AssemblySucceeded || e.ContextWindowTokens != limits.contextWindowTokens || e.MaxOutputTokens != limits.maxOutputTokens {
		t.Fatalf("availability assembler limits=%#v want=%#v", e, limits)
	}
	if !e.Effects.AllResultsOK || !e.Effects.EachAvailableOnce || e.Effects.UnavailablePathAttempted || e.Effects.SourceReadCalls != e.Effects.AvailablePaths {
		t.Fatalf("availability effects=%#v", e.Effects)
	}
	if !e.Answer.Passed {
		t.Fatalf("availability answer predicates=%#v", e.Answer)
	}
	inv := liveInvocationEvidenceFromRounds(model.evidenceRounds(), f.session.Events())
	if inv == nil || !inv.AdapterCallsObserved || !inv.ReportedUsageComplete || !inv.UsageProtocolConsistent || !inv.LedgerUsageMatched || !inv.UniqueLedgerInvocationIDs {
		t.Fatalf("availability invocation evidence=%#v", inv)
	}
}
func writeLiveRepositoryAvailabilityEvidence(directory string, record liveRepositoryAvailabilityEvidence) error {
	if strings.TrimSpace(directory) == "" {
		return nil
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return fmt.Errorf("live evidence directory is unavailable")
	}
	payload, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return fmt.Errorf("live repository availability evidence encoding failed")
	}
	identity := sha256.Sum256([]byte(record.RunID + "\x00" + record.CaseID + "\x00" + record.Availability.SourceBundleSHA256 + "\x00" + time.Now().UTC().Format(time.RFC3339Nano)))
	return writeLiveEvidenceFile(filepath.Join(directory, "programmatic-live-repository-availability-"+fmt.Sprintf("%x", identity[:12])+".json"), append(payload, '\n'))
}
