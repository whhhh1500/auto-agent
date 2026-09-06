package server

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/cc-auto-agent/harness-core/internal/testdb"
	"github.com/cc-auto-agent/harness-core/internal/testwasm"
	"github.com/cc-auto-agent/harness-core/pkg/core"
	"github.com/cc-auto-agent/harness-core/pkg/execution"
	"github.com/tetratelabs/wazero"
	"go.opentelemetry.io/otel/codes"
)

func serialWASMComputation(t *testing.T, model *serialAcceptanceModel) {
	open := testdb.Postgres(t)
	path := testwasm.Calc(t)
	bytes, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("serial wasm artifact=internal/calc bytes=%d sha256=%x", len(bytes), sha256.Sum256(bytes))
	api := newSerialAuditedAPI(t, open(), model, &atomic.Int32{})
	cache := wazero.NewCompilationCache()
	t.Cleanup(func() { _ = cache.Close(context.Background()) })
	executor := execution.WazeroExecutor{CompilationCache: cache}
	serialRegisterWASM(t, api, path, executor)
	session := serialSession(t, api, "serial.wasm")
	before := model.calls
	evidence := serialRun(t, api, session, "Use the WASM sum tool to add 137, -29 and 8. Reply only with the number returned by the tool.", "116")
	serialWantTool(t, evidence, "serial.wasm_sum", 1)
	if model.calls-before != 2 || len(evidence.calls) != 1 || len(evidence.results) != 1 || !evidence.results[0].OK || strings.TrimSpace(evidence.results[0].Content) != "116" || strings.TrimSpace(evidence.answer) != "116" {
		t.Fatal("WASM acceptance requires one real successful tool execution and exactly two model calls")
	}
	if !reflect.DeepEqual(evidence.calls[0].Args["inputs"], []any{float64(137), float64(-29), float64(8)}) {
		t.Fatal("WASM did not receive the requested argv values")
	}
	history := serialAuditHistory(t, api, session)
	api.http.Close()
	if err := api.db.Close(); err != nil {
		t.Fatal(err)
	}
	restored := newSerialAuditedAPI(t, open(), model, &atomic.Int32{})
	if !reflect.DeepEqual(history, serialAuditHistory(t, restored, session)) {
		t.Fatal("WASM conversation changed after HTTP service / pool replacement")
	}
	t.Logf("serial wasm restored_events=%d extra_model_calls=0 executor_revision=%s", len(history), executor.ArtifactRevision())
}

func serialRegisterWASM(t *testing.T, api *serialAuditedAPI, path string, executor execution.WazeroExecutor) {
	t.Helper()
	product, _, _ := serialScopes()
	schema := map[string]any{"type": "object", "properties": map[string]any{"inputs": map[string]any{"type": "array", "items": map[string]any{"type": "integer"}, "minItems": 1, "maxItems": 8}}, "required": []any{"inputs"}, "additionalProperties": false}
	manifest := core.CapabilityManifest{ID: "serial.wasm_sum", Version: "1", Name: "WASM integer sum", Kind: core.KindComputation,
		Contract: "harness.tool/v1", InputSchema: schema, Idempotent: true,
		Tool:      &core.ToolExposure{Description: "Sum the integer inputs in an actual WASI computation module.", Parameters: schema},
		Execution: &core.ExecutionSpec{Runtime: "wazero", Entrypoint: path}}
	if err := api.server.runtime.Capabilities.RegisterExecutor(product, manifest, executor); err != nil {
		t.Fatal(err)
	}
	serialProfile(t, api, "serial.wasm", "Use serial.wasm_sum exactly once with the user's integer inputs. Return only its exact result. Never calculate the result yourself.", manifest.ID)
}

func TestPostgresSerialWASMComputation(t *testing.T) {
	t.Setenv("HARNESS_LLM_MODEL", "serial-offline-fixture")
	model := &serialAcceptanceModel{inner: serialWASMFixtureModel{}, test: t}
	serialWASMComputation(t, model)
}

// The guest and database are real; only the model is scripted. This verifies
// that rejecting a module is durably recorded as failure, including its span.
func TestPostgresSerialWASMResourceFailure(t *testing.T) {
	t.Setenv("HARNESS_LLM_MODEL", "serial-offline-fixture")
	model := &serialAcceptanceModel{inner: serialWASMFixtureModel{}, test: t}
	api := newSerialAuditedAPI(t, testdb.Postgres(t)(), model, &atomic.Int32{})
	serialRegisterWASM(t, api, testwasm.Calc(t), execution.WazeroExecutor{MemoryLimitPages: 1})
	evidence := serialRun(t, api, serialSession(t, api, "serial.wasm"), "Use the sum tool with 137, -29 and 8 and report its result.", "memory")
	if model.calls != 2 || len(evidence.results) != 1 || evidence.results[0].OK || !strings.Contains(evidence.results[0].Content, "memory") {
		t.Fatal("memory rejection was not persisted as one failed tool result")
	}
	errorSpans := 0
	for _, span := range api.exporter.GetSpans() {
		if span.Name == core.SpanToolCall {
			if span.Status.Code != codes.Error {
				t.Fatal("rejected WASM execution lacks an error span")
			}
			errorSpans++
		}
	}
	if errorSpans != 1 {
		t.Fatalf("expected one failed tool execution span, got %d", errorSpans)
	}
}

type serialWASMFixtureModel struct{}

func (serialWASMFixtureModel) Provider() string { return "openai-compatible" }
func (serialWASMFixtureModel) Stream(_ context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	if len(options.Messages) == 0 {
		return fmt.Errorf("empty WASM fixture request")
	}
	last := options.Messages[len(options.Messages)-1]
	if last.Role == core.RoleTool {
		emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: last.Content})
		emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop})
		return nil
	}
	call := core.ToolCall{ID: "wasm-fixture-call", Name: "serial.wasm_sum", Args: map[string]any{"inputs": []any{137, -29, 8}}}
	emit(core.StreamChunk{Kind: core.StreamKindAssistant, ToolCall: &call})
	emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishToolCalls})
	return nil
}
