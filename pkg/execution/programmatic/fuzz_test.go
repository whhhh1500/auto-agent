package programmatic

import (
	"context"
	"encoding/json"
	"testing"
)

// Accepted untrusted source must remain bounded even when its values are later
// serialized by a host. No fixture call performs an external effect.
func FuzzCompileAndRunBounded(f *testing.F) {
	f.Add([]byte(`{"version":"ptc-ir/v1","body":[{"op":"return","value":{"op":"literal","value":1}}]}`))
	f.Add([]byte(branchProgram))
	f.Add([]byte(`{"version":"ptc-ir/v1","body":[{"op":"call","tool":"fixture.read","args":{"op":"map","entries":{}}}]}`))
	f.Fuzz(func(t *testing.T, source []byte) {
		program, err := Compile(source)
		if err != nil {
			return
		}
		result, err := program.Run(context.Background(), map[string]any{}, func(context.Context, Call) (any, error) {
			return []any{map[string]any{"id": "fixture", "active": true}}, nil
		}, Limits{MaxSteps: 256, MaxLoopIterations: 32, MaxToolCalls: 8, MaxArenaBytes: 64 << 10})
		if err != nil {
			return
		}
		encoded, err := json.Marshal(result.Value)
		if err != nil {
			t.Fatalf("successful result is not JSON serializable: %v", err)
		}
		if result.Steps > 256 || result.ToolCalls > 8 || result.ArenaBytes > 64<<10 || len(encoded) > 1<<20 {
			t.Fatalf("successful result exceeded the bounded fixture: steps=%d calls=%d arena=%d JSON=%d", result.Steps, result.ToolCalls, result.ArenaBytes, len(encoded))
		}
	})
}
