package execution

import (
	"context"
	"testing"

	"github.com/cc-auto-agent/harness-core/internal/testwasm"
	"github.com/tetratelabs/wazero"
)

func TestWazeroGoWASI(t *testing.T) {
	path := testwasm.Calc(t)
	cache := wazero.NewCompilationCache()
	defer cache.Close(context.Background())
	executor := WazeroExecutor{CompilationCache: cache}
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"137", "-29", "8"}, "116"},
		{[]string{"1", "2"}, "3"},
		{nil, "0"},
	} {
		got, err := executor.Run(context.Background(), path, tc.args)
		if err != nil || got != tc.want {
			t.Fatalf("Go WASI argv=%v stdout=%q want=%q error=%v", tc.args, got, tc.want, err)
		}
	}
}

// Measures the complete Run path, including reading the module, decoding,
// compilation/cache lookup, WASI setup, guest execution and runtime cleanup.
// Guest build and warmup are outside the measured region. Cache allocation and
// closure are included in the cold-cache arm; warm retained cache bytes are not
// measured by B/op and need separate host-memory accounting.
func BenchmarkWazeroGoWASI(b *testing.B) {
	path := testwasm.Calc(b)
	ctx := context.Background()
	args := []string{"137", "-29", "8"}
	for _, mode := range []string{"uncached", "cache_cold", "cache_warm"} {
		b.Run(mode, func(b *testing.B) {
			executor := WazeroExecutor{}
			if mode == "cache_warm" {
				cache := wazero.NewCompilationCache()
				b.Cleanup(func() { _ = cache.Close(ctx) })
				executor.CompilationCache = cache
				if got, err := executor.Run(ctx, path, args); err != nil || got != "116" {
					b.Fatalf("warmup output=%q error=%v", got, err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if mode == "cache_cold" {
					executor.CompilationCache = wazero.NewCompilationCache()
				}
				got, err := executor.Run(ctx, path, args)
				if mode == "cache_cold" {
					_ = executor.CompilationCache.Close(ctx)
				}
				if err != nil || got != "116" {
					b.Fatalf("output=%q error=%v", got, err)
				}
			}
		})
	}
}
