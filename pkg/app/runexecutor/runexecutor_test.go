package runexecutor

import (
	"context"
	"errors"
	"github.com/cc-auto-agent/harness-core/pkg/core"
	"sync"
	"testing"
)

func TestSequentialUsesPerResolveRuntime(t *testing.T) {
	r, _ := NewDefaultRegistry()
	a, b := &core.Runtime{}, &core.Runtime{}
	ea, _, _ := r.ResolveOrDefault("", "", Dependencies{Runtime: a})
	eb, _, _ := r.ResolveOrDefault("", "", Dependencies{Runtime: b})
	if ea.(*Sequential).runtime != a || eb.(*Sequential).runtime != b {
		t.Fatal("runtime leaked")
	}
}
func TestRegistryFrozenAndExact(t *testing.T) {
	r, e := NewRegistry(2, testRegistration("a", "1"), testRegistration("b", "1"))
	if e != nil {
		t.Fatal(e)
	}
	if _, _, e = r.Resolve("a", "1", Dependencies{}); e != nil {
		t.Fatal(e)
	}
	if _, _, e = r.Resolve("a", "2", Dependencies{}); !errors.Is(e, ErrVersionMismatch) {
		t.Fatal(e)
	}
}
func TestConcurrentResolve(t *testing.T) {
	r, _ := NewRegistry(1, testRegistration("a", "1"))
	var w sync.WaitGroup
	for i := 0; i < 32; i++ {
		w.Add(1)
		go func() {
			defer w.Done()
			if _, _, e := r.Resolve("a", "1", Dependencies{}); e != nil {
				t.Error(e)
			}
		}()
	}
	w.Wait()
}
func testRegistration(id, version string) Registration {
	return Registration{Metadata: Metadata{ID: id, Version: version, ImplementationRevision: "rev"}, Factory: func(Dependencies) (RunExecutor, error) { return testExecutor{}, nil }}
}

type testExecutor struct{}

func (testExecutor) RunTurn(context.Context, core.Principal, *core.Session, core.TurnInput, func(core.SessionEvent)) (core.TurnResult, error) {
	return core.TurnResult{}, nil
}
func (testExecutor) ResumeTurn(context.Context, core.Principal, *core.Session, core.ResumeInput, func(core.SessionEvent)) (core.TurnResult, error) {
	return core.TurnResult{}, nil
}
