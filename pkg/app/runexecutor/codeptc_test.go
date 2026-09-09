package runexecutor

import (
	"context"
	"errors"
	"testing"

	"github.com/whhhh1500/auto-agent/pkg/core"
)

func TestCodePTCReservationDoesNotRegisterDefaultExecutor(t *testing.T) {
	registry, err := NewDefaultRegistry()
	if err != nil {
		t.Fatal(err)
	}
	for _, metadata := range registry.List() {
		if metadata.ID == CodePTCID {
			t.Fatalf("default registry unexpectedly lists %q", CodePTCID)
		}
	}
}

func TestCodePTCExplicitSelectionDoesNotFallBack(t *testing.T) {
	registry, err := NewDefaultRegistry()
	if err != nil {
		t.Fatal(err)
	}

	for _, resolve := range []struct {
		name string
		call func() (RunExecutor, Metadata, error)
	}{
		{
			name: "exact",
			call: func() (RunExecutor, Metadata, error) {
				return registry.Resolve(CodePTCID, CodePTCVersion, Dependencies{})
			},
		},
		{
			name: "or_default",
			call: func() (RunExecutor, Metadata, error) {
				return registry.ResolveOrDefault(CodePTCID, CodePTCVersion, Dependencies{})
			},
		},
	} {
		t.Run(resolve.name, func(t *testing.T) {
			executor, metadata, err := resolve.call()
			if !errors.Is(err, ErrExecutorNotFound) {
				t.Fatalf("err=%v, want ErrExecutorNotFound", err)
			}
			if executor != nil || metadata != (Metadata{}) {
				t.Fatalf("executor=%T metadata=%#v, want no fallback", executor, metadata)
			}
		})
	}
}

func TestCodePTCFutureHostRegistrationResolvesExactReservation(t *testing.T) {
	registration := Registration{
		Metadata: Metadata{
			ID:                     CodePTCID,
			Version:                CodePTCVersion,
			ImplementationRevision: "host-test-v1",
		},
		Factory: func(Dependencies) (RunExecutor, error) { return codePTCTestExecutor{}, nil },
	}
	registry, err := NewRegistry(1, registration)
	if err != nil {
		t.Fatal(err)
	}

	executor, metadata, err := registry.Resolve(CodePTCID, CodePTCVersion, Dependencies{})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := executor.(codePTCTestExecutor); !ok {
		t.Fatalf("executor=%T", executor)
	}
	if metadata != registration.Metadata {
		t.Fatalf("metadata=%#v want=%#v", metadata, registration.Metadata)
	}
}

type codePTCTestExecutor struct{}

func (codePTCTestExecutor) RunTurn(context.Context, core.Principal, *core.Session, core.TurnInput, func(core.SessionEvent)) (core.TurnResult, error) {
	return core.TurnResult{}, nil
}

func (codePTCTestExecutor) ResumeTurn(context.Context, core.Principal, *core.Session, core.ResumeInput, func(core.SessionEvent)) (core.TurnResult, error) {
	return core.TurnResult{}, nil
}
