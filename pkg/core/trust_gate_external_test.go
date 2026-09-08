package core_test

import (
	"testing"

	"github.com/whhhh1500/auto-agent/pkg/core"
)

func TestExternalPackageCannotForgeAcceptedInvocation(t *testing.T) {
	if err := core.RequireAcceptedInvocation(core.CapabilityRequest{}); err == nil {
		t.Fatal("zero value was accepted as a tool authorization proof")
	}
}

func TestExternalPackageCannotForgeAcceptedModelCall(t *testing.T) {
	if err := core.RequireAcceptedModelCall(core.GenerateOptions{}); err == nil {
		t.Fatal("zero value was accepted as a model authorization proof")
	}
}
