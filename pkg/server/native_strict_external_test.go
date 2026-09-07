package server_test

import (
	"context"
	"reflect"
	"testing"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
	"github.com/cc-auto-agent/harness-core/pkg/server"
)

var _ func(context.Context, server.NativeStrictServerConfig) (*server.Server, error) = server.NewNativeStrictServer

func TestNativeStrictPublicAPIStaysClosed(t *testing.T) {
	configType := reflect.TypeFor[server.NativeStrictServerConfig]()
	for _, forbidden := range []string{
		"Runtime", "Capabilities", "Profiles", "Policies", "RunPrincipalResolver", "BindingJournal",
		"Releases", "Canaries", "CredentialResolver", "ModelResolver", "RunExecutors",
	} {
		if _, found := configType.FieldByName(forbidden); found {
			t.Fatalf("native strict config exposes %s", forbidden)
		}
	}
	bootstrapType := reflect.TypeFor[server.NativeStrictBootstrap]()
	for _, required := range []string{"Revision", "Root", "DefaultProfileID", "Profiles", "Policies", "Capabilities", "Model"} {
		if _, found := bootstrapType.FieldByName(required); !found {
			t.Fatalf("native strict bootstrap is missing %s", required)
		}
	}
	serverType := reflect.TypeOf((*server.Server)(nil))
	for _, forbidden := range []string{"Runtime", "Profiles", "Capabilities", "Policies", "NativeStrictOwnership"} {
		if _, found := serverType.MethodByName(forbidden); found {
			t.Fatalf("server exposes %s getter", forbidden)
		}
	}

	_ = server.NativeStrictCapability{Capability: nativeStrictExternalCapability{}}
	_ = server.NativeStrictModel{Selection: core.ModelSelection{Provider: "mock", Model: "static"}, Adapter: core.MockLlmAdapter{}}
}

type nativeStrictExternalCapability struct{}

func (nativeStrictExternalCapability) Manifest() core.CapabilityManifest {
	return core.CapabilityManifest{ID: "external.check", Version: "v1", Name: "External check", Kind: core.KindTool}
}

func (nativeStrictExternalCapability) Execute(context.Context, core.CapabilityRequest) (core.CapabilityResult, error) {
	return core.CapabilityResult{}, nil
}
