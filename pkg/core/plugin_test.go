package core

import (
	"context"
	"errors"
	"strings"
	"testing"
)

type testPlugin struct{}

type panicPlugin struct{ stage string }

type closeErrorPlugin struct {
	installErr error
	closeErr   error
	panic      bool
}

func (plugin closeErrorPlugin) Manifest() PluginManifest {
	return PluginManifest{ID: "close-error.plugin", Version: "1.0.0"}
}

func (plugin closeErrorPlugin) Install(_ context.Context, mount *PluginMount) error {
	mount.OnUnmount(func(context.Context) error { return plugin.closeErr })
	if plugin.panic {
		panic("install")
	}
	return plugin.installErr
}

func (p panicPlugin) Manifest() PluginManifest {
	if p.stage == "manifest" {
		panic("manifest")
	}
	return PluginManifest{ID: "panic.plugin", Version: "1.0.0"}
}

func (panicPlugin) Install(context.Context, *PluginMount) error { panic("install") }

func (testPlugin) Manifest() PluginManifest {
	return PluginManifest{ID: "example.plugin", Version: "1.0.0"}
}

func (testPlugin) Install(_ context.Context, mount *PluginMount) error {
	if err := mount.Capability(staticTool{manifest: toolManifest("plugin.echo", "1.0.0"), content: "ok"}); err != nil {
		return err
	}
	name := "Plugin agent"
	model := ModelSelection{Provider: "mock", Model: "mock-1"}
	return mount.Profile(AgentProfileLayer{
		ProfileID: "plugin.agent", Name: &name, Model: &model, AddCapabilities: []string{"plugin.echo"},
	})
}

func TestPluginMountIsReversible(t *testing.T) {
	_, product, _, user := testScopes()
	principal := testPrincipal(user)
	host := PluginHost{Capabilities: NewCapabilityRegistry(), Profiles: NewAgentProfileRegistry()}
	mounted, err := host.MountPlugin(context.Background(), product, testPlugin{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.Profiles.Resolve(principal, user, "plugin.agent"); err != nil {
		t.Fatal(err)
	}
	if err := mounted.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := host.Profiles.Resolve(principal, user, "plugin.agent"); err == nil {
		t.Fatal("profile remained after plugin unmount")
	}
	snapshot, err := (CapabilityResolver{Registry: host.Capabilities}).Resolve(principal, user)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Authorized("plugin.echo") {
		t.Fatal("capability remained after plugin unmount")
	}
}

func TestPluginPanicsAreContained(t *testing.T) {
	_, product, _, _ := testScopes()
	host := PluginHost{Capabilities: NewCapabilityRegistry(), Profiles: NewAgentProfileRegistry()}
	for _, plugin := range []Plugin{panicPlugin{stage: "manifest"}, panicPlugin{stage: "install"}} {
		if _, err := host.MountPlugin(context.Background(), product, plugin); err == nil {
			t.Fatal("plugin panic was not contained")
		}
	}
}

func TestPluginMountReturnsInstallAndRollbackErrors(t *testing.T) {
	_, product, _, _ := testScopes()
	host := PluginHost{Capabilities: NewCapabilityRegistry(), Profiles: NewAgentProfileRegistry()}
	installErr := errors.New("install failed")
	closeErr := errors.New("cleanup failed")
	err := func() error {
		_, err := host.MountPlugin(context.Background(), product, closeErrorPlugin{installErr: installErr, closeErr: closeErr})
		return err
	}()
	if !errors.Is(err, installErr) || !errors.Is(err, closeErr) || !strings.Contains(err.Error(), "install plugin close-error.plugin:") {
		t.Fatalf("install/rollback error=%v", err)
	}
	panicErr := func() error {
		_, err := host.MountPlugin(context.Background(), product, closeErrorPlugin{closeErr: closeErr, panic: true})
		return err
	}()
	if !errors.Is(panicErr, closeErr) || !strings.Contains(panicErr.Error(), "install plugin close-error.plugin:") || !strings.Contains(panicErr.Error(), "plugin install panicked") {
		t.Fatalf("panic/rollback error=%v", panicErr)
	}
}
