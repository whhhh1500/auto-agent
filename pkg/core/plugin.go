package core

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// PluginManifest identifies one installable bundle of capabilities and profiles.
type PluginManifest struct {
	ID          string            `json:"id"`
	Version     string            `json:"version"`
	Description string            `json:"description,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

// Plugin contributes definitions to a fixed ownership scope.
type Plugin interface {
	Manifest() PluginManifest
	Install(ctx context.Context, mount *PluginMount) error
}

// PluginHost owns the registries into which plugins are mounted.
type PluginHost struct {
	Capabilities *CapabilityRegistry
	Profiles     *AgentProfileRegistry
	Credentials  *CredentialRegistry
	Policies     *PolicyRegistry
}

// PluginMount exposes concise, scope-fixed registration methods to a plugin.
type PluginMount struct {
	scope    ScopePath
	host     PluginHost
	cleanups []func(context.Context) error
	unmounts []func()
	closed   bool
	mu       sync.Mutex
}

// Scope returns the ownership scope selected by the deployer.
func (m *PluginMount) Scope() ScopePath { return m.scope }

// Capability provides an in-process model-tool capability.
func (m *PluginMount) Capability(capability Capability) error {
	if capability == nil {
		return fmt.Errorf("plugin capability is nil")
	}
	return m.CapabilityBinding(CapabilityBinding{
		Mode: BindingProvide, Manifest: capability.Manifest(), Provider: capability,
	})
}

// CapabilityBinding contributes any provider contract at the mount scope.
func (m *PluginMount) CapabilityBinding(binding CapabilityBinding) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return fmt.Errorf("plugin mount is closed")
	}
	binding.Scope = m.scope
	unmount, err := m.host.Capabilities.Mount(binding)
	if err != nil {
		return err
	}
	m.unmounts = append(m.unmounts, unmount)
	return nil
}

// Profile contributes one layered agent or digital-human configuration.
func (m *PluginMount) Profile(layer AgentProfileLayer) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return fmt.Errorf("plugin mount is closed")
	}
	layer.Scope = m.scope
	unmount, err := m.host.Profiles.Mount(layer)
	if err != nil {
		return err
	}
	m.unmounts = append(m.unmounts, unmount)
	return nil
}

// Credential contributes a secret reference provider at the mount scope.
func (m *PluginMount) Credential(binding CredentialBinding) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return fmt.Errorf("plugin mount is closed")
	}
	if m.host.Credentials == nil {
		return fmt.Errorf("plugin host credential registry is nil")
	}
	binding.Scope = m.scope
	unmount, err := m.host.Credentials.Mount(binding)
	if err != nil {
		return err
	}
	m.unmounts = append(m.unmounts, unmount)
	return nil
}

// Policy contributes a narrowing policy layer at the mount scope.
func (m *PluginMount) Policy(layer PolicyLayer) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return fmt.Errorf("plugin mount is closed")
	}
	if m.host.Policies == nil {
		return fmt.Errorf("plugin host policy registry is nil")
	}
	layer.Scope = m.scope
	unmount, err := m.host.Policies.Mount(layer)
	if err != nil {
		return err
	}
	m.unmounts = append(m.unmounts, unmount)
	return nil
}

// OnUnmount registers resource cleanup in reverse installation order. Plugins
// must register cleanup synchronously from Install and must not retain a mount
// for asynchronous or post-Close registration.
func (m *PluginMount) OnUnmount(cleanup func(context.Context) error) {
	if cleanup == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cleanups = append(m.cleanups, cleanup)
}

// MountedPlugin is an installed, reversible plugin instance.
type MountedPlugin struct {
	Manifest PluginManifest
	mount    *PluginMount
	once     sync.Once
	err      error
}

// MountPlugin installs a plugin transactionally and rolls back on failure.
func (h PluginHost) MountPlugin(ctx context.Context, scope ScopePath, plugin Plugin) (*MountedPlugin, error) {
	if h.Capabilities == nil || h.Profiles == nil {
		return nil, fmt.Errorf("plugin host registries are incomplete")
	}
	if plugin == nil {
		return nil, fmt.Errorf("plugin is nil")
	}
	manifest, err := safePluginManifest(plugin)
	if err != nil {
		return nil, err
	}
	if ValidateNamespacedID(manifest.ID) != nil || manifest.Version == "" {
		return nil, fmt.Errorf("plugin manifest requires a namespaced id and version")
	}
	mount := &PluginMount{scope: scope, host: h}
	installed := &MountedPlugin{Manifest: manifest, mount: mount}
	if err := safePluginInstall(plugin, ctx, mount); err != nil {
		return nil, fmt.Errorf("install plugin %s: %w", manifest.ID, errors.Join(err, installed.Close(ctx)))
	}
	return installed, nil
}

// Close removes contributions and then releases plugin-owned resources.
func (p *MountedPlugin) Close(ctx context.Context) error {
	if p == nil || p.mount == nil {
		return nil
	}
	p.once.Do(func() {
		p.mount.mu.Lock()
		p.mount.closed = true
		unmounts := append([]func(){}, p.mount.unmounts...)
		cleanups := append([]func(context.Context) error{}, p.mount.cleanups...)
		p.mount.mu.Unlock()
		for i := len(unmounts) - 1; i >= 0; i-- {
			safePluginUnmount(unmounts[i])
		}
		for i := len(cleanups) - 1; i >= 0; i-- {
			if err := safePluginCleanup(cleanups[i], ctx); err != nil && p.err == nil {
				p.err = err
			}
		}
	})
	return p.err
}

func safePluginManifest(plugin Plugin) (manifest PluginManifest, err error) {
	defer func() {
		if recover() != nil {
			err = fmt.Errorf("plugin manifest panicked")
		}
	}()
	return plugin.Manifest(), nil
}

func safePluginInstall(plugin Plugin, ctx context.Context, mount *PluginMount) (err error) {
	defer func() {
		if recover() != nil {
			err = fmt.Errorf("plugin install panicked")
		}
	}()
	return plugin.Install(ctx, mount)
}

func safePluginUnmount(unmount func()) {
	defer func() { _ = recover() }()
	unmount()
}

func safePluginCleanup(cleanup func(context.Context) error, ctx context.Context) (err error) {
	defer func() {
		if recover() != nil {
			err = fmt.Errorf("plugin cleanup panicked")
		}
	}()
	return cleanup(ctx)
}
