package modelruntime

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"unicode"
	"unicode/utf8"

	"github.com/whhhh1500/auto-agent/pkg/app/modelcatalog"
	"github.com/whhhh1500/auto-agent/pkg/app/modelcontrol"
	"github.com/whhhh1500/auto-agent/pkg/app/modelexecution"
	appmodelsettings "github.com/whhhh1500/auto-agent/pkg/app/modelsettings"
)

const maxPlugins = 32

// ProviderBuildInput contains the private, one-time inputs needed to make a
// provider registration. Its endpoint and credential references are opaque
// evidence; BaseURL and APIKey never enter a ProviderPlan or artifact revision.
type ProviderBuildInput struct {
	Binding    modelcontrol.ImplementationBinding
	Endpoint   modelcontrol.EndpointRef
	BaseURL    string
	Credential modelcontrol.CredentialRef
	APIKey     string
}

// ProtocolBuildInput fixes an exact protocol implementation and its output
// bound for one compiled model configuration.
type ProtocolBuildInput struct {
	Binding   modelcontrol.ImplementationBinding
	MaxTokens int
}

// ProviderPlugin and ProtocolPlugin are independent registration seams. Build
// functions must be bounded, local construction only: no network requests,
// global mutation, or hidden fallback.
type ProviderPlugin struct {
	ID                     appmodelsettings.ProviderID
	Version                string
	ImplementationRevision string
	Build                  func(ProviderBuildInput) (modelexecution.ProviderRegistration, error)
}

type ProtocolPlugin struct {
	ID                     appmodelsettings.ProtocolID
	Version                string
	ImplementationRevision string
	Build                  func(ProtocolBuildInput) (modelexecution.ProtocolRegistration, error)
}

// PluginRegistry is immutable after construction. One ID has one active exact
// version; upgrade by creating and atomically publishing a replacement
// registry, rather than silently changing an existing compiler's evidence.
type PluginRegistry struct {
	providers       map[appmodelsettings.ProviderID]ProviderPlugin
	protocols       map[appmodelsettings.ProtocolID]ProtocolPlugin
	compatibilities []modelcatalog.Compatibility
	defaults        []modelcatalog.Default
	providerKeys    map[string]struct{}
	protocolKeys    map[string]struct{}
	providerRefs    map[string]modelcatalog.Ref
	protocolRefs    map[string]modelcatalog.Ref
	pairKeys        map[string]struct{}
	revision        string
}

var _ modelcatalog.View = (*PluginRegistry)(nil)

func (r *PluginRegistry) SnapshotRevision() string {
	if r == nil {
		return ""
	}
	return r.revision
}
func computeRevision(r *PluginRegistry) string {
	b, err := json.Marshal(struct {
		Providers []modelcatalog.Provider      `json:"providers"`
		Protocols []modelcatalog.Protocol      `json:"protocols"`
		Compat    []modelcatalog.Compatibility `json:"compatibilities"`
		Defaults  []modelcatalog.Default       `json:"defaults"`
	}{r.Providers(), r.Protocols(), r.Compatibilities(), r.Defaults()})
	if err != nil {
		return ""
	}
	d := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(d[:])
}
func (r *PluginRegistry) Providers() []modelcatalog.Provider {
	if r == nil {
		return nil
	}
	out := make([]modelcatalog.Provider, 0, len(r.providers))
	for id, p := range r.providers {
		out = append(out, modelcatalog.Provider{Ref: modelcatalog.Ref{ID: string(id), Version: p.Version}, ImplementationRevision: p.ImplementationRevision})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Ref.ID != out[j].Ref.ID {
			return out[i].Ref.ID < out[j].Ref.ID
		}
		return out[i].Ref.Version < out[j].Ref.Version
	})
	return out
}
func (r *PluginRegistry) Protocols() []modelcatalog.Protocol {
	if r == nil {
		return nil
	}
	out := make([]modelcatalog.Protocol, 0, len(r.protocols))
	for id, p := range r.protocols {
		out = append(out, modelcatalog.Protocol{Ref: modelcatalog.Ref{ID: string(id), Version: p.Version}, ImplementationRevision: p.ImplementationRevision})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Ref.ID != out[j].Ref.ID {
			return out[i].Ref.ID < out[j].Ref.ID
		}
		return out[i].Ref.Version < out[j].Ref.Version
	})
	return out
}
func (r *PluginRegistry) Compatibilities() []modelcatalog.Compatibility {
	if r == nil {
		return nil
	}
	return append([]modelcatalog.Compatibility(nil), r.compatibilities...)
}
func (r *PluginRegistry) Defaults() []modelcatalog.Default {
	if r == nil {
		return nil
	}
	return append([]modelcatalog.Default(nil), r.defaults...)
}
func (r *PluginRegistry) Compatible(provider, protocol modelcatalog.Ref) (bool, bool, bool) {
	if r == nil {
		return false, false, false
	}
	_, knownProvider := r.providerKeys[provider.ID+"\x00"+provider.Version]
	_, knownProtocol := r.protocolKeys[protocol.ID+"\x00"+protocol.Version]
	_, compatible := r.pairKeys[provider.ID+"\x00"+provider.Version+"\x00"+protocol.ID+"\x00"+protocol.Version]
	return knownProvider, knownProtocol, compatible
}
func (r *PluginRegistry) Assess(providerID, protocolID string) (bool, bool, bool) {
	if r == nil {
		return false, false, false
	}
	var provider, protocol modelcatalog.Ref
	provider = r.providerRefs[providerID]
	protocol = r.protocolRefs[protocolID]
	if provider.ID == "" || protocol.ID == "" {
		return provider.ID != "", protocol.ID != "", false
	}
	return r.Compatible(provider, protocol)
}
func NewPluginRegistry(providers []ProviderPlugin, protocols []ProtocolPlugin) (*PluginRegistry, error) {
	return NewPluginRegistryWithPolicy(providers, protocols, nil, nil)
}
func NewPluginRegistryWithPolicy(providers []ProviderPlugin, protocols []ProtocolPlugin, compatibilities []modelcatalog.Compatibility, defaults []modelcatalog.Default) (*PluginRegistry, error) {
	if len(providers) > maxPlugins || len(protocols) > maxPlugins || len(compatibilities) > maxPlugins*maxPlugins || len(defaults) > maxPlugins {
		return nil, fmt.Errorf("model runtime plugin capacity exceeded")
	}
	r := &PluginRegistry{providers: make(map[appmodelsettings.ProviderID]ProviderPlugin, len(providers)), protocols: make(map[appmodelsettings.ProtocolID]ProtocolPlugin, len(protocols)), compatibilities: append([]modelcatalog.Compatibility(nil), compatibilities...), defaults: append([]modelcatalog.Default(nil), defaults...), providerKeys: map[string]struct{}{}, protocolKeys: map[string]struct{}{}, providerRefs: map[string]modelcatalog.Ref{}, protocolRefs: map[string]modelcatalog.Ref{}, pairKeys: map[string]struct{}{}}
	for _, plugin := range providers {
		if err := validateProviderPlugin(plugin); err != nil {
			return nil, err
		}
		if _, exists := r.providers[plugin.ID]; exists {
			return nil, fmt.Errorf("duplicate model runtime provider %q", plugin.ID)
		}
		r.providers[plugin.ID] = plugin
		r.providerKeys[string(plugin.ID)+"\x00"+plugin.Version] = struct{}{}
		r.providerRefs[string(plugin.ID)] = modelcatalog.Ref{ID: string(plugin.ID), Version: plugin.Version}
	}
	for _, plugin := range protocols {
		if err := validateProtocolPlugin(plugin); err != nil {
			return nil, err
		}
		if _, exists := r.protocols[plugin.ID]; exists {
			return nil, fmt.Errorf("duplicate model runtime protocol %q", plugin.ID)
		}
		r.protocols[plugin.ID] = plugin
		r.protocolKeys[string(plugin.ID)+"\x00"+plugin.Version] = struct{}{}
		r.protocolRefs[string(plugin.ID)] = modelcatalog.Ref{ID: string(plugin.ID), Version: plugin.Version}
	}
	for _, c := range r.compatibilities {
		ck := c.Provider.ID + "\x00" + c.Provider.Version + "\x00" + c.Protocol.ID + "\x00" + c.Protocol.Version
		if _, exists := r.pairKeys[ck]; exists {
			return nil, fmt.Errorf("invalid model runtime compatibility")
		}
		r.pairKeys[ck] = struct{}{}
		if !safePluginID(c.Provider.ID) || !safePluginID(c.Provider.Version) || !safePluginID(c.Protocol.ID) || !safePluginID(c.Protocol.Version) {
			return nil, fmt.Errorf("invalid model runtime compatibility")
		}
		p, pok := r.providers[appmodelsettings.ProviderID(c.Provider.ID)]
		q, qok := r.protocols[appmodelsettings.ProtocolID(c.Protocol.ID)]
		if !pok || p.Version != c.Provider.Version {
			return nil, fmt.Errorf("invalid model runtime compatibility")
		}
		if !qok || q.Version != c.Protocol.Version {
			return nil, fmt.Errorf("invalid model runtime compatibility")
		}
	}
	seenDefaults := make(map[string]struct{}, len(r.defaults))
	for _, d := range r.defaults {
		if _, exists := seenDefaults[d.Provider.ID+"\x00"+d.Provider.Version]; exists {
			return nil, fmt.Errorf("invalid model runtime default")
		}
		seenDefaults[d.Provider.ID+"\x00"+d.Provider.Version] = struct{}{}
		if !safePluginID(d.Provider.ID) || !safePluginID(d.Provider.Version) || !safePluginID(d.Protocol.ID) || !safePluginID(d.Protocol.Version) {
			return nil, fmt.Errorf("invalid model runtime default")
		}
		p, pok := r.providers[appmodelsettings.ProviderID(d.Provider.ID)]
		q, qok := r.protocols[appmodelsettings.ProtocolID(d.Protocol.ID)]
		if !pok || p.Version != d.Provider.Version {
			return nil, fmt.Errorf("invalid model runtime default")
		}
		if !qok || q.Version != d.Protocol.Version {
			return nil, fmt.Errorf("invalid model runtime default")
		}
		for _, c := range r.compatibilities {
			if c.Provider == d.Provider && c.Protocol == d.Protocol {
				qok = true
				break
			} else {
				qok = false
			}
		}
		if !qok {
			return nil, fmt.Errorf("invalid model runtime default")
		}
	}
	sort.Slice(r.compatibilities, func(i, j int) bool {
		a, b := r.compatibilities[i], r.compatibilities[j]
		ak, bk := a.Provider.ID+"\x00"+a.Provider.Version+"\x00"+a.Protocol.ID+"\x00"+a.Protocol.Version, b.Provider.ID+"\x00"+b.Provider.Version+"\x00"+b.Protocol.ID+"\x00"+b.Protocol.Version
		return ak < bk
	})
	sort.Slice(r.defaults, func(i, j int) bool {
		a, b := r.defaults[i], r.defaults[j]
		return a.Provider.ID+"\x00"+a.Provider.Version+"\x00"+a.Protocol.ID+"\x00"+a.Protocol.Version < b.Provider.ID+"\x00"+b.Provider.Version+"\x00"+b.Protocol.ID+"\x00"+b.Protocol.Version
	})
	r.revision = computeRevision(r)
	if r.revision == "" {
		return nil, fmt.Errorf("invalid model runtime revision")
	}
	return r, nil
}

func (r *PluginRegistry) provider(id appmodelsettings.ProviderID) (ProviderPlugin, error) {
	if r == nil {
		return ProviderPlugin{}, fmt.Errorf("model runtime plugin registry is unavailable")
	}
	plugin, ok := r.providers[id]
	if !ok {
		return ProviderPlugin{}, fmt.Errorf("model runtime provider %q is not registered", id)
	}
	return plugin, nil
}

func (r *PluginRegistry) protocol(id appmodelsettings.ProtocolID) (ProtocolPlugin, error) {
	if r == nil {
		return ProtocolPlugin{}, fmt.Errorf("model runtime plugin registry is unavailable")
	}
	plugin, ok := r.protocols[id]
	if !ok {
		return ProtocolPlugin{}, fmt.Errorf("model runtime protocol %q is not registered", id)
	}
	return plugin, nil
}

// ProviderIDs and ProtocolIDs are sorted defensive diagnostic views.
func (r *PluginRegistry) ProviderIDs() []appmodelsettings.ProviderID {
	if r == nil {
		return nil
	}
	ids := make([]appmodelsettings.ProviderID, 0, len(r.providers))
	for id := range r.providers {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}
func (r *PluginRegistry) ProtocolIDs() []appmodelsettings.ProtocolID {
	if r == nil {
		return nil
	}
	ids := make([]appmodelsettings.ProtocolID, 0, len(r.protocols))
	for id := range r.protocols {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func validateProviderPlugin(p ProviderPlugin) error {
	if !safePluginID(string(p.ID)) || !safePluginID(p.Version) || !safePluginID(p.ImplementationRevision) || p.Build == nil {
		return fmt.Errorf("invalid model runtime provider plugin")
	}
	return nil
}
func validateProtocolPlugin(p ProtocolPlugin) error {
	if !safePluginID(string(p.ID)) || !safePluginID(p.Version) || !safePluginID(p.ImplementationRevision) || p.Build == nil {
		return fmt.Errorf("invalid model runtime protocol plugin")
	}
	return nil
}
func safePluginID(value string) bool {
	if value == "" || len(value) > 256 || !utf8.ValidString(value) {
		return false
	}
	for _, r := range value {
		if unicode.IsSpace(r) || unicode.IsControl(r) || unicode.Is(unicode.Cf, r) || unicode.Is(unicode.Cs, r) || unicode.Is(unicode.Co, r) {
			return false
		}
	}
	return true
}
