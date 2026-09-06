// Package modelcontrol resolves a credential-safe immutable provider plan from
// a versioned model catalog. It deliberately contains metadata only: storage,
// HTTP, provider SDKs, wire formats, authentication, and streaming execution
// remain outside this package.
//
// A Registry is a static snapshot. An upper layer may asynchronously refresh a
// catalog, validate a replacement Registry, and atomically publish it. Reads
// never refresh or fall back implicitly. A provider can pair with several
// protocols through Compatibility entries; each CatalogModel chooses one exact
// pair.
package modelcontrol

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"unicode"
	"unicode/utf8"
)

const (
	// DefaultMaxEntries is the hard maximum for every snapshot collection.
	DefaultMaxEntries      = 256
	maxIDBytes             = 128
	maxWireModelBytes      = 256
	maxModalities          = 8
	maxContextWindowTokens = 16_777_216
)

var (
	ErrInvalidCatalog       = errors.New("invalid model catalog")
	ErrInvalidProvider      = errors.New("invalid model provider")
	ErrInvalidProtocol      = errors.New("invalid model protocol")
	ErrInvalidCompatibility = errors.New("invalid model compatibility")
	ErrNotFound             = errors.New("model control entry not found")
	ErrVersionMismatch      = errors.New("model control version mismatch")
	ErrIncompatible         = errors.New("model provider and protocol are incompatible")
)

// Ref identifies one exact versioned catalog, provider, or protocol entry.
type Ref struct {
	ID      string `json:"id"`
	Version string `json:"version"`
}

// CredentialRef is opaque credential evidence. It never contains a secret or
// secret preview. Its zero value represents a keyless provider; otherwise ID
// and Revision must both be present.
type CredentialRef struct {
	ID       string `json:"id"`
	Revision string `json:"revision"`
}

// EndpointRef is opaque endpoint-configuration evidence. It binds a provider
// plan without embedding a URL, HTTP client, SDK type, or credential.
type EndpointRef struct {
	ID       string `json:"id"`
	Revision string `json:"revision"`
}

// Modality is a bounded model input or output modality.
type Modality string

const (
	ModalityText  Modality = "text"
	ModalityImage Modality = "image"
	ModalityAudio Modality = "audio"
	ModalityVideo Modality = "video"
)

// ModelCapabilities is bounded routing/context evidence. ContextWindowTokens
// and MaxOutputTokens are positive and the latter cannot exceed the former.
type ModelCapabilities struct {
	ContextWindowTokens int        `json:"context_window_tokens"`
	MaxOutputTokens     int        `json:"max_output_tokens"`
	ToolCalls           bool       `json:"tool_calls"`
	Reasoning           bool       `json:"reasoning"`
	Modalities          []Modality `json:"modalities"`
}

// CatalogModel maps a logical model/version to a wire model, exact provider
// and protocol, opaque credential evidence, and bounded capabilities.
type CatalogModel struct {
	Ref          Ref               `json:"ref"`
	WireModel    string            `json:"wire_model"`
	Provider     Ref               `json:"provider"`
	Protocol     Ref               `json:"protocol"`
	Credential   CredentialRef     `json:"credential"`
	Capabilities ModelCapabilities `json:"capabilities"`
}

// ProviderSpec is metadata for one exact provider version. Endpoint and
// ImplementationRevision are evidence only; neither creates a client,
// resolves authentication, nor makes a network request.
type ProviderSpec struct {
	Ref                    Ref         `json:"ref"`
	Endpoint               EndpointRef `json:"endpoint"`
	ImplementationRevision string      `json:"implementation_revision"`
}

// ProtocolSpec is metadata for one exact wire protocol version. Message
// handoff/conversion belongs to the future protocol execution contract; this
// snapshot binds a protocol version so it cannot be silently assumed.
type ProtocolSpec struct {
	Ref                    Ref    `json:"ref"`
	ImplementationRevision string `json:"implementation_revision"`
}

// Compatibility explicitly permits one exact provider/protocol relation.
// There is no heuristic, priority, or version fallback.
type Compatibility struct {
	Provider Ref `json:"provider"`
	Protocol Ref `json:"protocol"`
}

// ResolveInput names a catalog model and carries upper-layer composition
// evidence. Profile/database resolution belongs above this package.
type ResolveInput struct {
	Catalog             Ref    `json:"catalog"`
	CompositionRevision string `json:"composition_revision"`
}

// ImplementationBinding is immutable execution evidence included in a plan.
// It excludes implementation factories and runtime objects.
type ImplementationBinding struct {
	Ref                    Ref    `json:"ref"`
	ImplementationRevision string `json:"implementation_revision"`
}

// ProviderPlan is a pure metadata result for one exact resolution. It can be
// persisted, audited, or handed to a future execution adapter without exposing
// factories, SDK values, endpoint URLs, or credential values.
type ProviderPlan struct {
	Catalog             CatalogModel          `json:"catalog"`
	Provider            ImplementationBinding `json:"provider"`
	Endpoint            EndpointRef           `json:"endpoint"`
	Protocol            ImplementationBinding `json:"protocol"`
	Credential          CredentialRef         `json:"credential"`
	CompositionRevision string                `json:"composition_revision"`
	SnapshotRevision    string                `json:"snapshot_revision"`
}

// Registry is an immutable bounded metadata snapshot. Construct a new one
// after an explicit refresh; it has no global registration, goroutine, timer,
// implicit catalog refresh, or execution service locator.
type Registry struct {
	catalogs        map[string]CatalogModel
	providers       map[string]ProviderSpec
	protocols       map[string]ProtocolSpec
	compatibilities map[string]Compatibility
	catalogIDs      map[string]struct{}
	revision        string
}

// NewRegistry validates closure and creates a defensive immutable snapshot.
// Every catalog and compatibility reference must resolve inside this snapshot,
// and every catalog pair must have explicit Compatibility.
func NewRegistry(catalogs []CatalogModel, providers []ProviderSpec, protocols []ProtocolSpec, compatibilities []Compatibility) (*Registry, error) {
	if len(catalogs) > DefaultMaxEntries || len(providers) > DefaultMaxEntries || len(protocols) > DefaultMaxEntries || len(compatibilities) > DefaultMaxEntries {
		return nil, fmt.Errorf("%w: entry capacity", ErrInvalidCatalog)
	}
	r := &Registry{
		catalogs: make(map[string]CatalogModel, len(catalogs)), providers: make(map[string]ProviderSpec, len(providers)),
		protocols: make(map[string]ProtocolSpec, len(protocols)), compatibilities: make(map[string]Compatibility, len(compatibilities)),
		catalogIDs: make(map[string]struct{}, len(catalogs)),
	}
	for _, catalog := range catalogs {
		if err := validateCatalog(catalog); err != nil {
			return nil, err
		}
		key := refKey(catalog.Ref)
		if _, ok := r.catalogs[key]; ok {
			return nil, fmt.Errorf("%w: duplicate %s", ErrInvalidCatalog, key)
		}
		catalog = cloneCatalog(catalog)
		r.catalogs[key], r.catalogIDs[catalog.Ref.ID] = catalog, struct{}{}
	}
	for _, provider := range providers {
		if err := validateProvider(provider); err != nil {
			return nil, err
		}
		key := refKey(provider.Ref)
		if _, ok := r.providers[key]; ok {
			return nil, fmt.Errorf("%w: duplicate %s", ErrInvalidProvider, key)
		}
		r.providers[key] = provider
	}
	for _, protocol := range protocols {
		if err := validateProtocol(protocol); err != nil {
			return nil, err
		}
		key := refKey(protocol.Ref)
		if _, ok := r.protocols[key]; ok {
			return nil, fmt.Errorf("%w: duplicate %s", ErrInvalidProtocol, key)
		}
		r.protocols[key] = protocol
	}
	for _, compatibility := range compatibilities {
		if err := validateCompatibility(compatibility); err != nil {
			return nil, err
		}
		key := compatibilityKey(compatibility.Provider, compatibility.Protocol)
		if _, ok := r.compatibilities[key]; ok {
			return nil, fmt.Errorf("%w: duplicate %s", ErrInvalidCompatibility, key)
		}
		r.compatibilities[key] = compatibility
	}
	if err := r.validateClosure(); err != nil {
		return nil, err
	}
	revision, err := snapshotRevision(r.Catalogs(), r.Providers(), r.Protocols(), r.Compatibilities())
	if err != nil {
		return nil, fmt.Errorf("%w: snapshot revision", ErrInvalidCatalog)
	}
	r.revision = revision
	return r, nil
}

// Resolve produces a deterministic metadata ProviderPlan. Unknown models and
// requested-version drift fail closed. NewRegistry has already rejected broken
// internal snapshot references, so Resolve never routes around them.
func (r *Registry) Resolve(input ResolveInput) (ProviderPlan, error) {
	if r == nil {
		return ProviderPlan{}, fmt.Errorf("%w: nil registry", ErrNotFound)
	}
	if err := validateRef(input.Catalog, ErrInvalidCatalog); err != nil {
		return ProviderPlan{}, err
	}
	if err := validateText(input.CompositionRevision, maxIDBytes, "composition revision", ErrInvalidCatalog); err != nil {
		return ProviderPlan{}, err
	}
	catalog, ok := r.catalogs[refKey(input.Catalog)]
	if !ok {
		if _, sameID := r.catalogIDs[input.Catalog.ID]; sameID {
			return ProviderPlan{}, fmt.Errorf("%w: catalog %s@%s", ErrVersionMismatch, input.Catalog.ID, input.Catalog.Version)
		}
		return ProviderPlan{}, fmt.Errorf("%w: catalog %s", ErrNotFound, input.Catalog.ID)
	}
	provider, protocol := r.providers[refKey(catalog.Provider)], r.protocols[refKey(catalog.Protocol)]
	return ProviderPlan{Catalog: cloneCatalog(catalog), Provider: ImplementationBinding{Ref: provider.Ref, ImplementationRevision: provider.ImplementationRevision}, Endpoint: provider.Endpoint, Protocol: ImplementationBinding(protocol), Credential: catalog.Credential, CompositionRevision: input.CompositionRevision, SnapshotRevision: r.revision}, nil
}

// SnapshotRevision is a SHA-256 digest of all resolution-relevant metadata.
func (r *Registry) SnapshotRevision() string {
	if r == nil {
		return ""
	}
	return r.revision
}

// Catalogs returns sorted defensive copies for diagnostics.
func (r *Registry) Catalogs() []CatalogModel {
	if r == nil {
		return nil
	}
	items := make([]CatalogModel, 0, len(r.catalogs))
	for _, v := range r.catalogs {
		items = append(items, cloneCatalog(v))
	}
	sort.Slice(items, func(i, j int) bool { return refKey(items[i].Ref) < refKey(items[j].Ref) })
	return items
}

// Providers returns sorted pure metadata for diagnostics.
func (r *Registry) Providers() []ProviderSpec {
	if r == nil {
		return nil
	}
	items := make([]ProviderSpec, 0, len(r.providers))
	for _, v := range r.providers {
		items = append(items, v)
	}
	sort.Slice(items, func(i, j int) bool { return refKey(items[i].Ref) < refKey(items[j].Ref) })
	return items
}

// Protocols returns sorted pure metadata for diagnostics.
func (r *Registry) Protocols() []ProtocolSpec {
	if r == nil {
		return nil
	}
	items := make([]ProtocolSpec, 0, len(r.protocols))
	for _, v := range r.protocols {
		items = append(items, v)
	}
	sort.Slice(items, func(i, j int) bool { return refKey(items[i].Ref) < refKey(items[j].Ref) })
	return items
}

// Compatibilities returns sorted explicit relations for diagnostics.
func (r *Registry) Compatibilities() []Compatibility {
	if r == nil {
		return nil
	}
	items := make([]Compatibility, 0, len(r.compatibilities))
	for _, v := range r.compatibilities {
		items = append(items, v)
	}
	sort.Slice(items, func(i, j int) bool {
		return compatibilityKey(items[i].Provider, items[i].Protocol) < compatibilityKey(items[j].Provider, items[j].Protocol)
	})
	return items
}

func (r *Registry) validateClosure() error {
	for _, c := range r.compatibilities {
		if _, ok := r.providers[refKey(c.Provider)]; !ok {
			return fmt.Errorf("%w: unknown provider %s", ErrInvalidCompatibility, c.Provider.ID)
		}
		if _, ok := r.protocols[refKey(c.Protocol)]; !ok {
			return fmt.Errorf("%w: unknown protocol %s", ErrInvalidCompatibility, c.Protocol.ID)
		}
	}
	for _, c := range r.catalogs {
		if _, ok := r.providers[refKey(c.Provider)]; !ok {
			return fmt.Errorf("%w: unknown provider %s", ErrInvalidCatalog, c.Provider.ID)
		}
		if _, ok := r.protocols[refKey(c.Protocol)]; !ok {
			return fmt.Errorf("%w: unknown protocol %s", ErrInvalidCatalog, c.Protocol.ID)
		}
		if _, ok := r.compatibilities[compatibilityKey(c.Provider, c.Protocol)]; !ok {
			return fmt.Errorf("%w: catalog pair", ErrIncompatible)
		}
	}
	return nil
}

func validateCatalog(c CatalogModel) error {
	if err := validateRef(c.Ref, ErrInvalidCatalog); err != nil {
		return err
	}
	if err := validateText(c.WireModel, maxWireModelBytes, "wire model", ErrInvalidCatalog); err != nil {
		return err
	}
	if err := validateRef(c.Provider, ErrInvalidCatalog); err != nil {
		return err
	}
	if err := validateRef(c.Protocol, ErrInvalidCatalog); err != nil {
		return err
	}
	if err := validateCredential(c.Credential); err != nil {
		return err
	}
	return validateCapabilities(c.Capabilities)
}
func validateProvider(p ProviderSpec) error {
	if err := validateRef(p.Ref, ErrInvalidProvider); err != nil {
		return err
	}
	if err := validateEndpoint(p.Endpoint); err != nil {
		return err
	}
	return validateText(p.ImplementationRevision, maxIDBytes, "implementation revision", ErrInvalidProvider)
}
func validateProtocol(p ProtocolSpec) error {
	if err := validateRef(p.Ref, ErrInvalidProtocol); err != nil {
		return err
	}
	return validateText(p.ImplementationRevision, maxIDBytes, "implementation revision", ErrInvalidProtocol)
}
func validateCompatibility(c Compatibility) error {
	if err := validateRef(c.Provider, ErrInvalidCompatibility); err != nil {
		return err
	}
	return validateRef(c.Protocol, ErrInvalidCompatibility)
}
func validateCredential(c CredentialRef) error {
	if c.ID == "" && c.Revision == "" {
		return nil
	}
	if err := validateText(c.ID, maxIDBytes, "credential id", ErrInvalidCatalog); err != nil {
		return err
	}
	return validateText(c.Revision, maxIDBytes, "credential revision", ErrInvalidCatalog)
}
func validateEndpoint(e EndpointRef) error {
	if err := validateText(e.ID, maxIDBytes, "endpoint id", ErrInvalidProvider); err != nil {
		return err
	}
	return validateText(e.Revision, maxIDBytes, "endpoint revision", ErrInvalidProvider)
}
func validateCapabilities(c ModelCapabilities) error {
	if c.ContextWindowTokens <= 0 || c.ContextWindowTokens > maxContextWindowTokens || c.MaxOutputTokens <= 0 || c.MaxOutputTokens > c.ContextWindowTokens {
		return fmt.Errorf("%w: invalid context capabilities", ErrInvalidCatalog)
	}
	if len(c.Modalities) == 0 || len(c.Modalities) > maxModalities {
		return fmt.Errorf("%w: invalid modalities", ErrInvalidCatalog)
	}
	seen := make(map[Modality]struct{}, len(c.Modalities))
	for _, m := range c.Modalities {
		switch m {
		case ModalityText, ModalityImage, ModalityAudio, ModalityVideo:
		default:
			return fmt.Errorf("%w: invalid modality", ErrInvalidCatalog)
		}
		if _, dup := seen[m]; dup {
			return fmt.Errorf("%w: duplicate modality", ErrInvalidCatalog)
		}
		seen[m] = struct{}{}
	}
	return nil
}
func validateRef(r Ref, kind error) error {
	if err := validateText(r.ID, maxIDBytes, "id", kind); err != nil {
		return err
	}
	return validateText(r.Version, maxIDBytes, "version", kind)
}
func validateText(v string, maximum int, name string, kind error) error {
	if v == "" || len(v) > maximum || !utf8.ValidString(v) {
		return fmt.Errorf("%w: invalid %s", kind, name)
	}
	for _, c := range v {
		if unicode.IsSpace(c) || unicode.IsControl(c) {
			return fmt.Errorf("%w: invalid %s", kind, name)
		}
	}
	return nil
}
func snapshotRevision(c []CatalogModel, p []ProviderSpec, q []ProtocolSpec, x []Compatibility) (string, error) {
	encoded, err := json.Marshal(struct {
		Catalogs        []CatalogModel  `json:"catalogs"`
		Providers       []ProviderSpec  `json:"providers"`
		Protocols       []ProtocolSpec  `json:"protocols"`
		Compatibilities []Compatibility `json:"compatibilities"`
	}{c, p, q, x})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(digest[:]), nil
}
func refKey(r Ref) string              { return r.ID + "\x00" + r.Version }
func compatibilityKey(p, q Ref) string { return refKey(p) + "\x00" + refKey(q) }
func cloneCatalog(c CatalogModel) CatalogModel {
	c.Capabilities.Modalities = append([]Modality(nil), c.Capabilities.Modalities...)
	sort.Slice(c.Capabilities.Modalities, func(i, j int) bool { return c.Capabilities.Modalities[i] < c.Capabilities.Modalities[j] })
	return c
}
