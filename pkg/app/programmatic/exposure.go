// Package programmatic projects explicitly opted-in capabilities into a
// non-sensitive tool catalog for trusted program bridges.
package programmatic

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/whhhh1500/auto-agent/pkg/core"
)

const (
	// ExposureKey is the only metadata key that opts a model tool into a
	// programmatic bridge. It does not grant authority by itself.
	ExposureKey = "harness.programmatic.exposure"
	// ExposureVersion is the supported projection contract version.
	ExposureVersion = "1"
)

var (
	ErrInvalidCatalog     = errors.New("invalid programmatic capability catalog")
	ErrBindingMismatch    = errors.New("programmatic capability binding mismatch")
	ErrCapabilityNotBound = errors.New("programmatic capability is not bound")
)

// Descriptor is the deliberately narrow public contract for one callable
// capability. It omits manifest metadata, execution configuration,
// credentials, permissions, identity, provider references, and source scope.
type Descriptor struct {
	Schema            core.ToolSchema
	CapabilityVersion string
	BindingDigest     string
	OutputSchema      map[string]any
	MaxOutputBytes    int
}

// Catalog is an immutable projection of a host-filtered capability set.
type Catalog struct {
	byID map[string]Descriptor
}

// Project projects only capabilities whose explicit exposure marker equals the
// supported version. The input must already be the host's permission-filtered,
// provider-free snapshot metadata. An explicit unknown marker fails closed.
func Project(capabilities []core.SnapshotCapability) (*Catalog, error) {
	byID := make(map[string]Descriptor, len(capabilities))
	seen := make(map[string]struct{}, len(capabilities))
	for _, capability := range capabilities {
		manifest := capability.Manifest
		if err := core.ValidateNamespacedID(manifest.ID); err != nil {
			return nil, fmt.Errorf("%w: capability id %q: %v", ErrInvalidCatalog, manifest.ID, err)
		}
		if manifest.Version == "" {
			return nil, fmt.Errorf("%w: capability %q has no version", ErrInvalidCatalog, manifest.ID)
		}
		if _, duplicate := seen[manifest.ID]; duplicate {
			return nil, fmt.Errorf("%w: duplicate capability id %q", ErrInvalidCatalog, manifest.ID)
		}
		seen[manifest.ID] = struct{}{}

		version, explicit := manifest.Metadata[ExposureKey]
		if !explicit {
			continue
		}
		if version != ExposureVersion {
			return nil, fmt.Errorf("%w: capability %q requests unsupported exposure version %q", ErrInvalidCatalog, manifest.ID, version)
		}
		if manifest.Tool == nil {
			return nil, fmt.Errorf("%w: capability %q is exposed programmatically without tool exposure", ErrInvalidCatalog, manifest.ID)
		}

		schema, err := toolSchema(manifest)
		if err != nil {
			return nil, fmt.Errorf("%w: capability %q: %v", ErrInvalidCatalog, manifest.ID, err)
		}
		output, err := cloneSchema(manifest.OutputSchema)
		if err != nil {
			return nil, fmt.Errorf("%w: capability %q output schema: %v", ErrInvalidCatalog, manifest.ID, err)
		}
		digest, err := bindingDigest(capability)
		if err != nil {
			return nil, fmt.Errorf("%w: capability %q binding: %v", ErrInvalidCatalog, manifest.ID, err)
		}
		maxOutput := manifest.MaxOutputBytes
		if maxOutput == 0 {
			maxOutput = core.DefaultMaxCapabilityOutputBytes
		}
		if maxOutput < 0 || maxOutput > core.HardMaxCapabilityOutputBytes {
			return nil, fmt.Errorf("%w: capability %q has invalid max output", ErrInvalidCatalog, manifest.ID)
		}
		byID[manifest.ID] = Descriptor{
			Schema: schema, CapabilityVersion: manifest.Version, BindingDigest: digest,
			OutputSchema: output, MaxOutputBytes: maxOutput,
		}
	}
	return &Catalog{byID: byID}, nil
}

// Descriptors returns stable-order defensive copies.
func (c *Catalog) Descriptors() []Descriptor {
	if c == nil {
		return nil
	}
	ids := make([]string, 0, len(c.byID))
	for id := range c.byID {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]Descriptor, 0, len(ids))
	for _, id := range ids {
		out = append(out, cloneDescriptor(c.byID[id]))
	}
	return out
}

// Resolve returns a descriptor only when both its ID and immutable binding
// digest match this catalog.
func (c *Catalog) Resolve(id, digest string) (Descriptor, bool) {
	if c == nil || !validDigest(digest) {
		return Descriptor{}, false
	}
	descriptor, ok := c.byID[id]
	if !ok || descriptor.BindingDigest != digest {
		return Descriptor{}, false
	}
	return cloneDescriptor(descriptor), true
}

// ValidateBindings verifies a submitted program catalog against this snapshot.
// A caller may bind a subset, but every submitted binding must be exact.
func (c *Catalog) ValidateBindings(bindings map[string]string) error {
	if c == nil {
		return fmt.Errorf("%w: catalog is unavailable", ErrBindingMismatch)
	}
	for id, digest := range bindings {
		if _, ok := c.Resolve(id, digest); !ok {
			return fmt.Errorf("%w: %s", ErrBindingMismatch, id)
		}
	}
	return nil
}

func toolSchema(manifest core.CapabilityManifest) (core.ToolSchema, error) {
	description := manifest.Tool.Description
	if description == "" {
		description = manifest.Description
	}
	if description == "" {
		description = manifest.Name
	}
	parameters := manifest.Tool.Parameters
	if parameters == nil {
		parameters = manifest.InputSchema
	}
	cloned, err := cloneSchema(parameters)
	if err != nil {
		return core.ToolSchema{}, err
	}
	if strings.TrimSpace(description) == "" {
		return core.ToolSchema{}, errors.New("tool description is empty")
	}
	return core.ToolSchema{Name: manifest.ID, Description: description, Parameters: cloned}, nil
}

func bindingDigest(capability core.SnapshotCapability) (string, error) {
	// SnapshotCapability is the complete host-provided public binding record,
	// including provider revision. Hashing it binds the program without exposing
	// its internal fields through the descriptor.
	encoded, err := json.Marshal(capability)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

func cloneDescriptor(value Descriptor) Descriptor {
	value.Schema.Parameters, _ = cloneSchema(value.Schema.Parameters)
	value.OutputSchema, _ = cloneSchema(value.OutputSchema)
	return value
}

func cloneSchema(schema map[string]any) (map[string]any, error) {
	if err := core.ValidateSchema(schema); err != nil {
		return nil, err
	}
	if schema == nil {
		return nil, nil
	}
	encoded, err := json.Marshal(schema)
	if err != nil {
		return nil, err
	}
	var cloned map[string]any
	if err := json.Unmarshal(encoded, &cloned); err != nil {
		return nil, err
	}
	return cloned, nil
}

func validDigest(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}
