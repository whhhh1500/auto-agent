package runtime

import (
	"fmt"
	"strings"
	"time"
	"unicode"
)

type ModuleID string
type ExtensionID string
type ContractID string
type EffectKind string

type Version struct {
	Major int `json:"major"`
	Minor int `json:"minor"`
	Patch int `json:"patch"`
}

func (version Version) Valid() bool {
	return version.Major >= 0 && version.Minor >= 0 && version.Patch >= 0
}

func (version Version) Compare(other Version) int {
	if version.Major != other.Major {
		if version.Major < other.Major {
			return -1
		}
		return 1
	}
	if version.Minor != other.Minor {
		if version.Minor < other.Minor {
			return -1
		}
		return 1
	}
	if version.Patch < other.Patch {
		return -1
	}
	if version.Patch > other.Patch {
		return 1
	}
	return 0
}

func (version Version) String() string {
	return fmt.Sprintf("%d.%d.%d", version.Major, version.Minor, version.Patch)
}

type VersionRange struct {
	Min Version  `json:"min"`
	Max *Version `json:"max,omitempty"`
}

type VersionConstraint = VersionRange

func (versionRange VersionRange) Valid() bool {
	if !versionRange.Min.Valid() {
		return false
	}
	return versionRange.Max == nil || (versionRange.Max.Valid() && versionRange.Min.Compare(*versionRange.Max) <= 0)
}

func (versionRange VersionRange) Contains(version Version) bool {
	if !versionRange.Valid() || !version.Valid() {
		return false
	}
	if version.Compare(versionRange.Min) < 0 {
		return false
	}
	return versionRange.Max == nil || version.Compare(*versionRange.Max) <= 0
}

type ExtensionSemantic string

const (
	SemanticCollection       ExtensionSemantic = "collection"
	SemanticPipeline         ExtensionSemantic = "pipeline"
	SemanticScopedOverlay    ExtensionSemantic = "scoped_overlay"
	SemanticSingleStrategy   ExtensionSemantic = "single_strategy"
	SemanticStatefulResource ExtensionSemantic = "stateful_resource"
)

func (semantic ExtensionSemantic) Valid() bool {
	switch semantic {
	case SemanticCollection, SemanticPipeline, SemanticScopedOverlay, SemanticSingleStrategy, SemanticStatefulResource:
		return true
	default:
		return false
	}
}

// Dependency identifies one module contract. Required and Optional are kept
// separate on ModuleManifest; absence is tolerated only for Optional entries.
type Dependency struct {
	ModuleID ModuleID     `json:"module_id"`
	Contract ContractID   `json:"contract"`
	Version  VersionRange `json:"version"`
}

func (dependency Dependency) moduleID() ModuleID { return dependency.ModuleID }

type ProvidedExtension struct {
	ID          ExtensionID       `json:"id"`
	Semantic    ExtensionSemantic `json:"semantic"`
	Contract    ContractID        `json:"contract"`
	Version     Version           `json:"version"`
	Priority    int               `json:"priority"`
	Before      []ExtensionID     `json:"before,omitempty"`
	After       []ExtensionID     `json:"after,omitempty"`
	ResourceKey string            `json:"resource_key,omitempty"`
}

type ModuleManifest struct {
	ID            ModuleID            `json:"id"`
	Version       Version             `json:"version"`
	CompatibleAPI VersionRange        `json:"compatible_api"`
	Requires      []Dependency        `json:"requires,omitempty"`
	Optional      []Dependency        `json:"optional,omitempty"`
	Provides      []ProvidedExtension `json:"provides,omitempty"`
	Effects       []EffectKind        `json:"effects,omitempty"`
	DrainTimeout  time.Duration       `json:"drain_timeout"`
}

const (
	MaxModules                  = 1024
	MaxManifestDependencies     = 256
	MaxManifestProvides         = 1024
	MaxManifestEffects          = 256
	MaxSnapshotExtensions       = 4096
	MaxExtensionOrderingTargets = 64
	MaxPipelineGroup            = 2048
	MaxSnapshotOrderingEdges    = 8192
	DefaultDrainTimeout         = 5 * time.Minute
	MaxDrainTimeout             = 24 * time.Hour
)

func (manifest ModuleManifest) Clone() ModuleManifest {
	clone := manifest
	clone.CompatibleAPI = cloneVersionRange(manifest.CompatibleAPI)
	if manifest.Requires != nil {
		clone.Requires = make([]Dependency, len(manifest.Requires))
		for index, dependency := range manifest.Requires {
			clone.Requires[index] = dependency.Clone()
		}
	}
	if manifest.Optional != nil {
		clone.Optional = make([]Dependency, len(manifest.Optional))
		for index, dependency := range manifest.Optional {
			clone.Optional[index] = dependency.Clone()
		}
	}
	clone.Effects = append([]EffectKind(nil), manifest.Effects...)
	if manifest.Provides != nil {
		clone.Provides = make([]ProvidedExtension, len(manifest.Provides))
		for index, extension := range manifest.Provides {
			clone.Provides[index] = extension.Clone()
		}
	}
	return clone
}

func (manifest ModuleManifest) EffectiveDrainTimeout() time.Duration {
	if manifest.DrainTimeout == 0 {
		return DefaultDrainTimeout
	}
	return manifest.DrainTimeout
}

func (dependency Dependency) Clone() Dependency {
	clone := dependency
	clone.Version = cloneVersionRange(dependency.Version)
	return clone
}

func cloneVersionRange(versionRange VersionRange) VersionRange {
	clone := versionRange
	if versionRange.Max != nil {
		max := *versionRange.Max
		clone.Max = &max
	}
	return clone
}

func (extension ProvidedExtension) Clone() ProvidedExtension {
	clone := extension
	clone.Before = append([]ExtensionID(nil), extension.Before...)
	clone.After = append([]ExtensionID(nil), extension.After...)
	return clone
}

func validateID(value, label string) error {
	// IDs may contain '/', but this package treats them as opaque identifiers;
	// no runtime API uses them as filesystem or URL paths.
	if value == "" || len(value) > 128 || strings.TrimSpace(value) != value {
		return fmt.Errorf("%s id is empty, padded, or too long", label)
	}
	if strings.Contains(value, "..") {
		return fmt.Errorf("%s id contains a path traversal sequence", label)
	}
	for index, character := range value {
		if unicode.IsControl(character) || unicode.IsSpace(character) {
			return fmt.Errorf("%s id contains whitespace/control at %d", label, index)
		}
		if !(character >= 'a' && character <= 'z') && !(character >= 'A' && character <= 'Z') && !(character >= '0' && character <= '9') && !strings.ContainsRune("._/-", character) {
			return fmt.Errorf("%s id contains unsupported character", label)
		}
	}
	return nil
}

func validateResourceKey(value string) error {
	if value == "" {
		return nil
	}
	if err := validateID(value, "resource key"); err != nil {
		return err
	}
	return nil
}

func validateEffect(value EffectKind) error {
	if err := validateID(string(value), "effect"); err != nil {
		return err
	}
	return nil
}
