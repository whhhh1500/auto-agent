// Package modelprovider defines the provider-side registration metadata seam.
// It intentionally does not resolve models, credentials, routers, or wire
// protocols; those concerns belong to the M2 model-control layer.
package modelprovider

import (
	"fmt"
	"strings"

	"github.com/whhhh1500/auto-agent/pkg/runtime"
)

const providerExtensionPrefix = "model-provider/"

const Contract runtime.ContractID = "model.provider"

// Registration describes one provider metadata entry. Provider implementations
// and authentication material are deliberately not part of this type.
type Registration struct {
	ID       string
	Version  runtime.Version
	Priority int
}

// Extension validates the registration and returns its runtime collection
// extension. Protocol selection and model routing are intentionally deferred
// to the M2 model-control layer.
func (registration Registration) Extension() (runtime.ProvidedExtension, error) {
	extension := runtime.ProvidedExtension{
		ID:          runtime.ExtensionID(providerExtensionPrefix + registration.ID),
		Semantic:    runtime.SemanticCollection,
		Contract:    Contract,
		Version:     registration.Version,
		Priority:    registration.Priority,
		ResourceKey: registration.ID,
	}
	if err := registration.validate(extension); err != nil {
		return runtime.ProvidedExtension{}, err
	}
	return extension, nil
}

func (registration Registration) validate(extension runtime.ProvidedExtension) error {
	if strings.TrimSpace(registration.ID) == "" {
		return fmt.Errorf("model provider registration id is empty")
	}
	if err := runtime.ValidateExtension(extension); err != nil {
		return fmt.Errorf("model provider registration %q: %w", registration.ID, err)
	}
	return nil
}
