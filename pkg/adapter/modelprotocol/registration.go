// Package modelprotocol defines the protocol-side registration metadata seam.
// It does not implement a transport or bind a provider; those concerns belong
// to trusted adapters and the M2 model-control layer.
package modelprotocol

import (
	"fmt"
	"strings"

	"github.com/whhhh1500/auto-agent/pkg/runtime"
)

const protocolExtensionPrefix = "model-protocol/"

const Contract runtime.ContractID = "model.protocol"

// Registration describes one protocol metadata entry. A protocol can be
// shared by many providers, and a provider can advertise many protocols; this
// seam intentionally records neither relationship.
type Registration struct {
	ID       string
	Version  runtime.Version
	Priority int
}

// Extension validates the registration and returns its runtime collection
// extension. Provider/protocol compatibility is resolved by M2 model control.
func (registration Registration) Extension() (runtime.ProvidedExtension, error) {
	extension := runtime.ProvidedExtension{
		ID:          runtime.ExtensionID(protocolExtensionPrefix + registration.ID),
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
		return fmt.Errorf("model protocol registration id is empty")
	}
	if err := runtime.ValidateExtension(extension); err != nil {
		return fmt.Errorf("model protocol registration %q: %w", registration.ID, err)
	}
	return nil
}
