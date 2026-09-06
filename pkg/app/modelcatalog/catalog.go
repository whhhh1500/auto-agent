// Package modelcatalog exposes the credential-free, immutable model runtime
// catalog view used by transport and settings layers. Implementations live in
// adapter packages and may retain private factories.
package modelcatalog

import "errors"

var ErrUnavailable = errors.New("model catalog unavailable")

type Ref struct {
	ID      string `json:"id"`
	Version string `json:"version"`
}
type Provider struct {
	Ref                    Ref    `json:"ref"`
	ImplementationRevision string `json:"implementation_revision"`
}
type Protocol struct {
	Ref                    Ref    `json:"ref"`
	ImplementationRevision string `json:"implementation_revision"`
}
type Compatibility struct {
	Provider Ref `json:"provider"`
	Protocol Ref `json:"protocol"`
}
type Default struct {
	Provider Ref `json:"provider"`
	Protocol Ref `json:"protocol"`
}

type View interface {
	SnapshotRevision() string
	Providers() []Provider
	Protocols() []Protocol
	Compatibilities() []Compatibility
	Defaults() []Default
	Compatible(provider, protocol Ref) (knownProvider, knownProtocol, compatible bool)
	Assess(providerID, protocolID string) (knownProvider, knownProtocol, compatible bool)
}
