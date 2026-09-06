// Package runtime contains dependency-free V2 module metadata contracts.
// Lifecycle effects, registries, and dependency construction stay in
// composition code so this package remains safe to reuse by adapters.
package runtime

import "errors"

var (
	ErrInvalidManifest     = errors.New("invalid module manifest")
	ErrInvalidExtension    = errors.New("invalid extension")
	ErrDependency          = errors.New("invalid module dependency")
	ErrSemanticConflict    = errors.New("extension semantic conflict")
	ErrDependencyCycle     = errors.New("module dependency cycle")
	ErrInvalidHost         = errors.New("invalid module host")
	ErrStageFailed         = errors.New("module stage failed")
	ErrActivationFailed    = errors.New("module activation failed")
	ErrInvalidEffect       = errors.New("invalid effect")
	ErrEffectConflict      = errors.New("effect conflict")
	ErrEffectNotFound      = errors.New("effect not found")
	ErrLeaseMismatch       = errors.New("lease identity mismatch")
	ErrLeaseNotFound       = errors.New("lease not found")
	ErrLeasesRemaining     = errors.New("leases remain during drain")
	ErrModuleNotFound      = errors.New("module not found")
	ErrFenceUnauthorized   = errors.New("fence unauthorized")
	ErrInvalidFenceCommand = errors.New("invalid fence command")
	ErrFenceConflict       = errors.New("fence request conflict")
	ErrFenceUnknown        = errors.New("fence request outcome unknown")
	ErrFenceJournalFull    = errors.New("fence journal capacity exhausted")
	// ErrRecoveryBlocked means the durable document is malformed or cannot be
	// safely bound to the modules supplied by the process.
	ErrRecoveryBlocked = errors.New("durable recovery blocked")
	// ErrRecoveryRequired is returned deliberately when a previous process left
	// a non-empty composition checkpoint. This slice does not guess how to
	// reconstruct external owners or replay activation effects.
	ErrRecoveryRequired = errors.New("durable recovery required")
)
