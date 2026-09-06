//go:build !linux && !windows

package sandbox

import (
	"context"
)

// LocalProvider is intentionally unavailable on platforms without the Linux
// bwrap+prlimit implementation. It never falls back to host execution.
type LocalProvider struct{}

func (LocalProvider) ID() ProviderID { return "local-ephemeral" }

func (LocalProvider) Probe(context.Context) AssuranceReport {
	return AssuranceReport{UnavailableCause: "sandbox provider unavailable on this platform"}
}

func (LocalProvider) Start(ctx context.Context, spec SessionSpec) (Session, error) {
	spec = cloneSessionSpec(spec)
	if ctx == nil {
		return nil, context.Canceled
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := spec.Validate(); err != nil {
		return nil, err
	}
	return nil, ErrUnavailable
}
