package storageconfig

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Resolver applies the DB-authoritative precedence rule. LegacyInput is only
// considered when the database row is absent; a present malformed/inactive row
// never falls back to it.
type Resolver struct {
	Repository Repository
}

func (resolver Resolver) Resolve(ctx context.Context, kind Kind, legacy *LegacyInput, active ActiveState) (Resolution, error) {
	return resolver.resolve(ctx, kind, func(context.Context, Kind) (*LegacyInput, error) {
		return legacy, nil
	}, active)
}

// ResolveWithLegacyProvider preserves the DB-first precedence rule while
// allowing callers to defer expensive or side-effecting legacy mapping until
// absence is confirmed.
func (resolver Resolver) ResolveWithLegacyProvider(ctx context.Context, kind Kind, provider LegacyProvider, active ActiveState) (Resolution, error) {
	if provider == nil {
		provider = func(context.Context, Kind) (*LegacyInput, error) { return nil, nil }
	}
	return resolver.resolve(ctx, kind, provider, active)
}

func (resolver Resolver) resolve(ctx context.Context, kind Kind, provider LegacyProvider, active ActiveState) (Resolution, error) {
	if !kind.Valid() {
		return Resolution{}, fmt.Errorf("%w: %q", ErrInvalidKind, kind)
	}
	if resolver.Repository == nil {
		return Resolution{}, fmt.Errorf("storage configuration repository is nil")
	}
	desired, found, loadErr := resolver.Repository.Load(ctx, kind)
	evidence := ResolutionEvidence{
		Kind: kind, Found: found, ObservedAt: time.Now().UTC(),
		ActiveType: active.Backend, ActiveRevision: active.Revision,
	}
	if found {
		evidence.Source = evidenceSource(desired.Source)
		if normalizeSource(desired.Source) != evidence.Source {
			return resolver.invalid(ctx, kind, desired, evidence, fmt.Errorf("%w: unknown source %q", ErrInvalidConfiguration, desired.Source), ErrorCodeInvalid)
		}
		if loadErr != nil {
			return resolver.invalid(ctx, kind, desired, evidence, loadErr, ErrorCodeInvalid)
		}
		return resolver.resolvePresent(ctx, kind, desired, evidence, active)
	}
	if loadErr != nil {
		return Resolution{}, loadErr
	}

	legacy, legacyErr := provider(ctx, kind)
	if legacyErr != nil {
		return Resolution{}, legacyErr
	}
	if legacy != nil {
		desired := legacy.Configuration
		desired.Source = SourceEnvImport
		desired.Status = StatusActive
		desired, err := PrepareCreate(kind, desired)
		if err != nil {
			evidence.Source = SourceEnvImport
			return resolver.invalid(ctx, kind, desired, evidence, err, ErrorCodeInvalid)
		}
		evidence.Source = SourceEnvImport
		evidence.DesiredType = desired.Backend
		evidence.DesiredRevision = desired.DesiredRevision
		created, createErr := resolver.Repository.CreateIfAbsent(ctx, kind, desired)
		if createErr != nil {
			evidence.Status = StatusInvalid
			evidence.ErrorCode = ErrorCodePersistFailed
			return resolver.finish(ctx, kind, Resolution{Desired: desired, Evidence: evidence}, createErr)
		}
		if !created {
			winner, winnerFound, winnerErr := resolver.Repository.Load(ctx, kind)
			if winnerErr != nil {
				return resolver.invalid(ctx, kind, winner, evidence, winnerErr, ErrorCodeInvalid)
			}
			if !winnerFound {
				return resolver.invalid(ctx, kind, desired, evidence, fmt.Errorf("%w: atomic create lost without a database winner", ErrInvalidConfiguration), ErrorCodePersistFailed)
			}
			winnerEvidence := ResolutionEvidence{Kind: kind, Found: true, ActiveType: active.Backend, ActiveRevision: active.Revision}
			winnerEvidence.Source = evidenceSource(winner.Source)
			return resolver.resolvePresent(ctx, kind, winner, winnerEvidence, active)
		}
		evidence.Found = true
		return resolver.resolvePresent(ctx, kind, desired, evidence, active)
	}

	desired = StoredConfiguration{Backend: BackendEmbedded, Source: SourceBootstrap, Status: StatusActive}
	var normalizeErr error
	desired, normalizeErr = PrepareCreate(kind, desired)
	if normalizeErr != nil {
		return Resolution{}, normalizeErr
	}
	evidence.Source = SourceBootstrap
	evidence.Status = StatusActive
	evidence.DesiredType = BackendEmbedded
	evidence.DesiredRevision = desired.DesiredRevision
	if active.Backend == "" {
		evidence.ActiveType = BackendEmbedded
		evidence.ActiveRevision = desired.DesiredRevision
	}
	return resolver.finish(ctx, kind, Resolution{Desired: desired, Evidence: evidence}, nil)
}

func (resolver Resolver) resolvePresent(ctx context.Context, kind Kind, desired StoredConfiguration, evidence ResolutionEvidence, active ActiveState) (Resolution, error) {
	normalized, err := Normalize(kind, desired)
	if err != nil {
		evidence.Source = evidenceSource(desired.Source)
		return resolver.invalid(ctx, kind, desired, evidence, err, ErrorCodeInvalid)
	}
	desired = normalized
	evidence.Source = desired.Source
	evidence.Status = StatusActive
	evidence.DesiredType = desired.Backend
	evidence.DesiredRevision = desired.DesiredRevision
	if desired.Status == StatusInactive {
		evidence.Status = StatusInactive
		evidence.ErrorCode = ErrorCodeInactive
		return resolver.finish(ctx, kind, Resolution{Desired: desired, Evidence: evidence}, fmt.Errorf("%w: %s", ErrInactiveConfiguration, kind))
	}
	if evidence.ActiveType == "" {
		evidence.ActiveType = desired.Backend
	}
	if evidence.ActiveRevision == "" {
		evidence.ActiveRevision = active.Revision
	}
	if kind == KindSessions && active.Backend != "" && active.Revision != desired.DesiredRevision {
		evidence.Status = StatusRestartPending
		evidence.RestartPending = true
	}
	return resolver.finish(ctx, kind, Resolution{Desired: desired, Evidence: evidence}, nil)
}

func (resolver Resolver) invalid(ctx context.Context, kind Kind, desired StoredConfiguration, evidence ResolutionEvidence, primary error, code ErrorCode) (Resolution, error) {
	evidence.Status = StatusInvalid
	evidence.ErrorCode = code
	return resolver.finish(ctx, kind, Resolution{Desired: desired, Evidence: evidence}, primary)
}

func (resolver Resolver) finish(ctx context.Context, kind Kind, resolution Resolution, primary error) (Resolution, error) {
	if err := resolution.Evidence.Validate(); err != nil {
		if primary == nil {
			primary = err
		} else {
			primary = errors.Join(primary, err)
		}
	}
	if err := resolver.Repository.SaveEvidence(ctx, kind, resolution.Evidence); err != nil {
		primary = errors.Join(primary, fmt.Errorf("%w: %w", ErrorCodeEvidenceSaveFailed, err))
	}
	return resolution, primary
}
