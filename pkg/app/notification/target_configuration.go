package notification

import (
	"context"
	"errors"
	"math"
	"strconv"
)

const MaxTargetConfigurationBytes = 64 << 10

var (
	ErrInvalidTargetConfiguration               = errors.New("invalid notification target configuration")
	ErrTargetNotFound                           = errors.New("notification target not found")
	ErrTargetDisabled                           = errors.New("notification target disabled")
	ErrTargetRevisionConflict                   = errors.New("notification target revision conflict")
	ErrTargetRepository                         = errors.New("notification target repository failed")
	ErrTargetConfigurationFailure               = errors.New("notification target configuration unavailable")
	ErrInvalidTargetRevision                    = errors.New("invalid notification target revision")
	ErrTargetChannelChangeRequiresConfiguration = errors.New("notification target channel change requires configuration")
)

// TargetConfiguration is intentionally opaque to the application contract.
// A channel adapter owns its encoding and must treat Payload as caller-owned.
// It may contain provider-specific JSON or another bounded representation, but
// it must never be placed in a TargetDescriptor or capability response.
type TargetConfiguration struct {
	Payload []byte
}

// ConfigurationValidator is an optional provider-owned validation seam. The
// application never interprets Payload; a validator is selected by exact
// ChannelRef and must reject malformed provider configuration without
// returning provider secrets in its error.
type ConfigurationValidator interface {
	Channel() ChannelRef
	ValidateConfiguration([]byte) error
}

// Clone returns an independent configuration buffer.
func (configuration TargetConfiguration) Clone() TargetConfiguration {
	configuration.Payload = append([]byte(nil), configuration.Payload...)
	return configuration
}

// TargetRecord is a non-secret management view. Revision and Enabled are
// available for lifecycle/CAS operations, while configuration remains absent.
type TargetRecord struct {
	Descriptor TargetDescriptor
	Enabled    bool
	Revision   string
}

func (record TargetRecord) Clone() TargetRecord {
	record.Descriptor = record.Descriptor.Clone()
	return record
}

// Repository is the persistence seam for tenant-scoped notification targets.
// ResolveConfig is an internal provider boundary: callers must not expose its
// result over a public query or capability. Implementations must return fixed
// application errors and caller-owned configuration bytes.
type Repository interface {
	List(context.Context, string) ([]TargetDescriptor, error)
	ListRecords(context.Context, string) ([]TargetRecord, error)
	Create(context.Context, string, TargetDescriptor, TargetConfiguration, bool) (string, error)
	Update(context.Context, string, TargetDescriptor, TargetConfiguration, bool, string) (string, error)
	Delete(context.Context, string, TargetRef, string) error
	ResolveConfig(context.Context, string, TargetRef, ChannelRef) (TargetConfiguration, error)
}

// Service provides the application-facing target lifecycle. Queries return
// descriptors only; configuration is available solely through ResolveConfig
// for a concrete provider adapter.
type Service struct {
	repository Repository
	channels   []ChannelRef
	validators map[string]ConfigurationValidator
}

var _ interface {
	List(context.Context, string) ([]TargetDescriptor, error)
	ResolveConfig(context.Context, string, TargetRef, ChannelRef) ([]byte, error)
} = (*Service)(nil)

func NewService(repository Repository, channels []ChannelRef, validators ...ConfigurationValidator) (*Service, error) {
	if repository == nil || len(channels) > MaxChannels {
		return nil, ErrInvalidTargetDescriptor
	}
	known := make(map[string]struct{}, len(channels))
	copyChannels := append([]ChannelRef(nil), channels...)
	for _, channel := range copyChannels {
		if validateRef(channel) != nil {
			return nil, ErrInvalidTargetDescriptor
		}
		key := refKey(channel)
		if _, exists := known[key]; exists {
			return nil, ErrInvalidTargetDescriptor
		}
		known[key] = struct{}{}
	}
	configuredValidators := make(map[string]ConfigurationValidator, len(validators))
	for _, validator := range validators {
		if validator == nil || validateRef(validator.Channel()) != nil {
			return nil, ErrInvalidTargetDescriptor
		}
		key := refKey(validator.Channel())
		if _, exists := known[key]; !exists {
			return nil, ErrUnknownTargetChannel
		}
		if _, exists := configuredValidators[key]; exists {
			return nil, ErrInvalidTargetDescriptor
		}
		configuredValidators[key] = validator
	}
	return &Service{repository: repository, channels: copyChannels, validators: configuredValidators}, nil
}

func (service *Service) List(ctx context.Context, tenantID string) ([]TargetDescriptor, error) {
	records, err := service.ListRecords(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	result := make([]TargetDescriptor, 0, len(records))
	for _, record := range records {
		if record.Enabled {
			result = append(result, record.Descriptor.Clone())
		}
	}
	return result, nil
}

// ListRecords returns bounded non-secret management records, including
// disabled targets and the opaque revision needed for a later CAS.
func (service *Service) ListRecords(ctx context.Context, tenantID string) ([]TargetRecord, error) {
	if err := validateTargetContext(ctx, tenantID); err != nil {
		return nil, err
	}
	records, err := safeRepositoryListRecords(service.repository, ctx, tenantID)
	if err != nil {
		return nil, sanitizeTargetError(err)
	}
	if len(records) > MaxTargets {
		return nil, ErrTargetDirectoryCapacity
	}
	result := make([]TargetRecord, len(records))
	seen := make(map[string]struct{}, len(records))
	for index, record := range records {
		if err := record.Descriptor.Validate(service.channels); err != nil {
			return nil, ErrTargetRepository
		}
		if err := validateRevision(record.Revision); err != nil {
			return nil, ErrTargetRepository
		}
		key := record.Descriptor.Target.String()
		if _, exists := seen[key]; exists {
			return nil, ErrTargetRepository
		}
		seen[key] = struct{}{}
		result[index] = record.Clone()
	}
	return result, nil
}

func (service *Service) Create(ctx context.Context, tenantID string, target TargetDescriptor, configuration TargetConfiguration, enabled bool) (string, error) {
	if err := service.validateCommand(ctx, tenantID, target, configuration); err != nil {
		return "", err
	}
	owned := configuration.Clone()
	defer clearBytes(owned.Payload)
	revision, err := safeRepositoryCreate(service.repository, ctx, tenantID, target.Clone(), owned, enabled)
	if err != nil {
		return "", sanitizeTargetError(err)
	}
	if err := validateRevision(revision); err != nil {
		return "", ErrTargetRepository
	}
	return revision, nil
}

func (service *Service) Update(ctx context.Context, tenantID string, target TargetDescriptor, configuration TargetConfiguration, enabled bool, expectedRevision string) (string, error) {
	if err := service.validateCommand(ctx, tenantID, target, configuration); err != nil {
		return "", err
	}
	if err := validateRevision(expectedRevision); err != nil {
		return "", err
	}
	owned := configuration.Clone()
	defer clearBytes(owned.Payload)
	revision, err := safeRepositoryUpdate(service.repository, ctx, tenantID, target.Clone(), owned, enabled, expectedRevision)
	if err != nil {
		return "", sanitizeTargetError(err)
	}
	if err := validateRevision(revision); err != nil {
		return "", ErrTargetRepository
	}
	return revision, nil
}

func (service *Service) Delete(ctx context.Context, tenantID string, target TargetRef, expectedRevision string) error {
	if err := validateTargetContext(ctx, tenantID); err != nil {
		return err
	}
	if !validText(target.String(), MaxTargetRefBytes) || target.String() == "" {
		return ErrInvalidTargetDescriptor
	}
	if err := validateRevision(expectedRevision); err != nil {
		return err
	}
	if err := safeRepositoryDelete(service.repository, ctx, tenantID, target, expectedRevision); err != nil {
		return sanitizeTargetError(err)
	}
	return nil
}

// ResolveConfig is intentionally not part of the discovery/capability wire
// contract. The returned slice belongs to the caller and is never retained.
func (service *Service) ResolveConfig(ctx context.Context, tenantID string, target TargetRef, channel ChannelRef) ([]byte, error) {
	if err := validateTargetContext(ctx, tenantID); err != nil {
		return nil, err
	}
	if !validText(target.String(), MaxTargetRefBytes) || target.String() == "" || validateRef(channel) != nil {
		return nil, ErrInvalidTargetDescriptor
	}
	if !service.knownChannel(channel) {
		return nil, ErrUnknownTargetChannel
	}
	configuration, err := safeRepositoryResolveConfig(service.repository, ctx, tenantID, target, channel)
	if err != nil {
		clearBytes(configuration.Payload)
		return nil, sanitizeTargetError(err)
	}
	if len(configuration.Payload) > MaxTargetConfigurationBytes {
		clearBytes(configuration.Payload)
		return nil, ErrTargetConfigurationFailure
	}
	result := append([]byte(nil), configuration.Payload...)
	clearBytes(configuration.Payload)
	return result, nil
}

func (service *Service) validateCommand(ctx context.Context, tenantID string, target TargetDescriptor, configuration TargetConfiguration) error {
	if err := validateTargetContext(ctx, tenantID); err != nil {
		return err
	}
	if err := target.Validate(service.channels); err != nil {
		return err
	}
	if len(configuration.Payload) > MaxTargetConfigurationBytes {
		return ErrInvalidTargetConfiguration
	}
	if configuration.Payload != nil {
		validator, exists := service.validators[refKey(target.Channel)]
		if len(service.validators) > 0 && !exists {
			return ErrInvalidTargetConfiguration
		}
		if exists {
			candidate := append([]byte(nil), configuration.Payload...)
			defer clearBytes(candidate)
			if err := safeValidateConfiguration(validator, candidate); err != nil {
				return ErrInvalidTargetConfiguration
			}
		}
	}
	return nil
}

func (service *Service) knownChannel(channel ChannelRef) bool {
	for _, known := range service.channels {
		if known == channel {
			return true
		}
	}
	return false
}

func validateTargetContext(ctx context.Context, tenantID string) error {
	if ctx == nil {
		return context.Canceled
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return ValidateTenantID(tenantID)
}

func validateRevision(revision string) error {
	if !validText(revision, 32) {
		return ErrInvalidTargetRevision
	}
	parsed, err := strconv.ParseUint(revision, 10, 64)
	if err != nil || parsed == 0 || parsed > math.MaxInt64 {
		return ErrInvalidTargetRevision
	}
	return nil
}

func sanitizeTargetError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	for _, known := range []error{
		ErrTargetNotFound, ErrTargetDisabled, ErrTargetRevisionConflict,
		ErrTargetConfigurationFailure, ErrInvalidTargetConfiguration,
		ErrInvalidTargetDescriptor, ErrUnknownTargetChannel,
		ErrTargetDirectoryCapacity, ErrInvalidTargetRevision,
		ErrTargetChannelChangeRequiresConfiguration,
	} {
		if errors.Is(err, known) {
			return known
		}
	}
	return ErrTargetRepository
}

func safeRepositoryListRecords(repository Repository, ctx context.Context, tenantID string) (records []TargetRecord, err error) {
	defer func() {
		if recover() != nil {
			records, err = nil, ErrTargetRepository
		}
	}()
	return repository.ListRecords(ctx, tenantID)
}

func safeRepositoryCreate(repository Repository, ctx context.Context, tenantID string, target TargetDescriptor, configuration TargetConfiguration, enabled bool) (revision string, err error) {
	defer func() {
		if recover() != nil {
			revision, err = "", ErrTargetRepository
		}
	}()
	return repository.Create(ctx, tenantID, target, configuration, enabled)
}

func safeRepositoryUpdate(repository Repository, ctx context.Context, tenantID string, target TargetDescriptor, configuration TargetConfiguration, enabled bool, expectedRevision string) (revision string, err error) {
	defer func() {
		if recover() != nil {
			revision, err = "", ErrTargetRepository
		}
	}()
	return repository.Update(ctx, tenantID, target, configuration, enabled, expectedRevision)
}

func safeRepositoryDelete(repository Repository, ctx context.Context, tenantID string, target TargetRef, expectedRevision string) (err error) {
	defer func() {
		if recover() != nil {
			err = ErrTargetRepository
		}
	}()
	return repository.Delete(ctx, tenantID, target, expectedRevision)
}

func safeRepositoryResolveConfig(repository Repository, ctx context.Context, tenantID string, target TargetRef, channel ChannelRef) (configuration TargetConfiguration, err error) {
	defer func() {
		if recover() != nil {
			clearBytes(configuration.Payload)
			configuration, err = TargetConfiguration{}, ErrTargetRepository
		}
	}()
	return repository.ResolveConfig(ctx, tenantID, target, channel)
}

func safeValidateConfiguration(validator ConfigurationValidator, payload []byte) (err error) {
	defer func() {
		if recover() != nil {
			err = ErrInvalidTargetConfiguration
		}
	}()
	err = validator.ValidateConfiguration(payload)
	if err != nil {
		return ErrInvalidTargetConfiguration
	}
	return nil
}

func clearBytes(value []byte) {
	for index := range value {
		value[index] = 0
	}
}
