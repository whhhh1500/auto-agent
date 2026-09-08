// Package runtime assembles host-selected notification providers. It performs
// no provider discovery: registrations are supplied by the embedding process
// during startup and are frozen into one exact-version channel registry.
package runtime

import (
	"errors"
	"fmt"
	"reflect"
	"sort"

	"github.com/whhhh1500/auto-agent/pkg/app/notification"
)

var ErrInvalidRegistration = errors.New("invalid notification provider registration")

// ChannelFactory creates a channel after the common target Service exists.
// This breaks the natural construction cycle: the Service needs every channel
// reference and validator, while a provider channel commonly needs the Service
// to resolve its opaque target configuration.
type ChannelFactory func(*notification.Service) (notification.Channel, error)

// Registration is one host-owned provider declaration. Ref is the exact
// immutable provider version accepted by SQL and target management. Validator
// is optional and validates only this Ref's opaque target configuration.
// Build creates the corresponding transport after the shared Service exists.
// Neither this package nor the HTTP API discovers or loads providers at
// runtime; administrators only configure targets for registrations already
// supplied by the host process.
type Registration struct {
	Ref       notification.ChannelRef
	Validator notification.ConfigurationValidator
	Build     ChannelFactory
}

// Assembly is an immutable, validated startup plan. Its accessors return
// copied slices so callers cannot modify the configured provider set.
type Assembly struct {
	registrations []Registration
}

// New validates an explicit registration snapshot. It intentionally has no
// global registration or reflection-based provider discovery.
func New(registrations []Registration) (*Assembly, error) {
	if len(registrations) == 0 || len(registrations) > notification.MaxChannels {
		return nil, ErrInvalidRegistration
	}
	copyRegistrations := append([]Registration(nil), registrations...)
	seen := make(map[string]struct{}, len(copyRegistrations))
	for _, registration := range copyRegistrations {
		if registration.Build == nil || !validRef(registration.Ref) {
			return nil, ErrInvalidRegistration
		}
		key := refKey(registration.Ref)
		if _, exists := seen[key]; exists {
			return nil, ErrInvalidRegistration
		}
		seen[key] = struct{}{}
		if registration.Validator != nil {
			validatorRef, ok := safeValidatorRef(registration.Validator)
			if !ok || validatorRef != registration.Ref {
				return nil, ErrInvalidRegistration
			}
		}
	}
	sort.Slice(copyRegistrations, func(i, j int) bool {
		return refKey(copyRegistrations[i].Ref) < refKey(copyRegistrations[j].Ref)
	})
	return &Assembly{registrations: copyRegistrations}, nil
}

// Refs returns the exact channel references to pass to the target repository
// and Service before any provider transport is constructed.
func (assembly *Assembly) Refs() []notification.ChannelRef {
	if assembly == nil {
		return nil
	}
	refs := make([]notification.ChannelRef, len(assembly.registrations))
	for index, registration := range assembly.registrations {
		refs[index] = registration.Ref
	}
	return refs
}

// Validators returns the configured provider validators. A nil Validator
// means the provider accepts opaque, bounded configuration without additional
// provider-specific syntax checks.
func (assembly *Assembly) Validators() []notification.ConfigurationValidator {
	if assembly == nil {
		return nil
	}
	validators := make([]notification.ConfigurationValidator, 0, len(assembly.registrations))
	for _, registration := range assembly.registrations {
		if registration.Validator != nil {
			validators = append(validators, registration.Validator)
		}
	}
	return validators
}

// BuildRegistry constructs every configured transport and returns the core
// application's immutable exact-version Registry. The caller owns the Service
// lifetime and passes the same instance used for target management.
func (assembly *Assembly) BuildRegistry(targets *notification.Service) (*notification.Registry, error) {
	if assembly == nil || targets == nil {
		return nil, ErrInvalidRegistration
	}
	if !sameRefs(assembly.Refs(), targets.Channels()) {
		return nil, ErrInvalidRegistration
	}
	channels := make([]notification.Channel, 0, len(assembly.registrations))
	for _, registration := range assembly.registrations {
		channel, err := safeBuild(registration.Build, targets)
		if err != nil || channel == nil {
			return nil, fmt.Errorf("%w: %s@%s", ErrInvalidRegistration, registration.Ref.ID, registration.Ref.Version)
		}
		descriptor, ok := safeDescriptor(channel)
		if !ok || descriptor.Ref != registration.Ref {
			return nil, fmt.Errorf("%w: channel does not match %s@%s", ErrInvalidRegistration, registration.Ref.ID, registration.Ref.Version)
		}
		channels = append(channels, channel)
	}
	registry, err := notification.NewRegistry(channels)
	if err != nil {
		return nil, err
	}
	if len(registry.Descriptors()) != len(assembly.registrations) {
		return nil, ErrInvalidRegistration
	}
	for _, registration := range assembly.registrations {
		if _, err := registry.Resolve(registration.Ref); err != nil {
			return nil, ErrInvalidRegistration
		}
	}
	return registry, nil
}

func safeBuild(build ChannelFactory, targets *notification.Service) (channel notification.Channel, err error) {
	defer func() {
		if recover() != nil {
			channel, err = nil, ErrInvalidRegistration
		}
	}()
	return build(targets)
}

func safeValidatorRef(validator notification.ConfigurationValidator) (ref notification.ChannelRef, ok bool) {
	if validator == nil || isNilInterface(validator) {
		return notification.ChannelRef{}, false
	}
	defer func() {
		if recover() != nil {
			ref, ok = notification.ChannelRef{}, false
		}
	}()
	return validator.Channel(), true
}

func safeDescriptor(channel notification.Channel) (descriptor notification.Descriptor, ok bool) {
	defer func() {
		if recover() != nil {
			descriptor, ok = notification.Descriptor{}, false
		}
	}()
	return channel.Descriptor(), true
}

func isNilInterface(value any) bool {
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}

func sameRefs(left, right []notification.ChannelRef) bool {
	if len(left) != len(right) {
		return false
	}
	seen := make(map[string]struct{}, len(left))
	for _, ref := range left {
		seen[refKey(ref)] = struct{}{}
	}
	for _, ref := range right {
		if _, exists := seen[refKey(ref)]; !exists {
			return false
		}
		delete(seen, refKey(ref))
	}
	return len(seen) == 0
}

func validRef(ref notification.ChannelRef) bool {
	probe, err := notification.NewTargetRef("_")
	if err != nil {
		return false
	}
	return (notification.TargetDescriptor{Target: probe, Channel: ref}).Validate([]notification.ChannelRef{ref}) == nil
}

func refKey(ref notification.ChannelRef) string { return ref.ID + "\x00" + ref.Version }
