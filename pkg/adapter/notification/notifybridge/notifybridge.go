// Package notifybridge adapts one configured nikoksr/notify service to the
// provider-neutral notification channel contract.
//
// It deliberately contains no provider registration, credentials, target
// discovery, global notifier, or retry policy. A caller supplies a factory for
// each configured target; the factory selects the concrete notify service and
// its one receiver from private configuration.
package notifybridge

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/nikoksr/notify"
	"github.com/whhhh1500/auto-agent/pkg/app/notification"
)

const Subject = "auto-agent"

var (
	ErrInvalidChannel   = errors.New("invalid notify bridge channel")
	ErrTargetResolution = errors.New("notify bridge target resolution failed")
	ErrFactory          = errors.New("notify bridge factory failed")
	ErrDelivery         = errors.New("notify bridge delivery failed")
	ErrPanic            = errors.New("notify bridge callback panicked")
)

// Resolver returns the private configuration for one tenant-scoped target and
// exact channel ref. Its returned bytes transfer to Channel, which clears them
// before returning.
type Resolver interface {
	ResolveConfig(context.Context, string, notification.TargetRef, notification.ChannelRef) ([]byte, error)
}

// Factory parses one private target configuration and returns a notifier for
// exactly one receiving target. The supplied configuration is owned by the
// bridge only for the duration of this call and is cleared immediately after
// Factory returns. Factory must copy any material it needs before returning,
// must not retain the supplied slice, and must configure a service whose Send
// observes cancellation when that service supports it. It must return a
// non-nil notifier with an actual receiving service: some notify broadcasters
// can report success while disabled or empty, which this generic bridge cannot
// detect. The bridge cannot force cancellation on a service that ignores its
// context.
type Factory func(context.Context, []byte) (notify.Notifier, error)

// Channel sends one Delivery through a fresh, target-scoped notifier. It has
// no global services or platform registry.
type Channel struct {
	ref      notification.ChannelRef
	resolver Resolver
	factory  Factory
}

var _ notification.Channel = (*Channel)(nil)

// New constructs a bridge for one exact application channel ref. The ref is
// intentionally caller-selected, so integrations can add provider-specific
// adapters without changing this package.
func New(ref notification.ChannelRef, resolver Resolver, factory Factory) (*Channel, error) {
	if !validRef(ref) || isNil(resolver) || factory == nil {
		return nil, ErrInvalidChannel
	}
	return &Channel{ref: ref, resolver: resolver, factory: factory}, nil
}

func (c *Channel) Descriptor() notification.Descriptor {
	if c == nil {
		return notification.Descriptor{}
	}
	return notification.Descriptor{
		Ref:          c.ref,
		Capabilities: []notification.Capability{notification.CapabilityDeliver},
	}
}

// Deliver resolves one target configuration, constructs one notifier, and
// calls Send once. A successful Send only means the service accepted the
// attempt, so the receipt is ReceiptAccepted rather than ReceiptDelivered.
// Provider errors, panic values, and configuration details are never exposed.
func (c *Channel) Deliver(ctx context.Context, delivery notification.Delivery) (receipt notification.Receipt, err error) {
	if ctx == nil {
		return notification.Receipt{}, context.Canceled
	}
	if err := ctx.Err(); err != nil {
		return notification.Receipt{}, err
	}
	if err := notification.ValidateDelivery(delivery); err != nil {
		return notification.Receipt{}, err
	}
	if c == nil || !validRef(c.ref) || isNil(c.resolver) || c.factory == nil {
		return notification.Receipt{}, ErrInvalidChannel
	}

	config, err := safeResolve(c.resolver, ctx, delivery.TenantID, delivery.Target, c.ref)
	defer clearBytes(config)
	if err != nil {
		return notification.Receipt{}, classify(err, ErrTargetResolution)
	}
	if len(config) > notification.MaxTargetConfigurationBytes {
		return notification.Receipt{}, ErrTargetResolution
	}
	if err := ctx.Err(); err != nil {
		return notification.Receipt{}, err
	}

	owned := append([]byte(nil), config...)
	notifier, factoryErr := safeFactory(c.factory, ctx, owned)
	clearBytes(owned)
	if factoryErr != nil {
		return notification.Receipt{}, classify(factoryErr, ErrFactory)
	}
	if isNil(notifier) {
		return notification.Receipt{}, ErrFactory
	}
	if err := ctx.Err(); err != nil {
		return notification.Receipt{}, err
	}

	if err := safeSend(notifier, ctx, Subject, delivery.Text); err != nil {
		return notification.Receipt{}, classify(err, ErrDelivery)
	}
	if err := ctx.Err(); err != nil {
		return notification.Receipt{}, err
	}
	return notification.Receipt{
		Channel:    c.ref,
		DeliveryID: delivery.IdempotencyKey,
		Status:     notification.ReceiptAccepted,
	}, nil
}

func safeResolve(resolver Resolver, ctx context.Context, tenant string, target notification.TargetRef, ref notification.ChannelRef) (config []byte, err error) {
	defer func() {
		if recover() != nil {
			clearBytes(config)
			config, err = nil, ErrPanic
		}
	}()
	return resolver.ResolveConfig(ctx, tenant, target, ref)
}

func safeFactory(factory Factory, ctx context.Context, config []byte) (notifier notify.Notifier, err error) {
	defer func() {
		if recover() != nil {
			notifier, err = nil, ErrPanic
		}
	}()
	return factory(ctx, config)
}

func safeSend(notifier notify.Notifier, ctx context.Context, subject, text string) (err error) {
	defer func() {
		if recover() != nil {
			err = ErrPanic
		}
	}()
	return notifier.Send(ctx, subject, text)
}

func classify(err, fallback error) error {
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	if errors.Is(err, ErrPanic) {
		return ErrPanic
	}
	return fallback
}

func validRef(ref notification.ChannelRef) bool {
	if ref.ID == "" || ref.Version == "" || len(ref.ID) > notification.MaxChannelIDBytes || len(ref.Version) > notification.MaxChannelVersionBytes {
		return false
	}
	for _, value := range []string{ref.ID, ref.Version} {
		if !utf8.ValidString(value) || value != strings.TrimSpace(value) {
			return false
		}
		for _, runeValue := range value {
			if unicode.IsControl(runeValue) || unicode.In(runeValue, unicode.Cf, unicode.Cs, unicode.Co) {
				return false
			}
		}
	}
	return true
}

func isNil(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

func clearBytes(value []byte) {
	for index := range value {
		value[index] = 0
	}
}
