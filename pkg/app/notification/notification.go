// Package notification defines a small provider-neutral delivery boundary.
// It contains no transport, credential, persistence, or background-worker
// behavior; adapters provide those concerns behind Channel.
package notification

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	MaxChannels               = 64
	MaxTargets                = 256
	MaxChannelIDBytes         = 128
	MaxChannelVersionBytes    = 64
	MaxChannelCapabilityBytes = 64
	MaxTargetRefBytes         = 512
	MaxTenantIDBytes          = 256
	MaxRunIDBytes             = 128
	MaxCallIDBytes            = 128
	MaxIdempotencyKeyBytes    = 256
	MaxTextBytes              = 64 << 10
	MaxFormatBytes            = 64
	MaxMetadataEntries        = 32
	MaxMetadataKeyBytes       = 64
	MaxMetadataValueBytes     = 512
	MaxReceiptIDBytes         = 256
	MaxTargetLabelBytes       = 128
	MaxTargetFormats          = 16
	MaxTargetFormatBytes      = 64
)

var (
	ErrInvalidDescriptor       = errors.New("invalid notification channel descriptor")
	ErrInvalidDelivery         = errors.New("invalid notification delivery")
	ErrInvalidReceipt          = errors.New("invalid notification receipt")
	ErrChannelNotFound         = errors.New("notification channel not found")
	ErrChannelPanic            = errors.New("notification channel panicked")
	ErrChannelDelivery         = errors.New("notification channel delivery failed")
	ErrRegistryCapacity        = errors.New("notification channel registry capacity exceeded")
	ErrInvalidTargetDescriptor = errors.New("invalid notification target descriptor")
	ErrInvalidTenantID         = errors.New("invalid notification tenant id")
	ErrUnknownTargetChannel    = errors.New("notification target channel not found")
	ErrTargetDirectoryFailure  = errors.New("notification target directory failed")
	ErrTargetDirectoryPanic    = errors.New("notification target directory panicked")
	ErrTargetDirectoryCapacity = errors.New("notification target directory capacity exceeded")
)

// ValidateTenantID applies the notification boundary's bounded tenant
// identity rules. Directory implementations should reject invalid tenants
// before consulting a backing store.
func ValidateTenantID(tenantID string) error {
	if !validText(tenantID, MaxTenantIDBytes) {
		return ErrInvalidTenantID
	}
	return nil
}

// Capability is a non-secret channel capability advertised to callers.
type Capability string

const CapabilityDeliver Capability = "deliver"

// ChannelRef selects one exact immutable channel implementation.
type ChannelRef struct {
	ID      string `json:"id"`
	Version string `json:"version"`
}

// Descriptor is metadata only. It contains no endpoint, credential, or
// provider object and is safe to expose in a capability snapshot.
type Descriptor struct {
	Ref          ChannelRef   `json:"ref"`
	Capabilities []Capability `json:"capabilities"`
}

// TargetRef is opaque to the notification contract. It is deliberately not a
// URL or credential carrier; a concrete Channel interprets its stable value.
type TargetRef struct{ value string }

func NewTargetRef(value string) (TargetRef, error) {
	if !validText(value, MaxTargetRefBytes) || strings.Contains(value, "://") {
		return TargetRef{}, fmt.Errorf("%w: target ref", ErrInvalidDelivery)
	}
	return TargetRef{value: value}, nil
}

func (r TargetRef) String() string { return r.value }

// Delivery is the bounded, non-secret request passed to a Channel.
type Delivery struct {
	TenantID       string            `json:"tenant_id"`
	SessionID      string            `json:"session_id"`
	RunID          string            `json:"run_id"`
	CallID         string            `json:"call_id"`
	IdempotencyKey string            `json:"idempotency_key"`
	Target         TargetRef         `json:"target"`
	Text           string            `json:"text"`
	Format         string            `json:"format"`
	Metadata       map[string]string `json:"metadata,omitempty"`
}

// ReceiptStatus describes a successful provider acknowledgement.
type ReceiptStatus string

const (
	ReceiptAccepted  ReceiptStatus = "accepted"
	ReceiptDelivered ReceiptStatus = "delivered"
)

// Receipt contains bounded provider metadata only. A Channel must not place
// response bodies, URLs, tokens, or credentials in it.
type Receipt struct {
	Channel    ChannelRef        `json:"channel"`
	DeliveryID string            `json:"delivery_id"`
	Status     ReceiptStatus     `json:"status"`
	Metadata   map[string]string `json:"metadata,omitempty"`
}

// Channel is the only transport extension point. Implementations may support
// idempotency for Delivery.IdempotencyKey, but Registry does not assert that
// guarantee; it isolates panics and sanitizes provider errors.
type Channel interface {
	Descriptor() Descriptor
	Deliver(context.Context, Delivery) (Receipt, error)
}

type Registry struct {
	channels map[string]Channel
	list     []Descriptor
}

// NewRegistry builds an immutable exact-version registry. There is no global
// registration and no reflection-based discovery.
func NewRegistry(channels []Channel) (*Registry, error) {
	if len(channels) > MaxChannels {
		return nil, ErrRegistryCapacity
	}
	byRef := make(map[string]Channel, len(channels))
	descriptors := make([]Descriptor, 0, len(channels))
	for _, channel := range channels {
		if channel == nil {
			return nil, fmt.Errorf("%w: nil channel", ErrInvalidDescriptor)
		}
		descriptor, err := safeDescriptor(channel)
		if err != nil {
			return nil, err
		}
		descriptor, err = validateDescriptor(descriptor)
		if err != nil {
			return nil, err
		}
		key := refKey(descriptor.Ref)
		if _, exists := byRef[key]; exists {
			return nil, fmt.Errorf("%w: duplicate channel ref", ErrInvalidDescriptor)
		}
		byRef[key] = channel
		descriptors = append(descriptors, cloneDescriptor(descriptor))
	}
	sort.Slice(descriptors, func(i, j int) bool { return refKey(descriptors[i].Ref) < refKey(descriptors[j].Ref) })
	return &Registry{channels: byRef, list: descriptors}, nil
}

func (r *Registry) Descriptors() []Descriptor {
	if r == nil {
		return nil
	}
	out := make([]Descriptor, len(r.list))
	for i := range r.list {
		out[i] = cloneDescriptor(r.list[i])
	}
	return out
}

func (r *Registry) Resolve(ref ChannelRef) (Channel, error) {
	if r == nil {
		return nil, ErrChannelNotFound
	}
	if err := validateRef(ref); err != nil {
		return nil, err
	}
	channel, ok := r.channels[refKey(ref)]
	if !ok {
		return nil, ErrChannelNotFound
	}
	return channel, nil
}

func (r *Registry) Deliver(ctx context.Context, ref ChannelRef, delivery Delivery) (receipt Receipt, err error) {
	if ctx == nil {
		return Receipt{}, context.Canceled
	}
	if err := ValidateDelivery(delivery); err != nil {
		return Receipt{}, err
	}
	channel, err := r.Resolve(ref)
	if err != nil {
		return Receipt{}, err
	}
	defer func() {
		if recover() != nil {
			receipt = Receipt{}
			err = ErrChannelPanic
		}
	}()
	receipt, err = channel.Deliver(ctx, cloneDelivery(delivery))
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return Receipt{}, context.Canceled
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return Receipt{}, context.DeadlineExceeded
		}
		return Receipt{}, ErrChannelDelivery
	}
	validated, err := validateReceipt(receipt, ref)
	if err != nil {
		return Receipt{}, err
	}
	return validated, nil
}

func ValidateDelivery(delivery Delivery) error {
	if !validText(delivery.TenantID, MaxTenantIDBytes) || !validText(delivery.SessionID, MaxRunIDBytes) ||
		!validText(delivery.RunID, MaxRunIDBytes) || !validText(delivery.CallID, MaxCallIDBytes) ||
		!validText(delivery.IdempotencyKey, MaxIdempotencyKeyBytes) || !validText(delivery.Target.value, MaxTargetRefBytes) ||
		strings.Contains(delivery.Target.value, "://") || !validContent(delivery.Text, MaxTextBytes) || !validText(delivery.Format, MaxFormatBytes) {
		return ErrInvalidDelivery
	}
	if err := validateMetadata(delivery.Metadata); err != nil {
		return fmt.Errorf("%w: metadata: %v", ErrInvalidDelivery, err)
	}
	return nil
}

func safeDescriptor(channel Channel) (descriptor Descriptor, err error) {
	defer func() {
		if recover() != nil {
			err = ErrChannelPanic
		}
	}()
	return channel.Descriptor(), nil
}

func validateDescriptor(descriptor Descriptor) (Descriptor, error) {
	descriptor = cloneDescriptor(descriptor)
	if err := validateRef(descriptor.Ref); err != nil {
		return Descriptor{}, fmt.Errorf("%w: %v", ErrInvalidDescriptor, err)
	}
	if len(descriptor.Capabilities) == 0 || len(descriptor.Capabilities) > 16 {
		return Descriptor{}, ErrInvalidDescriptor
	}
	seen := make(map[Capability]struct{}, len(descriptor.Capabilities))
	for _, capability := range descriptor.Capabilities {
		if !validText(string(capability), MaxChannelCapabilityBytes) {
			return Descriptor{}, ErrInvalidDescriptor
		}
		if _, ok := seen[capability]; ok {
			return Descriptor{}, ErrInvalidDescriptor
		}
		seen[capability] = struct{}{}
	}
	sort.Slice(descriptor.Capabilities, func(i, j int) bool { return descriptor.Capabilities[i] < descriptor.Capabilities[j] })
	return cloneDescriptor(descriptor), nil
}

func validateRef(ref ChannelRef) error {
	if !validText(ref.ID, MaxChannelIDBytes) || !validText(ref.Version, MaxChannelVersionBytes) {
		return ErrInvalidDescriptor
	}
	return nil
}

func validateReceipt(receipt Receipt, expected ChannelRef) (Receipt, error) {
	if receipt.Channel != expected || !validText(receipt.DeliveryID, MaxReceiptIDBytes) ||
		(receipt.Status != ReceiptAccepted && receipt.Status != ReceiptDelivered) {
		return Receipt{}, ErrInvalidReceipt
	}
	if err := validateMetadata(receipt.Metadata); err != nil {
		return Receipt{}, fmt.Errorf("%w: metadata: %v", ErrInvalidReceipt, err)
	}
	return cloneReceipt(receipt), nil
}

func validateMetadata(metadata map[string]string) error {
	if len(metadata) > MaxMetadataEntries {
		return errors.New("too many metadata entries")
	}
	for key, value := range metadata {
		if !validText(key, MaxMetadataKeyBytes) || !validText(value, MaxMetadataValueBytes) || sensitiveMetadataKey(key) || sensitiveMetadataValue(value) {
			return errors.New("metadata must be bounded and non-secret")
		}
	}
	return nil
}

func sensitiveMetadataValue(value string) bool {
	lower := strings.ToLower(value)
	return strings.Contains(value, "://") || strings.Contains(lower, "bearer ") ||
		strings.Contains(lower, "token=") || strings.Contains(lower, "secret=") ||
		strings.Contains(lower, "password=") || strings.Contains(lower, "private_key=")
}

func sensitiveMetadataKey(key string) bool {
	lower := strings.ToLower(key)
	for _, marker := range []string{"secret", "token", "password", "credential", "authorization", "private_key", "endpoint", "url", "body"} {
		if strings.Contains(lower, marker) {
			return true
		}
	}
	return false
}

func validContent(value string, max int) bool {
	if value == "" || len(value) > max || !utf8.ValidString(value) {
		return false
	}
	for _, r := range value {
		if (unicode.IsControl(r) && r != '\n' && r != '\t') || unicode.In(r, unicode.Cf, unicode.Cs, unicode.Co) {
			return false
		}
	}
	return true
}

func validText(value string, max int) bool {
	if value == "" || len(value) > max || !utf8.ValidString(value) || strings.TrimSpace(value) != value {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) || unicode.In(r, unicode.Cf, unicode.Cs, unicode.Co) {
			return false
		}
	}
	return true
}

func refKey(ref ChannelRef) string { return ref.ID + "\x00" + ref.Version }

func cloneDescriptor(descriptor Descriptor) Descriptor {
	descriptor.Capabilities = append([]Capability(nil), descriptor.Capabilities...)
	return descriptor
}

func cloneDelivery(delivery Delivery) Delivery {
	delivery.Metadata = cloneMetadata(delivery.Metadata)
	return delivery
}

func cloneReceipt(receipt Receipt) Receipt {
	receipt.Metadata = cloneMetadata(receipt.Metadata)
	return receipt
}

func cloneMetadata(metadata map[string]string) map[string]string {
	if metadata == nil {
		return nil
	}
	out := make(map[string]string, len(metadata))
	for key, value := range metadata {
		out[key] = value
	}
	return out
}
