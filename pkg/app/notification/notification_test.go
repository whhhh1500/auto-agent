package notification

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

type fakeChannel struct {
	descriptor      Descriptor
	receipt         Receipt
	panic           bool
	descriptorPanic bool
	deliverErr      error
	seen            Delivery
}

func (c *fakeChannel) Descriptor() Descriptor {
	if c.descriptorPanic {
		panic("secret descriptor panic")
	}
	return c.descriptor
}
func (c *fakeChannel) Deliver(_ context.Context, delivery Delivery) (Receipt, error) {
	if c.panic {
		panic("secret channel panic")
	}
	if c.deliverErr != nil {
		return Receipt{}, c.deliverErr
	}
	c.seen = cloneDelivery(delivery)
	return c.receipt, nil
}

func validChannel() *fakeChannel {
	return &fakeChannel{
		descriptor: Descriptor{Ref: ChannelRef{ID: "email", Version: "1"}, Capabilities: []Capability{CapabilityDeliver}},
		receipt:    Receipt{Channel: ChannelRef{ID: "email", Version: "1"}, DeliveryID: "delivery-1", Status: ReceiptDelivered},
	}
}

func validDelivery(t *testing.T) Delivery {
	t.Helper()
	target, err := NewTargetRef("user:opaque-1")
	if err != nil {
		t.Fatal(err)
	}
	return Delivery{TenantID: "tenant-1", SessionID: "session-1", RunID: "run-1", CallID: "call-1", IdempotencyKey: "idem-1", Target: target, Text: "hello", Format: "text/plain", Metadata: map[string]string{"locale": "zh-CN"}}
}

func TestRegistryIsImmutableAndResolvesExactVersion(t *testing.T) {
	channel := validChannel()
	originalCapabilities := channel.descriptor.Capabilities
	registry, err := NewRegistry([]Channel{channel})
	if err != nil {
		t.Fatal(err)
	}
	descriptors := registry.Descriptors()
	descriptors[0].Ref.ID = "mutated"
	descriptors[0].Capabilities[0] = "mutated"
	if got := registry.Descriptors()[0].Ref.ID; got != "email" {
		t.Fatalf("descriptor leaked mutation: %q", got)
	}
	if len(originalCapabilities) != 1 || originalCapabilities[0] != CapabilityDeliver {
		t.Fatal("registry validation mutated channel-owned capabilities")
	}
	if _, err := registry.Resolve(ChannelRef{ID: "email", Version: "2"}); !errors.Is(err, ErrChannelNotFound) {
		t.Fatalf("inexact version resolved: %v", err)
	}
}

func TestRegistryDeliveryClonesAndValidatesReceipt(t *testing.T) {
	channel := validChannel()
	registry, err := NewRegistry([]Channel{channel})
	if err != nil {
		t.Fatal(err)
	}
	delivery := validDelivery(t)
	receipt, err := registry.Deliver(context.Background(), channel.descriptor.Ref, delivery)
	if err != nil || receipt.Status != ReceiptDelivered || channel.seen.Target.String() != "user:opaque-1" {
		t.Fatalf("delivery failed: receipt=%#v err=%v seen=%#v", receipt, err, channel.seen)
	}
	delivery.Metadata["locale"] = "mutated"
	if channel.seen.Metadata["locale"] != "zh-CN" {
		t.Fatal("delivery metadata was not cloned")
	}
	channel.receipt.Status = "invalid"
	if _, err := registry.Deliver(context.Background(), channel.descriptor.Ref, validDelivery(t)); !errors.Is(err, ErrInvalidReceipt) {
		t.Fatalf("invalid receipt accepted: %v", err)
	}
}

func TestRegistryPanicIsolatedWithoutPanicValue(t *testing.T) {
	channel := validChannel()
	channel.panic = true
	registry, err := NewRegistry([]Channel{channel})
	if err != nil {
		t.Fatal(err)
	}
	_, err = registry.Deliver(context.Background(), channel.descriptor.Ref, validDelivery(t))
	if !errors.Is(err, ErrChannelPanic) || err.Error() != ErrChannelPanic.Error() {
		t.Fatalf("panic was not isolated: %v", err)
	}
}

func TestRegistryCleansChannelErrors(t *testing.T) {
	channel := validChannel()
	channel.deliverErr = errors.New("provider token=top-secret body=https://private.example")
	registry, err := NewRegistry([]Channel{channel})
	if err != nil {
		t.Fatal(err)
	}
	_, err = registry.Deliver(context.Background(), channel.descriptor.Ref, validDelivery(t))
	if !errors.Is(err, ErrChannelDelivery) || err.Error() != ErrChannelDelivery.Error() {
		t.Fatalf("channel error was not sanitized: %v", err)
	}
	channel.deliverErr = fmt.Errorf("provider secret: %w", context.Canceled)
	_, err = registry.Deliver(context.Background(), channel.descriptor.Ref, validDelivery(t))
	if !errors.Is(err, context.Canceled) || err.Error() != context.Canceled.Error() {
		t.Fatalf("wrapped context error was not sanitized: %v", err)
	}
}

func TestNotificationBoundsAndNonSecretMetadata(t *testing.T) {
	if _, err := NewTargetRef("https://secret.example"); !errors.Is(err, ErrInvalidDelivery) {
		t.Fatalf("URL target accepted: %v", err)
	}
	delivery := validDelivery(t)
	delivery.Metadata = map[string]string{"access_token": "secret"}
	if err := ValidateDelivery(delivery); !errors.Is(err, ErrInvalidDelivery) {
		t.Fatalf("secret metadata accepted: %v", err)
	}
	delivery.Metadata = map[string]string{"x": "\u200b"}
	if err := ValidateDelivery(delivery); !errors.Is(err, ErrInvalidDelivery) {
		t.Fatalf("format control metadata accepted: %v", err)
	}
	for _, key := range []string{"endpoint", "token", "body"} {
		delivery.Metadata = map[string]string{key: "opaque"}
		if err := ValidateDelivery(delivery); !errors.Is(err, ErrInvalidDelivery) {
			t.Fatalf("sensitive metadata key accepted: %q", key)
		}
	}
	delivery.Metadata = map[string]string{"x": "https://private.example"}
	if err := ValidateDelivery(delivery); !errors.Is(err, ErrInvalidDelivery) {
		t.Fatal("metadata URL accepted")
	}
	delivery.Metadata = map[string]string{"x": "token=top-secret"}
	if err := ValidateDelivery(delivery); !errors.Is(err, ErrInvalidDelivery) {
		t.Fatal("metadata secret marker accepted")
	}
	delivery.Metadata = nil
	delivery.Text = "line one\n\tline two"
	if err := ValidateDelivery(delivery); err != nil {
		t.Fatalf("notification content newline/tab rejected: %v", err)
	}
	delivery.Text = "line one\rline two"
	if err := ValidateDelivery(delivery); !errors.Is(err, ErrInvalidDelivery) {
		t.Fatal("notification carriage return accepted")
	}
	channel := validChannel()
	channel.receipt.Metadata = map[string]string{"body": "opaque"}
	registry, err := NewRegistry([]Channel{channel})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Deliver(context.Background(), channel.descriptor.Ref, validDelivery(t)); !errors.Is(err, ErrInvalidReceipt) {
		t.Fatalf("sensitive receipt metadata accepted: %v", err)
	}
	delivery.Text = ""
	if err := ValidateDelivery(delivery); !errors.Is(err, ErrInvalidDelivery) {
		t.Fatalf("empty text accepted: %v", err)
	}
}

func TestRegistryRejectsDescriptorPanicAndCapacity(t *testing.T) {
	panicking := &fakeChannel{descriptorPanic: true}
	if _, err := NewRegistry([]Channel{panicking}); !errors.Is(err, ErrChannelPanic) {
		t.Fatalf("descriptor panic was not isolated: %v", err)
	}
	channels := make([]Channel, MaxChannels+1)
	for i := range channels {
		channels[i] = validChannel()
	}
	if _, err := NewRegistry(channels); !errors.Is(err, ErrRegistryCapacity) {
		t.Fatalf("registry capacity was not enforced: %v", err)
	}
}

func TestRegistryNilContextFailsClosed(t *testing.T) {
	channel := validChannel()
	registry, err := NewRegistry([]Channel{channel})
	if err != nil {
		t.Fatal(err)
	}
	var nilContext context.Context
	_, err = registry.Deliver(nilContext, channel.descriptor.Ref, validDelivery(t))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("nil context did not fail closed: %v", err)
	}
}

func TestSnapshotDirectoryIsTenantScopedSortedAndDefensive(t *testing.T) {
	channel := ChannelRef{ID: "webhook", Version: "1"}
	first, err := NewTargetRef("target:z")
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewTargetRef("target:a")
	if err != nil {
		t.Fatal(err)
	}
	input := map[string][]TargetDescriptor{
		"tenant-a": {
			{Target: first, Channel: channel, Label: "Z", Formats: []string{"text/plain", "text/html"}},
			{Target: second, Channel: channel, Label: "A", Formats: []string{"text/plain"}},
		},
		"tenant-b": {{Target: first, Channel: channel, Label: "other"}},
	}
	directory, err := NewSnapshotDirectory([]ChannelRef{channel}, input)
	if err != nil {
		t.Fatal(err)
	}
	input["tenant-a"][0].Label = "mutated"
	input["tenant-a"][0].Formats[0] = "mutated"
	got, err := directory.List(context.Background(), "tenant-a")
	if err != nil || len(got) != 2 || got[0].Target.String() != "target:a" || got[0].Formats[0] != "text/plain" {
		t.Fatalf("snapshot changed or was not sorted: %#v err=%v", got, err)
	}
	got[0].Formats[0] = "changed"
	got, err = directory.List(context.Background(), "tenant-a")
	if err != nil || got[0].Formats[0] != "text/plain" {
		t.Fatalf("list leaked mutable formats: %#v err=%v", got, err)
	}
	other, err := directory.List(context.Background(), "tenant-b")
	if err != nil || len(other) != 1 || other[0].Label != "other" {
		t.Fatalf("cross-tenant lookup failed: %#v err=%v", other, err)
	}
	if _, err := directory.List(context.Background(), "tenant-a\nattacker"); !errors.Is(err, ErrInvalidTenantID) {
		t.Fatalf("invalid tenant was accepted: %v", err)
	}
	var nilContext context.Context
	if _, err := directory.List(nilContext, "tenant-a"); !errors.Is(err, context.Canceled) {
		t.Fatalf("nil context was accepted: %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := directory.List(canceled, "tenant-a"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled context was accepted: %v", err)
	}
}

func TestSnapshotDirectoryRejectsUnknownDuplicateAndOverCapacity(t *testing.T) {
	channel := ChannelRef{ID: "webhook", Version: "1"}
	target, err := NewTargetRef("target:one")
	if err != nil {
		t.Fatal(err)
	}
	base := TargetDescriptor{Target: target, Channel: channel, Label: "one", Formats: []string{"text/plain"}}
	if _, err := NewSnapshotDirectory([]ChannelRef{{ID: "other", Version: "1"}}, map[string][]TargetDescriptor{"tenant": {base}}); !errors.Is(err, ErrUnknownTargetChannel) {
		t.Fatalf("unknown channel accepted: %v", err)
	}
	if _, err := NewSnapshotDirectory([]ChannelRef{channel}, map[string][]TargetDescriptor{"tenant": {base, base}}); !errors.Is(err, ErrInvalidTargetDescriptor) {
		t.Fatalf("duplicate target accepted: %v", err)
	}
	tooMany := make([]TargetDescriptor, MaxTargets+1)
	for i := range tooMany {
		ref, err := NewTargetRef(fmt.Sprintf("target:%03d", i))
		if err != nil {
			t.Fatal(err)
		}
		tooMany[i] = TargetDescriptor{Target: ref, Channel: channel}
	}
	if _, err := NewSnapshotDirectory([]ChannelRef{channel}, map[string][]TargetDescriptor{"tenant": tooMany}); !errors.Is(err, ErrTargetDirectoryCapacity) {
		t.Fatalf("target capacity was not enforced: %v", err)
	}
	if _, err := NewSnapshotDirectory([]ChannelRef{channel, channel}, nil); !errors.Is(err, ErrInvalidTargetDescriptor) {
		t.Fatalf("duplicate channel accepted: %v", err)
	}
}

func TestTargetDescriptorValidationAndClone(t *testing.T) {
	ref, err := NewTargetRef("target:one")
	if err != nil {
		t.Fatal(err)
	}
	descriptor := TargetDescriptor{Target: ref, Channel: ChannelRef{ID: "webhook", Version: "1"}, Label: "safe", Formats: []string{"z", "a"}}
	if err := descriptor.Validate([]ChannelRef{{ID: "webhook", Version: "1"}}); err != nil {
		t.Fatal(err)
	}
	clone := descriptor.Clone()
	if clone.Formats[0] != "a" || descriptor.Formats[0] != "z" {
		t.Fatalf("clone did not sort independently: clone=%#v original=%#v", clone.Formats, descriptor.Formats)
	}
	for _, invalid := range []TargetDescriptor{
		{Target: ref, Channel: ChannelRef{ID: "webhook", Version: "1"}, Label: "https://secret.example"},
		{Target: ref, Channel: ChannelRef{ID: "webhook", Version: "1"}, Label: "secret"},
		{Target: ref, Channel: ChannelRef{ID: "webhook", Version: "1"}, Formats: []string{"dup", "dup"}},
		{Target: ref, Channel: ChannelRef{ID: "webhook", Version: "1"}, Formats: []string{"\u200b"}},
	} {
		if err := invalid.Validate([]ChannelRef{{ID: "webhook", Version: "1"}}); !errors.Is(err, ErrInvalidTargetDescriptor) {
			t.Fatalf("invalid descriptor accepted: %#v err=%v", invalid, err)
		}
	}
}
