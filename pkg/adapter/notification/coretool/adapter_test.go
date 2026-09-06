package coretool

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/cc-auto-agent/harness-core/pkg/app/notification"
	"github.com/cc-auto-agent/harness-core/pkg/core"
)

type testChannel struct{}

func (testChannel) Descriptor() notification.Descriptor {
	return notification.Descriptor{Ref: notification.ChannelRef{ID: "test", Version: "1"}, Capabilities: []notification.Capability{notification.CapabilityDeliver}}
}
func (testChannel) Deliver(context.Context, notification.Delivery) (notification.Receipt, error) {
	return notification.Receipt{Channel: notification.ChannelRef{ID: "test", Version: "1"}, DeliveryID: "d1", Status: notification.ReceiptAccepted}, nil
}

type targetDirectoryFunc func(context.Context, string) ([]notification.TargetDescriptor, error)

func (f targetDirectoryFunc) List(ctx context.Context, tenantID string) ([]notification.TargetDescriptor, error) {
	return f(ctx, tenantID)
}

func TestNewExposesOnlyTwoGuardedCapabilities(t *testing.T) {
	registry, err := notification.NewRegistry([]notification.Channel{testChannel{}})
	if err != nil {
		t.Fatal(err)
	}
	capabilities, err := New(registry)
	if err != nil || len(capabilities) != 2 {
		t.Fatalf("capabilities=%#v err=%v", capabilities, err)
	}
	seen := map[string]bool{}
	for _, capability := range capabilities {
		manifest := capability.Manifest()
		seen[manifest.ID] = true
		if manifest.Tool == nil || manifest.Kind != core.KindTool || manifest.Contract != "notification/coretool/v1" {
			t.Fatalf("invalid manifest: %#v", manifest)
		}
		if manifest.ID == SendCapabilityID && !manifest.RequiresApproval {
			t.Fatal("notify.send is not approval guarded")
		}
		if manifest.ID == SendCapabilityID {
			if manifest.Idempotent {
				t.Fatal("notify.send claimed idempotency without a channel guarantee")
			}
			if manifest.RequiredCredentials != nil {
				t.Fatal("notify.send unexpectedly requests credentials")
			}
			if containsSensitiveSchemaText(manifest.Tool.Parameters) {
				t.Fatal("notify.send schema exposes credential or URL fields")
			}
		}
	}
	if !seen[ChannelsCapabilityID] || !seen[SendCapabilityID] {
		t.Fatalf("unexpected capability ids: %v", seen)
	}
}

func TestNewWithDirectoryExposesTargetsAndKeepsNewCompatible(t *testing.T) {
	registry, err := notification.NewRegistry([]notification.Channel{testChannel{}})
	if err != nil {
		t.Fatal(err)
	}
	target, err := notification.NewTargetRef("opaque:target-1")
	if err != nil {
		t.Fatal(err)
	}
	directory, err := notification.NewSnapshotDirectory([]notification.ChannelRef{{ID: "test", Version: "1"}}, map[string][]notification.TargetDescriptor{
		"tenant-1": {{Target: target, Channel: notification.ChannelRef{ID: "test", Version: "1"}, Label: "primary", Formats: []string{"text/plain"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	capabilities, err := NewWithDirectory(registry, directory)
	if err != nil || len(capabilities) != 3 {
		t.Fatalf("capabilities=%#v err=%v", capabilities, err)
	}
	if capabilities[0].Manifest().ID != ChannelsCapabilityID || capabilities[1].Manifest().ID != TargetsCapabilityID || capabilities[2].Manifest().ID != SendCapabilityID {
		t.Fatalf("unexpected capability order: %q %q %q", capabilities[0].Manifest().ID, capabilities[1].Manifest().ID, capabilities[2].Manifest().ID)
	}
	legacy, err := New(registry)
	if err != nil || len(legacy) != 2 {
		t.Fatalf("legacy New changed: len=%d err=%v", len(legacy), err)
	}
	manifest := capabilities[1].Manifest()
	if manifest.RequiredPermissions[0] != core.PermRead || !manifest.Idempotent || manifest.RequiresApproval {
		t.Fatalf("invalid targets manifest: %#v", manifest)
	}
	if manifest.Tool == nil || manifest.Tool.Parameters["additionalProperties"] != false {
		t.Fatalf("targets manifest is not empty-input: %#v", manifest.Tool)
	}
}

func TestCapabilitiesRejectDirectUnacceptedInvocation(t *testing.T) {
	registry, err := notification.NewRegistry([]notification.Channel{testChannel{}})
	if err != nil {
		t.Fatal(err)
	}
	directory, err := notification.NewSnapshotDirectory([]notification.ChannelRef{{ID: "test", Version: "1"}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	capabilities, _ := NewWithDirectory(registry, directory)
	for _, capability := range capabilities {
		result, err := capability.Execute(context.Background(), core.CapabilityRequest{})
		if err != nil || result.OK || result.Metadata["code"] != "accepted_invocation_required" {
			t.Fatalf("direct invocation was not rejected: result=%#v err=%v", result, err)
		}
	}
}

func TestTargetsListUsesTenantAndExplicitNonSecretWire(t *testing.T) {
	registry, err := notification.NewRegistry([]notification.Channel{testChannel{}})
	if err != nil {
		t.Fatal(err)
	}
	first, _ := notification.NewTargetRef("opaque:z")
	second, _ := notification.NewTargetRef("opaque:a")
	directory, err := notification.NewSnapshotDirectory([]notification.ChannelRef{{ID: "test", Version: "1"}}, map[string][]notification.TargetDescriptor{
		"tenant-1": {
			{Target: first, Channel: notification.ChannelRef{ID: "test", Version: "1"}, Label: "z", Formats: []string{"text/plain", "text/html"}},
			{Target: second, Channel: notification.ChannelRef{ID: "test", Version: "1"}, Label: "a", Formats: []string{"text/plain"}},
		},
		"tenant-2": {{Target: first, Channel: notification.ChannelRef{ID: "test", Version: "1"}, Label: "other"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	capabilities, err := NewWithDirectory(registry, directory)
	if err != nil {
		t.Fatal(err)
	}
	targets := capabilities[1].(capability)
	result, err := targets.listTargets(context.Background(), core.CapabilityRequest{Context: core.CapabilityContext{Principal: core.Principal{TenantID: "tenant-1"}}})
	if err != nil || !result.OK {
		t.Fatalf("target list failed: result=%#v err=%v", result, err)
	}
	var wire struct {
		Targets []targetWire `json:"targets"`
	}
	if err := json.Unmarshal([]byte(result.Content), &wire); err != nil {
		t.Fatal(err)
	}
	if len(wire.Targets) != 2 || wire.Targets[0].TargetRef != "opaque:a" || wire.Targets[0].Formats[0] != "text/plain" {
		t.Fatalf("unexpected wire targets: %#v", wire.Targets)
	}
	if strings.Contains(result.Content, "https://") || strings.Contains(result.Content, "secret") || strings.Contains(result.Content, "provider-response") || strings.Contains(result.Content, "database-id") {
		t.Fatalf("wire leaked non-secret boundary data: %s", result.Content)
	}
	other, err := targets.listTargets(context.Background(), core.CapabilityRequest{Context: core.CapabilityContext{Principal: core.Principal{TenantID: "tenant-2"}}})
	if err != nil || !other.OK || strings.Contains(other.Content, "opaque:a") {
		t.Fatalf("cross-tenant target visibility failed: result=%#v err=%v", other, err)
	}
}

func TestTargetsDirectoryFailuresAreFixedAndBounded(t *testing.T) {
	registry, err := notification.NewRegistry([]notification.Channel{testChannel{}})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name      string
		directory notification.TargetDirectory
		want      string
	}{
		{name: "error", directory: targetDirectoryFunc(func(context.Context, string) ([]notification.TargetDescriptor, error) {
			return nil, errors.New("token=secret endpoint=https://private.invalid")
		}), want: notification.ErrTargetDirectoryFailure.Error()},
		{name: "panic", directory: targetDirectoryFunc(func(context.Context, string) ([]notification.TargetDescriptor, error) { panic("secret panic") }), want: notification.ErrTargetDirectoryPanic.Error()},
		{name: "duplicate", directory: targetDirectoryFunc(func(context.Context, string) ([]notification.TargetDescriptor, error) {
			ref, _ := notification.NewTargetRef("opaque:dup")
			entry := notification.TargetDescriptor{Target: ref, Channel: notification.ChannelRef{ID: "test", Version: "1"}}
			return []notification.TargetDescriptor{entry, entry}, nil
		}), want: notification.ErrTargetDirectoryFailure.Error()},
	} {
		t.Run(test.name, func(t *testing.T) {
			capabilities, err := NewWithDirectory(registry, test.directory)
			if err != nil {
				t.Fatal(err)
			}
			result, err := capabilities[1].(capability).listTargets(context.Background(), core.CapabilityRequest{Context: core.CapabilityContext{Principal: core.Principal{TenantID: "tenant-1"}}})
			if err != nil || result.OK || result.Content != test.want || strings.Contains(result.Content, "secret") || strings.Contains(result.Content, "private.invalid") {
				t.Fatalf("failure was not sanitized: result=%#v err=%v", result, err)
			}
		})
	}
	tooMany := targetDirectoryFunc(func(context.Context, string) ([]notification.TargetDescriptor, error) {
		entries := make([]notification.TargetDescriptor, notification.MaxTargets+1)
		for i := range entries {
			ref, _ := notification.NewTargetRef(fmt.Sprintf("opaque:%d", i))
			entries[i] = notification.TargetDescriptor{Target: ref, Channel: notification.ChannelRef{ID: "test", Version: "1"}}
		}
		return entries, nil
	})
	capabilities, _ := NewWithDirectory(registry, tooMany)
	result, err := capabilities[1].(capability).listTargets(context.Background(), core.CapabilityRequest{Context: core.CapabilityContext{Principal: core.Principal{TenantID: "tenant-1"}}})
	if err != nil || result.OK || result.Content != notification.ErrTargetDirectoryCapacity.Error() {
		t.Fatalf("directory capacity was not bounded: result=%#v err=%v", result, err)
	}
	result, err = capabilities[1].(capability).listTargets(context.Background(), core.CapabilityRequest{Args: map[string]any{"tenant": "tenant-2"}, Context: core.CapabilityContext{Principal: core.Principal{TenantID: "tenant-1"}}})
	if err != nil || result.OK || result.Content != "notify.targets arguments must be empty" {
		t.Fatalf("non-empty target args accepted: result=%#v err=%v", result, err)
	}
}

func TestDeliveryFromRequestDerivesIdentityAndRejectsModelIdentity(t *testing.T) {
	request := core.CapabilityRequest{CallID: "call-1", IdempotencyKey: "call-1", Args: map[string]any{
		"channel_id": "test", "channel_version": "1", "target_ref": "opaque:target", "text": "hello", "format": "text/plain", "metadata": map[string]any{"locale": "zh-CN"},
	}, Context: core.CapabilityContext{Principal: core.Principal{TenantID: "tenant-1"}, Invocation: core.Invocation{SessionID: "session-1", RunID: "run-1", CallID: "call-1"}}}
	ref, delivery, err := deliveryFromRequest(request)
	if err != nil || ref.ID != "test" || delivery.TenantID != "tenant-1" || delivery.SessionID != "session-1" || delivery.RunID != "run-1" || delivery.CallID != "call-1" {
		t.Fatalf("identity derivation failed: ref=%#v delivery=%#v err=%v", ref, delivery, err)
	}
	request.IdempotencyKey = ""
	_, delivery, err = deliveryFromRequest(request)
	if err != nil || delivery.IdempotencyKey != request.CallID {
		t.Fatalf("bounded call identity fallback failed: delivery=%#v err=%v", delivery, err)
	}
	request.Args["url"] = "https://secret.example"
	if _, _, err := deliveryFromRequest(request); err == nil {
		t.Fatal("model-supplied URL field was accepted")
	}
}

func containsSensitiveSchemaText(value any) bool {
	switch item := value.(type) {
	case string:
		for _, marker := range []string{"url", "token", "secret", "password", "credential"} {
			for i := 0; i+len(marker) <= len(item); i++ {
				if equalFoldASCII(item[i:i+len(marker)], marker) {
					return true
				}
			}
		}
	case map[string]any:
		for key, nested := range item {
			if containsSensitiveSchemaText(key) || containsSensitiveSchemaText(nested) {
				return true
			}
		}
	case []any:
		for _, nested := range item {
			if containsSensitiveSchemaText(nested) {
				return true
			}
		}
	}
	return false
}

func equalFoldASCII(left, right string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] >= 'A' && left[i] <= 'Z' {
			left = left[:i] + string(left[i]+'a'-'A') + left[i+1:]
		}
	}
	return left == right
}
