package targetresolver

import (
	"context"
	"errors"
	"testing"

	"github.com/cc-auto-agent/harness-core/pkg/adapter/notification/webhook"
	"github.com/cc-auto-agent/harness-core/pkg/app/notification"
)

type serviceStub struct {
	targets []notification.TargetDescriptor
	payload []byte
	err     error
	panic   bool
}

func (stub *serviceStub) List(context.Context, string) ([]notification.TargetDescriptor, error) {
	if stub.panic {
		panic("secret callback panic")
	}
	return stub.targets, stub.err
}
func (stub *serviceStub) ResolveConfig(context.Context, string, notification.TargetRef, notification.ChannelRef) ([]byte, error) {
	if stub.panic {
		panic("secret callback panic")
	}
	return append([]byte(nil), stub.payload...), stub.err
}

func resolverFixture(t *testing.T, payload string) (*Resolver, *serviceStub, notification.TargetRef) {
	t.Helper()
	target, err := notification.NewTargetRef("ops")
	if err != nil {
		t.Fatal(err)
	}
	service := &serviceStub{payload: []byte(payload)}
	resolver, err := New(service)
	if err != nil {
		t.Fatal(err)
	}
	return resolver, service, target
}

func TestResolveStrictOpaqueConfiguration(t *testing.T) {
	resolver, _, target := resolverFixture(t, `{"url":"https://example.test/hook","secret":"secret-value"}`)
	resolved, err := resolver.Resolve(context.Background(), "tenant-a", target)
	if err != nil || resolved.URL != "https://example.test/hook" || string(resolved.Secret) != "secret-value" {
		t.Fatalf("resolved=%#v err=%v", resolved, err)
	}
	resolved.Secret[0] = 'X'
	cases := []string{
		`{"url":"https://example.test/hook","secret":"s","extra":"x"}`,
		`{"url":"https://example.test/hook","secret":"s","secret":"other"}`,
		`{"url":null,"secret":"s"}`,
		`{"url":"https://example.test/hook","secret":"s"} trailing`,
		`{"url":"https://example.test/hook"}`,
		`{"url":"https://example.test/hook","secret":""}`,
	}
	for _, payload := range cases {
		resolver, _, target = resolverFixture(t, payload)
		if _, err := resolver.Resolve(context.Background(), "tenant-a", target); !errors.Is(err, webhook.ErrTargetResolution) {
			t.Errorf("payload %q error=%v", payload, err)
		}
	}
}

func TestResolveClearsAndSanitizesCallbackFailure(t *testing.T) {
	resolver, service, target := resolverFixture(t, `{"url":"https://example.test/hook","secret":"secret-value"}`)
	service.err = errors.New("provider body contains secret-value")
	if _, err := resolver.Resolve(context.Background(), "tenant-a", target); !errors.Is(err, webhook.ErrTargetResolution) {
		t.Fatalf("error=%v", err)
	}
	service.err = nil
	service.panic = true
	if _, err := resolver.Resolve(context.Background(), "tenant-a", target); !errors.Is(err, webhook.ErrTargetResolution) {
		t.Fatalf("panic error=%v", err)
	}
}

func TestListFiltersWebhookAndDefensivelyCopies(t *testing.T) {
	resolver, service, target := resolverFixture(t, `{}`)
	service.targets = []notification.TargetDescriptor{
		{Target: target, Channel: notification.ChannelRef{ID: webhook.ChannelID, Version: webhook.ChannelVersion}, Label: "ops", Formats: []string{"z", "a"}},
		{Target: target, Channel: notification.ChannelRef{ID: "other", Version: "1"}, Label: "ignored"},
	}
	got, err := resolver.List(context.Background(), "tenant-a")
	if err != nil || len(got) != 1 {
		t.Fatalf("list=%#v err=%v", got, err)
	}
	got[0].Formats[0] = "changed"
	if service.targets[0].Formats[0] == "changed" {
		t.Fatal("list leaked service slice")
	}
	service.targets = append(service.targets[:1], service.targets[0])
	if _, err := resolver.List(context.Background(), "tenant-a"); !errors.Is(err, notification.ErrTargetDirectoryFailure) {
		t.Fatalf("duplicate/invalid result error=%v", err)
	}
}

func TestListPanicAndCancellation(t *testing.T) {
	resolver, service, _ := resolverFixture(t, `{}`)
	service.panic = true
	if _, err := resolver.List(context.Background(), "tenant-a"); !errors.Is(err, notification.ErrTargetDirectoryPanic) {
		t.Fatalf("panic list error=%v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := resolver.List(ctx, "tenant-a"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled list error=%v", err)
	}
}
