package notifybridge

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/nikoksr/notify"
	"github.com/whhhh1500/auto-agent/pkg/app/notification"
)

type resolverFunc func(context.Context, string, notification.TargetRef, notification.ChannelRef) ([]byte, error)

func (f resolverFunc) ResolveConfig(ctx context.Context, tenant string, target notification.TargetRef, ref notification.ChannelRef) ([]byte, error) {
	return f(ctx, tenant, target, ref)
}

type notifierFunc func(context.Context, string, string) error

func (f notifierFunc) Send(ctx context.Context, subject, text string) error {
	return f(ctx, subject, text)
}

var _ notify.Notifier = notifierFunc(nil)

type nilNotifier struct{}

func (*nilNotifier) Send(context.Context, string, string) error { return nil }

type nilResolver struct{}

func (*nilResolver) ResolveConfig(context.Context, string, notification.TargetRef, notification.ChannelRef) ([]byte, error) {
	return nil, nil
}

func testDelivery(t *testing.T, tenant string) notification.Delivery {
	t.Helper()
	target, err := notification.NewTargetRef("opaque:target")
	if err != nil {
		t.Fatal(err)
	}
	return notification.Delivery{
		TenantID: tenant, SessionID: "session-1", RunID: "run-1", CallID: "call-1",
		IdempotencyKey: "idem-1", Target: target, Text: "hello", Format: "text/plain",
	}
}

func testRef(id string) notification.ChannelRef { return notification.ChannelRef{ID: id, Version: "1"} }

func TestDeliverIsolatesTenantConfigurationAndClearsBuffers(t *testing.T) {
	ref := testRef("custom-a")
	configs := map[string][]byte{"tenant-a": []byte("secret-a"), "tenant-b": []byte("secret-b")}
	var mu sync.Mutex
	var factoryConfigs [][]byte
	var sends []string
	resolver := resolverFunc(func(_ context.Context, tenant string, _ notification.TargetRef, gotRef notification.ChannelRef) ([]byte, error) {
		if gotRef != ref {
			t.Fatalf("resolved wrong ref: %#v", gotRef)
		}
		return configs[tenant], nil
	})
	channel, err := New(ref, resolver, func(_ context.Context, config []byte) (notify.Notifier, error) {
		mu.Lock()
		factoryConfigs = append(factoryConfigs, config)
		mu.Unlock()
		copied := string(config)
		return notifierFunc(func(_ context.Context, subject, text string) error {
			if subject != Subject || text != "hello" {
				t.Fatalf("unexpected send: subject=%q text=%q", subject, text)
			}
			mu.Lock()
			sends = append(sends, copied)
			mu.Unlock()
			return nil
		}), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, tenant := range []string{"tenant-a", "tenant-b"} {
		receipt, err := channel.Deliver(context.Background(), testDelivery(t, tenant))
		if err != nil {
			t.Fatalf("tenant %s: %v", tenant, err)
		}
		if receipt.Channel != ref || receipt.Status != notification.ReceiptAccepted || receipt.DeliveryID != "idem-1" {
			t.Fatalf("unexpected receipt: %#v", receipt)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if got, want := strings.Join(sends, ","), "secret-a,secret-b"; got != want {
		t.Fatalf("tenant configs mixed: got %q want %q", got, want)
	}
	for tenant, config := range configs {
		for index, value := range config {
			if value != 0 {
				t.Fatalf("resolver config %s byte %d was not cleared", tenant, index)
			}
		}
	}
	for _, config := range factoryConfigs {
		for index, value := range config {
			if value != 0 {
				t.Fatalf("factory config byte %d was not cleared", index)
			}
		}
	}
}

func TestCustomRefsRemainIndependent(t *testing.T) {
	for _, ref := range []notification.ChannelRef{testRef("custom-a"), testRef("custom-b")} {
		t.Run(ref.ID, func(t *testing.T) {
			called := 0
			channel, err := New(ref, resolverFunc(func(_ context.Context, _ string, _ notification.TargetRef, got notification.ChannelRef) ([]byte, error) {
				if got != ref {
					t.Fatalf("got ref %#v, want %#v", got, ref)
				}
				return []byte("config"), nil
			}), func(context.Context, []byte) (notify.Notifier, error) {
				return notifierFunc(func(context.Context, string, string) error { called++; return nil }), nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := channel.Deliver(context.Background(), testDelivery(t, "tenant-a")); err != nil || called != 1 {
				t.Fatalf("err=%v sends=%d", err, called)
			}
		})
	}
}

func TestResolverFailuresDoNotSendAndAreSanitized(t *testing.T) {
	for _, resolveErr := range []error{notification.ErrTargetDisabled, errors.New("secret=https://private.invalid")} {
		sent := 0
		channel, err := New(testRef("custom"), resolverFunc(func(context.Context, string, notification.TargetRef, notification.ChannelRef) ([]byte, error) {
			return nil, resolveErr
		}), func(context.Context, []byte) (notify.Notifier, error) {
			sent++
			return notifierFunc(func(context.Context, string, string) error { return nil }), nil
		})
		if err != nil {
			t.Fatal(err)
		}
		_, err = channel.Deliver(context.Background(), testDelivery(t, "tenant-a"))
		if !errors.Is(err, ErrTargetResolution) || err.Error() != ErrTargetResolution.Error() || sent != 0 || strings.Contains(err.Error(), "private.invalid") {
			t.Fatalf("err=%v factory calls=%d", err, sent)
		}
	}
}

func TestFactoryAndSendFailuresAreSanitizedWithoutRetry(t *testing.T) {
	t.Run("factory", func(t *testing.T) {
		channel, err := New(testRef("custom"), resolverFunc(func(context.Context, string, notification.TargetRef, notification.ChannelRef) ([]byte, error) {
			return []byte("secret-config"), nil
		}), func(context.Context, []byte) (notify.Notifier, error) {
			return nil, errors.New("token=secret")
		})
		if err != nil {
			t.Fatal(err)
		}
		_, err = channel.Deliver(context.Background(), testDelivery(t, "tenant-a"))
		if !errors.Is(err, ErrFactory) || err.Error() != ErrFactory.Error() || strings.Contains(err.Error(), "secret") {
			t.Fatalf("factory error leaked: %v", err)
		}
	})
	t.Run("send", func(t *testing.T) {
		sends := 0
		channel, err := New(testRef("custom"), resolverFunc(func(context.Context, string, notification.TargetRef, notification.ChannelRef) ([]byte, error) {
			return []byte("secret-config"), nil
		}), func(context.Context, []byte) (notify.Notifier, error) {
			return notifierFunc(func(context.Context, string, string) error {
				sends++
				return errors.New("token=secret endpoint=https://private.invalid")
			}), nil
		})
		if err != nil {
			t.Fatal(err)
		}
		_, err = channel.Deliver(context.Background(), testDelivery(t, "tenant-a"))
		if !errors.Is(err, ErrDelivery) || err.Error() != ErrDelivery.Error() || sends != 1 || strings.Contains(err.Error(), "private.invalid") {
			t.Fatalf("err=%v sends=%d", err, sends)
		}
	})
}

func TestDeliverRespectsCancellationBeforeAndBetweenCallbacks(t *testing.T) {
	t.Run("before resolve", func(t *testing.T) {
		called := false
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		channel, err := New(testRef("custom"), resolverFunc(func(context.Context, string, notification.TargetRef, notification.ChannelRef) ([]byte, error) {
			called = true
			return nil, nil
		}), func(context.Context, []byte) (notify.Notifier, error) { return nil, nil })
		if err != nil {
			t.Fatal(err)
		}
		if _, err := channel.Deliver(ctx, testDelivery(t, "tenant-a")); !errors.Is(err, context.Canceled) || called {
			t.Fatalf("err=%v resolver called=%v", err, called)
		}
	})
	t.Run("after resolve", func(t *testing.T) {
		factoryCalled := false
		ctx, cancel := context.WithCancel(context.Background())
		channel, err := New(testRef("custom"), resolverFunc(func(context.Context, string, notification.TargetRef, notification.ChannelRef) ([]byte, error) {
			cancel()
			return []byte("secret-config"), nil
		}), func(context.Context, []byte) (notify.Notifier, error) {
			factoryCalled = true
			return nil, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := channel.Deliver(ctx, testDelivery(t, "tenant-a")); !errors.Is(err, context.Canceled) || factoryCalled {
			t.Fatalf("err=%v factory called=%v", err, factoryCalled)
		}
	})
	t.Run("after factory", func(t *testing.T) {
		sent := false
		ctx, cancel := context.WithCancel(context.Background())
		channel, err := New(testRef("custom"), resolverFunc(func(context.Context, string, notification.TargetRef, notification.ChannelRef) ([]byte, error) {
			return []byte("secret-config"), nil
		}), func(context.Context, []byte) (notify.Notifier, error) {
			cancel()
			return notifierFunc(func(context.Context, string, string) error { sent = true; return nil }), nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := channel.Deliver(ctx, testDelivery(t, "tenant-a")); !errors.Is(err, context.Canceled) || sent {
			t.Fatalf("err=%v sent=%v", err, sent)
		}
	})
}

func TestNewAndCallbacksFailClosed(t *testing.T) {
	if _, err := New(notification.ChannelRef{}, nil, nil); !errors.Is(err, ErrInvalidChannel) {
		t.Fatalf("invalid constructor: %v", err)
	}
	if _, err := New(testRef("custom"), (*nilResolver)(nil), func(context.Context, []byte) (notify.Notifier, error) { return nil, nil }); !errors.Is(err, ErrInvalidChannel) {
		t.Fatalf("typed nil resolver accepted: %v", err)
	}
	typedNil, err := New(testRef("custom"), resolverFunc(func(context.Context, string, notification.TargetRef, notification.ChannelRef) ([]byte, error) {
		return []byte("secret"), nil
	}), func(context.Context, []byte) (notify.Notifier, error) { return (*nilNotifier)(nil), nil })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := typedNil.Deliver(context.Background(), testDelivery(t, "tenant-a")); !errors.Is(err, ErrFactory) || err.Error() != ErrFactory.Error() {
		t.Fatalf("typed nil notifier was not rejected: %v", err)
	}
	for _, makeChannel := range []func() (*Channel, error){
		func() (*Channel, error) {
			return New(testRef("custom"), resolverFunc(func(context.Context, string, notification.TargetRef, notification.ChannelRef) ([]byte, error) {
				panic("secret")
			}), func(context.Context, []byte) (notify.Notifier, error) { return nil, nil })
		},
		func() (*Channel, error) {
			return New(testRef("custom"), resolverFunc(func(context.Context, string, notification.TargetRef, notification.ChannelRef) ([]byte, error) {
				return []byte("secret"), nil
			}), func(context.Context, []byte) (notify.Notifier, error) { panic("secret") })
		},
		func() (*Channel, error) {
			return New(testRef("custom"), resolverFunc(func(context.Context, string, notification.TargetRef, notification.ChannelRef) ([]byte, error) {
				return []byte("secret"), nil
			}), func(context.Context, []byte) (notify.Notifier, error) {
				return notifierFunc(func(context.Context, string, string) error { panic("secret") }), nil
			})
		},
	} {
		channel, err := makeChannel()
		if err != nil {
			t.Fatal(err)
		}
		_, err = channel.Deliver(context.Background(), testDelivery(t, "tenant-a"))
		if !errors.Is(err, ErrPanic) || err.Error() != ErrPanic.Error() || strings.Contains(err.Error(), "secret") {
			t.Fatalf("panic leaked: %v", err)
		}
	}
}
