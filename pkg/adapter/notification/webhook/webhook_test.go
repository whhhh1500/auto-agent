package webhook

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/whhhh1500/auto-agent/pkg/app/notification"
)

type resolverFunc func(context.Context, string, notification.TargetRef) (ResolvedTarget, error)

func (f resolverFunc) Resolve(ctx context.Context, tenant string, target notification.TargetRef) (ResolvedTarget, error) {
	return f(ctx, tenant, target)
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func testDelivery(t *testing.T) notification.Delivery {
	t.Helper()
	target, err := notification.NewTargetRef("customer:opaque-7")
	if err != nil {
		t.Fatal(err)
	}
	return notification.Delivery{
		TenantID: "tenant-1", SessionID: "session-1", RunID: "run-1", CallID: "call-1",
		IdempotencyKey: "idem-1", Target: target, Text: "hello\nworld", Format: "text/plain",
		Metadata: map[string]string{"locale": "zh-CN"},
	}
}

func newTestChannel(resolver TargetResolver, rt http.RoundTripper) *Channel {
	return &Channel{resolver: resolver, client: &http.Client{
		Transport: rt,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}}
}

func TestDeliverSignsBoundedJSONAndClearsSecret(t *testing.T) {
	delivery := testDelivery(t)
	secret := []byte("resolver-owned-secret")
	var seen struct {
		sync.Mutex
		request *http.Request
		body    string
	}
	channel := newTestChannel(resolverFunc(func(_ context.Context, tenant string, target notification.TargetRef) (ResolvedTarget, error) {
		if tenant != delivery.TenantID || target.String() != delivery.Target.String() {
			t.Fatalf("resolver received tenant=%q target=%q", tenant, target.String())
		}
		return ResolvedTarget{URL: "https://8.8.8.8/hook", Secret: secret}, nil
	}), roundTripperFunc(func(request *http.Request) (*http.Response, error) {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			return nil, err
		}
		seen.Lock()
		seen.request, seen.body = request, string(body)
		seen.Unlock()
		return &http.Response{StatusCode: http.StatusAccepted, Body: io.NopCloser(strings.NewReader("accepted")), Header: make(http.Header), Request: request}, nil
	}))

	receipt, err := channel.Deliver(context.Background(), delivery)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Status != notification.ReceiptAccepted || receipt.Channel.ID != ChannelID || receipt.Channel.Version != ChannelVersion || receipt.DeliveryID != delivery.IdempotencyKey {
		t.Fatalf("unexpected receipt: %#v", receipt)
	}
	if len(receipt.Metadata) != 1 || receipt.Metadata["class"] != "accepted" {
		t.Fatalf("unexpected receipt metadata: %#v", receipt.Metadata)
	}
	if strings.Contains(receiptMetadata(receipt), "8.8.8.8") || strings.Contains(receiptMetadata(receipt), "resolver-owned-secret") {
		t.Fatal("receipt contains provider details")
	}
	seen.Lock()
	request, body := seen.request, seen.body
	seen.Unlock()
	if request == nil || body == "" {
		t.Fatal("round trip was not captured")
	}
	if request.Method != http.MethodPost || request.URL.String() != "https://8.8.8.8/hook" {
		t.Fatalf("unexpected request: %s %s", request.Method, request.URL)
	}
	if request.Header.Get("Content-Type") != "application/json" || request.Header.Get("Idempotency-Key") != delivery.IdempotencyKey {
		t.Fatalf("missing request headers: %#v", request.Header)
	}
	if request.GetBody != nil {
		t.Fatal("request retained GetBody and may be replayed by net/http")
	}
	timestamp := request.Header.Get("X-Notification-Timestamp")
	if _, err := strconv.ParseInt(timestamp, 10, 64); err != nil {
		t.Fatalf("invalid timestamp %q: %v", timestamp, err)
	}
	mac := hmac.New(sha256.New, []byte("resolver-owned-secret"))
	_, _ = mac.Write([]byte(timestamp + "." + body))
	expected := "sha256=" + hex.EncodeToString(mac.Sum(nil))
	if request.Header.Get("X-Notification-Signature") != expected {
		t.Fatalf("signature mismatch: got %q want %q", request.Header.Get("X-Notification-Signature"), expected)
	}
	if strings.Contains(body, "198.51.100.7") || strings.Contains(body, "resolver-owned-secret") {
		t.Fatalf("request body leaked target/secret: %s", body)
	}
	for index, value := range secret {
		if value != 0 {
			t.Fatalf("secret byte %d was not cleared", index)
		}
	}
}

func receiptMetadata(receipt notification.Receipt) string {
	var builder strings.Builder
	for key, value := range receipt.Metadata {
		builder.WriteString(key)
		builder.WriteString(value)
	}
	return builder.String()
}

func TestDeliverRejectsUnsafeEndpointsBeforeTransport(t *testing.T) {
	for _, endpoint := range []string{
		"http://example.com/hook",
		"https://user:password@example.com/hook",
		"https://example.com/hook#fragment",
		"https://example.com/hook?api_key=secret",
		"https://127.0.0.1/hook",
		"https://[::1]/hook",
		"https://[::ffff:127.0.0.1]/hook",
		"https://[fc00::1]/hook",
		"https://[2001::1]/hook",
		"https://[2001:1ff::1]/hook",
		"https://[2001:2::1]/hook",
		"https://[2001:10::1]/hook",
		"https://[2001:20::1]/hook",
		"https://[2001:db8::1]/hook",
		"https://[2002::1]/hook",
		"https://[3fff::1]/hook",
		"https://169.254.169.254/latest",
		"https://100.64.0.1/hook",
		"https://0.0.0.0/hook",
		"https://203.0.113.10/hook",
	} {
		called := false
		channel := newTestChannel(resolverFunc(func(context.Context, string, notification.TargetRef) (ResolvedTarget, error) {
			return ResolvedTarget{URL: endpoint, Secret: []byte("secret")}, nil
		}), roundTripperFunc(func(*http.Request) (*http.Response, error) {
			called = true
			return nil, errors.New("must not send")
		}))
		_, err := channel.Deliver(context.Background(), testDelivery(t))
		if !errors.Is(err, ErrInvalidTarget) || err.Error() != ErrInvalidTarget.Error() {
			t.Errorf("endpoint %q: unexpected error %v", endpoint, err)
		}
		if called {
			t.Errorf("endpoint %q reached transport", endpoint)
		}
	}
}

func TestDeliverSanitizesResolverTransportAndProviderErrors(t *testing.T) {
	delivery := testDelivery(t)
	for _, test := range []struct {
		name string
		make Channel
		want error
	}{
		{
			name: "resolver error",
			make: *newTestChannel(resolverFunc(func(context.Context, string, notification.TargetRef) (ResolvedTarget, error) {
				return ResolvedTarget{}, errors.New("secret=https://private.invalid/token")
			}), roundTripperFunc(func(*http.Request) (*http.Response, error) { return nil, nil })),
			want: ErrTargetResolution,
		},
		{
			name: "resolver panic",
			make: *newTestChannel(resolverFunc(func(context.Context, string, notification.TargetRef) (ResolvedTarget, error) {
				panic("secret resolver panic")
			}), roundTripperFunc(func(*http.Request) (*http.Response, error) { return nil, nil })),
			want: ErrWebhookPanic,
		},
		{
			name: "transport error",
			make: *newTestChannel(resolverFunc(func(context.Context, string, notification.TargetRef) (ResolvedTarget, error) {
				return ResolvedTarget{URL: "https://8.8.8.8/hook", Secret: []byte("secret")}, nil
			}), roundTripperFunc(func(*http.Request) (*http.Response, error) {
				return nil, errors.New("token=secret endpoint=https://private.invalid")
			})),
			want: ErrWebhookTransport,
		},
		{
			name: "transport panic",
			make: *newTestChannel(resolverFunc(func(context.Context, string, notification.TargetRef) (ResolvedTarget, error) {
				return ResolvedTarget{URL: "https://8.8.8.8/hook", Secret: []byte("secret")}, nil
			}), roundTripperFunc(func(*http.Request) (*http.Response, error) {
				panic("secret round trip panic")
			})),
			want: ErrWebhookPanic,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := test.make.Deliver(context.Background(), delivery)
			if !errors.Is(err, test.want) || err.Error() != test.want.Error() {
				t.Fatalf("got %v, want %v", err, test.want)
			}
			if strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "private.invalid") {
				t.Fatal("sensitive callback error leaked")
			}
		})
	}
}

func TestResolverPartialSecretIsClearedOnError(t *testing.T) {
	delivery := testDelivery(t)
	secret := []byte("partial-secret")
	channel := newTestChannel(resolverFunc(func(context.Context, string, notification.TargetRef) (ResolvedTarget, error) {
		return ResolvedTarget{Secret: secret}, errors.New("resolver failed")
	}), roundTripperFunc(func(*http.Request) (*http.Response, error) { return nil, nil }))
	_, err := channel.Deliver(context.Background(), delivery)
	if !errors.Is(err, ErrTargetResolution) {
		t.Fatalf("unexpected resolver error: %v", err)
	}
	for index, value := range secret {
		if value != 0 {
			t.Fatalf("partial secret byte %d was not cleared", index)
		}
	}
}

func TestDeliverResponseIsBoundedAndStatusSanitized(t *testing.T) {
	delivery := testDelivery(t)
	for _, test := range []struct {
		name   string
		body   string
		status int
		want   error
	}{
		{name: "rejected", body: "token=top-secret body=https://private.invalid", status: http.StatusBadGateway, want: ErrWebhookRejected},
		{name: "too large", body: strings.Repeat("x", MaxWebhookResponseBytes+1), status: http.StatusOK, want: ErrWebhookResponseTooLarge},
	} {
		t.Run(test.name, func(t *testing.T) {
			channel := newTestChannel(resolverFunc(func(context.Context, string, notification.TargetRef) (ResolvedTarget, error) {
				return ResolvedTarget{URL: "https://8.8.8.8/hook", Secret: []byte("secret")}, nil
			}), roundTripperFunc(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: test.status, Body: io.NopCloser(strings.NewReader(test.body)), Header: make(http.Header)}, nil
			}))
			_, err := channel.Deliver(context.Background(), delivery)
			if !errors.Is(err, test.want) || err.Error() != test.want.Error() {
				t.Fatalf("got %v, want %v", err, test.want)
			}
			if strings.Contains(err.Error(), "top-secret") || strings.Contains(err.Error(), "private.invalid") {
				t.Fatal("response details leaked")
			}
		})
	}
}

func TestDeliverContextAndSecretBounds(t *testing.T) {
	delivery := testDelivery(t)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	channel := newTestChannel(resolverFunc(func(ctx context.Context, _ string, _ notification.TargetRef) (ResolvedTarget, error) {
		return ResolvedTarget{URL: "https://8.8.8.8/hook", Secret: []byte("secret")}, nil
	}), roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return nil, context.Canceled
	}))
	if _, err := channel.Deliver(canceled, delivery); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled context: %v", err)
	}
	channel.resolver = resolverFunc(func(context.Context, string, notification.TargetRef) (ResolvedTarget, error) {
		return ResolvedTarget{URL: "https://8.8.8.8/hook", Secret: []byte(strings.Repeat("s", MaxWebhookSecretBytes+1))}, nil
	})
	if _, err := channel.Deliver(context.Background(), delivery); !errors.Is(err, ErrWebhookSecret) {
		t.Fatalf("oversized secret: %v", err)
	}
}

func TestSecureDialRejectsDNSRebindingCandidates(t *testing.T) {
	dialed := false
	dial := secureDialContext(func(context.Context, string) ([]net.IPAddr, error) {
		return []net.IPAddr{{IP: net.ParseIP("8.8.8.8")}, {IP: net.ParseIP("10.0.0.2")}}, nil
	}, func(context.Context, string, string) (net.Conn, error) {
		dialed = true
		return nil, errors.New("not expected")
	})
	if _, err := dial(context.Background(), "tcp", "example.com:443"); !errors.Is(err, ErrWebhookTransport) {
		t.Fatalf("unexpected dial error: %v", err)
	}
	if dialed {
		t.Fatal("dialer ran after unsafe DNS answer")
	}

	dialedAddress := ""
	dial = secureDialContext(func(context.Context, string) ([]net.IPAddr, error) {
		return []net.IPAddr{{IP: net.ParseIP("8.8.8.8")}}, nil
	}, func(_ context.Context, _, address string) (net.Conn, error) {
		dialedAddress = address
		return nil, errors.New("public dial failed")
	})
	if _, err := dial(context.Background(), "tcp", "example.com:443"); !errors.Is(err, ErrWebhookTransport) {
		t.Fatalf("unexpected public dial error: %v", err)
	}
	if dialedAddress != "8.8.8.8:443" {
		t.Fatalf("did not dial resolved IP: %q", dialedAddress)
	}
}

func TestSecureDialPanicsFailClosed(t *testing.T) {
	lookupPanic := secureDialContext(func(context.Context, string) ([]net.IPAddr, error) {
		panic("resolver secret")
	}, func(context.Context, string, string) (net.Conn, error) { return nil, nil })
	if _, err := lookupPanic(context.Background(), "tcp", "example.com:443"); !errors.Is(err, ErrWebhookPanic) {
		t.Fatalf("lookup panic was not isolated: %v", err)
	}
	dialPanic := secureDialContext(func(context.Context, string) ([]net.IPAddr, error) {
		return []net.IPAddr{{IP: net.ParseIP("8.8.8.8")}}, nil
	}, func(context.Context, string, string) (net.Conn, error) {
		panic("dial secret")
	})
	if _, err := dialPanic(context.Background(), "tcp", "example.com:443"); !errors.Is(err, ErrWebhookPanic) {
		t.Fatalf("dial panic was not isolated: %v", err)
	}
}

type panicReader struct{}

func (panicReader) Read([]byte) (int, error) { panic("response body secret") }

func TestResponseBodyPanicFailsClosed(t *testing.T) {
	channel := newTestChannel(resolverFunc(func(context.Context, string, notification.TargetRef) (ResolvedTarget, error) {
		return ResolvedTarget{URL: "https://8.8.8.8/hook", Secret: []byte("secret")}, nil
	}), roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(panicReader{}), Header: make(http.Header)}, nil
	}))
	_, err := channel.Deliver(context.Background(), testDelivery(t))
	if !errors.Is(err, ErrWebhookPanic) || err.Error() != ErrWebhookPanic.Error() {
		t.Fatalf("response body panic was not isolated: %v", err)
	}
}

func TestNewInstallsNonBypassableHTTPPolicy(t *testing.T) {
	channel, err := New(resolverFunc(func(context.Context, string, notification.TargetRef) (ResolvedTarget, error) {
		return ResolvedTarget{}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	transport, ok := channel.client.Transport.(*http.Transport)
	if !ok || transport.Proxy != nil || transport.DialContext == nil || transport.MaxConnsPerHost != 16 || transport.MaxResponseHeaderBytes != 64<<10 {
		t.Fatalf("production transport policy is not direct/public-IP-only: %#v", channel.client.Transport)
	}
	if err := channel.client.CheckRedirect(nil, nil); !errors.Is(err, http.ErrUseLastResponse) {
		t.Fatalf("redirect policy not fail-closed: %v", err)
	}
	transport, ok = channel.client.Transport.(*http.Transport)
	if !ok || transport.TLSClientConfig == nil || transport.TLSClientConfig.MinVersion != tls.VersionTLS12 {
		t.Fatalf("production TLS policy is not TLS 1.2+: %#v", transport.TLSClientConfig)
	}
}

func TestInjectedClientDoesNotFollowRedirects(t *testing.T) {
	calls := 0
	channel := newTestChannel(resolverFunc(func(context.Context, string, notification.TargetRef) (ResolvedTarget, error) {
		return ResolvedTarget{URL: "https://8.8.8.8/hook", Secret: []byte("secret")}, nil
	}), roundTripperFunc(func(request *http.Request) (*http.Response, error) {
		calls++
		return &http.Response{
			StatusCode: http.StatusFound,
			Header:     http.Header{"Location": []string{"https://127.0.0.1/private"}},
			Body:       io.NopCloser(strings.NewReader("redirect")),
			Request:    request,
		}, nil
	}))
	_, err := channel.Deliver(context.Background(), testDelivery(t))
	if !errors.Is(err, ErrWebhookRejected) {
		t.Fatalf("redirect response was not classified as rejection: %v", err)
	}
	if calls != 1 {
		t.Fatalf("redirect caused additional request(s): %d", calls)
	}
}

func TestChannelDescriptorIsStable(t *testing.T) {
	channel := &Channel{}
	descriptor := channel.Descriptor()
	descriptor.Capabilities[0] = "mutated"
	if got := channel.Descriptor().Capabilities[0]; got != notification.CapabilityDeliver {
		t.Fatalf("descriptor leaked mutable capabilities: %q", got)
	}
}
