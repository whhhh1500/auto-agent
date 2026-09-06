// Package webhook implements the provider-neutral notification Channel over
// a preconfigured HTTPS webhook endpoint.
//
// The model only supplies an opaque notification.TargetRef. Endpoint and
// secret resolution belongs to the injected TargetResolver. Production
// construction installs a direct, public-IP-only dialer and never permits a
// caller to replace that SSRF boundary.
package webhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/cc-auto-agent/harness-core/pkg/app/notification"
)

const (
	ChannelID      = "webhook"
	ChannelVersion = "1"

	// Request and response limits are independent. The request limit bounds
	// JSON construction; the response limit is enforced while discarding the
	// provider body and never allocates the body in memory.
	MaxWebhookBodyBytes     = 128 << 10
	MaxWebhookResponseBytes = 16 << 10
	MaxWebhookEndpointBytes = 2048
	MaxWebhookSecretBytes   = 4096
)

var (
	ErrInvalidTarget           = errors.New("invalid webhook target")
	ErrTargetResolution        = errors.New("webhook target resolution failed")
	ErrWebhookSecret           = errors.New("webhook target secret unavailable")
	ErrWebhookTransport        = errors.New("webhook transport failed")
	ErrWebhookRejected         = errors.New("webhook delivery rejected")
	ErrWebhookResponseTooLarge = errors.New("webhook response too large")
	ErrWebhookPanic            = errors.New("webhook callback panicked")
)

// TargetResolver resolves an opaque target for one tenant. Resolve transfers
// ownership of Secret to the caller; Channel clears it before returning.
// Implementations must return a configured endpoint and must not derive either
// value from notification.Delivery.Text or Delivery.Metadata.
type TargetResolver interface {
	Resolve(context.Context, string, notification.TargetRef) (ResolvedTarget, error)
}

// ResolvedTarget is the private configuration needed for one delivery.
// Secret is caller-owned after Resolve returns and is zeroed by Channel.
type ResolvedTarget struct {
	URL    string
	Secret []byte
}

// Channel is a notification channel backed by a preconfigured HTTPS webhook.
type Channel struct {
	resolver TargetResolver
	client   *http.Client
}

// New constructs a production-safe webhook channel. The HTTP transport has
// no proxy, follows no redirects, and resolves every new connection to a
// validated public IP before dialing that exact IP.
func New(resolver TargetResolver) (*Channel, error) {
	if resolver == nil {
		return nil, ErrTargetResolution
	}
	client, err := newSecureHTTPClient()
	if err != nil {
		return nil, err
	}
	return &Channel{resolver: resolver, client: client}, nil
}

func (c *Channel) Descriptor() notification.Descriptor {
	return notification.Descriptor{
		Ref:          notification.ChannelRef{ID: ChannelID, Version: ChannelVersion},
		Capabilities: []notification.Capability{notification.CapabilityDeliver},
	}
}

// Deliver submits one POST attempt. Provider status/body details and callback
// errors are intentionally reduced to fixed classifications; retry and
// deduplication semantics belong to upper-layer policy and the receiver.
func (c *Channel) Deliver(ctx context.Context, delivery notification.Delivery) (receipt notification.Receipt, err error) {
	if ctx == nil {
		return notification.Receipt{}, context.Canceled
	}
	if err := notification.ValidateDelivery(delivery); err != nil {
		return notification.Receipt{}, err
	}
	if c == nil || c.resolver == nil || c.client == nil {
		return notification.Receipt{}, ErrTargetResolution
	}

	target, err := safeResolve(c.resolver, ctx, delivery.TenantID, delivery.Target)
	defer clear(target.Secret)
	if err != nil {
		return notification.Receipt{}, classifyCallbackError(err, ErrTargetResolution)
	}

	endpoint, err := validateEndpoint(target.URL)
	if err != nil {
		return notification.Receipt{}, ErrInvalidTarget
	}
	if len(target.Secret) == 0 || len(target.Secret) > MaxWebhookSecretBytes {
		return notification.Receipt{}, ErrWebhookSecret
	}

	body, err := json.Marshal(struct {
		Text     string            `json:"text"`
		Format   string            `json:"format"`
		Metadata map[string]string `json:"metadata,omitempty"`
	}{Text: delivery.Text, Format: delivery.Format, Metadata: delivery.Metadata})
	if err != nil || len(body) > MaxWebhookBodyBytes {
		return notification.Receipt{}, ErrWebhookTransport
	}

	timestamp := strconv.FormatInt(time.Now().UTC().Unix(), 10)
	signature := sign(target.Secret, timestamp, body)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return notification.Receipt{}, ErrInvalidTarget
	}
	// bytes.Reader makes net/http populate GetBody. Clearing it prevents the
	// transport from replaying this POST after a connection failure; delivery
	// Retries and deduplication semantics belong to upper-layer policy and the
	// receiver; this adapter only prevents transport-level body replay.
	request.GetBody = nil
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", delivery.IdempotencyKey)
	request.Header.Set("X-Notification-Timestamp", timestamp)
	request.Header.Set("X-Notification-Signature", "sha256="+signature)

	response, err := safeDo(c.client, request)
	if err != nil {
		if response != nil && response.Body != nil {
			safeClose(response.Body)
		}
		return notification.Receipt{}, classifyCallbackError(err, ErrWebhookTransport)
	}
	if response == nil || response.Body == nil {
		return notification.Receipt{}, ErrWebhookTransport
	}
	defer safeClose(response.Body)
	if err := safeDiscardResponse(response.Body); err != nil {
		if errors.Is(err, ErrWebhookResponseTooLarge) {
			return notification.Receipt{}, err
		}
		return notification.Receipt{}, classifyCallbackError(err, ErrWebhookTransport)
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return notification.Receipt{}, ErrWebhookRejected
	}

	return notification.Receipt{
		Channel:    c.Descriptor().Ref,
		DeliveryID: delivery.IdempotencyKey,
		Status:     notification.ReceiptAccepted,
		Metadata:   map[string]string{"class": "accepted"},
	}, nil
}

func sign(secret []byte, timestamp string, body []byte) string {
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte(timestamp))
	_, _ = mac.Write([]byte("."))
	_, _ = mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

func safeResolve(resolver TargetResolver, ctx context.Context, tenant string, target notification.TargetRef) (resolved ResolvedTarget, err error) {
	defer func() {
		if recover() != nil {
			resolved = ResolvedTarget{}
			err = ErrWebhookPanic
		}
	}()
	return resolver.Resolve(ctx, tenant, target)
}

func safeDo(client *http.Client, request *http.Request) (response *http.Response, err error) {
	defer func() {
		if recover() != nil {
			response = nil
			err = ErrWebhookPanic
		}
	}()
	return client.Do(request)
}

func classifyCallbackError(err, fallback error) error {
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	if errors.Is(err, ErrWebhookPanic) {
		return ErrWebhookPanic
	}
	return fallback
}

func discardResponse(body io.Reader) error {
	read := io.LimitReader(body, MaxWebhookResponseBytes+1)
	n, err := io.Copy(io.Discard, read)
	if n > MaxWebhookResponseBytes {
		return ErrWebhookResponseTooLarge
	}
	return err
}

func safeDiscardResponse(body io.Reader) (err error) {
	defer func() {
		if recover() != nil {
			err = ErrWebhookPanic
		}
	}()
	return discardResponse(body)
}

func safeClose(closer io.Closer) {
	defer func() { _ = recover() }()
	_ = closer.Close()
}

func validateEndpoint(raw string) (url.URL, error) {
	if raw == "" || len(raw) > MaxWebhookEndpointBytes || !utf8Safe(raw) {
		return url.URL{}, ErrInvalidTarget
	}
	parsed, err := url.Parse(raw)
	if err != nil || strings.ToLower(parsed.Scheme) != "https" || parsed.Host == "" || parsed.Opaque != "" || parsed.User != nil || parsed.Fragment != "" {
		return url.URL{}, ErrInvalidTarget
	}
	if strings.ContainsAny(parsed.Host, "\\\r\n\t") || strings.Contains(parsed.Hostname(), "%") {
		return url.URL{}, ErrInvalidTarget
	}
	if containsSecretQuery(parsed.RawQuery) {
		return url.URL{}, ErrInvalidTarget
	}
	host := parsed.Hostname()
	if host == "" {
		return url.URL{}, ErrInvalidTarget
	}
	if ip := net.ParseIP(host); ip != nil && !isPublicIP(ip) {
		return url.URL{}, ErrInvalidTarget
	}
	return *parsed, nil
}

func containsSecretQuery(rawQuery string) bool {
	if rawQuery == "" {
		return false
	}
	for _, item := range strings.Split(rawQuery, "&") {
		key := item
		if index := strings.IndexByte(key, '='); index >= 0 {
			key = key[:index]
		}
		decoded, err := url.QueryUnescape(key)
		if err != nil {
			return true
		}
		lower := strings.ToLower(decoded)
		for _, marker := range []string{"secret", "token", "password", "credential", "authorization", "api_key", "apikey", "key"} {
			if strings.Contains(lower, marker) {
				return true
			}
		}
	}
	return false
}

func isPublicIP(ip net.IP) bool {
	if ip == nil || !ip.IsGlobalUnicast() || ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsUnspecified() || ip.IsMulticast() {
		return false
	}
	v4 := ip.To4()
	if v4 == nil {
		return !isSpecialIPv6(ip)
	}
	if v4[0] == 0 || (v4[0] == 100 && v4[1] >= 64 && v4[1] <= 127) ||
		(v4[0] == 192 && v4[1] == 0 && v4[2] == 0) ||
		(v4[0] == 192 && v4[1] == 0 && v4[2] == 2) ||
		(v4[0] == 192 && v4[1] == 88 && v4[2] == 99) ||
		(v4[0] == 198 && v4[1] >= 18 && v4[1] <= 19) ||
		(v4[0] == 198 && v4[1] == 51 && v4[2] == 100) ||
		(v4[0] == 203 && v4[1] == 0 && v4[2] == 113) {
		return false
	}
	return true
}

func isSpecialIPv6(ip net.IP) bool {
	for _, network := range specialIPv6Networks {
		if network.Contains(ip) {
			return true
		}
	}
	return false
}

var specialIPv6Networks = []net.IPNet{
	{IP: net.ParseIP("2001::"), Mask: net.CIDRMask(23, 128)},     // IETF special-use/transition space.
	{IP: net.ParseIP("2001:2::"), Mask: net.CIDRMask(48, 128)},   // benchmarking.
	{IP: net.ParseIP("2001:10::"), Mask: net.CIDRMask(28, 128)},  // ORCHID.
	{IP: net.ParseIP("2001:20::"), Mask: net.CIDRMask(28, 128)},  // ORCHIDv2.
	{IP: net.ParseIP("2001:db8::"), Mask: net.CIDRMask(32, 128)}, // documentation.
	{IP: net.ParseIP("2002::"), Mask: net.CIDRMask(16, 128)},     // 6to4 transition.
	{IP: net.ParseIP("3fff::"), Mask: net.CIDRMask(20, 128)},     // documentation.
}

func utf8Safe(value string) bool {
	if !utf8.ValidString(value) {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) || unicode.In(r, unicode.Cf, unicode.Cs, unicode.Co) {
			return false
		}
	}
	return true
}

func newSecureHTTPClient() (*http.Client, error) {
	dialer := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	transport := &http.Transport{
		Proxy:                  nil,
		TLSClientConfig:        &tls.Config{MinVersion: tls.VersionTLS12},
		DialContext:            secureDialContext(net.DefaultResolver.LookupIPAddr, dialer.DialContext),
		ForceAttemptHTTP2:      true,
		MaxConnsPerHost:        16,
		MaxIdleConns:           16,
		MaxIdleConnsPerHost:    4,
		IdleConnTimeout:        90 * time.Second,
		TLSHandshakeTimeout:    10 * time.Second,
		ResponseHeaderTimeout:  30 * time.Second,
		MaxResponseHeaderBytes: 64 << 10,
		ExpectContinueTimeout:  1 * time.Second,
	}
	return &http.Client{
		Transport: transport,
		Timeout:   35 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}, nil
}

func secureDialContext(lookup func(context.Context, string) ([]net.IPAddr, error), dial func(context.Context, string, string) (net.Conn, error)) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(address)
		if err != nil || host == "" || port == "" {
			return nil, ErrWebhookTransport
		}
		var addresses []net.IPAddr
		if literal := net.ParseIP(strings.Trim(host, "[]")); literal != nil {
			addresses = []net.IPAddr{{IP: literal}}
		} else {
			addresses, err = safeLookup(lookup, ctx, host)
			if err != nil || len(addresses) == 0 {
				if err != nil {
					return nil, err
				}
				return nil, ErrWebhookTransport
			}
		}
		for _, resolved := range addresses {
			if !isPublicIP(resolved.IP) {
				return nil, ErrWebhookTransport
			}
		}
		var lastErr error
		for _, resolved := range addresses {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			conn, err := safeDial(dial, ctx, network, net.JoinHostPort(resolved.IP.String(), port))
			if err == nil {
				return conn, nil
			}
			if errors.Is(err, ErrWebhookPanic) {
				return nil, err
			}
			lastErr = err
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if lastErr != nil {
			return nil, ErrWebhookTransport
		}
		return nil, ErrWebhookTransport
	}
}

func safeLookup(lookup func(context.Context, string) ([]net.IPAddr, error), ctx context.Context, host string) (addresses []net.IPAddr, err error) {
	defer func() {
		if recover() != nil {
			addresses = nil
			err = ErrWebhookPanic
		}
	}()
	return lookup(ctx, host)
}

func safeDial(dial func(context.Context, string, string) (net.Conn, error), ctx context.Context, network, address string) (conn net.Conn, err error) {
	defer func() {
		if recover() != nil {
			conn = nil
			err = ErrWebhookPanic
		}
	}()
	return dial(ctx, network, address)
}
