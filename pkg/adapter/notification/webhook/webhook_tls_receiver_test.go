package webhook

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cc-auto-agent/harness-core/pkg/app/notification"
)

func TestDeliverPostsSignedPayloadToLocalTLSReceiver(t *testing.T) {
	var (
		mu       sync.Mutex
		body     []byte
		header   http.Header
		method   string
		requests atomic.Int32
	)
	receiver := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		got, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		mu.Lock()
		body = append([]byte(nil), got...)
		header = r.Header.Clone()
		method = r.Method
		mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
	}))
	defer receiver.Close()

	secret := []byte("local-test-webhook-secret")
	client := receiver.Client()
	transport := client.Transport.(*http.Transport).Clone()
	dialer := &net.Dialer{}
	transport.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
		return dialer.DialContext(ctx, network, receiver.Listener.Addr().String())
	}
	client.Transport = transport
	channel := &Channel{
		resolver: resolverFunc(func(context.Context, string, notification.TargetRef) (ResolvedTarget, error) {
			return ResolvedTarget{URL: "https://example.com/hook", Secret: secret}, nil
		}),
		client: client,
	}
	delivery := testDelivery(t)

	receipt, err := channel.Deliver(context.Background(), delivery)
	if err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if receipt.Status != notification.ReceiptAccepted || receipt.DeliveryID != delivery.IdempotencyKey {
		t.Fatalf("receipt=%+v", receipt)
	}
	if requests.Load() != 1 {
		t.Fatalf("receiver requests=%d, want 1", requests.Load())
	}
	if !allZero(secret) {
		t.Fatal("resolved secret was not cleared")
	}

	mu.Lock()
	gotBody := append([]byte(nil), body...)
	gotHeader := header.Clone()
	gotMethod := method
	mu.Unlock()
	if gotMethod != http.MethodPost {
		t.Fatalf("method=%q, want POST", gotMethod)
	}
	if gotHeader.Get("Content-Type") != "application/json" {
		t.Fatalf("content type=%q", gotHeader.Get("Content-Type"))
	}
	if gotHeader.Get("Idempotency-Key") != delivery.IdempotencyKey {
		t.Fatalf("idempotency key=%q", gotHeader.Get("Idempotency-Key"))
	}
	timestamp := gotHeader.Get("X-Notification-Timestamp")
	if _, err := strconv.ParseInt(timestamp, 10, 64); err != nil {
		t.Fatalf("timestamp=%q, want Unix seconds: %v", timestamp, err)
	}
	if want := independentSignature([]byte("local-test-webhook-secret"), timestamp, gotBody); gotHeader.Get("X-Notification-Signature") != want {
		t.Fatalf("signature=%q, want %q", gotHeader.Get("X-Notification-Signature"), want)
	}

	var payload struct {
		Text     string            `json:"text"`
		Format   string            `json:"format"`
		Metadata map[string]string `json:"metadata"`
	}
	if err := json.Unmarshal(gotBody, &payload); err != nil {
		t.Fatalf("decode receiver body: %v", err)
	}
	if payload.Text != delivery.Text || payload.Format != delivery.Format || !reflect.DeepEqual(payload.Metadata, delivery.Metadata) {
		t.Fatalf("receiver payload=%+v, want delivery=%+v", payload, delivery)
	}
}

// independentSignature intentionally does not call the production signer: the
// receiver must verify the documented HMAC-SHA256(timestamp + "." + body)
// wire contract independently.
func independentSignature(secret []byte, timestamp string, body []byte) string {
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte(timestamp))
	_, _ = mac.Write([]byte("."))
	_, _ = mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func allZero(value []byte) bool {
	for _, b := range value {
		if b != 0 {
			return false
		}
	}
	return true
}
