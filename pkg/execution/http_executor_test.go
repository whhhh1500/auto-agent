package execution

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	core "github.com/whhhh1500/auto-agent/pkg/core"
)

func TestHTTPExecutorDeniesPrivateNetworkByDefault(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("private endpoint must not be reached")
	}))
	defer server.Close()
	result, err := (HTTPExecutor{}).Execute(context.Background(), core.ExecutionSpec{Entrypoint: server.URL}, core.CapabilityRequest{})
	if err != nil || result.OK || result.Metadata["code"] != "network_denied" {
		t.Fatalf("private destination was not denied: %#v %v", result, err)
	}
}

func TestPublicHTTPClientRefusesRedirects(t *testing.T) {
	client := newPublicHTTPClient()
	request, err := http.NewRequest(http.MethodGet, "https://example.com", nil)
	if err != nil {
		t.Fatal(err)
	}
	redirect, _ := http.NewRequest(http.MethodGet, "http://127.0.0.1/internal", nil)
	if err := client.CheckRedirect(redirect, []*http.Request{request}); err != http.ErrUseLastResponse {
		t.Fatalf("redirect was not refused: %v", err)
	}
}

func TestValidatePublicHTTPURLRejectsUserInfo(t *testing.T) {
	if err := ValidatePublicHTTPURL("https://user:secret@example.com/api"); err == nil {
		t.Fatal("URL user information must be rejected")
	}
}

func TestHTTPExecutorMergesExistingQuery(t *testing.T) {
	var rawQuery string
	var idempotencyKey string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rawQuery = r.URL.RawQuery
		idempotencyKey = r.Header.Get("Idempotency-Key")
		_, _ = w.Write([]byte("ok"))
	}))
	defer server.Close()
	executor := HTTPExecutor{Client: server.Client(), AllowPrivateNetwork: true}
	result, err := executor.Execute(context.Background(), core.ExecutionSpec{
		Entrypoint: server.URL + "?fixed=yes", Method: http.MethodGet,
	}, core.CapabilityRequest{IdempotencyKey: "call-market-quote", Args: map[string]any{"symbol": "BTC"}})
	if err != nil || !result.OK || rawQuery != "fixed=yes&symbol=BTC" || idempotencyKey != "call-market-quote" {
		t.Fatalf("query/header propagation failed: query=%q key=%q result=%#v err=%v", rawQuery, idempotencyKey, result, err)
	}
}

func TestValidateHTTPMethodRejectsConnect(t *testing.T) {
	if err := ValidateHTTPMethod(http.MethodConnect); err == nil {
		t.Fatal("CONNECT must not be allowed for dynamic HTTP capabilities")
	}
}

func TestValidatePublicHTTPURLRejectsSecretQuery(t *testing.T) {
	if err := ValidatePublicHTTPURL("https://example.com/api?api_key=secret"); err == nil {
		t.Fatal("secret-bearing endpoint query was accepted")
	}
}

type crlfCredential struct{}

func (crlfCredential) Resolve(context.Context, core.CredentialRef) (core.CredentialValue, error) {
	return core.CredentialValue{Value: "secret\r\nX-Injected: 1"}, nil
}

func TestHTTPExecutorRejectsNewlineHeaders(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("newline header must not be sent")
	}))
	defer server.Close()
	executor := HTTPExecutor{Client: server.Client(), AllowPrivateNetwork: true}
	result, err := executor.Execute(context.Background(), core.ExecutionSpec{
		Entrypoint: server.URL,
		Headers:    map[string]string{"X-Trace": "ok\r\nX-Injected: 1"},
	}, core.CapabilityRequest{})
	if err != nil || result.OK || result.Metadata["code"] != "invalid_execution" {
		t.Fatalf("newline header was sent: %#v err=%v", result, err)
	}
}

func TestHTTPExecutorRejectsCredentialNewline(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("injected credential header must not be sent")
	}))
	defer server.Close()
	executor := HTTPExecutor{Client: server.Client(), AllowPrivateNetwork: true}
	result, err := executor.Execute(context.Background(), core.ExecutionSpec{
		Entrypoint: server.URL,
		Headers:    map[string]string{"Authorization": "$credential:secret.token"},
	}, core.CapabilityRequest{Context: core.CapabilityContext{Credentials: crlfCredential{}}})
	if err != nil || result.OK || result.Metadata["code"] != "invalid_execution" {
		t.Fatalf("credential newline was sent: %#v err=%v", result, err)
	}
}

func TestValidateHTTPHeader(t *testing.T) {
	if err := ValidateHTTPHeader("X-Trace", "ok"); err != nil {
		t.Fatal(err)
	}
	if err := ValidateHTTPHeader("X-Trace: bad", "ok"); err == nil {
		t.Fatal("colon in header name was accepted")
	}
	if err := ValidateHTTPHeader("", "ok"); err == nil {
		t.Fatal("empty header name was accepted")
	}
	if err := ValidateHTTPHeader("X-Trace", "ok\x00"); err == nil {
		t.Fatal("NUL header value was accepted")
	}
	if err := ValidateHTTPHeader(strings.Repeat("A", maxHTTPHeaderNameBytes), "ok"); err != nil {
		t.Fatalf("header name at byte limit rejected: %v", err)
	}
	if err := ValidateHTTPHeader(strings.Repeat("A", maxHTTPHeaderNameBytes+1), "ok"); err == nil {
		t.Fatal("oversized header name was accepted")
	}
	if err := ValidateHTTPHeader("X-Trace", strings.Repeat("v", maxHTTPHeaderValueBytes)); err != nil {
		t.Fatalf("header value at byte limit rejected: %v", err)
	}
	if err := ValidateHTTPHeader("X-Trace", strings.Repeat("v", maxHTTPHeaderValueBytes+1)); err == nil {
		t.Fatal("oversized header value was accepted")
	}
}

func TestHTTPExecutorRejectsOversizedHeaderList(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("oversized header list must not be sent")
	}))
	defer server.Close()
	executor := HTTPExecutor{Client: server.Client(), AllowPrivateNetwork: true}
	headers := map[string]string{}
	for i := 0; i < maxHTTPHeaders+1; i++ {
		headers[fmt.Sprintf("X-H-%d", i)] = "ok"
	}
	result, err := executor.Execute(context.Background(), core.ExecutionSpec{
		Entrypoint: server.URL, Headers: headers,
	}, core.CapabilityRequest{})
	if err != nil || result.OK || result.Metadata["code"] != "invalid_execution" {
		t.Fatalf("oversized header list was accepted: %#v err=%v", result, err)
	}
	if !strings.Contains(result.Content, "http header list exceeds") {
		t.Fatalf("unexpected denial: %#v", result)
	}
}

func TestHTTPExecutorAcceptsHeaderListAtCap(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	defer server.Close()
	executor := HTTPExecutor{Client: server.Client(), AllowPrivateNetwork: true}
	headers := map[string]string{}
	for i := 0; i < maxHTTPHeaders; i++ {
		headers[fmt.Sprintf("X-H-%d", i)] = "ok"
	}
	result, err := executor.Execute(context.Background(), core.ExecutionSpec{
		Entrypoint: server.URL, Headers: headers,
	}, core.CapabilityRequest{})
	if err != nil || !result.OK {
		t.Fatalf("header list at cap rejected: %#v err=%v", result, err)
	}
}

func TestHTTPExecutorRejectsNestedQueryArgs(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("nested query args must not be sent")
	}))
	defer server.Close()
	executor := HTTPExecutor{Client: server.Client(), AllowPrivateNetwork: true}
	result, err := executor.Execute(context.Background(), core.ExecutionSpec{
		Entrypoint: server.URL, Method: http.MethodGet,
	}, core.CapabilityRequest{Args: map[string]any{"filter": map[string]any{"symbol": "BTC"}}})
	if err != nil || result.OK || result.Metadata["code"] != "invalid_execution" {
		t.Fatalf("nested query arg was sent: %#v err=%v", result, err)
	}
}

func TestHTTPExecutorEncodesNumericQueryArgs(t *testing.T) {
	var rawQuery string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rawQuery = r.URL.RawQuery
		_, _ = w.Write([]byte("ok"))
	}))
	defer server.Close()
	executor := HTTPExecutor{Client: server.Client(), AllowPrivateNetwork: true}
	result, err := executor.Execute(context.Background(), core.ExecutionSpec{
		Entrypoint: server.URL, Method: http.MethodGet,
	}, core.CapabilityRequest{Args: map[string]any{"limit": float64(2), "ok": true}})
	if err != nil || !result.OK {
		t.Fatalf("numeric query args failed: %#v err=%v", result, err)
	}
	if rawQuery != "limit=2&ok=true" && rawQuery != "ok=true&limit=2" {
		t.Fatalf("scalar query encoding wrong: %q", rawQuery)
	}
}

func TestEncodeHTTPQueryURLRejectsInvalidNames(t *testing.T) {
	parsed, err := url.Parse("https://example.com/q")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := encodeHTTPQueryURL(parsed, map[string]any{"": "v"}); err == nil {
		t.Fatal("empty query name was accepted")
	}
}

func TestHTTPExecutorRejectsOversizedPOSTBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("oversized body must not be sent")
	}))
	defer server.Close()
	executor := HTTPExecutor{Client: server.Client(), AllowPrivateNetwork: true}
	result, err := executor.Execute(context.Background(), core.ExecutionSpec{
		Entrypoint: server.URL, Method: http.MethodPost,
	}, core.CapabilityRequest{Args: map[string]any{"blob": strings.Repeat("x", maxHTTPRequestBodyBytes)}})
	if err != nil || result.OK || result.Metadata["code"] != "invalid_execution" {
		t.Fatalf("oversized http body was accepted: %#v err=%v", result, err)
	}
	if !strings.Contains(result.Content, "http request body exceeds") {
		t.Fatalf("unexpected denial: %#v", result)
	}
}
