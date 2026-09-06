package settings

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/cc-auto-agent/harness-core/pkg/adapter/httpapi/jsonbody"
)

func TestGetResponseOmitsUnfoundValueAndKeepsFoundEmptyValue(t *testing.T) {
	unfound, err := json.Marshal(GetResponse{Key: "llm", Found: false})
	if err != nil {
		t.Fatal(err)
	}
	if string(unfound) != `{"key":"llm","found":false}` {
		t.Fatalf("unfound wire=%s", unfound)
	}
	empty := ""
	found, err := json.Marshal(GetResponse{Key: "llm", Found: true, Value: &empty})
	if err != nil {
		t.Fatal(err)
	}
	if string(found) != `{"key":"llm","found":true,"value":""}` {
		t.Fatalf("found empty wire=%s", found)
	}
}

func TestGetResponseMarksSecretReadWithoutValue(t *testing.T) {
	encoded, err := json.Marshal(GetResponse{
		Key: "llm", Found: true, Redacted: true, WriteOnly: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	wire := string(encoded)
	if wire != `{"key":"llm","found":true,"redacted":true,"write_only":true}` {
		t.Fatalf("secret wire=%s", wire)
	}
	if strings.Contains(wire, "value") || strings.Contains(wire, "prefix") {
		t.Fatalf("secret wire disclosed value state: %s", wire)
	}
}

func TestPutRequestUsesSharedForwardCompatibleDecoder(t *testing.T) {
	request := httptest.NewRequest(http.MethodPut, "/", strings.NewReader(`{"value":{"model":"fixture"},"future_client_field":true}`))
	var target PutRequest
	if err := jsonbody.Decode(httptest.NewRecorder(), request, 1024, &target); err != nil {
		t.Fatalf("decode request: %v", err)
	}
	if target.Value == nil {
		t.Fatal("value was not marked present")
	}
	if got := string(*target.Value); got != `{"model":"fixture"}` {
		t.Fatalf("raw value=%s", got)
	}

	wrongType := httptest.NewRequest(http.MethodPut, "/", strings.NewReader(`{"value":`))
	if err := jsonbody.Decode(httptest.NewRecorder(), wrongType, 1024, &target); err == nil {
		t.Fatal("invalid known value was accepted")
	}
}

func TestPutRequestTracksMissingNullAndExplicitEmptyValue(t *testing.T) {
	for name, body := range map[string]string{
		"missing": `{}`,
		"null":    `{"value":null}`,
		"empty":   `{"value":""}`,
	} {
		t.Run(name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPut, "/", strings.NewReader(body))
			var target PutRequest
			if err := jsonbody.Decode(httptest.NewRecorder(), request, 1024, &target); err != nil {
				t.Fatalf("decode: %v", err)
			}
			switch name {
			case "missing", "null":
				if target.Value != nil {
					t.Fatalf("value should be absent: %#v", target.Value)
				}
			case "empty":
				if target.Value == nil || string(*target.Value) != `""` {
					t.Fatalf("explicit empty value=%v", target.Value)
				}
			}
		})
	}
}
