package storage

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/whhhh1500/auto-agent/pkg/adapter/httpapi/jsonbody"
)

func TestPutRequestKeepsSecretPresenceDistinctFromEmpty(t *testing.T) {
	for name, body := range map[string]string{
		"omitted": `{"type":"s3","future":true}`,
		"empty":   `{"type":"s3","secret_key":""}`,
		"value":   `{"type":"s3","secret_key":"fixture-secret"}`,
	} {
		t.Run(name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPut, "/", strings.NewReader(body))
			var target PutRequest
			if err := jsonbody.Decode(httptest.NewRecorder(), request, 1024, &target); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if name == "omitted" && target.SecretKey.Present {
				t.Fatalf("omitted secret marked present: %#v", target.SecretKey)
			}
			if name == "empty" && (!target.SecretKey.Present || target.SecretKey.Value != "") {
				t.Fatalf("empty secret presence=%#v", target.SecretKey)
			}
			if name == "value" && (!target.SecretKey.Present || target.SecretKey.Value != "fixture-secret") {
				t.Fatalf("value secret presence=%#v", target.SecretKey)
			}
		})
	}
}

func TestPutRequestRejectsNullSecret(t *testing.T) {
	request := httptest.NewRequest(http.MethodPut, "/", strings.NewReader(`{"type":"s3","secret_key":null}`))
	var target PutRequest
	if err := jsonbody.Decode(httptest.NewRecorder(), request, 1024, &target); err == nil || !strings.Contains(err.Error(), "secret_key must be a string") {
		t.Fatalf("null secret error=%v", err)
	}
}

func TestPutRequestKeepsDisableConditionalWritesPresenceDistinctFromFalse(t *testing.T) {
	for name, body := range map[string]string{
		"omitted": `{"type":"file"}`,
		"false":   `{"type":"file","disable_conditional_writes":false}`,
		"true":    `{"type":"file","disable_conditional_writes":true}`,
	} {
		t.Run(name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPut, "/", strings.NewReader(body))
			var target PutRequest
			if err := jsonbody.Decode(httptest.NewRecorder(), request, 1024, &target); err != nil {
				t.Fatalf("decode: %v", err)
			}
			wantPresent := name != "omitted"
			wantValue := name == "true"
			if target.DisableConditionalWrites.Present != wantPresent || target.DisableConditionalWrites.Value != wantValue {
				t.Fatalf("disable_conditional_writes = %#v, want present=%t value=%t", target.DisableConditionalWrites, wantPresent, wantValue)
			}
		})
	}
	request := httptest.NewRequest(http.MethodPut, "/", strings.NewReader(`{"type":"file","disable_conditional_writes":null}`))
	var target PutRequest
	if err := jsonbody.Decode(httptest.NewRecorder(), request, 1024, &target); err == nil || !strings.Contains(err.Error(), "disable_conditional_writes must be a boolean") {
		t.Fatalf("null conditional-writes error=%v", err)
	}
}

func TestGetResponseHasNoSecretField(t *testing.T) {
	disableConditionalWrites := true
	response := GetResponse{
		Key: "storage.sessions", Found: true,
		Config: Configuration{
			Type: "file", Path: "data/sessions", Source: "db", Status: "active", DesiredRevision: "rev_desired",
			DisableConditionalWrites: &disableConditionalWrites, HasSecret: true, SecretPreview: "pre••••••••tail",
		},
		Active: "applies on restart", DesiredType: "file", ActiveType: "embedded",
		DesiredRevision: "rev_desired", ActiveRevision: "rev_active", RestartPending: true,
	}
	encoded, err := json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	wire := string(encoded)
	if !strings.Contains(wire, `"has_secret":true`) {
		t.Fatalf("missing secret state: %s", wire)
	}
	if !strings.Contains(wire, `"secret_preview":"pre••••••••tail"`) {
		t.Fatalf("missing fixed preview: %s", wire)
	}
	for _, want := range []string{
		`"path":"data/sessions"`, `"config_source":"db"`, `"config_status":"active"`,
		`"disable_conditional_writes":true`, `"desired_type":"file"`,
		`"active_type":"embedded"`, `"desired_revision":"rev_desired"`,
		`"active_revision":"rev_active"`, `"restart_pending":true`,
	} {
		if !strings.Contains(wire, want) {
			t.Fatalf("missing additive storage state %q: %s", want, wire)
		}
	}
	for _, forbidden := range []string{"secret_key", "fixture-secret", "prefix", "suffix"} {
		if strings.Contains(wire, forbidden) {
			t.Fatalf("GET storage wire exposed %q: %s", forbidden, wire)
		}
	}
}
