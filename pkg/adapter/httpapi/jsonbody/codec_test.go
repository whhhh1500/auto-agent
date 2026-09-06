package jsonbody

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDecodeIgnoresUnknownTopLevelField(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"message":"ok","future_client_field":true}`))
	var target struct {
		Message string `json:"message"`
	}
	if err := Decode(httptest.NewRecorder(), request, 1024, &target); err != nil {
		t.Fatalf("decode forward-compatible field: %v", err)
	}
	if target.Message != "ok" {
		t.Fatalf("decoded target=%#v", target)
	}
}

func TestDecodeRejectsNestedUnknownField(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"nested":{"known":"ok","unexpected":true}}`))
	var target struct {
		Nested struct {
			Known string `json:"known"`
		} `json:"nested"`
	}
	err := Decode(httptest.NewRecorder(), request, 1024, &target)
	if err == nil || !strings.Contains(err.Error(), `json: unknown field "unexpected"`) {
		t.Fatalf("nested unknown error=%v", err)
	}
}

func TestDecodeRejectsWrongKnownFieldType(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"message":false}`))
	var target struct {
		Message string `json:"message"`
	}
	err := Decode(httptest.NewRecorder(), request, 1024, &target)
	if err == nil || !strings.Contains(err.Error(), "cannot unmarshal bool into Go struct field") {
		t.Fatalf("wrong type error=%v", err)
	}
}

func TestDecodeRejectsTrailingJSONValue(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"message":"a"} {"message":"b"}`))
	var target struct {
		Message string `json:"message"`
	}
	err := Decode(httptest.NewRecorder(), request, 1024, &target)
	if err == nil || err.Error() != "request body contains multiple JSON values" {
		t.Fatalf("trailing JSON error=%v", err)
	}
}

func TestDecodeRejectsOversizeBody(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"message":"too long"}`))
	var target struct {
		Message string `json:"message"`
	}
	err := Decode(httptest.NewRecorder(), request, 8, &target)
	if err == nil || !strings.Contains(err.Error(), "http: request body too large") {
		t.Fatalf("oversize error=%v", err)
	}
}

func TestDecodeOptionalAcceptsEmptyBody(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/", nil)
	var target struct {
		Message string `json:"message"`
	}
	if err := DecodeOptional(httptest.NewRecorder(), request, 1024, &target); err != nil {
		t.Fatalf("optional empty body error=%v", err)
	}
	if target.Message != "" {
		t.Fatalf("optional target=%#v", target)
	}
}

func TestDecodeRejectsAnonymousEmbeddedDTOFieldRatherThanDroppingIt(t *testing.T) {
	type embeddedFields struct {
		Message string `json:"message"`
	}
	request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"message":"ok"}`))
	var target struct{ embeddedFields }
	err := Decode(httptest.NewRecorder(), request, 1024, &target)
	if err == nil || !strings.Contains(err.Error(), "unsupported anonymous embedded field embeddedFields") {
		t.Fatalf("anonymous embedded field error=%v", err)
	}
	if target.Message != "" {
		t.Fatalf("anonymous field was silently decoded=%#v", target)
	}
}

func TestDecodeAllowsExplicitlyIgnoredAnonymousField(t *testing.T) {
	type ignoredFields struct {
		Ignored string
	}
	request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"message":"ok"}`))
	var target struct {
		ignoredFields `json:"-"`
		Message       string `json:"message"`
	}
	if err := Decode(httptest.NewRecorder(), request, 1024, &target); err != nil {
		t.Fatalf("explicitly ignored embedded field error=%v", err)
	}
	if target.Message != "ok" {
		t.Fatalf("decoded target=%#v", target)
	}
}
