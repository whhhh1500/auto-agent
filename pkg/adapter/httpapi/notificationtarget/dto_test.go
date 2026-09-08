package notificationtarget

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	appnotification "github.com/whhhh1500/auto-agent/pkg/app/notification"
)

func TestDecodeRequestStrictJSONAndLegalMultiFieldMutation(t *testing.T) {
	valid := `{"target_ref":"ops","channel_id":"webhook","channel_version":"1","label":"Operations","formats":["text","markdown"],"config":{"url":"https://example.test/hook","secret":"fake-secret"},"enabled":true}`
	cases := []struct {
		name  string
		body  string
		valid bool
	}{
		{name: "unknown", body: strings.TrimSuffix(valid, "}") + `,"unknown":true}`, valid: false},
		{name: "duplicate top-level", body: `{"target_ref":"ops","target_ref":"other","channel_id":"webhook","channel_version":"1","config":{},"enabled":true}`, valid: false},
		{name: "duplicate nested", body: `{"target_ref":"ops","channel_id":"webhook","channel_version":"1","config":{"url":"https://example.test","url":"https://other.test"},"enabled":true}`, valid: false},
		{name: "null config", body: `{"target_ref":"ops","channel_id":"webhook","channel_version":"1","config":null,"enabled":true}`, valid: false},
		{name: "wrong type", body: `{"target_ref":"ops","channel_id":"webhook","channel_version":"1","config":{},"enabled":"true"}`, valid: false},
		{name: "trailing", body: valid + ` {"later":true}`, valid: false},
		{name: "legal multi-field", body: valid, valid: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tc.body))
			response := httptest.NewRecorder()
			var decoded CreateRequest
			err := DecodeRequest(response, request, 4096, &decoded)
			if tc.valid {
				if err != nil || decoded.Validate() != nil {
					t.Fatalf("valid request err=%v validate=%v", err, decoded.Validate())
				}
				return
			}
			if err == nil && decoded.Validate() == nil {
				t.Fatal("malformed request accepted")
			}
			if err != nil && err != ErrInvalidRequest {
				t.Fatalf("error=%v, want fixed ErrInvalidRequest", err)
			}
		})
	}
}

func TestUpdateRequestConfigPresenceSemantics(t *testing.T) {
	base := `{"target_ref":"ops","channel_id":"webhook","channel_version":"1","enabled":true,"expected_revision":"1"}`
	for _, tc := range []struct {
		name      string
		body      string
		wantNil   bool
		wantError bool
	}{
		{name: "omitted preserves", body: base, wantNil: true},
		{name: "explicit null rejects", body: strings.TrimSuffix(base, "}") + `,"config":null}`, wantError: true},
		{name: "object replaces", body: strings.TrimSuffix(base, "}") + `,"config":{"provider":"opaque"}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPut, "/", strings.NewReader(tc.body))
			response := httptest.NewRecorder()
			var decoded UpdateRequest
			if err := DecodeRequest(response, request, 4096, &decoded); err != nil {
				t.Fatal(err)
			}
			err := decoded.Validate()
			if tc.wantError {
				if err == nil {
					t.Fatal("null config accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if (decoded.Config == nil) != tc.wantNil {
				t.Fatalf("config nil=%v, want %v", decoded.Config == nil, tc.wantNil)
			}
		})
	}
}

func TestListResponseChannelsAreMetadataOnly(t *testing.T) {
	response := ListResponse{Targets: []TargetView{}, Channels: ChannelViews([]appnotification.ChannelRef{{ID: "webhook", Version: "1"}})}
	encoded, err := json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	const want = `{"targets":[],"channels":[{"id":"webhook","version":"1"}]}`
	if string(encoded) != want {
		t.Fatalf("response=%s, want %s", encoded, want)
	}
	for _, forbidden := range []string{"secret", "config", "url", "capabilities"} {
		if strings.Contains(string(encoded), forbidden) {
			t.Fatalf("channel discovery exposed %q: %s", forbidden, encoded)
		}
	}
}
