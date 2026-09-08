package modelsettings

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/whhhh1500/auto-agent/pkg/adapter/httpapi/jsonbody"
)

func TestGetResponseCarriesOnlyNonSensitiveConfigSource(t *testing.T) {
	encoded, err := json.Marshal(GetResponse{Found: true, Model: "model-a", Provider: "openai", Protocol: "openai-chat-completions", MaxTokens: 512, AllowedModels: []string{}, HasAPIKey: true, ConfigSource: "env_import"})
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != `{"found":true,"base_url":"","model":"model-a","provider":"openai","protocol":"openai-chat-completions","max_tokens":512,"allowed_models":[],"has_api_key":true,"config_source":"env_import","executable":false}` {
		t.Fatalf("model settings source wire=%s", encoded)
	}
}

func TestPutRequestTracksSecretAndAllowedModelPresence(t *testing.T) {
	for name, body := range map[string]string{
		"omitted":     `{"model":"model-a","future":true}`,
		"replacement": `{"model":"model-a","provider":"anthropic","protocol":"anthropic-messages","api_key":"replacement","max_tokens":512,"allowed_models":["model-b","model-a"]}`,
		"empty list":  `{"model":"model-a","allowed_models":[]}`,
	} {
		t.Run(name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPut, "/", strings.NewReader(body))
			var target PutRequest
			if err := jsonbody.Decode(httptest.NewRecorder(), request, 1024, &target); err != nil {
				t.Fatalf("decode: %v", err)
			}
			switch name {
			case "omitted":
				if target.APIKey.Present || target.AllowedModels.Present || target.Provider.Present || target.Protocol.Present {
					t.Fatalf("omitted fields=%#v", target)
				}
			case "replacement":
				if !target.APIKey.Present || target.APIKey.Value != "replacement" || !target.Provider.Present || target.Provider.Value != "anthropic" || !target.Protocol.Present || target.Protocol.Value != "anthropic-messages" || !target.MaxTokens.Present || target.MaxTokens.Value != 512 || !target.AllowedModels.Present || len(target.AllowedModels.Values) != 2 {
					t.Fatalf("replacement fields=%#v", target)
				}
			case "empty list":
				if !target.AllowedModels.Present || len(target.AllowedModels.Values) != 0 {
					t.Fatalf("explicit list replacement=%#v", target)
				}
			}
		})
	}
	for name, body := range map[string]string{
		"null api key":       `{"model":"model-a","api_key":null}`,
		"wrong api key type": `{"model":"model-a","api_key":17}`,
		"null allowed list":  `{"model":"model-a","allowed_models":null}`,
		"wrong allowed list": `{"model":"model-a","allowed_models":"model-a"}`,
		"null clear":         `{"model":"model-a","clear_api_key":null}`,
		"wrong clear":        `{"model":"model-a","clear_api_key":"true"}`,
		"null max tokens":    `{"model":"model-a","max_tokens":null}`,
		"fractional max":     `{"model":"model-a","max_tokens":1.5}`,
		"string max":         `{"model":"model-a","max_tokens":"512"}`,
		"null provider":      `{"model":"model-a","provider":null}`,
		"wrong provider":     `{"model":"model-a","provider":17}`,
		"null protocol":      `{"model":"model-a","protocol":null}`,
		"wrong protocol":     `{"model":"model-a","protocol":17}`,
	} {
		t.Run(name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPut, "/", strings.NewReader(body))
			var target PutRequest
			if err := jsonbody.Decode(httptest.NewRecorder(), request, 1024, &target); err == nil {
				t.Fatal("invalid known field was accepted")
			}
		})
	}
}

func TestPutRequestTracksClearAPIKeyBooleanPresence(t *testing.T) {
	for name, body := range map[string]string{
		"omitted": `{"model":"model-a"}`,
		"false":   `{"model":"model-a","clear_api_key":false}`,
		"true":    `{"model":"model-a","clear_api_key":true}`,
	} {
		t.Run(name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPut, "/", strings.NewReader(body))
			var target PutRequest
			if err := jsonbody.Decode(httptest.NewRecorder(), request, 1024, &target); err != nil {
				t.Fatal(err)
			}
			switch name {
			case "omitted":
				if target.ClearAPIKey.Present {
					t.Fatalf("omitted clear=%#v", target.ClearAPIKey)
				}
			case "false":
				if !target.ClearAPIKey.Present || target.ClearAPIKey.Value {
					t.Fatalf("false clear=%#v", target.ClearAPIKey)
				}
			case "true":
				if !target.ClearAPIKey.Present || !target.ClearAPIKey.Value {
					t.Fatalf("true clear=%#v", target.ClearAPIKey)
				}
			}
		})
	}
}

func TestLegacyPutRequestMapsOnlyEmptyNestedAPIKeyToPreserve(t *testing.T) {
	preserve, err := LegacyPutRequest([]byte(`{"base_url":"https://example.test/v1","model":"model-a","api_key":"","future":true}`))
	if err != nil {
		t.Fatal(err)
	}
	if preserve.APIKey.Present {
		t.Fatalf("legacy empty API key must map to omitted preserve intent: %#v", preserve)
	}
	replacement, err := LegacyPutRequest([]byte(`{"model":"model-a","api_key":"replacement"}`))
	if err != nil {
		t.Fatal(err)
	}
	if !replacement.APIKey.Present || replacement.APIKey.Value != "replacement" {
		t.Fatalf("legacy replacement=%#v", replacement)
	}
	if _, err := LegacyPutRequest([]byte(`{"model":"model-a","api_key":null}`)); err == nil {
		t.Fatal("legacy null must not be guessed as preserve")
	}
}
