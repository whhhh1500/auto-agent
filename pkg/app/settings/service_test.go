package settings

import (
	"context"
	"errors"
	"strings"
	"testing"

	appidentity "github.com/whhhh1500/auto-agent/pkg/app/identity"
)

func TestServiceRequiresPlatformAdmin(t *testing.T) {
	repository := &fakeRepository{}
	service, err := NewService(repository)
	if err != nil {
		t.Fatal(err)
	}

	_, err = service.Get(context.Background(), GetCommand{
		Actor: appidentity.AdminActor{Role: appidentity.RoleTenantAdmin}, Key: "llm",
	})
	if !errors.Is(err, ErrForbidden) {
		t.Fatalf("get error=%v; want forbidden", err)
	}
	err = service.Put(context.Background(), PutCommand{
		Actor: appidentity.AdminActor{Role: appidentity.RoleUser}, Key: "llm", Value: `{"model":"test"}`,
	})
	if !errors.Is(err, ErrForbidden) {
		t.Fatalf("put error=%v; want forbidden", err)
	}
	if repository.getCalls != 0 || repository.setCalls != 0 {
		t.Fatalf("unauthorized calls reached repository: %#v", repository)
	}
}

func TestServiceValidatesKeyBeforeRepository(t *testing.T) {
	service, err := NewService(&fakeRepository{})
	if err != nil {
		t.Fatal(err)
	}
	actor := appidentity.AdminActor{Role: appidentity.RoleAdmin}
	for name, key := range map[string]string{
		"empty":          " \t",
		"NUL":            "llm\x00key",
		"control":        "llm\nkey",
		"too many runes": strings.Repeat("界", maxSettingKeyRunes+1),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := service.Get(context.Background(), GetCommand{Actor: actor, Key: key})
			if !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("get %q error=%v; want invalid input", key, err)
			}
			err = service.Put(context.Background(), PutCommand{Actor: actor, Key: key})
			if !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("put %q error=%v; want invalid input", key, err)
			}
		})
	}
}

func TestServiceFailClosedGetNeverReturnsSettingValues(t *testing.T) {
	repository := &fakeRepository{values: map[string]string{
		"llm":               "LLM-middle-sensitive-TAIL",
		"llm.api_key":       "pre-middle-value-tail",
		"account.password":  "small",
		"service_token":     "tok-middle-sensitive-LAST",
		"storage.resources": `{"type":"s3","secret_key":"storage-resource-sentinel"}`,
		"storage.sessions":  `{"type":"s3","secret_key":"storage-session-sentinel"}`,
		"oauth_config":      `{"client_secret":"oauth-sentinel"}`,
		"config":            `{"token":"config-sentinel"}`,
		"empty":             "",
	}}
	service, err := NewService(repository)
	if err != nil {
		t.Fatal(err)
	}
	actor := appidentity.AdminActor{Role: appidentity.RoleAdmin}

	for _, test := range []struct {
		key          string
		found        bool
		wantRedacted bool
	}{
		{key: "llm", found: true, wantRedacted: true},
		{key: "llm.api_key", found: true, wantRedacted: true},
		{key: "account.password", found: true, wantRedacted: true},
		{key: "service_token", found: true, wantRedacted: true},
		{key: "storage.resources", found: true, wantRedacted: true},
		{key: "storage.sessions", found: true, wantRedacted: true},
		{key: "oauth_config", found: true, wantRedacted: true},
		{key: "config", found: true, wantRedacted: true},
		{key: "empty", found: true, wantRedacted: true},
		{key: "absent", found: false},
	} {
		t.Run(test.key, func(t *testing.T) {
			result, err := service.Get(context.Background(), GetCommand{Actor: actor, Key: test.key})
			if err != nil {
				t.Fatal(err)
			}
			if result.Key != test.key || result.Found != test.found || result.Value != "" ||
				result.Redacted != test.wantRedacted || result.WriteOnly != test.wantRedacted {
				t.Fatalf("get result=%#v", result)
			}
		})
	}
}

func TestServicePutUsesRawValue(t *testing.T) {
	repository := &fakeRepository{}
	service, err := NewService(repository)
	if err != nil {
		t.Fatal(err)
	}
	value := `{"api_key":"raw-secret"}`
	err = service.Put(context.Background(), PutCommand{
		Actor: appidentity.AdminActor{Role: appidentity.RoleAdmin}, Key: "llm", Value: value,
	})
	if err != nil {
		t.Fatal(err)
	}
	if repository.values["llm"] != value || repository.setCalls != 1 {
		t.Fatalf("put repository=%#v", repository)
	}
}

type fakeRepository struct {
	values   map[string]string
	getCalls int
	setCalls int
}

func (repository *fakeRepository) GetSetting(_ context.Context, key string) (string, bool, error) {
	repository.getCalls++
	value, found := repository.values[key]
	return value, found, nil
}

func (repository *fakeRepository) SetSetting(_ context.Context, key, value string) error {
	repository.setCalls++
	if repository.values == nil {
		repository.values = map[string]string{}
	}
	repository.values[key] = value
	return nil
}
