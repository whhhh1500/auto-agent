package main

import (
	"context"
	"reflect"
	"sync"
	"testing"

	appstorageconfig "github.com/whhhh1500/auto-agent/pkg/app/storageconfig"
	"github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/storage"
)

func lookupTestEnv(values map[string]string) func(string) (string, bool) {
	return func(name string) (string, bool) {
		value, ok := values[name]
		return value, ok
	}
}

func TestLegacyResourceInputFromEnv(t *testing.T) {
	tests := []struct {
		name          string
		env           map[string]string
		wantNil       bool
		wantEndpoint  string
		wantBucket    string
		wantAccessKey string
		wantSecret    string
		wantPathStyle bool
		wantInvalid   bool
		wantError     bool
	}{
		{
			name: "resource specific wins as a complete group",
			env: map[string]string{
				"HARNESS_RESOURCE_S3_ENDPOINT":          "https://resource.example",
				"HARNESS_RESOURCE_S3_REGION":            "resource-region",
				"HARNESS_RESOURCE_S3_BUCKET":            "resource-bucket",
				"HARNESS_RESOURCE_S3_ACCESS_KEY_ID":     "resource-access",
				"HARNESS_RESOURCE_S3_SECRET_ACCESS_KEY": "resource-secret",
				"HARNESS_RESOURCE_S3_PATH_STYLE":        "true",
				"HARNESS_S3_ENDPOINT":                   "https://session.example",
				"HARNESS_S3_BUCKET":                     "session-bucket",
			},
			wantEndpoint: "https://resource.example", wantBucket: "resource-bucket",
			wantAccessKey: "resource-access", wantSecret: "resource-secret", wantPathStyle: true,
		},
		{
			name: "partial resource group is not inherited",
			env: map[string]string{
				"HARNESS_RESOURCE_S3_ENDPOINT": "https://resource.example",
				"HARNESS_S3_BUCKET":            "session-bucket",
				"HARNESS_S3_ACCESS_KEY_ID":     "session-access",
				"HARNESS_S3_SECRET_ACCESS_KEY": "session-secret",
			},
			wantEndpoint: "https://resource.example", wantBucket: "", wantInvalid: true,
		},
		{
			name: "session S3 is inherited only without resource variables",
			env: map[string]string{
				"HARNESS_S3_ENDPOINT":          "https://session.example",
				"HARNESS_S3_REGION":            "session-region",
				"HARNESS_S3_BUCKET":            "session-bucket",
				"HARNESS_S3_ACCESS_KEY_ID":     "session-access",
				"HARNESS_S3_SECRET_ACCESS_KEY": "session-secret",
				"HARNESS_S3_PATH_STYLE":        "true",
			},
			wantEndpoint: "https://session.example", wantBucket: "session-bucket",
			wantAccessKey: "session-access", wantSecret: "session-secret", wantPathStyle: true,
		},
		{
			name:      "resource path style rejects non boolean",
			env:       map[string]string{"HARNESS_RESOURCE_S3_PATH_STYLE": "yes"},
			wantError: true,
		},
		{
			name:    "session-only disable variable does not configure resources",
			env:     map[string]string{"HARNESS_S3_DISABLE_CONDITIONAL_WRITES": "true"},
			wantNil: true,
		},
		{name: "unrelated session directory does not configure resources", env: map[string]string{"HARNESS_SESSION_DIR": "sessions"}, wantNil: true},
		{name: "no legacy variables", env: map[string]string{}, wantNil: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input, err := legacyResourceInputFromEnv(lookupTestEnv(test.env))
			if test.wantError {
				if err == nil {
					t.Fatal("expected strict boolean parsing error")
				}
				return
			}
			if err != nil {
				t.Fatalf("legacy resource resolution failed: %v", err)
			}
			if test.wantNil {
				if input != nil {
					t.Fatal("expected no legacy resource candidate")
				}
				return
			}
			if input == nil {
				t.Fatal("expected legacy resource candidate")
			}
			configuration := input.Configuration
			if configuration.Backend != appstorageconfig.BackendS3 {
				t.Error("legacy resource candidate is not S3")
			}
			if configuration.Endpoint != test.wantEndpoint || configuration.Bucket != test.wantBucket {
				t.Error("resource endpoint or bucket was resolved with the wrong precedence")
			}
			if configuration.AccessKey != test.wantAccessKey || configuration.SecretKey != test.wantSecret {
				t.Error("resource credentials were resolved with the wrong precedence")
			}
			if configuration.PathStyle != test.wantPathStyle {
				t.Error("resource path-style flag was not preserved")
			}
			if test.wantInvalid && appstorageconfig.Validate(appstorageconfig.KindResources, configuration) == nil {
				t.Error("partial resource S3 candidate should fail typed validation")
			}
		})
	}
}

func TestLegacySessionInputFromEnv(t *testing.T) {
	tests := []struct {
		name         string
		env          map[string]string
		wantNil      bool
		wantBackend  appstorageconfig.Backend
		wantPath     string
		wantEndpoint string
		wantDisable  bool
		wantError    bool
		wantInvalid  bool
	}{
		{
			name: "S3 wins over session directory",
			env: map[string]string{
				"HARNESS_S3_ENDPOINT": "https://session.example", "HARNESS_S3_REGION": "region",
				"HARNESS_S3_BUCKET": "bucket", "HARNESS_S3_ACCESS_KEY_ID": "access", "HARNESS_S3_SECRET_ACCESS_KEY": "secret",
				"HARNESS_SESSION_DIR": "legacy-sessions",
			},
			wantBackend: appstorageconfig.BackendS3, wantEndpoint: "https://session.example",
		},
		{
			name: "conditional writes flag is carried",
			env: map[string]string{
				"HARNESS_S3_ENDPOINT": "https://session.example", "HARNESS_S3_BUCKET": "bucket",
				"HARNESS_S3_ACCESS_KEY_ID": "access", "HARNESS_S3_SECRET_ACCESS_KEY": "secret",
				"HARNESS_S3_DISABLE_CONDITIONAL_WRITES": "true",
			},
			wantBackend: appstorageconfig.BackendS3, wantEndpoint: "https://session.example", wantDisable: true,
		},
		{
			name:        "partial S3 still wins over session directory",
			env:         map[string]string{"HARNESS_S3_BUCKET": "bucket", "HARNESS_SESSION_DIR": "legacy-sessions"},
			wantBackend: appstorageconfig.BackendS3, wantInvalid: true,
		},
		{
			name: "invalid path style is rejected",
			env:  map[string]string{"HARNESS_S3_PATH_STYLE": "1"}, wantError: true,
		},
		{
			name: "invalid conditional writes flag is rejected",
			env:  map[string]string{"HARNESS_S3_DISABLE_CONDITIONAL_WRITES": "yes"}, wantError: true,
		},
		{
			name:        "directory is used without S3",
			env:         map[string]string{"HARNESS_SESSION_DIR": "legacy-sessions"},
			wantBackend: appstorageconfig.BackendFile, wantPath: "legacy-sessions",
		},
		{name: "no legacy variables", env: map[string]string{}, wantNil: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input, err := legacySessionInputFromEnv(lookupTestEnv(test.env))
			if test.wantError {
				if err == nil {
					t.Fatal("expected strict boolean parsing error")
				}
				return
			}
			if err != nil {
				t.Fatalf("legacy session resolution failed: %v", err)
			}
			if test.wantNil {
				if input != nil {
					t.Fatal("expected no legacy session candidate")
				}
				return
			}
			if input == nil {
				t.Fatal("expected legacy session candidate")
			}
			configuration := input.Configuration
			if configuration.Backend != test.wantBackend {
				t.Error("legacy session backend precedence was not preserved")
			}
			if configuration.Path != test.wantPath || configuration.Endpoint != test.wantEndpoint {
				t.Error("legacy session fields were not resolved as expected")
			}
			if configuration.DisableConditionalWrites != test.wantDisable {
				t.Error("conditional writes flag was not resolved as expected")
			}
			if test.wantInvalid && appstorageconfig.Validate(appstorageconfig.KindSessions, configuration) == nil {
				t.Error("partial session S3 candidate should fail typed validation")
			}
		})
	}
}

func TestLegacyInputConcurrentLookup(t *testing.T) {
	lookup := lookupTestEnv(map[string]string{
		"HARNESS_S3_ENDPOINT":          "https://session.example",
		"HARNESS_S3_BUCKET":            "bucket",
		"HARNESS_S3_ACCESS_KEY_ID":     "access",
		"HARNESS_S3_SECRET_ACCESS_KEY": "secret",
		"HARNESS_S3_PATH_STYLE":        "false",
	})
	var group sync.WaitGroup
	errs := make(chan string, 64)
	for i := 0; i < 64; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			resource, err := legacyResourceInputFromEnv(lookup)
			if err != nil || resource == nil {
				errs <- "resource resolution failed"
			}
			session, err := legacySessionInputFromEnv(lookup)
			if err != nil || session == nil {
				errs <- "session resolution failed"
			}
		}()
	}
	group.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

type testSessionStore struct{}

func (*testSessionStore) Create(context.Context, *core.Session) error { return nil }
func (*testSessionStore) Load(context.Context, string) (*core.Session, error) {
	return nil, core.ErrSessionNotFound
}
func (*testSessionStore) Save(context.Context, *core.Session, int64) error { return nil }

func TestResourceBackendFromConfig(t *testing.T) {
	embedded := storage.NewMemoryObjectStore()
	tests := []struct {
		name        string
		config      appstorageconfig.StoredConfiguration
		embedded    storage.ObjectStore
		wantSame    bool
		wantLabel   string
		wantBackend appstorageconfig.Backend
		wantError   bool
	}{
		{name: "embedded uses injected store", config: appstorageconfig.StoredConfiguration{Backend: appstorageconfig.BackendEmbedded, DesiredRevision: "rev_embedded"}, embedded: embedded, wantSame: true, wantLabel: "embedded", wantBackend: appstorageconfig.BackendEmbedded},
		{name: "s3 constructs without provisioning", config: validS3Configuration(), wantLabel: "s3", wantBackend: appstorageconfig.BackendS3},
		{name: "resources reject disabled conditional writes", config: appstorageconfig.StoredConfiguration{Backend: appstorageconfig.BackendS3, Endpoint: "https://s3.example", Bucket: "bucket", AccessKey: "access", SecretKey: "secret", DisableConditionalWrites: true}, wantError: true},
		{name: "file is sessions only", config: appstorageconfig.StoredConfiguration{Backend: appstorageconfig.BackendFile, Path: t.TempDir()}, wantError: true},
		{name: "missing embedded store fails", config: appstorageconfig.StoredConfiguration{Backend: appstorageconfig.BackendEmbedded}, wantError: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			backend, active, label, err := resourceBackendFromConfig(test.config, test.embedded)
			if test.wantError {
				if err == nil {
					t.Fatal("expected resource builder error")
				}
				return
			}
			if err != nil {
				t.Fatalf("resource builder failed: %v", err)
			}
			if label != test.wantLabel || active.Backend != test.wantBackend {
				t.Error("resource builder returned incorrect active state")
			}
			if test.wantSame && backend != test.embedded {
				t.Error("embedded resource store was replaced")
			}
		})
	}
}

func TestSessionBackendFromConfig(t *testing.T) {
	embedded := &testSessionStore{}
	tests := []struct {
		name        string
		config      appstorageconfig.StoredConfiguration
		embedded    core.SessionStore
		wantSame    bool
		wantType    string
		wantLabel   string
		wantBackend appstorageconfig.Backend
		wantDisable bool
		wantError   bool
	}{
		{name: "embedded uses injected SQL store", config: appstorageconfig.StoredConfiguration{Backend: appstorageconfig.BackendEmbedded, DesiredRevision: "rev_embedded"}, embedded: embedded, wantSame: true, wantLabel: "embedded", wantBackend: appstorageconfig.BackendEmbedded},
		{name: "file constructs local session store", config: appstorageconfig.StoredConfiguration{Backend: appstorageconfig.BackendFile, Path: t.TempDir()}, wantType: "*storage.FileSessionStore", wantLabel: "file", wantBackend: appstorageconfig.BackendFile},
		{name: "s3 constructs session store without provisioning", config: validS3Configuration(), wantType: "*storage.S3SessionStore", wantLabel: "s3", wantBackend: appstorageconfig.BackendS3},
		{name: "s3 carries disabled conditional writes", config: func() appstorageconfig.StoredConfiguration {
			configuration := validS3Configuration()
			configuration.DisableConditionalWrites = true
			return configuration
		}(), wantType: "*storage.S3SessionStore", wantLabel: "s3", wantBackend: appstorageconfig.BackendS3, wantDisable: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			backend, active, label, err := sessionBackendFromConfig(test.config, test.embedded)
			if test.wantError {
				if err == nil {
					t.Fatal("expected session builder error")
				}
				return
			}
			if err != nil {
				t.Fatalf("session builder failed: %v", err)
			}
			if label != test.wantLabel || active.Backend != test.wantBackend {
				t.Error("session builder returned incorrect active state")
			}
			if test.wantSame && backend != test.embedded {
				t.Error("embedded session store was replaced")
			}
			if test.wantType != "" && reflect.TypeOf(backend).String() != test.wantType {
				t.Error("session builder returned an unexpected store type")
			}
			if test.wantDisable {
				store, ok := backend.(*storage.S3SessionStore)
				if !ok || !store.DisableConditionalWrites {
					t.Error("session S3 store did not receive disabled conditional writes")
				}
			}
		})
	}
}

func validS3Configuration() appstorageconfig.StoredConfiguration {
	return appstorageconfig.StoredConfiguration{
		Backend: appstorageconfig.BackendS3, Endpoint: "https://s3.example", Region: "region",
		Bucket: "bucket", AccessKey: "access", SecretKey: "secret", DesiredRevision: "rev_s3",
	}
}

func TestEvidenceForActiveMatchesConstructedRuntime(t *testing.T) {
	evidence := appstorageconfig.ResolutionEvidence{
		Kind: appstorageconfig.KindResources, Source: appstorageconfig.SourceDB,
		Status: appstorageconfig.StatusRestartPending, RestartPending: true,
		DesiredType: appstorageconfig.BackendS3, DesiredRevision: "rev_desired",
		ActiveType: appstorageconfig.BackendEmbedded, ActiveRevision: "rev_old",
	}
	refreshed := evidenceForActive(evidence, appstorageconfig.ActiveState{Backend: appstorageconfig.BackendS3, Revision: "rev_desired"})
	if refreshed.Status != appstorageconfig.StatusActive || refreshed.RestartPending || refreshed.ErrorCode != "" || refreshed.ActiveType != appstorageconfig.BackendS3 || refreshed.ActiveRevision != "rev_desired" {
		t.Fatalf("evidence does not match active runtime: %#v", refreshed)
	}
	if err := refreshed.Validate(); err != nil {
		t.Fatal(err)
	}
}
