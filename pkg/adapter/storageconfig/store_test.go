package storageconfig

import (
	"context"
	"errors"
	"strings"
	"testing"

	appsettings "github.com/whhhh1500/auto-agent/pkg/app/settings"
	appstorageconfig "github.com/whhhh1500/auto-agent/pkg/app/storageconfig"
)

func TestStoreLoadsLegacyConfigurationAndPreservesPresence(t *testing.T) {
	repository := &settingsRepository{values: map[string]string{
		"storage.sessions": `{"type":"file","path":"data/sessions","disable_conditional_writes":true}`,
	}}
	store, err := New(repository)
	if err != nil {
		t.Fatal(err)
	}
	configuration, found, err := store.Load(context.Background(), appstorageconfig.KindSessions)
	if err != nil || !found {
		t.Fatalf("configuration=%#v found=%t err=%v", configuration, found, err)
	}
	if configuration.Backend != appstorageconfig.BackendFile || configuration.Path != "data/sessions" {
		t.Fatalf("configuration=%#v", configuration)
	}
	if !configuration.DisableConditionalWrites {
		t.Fatal("session conditional-write compatibility flag was dropped")
	}
}

func TestStoreMalformedAndMaskedSecretsRemainPresentInvalid(t *testing.T) {
	for name, raw := range map[string]string{
		"malformed":     `{`,
		"masked secret": `{"type":"s3","endpoint":"https://s3.example","bucket":"b","access_key":"a","secret_key":"••••"}`,
	} {
		t.Run(name, func(t *testing.T) {
			repository := &settingsRepository{values: map[string]string{"storage.resources": raw}}
			store, err := New(repository)
			if err != nil {
				t.Fatal(err)
			}
			configuration, found, err := store.Load(context.Background(), appstorageconfig.KindResources)
			if err == nil || !found {
				t.Fatalf("configuration=%#v found=%t err=%v", configuration, found, err)
			}
			if strings.Contains(configuration.SecretKey, "••••") {
				t.Fatal("legacy mask was retained as a credential")
			}
		})
	}
}

func TestStoreCreateIfAbsentUsesLegacyShapeAndSeparateEvidence(t *testing.T) {
	repository := &settingsRepository{values: map[string]string{}, atomic: true}
	store, err := New(repository)
	if err != nil {
		t.Fatal(err)
	}
	configuration := appstorageconfig.StoredConfiguration{
		Backend: appstorageconfig.BackendS3, Endpoint: "https://s3.example",
		Bucket: "bucket", AccessKey: "access", SecretKey: "secret",
		Source: appstorageconfig.SourceEnvImport, Status: appstorageconfig.StatusActive,
	}
	created, err := store.CreateIfAbsent(context.Background(), appstorageconfig.KindResources, configuration)
	if err != nil || !created {
		t.Fatalf("created=%t err=%v", created, err)
	}
	raw := repository.values["storage.resources"]
	if !strings.Contains(raw, `"type":"s3"`) || !strings.Contains(raw, `"config_source":"env_import"`) || !strings.Contains(raw, `"config_status":"active"`) || !strings.Contains(raw, `"desired_revision":"rev_`) {
		t.Fatalf("raw=%s", raw)
	}
	if !strings.Contains(raw, "secret") {
		// The generic repository owns encryption. This fake intentionally stores
		// plaintext so the adapter test can assert the shape; production wiring
		// supplies the encrypted repository.
		t.Fatal("test fixture did not retain the configuration payload")
	}
	loaded, found, err := store.Load(context.Background(), appstorageconfig.KindResources)
	if err != nil || !found || loaded.DisableConditionalWrites {
		t.Fatalf("resources conditional-write flag was not safely normalized: loaded=%#v found=%t err=%v", loaded, found, err)
	}

	evidence := appstorageconfig.ResolutionEvidence{
		Kind: appstorageconfig.KindResources, Source: appstorageconfig.SourceEnvImport,
		Status: appstorageconfig.StatusActive, Found: true,
		DesiredType: appstorageconfig.BackendS3, ActiveType: appstorageconfig.BackendS3,
	}
	if err := store.SaveEvidence(context.Background(), appstorageconfig.KindResources, evidence); err != nil {
		t.Fatal(err)
	}
	evidenceRaw := repository.values["storage.resources.resolution"]
	if strings.Contains(evidenceRaw, "secret") || strings.Contains(evidenceRaw, "access") {
		t.Fatalf("evidence leaked sensitive values: %s", evidenceRaw)
	}
	loadedEvidence, found, err := store.LoadEvidence(context.Background(), appstorageconfig.KindResources)
	if err != nil || !found || loadedEvidence.Status != appstorageconfig.StatusActive {
		t.Fatalf("evidence=%#v found=%t err=%v", loadedEvidence, found, err)
	}
}

func TestStoreCreateIfAbsentFailsClosedWithoutAtomicCapability(t *testing.T) {
	repository := &nonAtomicSettingsRepository{values: map[string]string{}}
	store, err := New(repository)
	if err != nil {
		t.Fatal(err)
	}
	created, err := store.CreateIfAbsent(context.Background(), appstorageconfig.KindSessions, appstorageconfig.StoredConfiguration{Backend: appstorageconfig.BackendEmbedded})
	if created || !errors.Is(err, appstorageconfig.ErrAtomicCreateUnsupported) {
		t.Fatalf("created=%t err=%v", created, err)
	}
}

func TestStoreCreateIfAbsentPreservesFirstWriter(t *testing.T) {
	repository := &settingsRepository{values: map[string]string{}, atomic: true}
	store, err := New(repository)
	if err != nil {
		t.Fatal(err)
	}
	first := appstorageconfig.StoredConfiguration{Backend: appstorageconfig.BackendFile, Path: "first"}
	second := appstorageconfig.StoredConfiguration{Backend: appstorageconfig.BackendFile, Path: "second"}
	created, err := store.CreateIfAbsent(context.Background(), appstorageconfig.KindSessions, first)
	if err != nil || !created {
		t.Fatalf("first created=%t err=%v", created, err)
	}
	created, err = store.CreateIfAbsent(context.Background(), appstorageconfig.KindSessions, second)
	if err != nil || created {
		t.Fatalf("second created=%t err=%v", created, err)
	}
	loaded, found, err := store.Load(context.Background(), appstorageconfig.KindSessions)
	if err != nil || !found || loaded.Path != "first" {
		t.Fatalf("loaded=%#v found=%t err=%v", loaded, found, err)
	}
}

func TestStoreUpdateAndEvidenceCASUseRevisionGuard(t *testing.T) {
	repository := &settingsRepository{values: map[string]string{}, atomic: true}
	store, err := New(repository)
	if err != nil {
		t.Fatal(err)
	}
	created, err := store.CreateIfAbsent(context.Background(), appstorageconfig.KindResources, appstorageconfig.StoredConfiguration{Backend: appstorageconfig.BackendEmbedded})
	if err != nil || !created {
		t.Fatalf("created=%t err=%v", created, err)
	}
	current, found, err := store.Load(context.Background(), appstorageconfig.KindResources)
	if err != nil || !found {
		t.Fatalf("current=%#v found=%t err=%v", current, found, err)
	}
	next := appstorageconfig.StoredConfiguration{Backend: appstorageconfig.BackendS3, Endpoint: "https://s3.example", Bucket: "bucket", AccessKey: "access", SecretKey: "secret", Source: appstorageconfig.SourceDB, Status: appstorageconfig.StatusActive}
	updated, err := store.UpdateIfRevision(context.Background(), appstorageconfig.KindResources, current.DesiredRevision, next)
	if err != nil || !updated {
		t.Fatalf("updated=%t err=%v", updated, err)
	}
	updatedConfig, found, err := store.Load(context.Background(), appstorageconfig.KindResources)
	if err != nil || !found {
		t.Fatalf("updated config=%#v found=%t err=%v", updatedConfig, found, err)
	}
	evidence := appstorageconfig.ResolutionEvidence{Kind: appstorageconfig.KindResources, Source: appstorageconfig.SourceDB, Status: appstorageconfig.StatusActive, Found: true, DesiredType: appstorageconfig.BackendS3, DesiredRevision: updatedConfig.DesiredRevision}
	updated, err = store.SaveEvidenceIfDesiredRevision(context.Background(), appstorageconfig.KindResources, evidence)
	if err != nil || !updated {
		t.Fatalf("evidence updated=%t err=%v", updated, err)
	}
	stale := evidence
	stale.DesiredRevision = current.DesiredRevision
	updated, err = store.SaveEvidenceIfDesiredRevision(context.Background(), appstorageconfig.KindResources, stale)
	if err != nil || updated {
		t.Fatalf("stale evidence updated=%t err=%v", updated, err)
	}
}

type settingsRepository struct {
	values map[string]string
	atomic bool
}

type nonAtomicSettingsRepository struct{ values map[string]string }

func (repository *nonAtomicSettingsRepository) GetSetting(_ context.Context, key string) (string, bool, error) {
	value, found := repository.values[key]
	return value, found, nil
}

func (repository *nonAtomicSettingsRepository) SetSetting(_ context.Context, key, value string) error {
	repository.values[key] = value
	return nil
}

var _ appsettings.Repository = (*settingsRepository)(nil)

func (repository *settingsRepository) GetSetting(_ context.Context, key string) (string, bool, error) {
	value, found := repository.values[key]
	return value, found, nil
}

func (repository *settingsRepository) SetSetting(_ context.Context, key, value string) error {
	repository.values[key] = value
	return nil
}

func (repository *settingsRepository) SetSettingIfAbsent(_ context.Context, key, value string) (bool, error) {
	if !repository.atomic {
		return false, errors.New("atomic capability disabled")
	}
	if _, found := repository.values[key]; found {
		return false, nil
	}
	repository.values[key] = value
	return true, nil
}

func (repository *settingsRepository) CompareAndSwapSetting(_ context.Context, key, expected, next string) (bool, error) {
	current, found := repository.values[key]
	if !found {
		current = ""
	}
	if current != expected {
		return false, nil
	}
	repository.values[key] = next
	return true, nil
}
