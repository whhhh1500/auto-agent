package storageconfig

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
)

func TestResolverUsesBootstrapWhenRowAndLegacyAreAbsent(t *testing.T) {
	repository := &fakeRepository{}
	resolution, err := (Resolver{Repository: repository}).Resolve(context.Background(), KindResources, nil, ActiveState{})
	if err != nil {
		t.Fatal(err)
	}
	if resolution.Desired.Backend != BackendEmbedded || resolution.Evidence.Source != SourceBootstrap || resolution.Evidence.Status != StatusActive {
		t.Fatalf("resolution=%#v", resolution)
	}
	if resolution.Evidence.Found {
		t.Fatal("bootstrap resolution must not invent a desired row")
	}
}

func TestResolverImportsValidatedLegacyOnlyWhenRowAbsent(t *testing.T) {
	repository := &fakeRepository{}
	legacy := &LegacyInput{Configuration: StoredConfiguration{
		Backend: BackendS3, Endpoint: "https://s3.example", Bucket: "bucket",
		AccessKey: "access", SecretKey: "secret",
	}}
	resolution, err := (Resolver{Repository: repository}).Resolve(context.Background(), KindResources, legacy, ActiveState{})
	if err != nil {
		t.Fatal(err)
	}
	if resolution.Evidence.Source != SourceEnvImport || resolution.Evidence.Status != StatusActive || !resolution.Evidence.Found {
		t.Fatalf("evidence=%#v", resolution.Evidence)
	}
	if repository.saved.Backend != BackendS3 || repository.saved.Source != SourceEnvImport {
		t.Fatalf("saved=%#v", repository.saved)
	}
	if evidenceContains(resolution.Evidence, "secret") {
		t.Fatal("secret appeared in resolution evidence")
	}
}

func TestResolverLegacyProviderIsLazyAndSingleShot(t *testing.T) {
	called := 0
	provider := func(context.Context, Kind) (*LegacyInput, error) {
		called++
		return &LegacyInput{Configuration: StoredConfiguration{Backend: BackendEmbedded}}, nil
	}
	foundRepository := &fakeRepository{found: true, loaded: StoredConfiguration{Backend: BackendEmbedded, Source: SourceDB, Status: StatusActive}}
	if _, err := (Resolver{Repository: foundRepository}).ResolveWithLegacyProvider(context.Background(), KindResources, provider, ActiveState{}); err != nil {
		t.Fatal(err)
	}
	if called != 0 {
		t.Fatalf("legacy provider called for DB-present row: %d", called)
	}
	absentRepository := &fakeRepository{}
	if _, err := (Resolver{Repository: absentRepository}).ResolveWithLegacyProvider(context.Background(), KindResources, provider, ActiveState{}); err != nil {
		t.Fatal(err)
	}
	if called != 1 {
		t.Fatalf("legacy provider calls=%d, want one", called)
	}
}

func TestResolverLegacyProviderErrorDoesNotWrite(t *testing.T) {
	providerErr := errors.New("legacy environment mapping failed")
	repository := &fakeRepository{}
	_, err := (Resolver{Repository: repository}).ResolveWithLegacyProvider(context.Background(), KindResources, func(context.Context, Kind) (*LegacyInput, error) {
		return nil, providerErr
	}, ActiveState{})
	if !errors.Is(err, providerErr) {
		t.Fatalf("provider error was hidden: %v", err)
	}
	if repository.saved.Backend != "" || repository.evidenceFound {
		t.Fatalf("provider failure wrote state: saved=%#v evidenceFound=%t", repository.saved, repository.evidenceFound)
	}
}

func TestResolverPresentInvalidDoesNotUseLegacy(t *testing.T) {
	repository := &fakeRepository{
		found:   true,
		loadErr: errors.New("malformed storage JSON"),
	}
	legacy := &LegacyInput{Configuration: StoredConfiguration{Backend: BackendS3, Endpoint: "https://s3.example", Bucket: "bucket", AccessKey: "a", SecretKey: "secret"}}
	resolution, err := (Resolver{Repository: repository}).Resolve(context.Background(), KindResources, legacy, ActiveState{Backend: BackendEmbedded})
	if err == nil || !strings.Contains(err.Error(), "malformed") {
		t.Fatalf("err=%v", err)
	}
	if resolution.Evidence.Found != true || resolution.Evidence.Source != SourceDB || resolution.Evidence.Status != StatusInvalid {
		t.Fatalf("evidence=%#v", resolution.Evidence)
	}
	if repository.saved.Backend != "" {
		t.Fatal("invalid DB row must not be replaced by legacy input")
	}
}

func TestResolverPresentInactiveDoesNotUseLegacy(t *testing.T) {
	repository := &fakeRepository{found: true, loaded: StoredConfiguration{Status: StatusInactive}}
	legacy := &LegacyInput{Configuration: StoredConfiguration{Backend: BackendS3, Endpoint: "https://s3.example", Bucket: "bucket", AccessKey: "a", SecretKey: "secret"}}
	resolution, err := (Resolver{Repository: repository}).Resolve(context.Background(), KindSessions, legacy, ActiveState{Backend: BackendFile})
	if err == nil || !errors.Is(err, ErrInactiveConfiguration) {
		t.Fatalf("err=%v", err)
	}
	if resolution.Evidence.Source != SourceDB || resolution.Evidence.Status != StatusInactive || resolution.Evidence.ActiveType != BackendFile {
		t.Fatalf("evidence=%#v", resolution.Evidence)
	}
	if repository.saved.Backend != "" {
		t.Fatal("inactive DB row must not be replaced by legacy input")
	}
}

func TestResolverInvalidSourceProducesPersistableDatabaseEvidence(t *testing.T) {
	repository := &fakeRepository{found: true, loaded: StoredConfiguration{Backend: BackendEmbedded, Source: ConfigSource("unexpected")}}
	resolution, err := (Resolver{Repository: repository}).Resolve(context.Background(), KindResources, nil, ActiveState{})
	if err == nil || resolution.Evidence.Source != SourceDB || resolution.Evidence.Status != StatusInvalid {
		t.Fatalf("resolution=%#v err=%v", resolution, err)
	}
	if repository.evidence.Source != SourceDB || repository.evidence.ErrorCode != ErrorCodeInvalid {
		t.Fatalf("evidence was not normalized for persistence: %#v", repository.evidence)
	}
}

func TestResolverSessionChangeIsRestartPending(t *testing.T) {
	repository := &fakeRepository{found: true, loaded: StoredConfiguration{Backend: BackendS3, Endpoint: "https://s3.example", Bucket: "bucket", AccessKey: "a", SecretKey: "secret"}}
	resolution, err := (Resolver{Repository: repository}).Resolve(context.Background(), KindSessions, nil, ActiveState{Backend: BackendEmbedded})
	if err != nil {
		t.Fatal(err)
	}
	if resolution.Evidence.Status != StatusRestartPending || !resolution.Evidence.RestartPending || resolution.Evidence.ActiveType != BackendEmbedded {
		t.Fatalf("evidence=%#v", resolution.Evidence)
	}
}

func TestResolverLegacyRowRevisionIsStableAcrossReads(t *testing.T) {
	repository := &fakeRepository{found: true, loaded: StoredConfiguration{Backend: BackendEmbedded, Source: SourceDB, Status: StatusActive}}
	first, err := (Resolver{Repository: repository}).Resolve(context.Background(), KindResources, nil, ActiveState{})
	if err != nil {
		t.Fatal(err)
	}
	second, err := (Resolver{Repository: repository}).Resolve(context.Background(), KindResources, nil, ActiveState{})
	if err != nil {
		t.Fatal(err)
	}
	if first.Desired.DesiredRevision != legacyRevision || second.Desired.DesiredRevision != legacyRevision {
		t.Fatalf("legacy revision drifted: first=%q second=%q", first.Desired.DesiredRevision, second.Desired.DesiredRevision)
	}
}

func TestPrepareCreateGeneratesRandomRevision(t *testing.T) {
	configuration := StoredConfiguration{Backend: BackendEmbedded, Source: SourceEnvImport, Status: StatusActive}
	first, err := PrepareCreate(KindResources, configuration)
	if err != nil {
		t.Fatal(err)
	}
	second, err := PrepareCreate(KindResources, configuration)
	if err != nil {
		t.Fatal(err)
	}
	if first.DesiredRevision == "" || first.DesiredRevision == legacyRevision || first.DesiredRevision == second.DesiredRevision {
		t.Fatalf("new revisions are not random opaque values: first=%q second=%q", first.DesiredRevision, second.DesiredRevision)
	}
}

func TestResolverImportLoserAdoptsDatabaseWinner(t *testing.T) {
	created := false
	winner := StoredConfiguration{Backend: BackendEmbedded, Source: SourceDB, Status: StatusActive, DesiredRevision: "rev_database"}
	repository := &fakeRepository{createResult: &created, winner: winner}
	legacy := &LegacyInput{Configuration: StoredConfiguration{Backend: BackendFile, Path: "legacy"}}
	resolution, err := (Resolver{Repository: repository}).Resolve(context.Background(), KindSessions, legacy, ActiveState{Backend: BackendEmbedded, Revision: "rev_database"})
	if err != nil {
		t.Fatal(err)
	}
	if resolution.Desired.Backend != BackendEmbedded || resolution.Desired.Source != SourceDB {
		t.Fatalf("loser adopted legacy instead of database winner: %#v", resolution.Desired)
	}
}

func TestResolverConcurrentImportsHaveSingleWinner(t *testing.T) {
	repository := &fakeRepository{}
	legacy := &LegacyInput{Configuration: StoredConfiguration{Backend: BackendFile, Path: "sessions"}}
	results := make(chan Resolution, 8)
	errs := make(chan error, 8)
	var wait sync.WaitGroup
	for index := 0; index < 8; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			resolution, err := (Resolver{Repository: repository}).Resolve(context.Background(), KindSessions, legacy, ActiveState{})
			results <- resolution
			errs <- err
		}()
	}
	wait.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	for range results {
	}
	repository.mu.Lock()
	createCount := repository.createCount
	repository.mu.Unlock()
	if createCount != 1 {
		t.Fatalf("expected one atomic import winner, got %d", createCount)
	}
}

func TestResolverDetectsRevisionChangeForSameBackend(t *testing.T) {
	repository := &fakeRepository{found: true, loaded: StoredConfiguration{
		Backend: BackendFile, Path: "sessions", Source: SourceDB, Status: StatusActive, DesiredRevision: "rev_desired",
	}}
	resolution, err := (Resolver{Repository: repository}).Resolve(context.Background(), KindSessions, nil, ActiveState{Backend: BackendFile, Revision: "rev_active"})
	if err != nil {
		t.Fatal(err)
	}
	if resolution.Evidence.Status != StatusRestartPending || !resolution.Evidence.RestartPending {
		t.Fatalf("same backend revision change was not detected: %#v", resolution.Evidence)
	}
}

func TestValidateBackendScopeAndSecretMarker(t *testing.T) {
	if err := Validate(KindResources, StoredConfiguration{Backend: BackendFile, Path: "resources"}); err == nil {
		t.Fatal("file backend must be rejected for resources")
	}
	if err := Validate(KindSessions, StoredConfiguration{Backend: BackendFile, Path: "sessions"}); err != nil {
		t.Fatal(err)
	}
	if err := Validate(KindResources, StoredConfiguration{Backend: BackendS3, Endpoint: "https://s3.example", Bucket: "bucket", AccessKey: "a", SecretKey: "••••"}); err == nil {
		t.Fatal("legacy mask must not validate as an S3 credential")
	}
}

func TestConditionalWriteCompatibilityFlagIsSessionOnly(t *testing.T) {
	if err := Validate(KindResources, StoredConfiguration{Backend: BackendEmbedded, DisableConditionalWrites: true}); err == nil {
		t.Fatal("resources accepted the session-only conditional-write flag")
	}
	if err := Validate(KindSessions, StoredConfiguration{Backend: BackendEmbedded, DisableConditionalWrites: true}); err != nil {
		t.Fatal(err)
	}
}

func TestValidateRejectsUnsafeOrUnboundedFields(t *testing.T) {
	unsafe := StoredConfiguration{Backend: BackendS3, Endpoint: "https://s3.example\x00", Bucket: "bucket", AccessKey: "a", SecretKey: "secret"}
	if err := Validate(KindResources, unsafe); err == nil {
		t.Fatal("control character must be rejected")
	}
	for _, endpoint := range []string{"https://user:pass@s3.example", "https://s3.example/path?x=1", "https://s3.example#fragment"} {
		configuration := StoredConfiguration{Backend: BackendS3, Endpoint: endpoint, Bucket: "bucket", AccessKey: "a", SecretKey: "secret"}
		if err := Validate(KindResources, configuration); err == nil {
			t.Fatalf("unsafe endpoint accepted: %s", endpoint)
		}
	}
}

func TestInactiveConfigurationMayOmitBackendButStillRejectsUnsafeFields(t *testing.T) {
	if err := Validate(KindSessions, StoredConfiguration{Status: StatusInactive}); err != nil {
		t.Fatal(err)
	}
	if err := Validate(KindSessions, StoredConfiguration{Status: StatusInactive, Path: "bad\x00path"}); err == nil {
		t.Fatal("inactive configuration bypassed field safety validation")
	}
}

func TestResolverPreservesPrimaryErrorWhenEvidenceSaveFails(t *testing.T) {
	primary := errors.New("database unavailable")
	evidenceErr := errors.New("evidence write unavailable")
	repository := &fakeRepository{found: true, loadErr: primary, evidenceErr: evidenceErr}
	_, err := (Resolver{Repository: repository}).Resolve(context.Background(), KindResources, nil, ActiveState{})
	if !errors.Is(err, primary) || !errors.Is(err, evidenceErr) || !errors.Is(err, ErrorCodeEvidenceSaveFailed) {
		t.Fatalf("joined error lost a cause: %v", err)
	}
}

func TestEvidenceApplyFailedRequiresDedicatedErrorCode(t *testing.T) {
	evidence := ResolutionEvidence{Kind: KindResources, Source: SourceDB, Status: StatusApplyFailed, ErrorCode: ErrorCodePersistFailed}
	if err := evidence.Validate(); err == nil {
		t.Fatal("apply-failed evidence accepted a persistence error code")
	}
	evidence.ErrorCode = ErrorCodeApplyFailed
	if err := evidence.Validate(); err != nil {
		t.Fatal(err)
	}
}

func evidenceContains(evidence ResolutionEvidence, value string) bool {
	return strings.Contains(string(evidence.Source), value) || strings.Contains(string(evidence.Status), value) || strings.Contains(string(evidence.DesiredType), value)
}

type fakeRepository struct {
	mu            sync.Mutex
	loaded        StoredConfiguration
	found         bool
	loadErr       error
	saved         StoredConfiguration
	evidence      ResolutionEvidence
	evidenceFound bool
	createResult  *bool
	winner        StoredConfiguration
	evidenceErr   error
	createCount   int
}

func (repository *fakeRepository) Load(context.Context, Kind) (StoredConfiguration, bool, error) {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	return repository.loaded, repository.found, repository.loadErr
}

func (repository *fakeRepository) CreateIfAbsent(_ context.Context, _ Kind, configuration StoredConfiguration) (bool, error) {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	if repository.found {
		return false, nil
	}
	if repository.createResult != nil && !*repository.createResult {
		repository.found = true
		repository.loaded = repository.winner
		return false, nil
	}
	repository.saved = configuration
	repository.loaded = configuration
	repository.found = true
	repository.createCount++
	return true, nil
}

func (repository *fakeRepository) UpdateIfRevision(_ context.Context, kind Kind, expected string, configuration StoredConfiguration) (bool, error) {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	currentRevision := repository.loaded.DesiredRevision
	if !repository.found {
		currentRevision = ""
	} else if normalized, err := Normalize(kind, repository.loaded); err == nil {
		currentRevision = normalized.DesiredRevision
	}
	if currentRevision != expected {
		return false, nil
	}
	repository.loaded = configuration
	repository.saved = configuration
	repository.found = true
	return true, nil
}

func (repository *fakeRepository) LoadEvidence(context.Context, Kind) (ResolutionEvidence, bool, error) {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	return repository.evidence, repository.evidenceFound, nil
}

func (repository *fakeRepository) SaveEvidence(_ context.Context, _ Kind, evidence ResolutionEvidence) error {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	repository.evidence = evidence
	repository.evidenceFound = true
	return repository.evidenceErr
}

func (repository *fakeRepository) SaveEvidenceIfDesiredRevision(_ context.Context, _ Kind, evidence ResolutionEvidence) (bool, error) {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	if !repository.found || repository.loaded.DesiredRevision != evidence.DesiredRevision {
		return false, nil
	}
	repository.evidence = evidence
	repository.evidenceFound = true
	return true, repository.evidenceErr
}
