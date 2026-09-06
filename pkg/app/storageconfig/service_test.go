package storageconfig

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	appidentity "github.com/cc-auto-agent/harness-core/pkg/app/identity"
)

func admin() appidentity.AdminActor {
	return appidentity.AdminActor{AccountID: "admin", Role: appidentity.RoleAdmin}
}

type fakeRuntime struct {
	active   ActiveState
	applyErr error
	apply    []StoredConfiguration
}

func (runtime *fakeRuntime) Active(context.Context, Kind) (ActiveState, error) {
	return runtime.active, nil
}

func (runtime *fakeRuntime) Apply(_ context.Context, _ Kind, configuration StoredConfiguration) (ActiveState, error) {
	runtime.apply = append(runtime.apply, configuration)
	if runtime.applyErr != nil {
		return runtime.active, runtime.applyErr
	}
	runtime.active = ActiveState{Backend: configuration.Backend, Revision: configuration.DesiredRevision}
	return runtime.active, nil
}

func TestServiceGetIsSafeAndIgnoresStaleEvidence(t *testing.T) {
	desired := StoredConfiguration{Backend: BackendS3, Endpoint: "https://s3.example", Bucket: "bucket", AccessKey: "access", SecretKey: "super-secret", Source: SourceDB, Status: StatusActive, DesiredRevision: "rev_current"}
	repository := &fakeRepository{found: true, loaded: desired, evidenceFound: true, evidence: ResolutionEvidence{Kind: KindResources, Source: SourceDB, Status: StatusActive, Found: true, DesiredRevision: "rev_old"}}
	runtime := &fakeRuntime{active: ActiveState{Backend: BackendEmbedded, Revision: "rev_active"}}
	service, err := NewService(repository, runtime)
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.Get(context.Background(), GetCommand{Actor: admin(), Kind: KindResources})
	if err != nil {
		t.Fatal(err)
	}
	if result.Config.HasSecret != true || result.Config.SecretPreview == "" || strings.Contains(result.Config.SecretPreview, "super-secret") {
		t.Fatalf("unsafe config view: %#v", result.Config)
	}
	if result.Evidence.DesiredRevision != "" {
		t.Fatalf("stale evidence was surfaced: %#v", result.Evidence)
	}
}

func TestServiceResourcePutPersistsThenApplies(t *testing.T) {
	repository := &fakeRepository{}
	runtime := &fakeRuntime{}
	service, err := NewService(repository, runtime)
	if err != nil {
		t.Fatal(err)
	}
	secret := "new-secret"
	result, err := service.Put(context.Background(), PutCommand{Actor: admin(), Kind: KindResources, Backend: BackendS3, Endpoint: "https://s3.example", Bucket: "bucket", AccessKey: "access", SecretKey: &secret})
	if err != nil {
		t.Fatal(err)
	}
	if len(runtime.apply) != 1 || result.Evidence.Status != StatusActive || result.Config.HasSecret != true {
		t.Fatalf("put result=%#v apply=%#v", result, runtime.apply)
	}
	if repository.saved.DesiredRevision == "" || repository.evidence.DesiredRevision != repository.saved.DesiredRevision {
		t.Fatalf("revision/evidence mismatch: saved=%#v evidence=%#v", repository.saved, repository.evidence)
	}
}

func TestServiceApplyFailureRetainsOldActiveState(t *testing.T) {
	applyErr := errors.New("backend unavailable")
	repository := &fakeRepository{found: true, loaded: StoredConfiguration{Backend: BackendEmbedded, Source: SourceDB, Status: StatusActive, DesiredRevision: "rev_old"}}
	runtime := &fakeRuntime{active: ActiveState{Backend: BackendEmbedded, Revision: "rev_old"}, applyErr: applyErr}
	service, err := NewService(repository, runtime)
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.Put(context.Background(), PutCommand{Actor: admin(), Kind: KindResources, Backend: BackendS3, Endpoint: "https://s3.example", Bucket: "bucket", AccessKey: "access", SecretKey: stringPtr("secret"), ExpectedRevision: "rev_old"})
	if !errors.Is(err, ErrApplyFailed) || !errors.Is(err, applyErr) {
		t.Fatalf("apply failure was not preserved: %v", err)
	}
	if runtime.active.Backend != BackendEmbedded || runtime.active.Revision != "rev_old" {
		t.Fatalf("active runtime was replaced after failed apply: %#v", runtime.active)
	}
	if repository.evidence.Status != StatusApplyFailed || repository.evidence.ErrorCode != ErrorCodeApplyFailed || repository.evidence.ActiveRevision != "rev_old" {
		t.Fatalf("apply failure evidence=%#v", repository.evidence)
	}
}

func TestServiceSessionPutWritesRestartPendingWithoutApply(t *testing.T) {
	repository := &fakeRepository{found: true, loaded: StoredConfiguration{Backend: BackendFile, Path: "old", DisableConditionalWrites: true, Source: SourceDB, Status: StatusActive, DesiredRevision: "rev_old"}}
	runtime := &fakeRuntime{active: ActiveState{Backend: BackendFile, Revision: "rev_old"}}
	service, err := NewService(repository, runtime)
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.Put(context.Background(), PutCommand{Actor: admin(), Kind: KindSessions, Backend: BackendFile, Path: "new", ExpectedRevision: "rev_old"})
	if err != nil {
		t.Fatal(err)
	}
	if len(runtime.apply) != 0 || result.Evidence.Status != StatusRestartPending || !result.Evidence.RestartPending {
		t.Fatalf("session put unexpectedly applied or missed pending: result=%#v apply=%#v", result, runtime.apply)
	}
	if !repository.saved.DisableConditionalWrites {
		t.Fatal("omitted session conditional-write flag was cleared")
	}
}

func TestServicePutRejectsRevisionConflictAndInactiveOverwrite(t *testing.T) {
	conflictRepo := &fakeRepository{found: true, loaded: StoredConfiguration{Backend: BackendEmbedded, Source: SourceDB, Status: StatusActive, DesiredRevision: "rev_current"}}
	service, err := NewService(conflictRepo, &fakeRuntime{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.Put(context.Background(), PutCommand{Actor: admin(), Kind: KindResources, Backend: BackendEmbedded, ExpectedRevision: "rev_wrong"})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("expected revision conflict, got %v", err)
	}
	inactiveRepo := &fakeRepository{found: true, loaded: StoredConfiguration{Status: StatusInactive, Source: SourceDB}}
	inactiveService, err := NewService(inactiveRepo, &fakeRuntime{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = inactiveService.Put(context.Background(), PutCommand{Actor: admin(), Kind: KindResources, Backend: BackendEmbedded})
	if !errors.Is(err, ErrInactiveConfiguration) || inactiveRepo.saved.Backend != "" {
		t.Fatalf("inactive row was overwritten: err=%v saved=%#v", err, inactiveRepo.saved)
	}
}

func TestServicePutSecretOmissionAndClear(t *testing.T) {
	repository := &fakeRepository{found: true, loaded: StoredConfiguration{Backend: BackendS3, Endpoint: "https://s3.example", Bucket: "bucket", AccessKey: "access", SecretKey: "old-secret", Source: SourceDB, Status: StatusActive, DesiredRevision: "rev_old"}}
	service, err := NewService(repository, &fakeRuntime{})
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.Put(context.Background(), PutCommand{Actor: admin(), Kind: KindResources, Backend: BackendS3, Endpoint: "https://s3.example", Bucket: "bucket", AccessKey: "access", ExpectedRevision: "rev_old"})
	if err != nil || !result.Config.HasSecret || repository.saved.SecretKey != "old-secret" {
		t.Fatalf("omitted secret was not preserved: err=%v saved=%#v", err, repository.saved)
	}
	expected := repository.saved.DesiredRevision
	_, err = service.Put(context.Background(), PutCommand{Actor: admin(), Kind: KindResources, Backend: BackendS3, Endpoint: "https://s3.example", Bucket: "bucket", AccessKey: "access", Status: StatusInactive, ClearSecret: true, ExpectedRevision: expected})
	if !errors.Is(err, ErrUnsupportedTransition) || repository.saved.SecretKey != "old-secret" {
		t.Fatalf("active S3 credential transition was not rejected: err=%v saved=%#v", err, repository.saved)
	}
}

func TestServiceSerializesConcurrentPutsPerKind(t *testing.T) {
	repository := &fakeRepository{found: true, loaded: StoredConfiguration{Backend: BackendEmbedded, Source: SourceDB, Status: StatusActive, DesiredRevision: "rev_old"}}
	service, err := NewService(repository, &fakeRuntime{})
	if err != nil {
		t.Fatal(err)
	}
	var wait sync.WaitGroup
	results := make(chan error, 2)
	for index := 0; index < 2; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, putErr := service.Put(context.Background(), PutCommand{Actor: admin(), Kind: KindResources, Backend: BackendEmbedded, ExpectedRevision: "rev_old"})
			results <- putErr
		}()
	}
	wait.Wait()
	close(results)
	successes := 0
	conflicts := 0
	for putErr := range results {
		switch {
		case putErr == nil:
			successes++
		case errors.Is(putErr, ErrConflict):
			conflicts++
		default:
			t.Fatalf("unexpected concurrent put error: %v", putErr)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("expected one success and one conflict, got successes=%d conflicts=%d", successes, conflicts)
	}
}

func stringPtr(value string) *string { return &value }
