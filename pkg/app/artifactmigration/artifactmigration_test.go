package artifactmigration

import (
	"errors"
	"testing"
	"time"
)

func validMigration() Migration {
	return Migration{ID: "migration-1", DesiredRevision: "rev_1", Source: BackendIdentity{Backend: BackendLocal, Identity: "local-root"}, Target: BackendIdentity{Backend: BackendS3, Identity: "s3-target"}, State: StateSyncing, Generation: 1}
}

func TestValidateMigrationBoundsLifecycleAndLeases(t *testing.T) {
	if err := ValidateMigration(validMigration()); err != nil {
		t.Fatal(err)
	}
	for name, migration := range map[string]Migration{
		"same backend":    func() Migration { value := validMigration(); value.Target.Backend = BackendLocal; return value }(),
		"failed no error": func() Migration { value := validMigration(); value.State = StateApplyFailed; return value }(),
		"terminal lease": func() Migration {
			value := validMigration()
			value.State = StateS3Active
			value.LeaseOwner = "worker"
			value.LeaseExpiresAt = time.Now().UTC()
			return value
		}(),
		"counter regression": func() Migration {
			value := validMigration()
			value.CopiedBytes = 1
			value.VerifiedBytes = 2
			return value
		}(),
		"endpoint identity": func() Migration {
			value := validMigration()
			value.Target.Identity = "https://endpoint.example"
			return value
		}(),
		"control character": func() Migration { value := validMigration(); value.ErrorDetail = "bad\n"; return value }(),
	} {
		if err := ValidateMigration(migration); !errors.Is(err, ErrInvalidMigration) {
			t.Errorf("%s error=%v", name, err)
		}
	}
}

func TestValidateMutationAppendRejectsUnboundedBodiesAndBadToken(t *testing.T) {
	token := MutationToken{MigrationID: "migration-1", Generation: 1, DesiredRevision: "rev_1"}
	append := MutationAppend{Token: token, Operation: MutationPut, ObjectKey: "artifact/a"}
	if err := ValidateMutationAppend(append); err != nil {
		t.Fatal(err)
	}
	append.ObjectKey = string(make([]byte, MaxMutationObjectKeyBytes+1))
	if err := ValidateMutationAppend(append); !errors.Is(err, ErrInvalidMigration) {
		t.Fatalf("long key error=%v", err)
	}
}
