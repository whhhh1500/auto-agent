package artifactmigration

import (
	"fmt"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

// ValidateMigration validates caller-controlled durable migration fields.
// Repository-owned CAS and timestamp values may be zero before initial save.
func ValidateMigration(migration Migration) error {
	for _, field := range []struct {
		name string
		text string
		max  int
		need bool
	}{
		{"migration id", migration.ID, MaxMigrationIDBytes, true},
		{"desired revision", migration.DesiredRevision, MaxDesiredRevisionBytes, true},
		{"source identity", migration.Source.Identity, MaxBackendIdentityBytes, true},
		{"target identity", migration.Target.Identity, MaxBackendIdentityBytes, true},
		{"listing cursor", migration.ListingCursor, MaxCursorBytes, false},
		{"error code", migration.ErrorCode, MaxErrorCodeBytes, false},
		{"error detail", migration.ErrorDetail, MaxErrorDetailBytes, false},
		{"lease owner", migration.LeaseOwner, MaxLeaseOwnerBytes, false},
	} {
		if err := validateText(field.name, field.text, field.max, field.need); err != nil {
			return err
		}
	}
	if !migration.Source.Backend.Valid() || !migration.Target.Backend.Valid() || migration.Source.Backend == migration.Target.Backend {
		return fmt.Errorf("%w: source and target backends must be distinct supported values", ErrInvalidMigration)
	}
	if !opaqueIdentity(migration.Source.Identity) || !opaqueIdentity(migration.Target.Identity) {
		return fmt.Errorf("%w: backend identity must be an opaque non-endpoint reference", ErrInvalidMigration)
	}
	if !migration.State.Valid() || migration.Generation == 0 {
		return fmt.Errorf("%w: state or generation is invalid", ErrInvalidMigration)
	}
	if migration.VerifiedObjects > migration.CopiedObjects || migration.VerifiedBytes > migration.CopiedBytes {
		return fmt.Errorf("%w: verified counters exceed copied counters", ErrInvalidMigration)
	}
	if migration.State == StateApplyFailed && migration.ErrorCode == "" {
		return fmt.Errorf("%w: apply_failed requires an error code", ErrInvalidMigration)
	}
	if migration.State != StateApplyFailed && migration.ErrorCode != "" {
		return fmt.Errorf("%w: error code is only valid for apply_failed", ErrInvalidMigration)
	}
	if migration.ErrorDetail != "" && migration.ErrorCode == "" {
		return fmt.Errorf("%w: error detail requires an error code", ErrInvalidMigration)
	}
	if migration.State.Terminal() && (migration.LeaseOwner != "" || !migration.LeaseExpiresAt.IsZero()) {
		return fmt.Errorf("%w: terminal migration retains a lease", ErrInvalidMigration)
	}
	if migration.LeaseOwner == "" && !migration.LeaseExpiresAt.IsZero() {
		return fmt.Errorf("%w: lease expiry requires an owner", ErrInvalidMigration)
	}
	for _, timestamp := range []time.Time{migration.LeaseExpiresAt, migration.CreatedAt, migration.UpdatedAt, migration.VerifiedAt, migration.ActivatedAt} {
		if !timestamp.IsZero() && timestamp.Location() != time.UTC {
			return fmt.Errorf("%w: timestamps must use UTC", ErrInvalidMigration)
		}
	}
	return nil
}

// ValidateLease rejects stale-shaped or unbounded lease handles before any
// repository call. The repository still performs the authoritative check.
func ValidateLease(lease Lease) error {
	if err := validateText("lease migration id", lease.MigrationID, MaxMigrationIDBytes, true); err != nil {
		return err
	}
	if err := validateText("lease owner", lease.Owner, MaxLeaseOwnerBytes, true); err != nil {
		return err
	}
	if lease.Generation == 0 || lease.ExpiresAt.IsZero() || lease.ExpiresAt.Location() != time.UTC {
		return fmt.Errorf("%w: lease handle is invalid", ErrInvalidMigration)
	}
	return nil
}

// ValidateMutationToken validates a foreground-write token. The repository
// still verifies that it names the current migration generation.
func ValidateMutationToken(token MutationToken) error {
	if err := validateText("mutation migration id", token.MigrationID, MaxMigrationIDBytes, true); err != nil {
		return err
	}
	if err := validateText("mutation desired revision", token.DesiredRevision, MaxDesiredRevisionBytes, true); err != nil {
		return err
	}
	if token.Generation == 0 {
		return fmt.Errorf("%w: mutation generation is invalid", ErrInvalidMigration)
	}
	return nil
}

// ValidateMutationAppend validates bounded journal metadata. Object bodies are
// intentionally absent from the public contract.
func ValidateMutationAppend(append MutationAppend) error {
	if err := ValidateMutationToken(append.Token); err != nil {
		return err
	}
	if !append.Operation.Valid() {
		return fmt.Errorf("%w: mutation operation is invalid", ErrInvalidMigration)
	}
	for _, field := range []struct {
		name string
		text string
		max  int
		need bool
	}{
		{"object key", append.ObjectKey, MaxMutationObjectKeyBytes, true},
		{"etag", append.ETag, MaxMutationETagBytes, false},
		{"digest", append.Digest, MaxMutationDigestBytes, false},
	} {
		if err := validateText(field.name, field.text, field.max, field.need); err != nil {
			return err
		}
	}
	return nil
}

func validateText(name, value string, maximum int, required bool) error {
	if required && value == "" {
		return fmt.Errorf("%w: %s is required", ErrInvalidMigration, name)
	}
	if len(value) > maximum || !utf8.ValidString(value) || strings.IndexByte(value, 0) >= 0 || strings.IndexFunc(value, unicode.IsControl) >= 0 {
		return fmt.Errorf("%w: %s is invalid", ErrInvalidMigration, name)
	}
	return nil
}

func opaqueIdentity(value string) bool {
	if strings.Contains(value, "://") || strings.ContainsAny(value, "@?#/\\") {
		return false
	}
	return strings.IndexFunc(value, func(r rune) bool {
		return !(r >= 'a' && r <= 'z') && !(r >= 'A' && r <= 'Z') && !(r >= '0' && r <= '9') && r != '.' && r != '_' && r != '-'
	}) < 0
}

func validTTL(ttl time.Duration) bool { return ttl > 0 && ttl <= MaxLeaseTTL }
