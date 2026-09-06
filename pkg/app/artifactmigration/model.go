package artifactmigration

import "time"

const (
	// ResourceKey is the one resource-object migration slot. A SQL adapter
	// enforces this invariant so two migrations cannot run concurrently.
	ResourceKey = "resources"

	MaxMigrationIDBytes       = 128
	MaxDesiredRevisionBytes   = 128
	MaxBackendIdentityBytes   = 256
	MaxCursorBytes            = 4096
	MaxErrorCodeBytes         = 128
	MaxErrorDetailBytes       = 1024
	MaxLeaseOwnerBytes        = 128
	MaxMutationObjectKeyBytes = 4096
	MaxMutationETagBytes      = 256
	MaxMutationDigestBytes    = 256
	MaxMutationListLimit      = 256
	MaxArtifactActivationKeys = 256
	MaxLeaseTTL               = 5 * time.Minute
)

// BackendIdentity is an opaque credential-free stable backend reference. It
// must never contain a complete endpoint, credential, or object key.
type BackendIdentity struct {
	Backend  Backend
	Identity string
}

// Migration is the durable progress record. StoreRevision is the adapter CAS
// token and Generation fences a worker after a desired-revision replacement.
// Timestamps and lease fields are populated by the repository.
type Migration struct {
	ID              string
	DesiredRevision string
	Source          BackendIdentity
	Target          BackendIdentity
	State           State
	Generation      uint64
	StoreRevision   uint64

	ListingCursor     string
	MutationHighWater uint64
	// Copied counters are cumulative lower bounds for data proven copied to
	// the target. They can be raised by a later full verification because
	// journal replay may add objects after the initial streaming copy.
	CopiedObjects   uint64
	CopiedBytes     uint64
	VerifiedObjects uint64
	VerifiedBytes   uint64
	ErrorCode       string
	ErrorDetail     string
	LeaseOwner      string
	LeaseExpiresAt  time.Time
	CreatedAt       time.Time
	UpdatedAt       time.Time
	VerifiedAt      time.Time
	ActivatedAt     time.Time
}

// Lease identifies one generation-fenced worker claim. It is intentionally
// separate from Migration so callers cannot manufacture a writable record by
// editing lease fields in a loaded value.
type Lease struct {
	MigrationID string
	Generation  uint64
	Owner       string
	ExpiresAt   time.Time
}

// MutationToken is the non-lease handle issued to normal object-store write
// paths while a migration generation is active. It deliberately has no worker
// owner or expiry, so foreground writes cannot fail when a worker renews.
type MutationToken struct {
	MigrationID     string
	Generation      uint64
	DesiredRevision string
}

// MutationToken returns the foreground-write token for this migration.
func (migration Migration) MutationToken() MutationToken {
	return MutationToken{MigrationID: migration.ID, Generation: migration.Generation, DesiredRevision: migration.DesiredRevision}
}

// Mutation is metadata-only mutation evidence. Digest and ETag are optional
// for deletes and for stores that cannot provide them.
type Mutation struct {
	MigrationID string
	Sequence    uint64
	Operation   MutationOperation
	ObjectKey   string
	ETag        string
	Digest      string
	State       MutationState
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// MutationAppend is a foreground-write journal append. Sequence allocation is
// atomic in the repository; callers never pre-read the migration high-water.
type MutationAppend struct {
	Token     MutationToken
	Operation MutationOperation
	ObjectKey string
	ETag      string
	Digest    string
}
