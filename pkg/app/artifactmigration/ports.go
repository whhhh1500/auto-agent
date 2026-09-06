package artifactmigration

import (
	"context"
	"time"
)

// ArtifactRuntime is the single composition seam used by the coordinator.
// Implementations bind concrete storage backends outside the application
// package; the coordinator only sees bounded copy/verify operations and an
// atomic activation barrier.
type ArtifactRuntime interface {
	// Bind attaches a new or already-existing migration generation without
	// copying data or changing the active route.
	Bind(context.Context, string, MutationRecorder) (ArtifactCopy, error)
	// RestoreActive makes the configured target route authoritative after a
	// restart whose durable state is already active.
	RestoreActive(context.Context) error
	// Activate runs apply under a short barrier and switches the route only
	// when apply returns nil.
	Activate(context.Context, string, func(context.Context, ArtifactCopy) error) error
	// Cancel abandons a candidate without changing the active route.
	Cancel(string)
}

// ArtifactCopy is the bounded data-plane seam returned by ArtifactRuntime.
// Implementations may use local files, S3, or another object system without
// exposing those types to the application coordinator.
type ArtifactCopy interface {
	Copy(context.Context, string, string, ProgressFunc) (TransferStats, error)
	Reconcile(context.Context, string, ProgressFunc) (TransferStats, error)
	ApplyAndVerify(context.Context, []KeyChange) error
}

// ProgressFunc is called synchronously after a bounded copy/reconcile batch.
// A non-nil error stops the operation immediately.
type ProgressFunc func(context.Context, TransferProgress) error

type TransferProgress struct {
	LastKey string
	Objects int
	Bytes   int64
}

type TransferStats struct {
	LastKey string
	Objects int
	Bytes   int64
}

type KeyChange struct {
	Key     string
	Deleted bool
}

// MutationEvidence is metadata-only evidence emitted after an authoritative
// foreground mutation commits. The migration binding supplies its identity.
type MutationEvidence struct {
	Operation MutationOperation
	Key       string
	ETag      string
	Digest    string
	Size      int64
}

type MutationRecorder interface {
	Record(context.Context, MutationEvidence) error
}

// Repository owns the one durable resources migration record, its fenced
// worker lease, and its metadata-only mutation journal. Implementations must
// reject stale CAS/generation/lease writes; they must not start workers.
type Repository interface {
	Load(context.Context) (Migration, bool, error)
	CompareAndSwap(context.Context, uint64, Migration) (Migration, error)
	UpdateLeaseHeld(context.Context, Lease, uint64, Migration) (Migration, error)
	ClaimLease(context.Context, string, time.Duration) (Lease, error)
	RenewLease(context.Context, Lease, time.Duration) (Lease, error)
	ReleaseLease(context.Context, Lease) error
	ValidateLease(context.Context, Lease) error
	AppendMutation(context.Context, MutationAppend) (Mutation, error)
	FailMutation(context.Context, MutationToken, string) error
	MarkMutation(context.Context, Lease, uint64, MutationState) error
	MarkMutationsThrough(context.Context, Lease, uint64) error
	ListMutations(context.Context, string, uint64, int) ([]Mutation, error)
}
