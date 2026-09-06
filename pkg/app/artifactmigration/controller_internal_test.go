package artifactmigration

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRenewLeaseIfNeededUsesFreshHandleAndPropagatesFailure(t *testing.T) {
	now := time.Now().UTC()
	base := &renewRepository{lease: Lease{MigrationID: "migration", Generation: 1, Owner: "worker", ExpiresAt: now.Add(time.Minute)}}
	lease, err := renewLeaseIfNeeded(context.Background(), base, base.lease, 2*time.Minute, 5*time.Minute)
	if err != nil || base.renewals != 1 || !lease.ExpiresAt.After(base.lease.ExpiresAt) {
		t.Fatalf("lease=%#v renewals=%d err=%v", lease, base.renewals, err)
	}
	base.err = errors.New("renew failed")
	if _, err := renewLeaseIfNeeded(context.Background(), base, base.lease, 2*time.Minute, 5*time.Minute); !errors.Is(err, base.err) {
		t.Fatalf("renew failure=%v", err)
	}
}

func TestFailUsesDetachedBoundedContext(t *testing.T) {
	repository := &blockingFailureRepository{}
	coordinator := &Coordinator{repository: repository}
	cause := errors.New("copy failed")
	parent, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := coordinator.fail(parent, Lease{}, Migration{StoreRevision: 1}, "copy_failed", cause)
	if !errors.Is(err, cause) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("failure error=%v", err)
	}
}

type renewRepository struct {
	Repository
	lease    Lease
	renewals int
	err      error
}

type blockingFailureRepository struct {
	Repository
}

func (repository *blockingFailureRepository) UpdateLeaseHeld(ctx context.Context, _ Lease, _ uint64, _ Migration) (Migration, error) {
	deadline, ok := ctx.Deadline()
	if !ok {
		return Migration{}, errors.New("failure update has no deadline")
	}
	if remaining := time.Until(deadline); remaining <= 0 || remaining > DurableOperationTimeout {
		return Migration{}, errors.New("failure update has an invalid deadline")
	}
	if err := ctx.Err(); err != nil {
		return Migration{}, errors.New("failure update inherited parent cancellation")
	}
	return Migration{}, context.DeadlineExceeded
}

func (repository *renewRepository) RenewLease(_ context.Context, _ Lease, ttl time.Duration) (Lease, error) {
	repository.renewals++
	if repository.err != nil {
		return Lease{}, repository.err
	}
	return Lease{MigrationID: repository.lease.MigrationID, Generation: repository.lease.Generation, Owner: repository.lease.Owner, ExpiresAt: repository.lease.ExpiresAt.Add(ttl)}, nil
}
