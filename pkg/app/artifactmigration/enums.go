package artifactmigration

// Backend identifies a credential-free object-store backend identity.
type Backend string

const (
	BackendLocal Backend = "local"
	BackendS3    Backend = "s3"
)

func (backend Backend) Valid() bool { return backend == BackendLocal || backend == BackendS3 }

// State is the closed durable migration lifecycle. New worker execution is
// permitted only while a record is non-terminal.
type State string

const (
	StateLocalActive State = "local_active"
	StateSyncing     State = "syncing"
	StateCatchingUp  State = "catching_up"
	StateVerifying   State = "verifying"
	StateS3Active    State = "s3_active"
	StateApplyFailed State = "apply_failed"
	StateCancelled   State = "cancelled"
)

func (state State) Valid() bool {
	switch state {
	case StateLocalActive, StateSyncing, StateCatchingUp, StateVerifying, StateS3Active, StateApplyFailed, StateCancelled:
		return true
	default:
		return false
	}
}

// Terminal reports whether no worker may claim the migration.
func (state State) Terminal() bool {
	switch state {
	case StateLocalActive, StateS3Active, StateApplyFailed, StateCancelled:
		return true
	default:
		return false
	}
}

// MutationOperation is the only object mutation captured by the journal.
type MutationOperation string

const (
	MutationPut    MutationOperation = "put"
	MutationDelete MutationOperation = "delete"
)

func (operation MutationOperation) Valid() bool {
	return operation == MutationPut || operation == MutationDelete
}

// MutationState records whether a journal entry has been replayed by a later
// worker. The journal never contains object bodies.
type MutationState string

const (
	MutationPending MutationState = "pending"
	MutationApplied MutationState = "applied"
)

func (state MutationState) Valid() bool {
	return state == MutationPending || state == MutationApplied
}
