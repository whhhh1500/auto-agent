package storage

import (
	"context"
	"errors"
)

// ErrAuthorizationEpochChanged reports that a durable authorization snapshot
// is no longer current at the SQL transaction that needs to use it.
var ErrAuthorizationEpochChanged = errors.New("authorization epoch changed")

// AuthorizationEpochReader is the optional read side of the shared SQL
// authorization epoch. A strict recovery coordinator reads an epoch before it
// builds a delivery snapshot, then passes that value to a storage operation
// which compares and locks it in the same transaction as its durable write.
//
// Reading this value alone is not an authorization grant. In particular, a
// non-SQL profile, policy, principal, or capability resolver cannot be made
// transactionally current merely by implementing this interface.
type AuthorizationEpochReader interface {
	AuthorizationEpoch(context.Context) (int64, error)
}
