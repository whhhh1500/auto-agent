package storage

import (
	"context"
	"fmt"
	"strings"
	"time"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
)

// SessionLeaser is the distributed mutual exclusion primitive for
// multi-instance deployments: exactly one instance works a session at a
// time. Leases expire, so a crashed holder cannot deadlock a session.
// The in-process per-session lock of the HTTP adapter remains the first
// gate; the lease is the cross-instance gate behind it.
type SessionLeaser interface {
	// AcquireSessionLease creates a lease or takes over an expired lease.
	// A live lease, including one with the same holder, is not acquired again;
	// callers renew an existing lease explicitly through RenewSessionLease.
	AcquireSessionLease(ctx context.Context, sessionID, holder string, ttl time.Duration) (bool, error)
	// RenewSessionLease extends a live lease only when holder still owns it.
	// It returns false after expiry or ownership loss.
	RenewSessionLease(ctx context.Context, sessionID, holder string, ttl time.Duration) (bool, error)
	// ReleaseSessionLease drops the lease if this holder owns it.
	ReleaseSessionLease(ctx context.Context, sessionID, holder string) error
}

var (
	sqlLeaseSteal  = sqlQuery{"UPDATE session_leases SET holder = ?, expires_at = ? WHERE session_id = ? AND expires_at <= ?"}
	sqlLeaseInsert = sqlQuery{"INSERT INTO session_leases (session_id, holder, expires_at) VALUES (?, ?, ?)"}
	sqlLeaseRenew  = sqlQuery{"UPDATE session_leases SET expires_at = ? WHERE session_id = ? AND holder = ? AND expires_at > ?"}
	sqlLeaseDelete = sqlQuery{"DELETE FROM session_leases WHERE session_id = ? AND holder = ?"}
)

// AcquireSessionLease implements SessionLeaser on the SQL backend.
func (s *SQLSessionStore) AcquireSessionLease(ctx context.Context, sessionID, holder string, ttl time.Duration) (bool, error) {
	if err := core.ValidateSessionID(sessionID); err != nil {
		return false, err
	}
	if err := validateLeaseHolder(holder); err != nil {
		return false, err
	}
	if ttl <= 0 {
		return false, fmt.Errorf("session lease ttl must be positive")
	}
	nowTime := time.Now().UTC()
	now := nowTime.UnixMilli()
	expiry := nowTime.Add(ttl).UnixMilli()
	result, err := s.db.ExecContext(ctx, sqlLeaseSteal.bind(s.dialect),
		holder, expiry, sessionID, now,
	)
	if err != nil {
		return false, err
	}
	if affected, err := result.RowsAffected(); err == nil && affected > 0 {
		return true, nil
	}
	// No row matched: either the lease is healthy elsewhere or absent.
	if _, err := s.db.ExecContext(ctx, sqlLeaseInsert.bind(s.dialect), sessionID, holder, expiry); err != nil {
		if isDuplicateConstraint(err) {
			return false, nil // a live lease from another holder
		}
		return false, err
	}
	return true, nil
}

// RenewSessionLease implements SessionLeaser on the SQL backend. Renewal is
// deliberately distinct from acquisition: once a lease has expired, its old
// holder cannot revive it and must stop work even if no replacement has yet
// acquired the row.
func (s *SQLSessionStore) RenewSessionLease(ctx context.Context, sessionID, holder string, ttl time.Duration) (bool, error) {
	if err := core.ValidateSessionID(sessionID); err != nil {
		return false, err
	}
	if err := validateLeaseHolder(holder); err != nil {
		return false, err
	}
	if ttl <= 0 {
		return false, fmt.Errorf("session lease ttl must be positive")
	}
	nowTime := time.Now().UTC()
	result, err := s.db.ExecContext(ctx, sqlLeaseRenew.bind(s.dialect),
		nowTime.Add(ttl).UnixMilli(), sessionID, holder, nowTime.UnixMilli(),
	)
	if err != nil {
		return false, err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return affected > 0, nil
}

// ReleaseSessionLease implements SessionLeaser on the SQL backend.
func (s *SQLSessionStore) ReleaseSessionLease(ctx context.Context, sessionID, holder string) error {
	if err := core.ValidateSessionID(sessionID); err != nil {
		return err
	}
	if err := validateLeaseHolder(holder); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, sqlLeaseDelete.bind(s.dialect), sessionID, holder)
	return err
}

func validateLeaseHolder(holder string) error {
	if strings.TrimSpace(holder) == "" {
		return fmt.Errorf("session lease holder is empty")
	}
	return validateSQLTextFilter("session lease holder", holder)
}
