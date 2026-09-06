package storage

import (
	"database/sql"
	"fmt"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
)

// atomicSessionFenceDomain identifies one SQL database handle and dialect on
// which the session, run-control, queue, and session-lease rows participate in
// the same fenced append transaction. It is intentionally sealed within this
// package: an arbitrary adapter cannot mint a matching declaration from a
// database handle alone.
//
// Equality is deliberately strict. Distinct *sql.DB handles are rejected even
// when they may happen to point at the same server, because the server cannot
// prove their transaction configuration is equivalent at startup. The core SQL
// stores use unqualified table names, so a PostgreSQL deployment must also keep
// one stable shared schema/search_path for the whole pool. The value does not
// attempt to discover or prove that operational invariant.
type atomicSessionFenceDomain struct {
	db      *sql.DB
	dialect SQLDialect
}

func newAtomicSessionFenceDomain(db *sql.DB, dialect SQLDialect) (atomicSessionFenceDomain, error) {
	if db == nil {
		return atomicSessionFenceDomain{}, fmt.Errorf("atomic session fence domain requires a database handle")
	}
	if err := validateSQLDialect(dialect); err != nil {
		return atomicSessionFenceDomain{}, err
	}
	return atomicSessionFenceDomain{db: db, dialect: dialect}, nil
}

func (d atomicSessionFenceDomain) valid() bool {
	return d.db != nil && validateSQLDialect(d.dialect) == nil
}

func (d atomicSessionFenceDomain) equal(other atomicSessionFenceDomain) bool {
	return d.valid() && other.valid() && d.db == other.db && d.dialect == other.dialect
}

// atomicSessionFenceDomainProvider is sealed by its unexported method. Native
// SQL stores implement it directly; a transparent wrapper that embeds one of
// those stores retains the same capability by method promotion. Independently
// implemented adapters fail closed rather than asserting an easily forged
// domain token.
type atomicSessionFenceDomainProvider interface {
	atomicSessionFenceDomain() atomicSessionFenceDomain
}

// ValidateQueuedSessionFenceDomain fail-closes durable queued execution unless
// Sessions can append through FencedSessionAppender and Sessions, RunQueue, and
// SessionLeaser all explicitly prove the same atomic SQL fence domain.
//
// The helper intentionally has no cross-store fallback. File, Memory, S3, and
// separately configured SQL stores must not be used as the Session backend of
// server.Config.RunQueue until they can provide an equally atomic protocol.
func ValidateQueuedSessionFenceDomain(sessions core.SessionStore, queue RunQueueStore, leaser SessionLeaser) error {
	if sessions == nil || queue == nil || leaser == nil {
		return fmt.Errorf("queued session fence domain requires sessions, run queue, and session leaser")
	}
	if _, ok := sessions.(FencedSessionAppender); !ok {
		return fmt.Errorf("queued session fence requires sessions to implement %T", (*FencedSessionAppender)(nil))
	}
	if _, ok := sessions.(fencedSessionRepairAppender); !ok {
		return fmt.Errorf("queued session fence requires native storage fenced interrupted-session repair")
	}
	sessionProvider, ok := sessions.(atomicSessionFenceDomainProvider)
	if !ok {
		return fmt.Errorf("queued session fence requires sessions backed by the storage SQL fence domain")
	}
	queueProvider, ok := queue.(atomicSessionFenceDomainProvider)
	if !ok {
		return fmt.Errorf("queued session fence requires run queue backed by the storage SQL fence domain")
	}
	leaseProvider, ok := leaser.(atomicSessionFenceDomainProvider)
	if !ok {
		return fmt.Errorf("queued session fence requires session leaser backed by the storage SQL fence domain")
	}
	sessionDomain := sessionProvider.atomicSessionFenceDomain()
	queueDomain := queueProvider.atomicSessionFenceDomain()
	leaseDomain := leaseProvider.atomicSessionFenceDomain()
	if !sessionDomain.valid() || !queueDomain.valid() || !leaseDomain.valid() {
		return fmt.Errorf("queued session fence atomic domain is invalid")
	}
	if !sessionDomain.equal(queueDomain) || !sessionDomain.equal(leaseDomain) {
		return fmt.Errorf("queued session fence requires sessions, run queue, and session leaser to share one SQL database handle and dialect")
	}
	return nil
}

// ValidateAuthorizationEpochSQLAuthority proves only the SQL half of a future
// strict recovery configuration: Session events, queue claims, Session leases,
// the tool journal, and the authorization epoch must share one sealed SQL
// authority. It deliberately cannot bless an arbitrary in-process or remote
// profile, policy, capability, or principal resolver; a future recovery
// feature must reject those unless it has an epoch-bound control-plane adapter.
func ValidateAuthorizationEpochSQLAuthority(sessions core.SessionStore, queue RunQueueStore, leaser SessionLeaser, journal core.ToolInvocationJournal) error {
	if err := ValidateQueuedSessionFenceDomain(sessions, queue, leaser); err != nil {
		return err
	}
	if _, ok := sessions.(AuthorizationEpochReader); !ok {
		return fmt.Errorf("authorization epoch requires sessions to implement %T", (*AuthorizationEpochReader)(nil))
	}
	sessionProvider := sessions.(atomicSessionFenceDomainProvider)
	journalProvider, ok := journal.(atomicSessionFenceDomainProvider)
	if !ok {
		return fmt.Errorf("authorization epoch requires a tool journal backed by the storage SQL authority")
	}
	if !sessionProvider.atomicSessionFenceDomain().equal(journalProvider.atomicSessionFenceDomain()) {
		return fmt.Errorf("authorization epoch requires sessions and tool journal to share one SQL database handle and dialect")
	}
	return nil
}

func (s *SQLSessionStore) atomicSessionFenceDomain() atomicSessionFenceDomain {
	domain, _ := newAtomicSessionFenceDomain(s.db, s.dialect)
	return domain
}

func (s *SQLRunControlStore) atomicSessionFenceDomain() atomicSessionFenceDomain {
	domain, _ := newAtomicSessionFenceDomain(s.db, s.dialect)
	return domain
}
