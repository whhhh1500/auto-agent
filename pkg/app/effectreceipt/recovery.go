package effectreceipt

import (
	"context"
	"errors"
	"strings"
	"unicode/utf8"

	"github.com/whhhh1500/auto-agent/pkg/core"
)

var (
	ErrRecoveryReader           = errors.New("external effect recovery reader failure")
	ErrRecoveryReaderPanic      = errors.New("external effect recovery reader panic")
	ErrRecoveryAuthorizer       = errors.New("external effect recovery authorization failure")
	ErrRecoveryAuthorizerPanic  = errors.New("external effect recovery authorization panic")
	ErrRecoveryDriverMissing    = errors.New("external effect recovery driver is not registered")
	ErrRecoveryDriver           = errors.New("external effect recovery driver failure")
	ErrRecoveryDriverPanic      = errors.New("external effect recovery driver panic")
	ErrRecoveryStore            = errors.New("external effect recovery store failure")
	ErrRecoveryStorePanic       = errors.New("external effect recovery store panic")
	ErrRecoveryCoordinatorPanic = errors.New("external effect recovery coordinator panic")
)

// RecoveryAuthorizer re-checks the caller's current authority before a
// recovery read-back. Returning false intentionally skips that row; returning
// an error is transient and leaves the row's cursor unadvanced.
type RecoveryAuthorizer interface {
	AuthorizeEffectRecovery(context.Context, RecoveryRecord) (bool, error)
}

// RecoveryPageResult describes one bounded driver page. Next advances only
// through records that were denied or reconciled without a transient failure.
// Callers own scheduling and retry by passing Next back to RecoverPage.
type RecoveryPageResult struct {
	Next         RecoveryCursor
	Examined     int
	Reconciled   int
	Unauthorized int
}

// RecoveryCoordinator turns a registry-selected recovery page into bound
// Service.Reconcile calls. It has no Dispatch path and does not expose a
// provider fallback when a DriverRef has no registration.
type RecoveryCoordinator struct {
	reader     RecoveryReader
	store      Store
	registry   *DriverRegistry
	authorizer RecoveryAuthorizer
}

// NewRecoveryCoordinator creates the small application-layer recovery seam.
func NewRecoveryCoordinator(reader RecoveryReader, store Store, registry *DriverRegistry, authorizer RecoveryAuthorizer) (*RecoveryCoordinator, error) {
	if isNilInterface(reader) || isNilInterface(store) || registry == nil || isNilInterface(authorizer) {
		return nil, ErrInvalidService
	}
	return &RecoveryCoordinator{reader: reader, store: store, registry: registry, authorizer: authorizer}, nil
}

// Drivers returns a stable registration snapshot for caller-owned schedulers.
func (c *RecoveryCoordinator) Drivers() []DriverRef {
	if c == nil || c.registry == nil {
		return nil
	}
	return c.registry.Refs()
}

// RecoverPage reconciles one bounded page owned by an exact registered driver.
// Reader, authorizer, driver, or store faults are converted to fixed errors
// and return the last safely completed cursor, never the reader's unprocessed
// page cursor.
func (c *RecoveryCoordinator) RecoverPage(ctx context.Context, query RecoveryQuery, cursor RecoveryCursor, limit int) (result RecoveryPageResult, err error) {
	if ctx == nil {
		return RecoveryPageResult{Next: cursor}, ErrInvalidContext
	}
	if c == nil || c.reader == nil || c.store == nil || c.registry == nil || c.authorizer == nil {
		return RecoveryPageResult{Next: cursor}, ErrInvalidService
	}
	if !validDriverRef(query.Driver) || !validRecoveryCursor(cursor) || limit < 1 || limit > MaxRecoveryPageSize {
		return RecoveryPageResult{Next: cursor}, ErrInvalidRequest
	}
	driver, found, lookupErr := c.registry.Lookup(query.Driver)
	if lookupErr != nil {
		return RecoveryPageResult{Next: cursor}, ErrRecoveryDriver
	}
	if !found {
		return RecoveryPageResult{Next: cursor}, ErrRecoveryDriverMissing
	}
	service, serviceErr := NewService(c.store, driver)
	if serviceErr != nil {
		return RecoveryPageResult{Next: cursor}, ErrRecoveryStore
	}
	records, _, readErr := c.readPage(ctx, query, cursor, limit)
	if readErr != nil {
		return RecoveryPageResult{Next: cursor}, readErr
	}
	result.Next = cursor
	for _, durable := range records {
		next, valid := recoveryCursorFromRecord(durable)
		if !valid || !recoveryCursorAfter(next, result.Next) || durable.Intent.Driver != query.Driver || !unresolvedState(durable.State) || ValidateRecord(durable.Record) != nil {
			return result, ErrRecoveryReader
		}
		result.Examined++
		allowed, authorizeErr := c.authorize(ctx, durable)
		if authorizeErr != nil {
			return result, authorizeErr
		}
		if !allowed {
			result.Unauthorized++
			result.Next = next
			continue
		}
		if _, reconcileErr := reconcileRecovery(ctx, service, durable.Intent); reconcileErr != nil {
			return result, reconcileErr
		}
		result.Reconciled++
		result.Next = next
	}
	return result, nil
}

func (c *RecoveryCoordinator) readPage(ctx context.Context, query RecoveryQuery, cursor RecoveryCursor, limit int) (records []RecoveryRecord, next RecoveryCursor, err error) {
	defer func() {
		if recover() != nil {
			records = nil
			next = RecoveryCursor{}
			err = ErrRecoveryReaderPanic
		}
	}()
	records, next, err = c.reader.ListUnresolved(ctx, query, cursor, limit)
	if err != nil {
		return nil, RecoveryCursor{}, ErrRecoveryReader
	}
	return records, next, nil
}

func (c *RecoveryCoordinator) authorize(ctx context.Context, record RecoveryRecord) (allowed bool, err error) {
	defer func() {
		if recover() != nil {
			err = ErrRecoveryAuthorizerPanic
		}
	}()
	allowed, err = c.authorizer.AuthorizeEffectRecovery(ctx, record)
	if err != nil {
		return false, ErrRecoveryAuthorizer
	}
	return allowed, nil
}

func reconcileRecovery(ctx context.Context, service *Service, intent Intent) (record Record, err error) {
	defer func() {
		if recover() != nil {
			err = ErrRecoveryCoordinatorPanic
		}
	}()
	record, err = service.Reconcile(ctx, intent)
	if err != nil {
		return Record{}, classifyRecoveryError(err)
	}
	return record, nil
}

func classifyRecoveryError(err error) error {
	switch {
	case errors.Is(err, ErrReadBackPanic), errors.Is(err, ErrDriverPanic):
		return ErrRecoveryDriverPanic
	case errors.Is(err, ErrReadBack), errors.Is(err, ErrReadBackMismatch), errors.Is(err, ErrInvalidObservation), errors.Is(err, ErrDriver):
		return ErrRecoveryDriver
	case errors.Is(err, ErrStorePanic):
		return ErrRecoveryStorePanic
	case errors.Is(err, ErrStore), errors.Is(err, ErrConflict), errors.Is(err, ErrNotFound), errors.Is(err, ErrInvalidState), errors.Is(err, ErrInvalidIntent):
		return ErrRecoveryStore
	default:
		return ErrRecoveryStore
	}
}

func unresolvedState(state State) bool {
	return state == StateDispatching || state == StateAccepted || state == StateUnknown
}

func validRecoveryCursor(cursor RecoveryCursor) bool {
	values := []string{cursor.TenantID, cursor.SubjectID, cursor.SessionID, cursor.RunID, cursor.CallID}
	empty := 0
	for _, value := range values {
		if value == "" {
			empty++
		}
	}
	if empty == len(values) {
		return cursor.UpdatedAt.IsZero()
	}
	if empty != 0 || cursor.UpdatedAt.IsZero() || cursor.UpdatedAt.UnixMilli() <= 0 ||
		!validRecoveryPrincipalPart(cursor.TenantID) || !validRecoveryPrincipalPart(cursor.SubjectID) ||
		core.ValidateSessionID(cursor.SessionID) != nil || core.ValidateRunID(cursor.RunID) != nil {
		return false
	}
	return validRecoveryCallID(cursor.CallID)
}

func validRecoveryPrincipalPart(value string) bool {
	return strings.TrimSpace(value) != "" && len(value) <= 512 && utf8.ValidString(value) && !strings.ContainsAny(value, "\r\n\x00")
}

func validRecoveryCallID(value string) bool {
	return strings.TrimSpace(value) != "" && len(value) <= 256 && utf8.ValidString(value) && !strings.ContainsAny(value, "\r\n\x00")
}

func recoveryCursorFromRecord(record RecoveryRecord) (RecoveryCursor, bool) {
	if record.UpdatedAt.IsZero() || record.UpdatedAt.UnixMilli() <= 0 {
		return RecoveryCursor{}, false
	}
	invocation := record.Intent.Invocation
	cursor := RecoveryCursor{UpdatedAt: record.UpdatedAt.UTC(), TenantID: invocation.TenantID, SubjectID: invocation.SubjectID,
		SessionID: invocation.SessionID, RunID: invocation.RunID, CallID: invocation.CallID}
	return cursor, validRecoveryCursor(cursor)
}

func recoveryCursorAfter(left, right RecoveryCursor) bool {
	if right.UpdatedAt.IsZero() {
		return true
	}
	if left.UpdatedAt.After(right.UpdatedAt) {
		return true
	}
	if !left.UpdatedAt.Equal(right.UpdatedAt) {
		return false
	}
	leftValues := []string{left.TenantID, left.SubjectID, left.SessionID, left.RunID, left.CallID}
	rightValues := []string{right.TenantID, right.SubjectID, right.SessionID, right.RunID, right.CallID}
	for index := range leftValues {
		if leftValues[index] == rightValues[index] {
			continue
		}
		return leftValues[index] > rightValues[index]
	}
	return false
}
