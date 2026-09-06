package artifactmigration

import "errors"

var (
	ErrInvalidMigration  = errors.New("invalid artifact migration")
	ErrMigrationConflict = errors.New("artifact migration conflict")
	ErrLeaseConflict     = errors.New("artifact migration lease conflict")
	ErrLeaseLost         = errors.New("artifact migration lease lost")
	ErrMutationConflict  = errors.New("artifact migration mutation conflict")
	ErrMigrationBusy     = errors.New("artifact migration remains busy")
)
