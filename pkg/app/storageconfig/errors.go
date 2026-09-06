package storageconfig

import "errors"

var (
	ErrInvalidKind             = errors.New("invalid storage configuration kind")
	ErrInvalidConfiguration    = errors.New("invalid storage configuration")
	ErrInactiveConfiguration   = errors.New("storage configuration is inactive")
	ErrAtomicCreateUnsupported = errors.New("atomic storage configuration create is unsupported")
	ErrCASUnsupported          = errors.New("storage configuration compare-and-swap is unsupported")
	ErrUnsupportedTransition   = errors.New("storage configuration transition is unsupported")
	ErrForbidden               = errors.New("storage configuration administration forbidden")
	ErrInvalidInput            = errors.New("invalid storage configuration input")
	ErrConflict                = errors.New("storage configuration revision conflict")
)
