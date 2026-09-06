package storageconfig

// Kind identifies the independently configurable storage domain.
type Kind string

const (
	KindResources Kind = "resources"
	KindSessions  Kind = "sessions"
)

func (kind Kind) Valid() bool {
	return kind == KindResources || kind == KindSessions
}

// Backend identifies a storage implementation. File is deliberately valid
// only for sessions; resources use the embedded filesystem or S3.
type Backend string

const (
	BackendEmbedded Backend = "embedded"
	BackendS3       Backend = "s3"
	BackendFile     Backend = "file"
)

func (backend Backend) ValidFor(kind Kind) bool {
	switch kind {
	case KindResources:
		return backend == BackendEmbedded || backend == BackendS3
	case KindSessions:
		return backend == BackendEmbedded || backend == BackendS3 || backend == BackendFile
	default:
		return false
	}
}

// ConfigSource is non-sensitive provenance for one effective configuration.
type ConfigSource string

const (
	SourceBootstrap      ConfigSource = "bootstrap"
	SourceDB             ConfigSource = "db"
	SourceEnvImport      ConfigSource = "env_import"
	SourceLegacyFallback ConfigSource = "legacy_fallback"
)

func (source ConfigSource) Valid() bool {
	switch source {
	case SourceBootstrap, SourceDB, SourceEnvImport, SourceLegacyFallback:
		return true
	default:
		return false
	}
}

// ConfigStatus describes desired configuration and its application state.
type ConfigStatus string

const (
	StatusActive           ConfigStatus = "active"
	StatusInactive         ConfigStatus = "inactive"
	StatusInvalid          ConfigStatus = "invalid"
	StatusApplyFailed      ConfigStatus = "apply_failed"
	StatusRestartPending   ConfigStatus = "restart_pending"
	StatusMigrationPending ConfigStatus = "migration_pending"
)

func (status ConfigStatus) Valid() bool {
	switch status {
	case StatusActive, StatusInactive, StatusInvalid, StatusApplyFailed, StatusRestartPending, StatusMigrationPending:
		return true
	default:
		return false
	}
}

// ErrorCode is a bounded, non-sensitive resolution error classification.
type ErrorCode string

const (
	ErrorCodeInvalid            ErrorCode = "storage_config_invalid"
	ErrorCodeInactive           ErrorCode = "storage_config_inactive"
	ErrorCodePersistFailed      ErrorCode = "storage_config_persist_failed"
	ErrorCodeApplyFailed        ErrorCode = "storage_config_apply_failed"
	ErrorCodeEvidenceSaveFailed ErrorCode = "storage_evidence_save_failed"
)

func (code ErrorCode) Valid() bool {
	switch code {
	case "", ErrorCodeInvalid, ErrorCodeInactive, ErrorCodePersistFailed, ErrorCodeApplyFailed, ErrorCodeEvidenceSaveFailed:
		return true
	default:
		return false
	}
}

func (code ErrorCode) Error() string { return string(code) }
