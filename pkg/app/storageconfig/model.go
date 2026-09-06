package storageconfig

import (
	"context"
	"time"
)

// StoredConfiguration is the typed at-rest configuration. SecretKey is only
// available to the application/adapter boundary and must never be copied into
// ResolutionEvidence or a transport result.
//
// The adapter preserves the historical JSON field "type" for Backend and
// adds source/status/path fields without changing the two existing setting
// keys.
type StoredConfiguration struct {
	Backend                  Backend
	Path                     string
	Endpoint                 string
	Region                   string
	Bucket                   string
	AccessKey                string
	SecretKey                string
	PathStyle                bool
	DisableConditionalWrites bool
	Source                   ConfigSource
	Status                   ConfigStatus
	DesiredRevision          string
}

// LegacyInput is an explicit caller-supplied legacy environment candidate. The
// application package never reads os.Getenv; callers decide whether this input
// is present and how it was assembled.
type LegacyInput struct {
	Configuration StoredConfiguration
}

// LegacyProvider is evaluated only after the authoritative repository reports
// that the row is absent. Implementations may read environment or other
// legacy sources lazily without leaking those reads into the DB-present path.
type LegacyProvider func(context.Context, Kind) (*LegacyInput, error)

// ActiveState describes the backend currently used by a running consumer.
// It deliberately contains no credentials.
type ActiveState struct {
	Backend  Backend
	Revision string
}

// ResolutionEvidence is safe to persist, audit, or expose to an administrator.
// It intentionally contains no secret, secret hash, endpoint credential, or
// raw configuration payload.
type ResolutionEvidence struct {
	Kind            Kind         `json:"kind"`
	Source          ConfigSource `json:"source"`
	Status          ConfigStatus `json:"status"`
	Found           bool         `json:"found"`
	DesiredType     Backend      `json:"desired_type,omitempty"`
	ActiveType      Backend      `json:"active_type,omitempty"`
	DesiredRevision string       `json:"desired_revision,omitempty"`
	ActiveRevision  string       `json:"active_revision,omitempty"`
	RestartPending  bool         `json:"restart_pending"`
	ErrorCode       ErrorCode    `json:"error_code,omitempty"`
	ObservedAt      time.Time    `json:"observed_at"`
}

// Resolution contains the desired configuration and non-sensitive evidence.
// Desired.SecretKey is for internal construction only and must not cross a
// transport boundary.
type Resolution struct {
	Desired  StoredConfiguration
	Evidence ResolutionEvidence
}
