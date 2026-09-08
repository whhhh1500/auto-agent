package storageconfig

import (
	"context"
	"time"

	appidentity "github.com/whhhh1500/auto-agent/pkg/app/identity"
)

// Repository is the consumer-owned persistence port for storage configuration.
// Implementations must preserve row presence independently from decode errors.
type Repository interface {
	Load(context.Context, Kind) (StoredConfiguration, bool, error)
	CreateIfAbsent(context.Context, Kind, StoredConfiguration) (bool, error)
	UpdateIfRevision(context.Context, Kind, string, StoredConfiguration) (bool, error)
	LoadEvidence(context.Context, Kind) (ResolutionEvidence, bool, error)
	SaveEvidence(context.Context, Kind, ResolutionEvidence) error
	SaveEvidenceIfDesiredRevision(context.Context, Kind, ResolutionEvidence) (bool, error)
}

// RuntimePort is the narrow runtime seam used by the application service. It
// intentionally exposes only the current active state and an apply operation;
// storage construction remains outside this package.
type RuntimePort interface {
	Active(context.Context, Kind) (ActiveState, error)
	Apply(context.Context, Kind, StoredConfiguration) (ActiveState, error)
}

// MigrationRuntimePort is an optional resource-runtime seam. Implementations
// keep the current backend active while a database-recorded migration runs.
type MigrationRuntimePort interface {
	PrepareResourceMigration(context.Context, StoredConfiguration) (MigrationProgress, error)
	MigrationStatus(context.Context) (MigrationProgress, bool, error)
}

type MigrationCancellationPort interface {
	CancelResourceMigration(context.Context, string) (MigrationProgress, error)
}

// MigrationProgress is a bounded, credential-free projection for transports.
type MigrationProgress struct {
	State                                         string
	CopiedObjects, CopiedBytes                    uint64
	VerifiedObjects, VerifiedBytes                uint64
	ErrorCode                                     string
	CreatedAt, UpdatedAt, VerifiedAt, ActivatedAt time.Time
}

// UseCases is the administrator-facing storage configuration application
// surface. Transport DTOs belong outside this package.
type UseCases interface {
	Get(context.Context, GetCommand) (GetResult, error)
	Put(context.Context, PutCommand) (PutResult, error)
}

type GetCommand struct {
	Actor appidentity.AdminActor
	Kind  Kind
}

// ConfigView deliberately omits SecretKey while retaining a fixed preview and
// presence bit for an administrator UI.
type ConfigView struct {
	Backend                  Backend
	Path                     string
	Endpoint                 string
	Region                   string
	Bucket                   string
	HasAccessKey             bool
	AccessKeyPreview         string
	PathStyle                bool
	DisableConditionalWrites bool
	Source                   ConfigSource
	Status                   ConfigStatus
	DesiredRevision          string
	HasSecret                bool
	SecretPreview            string
}

type GetResult struct {
	Found     bool
	Config    ConfigView
	Evidence  ResolutionEvidence
	Active    ActiveState
	Migration MigrationProgress
}

// PutCommand distinguishes omitted secret, replacement, and explicit clear.
// A zero ExpectedRevision means use the revision read immediately before the
// update; callers that provide one get an explicit optimistic-concurrency
// check.
type PutCommand struct {
	Actor                           appidentity.AdminActor
	Kind                            Kind
	Backend                         Backend
	Path                            string
	Endpoint                        string
	Region                          string
	Bucket                          string
	AccessKey                       string
	AccessKeyPresent                bool
	PathStyle                       bool
	DisableConditionalWrites        bool
	DisableConditionalWritesPresent bool
	Status                          ConfigStatus
	SecretKey                       *string
	ClearSecret                     bool
	ExpectedRevision                string
}

type PutResult struct {
	Config    ConfigView
	Evidence  ResolutionEvidence
	Migration MigrationProgress
}
