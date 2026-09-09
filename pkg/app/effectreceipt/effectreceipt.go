// Package effectreceipt defines a provider-neutral, durable contract for
// externally visible effects. It deliberately records only fixed identities
// and digests; adapters retain any provider-specific receipt material.
package effectreceipt

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/whhhh1500/auto-agent/pkg/core"
)

const (
	// ProtocolV1 identifies the canonical identity encoding used by this
	// package. Changing it requires a new protocol version.
	ProtocolV1 = "harness.external-effect/v1"

	// MaxTargetBytes and MaxPayloadBytes bound the ephemeral material that is
	// hashed before dispatch. Neither value is retained in Record.
	MaxTargetBytes  = 4 << 10
	MaxPayloadBytes = 256 << 10
	// MaxDispatchAttempts bounds the persisted number of provider crossings for
	// one intent. A Store must reject a record beyond this limit.
	MaxDispatchAttempts = 1024
	// MaxRecoveryPageSize bounds discovery queries over non-terminal effects.
	MaxRecoveryPageSize = 256

	maxDriverIDBytes      = 128
	maxDriverVersionBytes = 64
)

var (
	ErrInvalidIntent      = errors.New("invalid external effect intent")
	ErrInvalidRequest     = errors.New("invalid external effect request")
	ErrInvalidService     = errors.New("invalid external effect service")
	ErrInvalidContext     = errors.New("invalid external effect context")
	ErrInvalidState       = errors.New("invalid external effect state")
	ErrInvalidSubmission  = errors.New("invalid external effect submission")
	ErrInvalidObservation = errors.New("invalid external effect observation")
	ErrConflict           = errors.New("external effect identity conflict")
	ErrNotFound           = errors.New("external effect record not found")
	ErrStore              = errors.New("external effect store failure")
	ErrStorePanic         = errors.New("external effect store panic")
	ErrDriver             = errors.New("external effect driver failure")
	ErrDriverPanic        = errors.New("external effect driver panic")
	ErrReadBack           = errors.New("external effect read-back failure")
	ErrReadBackPanic      = errors.New("external effect read-back panic")
	ErrReadBackMismatch   = errors.New("external effect read-back mismatch")
)

// DriverRef fixes the provider adapter and version that own an effect.
// Implementations must reject a request whose ref does not match their own.
type DriverRef struct {
	ID      string `json:"id"`
	Version string `json:"version"`
}

// Intent is the immutable, durable identity of one external effect. Target
// and payload are represented solely by SHA-256 digests, never by raw bytes.
type Intent struct {
	Protocol      string              `json:"protocol"`
	Invocation    core.ToolInvocation `json:"invocation"`
	Driver        DriverRef           `json:"driver"`
	TargetDigest  string              `json:"target_digest"`
	PayloadDigest string              `json:"payload_digest"`
	IntentDigest  string              `json:"intent_digest"`
	OperationKey  string              `json:"operation_key"`
}

// DispatchRequest carries ephemeral dispatch material. A Store must never
// persist Target or Payload; Service validates that both match Intent first.
type DispatchRequest struct {
	Intent  Intent
	Target  []byte
	Payload []byte
}

// State is the durable evidence state. Accepted is explicitly non-terminal:
// it records a provider acknowledgement, not proof of the external effect.
type State string

const (
	StatePrepared    State = "prepared"
	StateDispatching State = "dispatching"
	StateAccepted    State = "accepted"
	StateConfirmed   State = "confirmed"
	StateRejected    State = "rejected"
	StateUnknown     State = "unknown"
)

// Unknown reasons are fixed values so no provider error text becomes durable
// effect-journal data.
const (
	CodeDriverFailure           = "driver_failure"
	CodeDriverPanic             = "driver_panic"
	CodeDriverInvalidSubmission = "driver_invalid_submission"
	CodeReadBackFailure         = "read_back_failure"
	CodeReadBackPanic           = "read_back_panic"
	CodeReadBackMismatch        = "read_back_mismatch"
	CodeReadBackInvalid         = "read_back_invalid"
	CodeReadBackPending         = "read_back_pending"
	CodeReadBackUnknown         = "read_back_unknown"
)

// Record is the complete durable view of an effect. ReceiptDigest represents
// an acknowledgement only. EvidenceDigest is required for confirmed/rejected
// and must originate from a bound Driver.ReadBack observation.
type Record struct {
	Intent           Intent `json:"intent"`
	State            State  `json:"state"`
	DispatchAttempts uint32 `json:"dispatch_attempts"`
	ReceiptDigest    string `json:"receipt_digest,omitempty"`
	EvidenceDigest   string `json:"evidence_digest,omitempty"`
	ErrorCode        string `json:"error_code,omitempty"`
}

// Submission is a provider acknowledgement returned by Dispatch. It must be
// bound to the request operation key and intent digest before it can create an
// accepted record.
type Submission struct {
	OperationKey  string `json:"operation_key"`
	IntentDigest  string `json:"intent_digest"`
	ReceiptDigest string `json:"receipt_digest"`
}

// ObservationState describes a provider read-back result. Only confirmed and
// rejected observations can move a durable record to a terminal state.
type ObservationState string

const (
	ObservationPending   ObservationState = "pending"
	ObservationConfirmed ObservationState = "confirmed"
	ObservationRejected  ObservationState = "rejected"
	ObservationUnknown   ObservationState = "unknown"
)

// Observation is provider read-back evidence. The Service verifies its
// operation key and intent digest before using it.
type Observation struct {
	State          ObservationState `json:"state"`
	OperationKey   string           `json:"operation_key"`
	IntentDigest   string           `json:"intent_digest"`
	EvidenceDigest string           `json:"evidence_digest,omitempty"`
}

// Driver owns the provider-specific dispatch and query implementation. It
// receives raw material only during Dispatch. Adapters bind OperationKey and
// IntentDigest to their provider idempotency/query contract, then return only
// digests. ReadBack receives the durable identity, so recovery can never
// recreate a second dispatch from payload.
type Driver interface {
	Ref() DriverRef
	Dispatch(context.Context, DispatchRequest) (Submission, error)
	ReadBack(context.Context, Intent) (Observation, error)
}

// Store is the durable port. Ensure must atomically create a prepared record or
// return the exact canonical record; reusing an invocation identity with a
// different Intent must return ErrConflict. Mutation methods must atomically
// enforce the documented state transition and return its canonical record.
// Store implementations retain Record only; raw dispatch bytes are out of
// contract and must never be written through this interface.
type Store interface {
	Ensure(context.Context, Intent) (Record, error)
	Get(context.Context, Intent) (Record, bool, error)
	// BeginDispatch atomically changes prepared to dispatching. begun is true
	// only for the caller that won that transition and may call Driver.Dispatch.
	// A false result is an existing canonical record and must be read back.
	BeginDispatch(context.Context, Intent) (Record, bool, error)
	MarkAccepted(context.Context, Intent, Submission) (Record, error)
	MarkConfirmed(context.Context, Intent, Observation) (Record, error)
	MarkRejected(context.Context, Intent, Observation) (Record, error)
	MarkUnknown(context.Context, Intent, string) (Record, error)
}

// RecoveryRecord is a durable effect record with SQL- or adapter-owned
// timestamps. It contains no raw target, payload, provider receipt, or
// provider response body.
type RecoveryRecord struct {
	Record
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// RecoveryQuery restricts discovery to one provider adapter. A recovery worker
// must only query effects whose read-back semantics it owns.
type RecoveryQuery struct {
	Driver DriverRef `json:"driver"`
}

// RecoveryCursor is a deterministic updated_at/logical-key cursor for
// unresolved effects. An all-empty cursor starts at the beginning; otherwise
// UpdatedAt and every identity field are present. It is discovery state, never
// evidence of an external effect.
type RecoveryCursor struct {
	UpdatedAt time.Time `json:"updated_at"`
	TenantID  string    `json:"tenant_id"`
	SubjectID string    `json:"subject_id"`
	SessionID string    `json:"session_id"`
	RunID     string    `json:"run_id"`
	CallID    string    `json:"call_id"`
}

// RunScope selects one caller-authorized run for diagnostic or recovery
// reads. The Store remains responsible for enforcing this exact scope.
type RunScope struct {
	TenantID  string `json:"tenant_id"`
	SubjectID string `json:"subject_id"`
	SessionID string `json:"session_id"`
	RunID     string `json:"run_id"`
}

// RunCursor is a bounded keyset cursor within a single RunScope.
type RunCursor struct {
	CallID string `json:"call_id"`
}

// RecoveryReader is an optional discovery port for records that require
// read-back. It never dispatches and its bounded keyset cursor is stable over
// changes to state timestamps.
type RecoveryReader interface {
	ListUnresolved(context.Context, RecoveryQuery, RecoveryCursor, int) ([]RecoveryRecord, RecoveryCursor, error)
}

// RunReader is an optional scoped inspection port. It is deliberately unable
// to read another tenant, subject, session, or run by a partial key.
type RunReader interface {
	ListRun(context.Context, RunScope, RunCursor, int) ([]RecoveryRecord, RunCursor, error)
}

// NewIntent constructs the immutable durable identity of an effect. It hashes
// bounded target and payload bytes immediately and does not retain them.
func NewIntent(invocation core.ToolInvocation, driver DriverRef, target, payload []byte) (Intent, error) {
	if err := core.ValidateToolInvocation(invocation); err != nil {
		return Intent{}, ErrInvalidIntent
	}
	if !validDriverRef(driver) || !validDispatchBytes(target, payload) {
		return Intent{}, ErrInvalidIntent
	}
	intent := Intent{
		Protocol:      ProtocolV1,
		Invocation:    invocation,
		Driver:        driver,
		TargetDigest:  SHA256Digest(target),
		PayloadDigest: SHA256Digest(payload),
	}
	intent.IntentDigest = canonicalIntentDigest(intent)
	intent.OperationKey = operationKey(intent.IntentDigest)
	return intent, nil
}

// SHA256Digest returns the lowercase SHA-256 digest used by this contract.
func SHA256Digest(value []byte) string {
	digest := sha256.Sum256(value)
	return hex.EncodeToString(digest[:])
}

// ValidateIntent verifies the immutable fields and their deterministic
// digests. It never returns parser or validation details from another package.
func ValidateIntent(intent Intent) error {
	if intent.Protocol != ProtocolV1 || core.ValidateToolInvocation(intent.Invocation) != nil || !validDriverRef(intent.Driver) ||
		!validDigest(intent.TargetDigest) || !validDigest(intent.PayloadDigest) ||
		!validDigest(intent.IntentDigest) || !validDigest(intent.OperationKey) {
		return ErrInvalidIntent
	}
	if canonicalIntentDigest(intent) != intent.IntentDigest || operationKey(intent.IntentDigest) != intent.OperationKey {
		return ErrInvalidIntent
	}
	return nil
}

// ValidateDispatchRequest verifies the ephemeral bytes before they are given
// to a provider. It protects an operation key from a changed payload.
func ValidateDispatchRequest(request DispatchRequest) error {
	if ValidateIntent(request.Intent) != nil || !validDispatchBytes(request.Target, request.Payload) ||
		SHA256Digest(request.Target) != request.Intent.TargetDigest ||
		SHA256Digest(request.Payload) != request.Intent.PayloadDigest {
		return ErrInvalidRequest
	}
	return nil
}

// ValidateRecord verifies that the durable data obeys the v1 evidence rules.
func ValidateRecord(record Record) error {
	if ValidateIntent(record.Intent) != nil || record.DispatchAttempts > MaxDispatchAttempts {
		return ErrInvalidState
	}
	validReceipt := record.ReceiptDigest == "" || validDigest(record.ReceiptDigest)
	if !validReceipt {
		return ErrInvalidState
	}
	switch record.State {
	case StatePrepared:
		if record.DispatchAttempts != 0 || record.ReceiptDigest != "" || record.EvidenceDigest != "" || record.ErrorCode != "" {
			return ErrInvalidState
		}
	case StateDispatching:
		if record.DispatchAttempts == 0 || record.ReceiptDigest != "" || record.EvidenceDigest != "" || record.ErrorCode != "" {
			return ErrInvalidState
		}
	case StateAccepted:
		if record.DispatchAttempts == 0 || !validDigest(record.ReceiptDigest) || record.EvidenceDigest != "" || record.ErrorCode != "" {
			return ErrInvalidState
		}
	case StateConfirmed, StateRejected:
		if record.DispatchAttempts == 0 || !validDigest(record.EvidenceDigest) || record.ErrorCode != "" {
			return ErrInvalidState
		}
	case StateUnknown:
		if record.DispatchAttempts == 0 || record.EvidenceDigest != "" || !validUnknownCode(record.ErrorCode) {
			return ErrInvalidState
		}
	default:
		return ErrInvalidState
	}
	return nil
}

func canonicalIntentDigest(intent Intent) string {
	canonical := struct {
		Protocol      string              `json:"protocol"`
		Invocation    core.ToolInvocation `json:"invocation"`
		Driver        DriverRef           `json:"driver"`
		TargetDigest  string              `json:"target_digest"`
		PayloadDigest string              `json:"payload_digest"`
	}{
		Protocol: intent.Protocol, Invocation: intent.Invocation, Driver: intent.Driver,
		TargetDigest: intent.TargetDigest, PayloadDigest: intent.PayloadDigest,
	}
	encoded, err := json.Marshal(canonical)
	if err != nil {
		return ""
	}
	return SHA256Digest(encoded)
}

func operationKey(intentDigest string) string {
	return SHA256Digest([]byte(ProtocolV1 + "\x00" + intentDigest))
}

func validDispatchBytes(target, payload []byte) bool {
	return len(target) > 0 && len(target) <= MaxTargetBytes && len(payload) <= MaxPayloadBytes
}

func validDriverRef(ref DriverRef) bool {
	return validText(ref.ID, maxDriverIDBytes) && validText(ref.Version, maxDriverVersionBytes)
}

func validText(value string, max int) bool {
	return value == strings.TrimSpace(value) && value != "" && len(value) <= max && utf8.ValidString(value) && !strings.ContainsAny(value, "\r\n\x00")
}

func validDigest(value string) bool {
	if len(value) != sha256.Size*2 || value != strings.ToLower(value) {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func validUnknownCode(code string) bool {
	switch code {
	case CodeDriverFailure, CodeDriverPanic, CodeDriverInvalidSubmission, CodeReadBackFailure, CodeReadBackPanic, CodeReadBackMismatch, CodeReadBackInvalid, CodeReadBackPending, CodeReadBackUnknown:
		return true
	default:
		return false
	}
}

func validateSubmission(intent Intent, submission Submission) error {
	if submission.OperationKey != intent.OperationKey || submission.IntentDigest != intent.IntentDigest || !validDigest(submission.ReceiptDigest) {
		return ErrInvalidSubmission
	}
	return nil
}

func validateObservation(intent Intent, observation Observation) error {
	if observation.OperationKey != intent.OperationKey || observation.IntentDigest != intent.IntentDigest {
		return ErrReadBackMismatch
	}
	switch observation.State {
	case ObservationConfirmed, ObservationRejected:
		if !validDigest(observation.EvidenceDigest) {
			return ErrInvalidObservation
		}
	case ObservationPending, ObservationUnknown:
		if observation.EvidenceDigest != "" {
			return ErrInvalidObservation
		}
	default:
		return ErrInvalidObservation
	}
	return nil
}

func recordForIntent(record Record, intent Intent) error {
	if record.Intent != intent || ValidateRecord(record) != nil {
		return ErrStore
	}
	return nil
}

func cloneDispatchRequest(request DispatchRequest) DispatchRequest {
	request.Target = append([]byte(nil), request.Target...)
	request.Payload = append([]byte(nil), request.Payload...)
	return request
}

// StateTransitionAllowed exposes the transitions that Store implementations
// must make atomic. A transition to unknown is also allowed from unknown so a
// newer fixed error code can replace an earlier one without redispatching.
func StateTransitionAllowed(from, to State) bool {
	switch from {
	case StatePrepared:
		return to == StateDispatching
	case StateDispatching:
		return to == StateAccepted || to == StateConfirmed || to == StateRejected || to == StateUnknown
	case StateAccepted:
		return to == StateConfirmed || to == StateRejected || to == StateUnknown
	case StateUnknown:
		return to == StateConfirmed || to == StateRejected || to == StateUnknown
	default:
		return false
	}
}

// fixedError intentionally discards implementation error text. It is used by
// Service to avoid returning or persisting provider/store details.
func fixedError(err error, fallback error) error {
	if errors.Is(err, ErrConflict) {
		return ErrConflict
	}
	if errors.Is(err, ErrNotFound) {
		return ErrNotFound
	}
	return fallback
}
