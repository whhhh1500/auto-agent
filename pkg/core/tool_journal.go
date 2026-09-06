package core

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// ToolInvocationState is the durable side-effect state of one tool call.
type ToolInvocationState string

const (
	ToolInvocationStarted   ToolInvocationState = "started"
	ToolInvocationCompleted ToolInvocationState = "completed"
	ToolInvocationUncertain ToolInvocationState = "uncertain"
)

// ToolInvocationDecision tells the guard funnel whether to execute a provider,
// replay a completed result, or fail closed around an uncertain side effect.
type ToolInvocationDecision string

const (
	ToolInvocationExecuteNew   ToolInvocationDecision = "execute_new"
	ToolInvocationExecuteRetry ToolInvocationDecision = "execute_retry"
	ToolInvocationReplay       ToolInvocationDecision = "replay"
	ToolInvocationUnknown      ToolInvocationDecision = "outcome_unknown"
	ToolInvocationConflict     ToolInvocationDecision = "conflict"
)

const (
	CodeToolJournalUnavailable  = "tool_journal_unavailable"
	CodeToolIdempotencyConflict = "tool_idempotency_conflict"
)

// ToolInvocation is the immutable identity and argument fingerprint of one
// guarded provider call. RunID + CallID identify the logical call; the digest
// detects an unsafe attempt to reuse that identity for different arguments.
type ToolInvocation struct {
	TenantID     string `json:"tenant_id"`
	SubjectID    string `json:"subject_id"`
	SessionID    string `json:"session_id"`
	RunID        string `json:"run_id"`
	CallID       string `json:"call_id"`
	CapabilityID string `json:"capability_id"`
	ArgsDigest   string `json:"args_digest"`
	Idempotent   bool   `json:"idempotent"`
}

// ToolInvocationRecord is the durable journal view returned to the kernel.
// Result is populated only for completed calls.
type ToolInvocationRecord struct {
	ToolInvocation
	State       ToolInvocationState `json:"state"`
	Result      *CapabilityResult   `json:"result,omitempty"`
	ErrorCode   string              `json:"error_code,omitempty"`
	StartedAt   time.Time           `json:"started_at"`
	UpdatedAt   time.Time           `json:"updated_at"`
	CompletedAt time.Time           `json:"completed_at,omitempty"`
}

// ToolInvocationJournal durably fences side effects at the final guard-funnel
// boundary. Implementations must atomically create-or-inspect Begin records and
// return the canonical completed result when multiple idempotent attempts race.
type ToolInvocationJournal interface {
	BeginToolInvocation(ctx context.Context, invocation ToolInvocation) (ToolInvocationRecord, ToolInvocationDecision, error)
	CompleteToolInvocation(ctx context.Context, invocation ToolInvocation, result CapabilityResult) (ToolInvocationRecord, error)
	MarkToolInvocationUncertain(ctx context.Context, invocation ToolInvocation, errorCode string) error
}

// ToolInvocationReader is an optional, read-only companion to
// ToolInvocationJournal. GetToolInvocation must never create a started record
// or otherwise mutate durable journal state. found is true only when the
// complete immutable invocation identity matches exactly. Missing records,
// cross-principal requests, and same-key identity conflicts are
// indistinguishable as found=false; callers must fail closed.
type ToolInvocationReader interface {
	GetToolInvocation(ctx context.Context, invocation ToolInvocation) (record ToolInvocationRecord, found bool, err error)
}

// NewToolInvocation builds a validated journal identity and a deterministic
// SHA-256 digest over the JSON arguments. encoding/json sorts string map keys,
// making the digest stable across process restarts.
func NewToolInvocation(info RunInfo, call ToolCall, idempotent bool) (ToolInvocation, error) {
	args := call.Args
	if args == nil {
		args = map[string]any{}
	}
	encoded, err := json.Marshal(args)
	if err != nil {
		return ToolInvocation{}, fmt.Errorf("encode tool invocation args: %w", err)
	}
	digest := sha256.Sum256(encoded)
	invocation := ToolInvocation{
		TenantID: info.Principal.TenantID, SubjectID: info.Principal.SubjectID,
		SessionID: info.SessionID, RunID: info.RunID, CallID: call.ID,
		CapabilityID: call.Name, ArgsDigest: hex.EncodeToString(digest[:]),
		Idempotent: idempotent,
	}
	if err := ValidateToolInvocation(invocation); err != nil {
		return ToolInvocation{}, err
	}
	return invocation, nil
}

// ValidateToolInvocation validates a journal identity without inspecting any
// provider-specific data.
func ValidateToolInvocation(invocation ToolInvocation) error {
	if err := ValidateSessionID(invocation.SessionID); err != nil {
		return err
	}
	if err := ValidateRunID(invocation.RunID); err != nil {
		return err
	}
	if err := validateToolCallID(invocation.CallID); err != nil {
		return err
	}
	if err := validateCapabilityID(invocation.CapabilityID); err != nil {
		return err
	}
	for name, value := range map[string]string{"tenant id": invocation.TenantID, "subject id": invocation.SubjectID} {
		if strings.TrimSpace(value) == "" || len(value) > 512 || strings.ContainsAny(value, "\r\n\x00") {
			return fmt.Errorf("tool invocation %s is empty, too long, or contains control characters", name)
		}
	}
	if len(invocation.ArgsDigest) != sha256.Size*2 {
		return fmt.Errorf("tool invocation args digest is not SHA-256")
	}
	if _, err := hex.DecodeString(invocation.ArgsDigest); err != nil {
		return fmt.Errorf("tool invocation args digest is invalid: %w", err)
	}
	return nil
}

// CloneToolInvocationRecord returns a defensive copy suitable for adapter
// boundaries and replay.
func CloneToolInvocationRecord(record ToolInvocationRecord) ToolInvocationRecord {
	if record.Result != nil {
		result := cloneCapabilityResult(*record.Result)
		record.Result = &result
	}
	return record
}

func cloneCapabilityResult(result CapabilityResult) CapabilityResult {
	out := CapabilityResult{Content: result.Content, OK: result.OK}
	if result.Metadata != nil {
		encoded, err := json.Marshal(result.Metadata)
		if err == nil {
			_ = json.Unmarshal(encoded, &out.Metadata)
		}
	}
	return out
}
