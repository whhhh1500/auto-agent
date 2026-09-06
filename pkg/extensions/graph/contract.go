package graph

import (
	"encoding/json"
	"time"
)

const (
	// Contract is the stable Graph-G0 definition contract identifier.
	Contract = "harness.graph/v1"

	MaxDefinitionBytes   = 256 << 10
	MaxGraphIDBytes      = 128
	MaxGraphVersionBytes = 64
	MaxNodeIDBytes       = 128
	MaxNodeKindBytes     = 128
	MaxNodes             = 128
	MaxEdges             = 512
	MaxStateFields       = 64
	MaxStateFieldBytes   = 64 << 10
	MaxStateBytes        = 512 << 10
	MaxNodeConfigBytes   = 16 << 10
	// MaxRegistryEntries is the final registry size, including the built-in
	// top-level JSON patch reducer in a ReducerRegistry.
	MaxRegistryEntries       = 256
	MaxSteps                 = 1024
	MaxVisitsPerNode         = 128
	MaxRetryAttempts         = 10
	MaxNodeTimeout           = 5 * time.Minute
	ReducerTopLevelJSONPatch = "top-level-json-patch/v1"
)

// Definition is a declarative graph. Revision is optional input evidence: if
// present, it must be the SHA-256 revision derived from every other field.
type Definition struct {
	ID        string          `json:"id"`
	Version   string          `json:"version"`
	Revision  string          `json:"revision,omitempty"`
	EntryNode string          `json:"entry_node"`
	Nodes     []NodeSpec      `json:"nodes"`
	Edges     []EdgeSpec      `json:"edges"`
	State     StateSchema     `json:"state"`
	Limits    Limits          `json:"limits"`
	Redaction RedactionSchema `json:"redaction"`
}

// NodeSpec names a future node implementation and declares only its bounded
// inputs. Config is opaque to G0 after canonical JSON validation.
type NodeSpec struct {
	ID                   string               `json:"id"`
	Kind                 string               `json:"kind"`
	KindVersion          string               `json:"kind_version"`
	Config               json.RawMessage      `json:"config,omitempty"`
	ContextView          ContextView          `json:"context_view"`
	ContextBudget        ContextBudget        `json:"context_budget"`
	Retry                RetryPolicy          `json:"retry"`
	Timeout              time.Duration        `json:"timeout"`
	Terminal             bool                 `json:"terminal"`
	UsesSandbox          bool                 `json:"uses_sandbox"`
	ApprovalDeniedPolicy ApprovalDeniedPolicy `json:"approval_denied_policy,omitempty"`
}

// ApprovalDeniedPolicy determines whether a denied approval terminates the
// node or resumes it with the durable denial evidence. Empty is the legacy
// default and is canonicalized to ApprovalDeniedFail.
type ApprovalDeniedPolicy string

const (
	ApprovalDeniedFail               ApprovalDeniedPolicy = "failed"
	ApprovalDeniedResumeWithDecision ApprovalDeniedPolicy = "resume_with_decision"
)

// EdgeKind distinguishes normal default flow, predicate-selected flow, and
// error flow. G0 validates their shape but does not select or execute edges.
type EdgeKind string

const (
	EdgeDefault     EdgeKind = "default"
	EdgeConditional EdgeKind = "conditional"
	EdgeError       EdgeKind = "error"
)

type EdgeSpec struct {
	From             string   `json:"from"`
	To               string   `json:"to"`
	Kind             EdgeKind `json:"kind"`
	Predicate        string   `json:"predicate,omitempty"`
	PredicateVersion string   `json:"predicate_version,omitempty"`
}

// Limits bound a graph before any future executor can accept it. A cyclic
// graph additionally requires a positive per-node visit cap.
type Limits struct {
	MaxSteps         int `json:"max_steps"`
	MaxVisitsPerNode int `json:"max_visits_per_node,omitempty"`
}

// RetryPolicy is declaration-only in G0. Future execution may interpret it
// only after its own retry/timeout/cancel contract is accepted.
type RetryPolicy struct {
	MaxAttempts int           `json:"max_attempts,omitempty"`
	Backoff     time.Duration `json:"backoff,omitempty"`
}

// StateSchema is a bounded top-level field schema, not a general JSON Schema.
type StateSchema struct {
	Reducer        string       `json:"reducer"`
	ReducerVersion string       `json:"reducer_version"`
	Fields         []StateField `json:"fields"`
}

type StateField struct {
	Name      string         `json:"name"`
	Type      StateValueType `json:"type"`
	Required  bool           `json:"required,omitempty"`
	Sensitive bool           `json:"sensitive,omitempty"`
	MaxBytes  int            `json:"max_bytes"`
}

type StateValueType string

const (
	StateString  StateValueType = "string"
	StateNumber  StateValueType = "number"
	StateBoolean StateValueType = "boolean"
	StateObject  StateValueType = "object"
	StateArray   StateValueType = "array"
)

// RedactionSchema declares state fields that future evidence and telemetry
// producers must redact. It does not perform logging or transport work.
type RedactionSchema struct {
	StateFields []string `json:"state_fields,omitempty"`
}
