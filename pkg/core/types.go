package core

import "context"

// CapabilityKind classifies extension points without prescribing execution.
type CapabilityKind string

const (
	KindKnowledge    CapabilityKind = "knowledge"
	KindRAG          CapabilityKind = "rag"
	KindPrompt       CapabilityKind = "prompt"
	KindContext      CapabilityKind = "context"
	KindTool         CapabilityKind = "tool"
	KindPlugin       CapabilityKind = "plugin"
	KindAgent        CapabilityKind = "agent"
	KindComputation  CapabilityKind = "computation"
	KindConnector    CapabilityKind = "connector"
	KindSkill        CapabilityKind = "skill"
	KindWorkflow     CapabilityKind = "workflow"
	KindMemory       CapabilityKind = "memory"
	KindRouter       CapabilityKind = "router"
	KindPolicy       CapabilityKind = "policy"
	KindHook         CapabilityKind = "hook"
	KindEvaluator    CapabilityKind = "evaluator"
	KindModel        CapabilityKind = "model"
	KindSandbox      CapabilityKind = "sandbox"
	KindPresentation CapabilityKind = "presentation"
)

// SandboxMode describes the file effects allowed for one execution.
type SandboxMode string

const (
	SandboxReadOnly         SandboxMode = "read-only"
	SandboxWorkspaceWrite   SandboxMode = "workspace-write"
	SandboxDangerFullAccess SandboxMode = "danger-full-access"
)

// Permission is a namespaced operation grant such as data.read or crm.send.
type Permission string

const (
	PermRead  Permission = "data.read"
	PermWrite Permission = "data.write"
	PermSend  Permission = "data.send"
)

// Perm is kept as a source-level alias while integrations move to Permission.
type Perm = Permission

// PermissionSet is an immutable-by-convention set copied at API boundaries.
type PermissionSet map[Permission]bool

// NewPermissionSet builds a set from individual grants.
func NewPermissionSet(grants ...Permission) PermissionSet {
	out := PermissionSet{}
	for _, grant := range grants {
		out[grant] = true
	}
	return out
}

// Clone returns an independent permission set.
func (s PermissionSet) Clone() PermissionSet {
	out := PermissionSet{}
	for permission, allowed := range s {
		if allowed {
			out[permission] = true
		}
	}
	return out
}

// Allows reports whether every required permission is present.
func (s PermissionSet) Allows(required []Permission) bool {
	for _, permission := range required {
		if !s[permission] {
			return false
		}
	}
	return true
}

// Intersect returns the grants allowed by both sets.
func (s PermissionSet) Intersect(other PermissionSet) PermissionSet {
	out := PermissionSet{}
	for permission, allowed := range s {
		if allowed && other[permission] {
			out[permission] = true
		}
	}
	return out
}

// Principal is the authenticated caller and its effective ownership scope.
type Principal struct {
	SubjectID  string            `json:"subject_id"`
	TenantID   string            `json:"tenant_id"`
	Scope      ScopePath         `json:"scope"`
	Grants     PermissionSet     `json:"grants"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

// CapabilityManifest is the stable, language-neutral capability declaration.
type CapabilityManifest struct {
	ID                  string            `json:"id"`
	Version             string            `json:"version"`
	Name                string            `json:"name"`
	Description         string            `json:"description,omitempty"`
	Kind                CapabilityKind    `json:"kind"`
	Contract            string            `json:"contract,omitempty"`
	RequiredPermissions []Permission      `json:"required_permissions,omitempty"`
	RequiredCredentials []CredentialRef   `json:"required_credentials,omitempty"`
	InputSchema         map[string]any    `json:"input_schema,omitempty"`
	OutputSchema        map[string]any    `json:"output_schema,omitempty"`
	MaxOutputBytes      int               `json:"max_output_bytes,omitempty"`
	PerTurnBudget       int               `json:"per_turn_budget,omitempty"`
	TimeoutMs           int               `json:"timeout_ms,omitempty"`
	Idempotent          bool              `json:"idempotent,omitempty"`
	RequiresApproval    bool              `json:"requires_approval,omitempty"`
	Tool                *ToolExposure     `json:"tool,omitempty"`
	Execution           *ExecutionSpec    `json:"execution,omitempty"`
	Metadata            map[string]string `json:"metadata,omitempty"`
}

// ArtifactRevisioner optionally supplies a stable, non-secret revision for a
// provider or model adapter. Revisions are recorded in run composition for
// audit and replay diagnostics; implementations must never return credentials.
type ArtifactRevisioner interface {
	ArtifactRevision() string
}

// ToolExposure opts a capability into the model-facing tool consumer.
type ToolExposure struct {
	Description string         `json:"description,omitempty"`
	Parameters  map[string]any `json:"parameters,omitempty"`
}

// ExecutionSpec describes an executor-backed provider without exposing it to a model.
type ExecutionSpec struct {
	Runtime    string        `json:"runtime"`
	Entrypoint string        `json:"entrypoint"`
	Workdir    string        `json:"workdir,omitempty"`
	Sandbox    SandboxPolicy `json:"sandbox"`
	Writes     bool          `json:"writes,omitempty"`

	// HTTP execution (Runtime "http"): Entrypoint is the absolute URL. Method
	// defaults to POST with args as the JSON body; GET renders flat args as a
	// query string. Header values of the form "$credential:<ref>" resolve the
	// named credential at call time and fail closed when unavailable.
	Method  string            `json:"method,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
}

// ToolCall is one model-requested capability invocation.
type ToolCall struct {
	ID   string         `json:"id"`
	Name string         `json:"name"`
	Args map[string]any `json:"args,omitempty"`
}

// ProtectedToolInvoker invokes another capability through the active run's
// guard funnel. Composite capabilities receive this interface at call time;
// they must not retain or invoke raw capability providers directly.
type ProtectedToolInvoker interface {
	InvokeTool(ctx context.Context, call ToolCall) (CapabilityResult, error)
}

// CapabilityFilter narrows the already-resolved run snapshot. It can never
// add capabilities or bypass scope, permission, policy, credential, approval,
// or profile checks.
type CapabilityFilter interface {
	AllowCapability(manifest CapabilityManifest) bool
}

type CapabilityFilterFunc func(CapabilityManifest) bool

func (f CapabilityFilterFunc) AllowCapability(manifest CapabilityManifest) bool {
	return f(manifest)
}

// ToolSchema is the provider-neutral schema exposed to an LLM adapter.
type ToolSchema struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	Parameters  map[string]any `json:"parameters"`
}

// ChatRole identifies a model conversation message.
type ChatRole string

const (
	RoleUser      ChatRole = "user"
	RoleAssistant ChatRole = "assistant"
	RoleTool      ChatRole = "tool"
)

// ContextProvenance is additive local projection metadata. Model protocol
// adapters ignore it; its sole purpose is bounded context selection evidence.
type ContextProvenance struct {
	Kind        string `json:"kind,omitempty"`
	SourceStart int64  `json:"source_start,omitempty"`
	SourceEnd   int64  `json:"source_end,omitempty"`
	Revision    string `json:"revision,omitempty"`
}

// ChatMessage is the provider-neutral message projected from session events.
// SourceSeq records the log event the message derives from, so summarizers
// can address exact seq ranges.
type ChatMessage struct {
	Role       ChatRole           `json:"role"`
	Content    string             `json:"content"`
	ToolCall   *ToolCall          `json:"tool_call,omitempty"`
	ToolCalls  []ToolCall         `json:"tool_calls,omitempty"`
	ToolCallID string             `json:"tool_call_id,omitempty"`
	SourceSeq  int64              `json:"source_seq,omitempty"`
	Provenance *ContextProvenance `json:"provenance,omitempty"`
}

// CapabilityResult is the standardized result returned to a consumer.
type CapabilityResult struct {
	Content  string         `json:"content"`
	OK       bool           `json:"ok"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

// Invocation identifies the immutable parent run location of one tool call.
// It is populated only by Agent's guarded execution funnel.
type Invocation struct {
	SessionID string `json:"session_id"`
	RunID     string `json:"run_id"`
	CallID    string `json:"call_id"`
}

// CapabilityContext carries per-run identity, data and execution policy.
type CapabilityContext struct {
	Principal    Principal `json:"principal"`
	Scope        ScopePath `json:"scope"`
	CapabilityID string    `json:"capability_id"`
	// CompositionRevision is the immutable composition selected for this run.
	// Capability adapters may use it for lease and artifact provenance.
	CompositionRevision string               `json:"composition_revision,omitempty"`
	Data                any                  `json:"-"`
	Policy              SandboxPolicy        `json:"policy"`
	Credentials         CredentialAccessor   `json:"-"`
	Invoker             ProtectedToolInvoker `json:"-"`
	Invocation          Invocation           `json:"invocation"`
	RemainingToolCalls  int                  `json:"remaining_tool_calls"`
	Accepted            AcceptedInvocation   `json:"-"`
}

// CapabilityRequest is the complete per-call input to a provider.
type CapabilityRequest struct {
	CallID         string            `json:"call_id"`
	IdempotencyKey string            `json:"idempotency_key,omitempty"`
	Args           map[string]any    `json:"args,omitempty"`
	Context        CapabilityContext `json:"context"`
}
