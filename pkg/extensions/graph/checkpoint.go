package graph

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

// Graph-G1 checkpoint contracts deliberately remain value-only and
// standard-library-only. Execution, storage, leases, and observers live in
// their own packages.
const (
	MaxCheckpointIDBytes         = 128
	MaxCheckpointTransitions     = 4096
	MaxTransitionPageSize        = 256
	MaxCheckpointVersionPageSize = 256
	MaxCheckpointFailureBytes    = 128
	MaxCheckpointEdgeNameBytes   = 128
	MaxApprovalActorBytes        = 128
	MaxApprovalBasisBytes        = 256
)

var (
	ErrCheckpointConflict = errors.New("graph checkpoint conflict")
	ErrCheckpointNotFound = errors.New("graph checkpoint not found")
	ErrInvalidCheckpoint  = errors.New("invalid graph checkpoint")
	ErrInvalidTransition  = errors.New("invalid graph transition")
	ErrCheckpointCapacity = errors.New("graph checkpoint capacity exceeded")
)

// CommitDisposition tells callers whether a successful mutation was applied
// by this call or was an exact idempotent replay of an existing fact.
type CommitDisposition uint8

const (
	CommitUnknown  CommitDisposition = iota
	CommitApplied                    // this call persisted the checkpoint and transition
	CommitReplayed                   // an identical persisted fact already existed
)

func (d CommitDisposition) Valid() bool {
	return d == CommitApplied || d == CommitReplayed
}

// CheckpointKey is the stable execution identity. Segment identity is
// intentionally separate because an approval resume keeps the same run.
type CheckpointKey struct {
	TenantID  string `json:"tenant_id"`
	SessionID string `json:"session_id"`
	RunID     string `json:"run_id"`
}

// CheckpointStatus is a closed execution-state vocabulary.
type CheckpointStatus string

const (
	CheckpointReady           CheckpointStatus = "ready"
	CheckpointExecuting       CheckpointStatus = "executing"
	CheckpointWaitingApproval CheckpointStatus = "waiting_approval"
	CheckpointCompleted       CheckpointStatus = "completed"
	CheckpointFailed          CheckpointStatus = "failed"
	CheckpointCancelled       CheckpointStatus = "cancelled"
	CheckpointUnknown         CheckpointStatus = "unknown"
)

// EdgeEvidence identifies a selected edge without leaking state or predicate
// inputs. Predicate is empty for default and error edges.
type EdgeEvidence struct {
	From      string   `json:"from"`
	To        string   `json:"to"`
	Kind      EdgeKind `json:"kind"`
	Predicate string   `json:"predicate,omitempty"`
}

// ApprovalEvidence is the bounded, non-secret authorization fact attached to
// an approval resolution transition. Older transitions may omit it.
type ApprovalEvidence struct {
	TenantID           string `json:"tenant_id"`
	ApprovalID         string `json:"approval_id"`
	Decision           string `json:"decision"`
	Revision           uint64 `json:"revision"`
	SourceSegmentID    string `json:"source_segment_id"`
	ActorID            string `json:"actor_id"`
	AuthorizationBasis string `json:"authorization_basis"`
}

// Checkpoint is the bounded, canonical fact for one Graph run. Revision is
// assigned by Store.Create/CAS and participates in every transition identity.
type Checkpoint struct {
	Key                    CheckpointKey     `json:"key"`
	SegmentID              string            `json:"segment_id"`
	GraphID                string            `json:"graph_id"`
	DefinitionRevision     string            `json:"definition_revision"`
	CompositionRevision    string            `json:"composition_revision"`
	ImplementationRevision string            `json:"implementation_revision"`
	HostGeneration         uint64            `json:"host_generation"`
	CurrentNodeID          string            `json:"current_node_id"`
	Attempt                int               `json:"attempt"`
	AttemptID              string            `json:"attempt_id,omitempty"`
	Steps                  int               `json:"steps"`
	Visits                 map[string]int    `json:"visits"`
	Revision               uint64            `json:"revision"`
	State                  State             `json:"state"`
	LastEdge               *EdgeEvidence     `json:"last_edge,omitempty"`
	PendingApprovalID      string            `json:"pending_approval_id,omitempty"`
	ResolvedApproval       *ApprovalEvidence `json:"resolved_approval,omitempty"`
	Status                 CheckpointStatus  `json:"status"`
	FailureCode            string            `json:"failure_code,omitempty"`
}

// CheckpointVersionInfo is bounded metadata for one immutable checkpoint
// revision. CreatedAt is the storage-assigned timestamp.
type CheckpointVersionInfo struct {
	ID             string                  `json:"id"`
	ParentID       string                  `json:"parent_id,omitempty"`
	Origin         CheckpointVersionOrigin `json:"origin"`
	Key            CheckpointKey           `json:"key"`
	Revision       uint64                  `json:"revision"`
	CheckpointHash string                  `json:"checkpoint_hash"`
	CreatedAt      int64                   `json:"created_at"`
}

// CheckpointVersionOrigin records why the first retained fact exists. It
// keeps migration floors and future cross-run Fork lineage explicit rather
// than overloading an empty parent ID.
type CheckpointVersionOrigin string

const (
	CheckpointVersionOriginCommit         CheckpointVersionOrigin = "commit"
	CheckpointVersionOriginMigrationFloor CheckpointVersionOrigin = "migration_floor"
	CheckpointVersionOriginFork           CheckpointVersionOrigin = "fork"
)

// CheckpointVersion is a validated immutable checkpoint snapshot and its
// metadata. ListVersions returns only CheckpointVersionInfo values so callers
// do not accidentally materialize many checkpoint documents.
type CheckpointVersion struct {
	Info       CheckpointVersionInfo `json:"info"`
	Checkpoint Checkpoint            `json:"checkpoint"`
}

// Transition is appended atomically with its resulting checkpoint. ID is
// deterministic from Key and Revision, which gives replay a safe identity.
type Transition struct {
	ID                   string            `json:"id"`
	Key                  CheckpointKey     `json:"key"`
	Revision             uint64            `json:"revision"`
	From                 CheckpointStatus  `json:"from,omitempty"`
	To                   CheckpointStatus  `json:"to"`
	SegmentID            string            `json:"segment_id"`
	SourceSegmentID      string            `json:"source_segment_id,omitempty"`
	SourceHostGeneration uint64            `json:"source_host_generation,omitempty"`
	SourceNodeID         string            `json:"source_node_id,omitempty"`
	SourceAttemptID      string            `json:"source_attempt_id,omitempty"`
	CurrentNodeID        string            `json:"current_node_id"`
	AttemptID            string            `json:"attempt_id,omitempty"`
	OutcomeCode          string            `json:"outcome_code,omitempty"`
	Edge                 *EdgeEvidence     `json:"edge,omitempty"`
	Approval             *ApprovalEvidence `json:"approval,omitempty"`
}

// Store is a narrow compare-and-swap port. Implementations must atomically
// commit a successful checkpoint replacement and exactly one transition. A
// nil error must carry either CommitApplied or CommitReplayed so callers can
// distinguish the mutation winner from an idempotent replay.
type Store interface {
	Load(context.Context, CheckpointKey) (Checkpoint, error)
	Create(context.Context, Checkpoint, Transition) (Checkpoint, CommitDisposition, error)
	CompareAndSwap(context.Context, CheckpointKey, uint64, Checkpoint, Transition) (Checkpoint, CommitDisposition, error)
	ListTransitions(context.Context, CheckpointKey, uint64, int) ([]Transition, error)
}

// HistoryStore is an optional checkpoint-history read port. Implementations
// of Store need not also implement this capability.
type HistoryStore interface {
	LoadVersion(context.Context, CheckpointKey, uint64) (CheckpointVersion, error)
	ListVersions(context.Context, CheckpointKey, uint64, int) ([]CheckpointVersionInfo, error)
}

func (k CheckpointKey) String() string {
	return k.TenantID + "/" + k.SessionID + "/" + k.RunID
}

func TransitionID(key CheckpointKey, revision uint64) string {
	input := key.TenantID + "\x00" + key.SessionID + "\x00" + key.RunID + "\x00" + fmt.Sprintf("%d", revision)
	sum := sha256.Sum256([]byte(input))
	return hex.EncodeToString(sum[:])
}

// CheckpointVersionID deterministically identifies one run revision. It is an
// identity, rather than a digest of checkpoint content.
func CheckpointVersionID(key CheckpointKey, revision uint64) string {
	input := "checkpoint-version\x00" + key.TenantID + "\x00" + key.SessionID + "\x00" + key.RunID + "\x00" + fmt.Sprintf("%d", revision)
	sum := sha256.Sum256([]byte(input))
	return hex.EncodeToString(sum[:])
}

// CheckpointDigest returns the SHA-256 digest of the canonical checkpoint
// document. Invalid checkpoint facts cannot be digested.
func CheckpointDigest(checkpoint Checkpoint) (string, error) {
	canonical, err := ValidateCheckpoint(checkpoint)
	if err != nil {
		return "", err
	}
	encoded, err := json.Marshal(canonical)
	if err != nil {
		return "", fmt.Errorf("%w: canonical checkpoint encoding: %v", ErrInvalidCheckpoint, err)
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

// ValidateCheckpointKey checks the bounded run identity before a Store lookup
// or mutation. It exposes no persistence or execution behavior.
func ValidateCheckpointKey(key CheckpointKey) error {
	if err := validateCheckpointKey(key); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidCheckpoint, err)
	}
	return nil
}

func CloneCheckpoint(checkpoint Checkpoint) Checkpoint {
	copyOf := checkpoint
	copyOf.State = cloneCheckpointState(checkpoint.State)
	copyOf.Visits = make(map[string]int, len(checkpoint.Visits))
	for key, value := range checkpoint.Visits {
		copyOf.Visits[key] = value
	}
	if checkpoint.LastEdge != nil {
		edge := *checkpoint.LastEdge
		copyOf.LastEdge = &edge
	}
	if checkpoint.ResolvedApproval != nil {
		approval := *checkpoint.ResolvedApproval
		copyOf.ResolvedApproval = &approval
	}
	return copyOf
}

func CloneCheckpointVersionInfo(info CheckpointVersionInfo) CheckpointVersionInfo {
	return info
}

func CloneCheckpointVersion(version CheckpointVersion) CheckpointVersion {
	return CheckpointVersion{
		Info:       CloneCheckpointVersionInfo(version.Info),
		Checkpoint: CloneCheckpoint(version.Checkpoint),
	}
}

func CloneTransition(transition Transition) Transition {
	copyOf := transition
	if transition.Edge != nil {
		edge := *transition.Edge
		copyOf.Edge = &edge
	}
	if transition.Approval != nil {
		approval := *transition.Approval
		copyOf.Approval = &approval
	}
	return copyOf
}

// ValidateCheckpoint validates bounded checkpoint evidence. It cannot validate
// state field types because that requires the immutable Definition held by the
// executor; it does canonicalize every stored JSON value and enforces G0's
// global state limits.
func ValidateCheckpoint(checkpoint Checkpoint) (Checkpoint, error) {
	if err := ValidateCheckpointKey(checkpoint.Key); err != nil {
		return Checkpoint{}, err
	}
	for name, value := range map[string]string{
		"segment id": checkpoint.SegmentID, "graph id": checkpoint.GraphID,
		"definition revision":     checkpoint.DefinitionRevision,
		"composition revision":    checkpoint.CompositionRevision,
		"implementation revision": checkpoint.ImplementationRevision,
		"current node id":         checkpoint.CurrentNodeID,
	} {
		if err := validateCheckpointIdentifier(value, MaxCheckpointIDBytes, name); err != nil {
			return Checkpoint{}, fmt.Errorf("%w: %v", ErrInvalidCheckpoint, err)
		}
	}
	if checkpoint.Revision == 0 || checkpoint.Attempt < 0 || checkpoint.Attempt > MaxRetryAttempts || checkpoint.Steps < 0 || checkpoint.Steps > MaxSteps {
		return Checkpoint{}, fmt.Errorf("%w: invalid revision, attempt, or step count", ErrInvalidCheckpoint)
	}
	if checkpoint.AttemptID != "" {
		if err := validateCheckpointIdentifier(checkpoint.AttemptID, MaxCheckpointIDBytes, "attempt id"); err != nil {
			return Checkpoint{}, fmt.Errorf("%w: %v", ErrInvalidCheckpoint, err)
		}
	}
	// executing and waiting_approval represent a live or suspended external
	// attempt. Their stable identity is mandatory; a ready checkpoint may retain
	// one only for an explicitly approved resume.
	if (checkpoint.Status == CheckpointExecuting || checkpoint.Status == CheckpointWaitingApproval || checkpoint.Status == CheckpointUnknown) && (checkpoint.Attempt <= 0 || checkpoint.AttemptID == "") {
		return Checkpoint{}, fmt.Errorf("%w: %s checkpoint lacks stable attempt evidence", ErrInvalidCheckpoint, checkpoint.Status)
	}
	if len(checkpoint.Visits) > MaxNodes {
		return Checkpoint{}, fmt.Errorf("%w: visit count exceeds %d", ErrInvalidCheckpoint, MaxNodes)
	}
	for node, visits := range checkpoint.Visits {
		if err := validateCheckpointIdentifier(node, MaxNodeIDBytes, "visit node"); err != nil || visits < 0 || visits > MaxVisitsPerNode {
			return Checkpoint{}, fmt.Errorf("%w: invalid visit evidence", ErrInvalidCheckpoint)
		}
	}
	if err := validateCheckpointStatus(checkpoint.Status); err != nil {
		return Checkpoint{}, fmt.Errorf("%w: %v", ErrInvalidCheckpoint, err)
	}
	switch checkpoint.Status {
	case CheckpointFailed, CheckpointCancelled, CheckpointUnknown:
		if checkpoint.FailureCode == "" {
			return Checkpoint{}, fmt.Errorf("%w: %s checkpoint lacks a failure code", ErrInvalidCheckpoint, checkpoint.Status)
		}
	default:
		if checkpoint.FailureCode != "" {
			return Checkpoint{}, fmt.Errorf("%w: %s checkpoint cannot carry a failure code", ErrInvalidCheckpoint, checkpoint.Status)
		}
	}
	if checkpoint.PendingApprovalID != "" {
		if err := validateCheckpointIdentifier(checkpoint.PendingApprovalID, MaxCheckpointIDBytes, "approval id"); err != nil {
			return Checkpoint{}, fmt.Errorf("%w: %v", ErrInvalidCheckpoint, err)
		}
	}
	if checkpoint.Status == CheckpointWaitingApproval && checkpoint.PendingApprovalID == "" {
		return Checkpoint{}, fmt.Errorf("%w: waiting approval lacks an approval id", ErrInvalidCheckpoint)
	}
	if checkpoint.Status != CheckpointWaitingApproval && checkpoint.PendingApprovalID != "" {
		return Checkpoint{}, fmt.Errorf("%w: only waiting approval may carry an approval id", ErrInvalidCheckpoint)
	}
	if checkpoint.PendingApprovalID != "" && checkpoint.ResolvedApproval != nil {
		return Checkpoint{}, fmt.Errorf("%w: pending and resolved approval cannot coexist", ErrInvalidCheckpoint)
	}
	if checkpoint.ResolvedApproval != nil {
		if checkpoint.Status == CheckpointWaitingApproval {
			return Checkpoint{}, fmt.Errorf("%w: waiting approval cannot carry resolved approval", ErrInvalidCheckpoint)
		}
		if err := validateCheckpointApproval(*checkpoint.ResolvedApproval, checkpoint); err != nil {
			return Checkpoint{}, fmt.Errorf("%w: %v", ErrInvalidCheckpoint, err)
		}
	}
	if (checkpoint.Status == CheckpointCompleted || checkpoint.Status == CheckpointFailed || checkpoint.Status == CheckpointCancelled) && checkpoint.AttemptID != "" {
		return Checkpoint{}, fmt.Errorf("%w: terminal checkpoint cannot carry an attempt id", ErrInvalidCheckpoint)
	}
	if len(checkpoint.FailureCode) > MaxCheckpointFailureBytes || !utf8.ValidString(checkpoint.FailureCode) || containsControl(checkpoint.FailureCode) {
		return Checkpoint{}, fmt.Errorf("%w: invalid failure code", ErrInvalidCheckpoint)
	}
	if checkpoint.LastEdge != nil {
		if err := validateEdgeEvidence(*checkpoint.LastEdge); err != nil {
			return Checkpoint{}, fmt.Errorf("%w: %v", ErrInvalidCheckpoint, err)
		}
	}
	canonical, err := canonicalCheckpointState(checkpoint.State)
	if err != nil {
		return Checkpoint{}, fmt.Errorf("%w: %v", ErrInvalidCheckpoint, err)
	}
	checkpoint.State = canonical
	checkpoint = CloneCheckpoint(checkpoint)
	return checkpoint, nil
}

// ValidateCheckpointVersion verifies the immutable metadata, checkpoint
// identity, origin, and digest before returning a defensive canonical copy.
func ValidateCheckpointVersion(version CheckpointVersion) (CheckpointVersion, error) {
	checkpoint, err := ValidateCheckpoint(version.Checkpoint)
	if err != nil {
		return CheckpointVersion{}, err
	}
	info, err := ValidateCheckpointVersionInfo(version.Info)
	if err != nil {
		return CheckpointVersion{}, err
	}
	if info.Key != checkpoint.Key || info.Revision != checkpoint.Revision {
		return CheckpointVersion{}, fmt.Errorf("%w: version does not describe checkpoint", ErrInvalidCheckpoint)
	}
	hash, err := CheckpointDigest(checkpoint)
	if err != nil {
		return CheckpointVersion{}, err
	}
	if info.CheckpointHash != hash {
		return CheckpointVersion{}, fmt.Errorf("%w: checkpoint version digest mismatch", ErrInvalidCheckpoint)
	}
	return CloneCheckpointVersion(CheckpointVersion{Info: info, Checkpoint: checkpoint}), nil
}

// ValidateCheckpointVersionInfo verifies bounded history metadata without
// materializing a checkpoint document.
func ValidateCheckpointVersionInfo(info CheckpointVersionInfo) (CheckpointVersionInfo, error) {
	if err := ValidateCheckpointKey(info.Key); err != nil {
		return CheckpointVersionInfo{}, err
	}
	if info.Revision == 0 || info.ID != CheckpointVersionID(info.Key, info.Revision) {
		return CheckpointVersionInfo{}, fmt.Errorf("%w: invalid checkpoint version identity", ErrInvalidCheckpoint)
	}
	switch info.Origin {
	case CheckpointVersionOriginCommit:
		if info.Revision == 1 && info.ParentID != "" {
			return CheckpointVersionInfo{}, fmt.Errorf("%w: initial commit cannot have a parent", ErrInvalidCheckpoint)
		}
		if info.Revision > 1 && info.ParentID != CheckpointVersionID(info.Key, info.Revision-1) {
			return CheckpointVersionInfo{}, fmt.Errorf("%w: commit parent must be the preceding version", ErrInvalidCheckpoint)
		}
	case CheckpointVersionOriginMigrationFloor:
		if info.ParentID != "" {
			return CheckpointVersionInfo{}, fmt.Errorf("%w: migration floor cannot have a parent", ErrInvalidCheckpoint)
		}
	case CheckpointVersionOriginFork:
		if info.Revision != 1 || !validCheckpointVersionID(info.ParentID) || info.ParentID == info.ID {
			return CheckpointVersionInfo{}, fmt.Errorf("%w: invalid fork parent version identity", ErrInvalidCheckpoint)
		}
	default:
		return CheckpointVersionInfo{}, fmt.Errorf("%w: invalid checkpoint version origin", ErrInvalidCheckpoint)
	}
	if !validCheckpointVersionID(info.CheckpointHash) {
		return CheckpointVersionInfo{}, fmt.Errorf("%w: invalid checkpoint version digest", ErrInvalidCheckpoint)
	}
	if info.CreatedAt <= 0 {
		return CheckpointVersionInfo{}, fmt.Errorf("%w: invalid checkpoint version timestamp", ErrInvalidCheckpoint)
	}
	return CloneCheckpointVersionInfo(info), nil
}

func validCheckpointVersionID(value string) bool {
	return len(value) == sha256.Size*2 && isLowerHex(value)
}

func ValidateTransition(transition Transition, checkpoint Checkpoint) (Transition, error) {
	if err := ValidateCheckpointKey(transition.Key); err != nil || transition.Key != checkpoint.Key {
		return Transition{}, fmt.Errorf("%w: transition key does not match checkpoint", ErrInvalidTransition)
	}
	if transition.Revision != checkpoint.Revision || transition.Revision == 0 || transition.ID != TransitionID(transition.Key, transition.Revision) {
		return Transition{}, fmt.Errorf("%w: invalid transition identity", ErrInvalidTransition)
	}
	if transition.To != checkpoint.Status || transition.SegmentID != checkpoint.SegmentID || transition.CurrentNodeID != checkpoint.CurrentNodeID || transition.AttemptID != checkpoint.AttemptID {
		return Transition{}, fmt.Errorf("%w: transition does not describe resulting checkpoint", ErrInvalidTransition)
	}
	if transition.SourceNodeID != "" {
		if err := validateCheckpointIdentifier(transition.SourceNodeID, MaxCheckpointIDBytes, "transition source node"); err != nil {
			return Transition{}, fmt.Errorf("%w: %v", ErrInvalidTransition, err)
		}
	}
	if transition.SourceSegmentID != "" {
		if err := validateCheckpointIdentifier(transition.SourceSegmentID, MaxCheckpointIDBytes, "transition source segment"); err != nil {
			return Transition{}, fmt.Errorf("%w: %v", ErrInvalidTransition, err)
		}
	}
	if transition.SourceAttemptID != "" {
		if err := validateCheckpointIdentifier(transition.SourceAttemptID, MaxCheckpointIDBytes, "transition source attempt"); err != nil {
			return Transition{}, fmt.Errorf("%w: %v", ErrInvalidTransition, err)
		}
	}
	if !legalTransition(transition.From, transition.To) {
		return Transition{}, fmt.Errorf("%w: illegal status transition %q -> %q", ErrInvalidTransition, transition.From, transition.To)
	}
	if transition.To == CheckpointReady && transition.AttemptID != "" && (transition.From != CheckpointWaitingApproval || (transition.OutcomeCode != "approval_approved" && transition.OutcomeCode != "approval_denied")) {
		return Transition{}, fmt.Errorf("%w: ready checkpoint attempt requires approved resume", ErrInvalidTransition)
	}
	if len(transition.OutcomeCode) > MaxCheckpointFailureBytes || !utf8.ValidString(transition.OutcomeCode) || containsControl(transition.OutcomeCode) {
		return Transition{}, fmt.Errorf("%w: invalid outcome code", ErrInvalidTransition)
	}
	if transition.Edge != nil {
		if err := validateEdgeEvidence(*transition.Edge); err != nil {
			return Transition{}, fmt.Errorf("%w: %v", ErrInvalidTransition, err)
		}
	}
	if transition.Approval != nil {
		if err := validateApprovalEvidence(*transition.Approval, transition, checkpoint); err != nil {
			return Transition{}, fmt.Errorf("%w: %v", ErrInvalidTransition, err)
		}
	}
	return CloneTransition(transition), nil
}

func validateApprovalEvidence(evidence ApprovalEvidence, transition Transition, checkpoint Checkpoint) error {
	if evidence.TenantID == "" || evidence.TenantID != checkpoint.Key.TenantID ||
		!validCheckpointText(evidence.TenantID, MaxCheckpointIDBytes, "approval tenant") ||
		!validCheckpointText(evidence.ApprovalID, MaxCheckpointIDBytes, "approval id") ||
		!validCheckpointText(evidence.SourceSegmentID, MaxCheckpointIDBytes, "approval source segment") ||
		!validCheckpointText(evidence.ActorID, MaxApprovalActorBytes, "approval actor") ||
		!validCheckpointText(evidence.AuthorizationBasis, MaxApprovalBasisBytes, "approval authorization basis") {
		return errors.New("invalid approval evidence")
	}
	if transition.SourceSegmentID == "" || evidence.SourceSegmentID != transition.SourceSegmentID {
		return errors.New("approval evidence source segment does not match transition source")
	}
	if evidence.Decision != "approved" && evidence.Decision != "denied" {
		return errors.New("invalid approval decision")
	}
	if evidence.Revision == 0 || evidence.Revision+1 != checkpoint.Revision {
		return errors.New("approval evidence does not match source checkpoint")
	}
	if evidence.Decision == "approved" && (transition.From != CheckpointWaitingApproval || checkpoint.Status != CheckpointReady || transition.OutcomeCode != "approval_approved") {
		return errors.New("approved evidence does not describe an approved resume")
	}
	if evidence.Decision == "denied" && (transition.From != CheckpointWaitingApproval || (checkpoint.Status != CheckpointFailed && checkpoint.Status != CheckpointReady) || transition.OutcomeCode != "approval_denied") {
		return errors.New("denied evidence does not describe a denied resume")
	}
	return nil
}

func validateCheckpointApproval(evidence ApprovalEvidence, checkpoint Checkpoint) error {
	if evidence.TenantID == "" || evidence.TenantID != checkpoint.Key.TenantID ||
		!validCheckpointText(evidence.TenantID, MaxCheckpointIDBytes, "approval tenant") ||
		!validCheckpointText(evidence.ApprovalID, MaxCheckpointIDBytes, "approval id") ||
		!validCheckpointText(evidence.SourceSegmentID, MaxCheckpointIDBytes, "approval source segment") ||
		!validCheckpointText(evidence.ActorID, MaxApprovalActorBytes, "approval actor") ||
		!validCheckpointText(evidence.AuthorizationBasis, MaxApprovalBasisBytes, "approval authorization basis") ||
		(evidence.Decision != "approved" && evidence.Decision != "denied") || evidence.Revision == 0 {
		return errors.New("invalid resolved approval evidence")
	}
	return nil
}

func validCheckpointText(value string, max int, _ string) bool {
	if value == "" || len(value) > max || !utf8.ValidString(value) || strings.TrimSpace(value) != value {
		return false
	}
	return !containsControl(value)
}

func legalTransition(from, to CheckpointStatus) bool {
	switch from {
	case "":
		return to == CheckpointReady
	case CheckpointReady:
		return to == CheckpointExecuting || to == CheckpointCancelled
	case CheckpointExecuting:
		return to == CheckpointReady || to == CheckpointWaitingApproval || to == CheckpointCompleted || to == CheckpointFailed || to == CheckpointCancelled || to == CheckpointUnknown
	case CheckpointWaitingApproval:
		return to == CheckpointReady || to == CheckpointFailed
	default:
		return false
	}
}

func validateCheckpointStatus(status CheckpointStatus) error {
	switch status {
	case CheckpointReady, CheckpointExecuting, CheckpointWaitingApproval, CheckpointCompleted, CheckpointFailed, CheckpointCancelled, CheckpointUnknown:
		return nil
	default:
		return fmt.Errorf("unknown checkpoint status %q", status)
	}
}

func validateCheckpointKey(key CheckpointKey) error {
	for name, value := range map[string]string{"tenant id": key.TenantID, "session id": key.SessionID, "run id": key.RunID} {
		if err := validateCheckpointIdentifier(value, MaxCheckpointIDBytes, name); err != nil {
			return err
		}
	}
	return nil
}

func validateCheckpointIdentifier(value string, maximum int, name string) error {
	if value == "" || len(value) > maximum || !utf8.ValidString(value) || strings.TrimSpace(value) != value || containsControl(value) {
		return fmt.Errorf("%s is invalid", name)
	}
	for _, r := range value {
		if unicode.IsSpace(r) {
			return fmt.Errorf("%s contains whitespace", name)
		}
	}
	return nil
}

func containsControl(value string) bool {
	for _, r := range value {
		if unicode.IsControl(r) {
			return true
		}
	}
	return false
}

func validateEdgeEvidence(edge EdgeEvidence) error {
	if err := validateCheckpointIdentifier(edge.From, MaxCheckpointEdgeNameBytes, "edge from"); err != nil {
		return err
	}
	if err := validateCheckpointIdentifier(edge.To, MaxCheckpointEdgeNameBytes, "edge to"); err != nil {
		return err
	}
	switch edge.Kind {
	case EdgeDefault, EdgeError:
		if edge.Predicate != "" {
			return errors.New("default/error evidence has predicate")
		}
	case EdgeConditional:
		if err := validateCheckpointIdentifier(edge.Predicate, MaxCheckpointEdgeNameBytes, "edge predicate"); err != nil {
			return err
		}
	default:
		return errors.New("unknown edge evidence kind")
	}
	return nil
}

func canonicalCheckpointState(input State) (State, error) {
	if len(input) > MaxStateFields {
		return nil, fmt.Errorf("state field count exceeds %d", MaxStateFields)
	}
	out := make(State, len(input))
	for key, raw := range input {
		if err := validateCheckpointIdentifier(key, MaxNodeIDBytes, "state field"); err != nil {
			return nil, err
		}
		if len(raw) == 0 || len(raw) > MaxStateFieldBytes || !utf8.Valid(raw) {
			return nil, fmt.Errorf("state field %q has invalid size or UTF-8", key)
		}
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		var value any
		if err := decoder.Decode(&value); err != nil || len(bytes.TrimSpace(raw[decoder.InputOffset():])) != 0 {
			return nil, fmt.Errorf("state field %q is not one JSON value", key)
		}
		canonical, err := json.Marshal(value)
		if err != nil || len(canonical) > MaxStateFieldBytes {
			return nil, fmt.Errorf("state field %q cannot be canonicalized", key)
		}
		out[key] = json.RawMessage(canonical)
	}
	encoded, err := json.Marshal(out)
	if err != nil || len(encoded) > MaxStateBytes {
		return nil, fmt.Errorf("state exceeds %d bytes", MaxStateBytes)
	}
	return out, nil
}

func cloneCheckpointState(input State) State {
	out := make(State, len(input))
	for key, raw := range input {
		out[key] = append(json.RawMessage(nil), raw...)
	}
	return out
}
