package programmatic

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/whhhh1500/auto-agent/pkg/core"
)

const (
	// RouteAutoProbeOnce is the opt-in v2 route. Its projection is deliberately
	// implemented outside the generic PTC catalog and execute capabilities.
	RouteAutoProbeOnce RouteMode = "auto_probe_once"

	// ProbeManifestKey and ProbeManifestVersion opt a capability into the
	// neutral-probe contract. The marker is a routing trait, never authority.
	ProbeManifestKey     = "harness.programmatic.neutral_probe"
	ProbeManifestVersion = "1"

	// ProbeFactsMetadataKey and ProbeFactsVersion identify the trusted result
	// metadata emitted by a neutral-probe provider.
	ProbeFactsMetadataKey = "harness.programmatic.probe_facts"
	ProbeFactsVersion     = "1"

	// The limits below bound the trusted, provider-produced data held in a
	// route plan. They are intentionally independent from the larger generic
	// capability-output and PTC limits.
	MaxProbeOutputBytes      = 64 << 10
	MaxProbeFactsBytes       = MaxProbeOutputBytes
	MaxProbeCandidateBytes   = 16 << 10
	MaxProbeCandidates       = 128
	MaxProbeSelections       = 128
	MaxProbeJSONDepth        = 32
	MaxProbeJSONNodes        = 4096
	MaxProbeLabelBytes       = 256
	MaxProbeModelReturnBytes = MaxProbeOutputBytes
	MaxProbeSchemaBytes      = 256 << 10
	MaxProbeSchemaDepth      = 64
	// Projection is deliberately shallow. Its values are selected from one
	// frozen follow-up contract, never from model-provided source or paths.
	MaxProbeProjectionFields  = 16
	probePlanAlgorithmVersion = "probe-plan/v3-direct-choice-guidance"
	probeProjectionVersion    = "probe-projection/v1"
	projectedEnvelopeBytes    = 256
)

const programmaticCapabilityContract = "harness.programmatic/v1"

var (
	ErrInvalidProbePlan       = errors.New("invalid probe route plan")
	ErrInvalidProbeFacts      = errors.New("invalid probe facts")
	ErrProbeCandidateRejected = errors.New("probe candidate is not authorized")
	ErrProbeReturnRejected    = errors.New("probe route execute result is invalid")
)

// ProbeCandidate is one host-approved follow-up input. Facts are JSON-native
// data for route-local PTC filtering; they are never parsed from probe Content.
// Returned values are always detached from the plan that produced them.
type ProbeCandidate struct {
	Label    string
	Args     map[string]any
	Facts    any
	HasFacts bool
}

// ProbeFacts is the strict, versioned result contract of a neutral probe.
// Its exported fields are copies, so callers can inspect or modify them
// without changing a constructed ProbePlan.
type ProbeFacts struct {
	Version              string
	FollowupCapabilityID string
	Candidates           []ProbeCandidate
	MaxModelReturnBytes  int
}

// ProbePlanOptions identifies the generic execute capability to receive the
// transformed route-specific call. Empty selects program.execute.
type ProbePlanOptions struct {
	ExecuteToolID string
	// ProgramContract supplies the route-local PTC contract. A missing or
	// invalid contract is rejected when the plan is constructed; callers must
	// inject the actual execution compiler rather than accepting source locally.
	ProgramContract ProbeProgramContract
}

// ProbeProgramContract is the explicit, immutable seam between a route plan
// and its program executor. The application layer owns candidate authorization;
// the injected validator owns the complete PTC grammar and literal tool scan.
// It must fail closed by returning an error for malformed or unsupported source.
type ProbeProgramContract struct {
	Version        string
	LanguageGuide  string
	MaxSourceBytes int
	// MaxToolCalls is the interpreter's fixed per-program child-call ceiling.
	// The route plan clamps model selection to it before any effect begins.
	MaxToolCalls   int
	ValidateSource func(source string) ([]string, error)
}

// projectedOutput is one host-approved, top-level scalar output field. Its
// source is constructed once at plan creation and never supplied by a model.
type projectedOutput struct {
	Name            string
	Schema          map[string]any
	MaxEncodedBytes int
	Source          string
	SourceDigest    string
}

// ProbePlan is an immutable logical plan derived from a completed neutral
// probe, the exact frozen probe/follow-up capability records, and fixed PTC
// grammar. All fields are private so copies of the value retain the same
// immutable meaning.
type ProbePlan struct {
	facts               ProbeFacts
	probeID             string
	probeBindingDigest  string
	followupID          string
	followupBinding     string
	executeID           string
	inputSchema         map[string]any
	directSourceSchema  core.ToolSchema
	directSchema        core.ToolSchema
	executeSchema       core.ToolSchema
	maxSelections       int
	digest              string
	executeSchemaDigest string
	programContract     ProbeProgramContract
	projections         []projectedOutput
}

// ProbePlanVerifier reconstructs one plan from durable probe evidence. isProbe
// distinguishes an ordinary completed call from an invalid or unavailable
// probe receipt; a nil verifier must be treated as unavailable by consumers.
type ProbePlanVerifier func(ctx context.Context, runID, callID string) (plan ProbePlan, isProbe bool, err error)

// ParseProbeFacts extracts only the versioned trusted metadata value. It does
// not read result.Content, so free-form provider output can never mint route
// authority. Unknown fields, duplicate-like hostile values, non-JSON data and
// all bound violations fail closed.
func ParseProbeFacts(result core.CapabilityResult) (facts ProbeFacts, err error) {
	defer func() {
		if recover() != nil {
			facts = ProbeFacts{}
			err = ErrInvalidProbeFacts
		}
	}()
	if !result.OK || len(result.Metadata) != 1 {
		return ProbeFacts{}, fmt.Errorf("%w: successful metadata is required", ErrInvalidProbeFacts)
	}
	raw, present := result.Metadata[ProbeFactsMetadataKey]
	if !present {
		return ProbeFacts{}, fmt.Errorf("%w: metadata marker is missing", ErrInvalidProbeFacts)
	}
	cloned, err := cloneProbeJSON(raw, probeJSONLimits{maxBytes: MaxProbeFactsBytes, maxDepth: MaxProbeJSONDepth, maxNodes: MaxProbeJSONNodes})
	if err != nil {
		return ProbeFacts{}, fmt.Errorf("%w: metadata is not bounded JSON", ErrInvalidProbeFacts)
	}
	root, ok := cloned.(map[string]any)
	if !ok || root == nil {
		return ProbeFacts{}, fmt.Errorf("%w: metadata must be an object", ErrInvalidProbeFacts)
	}
	encoded, err := marshalProbeJSON(root, MaxProbeFactsBytes, MaxProbeJSONDepth)
	if err != nil {
		return ProbeFacts{}, fmt.Errorf("%w: metadata encoding is invalid", ErrInvalidProbeFacts)
	}
	decoded, err := decodeProbeJSON(encoded, MaxProbeJSONDepth, MaxProbeJSONNodes)
	if err != nil {
		return ProbeFacts{}, fmt.Errorf("%w: metadata decoding is invalid", ErrInvalidProbeFacts)
	}
	object, ok := decoded.(map[string]any)
	if !ok {
		return ProbeFacts{}, fmt.Errorf("%w: metadata must be an object", ErrInvalidProbeFacts)
	}
	return parseProbeFactsObject(object)
}

// NewProbePlan validates the two frozen capabilities and derives an immutable
// route plan from the completed probe result. The caller supplies the exact
// frozen follow-up selected by the facts contract; callers with a full frozen
// snapshot should resolve that ID before calling this constructor.
func NewProbePlan(probe, followup core.SnapshotCapability, result core.CapabilityResult, options ProbePlanOptions) (plan ProbePlan, err error) {
	defer func() {
		if recover() != nil {
			plan = ProbePlan{}
			err = ErrInvalidProbePlan
		}
	}()
	if err := validateProbeProgramContract(options.ProgramContract); err != nil {
		return ProbePlan{}, err
	}
	facts, err := ParseProbeFacts(result)
	if err != nil {
		return ProbePlan{}, err
	}
	if err := validateProbeManifest(probe.Manifest); err != nil {
		return ProbePlan{}, err
	}
	if len(result.Content) > probe.Manifest.MaxOutputBytes {
		return ProbePlan{}, fmt.Errorf("%w: probe result content exceeds its frozen output limit", ErrInvalidProbePlan)
	}
	if raw := result.Metadata[ProbeFactsMetadataKey]; raw == nil {
		return ProbePlan{}, fmt.Errorf("%w: probe facts metadata is unavailable", ErrInvalidProbePlan)
	} else if _, err := canonicalProbeJSON(raw, probeJSONLimits{maxBytes: probe.Manifest.MaxOutputBytes, maxDepth: MaxProbeJSONDepth, maxNodes: MaxProbeJSONNodes}); err != nil {
		return ProbePlan{}, fmt.Errorf("%w: probe facts exceed its frozen output limit", ErrInvalidProbePlan)
	}
	if err := validateFollowupManifest(followup.Manifest, probe.Manifest.ID); err != nil {
		return ProbePlan{}, err
	}
	if facts.FollowupCapabilityID != followup.Manifest.ID {
		return ProbePlan{}, fmt.Errorf("%w: facts follow-up does not match frozen capability", ErrInvalidProbePlan)
	}

	executeID := options.ExecuteToolID
	if executeID == "" {
		executeID = DefaultExecuteToolID
	}
	if core.ValidateNamespacedID(executeID) != nil || executeID == probe.Manifest.ID || executeID == followup.Manifest.ID {
		return ProbePlan{}, fmt.Errorf("%w: execute tool identifier is invalid", ErrInvalidProbePlan)
	}

	direct, err := toolSchema(followup.Manifest)
	if err != nil {
		return ProbePlan{}, fmt.Errorf("%w: follow-up tool schema: %v", ErrInvalidProbePlan, err)
	}
	direct, err = cloneProbeToolSchema(direct)
	if err != nil {
		return ProbePlan{}, fmt.Errorf("%w: follow-up tool schema is not bounded", ErrInvalidProbePlan)
	}
	directSource := direct
	inputSchema, err := cloneSchema(followup.Manifest.InputSchema)
	if err != nil {
		return ProbePlan{}, fmt.Errorf("%w: follow-up input schema is invalid", ErrInvalidProbePlan)
	}
	outputSchema, err := cloneSchema(followup.Manifest.OutputSchema)
	if err != nil {
		return ProbePlan{}, fmt.Errorf("%w: follow-up output schema is invalid", ErrInvalidProbePlan)
	}
	for index, candidate := range facts.Candidates {
		if err := validateCandidateArgs(candidate.Args, inputSchema, direct.Parameters); err != nil {
			return ProbePlan{}, fmt.Errorf("%w: candidate %d arguments: %v", ErrInvalidProbePlan, index, err)
		}
	}

	probeDigest, err := bindingDigest(probe)
	if err != nil {
		return ProbePlan{}, fmt.Errorf("%w: probe binding is invalid", ErrInvalidProbePlan)
	}
	followupDigest, err := bindingDigest(followup)
	if err != nil {
		return ProbePlan{}, fmt.Errorf("%w: follow-up binding is invalid", ErrInvalidProbePlan)
	}
	maxSelections := MaxProbeSelections
	if len(facts.Candidates) < maxSelections {
		maxSelections = len(facts.Candidates)
	}
	if limit := followup.Manifest.PerTurnBudget; limit > 0 && limit < maxSelections {
		maxSelections = limit
	}
	if options.ProgramContract.MaxToolCalls < maxSelections {
		maxSelections = options.ProgramContract.MaxToolCalls
	}
	if maxSelections < 1 {
		return ProbePlan{}, fmt.Errorf("%w: follow-up has no safe selection budget", ErrInvalidProbePlan)
	}

	projections, err := buildProjectedOutputs(outputSchema, facts.MaxModelReturnBytes, maxSelections, followup.Manifest.ID, options.ProgramContract)
	if err != nil {
		return ProbePlan{}, err
	}
	direct, err = buildProbeDirectSchema(direct, len(facts.Candidates), len(projections) > 0)
	if err != nil {
		return ProbePlan{}, fmt.Errorf("%w: direct choice schema", ErrInvalidProbePlan)
	}
	var execute core.ToolSchema
	var executeSchemaDigest string
	if len(projections) > 0 {
		execute, err = buildProbeExecuteSchema(executeID, followup.Manifest, followupDigest, len(facts.Candidates), maxSelections, facts.MaxModelReturnBytes, projections)
		if err != nil {
			return ProbePlan{}, err
		}
		executeSchemaDigest, err = probeDigestFor(execute, MaxProbeSchemaBytes)
		if err != nil {
			return ProbePlan{}, fmt.Errorf("%w: execute schema digest", ErrInvalidProbePlan)
		}
	}
	plan = ProbePlan{
		facts:               facts,
		probeID:             probe.Manifest.ID,
		probeBindingDigest:  probeDigest,
		followupID:          followup.Manifest.ID,
		followupBinding:     followupDigest,
		executeID:           executeID,
		inputSchema:         inputSchema,
		directSourceSchema:  directSource,
		directSchema:        direct,
		executeSchema:       execute,
		maxSelections:       maxSelections,
		executeSchemaDigest: executeSchemaDigest,
		programContract:     options.ProgramContract,
		projections:         projections,
	}
	plan.digest, err = planDigest(plan)
	if err != nil {
		return ProbePlan{}, fmt.Errorf("%w: plan digest", ErrInvalidProbePlan)
	}
	return plan, nil
}

// Facts returns a detached logical copy of the trusted probe facts.
func (p ProbePlan) Facts() ProbeFacts {
	if !p.valid() {
		return ProbeFacts{}
	}
	copy, err := cloneProbeFacts(p.facts)
	if err != nil {
		return ProbeFacts{}
	}
	return copy
}

// Digest returns the SHA-256 digest of the canonical route-plan representation.
func (p ProbePlan) Digest() string {
	if !p.valid() {
		return ""
	}
	return p.digest
}

// ExecuteSchemaDigest returns the digest of the exact dynamic execute schema.
func (p ProbePlan) ExecuteSchemaDigest() string {
	if !p.valid() {
		return ""
	}
	return p.executeSchemaDigest
}

// DirectToolSchema returns the only permitted direct follow-up tool. Its
// parameters remain the follow-up schema; ValidateDirect performs the
// candidate-membership check before Core receives a call.
func (p ProbePlan) DirectToolSchema() core.ToolSchema {
	if !p.valid() {
		return core.ToolSchema{}
	}
	return cloneProbeToolSchemaOrZero(p.directSchema)
}

// MaxSelections is the immutable number of follow-up targets that one v2
// choice may reserve. Zero reports an invalid plan and must be treated as a
// fail-closed value by route consumers.
func (p ProbePlan) MaxSelections() int {
	if !p.valid() || p.maxSelections < 1 {
		return 0
	}
	return p.maxSelections
}

// ExecuteToolSchema returns the dynamic route-specific execute schema. It is
// intentionally not the generic program.execute schema: only source and
// bounded candidate selection are model-provided.
func (p ProbePlan) ExecuteToolSchema() core.ToolSchema {
	if !p.valid() {
		return core.ToolSchema{}
	}
	return cloneProbeToolSchemaOrZero(p.executeSchema)
}

// DynamicExecuteToolSchema is the explicit route-facing name for
// ExecuteToolSchema. It keeps the generic program.execute schema distinct from
// this per-plan model surface.
func (p ProbePlan) DynamicExecuteToolSchema() core.ToolSchema {
	return p.ExecuteToolSchema()
}

// ChoiceTools returns the planned direct follow-up and, only when the frozen
// output contract has a safe compact projection, a route-specific execute
// surface. Neither exposes probe metadata or raw candidate data.
func (p ProbePlan) ChoiceTools() (tools []core.ToolSchema, err error) {
	defer func() {
		if recover() != nil {
			tools = nil
			err = ErrInvalidProbePlan
		}
	}()
	if !p.valid() {
		return nil, ErrInvalidProbePlan
	}
	direct := cloneProbeToolSchemaOrZero(p.directSchema)
	if direct.Name == "" {
		return nil, ErrInvalidProbePlan
	}
	if len(p.projections) == 0 {
		return []core.ToolSchema{direct}, nil
	}
	execute := cloneProbeToolSchemaOrZero(p.executeSchema)
	if execute.Name == "" {
		return nil, ErrInvalidProbePlan
	}
	return []core.ToolSchema{direct, execute}, nil
}

// ValidateDirect accepts only an exact frozen candidate argument object. It
// validates both the follow-up input contract and model-facing tool schema.
func (p ProbePlan) ValidateDirect(args map[string]any) (err error) {
	defer func() {
		if recover() != nil {
			err = ErrProbeCandidateRejected
		}
	}()
	if !p.valid() || args == nil {
		return ErrProbeCandidateRejected
	}
	if err := validateCandidateArgs(args, p.inputSchema, p.directSchema.Parameters); err != nil {
		return fmt.Errorf("%w: arguments do not satisfy follow-up schema", ErrProbeCandidateRejected)
	}
	encoded, err := marshalCandidateArgs(args)
	if err != nil {
		return ErrProbeCandidateRejected
	}
	for _, candidate := range p.facts.Candidates {
		approved, candidateErr := marshalCandidateArgs(candidate.Args)
		if candidateErr == nil && bytes.Equal(encoded, approved) {
			return nil
		}
	}
	return ErrProbeCandidateRejected
}

// PrepareExecute converts a valid route-specific projected choice into the
// unchanged generic program.execute contract. The model never supplies PTC
// source, bindings, targets, or property paths. Candidate indices never reach
// Core: the returned input.targets array contains only host-owned detached
// candidate args and facts.
func (p ProbePlan) PrepareExecute(args map[string]any) (prepared map[string]any, err error) {
	defer func() {
		if recover() != nil {
			prepared = nil
			err = ErrInvalidProbePlan
		}
	}()
	if !p.valid() || args == nil {
		return nil, ErrInvalidProbePlan
	}
	if err := core.ValidateArgs(p.executeSchema.Parameters, args); err != nil {
		return nil, fmt.Errorf("%w: execute arguments", ErrInvalidProbePlan)
	}
	projection, ok := args["projection"].(string)
	if !ok {
		return nil, fmt.Errorf("%w: projection", ErrInvalidProbePlan)
	}
	projected, ok := p.projection(projection)
	if !ok {
		return nil, fmt.Errorf("%w: projection", ErrInvalidProbePlan)
	}
	selection, err := p.selection(args["selection"])
	if err != nil {
		return nil, err
	}
	targets := make([]any, 0, len(selection))
	for _, index := range selection {
		candidate := p.facts.Candidates[index]
		argsCopy, err := cloneProbeMap(candidate.Args)
		if err != nil {
			return nil, ErrInvalidProbePlan
		}
		factsCopy, err := cloneProbeJSONValue(candidate.Facts)
		if err != nil {
			return nil, ErrInvalidProbePlan
		}
		// Every target has a facts key. nil is the fixed representation of an
		// optional omitted facts value, which keeps the model grammar stable.
		targets = append(targets, map[string]any{"args": argsCopy, "facts": factsCopy})
	}
	return map[string]any{
		"source":   projected.Source,
		"bindings": map[string]any{p.followupID: p.followupBinding},
		"input":    map[string]any{"targets": targets, "projection": projection},
	}, nil
}

// ValidateReturn checks the whole serialized execute result before it enters
// the final model context. It accepts neither oversized nor non-JSON-native
// result metadata, and it does not retain the provider-owned result.
func (p ProbePlan) ValidateReturn(result core.CapabilityResult) (err error) {
	defer func() {
		if recover() != nil {
			err = ErrProbeReturnRejected
		}
	}()
	if !p.valid() {
		return ErrProbeReturnRejected
	}
	value := map[string]any{"content": result.Content, "ok": result.OK}
	if result.Metadata != nil {
		value["metadata"] = result.Metadata
	}
	if _, err := canonicalProbeJSON(value, probeJSONLimits{maxBytes: p.facts.MaxModelReturnBytes, maxDepth: MaxProbeJSONDepth, maxNodes: MaxProbeJSONNodes}); err != nil {
		return ErrProbeReturnRejected
	}
	var output map[string]any
	if json.Unmarshal([]byte(result.Content), &output) != nil {
		return ErrProbeReturnRejected
	}
	items, ok := output["value"].([]any)
	if !ok || len(items) == 0 {
		return ErrProbeReturnRejected
	}
	if _, ok := output["tool_calls"].(float64); !ok {
		return ErrProbeReturnRejected
	}
	return nil
}

// ValidateExecuteReturn verifies the compact generic execute result against the
// exact host-prepared projection and target list before a final model turn.
func (p ProbePlan) ValidateExecuteReturn(result core.CapabilityResult, prepared map[string]any) error {
	if err := p.ValidateReturn(result); err != nil {
		return err
	}
	input, ok := prepared["input"].(map[string]any)
	if !ok {
		return ErrProbeReturnRejected
	}
	projectionName, ok := input["projection"].(string)
	if !ok {
		return ErrProbeReturnRejected
	}
	projection, ok := p.projection(projectionName)
	if !ok {
		return ErrProbeReturnRejected
	}
	targets, ok := input["targets"].([]any)
	if !ok || len(targets) == 0 || len(targets) > p.maxSelections {
		return ErrProbeReturnRejected
	}
	var output map[string]any
	if json.Unmarshal([]byte(result.Content), &output) != nil {
		return ErrProbeReturnRejected
	}
	items, _ := output["value"].([]any)
	if len(items) != len(targets) || output["tool_calls"] != float64(len(targets)) {
		return ErrProbeReturnRejected
	}
	for _, item := range items {
		if core.ValidateJSONValue(projection.Schema, item) != nil {
			return ErrProbeReturnRejected
		}
	}
	return nil
}

func parseProbeFactsObject(object map[string]any) (ProbeFacts, error) {
	if !exactProbeKeys(object, "version", "followup_capability_id", "candidates", "max_model_return_bytes") {
		return ProbeFacts{}, fmt.Errorf("%w: unknown or missing field", ErrInvalidProbeFacts)
	}
	version, ok := object["version"].(string)
	if !ok || version != ProbeFactsVersion {
		return ProbeFacts{}, fmt.Errorf("%w: unsupported version", ErrInvalidProbeFacts)
	}
	followupID, ok := object["followup_capability_id"].(string)
	if !ok || core.ValidateNamespacedID(followupID) != nil {
		return ProbeFacts{}, fmt.Errorf("%w: follow-up capability identifier", ErrInvalidProbeFacts)
	}
	maxReturn, ok := positiveProbeInt(object["max_model_return_bytes"], MaxProbeModelReturnBytes)
	if !ok {
		return ProbeFacts{}, fmt.Errorf("%w: max model return bytes", ErrInvalidProbeFacts)
	}
	rawCandidates, ok := object["candidates"].([]any)
	if !ok || len(rawCandidates) == 0 || len(rawCandidates) > MaxProbeCandidates {
		return ProbeFacts{}, fmt.Errorf("%w: candidate count", ErrInvalidProbeFacts)
	}
	candidates := make([]ProbeCandidate, 0, len(rawCandidates))
	for index, raw := range rawCandidates {
		candidate, err := parseProbeCandidate(raw)
		if err != nil {
			return ProbeFacts{}, fmt.Errorf("%w: candidate %d", err, index)
		}
		candidates = append(candidates, candidate)
	}
	return ProbeFacts{
		Version: version, FollowupCapabilityID: followupID, Candidates: candidates, MaxModelReturnBytes: maxReturn,
	}, nil
}

func parseProbeCandidate(raw any) (ProbeCandidate, error) {
	object, ok := raw.(map[string]any)
	if !ok || object == nil || !allowedProbeCandidateKeys(object) {
		return ProbeCandidate{}, fmt.Errorf("%w: malformed candidate", ErrInvalidProbeFacts)
	}
	label, ok := object["label"].(string)
	if !ok || label == "" || !utf8.ValidString(label) || len(label) > MaxProbeLabelBytes {
		return ProbeCandidate{}, fmt.Errorf("%w: label", ErrInvalidProbeFacts)
	}
	args, ok := object["args"].(map[string]any)
	if !ok || args == nil {
		return ProbeCandidate{}, fmt.Errorf("%w: args", ErrInvalidProbeFacts)
	}
	argsCopy, err := cloneProbeMap(args)
	if err != nil {
		return ProbeCandidate{}, fmt.Errorf("%w: args", ErrInvalidProbeFacts)
	}
	candidate := ProbeCandidate{Label: label, Args: argsCopy}
	if facts, present := object["facts"]; present {
		factsCopy, err := cloneProbeJSONValue(facts)
		if err != nil {
			return ProbeCandidate{}, fmt.Errorf("%w: facts", ErrInvalidProbeFacts)
		}
		candidate.Facts, candidate.HasFacts = factsCopy, true
	}
	if _, err := canonicalProbeJSON(candidateJSONValue(candidate), probeJSONLimits{maxBytes: MaxProbeCandidateBytes, maxDepth: MaxProbeJSONDepth, maxNodes: MaxProbeJSONNodes}); err != nil {
		return ProbeCandidate{}, fmt.Errorf("%w: candidate bounds", ErrInvalidProbeFacts)
	}
	return candidate, nil
}

func validateProbeManifest(manifest core.CapabilityManifest) error {
	if err := core.ValidateNamespacedID(manifest.ID); err != nil || manifest.Metadata == nil || manifest.Metadata[ProbeManifestKey] != ProbeManifestVersion {
		return fmt.Errorf("%w: probe marker", ErrInvalidProbePlan)
	}
	if _, exposed := manifest.Metadata[ExposureKey]; exposed {
		return fmt.Errorf("%w: probe must be excluded from the program catalog", ErrInvalidProbePlan)
	}
	if manifest.Kind != core.KindTool || manifest.Tool == nil || !manifest.Idempotent || manifest.RequiresApproval || manifest.MaxOutputBytes <= 0 || manifest.MaxOutputBytes > MaxProbeOutputBytes {
		return fmt.Errorf("%w: probe execution contract", ErrInvalidProbePlan)
	}
	return validateReadOnlyManifest(manifest, "probe")
}

func validateFollowupManifest(manifest core.CapabilityManifest, probeID string) error {
	if err := core.ValidateNamespacedID(manifest.ID); err != nil || manifest.ID == probeID || manifest.Metadata == nil || manifest.Metadata[ExposureKey] != ExposureVersion {
		return fmt.Errorf("%w: follow-up exposure", ErrInvalidProbePlan)
	}
	if _, probe := manifest.Metadata[ProbeManifestKey]; probe || manifest.Contract == programmaticCapabilityContract {
		return fmt.Errorf("%w: follow-up cannot be a probe or generic program capability", ErrInvalidProbePlan)
	}
	if manifest.Kind != core.KindTool || manifest.Tool == nil || !manifest.Idempotent || manifest.RequiresApproval {
		return fmt.Errorf("%w: follow-up execution contract", ErrInvalidProbePlan)
	}
	return validateReadOnlyManifest(manifest, "follow-up")
}

func validateReadOnlyManifest(manifest core.CapabilityManifest, role string) error {
	for _, permission := range manifest.RequiredPermissions {
		if writeOrSendPermission(permission) {
			return fmt.Errorf("%w: %s requests write/send permission", ErrInvalidProbePlan, role)
		}
	}
	if manifest.Execution == nil {
		return nil
	}
	if manifest.Execution.Writes || manifest.Execution.Sandbox.Mode != core.SandboxReadOnly {
		return fmt.Errorf("%w: %s declares write-capable execution", ErrInvalidProbePlan, role)
	}
	return nil
}

func writeOrSendPermission(permission core.Permission) bool {
	value := strings.ToLower(string(permission))
	return value == string(core.PermWrite) || value == string(core.PermSend) ||
		strings.HasSuffix(value, ".write") || strings.HasSuffix(value, ".send")
}

func validateCandidateArgs(args, inputSchema, toolSchema map[string]any) error {
	if args == nil {
		return errors.New("arguments are missing")
	}
	if _, err := marshalCandidateArgs(args); err != nil {
		return errors.New("arguments are not bounded JSON")
	}
	if err := core.ValidateArgs(inputSchema, args); err != nil {
		return err
	}
	if err := core.ValidateArgs(toolSchema, args); err != nil {
		return err
	}
	return nil
}

// buildProbeDirectSchema retains the frozen follow-up description and adds
// deterministic route guidance. It deliberately exposes only the candidate
// count: candidate labels, args, facts, probe content, and host bindings stay
// outside the model-visible description.
func buildProbeDirectSchema(schema core.ToolSchema, candidateCount int, executeEligible bool) (core.ToolSchema, error) {
	if candidateCount < 1 || candidateCount > MaxProbeCandidates || strings.TrimSpace(schema.Description) == "" {
		return core.ToolSchema{}, ErrInvalidProbePlan
	}
	guidance := "Routing guidance: This response is the unique Direct batch for this route; choosing Direct locks the route. Call every task-required candidate in this same assistant response because no later tool round is available. There are " + strconv.Itoa(candidateCount) + " host-approved candidates. Use Direct only for a small, deliberate subset. "
	if executeEligible {
		guidance += "When many or all candidates are needed, follow-up work would repeat, or outputs create context pressure, choose the eligible bounded execute tool instead."
	} else {
		guidance += "No eligible execute tool is available, so every task-required candidate call must be in this same Direct response."
	}
	schema.Description += " " + guidance
	return cloneProbeToolSchema(schema)
}

func buildProbeExecuteSchema(executeID string, followup core.CapabilityManifest, binding string, candidateCount, maxSelections, maxReturn int, projections []projectedOutput) (core.ToolSchema, error) {
	if candidateCount < 1 || maxSelections < 1 || maxSelections > candidateCount || !validDigest(binding) {
		return core.ToolSchema{}, ErrInvalidProbePlan
	}
	choices := make([]any, 0, len(projections))
	for _, projection := range projections {
		choices = append(choices, projection.Name)
	}
	if len(choices) == 0 {
		return core.ToolSchema{}, ErrInvalidProbePlan
	}
	parameters := map[string]any{
		"type":                 "object",
		"required":             []any{"selection", "projection"},
		"additionalProperties": false,
		"properties": map[string]any{
			"selection": map[string]any{
				"type": "array", "minItems": 1, "maxItems": maxSelections,
				"items":       map[string]any{"type": "integer", "minimum": 0, "maximum": candidateCount - 1},
				"description": "Unique indices into the host-owned probe candidate list.",
			},
			"projection": map[string]any{"type": "string", "enum": choices, "description": "One compact, host-approved top-level output field returned for every selected candidate."},
		},
	}
	description := "Routing guidance: When many or all candidates are needed, follow-up work would repeat, or outputs create context pressure, prefer a single bounded execute call. For a small, deliberate subset of candidates, call the follow-up tool directly. This is guidance: you retain the route choice and must satisfy task coverage. " +
		"The host executes a fixed bounded program over host-selected neutral-probe candidates. " +
		"The only follow-up binding is " + followup.ID + " (" + binding + "). " +
		"There are " + strconv.Itoa(candidateCount) + " candidates and the returned model-visible result is limited to " + strconv.Itoa(maxReturn) + " bytes."
	schema, err := cloneProbeToolSchema(core.ToolSchema{Name: executeID, Description: description, Parameters: parameters})
	if err != nil {
		return core.ToolSchema{}, fmt.Errorf("%w: dynamic execute schema", ErrInvalidProbePlan)
	}
	return schema, nil
}

func buildProjectedOutputs(schema map[string]any, maxReturn, maxSelections int, followupID string, contract ProbeProgramContract) ([]projectedOutput, error) {
	if len(schema) == 0 || schema["type"] != "object" || maxReturn <= projectedEnvelopeBytes || maxSelections < 1 {
		return nil, nil
	}
	properties, ok := schema["properties"].(map[string]any)
	if !ok || len(properties) == 0 {
		return nil, nil
	}
	required := map[string]bool{}
	if raw, ok := schema["required"].([]any); ok {
		for _, item := range raw {
			if name, ok := item.(string); ok {
				required[name] = true
			}
		}
	}
	names := make([]string, 0, len(properties))
	for name := range properties {
		names = append(names, name)
	}
	sort.Strings(names)
	projected := make([]projectedOutput, 0, minProbeInt(len(names), MaxProbeProjectionFields))
	for _, name := range names {
		if len(projected) == MaxProbeProjectionFields || !required[name] || name == "" || !utf8.ValidString(name) || len(name) > MaxProbeLabelBytes {
			continue
		}
		fieldSchema, ok := properties[name].(map[string]any)
		if !ok {
			continue
		}
		maximum, ok := projectedScalarBytes(fieldSchema)
		if !ok || maximum > (maxReturn-projectedEnvelopeBytes)/maxSelections {
			continue
		}
		cloned, err := cloneSchema(fieldSchema)
		if err != nil {
			return nil, ErrInvalidProbePlan
		}
		source, err := canonicalProjectedSource(contract.Version, followupID, name)
		if err != nil || len(source) > contract.MaxSourceBytes {
			return nil, ErrInvalidProbePlan
		}
		tools, err := contract.ValidateSource(source)
		if err != nil || !singleProgramTool(tools, followupID) {
			return nil, ErrInvalidProbePlan
		}
		digest, err := probeDigestFor(source, contract.MaxSourceBytes)
		if err != nil {
			return nil, ErrInvalidProbePlan
		}
		projected = append(projected, projectedOutput{Name: name, Schema: cloned, MaxEncodedBytes: maximum, Source: source, SourceDigest: digest})
	}
	return projected, nil
}

func projectedScalarBytes(schema map[string]any) (int, bool) {
	switch schema["type"] {
	case "boolean":
		return len("false"), true
	case "string":
		maximum, ok := positiveSchemaInt(schema["maxLength"])
		if !ok || maximum > MaxProbeModelReturnBytes/6 {
			return 0, false
		}
		return maximum*6 + 2, true
	default:
		return 0, false
	}
}

func positiveSchemaInt(raw any) (int, bool) {
	switch value := raw.(type) {
	case int:
		return value, value > 0
	case float64:
		if value > 0 && value <= float64(math.MaxInt) && value == math.Trunc(value) {
			return int(value), true
		}
	}
	return 0, false
}

func canonicalProjectedSource(version, followupID, projection string) (string, error) {
	value := map[string]any{"version": version, "body": []any{
		map[string]any{"op": "assign", "name": "values", "value": map[string]any{"op": "list", "items": []any{}}},
		map[string]any{"op": "for", "var": "target", "in": map[string]any{"op": "get", "object": map[string]any{"op": "var", "name": "input"}, "key": "targets"}, "body": []any{
			map[string]any{"op": "call", "assign": "detail", "tool": followupID, "args": map[string]any{"op": "get", "object": map[string]any{"op": "var", "name": "target"}, "key": "args"}},
			map[string]any{"op": "append", "target": "values", "value": map[string]any{"op": "get", "object": map[string]any{"op": "get", "object": map[string]any{"op": "var", "name": "detail"}, "key": "data"}, "key": projection}},
		}},
		map[string]any{"op": "return", "value": map[string]any{"op": "var", "name": "values"}},
	}}
	encoded, err := json.Marshal(value)
	return string(encoded), err
}

func minProbeInt(left, right int) int {
	if left < right {
		return left
	}
	return right
}

func (p ProbePlan) selection(raw any) ([]int, error) {
	items, ok := raw.([]any)
	if !ok || len(items) == 0 || len(items) > p.maxSelections {
		return nil, fmt.Errorf("%w: selection", ErrInvalidProbePlan)
	}
	seen := make(map[int]struct{}, len(items))
	indices := make([]int, 0, len(items))
	for _, rawIndex := range items {
		index, ok := positiveOrZeroProbeInt(rawIndex, len(p.facts.Candidates)-1)
		if !ok {
			return nil, fmt.Errorf("%w: selection index", ErrInvalidProbePlan)
		}
		if _, duplicate := seen[index]; duplicate {
			return nil, fmt.Errorf("%w: duplicate selection", ErrInvalidProbePlan)
		}
		seen[index] = struct{}{}
		indices = append(indices, index)
	}
	return indices, nil
}

func singleProgramTool(tools []string, followupID string) bool {
	return len(tools) == 1 && tools[0] == followupID
}

func (p ProbePlan) projection(name string) (projectedOutput, bool) {
	for _, projection := range p.projections {
		if projection.Name == name {
			return projection, true
		}
	}
	return projectedOutput{}, false
}

func (p ProbePlan) valid() bool {
	return p.probeID != "" && p.followupID != "" && p.executeID != "" && p.maxSelections > 0 &&
		len(p.facts.Candidates) > 0 && p.facts.MaxModelReturnBytes > 0 && validDigest(p.digest) &&
		validDigest(p.probeBindingDigest) && validDigest(p.followupBinding) && validateProbeProgramContract(p.programContract) == nil
}

func validateProbeProgramContract(contract ProbeProgramContract) error {
	if contract.ValidateSource == nil || contract.Version == "" || contract.LanguageGuide == "" ||
		!utf8.ValidString(contract.Version) || !utf8.ValidString(contract.LanguageGuide) ||
		len(contract.Version) > MaxProbeLabelBytes || len(contract.LanguageGuide) > MaxProbeSchemaBytes ||
		contract.MaxSourceBytes < 1 || contract.MaxSourceBytes > MaxProbeOutputBytes || contract.MaxToolCalls < 1 || contract.MaxToolCalls > MaxProbeSelections {
		return fmt.Errorf("%w: program contract", ErrInvalidProbePlan)
	}
	return nil
}

func planDigest(plan ProbePlan) (string, error) {
	directSchemaDigest, err := probeDigestFor(plan.directSchema, MaxProbeSchemaBytes)
	if err != nil {
		return "", err
	}
	value := struct {
		Algorithm           string     `json:"algorithm"`
		ProbeID             string     `json:"probe_id"`
		ProbeBindingDigest  string     `json:"probe_binding_digest"`
		FollowupID          string     `json:"followup_id"`
		FollowupBinding     string     `json:"followup_binding_digest"`
		ExecuteID           string     `json:"execute_id"`
		Facts               ProbeFacts `json:"facts"`
		MaxSelections       int        `json:"max_selections"`
		PTCVersion          string     `json:"ptc_version"`
		DirectSchemaDigest  string     `json:"direct_schema_digest"`
		ExecuteSchemaDigest string     `json:"execute_schema_digest"`
		Projections         []struct {
			Version, Name, SourceDigest string
			MaxEncodedBytes             int
		} `json:"projections"`
	}{
		Algorithm: probePlanAlgorithmVersion, ProbeID: plan.probeID, ProbeBindingDigest: plan.probeBindingDigest,
		FollowupID: plan.followupID, FollowupBinding: plan.followupBinding, ExecuteID: plan.executeID,
		Facts: plan.facts, MaxSelections: plan.maxSelections, PTCVersion: plan.programContract.Version,
		DirectSchemaDigest: directSchemaDigest, ExecuteSchemaDigest: plan.executeSchemaDigest,
	}
	value.Projections = make([]struct {
		Version, Name, SourceDigest string
		MaxEncodedBytes             int
	}, len(plan.projections))
	for index, projection := range plan.projections {
		value.Projections[index] = struct {
			Version, Name, SourceDigest string
			MaxEncodedBytes             int
		}{Version: probeProjectionVersion, Name: projection.Name, SourceDigest: projection.SourceDigest, MaxEncodedBytes: projection.MaxEncodedBytes}
	}
	return probeDigestFor(value, MaxProbeFactsBytes+MaxProbeSchemaBytes)
}

func probeDigestFor(value any, maximum int) (string, error) {
	encoded, err := safeProbeMarshal(value)
	if err != nil || len(encoded) == 0 || len(encoded) > maximum {
		return "", ErrInvalidProbePlan
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

func cloneProbeToolSchema(tool core.ToolSchema) (core.ToolSchema, error) {
	if core.ValidateNamespacedID(tool.Name) != nil || strings.TrimSpace(tool.Description) == "" {
		return core.ToolSchema{}, ErrInvalidProbePlan
	}
	parameters, err := cloneSchema(tool.Parameters)
	if err != nil {
		return core.ToolSchema{}, err
	}
	tool.Parameters = parameters
	encoded, err := safeProbeMarshal(tool)
	if err != nil || len(encoded) == 0 || len(encoded) > MaxProbeSchemaBytes || projectedJSONDepth(encoded) > MaxProbeSchemaDepth {
		return core.ToolSchema{}, ErrInvalidProbePlan
	}
	return tool, nil
}

func cloneProbeToolSchemaOrZero(tool core.ToolSchema) core.ToolSchema {
	copy, err := cloneProbeToolSchema(tool)
	if err != nil {
		return core.ToolSchema{}
	}
	return copy
}

func cloneProbeFacts(facts ProbeFacts) (ProbeFacts, error) {
	out := ProbeFacts{
		Version: facts.Version, FollowupCapabilityID: facts.FollowupCapabilityID, MaxModelReturnBytes: facts.MaxModelReturnBytes,
		Candidates: make([]ProbeCandidate, len(facts.Candidates)),
	}
	for index, candidate := range facts.Candidates {
		args, err := cloneProbeMap(candidate.Args)
		if err != nil {
			return ProbeFacts{}, err
		}
		values, err := cloneProbeJSONValue(candidate.Facts)
		if err != nil {
			return ProbeFacts{}, err
		}
		out.Candidates[index] = ProbeCandidate{Label: candidate.Label, Args: args, Facts: values, HasFacts: candidate.HasFacts}
	}
	return out, nil
}

func candidateJSONValue(candidate ProbeCandidate) map[string]any {
	value := map[string]any{"label": candidate.Label, "args": candidate.Args}
	if candidate.HasFacts {
		value["facts"] = candidate.Facts
	}
	return value
}

func marshalCandidateArgs(args map[string]any) ([]byte, error) {
	return canonicalProbeJSON(args, probeJSONLimits{maxBytes: MaxProbeCandidateBytes, maxDepth: MaxProbeJSONDepth, maxNodes: MaxProbeJSONNodes})
}

func cloneProbeMap(value map[string]any) (map[string]any, error) {
	cloned, err := cloneProbeJSON(value, probeJSONLimits{maxBytes: MaxProbeCandidateBytes, maxDepth: MaxProbeJSONDepth, maxNodes: MaxProbeJSONNodes})
	if err != nil {
		return nil, err
	}
	object, ok := cloned.(map[string]any)
	if !ok || object == nil {
		return nil, ErrInvalidProbePlan
	}
	return object, nil
}

type probeJSONLimits struct {
	maxBytes int
	maxDepth int
	maxNodes int
}

type probeJSONState struct {
	limits probeJSONLimits
	nodes  int
	active map[probeJSONVisit]struct{}
}

type probeJSONVisit struct {
	kind byte
	ptr  uintptr
}

func canonicalProbeJSON(value any, limits probeJSONLimits) ([]byte, error) {
	cloned, err := cloneProbeJSON(value, limits)
	if err != nil {
		return nil, err
	}
	return marshalProbeJSON(cloned, limits.maxBytes, limits.maxDepth)
}

func cloneProbeJSON(value any, limits probeJSONLimits) (cloned any, err error) {
	defer func() {
		if recover() != nil {
			cloned = nil
			err = ErrInvalidProbeFacts
		}
	}()
	if limits.maxBytes < 1 || limits.maxDepth < 1 || limits.maxNodes < 1 {
		return nil, ErrInvalidProbeFacts
	}
	state := probeJSONState{limits: limits, active: make(map[probeJSONVisit]struct{})}
	return state.clone(value, 1)
}

func cloneProbeJSONValue(value any) (any, error) {
	return cloneProbeJSON(value, probeJSONLimits{maxBytes: MaxProbeCandidateBytes, maxDepth: MaxProbeJSONDepth, maxNodes: MaxProbeJSONNodes})
}

func (s *probeJSONState) clone(value any, depth int) (any, error) {
	if depth > s.limits.maxDepth {
		return nil, ErrInvalidProbeFacts
	}
	s.nodes++
	if s.nodes > s.limits.maxNodes {
		return nil, ErrInvalidProbeFacts
	}
	switch typed := value.(type) {
	case nil:
		return nil, nil
	case bool:
		return typed, nil
	case string:
		if !utf8.ValidString(typed) || len(typed) > s.limits.maxBytes {
			return nil, ErrInvalidProbeFacts
		}
		return typed, nil
	case json.Number:
		if !validProbeJSONNumber(typed) {
			return nil, ErrInvalidProbeFacts
		}
		return typed, nil
	case float64:
		if math.IsNaN(typed) || math.IsInf(typed, 0) {
			return nil, ErrInvalidProbeFacts
		}
		return typed, nil
	case float32:
		if math.IsNaN(float64(typed)) || math.IsInf(float64(typed), 0) {
			return nil, ErrInvalidProbeFacts
		}
		return typed, nil
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32:
		return typed, nil
	case uint64:
		if typed > math.MaxInt64 {
			return nil, ErrInvalidProbeFacts
		}
		return typed, nil
	case []any:
		if typed == nil {
			return nil, nil
		}
		visit := probeJSONVisit{kind: 's', ptr: reflect.ValueOf(typed).Pointer()}
		if _, exists := s.active[visit]; exists {
			return nil, ErrInvalidProbeFacts
		}
		s.active[visit] = struct{}{}
		out := make([]any, len(typed))
		for index, item := range typed {
			copy, err := s.clone(item, depth+1)
			if err != nil {
				delete(s.active, visit)
				return nil, err
			}
			out[index] = copy
		}
		delete(s.active, visit)
		return out, nil
	case map[string]any:
		if typed == nil {
			return nil, nil
		}
		visit := probeJSONVisit{kind: 'm', ptr: reflect.ValueOf(typed).Pointer()}
		if _, exists := s.active[visit]; exists {
			return nil, ErrInvalidProbeFacts
		}
		s.active[visit] = struct{}{}
		out := make(map[string]any, len(typed))
		for key, item := range typed {
			if !utf8.ValidString(key) || len(key) > s.limits.maxBytes {
				delete(s.active, visit)
				return nil, ErrInvalidProbeFacts
			}
			copy, err := s.clone(item, depth+1)
			if err != nil {
				delete(s.active, visit)
				return nil, err
			}
			out[key] = copy
		}
		delete(s.active, visit)
		return out, nil
	default:
		return nil, ErrInvalidProbeFacts
	}
}

func validProbeJSONNumber(value json.Number) bool {
	text := value.String()
	if text == "" || !json.Valid([]byte(text)) {
		return false
	}
	number, err := strconv.ParseFloat(text, 64)
	return err == nil && !math.IsNaN(number) && !math.IsInf(number, 0)
}

func marshalProbeJSON(value any, maximum, maxDepth int) ([]byte, error) {
	encoded, err := safeProbeMarshal(value)
	if err != nil || len(encoded) == 0 || len(encoded) > maximum || projectedJSONDepth(encoded) > maxDepth {
		return nil, ErrInvalidProbeFacts
	}
	return encoded, nil
}

func safeProbeMarshal(value any) (encoded []byte, err error) {
	defer func() {
		if recover() != nil {
			encoded = nil
			err = ErrInvalidProbeFacts
		}
	}()
	return json.Marshal(value)
}

func decodeProbeJSON(encoded []byte, maxDepth, maxNodes int) (any, error) {
	if len(encoded) == 0 || projectedJSONDepth(encoded) > maxDepth {
		return nil, ErrInvalidProbeFacts
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, ErrInvalidProbeFacts
	}
	var extra any
	if err := decoder.Decode(&extra); err == nil {
		return nil, ErrInvalidProbeFacts
	}
	if _, err := cloneProbeJSON(value, probeJSONLimits{maxBytes: len(encoded), maxDepth: maxDepth, maxNodes: maxNodes}); err != nil {
		return nil, ErrInvalidProbeFacts
	}
	return value, nil
}

func positiveProbeInt(value any, maximum int) (int, bool) {
	index, ok := positiveOrZeroProbeInt(value, maximum)
	return index, ok && index > 0
}

func positiveOrZeroProbeInt(value any, maximum int) (int, bool) {
	var parsed int64
	switch number := value.(type) {
	case json.Number:
		var err error
		parsed, err = strconv.ParseInt(number.String(), 10, 64)
		if err != nil {
			return 0, false
		}
	case int:
		parsed = int64(number)
	case int8:
		parsed = int64(number)
	case int16:
		parsed = int64(number)
	case int32:
		parsed = int64(number)
	case int64:
		parsed = number
	case uint:
		if uint64(number) > math.MaxInt64 {
			return 0, false
		}
		parsed = int64(number)
	case uint8:
		parsed = int64(number)
	case uint16:
		parsed = int64(number)
	case uint32:
		parsed = int64(number)
	case uint64:
		if number > math.MaxInt64 {
			return 0, false
		}
		parsed = int64(number)
	case float64:
		if math.IsNaN(number) || math.IsInf(number, 0) || math.Trunc(number) != number || number > math.MaxInt64 || number < math.MinInt64 {
			return 0, false
		}
		parsed = int64(number)
	default:
		return 0, false
	}
	if parsed < 0 || parsed > int64(maximum) {
		return 0, false
	}
	return int(parsed), true
}

func exactProbeKeys(object map[string]any, names ...string) bool {
	if len(object) != len(names) {
		return false
	}
	for _, name := range names {
		if _, present := object[name]; !present {
			return false
		}
	}
	return true
}

func allowedProbeCandidateKeys(object map[string]any) bool {
	if len(object) < 2 || len(object) > 3 {
		return false
	}
	for key := range object {
		if key != "label" && key != "args" && key != "facts" {
			return false
		}
	}
	_, label := object["label"]
	_, args := object["args"]
	return label && args
}
