// Package graph contains the optional, bounded graph orchestration contracts.
//
// Context is deliberately a contract-only package at this layer. It does not
// materialize prompts, tokenize text, call a model, or read a store. A caller
// must first validate the node-local view, budget, sources, and plan before it
// hands them to a materializer.
package graph

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"
)

// LayerKind is the only vocabulary a ContextView may use for context layers.
type LayerKind string

const (
	LayerCurrentInput    LayerKind = "current_input"
	LayerRequiredState   LayerKind = "required_state"
	LayerPendingApproval LayerKind = "pending_approval"
	LayerRecentHistory   LayerKind = "recent_history"
	LayerSummary         LayerKind = "summary"
	LayerArtifact        LayerKind = "artifact"
	LayerRetrieval       LayerKind = "retrieval"
)

// The caps are intentionally small: this contract must remain cheap to
// validate even when it is fed untrusted plugin input.
const (
	MaxContextTotalTokens   int64 = 1_000_000
	MaxContextViewLayers          = 7
	MaxContextStateFields         = 64
	MaxContextSources             = 256
	MaxContextSummaryRefs         = 256
	MaxContextIDBytes             = 256
	MaxContextRevisionBytes       = 256
	MaxContextReasonBytes         = 1024
	MaxContextViewBytes           = 16 << 10
	MaxContextPlanBytes           = 128 << 10
	MaxContextSourceTokens  int64 = 1_000_000
)

var (
	ErrInvalidContextView   = errors.New("invalid context view")
	ErrInvalidContextBudget = errors.New("invalid context budget")
	ErrInvalidContextPlan   = errors.New("invalid context plan")
)

// ContextView is an explicit node-local projection. An empty StateFields is
// meaningful: it requests no state. There is no all-state or global wildcard.
type ContextView struct {
	StateFields []string    `json:"state_fields,omitempty"`
	Layers      []LayerKind `json:"layers,omitempty"`
}

// Clone returns an independent copy of the view.
func (v ContextView) Clone() ContextView {
	return ContextView{
		StateFields: append([]string(nil), v.StateFields...),
		Layers:      append([]LayerKind(nil), v.Layers...),
	}
}

// ContextBudget bounds input layers and reserves output capacity.
type ContextBudget struct {
	TotalTokens        int64               `json:"total_tokens"`
	OutputReserve      int64               `json:"output_reserve"`
	StablePrefixTokens int64               `json:"stable_prefix_tokens"`
	LayerBudgets       map[LayerKind]int64 `json:"layer_budgets"`
}

// Clone returns an independent copy of the budget.
func (b ContextBudget) Clone() ContextBudget {
	clone := b
	if b.LayerBudgets != nil {
		clone.LayerBudgets = make(map[LayerKind]int64, len(b.LayerBudgets))
		for layer, tokens := range b.LayerBudgets {
			clone.LayerBudgets[layer] = tokens
		}
	}
	return clone
}

// ContextSourceRef identifies the exact source revision summarized by a
// summary source. A source revision, rather than only a source ID, prevents
// summaries from silently drifting across execution segments.
type ContextSourceRef struct {
	Layer    LayerKind `json:"layer"`
	SourceID string    `json:"source_id"`
	Revision string    `json:"revision"`
}

// ContextSource describes one isolated, revisioned input to a plan.
type ContextSource struct {
	Layer           LayerKind          `json:"layer"`
	SourceID        string             `json:"source_id"`
	Revision        string             `json:"revision"`
	EstimatedTokens int64              `json:"estimated_tokens"`
	Required        bool               `json:"required"`
	SummaryOf       []ContextSourceRef `json:"summary_of,omitempty"`
}

// Clone returns an independent copy of the source and its lineage.
func (s ContextSource) Clone() ContextSource {
	s.SummaryOf = append([]ContextSourceRef(nil), s.SummaryOf...)
	return s
}

// ContextPlan is immutable evidence for one node-local context allocation.
// Sources are typed and revisioned so a materializer cannot substitute a
// similarly named artifact, retrieval hit, or stale summary.
type ContextPlan struct {
	DefinitionRevision string          `json:"definition_revision"`
	PlanRevision       string          `json:"plan_revision"`
	View               ContextView     `json:"view"`
	Budget             ContextBudget   `json:"budget"`
	Sources            []ContextSource `json:"sources"`
	CompactionReason   string          `json:"compaction_reason,omitempty"`
}

// Clone returns a fully independent copy of the plan.
func (p ContextPlan) Clone() ContextPlan {
	clone := p
	clone.View = p.View.Clone()
	clone.Budget = p.Budget.Clone()
	if p.Sources != nil {
		clone.Sources = make([]ContextSource, len(p.Sources))
		for i, source := range p.Sources {
			clone.Sources[i] = source.Clone()
		}
	}
	return clone
}

// ValidateContextView validates explicit selection and rejects wildcard or
// global state requests.
func ValidateContextView(view ContextView) error {
	if len(view.StateFields) > MaxContextStateFields {
		return contextError(ErrInvalidContextView, "too many state fields")
	}
	if len(view.Layers) > MaxContextViewLayers {
		return contextError(ErrInvalidContextView, "too many layers")
	}
	seenFields := make(map[string]struct{}, len(view.StateFields))
	for _, field := range view.StateFields {
		if err := validateContextName(field, MaxContextIDBytes, "state field"); err != nil {
			return contextError(ErrInvalidContextView, "%v", err)
		}
		if strings.ContainsAny(field, "*?") || isWildcardName(field) {
			return contextError(ErrInvalidContextView, "state field %q is not explicit", field)
		}
		if _, ok := seenFields[field]; ok {
			return contextError(ErrInvalidContextView, "duplicate state field %q", field)
		}
		seenFields[field] = struct{}{}
	}
	seenLayers := make(map[LayerKind]struct{}, len(view.Layers))
	for _, layer := range view.Layers {
		if !validLayer(layer) || isWildcardName(string(layer)) {
			return contextError(ErrInvalidContextView, "unsupported or wildcard layer %q", layer)
		}
		if _, ok := seenLayers[layer]; ok {
			return contextError(ErrInvalidContextView, "duplicate layer %q", layer)
		}
		seenLayers[layer] = struct{}{}
	}
	if len(view.StateFields) > 0 {
		if _, ok := seenLayers[LayerRequiredState]; !ok {
			return contextError(ErrInvalidContextView, "state fields require the required_state layer")
		}
	}
	encoded, err := json.Marshal(view)
	if err != nil {
		return contextError(ErrInvalidContextView, "marshal: %v", err)
	}
	if len(encoded) > MaxContextViewBytes {
		return contextError(ErrInvalidContextView, "encoded view exceeds %d bytes", MaxContextViewBytes)
	}
	return nil
}

// ValidateContextBudget validates bounded, overflow-safe token arithmetic.
func ValidateContextBudget(budget ContextBudget) error {
	if budget.TotalTokens < 0 || budget.TotalTokens > MaxContextTotalTokens {
		return contextError(ErrInvalidContextBudget, "total tokens out of range")
	}
	if budget.OutputReserve < 0 || budget.StablePrefixTokens < 0 {
		return contextError(ErrInvalidContextBudget, "negative token budget")
	}
	if budget.OutputReserve > budget.TotalTokens {
		return contextError(ErrInvalidContextBudget, "output reserve exceeds total tokens")
	}
	inputBudget := budget.TotalTokens - budget.OutputReserve
	if budget.StablePrefixTokens > inputBudget {
		return contextError(ErrInvalidContextBudget, "stable prefix exceeds input budget")
	}
	if len(budget.LayerBudgets) > MaxContextViewLayers {
		return contextError(ErrInvalidContextBudget, "too many layer budgets")
	}
	var layerTotal int64
	for _, layer := range sortedBudgetLayers(budget.LayerBudgets) {
		tokens := budget.LayerBudgets[layer]
		if !validLayer(layer) {
			return contextError(ErrInvalidContextBudget, "unsupported layer budget %q", layer)
		}
		if tokens < 0 || tokens > MaxContextSourceTokens {
			return contextError(ErrInvalidContextBudget, "layer %q token budget out of range", layer)
		}
		if tokens > math.MaxInt64-layerTotal {
			return contextError(ErrInvalidContextBudget, "layer token budget overflows")
		}
		layerTotal += tokens
	}
	if layerTotal > budget.TotalTokens-budget.OutputReserve {
		return contextError(ErrInvalidContextBudget, "layer budgets plus output reserve exceed total tokens")
	}
	return nil
}

// ValidateContextPlan validates the view/budget contract, source isolation,
// exact summary lineage, and bounded source estimates.
func ValidateContextPlan(plan ContextPlan) error {
	// Check cheap attacker-controlled bounds before asking encoding/json to
	// allocate a representation of the whole plan.
	if len(plan.Sources) > MaxContextSources {
		return contextError(ErrInvalidContextPlan, "too many sources")
	}
	if len(plan.CompactionReason) > MaxContextReasonBytes {
		return contextError(ErrInvalidContextPlan, "compaction reason exceeds %d bytes", MaxContextReasonBytes)
	}
	if err := validateContextName(plan.DefinitionRevision, MaxContextRevisionBytes, "definition revision"); err != nil {
		return contextError(ErrInvalidContextPlan, "%v", err)
	}
	if err := validateContextName(plan.PlanRevision, MaxContextRevisionBytes, "plan revision"); err != nil {
		return contextError(ErrInvalidContextPlan, "%v", err)
	}
	if err := ValidateContextView(plan.View); err != nil {
		return contextError(ErrInvalidContextPlan, "%v", err)
	}
	if err := ValidateContextBudget(plan.Budget); err != nil {
		return contextError(ErrInvalidContextPlan, "%v", err)
	}
	if err := validateContextText(plan.CompactionReason, MaxContextReasonBytes, "compaction reason", true); err != nil {
		return contextError(ErrInvalidContextPlan, "%v", err)
	}
	viewLayers := make(map[LayerKind]struct{}, len(plan.View.Layers))
	for _, layer := range plan.View.Layers {
		viewLayers[layer] = struct{}{}
		if _, ok := plan.Budget.LayerBudgets[layer]; !ok {
			return contextError(ErrInvalidContextPlan, "view layer %q has no allocation", layer)
		}
	}
	for layer, tokens := range plan.Budget.LayerBudgets {
		if tokens != 0 {
			if _, ok := viewLayers[layer]; !ok {
				return contextError(ErrInvalidContextPlan, "budget allocates unselected layer %q", layer)
			}
		}
	}

	type sourceKey struct{ id, revision string }
	seen := make(map[sourceKey]struct{}, len(plan.Sources))
	seenIDs := make(map[string]struct{}, len(plan.Sources))
	layerTotals := make(map[LayerKind]int64, len(viewLayers))
	requiredByLayer := make(map[LayerKind]bool, len(viewLayers))
	hasSummary := false
	roughBytes := int64(len(plan.DefinitionRevision) + len(plan.PlanRevision) + len(plan.CompactionReason))
	if err := addContextBytes(&roughBytes, int64(len(plan.Sources))*64); err != nil {
		return contextError(ErrInvalidContextPlan, "%v", err)
	}
	for index, source := range plan.Sources {
		if len(source.SummaryOf) > MaxContextSummaryRefs {
			return contextError(ErrInvalidContextPlan, "source %d has too many lineage references", index)
		}
		if _, ok := viewLayers[source.Layer]; !ok {
			return contextError(ErrInvalidContextPlan, "source %d uses unselected layer %q", index, source.Layer)
		}
		if !validLayer(source.Layer) {
			return contextError(ErrInvalidContextPlan, "source %d has unsupported layer", index)
		}
		if err := validateContextName(source.SourceID, MaxContextIDBytes, "source ID"); err != nil {
			return contextError(ErrInvalidContextPlan, "source %d: %v", index, err)
		}
		if err := validateContextName(source.Revision, MaxContextRevisionBytes, "source revision"); err != nil {
			return contextError(ErrInvalidContextPlan, "source %d: %v", index, err)
		}
		if err := addContextBytes(&roughBytes, int64(len(source.SourceID))); err != nil {
			return contextError(ErrInvalidContextPlan, "%v", err)
		}
		if err := addContextBytes(&roughBytes, int64(len(source.Revision))); err != nil {
			return contextError(ErrInvalidContextPlan, "%v", err)
		}
		if err := addContextBytes(&roughBytes, int64(len(source.SummaryOf))*32); err != nil {
			return contextError(ErrInvalidContextPlan, "%v", err)
		}
		if roughBytes > MaxContextPlanBytes {
			return contextError(ErrInvalidContextPlan, "plan input exceeds %d bytes", MaxContextPlanBytes)
		}
		if source.EstimatedTokens < 0 || source.EstimatedTokens > MaxContextSourceTokens {
			return contextError(ErrInvalidContextPlan, "source %d estimate out of range", index)
		}
		key := sourceKey{source.SourceID, source.Revision}
		if _, ok := seen[key]; ok {
			return contextError(ErrInvalidContextPlan, "duplicate source identity and revision %q@%q", source.SourceID, source.Revision)
		}
		seen[key] = struct{}{}
		if _, ok := seenIDs[source.SourceID]; ok {
			return contextError(ErrInvalidContextPlan, "source identity %q is repeated", source.SourceID)
		}
		seenIDs[source.SourceID] = struct{}{}
		if isCanonicalLayer(source.Layer) && isNonCanonicalID(source.SourceID) {
			return contextError(ErrInvalidContextPlan, "non-canonical source %q cannot serve layer %q", source.SourceID, source.Layer)
		}
		if source.Required {
			requiredByLayer[source.Layer] = true
		}
		if source.Layer == LayerSummary {
			hasSummary = true
			if len(source.SummaryOf) == 0 {
				return contextError(ErrInvalidContextPlan, "summary source %q must carry bounded non-summary lineage", source.SourceID)
			}
			refs := make(map[sourceKey]struct{}, len(source.SummaryOf))
			for refIndex, ref := range source.SummaryOf {
				if !validLayer(ref.Layer) || ref.Layer == LayerSummary {
					return contextError(ErrInvalidContextPlan, "source %d lineage %d has invalid summary source layer %q", index, refIndex, ref.Layer)
				}
				if err := validateContextName(ref.SourceID, MaxContextIDBytes, "summary source ID"); err != nil {
					return contextError(ErrInvalidContextPlan, "source %d lineage %d: %v", index, refIndex, err)
				}
				if err := validateContextName(ref.Revision, MaxContextRevisionBytes, "summary source revision"); err != nil {
					return contextError(ErrInvalidContextPlan, "source %d lineage %d: %v", index, refIndex, err)
				}
				if err := addContextBytes(&roughBytes, int64(len(ref.SourceID))); err != nil {
					return contextError(ErrInvalidContextPlan, "%v", err)
				}
				if err := addContextBytes(&roughBytes, int64(len(ref.Revision))); err != nil {
					return contextError(ErrInvalidContextPlan, "%v", err)
				}
				if roughBytes > MaxContextPlanBytes {
					return contextError(ErrInvalidContextPlan, "plan input exceeds %d bytes", MaxContextPlanBytes)
				}
				refKey := sourceKey{ref.SourceID, ref.Revision}
				if ref.SourceID == source.SourceID && ref.Revision == source.Revision {
					return contextError(ErrInvalidContextPlan, "source %d references itself", index)
				}
				if _, ok := refs[refKey]; ok {
					return contextError(ErrInvalidContextPlan, "source %d has duplicate lineage", index)
				}
				refs[refKey] = struct{}{}
			}
		} else if len(source.SummaryOf) != 0 {
			return contextError(ErrInvalidContextPlan, "non-summary source %q has summary lineage", source.SourceID)
		}
		if source.EstimatedTokens > math.MaxInt64-layerTotals[source.Layer] {
			return contextError(ErrInvalidContextPlan, "source estimates overflow")
		}
		layerTotals[source.Layer] += source.EstimatedTokens
	}

	for _, layer := range plan.View.Layers {
		if (layer == LayerCurrentInput || (layer == LayerRequiredState && len(plan.View.StateFields) > 0)) && plan.Budget.LayerBudgets[layer] <= 0 {
			return contextError(ErrInvalidContextPlan, "layer %q requires a positive input budget", layer)
		}
		if layer == LayerCurrentInput || layer == LayerRequiredState || layer == LayerPendingApproval {
			if !requiredByLayer[layer] {
				return contextError(ErrInvalidContextPlan, "canonical layer %q lacks a required source", layer)
			}
		}
		if layerTotals[layer] > plan.Budget.LayerBudgets[layer] {
			return contextError(ErrInvalidContextPlan, "source estimates exceed %q allocation", layer)
		}
	}
	if hasSummary && strings.TrimSpace(plan.CompactionReason) == "" {
		return contextError(ErrInvalidContextPlan, "summary sources require a compaction reason")
	}
	var estimateTotal int64
	for _, total := range layerTotals {
		if total > math.MaxInt64-estimateTotal {
			return contextError(ErrInvalidContextPlan, "source estimates overflow")
		}
		estimateTotal += total
	}
	if estimateTotal > plan.Budget.TotalTokens-plan.Budget.OutputReserve {
		return contextError(ErrInvalidContextPlan, "source estimates plus output reserve exceed total")
	}
	encoded, err := json.Marshal(plan)
	if err != nil {
		return contextError(ErrInvalidContextPlan, "marshal: %v", err)
	}
	if len(encoded) > MaxContextPlanBytes {
		return contextError(ErrInvalidContextPlan, "encoded plan exceeds %d bytes", MaxContextPlanBytes)
	}
	return nil
}

func validLayer(layer LayerKind) bool {
	switch layer {
	case LayerCurrentInput, LayerRequiredState, LayerPendingApproval, LayerRecentHistory, LayerSummary, LayerArtifact, LayerRetrieval:
		return true
	default:
		return false
	}
}

func isCanonicalLayer(layer LayerKind) bool {
	return layer == LayerCurrentInput || layer == LayerRequiredState || layer == LayerPendingApproval
}

func isNonCanonicalID(id string) bool {
	id = strings.ToLower(strings.TrimSpace(id))
	return strings.HasPrefix(id, "artifact/") || strings.HasPrefix(id, "artifact:") ||
		strings.HasPrefix(id, "retrieval/") || strings.HasPrefix(id, "retrieval:")
}

func isWildcardName(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "*", "all", "global", "all_state", "global_state":
		return true
	default:
		return false
	}
}

func validateContextName(value string, maxBytes int, label string) error {
	if err := validateContextText(value, maxBytes, label, false); err != nil {
		return err
	}
	for _, r := range value {
		if unicode.IsSpace(r) {
			return fmt.Errorf("%s contains whitespace", label)
		}
	}
	return nil
}

func validateContextText(value string, maxBytes int, label string, allowEmpty bool) error {
	if !allowEmpty && value == "" {
		return fmt.Errorf("%s is required", label)
	}
	if len(value) > maxBytes {
		return fmt.Errorf("%s exceeds %d bytes", label, maxBytes)
	}
	if !utf8.ValidString(value) {
		return fmt.Errorf("%s is not valid UTF-8", label)
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return fmt.Errorf("%s contains a control character", label)
		}
	}
	return nil
}

func addContextBytes(total *int64, amount int64) error {
	if amount < 0 || *total > math.MaxInt64-amount {
		return fmt.Errorf("context byte estimate overflows")
	}
	*total += amount
	return nil
}

func sortedBudgetLayers(budgets map[LayerKind]int64) []LayerKind {
	layers := make([]LayerKind, 0, len(budgets))
	for layer := range budgets {
		layers = append(layers, layer)
	}
	sort.Slice(layers, func(i, j int) bool { return layers[i] < layers[j] })
	return layers
}

func contextError(kind error, format string, args ...any) error {
	return fmt.Errorf("%w: %s", kind, fmt.Sprintf(format, args...))
}
