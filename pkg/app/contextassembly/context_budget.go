// Package contextassembly provides an optional, provider-neutral context
// budgeting seam. It deliberately lives outside core so the stable message
// kernel does not grow with policy and provider concerns.
package contextassembly

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/cc-auto-agent/harness-core/pkg/core"
)

type Layer string

const (
	LayerSystem      Layer = "system"
	LayerProfile     Layer = "profile"
	LayerSummary     Layer = "summary"
	LayerRecent      Layer = "recent"
	LayerToolResults Layer = "tool_results"
	MaxItems               = 4096
	MaxBytes               = 16 << 20
	MaxTokens              = 16 << 20
	MaxLineageRefs         = 16
	MaxMetadataBytes       = 1024
)

var (
	ErrBudgetExceeded = errors.New("context budget exceeded")
	ErrInvalid        = errors.New("invalid context assembly")
)

type Cost struct{ Bytes, Tokens int64 }

type BudgetEstimator interface {
	Estimate(core.ChatMessage) (Cost, error)
	EstimateFragment(Layer, string) (Cost, error)
}
type TokenEstimator interface {
	EstimateTokens(core.ChatMessage) (int64, error)
	EstimateFragmentTokens(Layer, string) (int64, error)
}

// ByteEstimator counts the complete JSON wire representation, including its
// protocol/container overhead. It is conservative and does not truncate data.
type ByteEstimator struct{}

func (ByteEstimator) EstimateFragment(_ Layer, text string) (Cost, error) {
	return Cost{Bytes: int64(len(text)) + 32}, nil
}

func (ByteEstimator) Estimate(m core.ChatMessage) (Cost, error) {
	if err := validateMessage(m); err != nil {
		return Cost{}, err
	}
	a, err := jsonStringUpperBound(string(m.Role))
	if err != nil {
		return Cost{}, err
	}
	b, err := jsonStringUpperBound(m.Content)
	if err != nil {
		return Cost{}, err
	}
	n := int64(64) + a + b
	if m.ToolCall != nil {
		a, err := jsonValueBytes(m.ToolCall)
		if err != nil {
			return Cost{}, err
		}
		n += 64 + a
	}
	for _, tc := range m.ToolCalls {
		a, err := jsonValueBytes(tc)
		if err != nil {
			return Cost{}, err
		}
		n += 64 + a
	}
	a, err = jsonStringUpperBound(m.ToolCallID)
	if err != nil {
		return Cost{}, err
	}
	n += a
	return Cost{Bytes: n}, nil
}
func jsonStringUpperBound(s string) (int64, error) {
	if !utf8.ValidString(s) {
		return 0, ErrInvalid
	}
	n := int64(2)
	for len(s) > 0 {
		r, k := utf8.DecodeRuneInString(s)
		s = s[k:]
		switch r {
		case '"', '\\', '\b', '\f', '\n', '\r', '\t':
			n += 2
		case '<', '>', '&', '\u2028', '\u2029':
			n += 6
		default:
			if r < 0x20 {
				n += 6
			} else {
				n += int64(k)
			}
		}
	}
	return n, nil
}
func jsonValueBytes(v any) (int64, error) {
	if v == nil {
		return 0, nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return 0, err
	}
	return int64(len(b)), nil
}

type CompositeBudgetEstimator struct{ Tokenizer TokenEstimator }

func (e CompositeBudgetEstimator) EstimateFragment(l Layer, text string) (Cost, error) {
	c, err := ByteEstimator{}.EstimateFragment(l, text)
	if e.Tokenizer != nil {
		c.Tokens, err = e.Tokenizer.EstimateFragmentTokens(l, text)
	}
	return c, err
}

func (e CompositeBudgetEstimator) Estimate(m core.ChatMessage) (Cost, error) {
	c, err := (ByteEstimator{}).Estimate(m)
	if err != nil {
		return Cost{}, err
	}
	if e.Tokenizer != nil {
		c.Tokens, err = e.Tokenizer.EstimateTokens(m)
	}
	if err != nil || c.Bytes < 0 || c.Tokens < 0 {
		return Cost{}, ErrInvalid
	}
	return c, nil
}

type Budget struct {
	TotalBytes int64
	// TotalTokens==0 disables token gating. A layer token entry of zero is an
	// explicit deny; an absent entry means unlimited for that layer.
	TotalTokens int64
	LayerBytes  map[Layer]int64
	LayerTokens map[Layer]int64
}

type Item struct {
	Message  core.ChatMessage
	Layer    Layer
	SourceID string
	Revision string
	Lineage  []string
	// Items sharing a non-empty GroupID are selected atomically. This prevents
	// splitting assistant tool-call/tool-result protocol pairs.
	GroupID string
	// Priority is explicit selection order: higher values are considered first.
	// It is bounded and never changes output protocol order, which remains input
	// order after selection. Zero is a valid lowest priority.
	Priority int
	// Required makes this item (and its atomic group, when present) mandatory.
	// Assembly fails closed when the complete required group cannot fit.
	Required bool
}

type Request struct {
	Budget          Budget
	Estimator       BudgetEstimator
	RequiredSystem  string
	RequiredProfile string
	Items           []Item
	// LightweightEvidence keeps only aggregate counts and costs. Runtime model
	// calls use it to avoid retaining a per-message diagnostic projection.
	LightweightEvidence bool
}

type EvidenceItem struct {
	SourceID, Revision, GroupID, Reason string
	Layer                               Layer
	Lineage                             []string
}
type Evidence struct {
	Used              Cost
	ByLayer           map[Layer]Cost
	Selected, Dropped int
	DroppedGroups     int
	SelectedItems     []EvidenceItem
	DroppedItems      []EvidenceItem
}
type Result struct {
	System, Profile string
	Messages        []core.ChatMessage
	Evidence        Evidence
}

func (b Budget) Validate() error {
	if b.TotalBytes <= 0 || b.TotalBytes > MaxBytes || b.TotalTokens < 0 || b.TotalTokens > MaxTokens {
		return ErrInvalid
	}
	for l, n := range b.LayerBytes {
		if !validLayer(l) || n < 0 || n > b.TotalBytes {
			return ErrInvalid
		}
	}
	for l, n := range b.LayerTokens {
		if !validLayer(l) || n < 0 || n > MaxTokens || (b.TotalTokens > 0 && n > b.TotalTokens) {
			return ErrInvalid
		}
	}
	return nil
}

func (r Request) Validate() error {
	if err := r.Budget.Validate(); err != nil || r.Estimator == nil || len(r.Items) > MaxItems {
		return ErrInvalid
	}
	for _, text := range []string{r.RequiredSystem, r.RequiredProfile} {
		if err := validatePromptText(text); err != nil {
			return err
		}
	}
	for _, it := range r.Items {
		if err := validateMessage(it.Message); err != nil {
			return err
		}
		if !validLayer(it.Layer) || it.Layer == LayerSystem || it.Layer == LayerProfile || it.SourceID == "" || it.Revision == "" || len(it.Lineage) > MaxLineageRefs || it.Priority < 0 || it.Priority > 10000 {
			return ErrInvalid
		}
		for _, s := range []string{it.SourceID, it.Revision, it.GroupID} {
			if err := validateIdentifier(s, MaxMetadataBytes, true); err != nil {
				return err
			}
		}
		for _, s := range it.Lineage {
			if err := validateIdentifier(s, MaxMetadataBytes, true); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateMessage(m core.ChatMessage) error {
	switch m.Role {
	case core.RoleUser, core.RoleAssistant, core.RoleTool:
	default:
		return ErrInvalid
	}
	if !utf8.ValidString(m.Content) || strings.IndexByte(m.Content, 0) >= 0 {
		return ErrInvalid
	}
	if m.Role == core.RoleUser && (m.ToolCall != nil || len(m.ToolCalls) > 0 || m.ToolCallID != "") {
		return ErrInvalid
	}
	if m.Role == core.RoleAssistant && m.ToolCallID != "" {
		return ErrInvalid
	}
	if m.ToolCallID != "" && validateIdentifier(m.ToolCallID, MaxMetadataBytes, false) != nil {
		return ErrInvalid
	}
	if m.Role == core.RoleTool && (m.ToolCall != nil || len(m.ToolCalls) > 0 || m.ToolCallID == "") {
		return ErrInvalid
	}
	if m.ToolCall != nil && len(m.ToolCalls) > 0 {
		return ErrInvalid
	}
	ids := map[string]bool{}
	check := func(tc core.ToolCall) error {
		if len(tc.Continuation) > 64<<10 || !utf8.ValidString(tc.Continuation) {
			return ErrInvalid
		}
		if validateIdentifier(tc.ID, MaxMetadataBytes, false) != nil || validateIdentifier(tc.Name, MaxMetadataBytes, false) != nil || ids[tc.ID] {
			return ErrInvalid
		}
		ids[tc.ID] = true
		if tc.Args != nil {
			b, e := json.Marshal(tc.Args)
			if e != nil || len(b) > MaxBytes {
				return ErrInvalid
			}
			if core.ValidateJSONValue(nil, tc.Args) != nil {
				return ErrInvalid
			}
		}
		return nil
	}
	if m.ToolCall != nil {
		if err := check(*m.ToolCall); err != nil {
			return err
		}
	}
	for _, tc := range m.ToolCalls {
		if err := check(tc); err != nil {
			return err
		}
	}
	return nil
}

func validateText(s string, max int, allowEmpty bool) error {
	if s == "" && allowEmpty {
		return nil
	}
	if s == "" || len(s) > max || !utf8.ValidString(s) || strings.IndexByte(s, 0) >= 0 {
		return ErrInvalid
	}
	for _, r := range s {
		if unicode.IsControl(r) || unicode.In(r, unicode.Cf, unicode.Cs, unicode.Co) || r == '\u007f' {
			return ErrInvalid
		}
	}
	return nil
}

func validatePromptText(s string) error {
	if len(s) > MaxBytes || !utf8.ValidString(s) || strings.IndexByte(s, 0) >= 0 {
		return ErrInvalid
	}
	return nil
}
func validateMetadata(s string, max int, allowEmpty bool) error {
	return validateText(s, max, allowEmpty)
}

func validateIdentifier(s string, max int, allowEmpty bool) error {
	if err := validateMetadata(s, max, allowEmpty); err != nil {
		return err
	}
	if s != "" && strings.TrimSpace(s) != s {
		return ErrInvalid
	}
	return nil
}

func Assemble(ctx context.Context, r Request) (Result, error) {
	if ctx == nil {
		return Result{}, ErrInvalid
	}
	if err := r.Validate(); err != nil {
		return Result{}, err
	}
	res := Result{System: r.RequiredSystem, Profile: r.RequiredProfile, Evidence: Evidence{ByLayer: map[Layer]Cost{}}}
	var used Cost
	add := func(layer Layer, text string) error {
		if text == "" {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		var c Cost
		var err error
		c, err = safeFragmentEstimate(r.Estimator, layer, text)
		if err != nil {
			return err
		}
		if !validCost(c) {
			return ErrInvalid
		}
		if !fits(r.Budget, layer, c, used, res.Evidence.ByLayer) {
			return fmt.Errorf("%w: required %s", ErrBudgetExceeded, layer)
		}
		used.Bytes += c.Bytes
		used.Tokens += c.Tokens
		res.Evidence.ByLayer[layer] = addCost(res.Evidence.ByLayer[layer], c)
		return nil
	}
	if err := add(LayerSystem, r.RequiredSystem); err != nil {
		return Result{}, err
	}
	if err := add(LayerProfile, r.RequiredProfile); err != nil {
		return Result{}, err
	}
	groups := map[string][]int{}
	for i, it := range r.Items {
		if it.GroupID != "" {
			groups[it.GroupID] = append(groups[it.GroupID], i)
		}
	}
	selected := make([]bool, len(r.Items))
	type candidate struct {
		index            int
		indices          []int
		priority, newest int
		required         bool
	}
	candidates := make([]candidate, 0, len(r.Items))
	processed := make([]bool, len(r.Items))
	for i, it := range r.Items {
		if processed[i] {
			continue
		}
		if it.GroupID == "" {
			processed[i] = true
			candidates = append(candidates, candidate{index: i, priority: it.Priority, newest: i, required: it.Required})
			continue
		}
		indices := groups[it.GroupID]
		priority, newest, required := 0, i, false
		for _, j := range indices {
			processed[j] = true
			if r.Items[j].Priority > priority {
				priority = r.Items[j].Priority
			}
			if j > newest {
				newest = j
			}
			required = required || r.Items[j].Required
		}
		candidates = append(candidates, candidate{index: -1, indices: indices, priority: priority, newest: newest, required: required})
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].required != candidates[j].required {
			return candidates[i].required
		}
		if candidates[i].priority != candidates[j].priority {
			return candidates[i].priority > candidates[j].priority
		}
		return candidates[i].newest > candidates[j].newest
	})
	for _, candidate := range candidates {
		if candidate.indices == nil {
			j := candidate.index
			it := r.Items[j]
			cost, err := safeEstimate(r.Estimator, it.Message)
			if err != nil || !validCost(cost) {
				if err != nil {
					return Result{}, err
				}
				return Result{}, ErrInvalid
			}
			if !fits(r.Budget, it.Layer, cost, used, res.Evidence.ByLayer) {
				if candidate.required {
					return Result{}, fmt.Errorf("%w: required messages", ErrBudgetExceeded)
				}
				res.Evidence.Dropped++
				res.Evidence.DroppedGroups++
				if !r.LightweightEvidence {
					res.Evidence.DroppedItems = append(res.Evidence.DroppedItems, EvidenceItem{it.SourceID, it.Revision, it.GroupID, "budget", it.Layer, append([]string(nil), it.Lineage...)})
				}
				continue
			}
			selected[j] = true
			res.Evidence.Selected++
			if !r.LightweightEvidence {
				res.Evidence.SelectedItems = append(res.Evidence.SelectedItems, EvidenceItem{it.SourceID, it.Revision, it.GroupID, "selected", it.Layer, append([]string(nil), it.Lineage...)})
			}
			used = addCost(used, cost)
			res.Evidence.ByLayer[it.Layer] = addCost(res.Evidence.ByLayer[it.Layer], cost)
			continue
		}
		indices := candidate.indices
		var total Cost
		byGroupLayer := map[Layer]Cost{}
		for _, j := range indices {
			c, err := safeEstimate(r.Estimator, r.Items[j].Message)
			if err != nil {
				return Result{}, err
			}
			if c.Bytes < 0 || c.Tokens < 0 {
				return Result{}, ErrInvalid
			}
			if !validCost(c) {
				return Result{}, ErrInvalid
			}
			total = addCost(total, c)
			byGroupLayer[r.Items[j].Layer] = addCost(byGroupLayer[r.Items[j].Layer], c)
		}
		if !fitsGroup(r.Budget, total, used, res.Evidence.ByLayer, byGroupLayer) {
			if candidate.required {
				return Result{}, fmt.Errorf("%w: required messages", ErrBudgetExceeded)
			}
			res.Evidence.DroppedGroups++
			for _, j := range indices {
				it := r.Items[j]
				res.Evidence.Dropped++
				if !r.LightweightEvidence {
					res.Evidence.DroppedItems = append(res.Evidence.DroppedItems, EvidenceItem{it.SourceID, it.Revision, it.GroupID, "budget", it.Layer, append([]string(nil), it.Lineage...)})
				}
			}
			continue
		}
		for _, j := range indices {
			selected[j] = true
			res.Evidence.Selected++
			it := r.Items[j]
			if !r.LightweightEvidence {
				res.Evidence.SelectedItems = append(res.Evidence.SelectedItems, EvidenceItem{it.SourceID, it.Revision, it.GroupID, "selected", it.Layer, append([]string(nil), it.Lineage...)})
			}
		}
		used = addCost(used, total)
		for l, c := range byGroupLayer {
			res.Evidence.ByLayer[l] = addCost(res.Evidence.ByLayer[l], c)
		}
	}
	res.Messages = make([]core.ChatMessage, 0, res.Evidence.Selected)
	for i, it := range r.Items {
		if selected[i] {
			if r.LightweightEvidence {
				// The runtime path receives core's private request copy and consumes
				// this result synchronously as immutable model input.
				res.Messages = append(res.Messages, it.Message)
			} else {
				res.Messages = append(res.Messages, cloneMessage(it.Message))
			}
		}
	}
	res.Evidence.Used = used
	return res, nil
}

func addCost(a, b Cost) Cost { return Cost{Bytes: a.Bytes + b.Bytes, Tokens: a.Tokens + b.Tokens} }
func safeEstimate(e BudgetEstimator, m core.ChatMessage) (c Cost, err error) {
	defer func() {
		if recover() != nil {
			c = Cost{}
			err = ErrInvalid
		}
	}()
	return e.Estimate(m)
}
func safeFragmentEstimate(e BudgetEstimator, l Layer, text string) (c Cost, err error) {
	defer func() {
		if recover() != nil {
			c = Cost{}
			err = ErrInvalid
		}
	}()
	return e.EstimateFragment(l, text)
}
func validCost(c Cost) bool {
	return c.Bytes >= 0 && c.Bytes <= MaxBytes && c.Tokens >= 0 && c.Tokens <= MaxTokens
}
func fits(b Budget, l Layer, c, used Cost, by map[Layer]Cost) bool {
	if c.Bytes > b.TotalBytes-used.Bytes || (b.TotalTokens > 0 && c.Tokens > b.TotalTokens-used.Tokens) {
		return false
	}
	if n, ok := b.LayerBytes[l]; ok && c.Bytes > n-by[l].Bytes {
		return false
	}
	if n, ok := b.LayerTokens[l]; ok && c.Tokens > n-by[l].Tokens {
		return false
	}
	return true
}
func fitsGroup(b Budget, total, used Cost, by map[Layer]Cost, group map[Layer]Cost) bool {
	if total.Bytes > b.TotalBytes-used.Bytes || (b.TotalTokens > 0 && total.Tokens > b.TotalTokens-used.Tokens) {
		return false
	}
	for l, c := range group {
		if n, ok := b.LayerBytes[l]; ok && c.Bytes > n-by[l].Bytes {
			return false
		}
		if n, ok := b.LayerTokens[l]; ok && c.Tokens > n-by[l].Tokens {
			return false
		}
	}
	return true
}
func validLayer(l Layer) bool {
	switch l {
	case LayerSystem, LayerProfile, LayerSummary, LayerRecent, LayerToolResults:
		return true
	}
	return false
}

func cloneMessage(m core.ChatMessage) core.ChatMessage {
	c := m
	if m.ToolCall != nil {
		tc := *m.ToolCall
		if m.ToolCall.Args != nil {
			tc.Args = cloneMap(m.ToolCall.Args)
		}
		c.ToolCall = &tc
	}
	if m.ToolCalls != nil {
		c.ToolCalls = make([]core.ToolCall, len(m.ToolCalls))
		for i, tc := range m.ToolCalls {
			c.ToolCalls[i] = tc
			if tc.Args != nil {
				c.ToolCalls[i].Args = cloneMap(tc.Args)
			}
		}
	}
	return c
}

func cloneMap(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = cloneValue(v)
	}
	return out
}
func cloneValue(v any) any {
	switch x := v.(type) {
	case map[string]any:
		return cloneMap(x)
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = cloneValue(e)
		}
		return out
	case []string:
		return append([]string(nil), x...)
	default:
		return v
	}
}
