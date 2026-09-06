package core

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strings"
	"unicode"
)

const disclosedLibraryID = "harness.tool.library"

const (
	discloseListLimit     = 20
	discloseMaxListLimit  = 50
	discloseSearchTopK    = 8
	discloseMaxSearchTopK = 32
	discloseSummaryRunes  = 240
	discloseActiveLimit   = 8
)

type libraryDoc struct {
	ID          string
	Library     string
	Name        string
	Description string
	Schema      map[string]any
	Triggers    []string
	NotFor      []string
	nameTokens  map[string]bool
	descTokens  map[string]bool
	notTokens   map[string]bool
	allTokens   map[string]bool
}

// libraryIndex is immutable after construction. Keeping the token sets and
// name buckets next to the snapshot avoids rebuilding the whole catalog for
// every model/tool-library search. The snapshot itself is immutable, so a new
// capability snapshot naturally creates and invalidates a new index.
type libraryIndex struct {
	docs   []libraryDoc
	byID   map[string]int
	byName map[string][]int
}

type disclosedRuntime struct {
	inner      ToolRuntime
	index      *libraryIndex
	hotSchemas []ToolSchema
}

func discloseToolRuntime(tools ToolRuntime) ToolRuntime {
	snap, ok := tools.(*CapabilitySnapshot)
	if !ok || snap == nil {
		return tools
	}
	if _, reserved := snap.providers[disclosedLibraryID]; reserved {
		return tools // the host owns this discovery protocol and its dispatch
	}
	docs := []libraryDoc{}
	hotSchemas := []ToolSchema{}
	for _, capability := range snap.capabilities {
		id := capability.Manifest.ID
		if _, executable := snap.providers[id].(ToolProvider); !executable {
			continue
		}
		if capability.Manifest.Tool == nil {
			continue
		}
		if hotKind(capability.Manifest.Kind) {
			description := capability.Manifest.Tool.Description
			if description == "" {
				description = capability.Manifest.Description
			}
			if description == "" {
				description = capability.Manifest.Name
			}
			schema := capability.Manifest.Tool.Parameters
			if schema == nil {
				schema = capability.Manifest.InputSchema
			}
			hotSchemas = append(hotSchemas, ToolSchema{Name: id, Description: description, Parameters: schema})
			continue
		}
		name := id
		if i := strings.LastIndex(id, "."); i >= 0 && i+1 < len(id) {
			name = id[i+1:]
		}
		description := capability.Manifest.Tool.Description
		if description == "" {
			description = capability.Manifest.Description
		}
		schema := capability.Manifest.Tool.Parameters
		if schema == nil {
			schema = capability.Manifest.InputSchema
		}
		docs = append(docs, libraryDoc{
			ID: id, Library: string(capability.Manifest.Kind), Name: name,
			Description: description, Schema: schema,
		})
	}
	if len(docs) == 0 {
		return tools
	}
	docs = deriveSiblingNotFor(docs)
	return &disclosedRuntime{inner: tools, index: newLibraryIndex(docs), hotSchemas: hotSchemas}
}

func newLibraryIndex(docs []libraryDoc) *libraryIndex {
	index := &libraryIndex{
		docs:   make([]libraryDoc, len(docs)),
		byID:   make(map[string]int, len(docs)),
		byName: make(map[string][]int, len(docs)),
	}
	copy(index.docs, docs)
	for i := range index.docs {
		doc := &index.docs[i]
		doc.Triggers = append([]string(nil), doc.Triggers...)
		doc.NotFor = append([]string(nil), doc.NotFor...)
		doc.nameTokens = discloseTokens(doc.Name)
		doc.descTokens = discloseTokens(doc.Description + " " + strings.Join(doc.Triggers, " "))
		doc.notTokens = discloseTokens(strings.Join(doc.NotFor, " "))
		doc.allTokens = discloseMergeTokens(doc.nameTokens, doc.descTokens)
		index.byID[doc.ID] = i
		index.byName[doc.Name] = append(index.byName[doc.Name], i)
	}
	return index
}

func hotKind(kind CapabilityKind) bool {
	switch kind {
	case KindMemory, KindAgent, KindWorkflow, KindRouter, KindKnowledge:
		return true
	default:
		return false
	}
}

func (d *disclosedRuntime) Schemas() []ToolSchema {
	out := cloneToolSchemas(d.hotSchemas)
	out = append(out, ToolSchema{
		Name:        disclosedLibraryID,
		Description: "Search the tool library, then describe one id. Call discovered tools by their qualified id.",
		Parameters:  discloseLibrarySchema(),
	})
	return out
}

// Rebuild the bounded selection from paired model history, not mutable runtime
// state. A fresh Agent can resume with current, authorized schemas; old result
// schemas never become authority. Compacted-away discoveries may be repeated.
func (d *disclosedRuntime) schemasForMessages(messages []ChatMessage) []ToolSchema {
	selected := make([]int, 0, discloseActiveLimit)
	pending := map[string]ToolCall{}
	for _, message := range messages {
		if message.Role == RoleAssistant {
			calls := message.ToolCalls
			if len(calls) == 0 && message.ToolCall != nil {
				calls = []ToolCall{*message.ToolCall}
			}
			for _, call := range calls {
				pending[call.ID] = call
			}
			continue
		}
		if message.Role != RoleTool {
			continue
		}
		call, paired := pending[message.ToolCallID]
		delete(pending, message.ToolCallID)
		if !paired {
			continue
		}
		id := call.Name
		if id == disclosedLibraryID {
			if call.Args["action"] != "describe" || len(message.Content) > MaxCapabilityManifestBytes {
				continue
			}
			var result struct{ Status, ID string }
			if json.Unmarshal([]byte(message.Content), &result) != nil || result.Status != "ok" {
				continue
			}
			index, visible := d.index.byID[result.ID]
			if !visible {
				continue
			}
			requestedID, _ := call.Args["id"].(string)
			requestedName, _ := call.Args["name"].(string)
			requestedID = strings.TrimSpace(requestedID)
			if (requestedID != "" && requestedID != result.ID) || (requestedID == "" && strings.TrimSpace(requestedName) != d.index.docs[index].Name) {
				continue
			}
			id = result.ID
		}
		index, visible := d.index.byID[id]
		if !visible {
			continue
		}
		if previous := slices.Index(selected, index); previous >= 0 {
			selected = slices.Delete(selected, previous, previous+1)
		} else if len(selected) == discloseActiveLimit {
			copy(selected, selected[1:])
			selected = selected[:len(selected)-1]
		}
		selected = append(selected, index)
	}
	out := d.Schemas()
	for _, index := range selected {
		doc := d.index.docs[index]
		out = append(out, ToolSchema{Name: doc.ID, Description: doc.Description, Parameters: cloneMap(doc.Schema)})
	}
	return out
}

func cloneToolSchemas(schemas []ToolSchema) []ToolSchema {
	out := make([]ToolSchema, len(schemas))
	for i, schema := range schemas {
		out[i] = schema
		out[i].Parameters = cloneMap(schema.Parameters)
	}
	return out
}

func (d *disclosedRuntime) Authorized(name string) bool {
	if name == disclosedLibraryID {
		return true
	}
	return d.inner.Authorized(name)
}

func (d *disclosedRuntime) MaxCallBudget() int { return d.inner.MaxCallBudget() }

func (d *disclosedRuntime) ManifestFor(name string) (CapabilityManifest, bool) {
	if name == disclosedLibraryID {
		return discloseLibraryManifest(len(d.index.docs)), true
	}
	if source, ok := d.inner.(manifestSource); ok {
		return source.ManifestFor(name)
	}
	return CapabilityManifest{}, false
}

func (d *disclosedRuntime) BudgetFor(name string) int {
	if name == disclosedLibraryID {
		return 0
	}
	if reporter, ok := d.inner.(budgetReporter); ok {
		return reporter.BudgetFor(name)
	}
	return 0
}

func (d *disclosedRuntime) Execute(ctx context.Context, call ToolCall) (CapabilityResult, error) {
	if call.Name == disclosedLibraryID {
		return d.executeLibrary(call)
	}
	return d.inner.Execute(ctx, call)
}

func (d *disclosedRuntime) executeProtected(ctx context.Context, call ToolCall, invoker ProtectedToolInvoker) (CapabilityResult, error) {
	if call.Name == disclosedLibraryID {
		return d.executeLibrary(call)
	}
	if runtime, ok := d.inner.(protectedToolRuntime); ok {
		return runtime.executeProtected(ctx, call, invoker)
	}
	return d.inner.Execute(ctx, call)
}

func (d *disclosedRuntime) executeLibrary(call ToolCall) (CapabilityResult, error) {
	action, _ := call.Args["action"].(string)
	switch strings.TrimSpace(action) {
	case "search":
		return d.search(call.Args)
	case "list":
		return d.list(call.Args)
	case "describe":
		return d.describe(call.Args)
	default:
		return deniedResult(CodeInvalidArgs, "tool library action must be search, list, or describe"), nil
	}
}

func (d *disclosedRuntime) list(args map[string]any) (CapabilityResult, error) {
	offset, err := discloseIntArg(args, "offset", 0)
	if err != nil || offset < 0 {
		return deniedResult(CodeInvalidArgs, "tool library offset must be >= 0"), nil
	}
	limit, err := discloseIntArg(args, "limit", discloseListLimit)
	if err != nil || limit < 1 || limit > discloseMaxListLimit {
		return deniedResult(CodeInvalidArgs, fmt.Sprintf("tool library limit must be between 1 and %d", discloseMaxListLimit)), nil
	}
	total := len(d.index.docs)
	if offset > total {
		offset = total
	}
	end := offset + limit
	if end > total {
		end = total
	}
	page := d.index.docs[offset:end]
	return discloseJSON(map[string]any{
		"status": "ok", "total": total, "offset": offset, "limit": limit,
		"tools": discloseCards(page),
	}, map[string]any{"library.action": "list", "library.hits": len(page), "library.total": total})
}

func (d *disclosedRuntime) search(args map[string]any) (CapabilityResult, error) {
	query, _ := args["query"].(string)
	query = strings.TrimSpace(query)
	if query == "" {
		return deniedResult(CodeInvalidArgs, "tool library search requires a query"), nil
	}
	topK, err := discloseIntArg(args, "top_k", discloseSearchTopK)
	if err != nil || topK < 1 || topK > discloseMaxSearchTopK {
		return deniedResult(CodeInvalidArgs, fmt.Sprintf("tool library top_k must be between 1 and %d", discloseMaxSearchTopK)), nil
	}
	hits := d.index.search(query, topK)
	ids := make([]string, 0, len(hits))
	for _, hit := range hits {
		ids = append(ids, hit.ID)
	}
	if len(ids) > 8 {
		ids = ids[:8]
	}
	return discloseJSON(map[string]any{"status": "ok", "query": query, "tools": hits}, map[string]any{
		"library.action": "search", "library.hits": len(hits), "library.query": discloseSummary(query), "library.hit_ids": ids,
	})
}

func (d *disclosedRuntime) describe(args map[string]any) (CapabilityResult, error) {
	id, _ := args["id"].(string)
	id = strings.TrimSpace(id)
	name, _ := args["name"].(string)
	name = strings.TrimSpace(name)
	if id != "" {
		index, ok := d.index.byID[id]
		if !ok {
			return deniedResult(CodeInvalidArgs, fmt.Sprintf("tool library has no id %q", id)), nil
		}
		doc := d.index.docs[index]
		return discloseJSON(map[string]any{
			"status": "ok", "id": doc.ID, "library": doc.Library, "name": doc.Name,
			"description": doc.Description, "input_schema": cloneMap(doc.Schema),
		}, map[string]any{"library.action": "describe", "library.id": doc.ID, "library.hits": 1})
	}
	if name == "" {
		return deniedResult(CodeInvalidArgs, "tool library describe requires id or name"), nil
	}
	matchIndexes := d.index.byName[name]
	matches := make([]libraryDoc, 0, len(matchIndexes))
	for _, index := range matchIndexes {
		matches = append(matches, d.index.docs[index])
	}
	if len(matches) == 0 {
		return deniedResult(CodeInvalidArgs, fmt.Sprintf("tool library has no tool %q", name)), nil
	}
	if len(matches) > 1 {
		return discloseJSON(map[string]any{
			"status": "explore",
			"reason": "multiple tools share this name; describe one id then call that id",
			"tools":  discloseCards(matches),
		}, map[string]any{"library.action": "explore", "library.hits": len(matches), "library.explore": true})
	}
	doc := matches[0]
	return discloseJSON(map[string]any{
		"status": "ok", "id": doc.ID, "library": doc.Library, "name": doc.Name,
		"description": doc.Description, "input_schema": cloneMap(doc.Schema),
	}, map[string]any{"library.action": "describe", "library.id": doc.ID, "library.hits": 1})
}

type libraryHit struct {
	ID          string   `json:"id"`
	Library     string   `json:"library,omitempty"`
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Score       int      `json:"score"`
	Ambiguous   bool     `json:"ambiguous,omitempty"`
	SameName    []string `json:"same_name,omitempty"`
}

func (index *libraryIndex) search(query string, topK int) []libraryHit {
	queryTokens := discloseQueryTokens(query)
	if len(queryTokens) == 0 {
		return nil
	}
	hits := []libraryHit{}
	for _, doc := range index.docs {
		nameScore := discloseOverlap(queryTokens, doc.nameTokens)
		descScore := discloseOverlap(queryTokens, doc.descTokens)
		covered := discloseOverlap(queryTokens, doc.allTokens)
		notScore := discloseOverlap(queryTokens, doc.notTokens)
		if notScore > 0 && notScore >= descScore && notScore >= nameScore {
			continue
		}
		if covered <= 0 {
			continue
		}
		if len(queryTokens) >= 2 && descScore == 0 && covered < len(queryTokens) {
			continue
		}
		if len(queryTokens) >= 2 && covered*2 < len(queryTokens) {
			continue
		}
		same := []string{}
		for _, otherIndex := range index.byName[doc.Name] {
			other := index.docs[otherIndex].ID
			if other != doc.ID {
				same = append(same, other)
			}
		}
		hits = append(hits, libraryHit{
			ID: doc.ID, Library: doc.Library, Name: doc.Name,
			Description: discloseSummary(doc.Description), Score: descScore*3 + nameScore,
			Ambiguous: len(same) > 0, SameName: same,
		})
	}
	sort.Slice(hits, func(i, j int) bool {
		if hits[i].Score != hits[j].Score {
			return hits[i].Score > hits[j].Score
		}
		return hits[i].ID < hits[j].ID
	})
	if topK > 0 && len(hits) > topK {
		hits = hits[:topK]
	}
	return hits
}

func discloseCards(docs []libraryDoc) []map[string]string {
	out := make([]map[string]string, 0, len(docs))
	for _, doc := range docs {
		out = append(out, map[string]string{
			"id": doc.ID, "library": doc.Library, "name": doc.Name, "description": discloseSummary(doc.Description),
		})
	}
	return out
}

func discloseLibraryManifest(count int) CapabilityManifest {
	return CapabilityManifest{
		ID: disclosedLibraryID, Version: "1.0.0", Name: disclosedLibraryID,
		Description: fmt.Sprintf("Tool library over %d capabilities. Search, then describe one id, then call that id.", count),
		Kind:        KindKnowledge, Contract: "harness.tool.library/v1",
		Tool: &ToolExposure{Parameters: discloseLibrarySchema()},
	}
}

func discloseLibrarySchema() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"action": map[string]any{"type": "string", "enum": []any{"search", "list", "describe"}},
			"query":  map[string]any{"type": "string"},
			"id":     map[string]any{"type": "string"},
			"name":   map[string]any{"type": "string"},
			"offset": map[string]any{"type": "integer", "minimum": 0},
			"limit":  map[string]any{"type": "integer", "minimum": 1, "maximum": discloseMaxListLimit},
			"top_k":  map[string]any{"type": "integer", "minimum": 1, "maximum": discloseMaxSearchTopK},
		},
		"required": []any{"action"},
	}
}

func discloseQueryTokens(text string) map[string]bool {
	tokens := discloseTokens(text)
	filtered := map[string]bool{}
	for token := range tokens {
		if discloseStopwords[token] {
			continue
		}
		filtered[token] = true
	}
	if len(filtered) == 0 {
		return tokens
	}
	return filtered
}

func discloseTokens(text string) map[string]bool {
	tokens := map[string]bool{}
	normalized := strings.NewReplacer("-", " ", "_", " ", "/", " ", ".", " ").Replace(strings.ToLower(text))
	for _, field := range strings.Fields(normalized) {
		tokens[strings.Trim(field, ".,;:!?")] = true
	}
	delete(tokens, "")
	return tokens
}

func discloseMergeTokens(parts ...map[string]bool) map[string]bool {
	out := map[string]bool{}
	for _, part := range parts {
		for token := range part {
			out[token] = true
		}
	}
	return out
}

func discloseOverlap(query, doc map[string]bool) int {
	score := 0
	for token := range query {
		if doc[token] {
			score++
		}
	}
	return score
}

func discloseSummary(text string) string {
	var out strings.Builder
	if len(text) < discloseSummaryRunes {
		out.Grow(len(text))
	}
	runes := 0
	for _, r := range text {
		if runes >= discloseSummaryRunes {
			break
		}
		if r == '\n' || r == '\t' {
			r = ' '
		} else if unicode.IsControl(r) {
			continue
		}
		out.WriteRune(r)
		runes++
	}
	return out.String()
}

func discloseIntArg(args map[string]any, key string, fallback int) (int, error) {
	raw, ok := args[key]
	if !ok || raw == nil {
		return fallback, nil
	}
	switch typed := raw.(type) {
	case int:
		return typed, nil
	case int64:
		return int(typed), nil
	case float64:
		if typed != float64(int(typed)) {
			return 0, fmt.Errorf("not an integer")
		}
		return int(typed), nil
	case json.Number:
		value, err := typed.Int64()
		return int(value), err
	default:
		return 0, fmt.Errorf("not an integer")
	}
}

func discloseJSON(value any, metadata map[string]any) (CapabilityResult, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return CapabilityResult{}, err
	}
	return CapabilityResult{Content: string(encoded), OK: true, Metadata: metadata}, nil
}

var discloseStopwords = map[string]bool{
	"a": true, "an": true, "the": true, "to": true, "for": true, "of": true,
	"and": true, "or": true, "in": true, "on": true, "with": true, "into": true,
}

func deriveSiblingNotFor(docs []libraryDoc) []libraryDoc {
	byName := map[string][]int{}
	for i, doc := range docs {
		byName[doc.Name] = append(byName[doc.Name], i)
	}
	out := append([]libraryDoc(nil), docs...)
	for _, idxs := range byName {
		if len(idxs) < 2 {
			continue
		}
		for _, i := range idxs {
			mine := discloseTokens(out[i].Description + " " + out[i].Name)
			seen := map[string]bool{}
			for _, j := range idxs {
				if i == j {
					continue
				}
				for token := range discloseTokens(out[j].Description) {
					if mine[token] || discloseStopwords[token] || seen[token] {
						continue
					}
					seen[token] = true
					out[i].NotFor = append(out[i].NotFor, token)
				}
			}
		}
	}
	return out
}
