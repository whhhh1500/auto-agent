package execution

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"unicode"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
	"github.com/cc-auto-agent/harness-core/pkg/extensions/toollib"
)

const (
	ContractToolLibraryV1 = "harness.tool.library/v1"
	maxMCPListLimit       = 50
	defaultMCPListLimit   = 20
	maxMCPSearchTopK      = 32
	defaultMCPSearchTopK  = 8
	maxMCPSummaryRunes    = 240
)

// toolDocument is one remote tool stored as a knowledge-base record.
// ID is library-qualified so two catalogs may share a local name without
// colliding. List/search return identities and short descriptions only;
// schemas stay behind describe.
type toolDocument struct {
	ID          string
	Library     string
	Name        string
	Description string
	InputSchema map[string]any
	Triggers    []string
	NotFor      []string
}

type mcpGateway struct {
	mu       sync.RWMutex
	conn     mcpTransport
	observer ToolLibraryObserver
	catalog  *toollib.Catalog
	library  string
	version  string
	tools    []toolDocument
	byID     map[string]toolDocument
	byName   map[string][]toolDocument
}

func (p *mcpGateway) ArtifactRevision() string {
	if p == nil || p.conn == nil {
		return "mcp-library/unconfigured"
	}
	return fmt.Sprintf("mcp-library/v1/%s/%s", p.library, p.version)
}

func indexMCPTools(tools []mcpToolDescription) []toolDocument {
	seen := map[string]bool{}
	out := make([]toolDocument, 0, len(tools))
	for _, tool := range tools {
		name := strings.TrimSpace(tool.Name)
		if name == "" || seen[name] || strings.ContainsRune(name, 0) {
			continue
		}
		seen[name] = true
		out = append(out, toolDocument{
			Name:        name,
			Description: tool.Description,
			InputSchema: tool.InputSchema,
			Triggers:    append([]string(nil), tool.Triggers...),
			NotFor:      append([]string(nil), tool.NotFor...),
		})
	}
	return out
}

func newMCPGateway(conn mcpTransport, tools []toolDocument) *mcpGateway {
	identity := mcpTransportIdentity{}
	if conn != nil {
		identity = conn.identity()
	}
	catalog := toollib.NewCatalog()
	catalog.SetSearcher(toollib.HybridSearcher{})
	gateway := &mcpGateway{
		conn: conn, observer: identity.observer, catalog: catalog, library: identity.namespace, version: identity.version,
		byID: map[string]toolDocument{}, byName: map[string][]toolDocument{},
	}
	qualified := qualifyToolLibrary(identity.namespace, tools)
	if len(qualified) > 0 {
		_ = gateway.replaceListing(qualified)
	}
	return gateway
}

func (p *mcpGateway) replaceListing(tools []toolDocument) error {
	if p == nil || p.catalog == nil {
		return fmt.Errorf("mcp library catalog is not configured")
	}
	records := make([]toollib.Record, 0, len(tools))
	for _, tool := range tools {
		records = append(records, toollib.Record{
			ID: tool.ID, Library: tool.Library, Name: tool.Name,
			Description: tool.Description, Schema: tool.InputSchema, RemoteName: tool.Name,
			Triggers: tool.Triggers, NotFor: tool.NotFor,
		})
	}
	if _, err := p.catalog.Apply(records); err != nil {
		return err
	}
	p.syncFromCatalog()
	return nil
}

func (p *mcpGateway) syncFromCatalog() {
	records := p.catalog.Snapshot()
	tools := make([]toolDocument, 0, len(records))
	byID := make(map[string]toolDocument, len(records))
	byName := map[string][]toolDocument{}
	for _, rec := range records {
		doc := toolDocument{
			ID: rec.ID, Library: rec.Library, Name: rec.Name,
			Description: rec.Description, InputSchema: rec.Schema,
		}
		tools = append(tools, doc)
		byID[doc.ID] = doc
		byName[doc.Name] = append(byName[doc.Name], doc)
	}
	p.tools = tools
	p.byID = byID
	p.byName = byName
}

// Refresh reloads tools/list from the MCP server and reconciles the catalog.
// A failed or empty listing leaves the previous catalog in place.
func (p *mcpGateway) Refresh(ctx context.Context) error {
	if p == nil || p.conn == nil {
		return fmt.Errorf("mcp library is not connected")
	}
	tools, err := listMCPDocuments(ctx, p.conn)
	if err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.replaceListing(qualifyToolLibrary(p.library, tools))
}

func qualifyToolLibrary(library string, tools []toolDocument) []toolDocument {
	out := make([]toolDocument, 0, len(tools))
	for _, tool := range tools {
		tool.Library = library
		if library != "" {
			tool.ID = library + "/" + tool.Name
		} else if tool.ID == "" {
			tool.ID = tool.Name
		}
		out = append(out, tool)
	}
	return out
}

func mcpLibraryManifest(id, version string, toolCount int) core.CapabilityManifest {
	if version == "" {
		version = "1.0.0"
	}
	parameters := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"action": map[string]any{
				"type":        "string",
				"enum":        []any{"search", "list", "describe", "call"},
				"description": "search retrieves candidate tools; list pages names; describe loads one schema; call executes one tool.",
			},
			"query": map[string]any{
				"type":        "string",
				"description": "Natural-language or keyword query for search.",
			},
			"id": map[string]any{
				"type":        "string",
				"description": "Fully qualified tool id (library/name). Prefer this over a bare name.",
			},
			"name": map[string]any{
				"type":        "string",
				"description": "Local tool name. Rejected when more than one library uses it.",
			},
			"arguments": map[string]any{
				"type":        "object",
				"description": "Arguments for call.",
			},
			"offset": map[string]any{"type": "integer", "minimum": 0},
			"limit":  map[string]any{"type": "integer", "minimum": 1, "maximum": maxMCPListLimit},
			"top_k":  map[string]any{"type": "integer", "minimum": 1, "maximum": maxMCPSearchTopK},
		},
		"required": []any{"action"},
	}
	return core.CapabilityManifest{
		ID: id, Version: version, Name: id,
		Description: fmt.Sprintf("Tool library over %d remote MCP tools. Search, then describe or call; schemas are not inlined.", toolCount),
		Kind:        core.KindKnowledge,
		Contract:    ContractToolLibraryV1,
		Tool:        &core.ToolExposure{Description: "Progressive disclosure over a remote tool library.", Parameters: parameters},
	}
}

func (p *mcpGateway) Execute(ctx context.Context, request core.CapabilityRequest) (core.CapabilityResult, error) {
	action, _ := request.Args["action"].(string)
	switch strings.TrimSpace(action) {
	case "search":
		return p.search(ctx, request)
	case "list":
		return p.list(ctx, request)
	case "describe":
		return p.describe(ctx, request)
	case "call":
		return p.callTool(ctx, request)
	default:
		return deniedResult("invalid_args", "mcp library action must be search, list, describe, or call"), nil
	}
}

func (p *mcpGateway) list(ctx context.Context, request core.CapabilityRequest) (core.CapabilityResult, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	args := request.Args
	offset, err := mcpIntArg(args, "offset", 0)
	if err != nil || offset < 0 {
		return deniedResult("invalid_args", "mcp library offset must be >= 0"), nil
	}
	limit, err := mcpIntArg(args, "limit", defaultMCPListLimit)
	if err != nil || limit < 1 || limit > maxMCPListLimit {
		return deniedResult("invalid_args", fmt.Sprintf("mcp library limit must be between 1 and %d", maxMCPListLimit)), nil
	}
	total := len(p.tools)
	if offset > total {
		offset = total
	}
	end := offset + limit
	if end > total {
		end = total
	}
	page := mcpSummaries(p.tools[offset:end])
	obs := ToolLibraryObservation{Action: "list", Hits: len(page), HitIDs: mcpHitIDs(p.tools[offset:end])}
	p.observe(ctx, request, obs)
	return mcpJSONResult(map[string]any{
		"status": "ok",
		"total":  total,
		"offset": offset,
		"limit":  limit,
		"tools":  page,
	}, libraryMetadata(obs))
}

func (p *mcpGateway) search(ctx context.Context, request core.CapabilityRequest) (core.CapabilityResult, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	args := request.Args
	query, _ := args["query"].(string)
	query = strings.TrimSpace(query)
	if query == "" {
		return deniedResult("invalid_args", "mcp library search requires a query"), nil
	}
	topK, err := mcpIntArg(args, "top_k", defaultMCPSearchTopK)
	if err != nil || topK < 1 || topK > maxMCPSearchTopK {
		return deniedResult("invalid_args", fmt.Sprintf("mcp library top_k must be between 1 and %d", maxMCPSearchTopK)), nil
	}
	var catalogHits []toollib.Hit
	if p.catalog != nil {
		catalogHits = p.catalog.Search(query, topK)
	} else {
		catalogHits = toollib.SearchRecords(mcpDocumentsToRecords(p.tools), query, topK)
	}
	hits := make([]toolHit, 0, len(catalogHits))
	for _, hit := range catalogHits {
		hits = append(hits, toolHit{
			ID: hit.ID, Library: hit.Library, Name: hit.Name, Description: hit.Description,
			Score: hit.Score, Ambiguous: hit.Ambiguous, SameName: hit.SameName,
		})
	}
	obs := ToolLibraryObservation{Action: "search", Query: mcpSummary(query), Hits: len(hits), HitIDs: mcpHitIDsFromHits(hits)}
	p.observe(ctx, request, obs)
	return mcpJSONResult(map[string]any{
		"status": "ok",
		"query":  query,
		"tools":  hits,
	}, libraryMetadata(obs))
}

func (p *mcpGateway) describe(ctx context.Context, request core.CapabilityRequest) (core.CapabilityResult, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	resolved := p.resolveTool(request.Args)
	if resolved.denied {
		return resolved.deny, nil
	}
	if len(resolved.candidates) > 1 {
		obs := ToolLibraryObservation{Action: "explore", Name: strings.TrimSpace(fmt.Sprint(request.Args["name"])), Hits: len(resolved.candidates), HitIDs: mcpHitIDs(resolved.candidates), Explore: true}
		p.observe(ctx, request, obs)
		return mcpJSONResult(map[string]any{
			"status": "explore",
			"reason": "multiple tools share this name; describe or call one id",
			"tools":  mcpExploreCards(resolved.candidates),
		}, libraryMetadata(obs))
	}
	tool := resolved.tool
	obs := ToolLibraryObservation{Action: "describe", ToolID: tool.ID, Library: tool.Library, Name: tool.Name, Hits: 1, HitIDs: []string{tool.ID}}
	p.observe(ctx, request, obs)
	return mcpJSONResult(map[string]any{
		"status":       "ok",
		"id":           tool.ID,
		"library":      tool.Library,
		"name":         tool.Name,
		"description":  tool.Description,
		"input_schema": tool.InputSchema,
	}, libraryMetadata(obs))
}

func (p *mcpGateway) callTool(ctx context.Context, request core.CapabilityRequest) (core.CapabilityResult, error) {
	args := request.Args
	p.mu.RLock()
	resolved := p.resolveTool(args)
	p.mu.RUnlock()
	if resolved.denied {
		return resolved.deny, nil
	}
	if len(resolved.candidates) > 1 {
		obs := ToolLibraryObservation{Action: "explore", Name: strings.TrimSpace(fmt.Sprint(args["name"])), Hits: len(resolved.candidates), HitIDs: mcpHitIDs(resolved.candidates), Explore: true}
		p.observe(ctx, request, obs)
		return mcpJSONResult(map[string]any{
			"status": "explore",
			"reason": "multiple tools share this name; call one id after comparing descriptions",
			"tools":  mcpExploreCards(resolved.candidates),
		}, libraryMetadata(obs))
	}
	tool := resolved.tool
	name := tool.Name
	arguments, _ := args["arguments"].(map[string]any)
	if arguments == nil {
		arguments = map[string]any{}
	}
	result, err := p.conn.call(ctx, "tools/call", map[string]any{
		"name": name, "arguments": arguments,
	})
	if err != nil {
		return core.CapabilityResult{Content: err.Error(), OK: false}, nil
	}
	var payload struct {
		Content []mcpContentBlock `json:"content"`
		IsError bool              `json:"isError"`
	}
	if err := json.Unmarshal(result, &payload); err != nil {
		return core.CapabilityResult{Content: fmt.Sprintf("mcp result decode: %v", err), OK: false}, nil
	}
	text, err := mcpTextContent(payload.Content, core.DefaultMaxCapabilityOutputBytes)
	if err != nil {
		return core.CapabilityResult{Content: err.Error(), OK: false, Metadata: map[string]any{"code": "capability_output_too_large"}}, nil
	}
	return core.CapabilityResult{
		Content: text, OK: !payload.IsError,
		Metadata: map[string]any{"library.action": "call", "library.id": tool.ID, "library.name": name},
	}, nil
}

type toolHit struct {
	ID          string   `json:"id"`
	Library     string   `json:"library,omitempty"`
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Score       int      `json:"score"`
	Ambiguous   bool     `json:"ambiguous,omitempty"`
	SameName    []string `json:"same_name,omitempty"`
}

type toolResolution struct {
	tool       toolDocument
	candidates []toolDocument
	deny       core.CapabilityResult
	denied     bool
}

func (p *mcpGateway) resolveTool(args map[string]any) toolResolution {
	id, _ := args["id"].(string)
	id = strings.TrimSpace(id)
	name, _ := args["name"].(string)
	name = strings.TrimSpace(name)
	if id != "" {
		tool, ok := p.byID[id]
		if !ok {
			return toolResolution{denied: true, deny: deniedResult("invalid_args", fmt.Sprintf("mcp library has no tool id %q", id))}
		}
		return toolResolution{tool: tool, candidates: []toolDocument{tool}}
	}
	if name == "" {
		return toolResolution{denied: true, deny: deniedResult("invalid_args", "mcp library describe/call requires id or name")}
	}
	matches := p.byName[name]
	if len(matches) == 0 {
		return toolResolution{denied: true, deny: deniedResult("invalid_args", fmt.Sprintf("mcp library has no tool %q", name))}
	}
	if len(matches) == 1 {
		return toolResolution{tool: matches[0], candidates: matches}
	}
	return toolResolution{candidates: matches}
}

func mcpExploreCards(tools []toolDocument) []map[string]string {
	out := make([]map[string]string, 0, len(tools))
	for _, tool := range tools {
		out = append(out, map[string]string{
			"id":          tool.ID,
			"library":     tool.Library,
			"name":        tool.Name,
			"description": mcpSummary(tool.Description),
		})
	}
	return out
}

func searchToolLibrary(tools []toolDocument, query string, topK int) []toolHit {
	queryTokens := mcpQueryTokens(query)
	if len(queryTokens) == 0 {
		return nil
	}
	byName := map[string][]string{}
	for _, tool := range tools {
		byName[tool.Name] = append(byName[tool.Name], tool.ID)
	}
	hits := []toolHit{}
	for _, tool := range tools {
		nameTokens := mcpTokens(tool.Name)
		descTokens := mcpTokens(tool.Description)
		docTokens := mcpMergeTokens(nameTokens, descTokens)
		nameScore := mcpOverlap(queryTokens, nameTokens)
		descScore := mcpOverlap(queryTokens, descTokens)
		covered := mcpOverlap(queryTokens, docTokens)
		if covered <= 0 {
			continue
		}
		// Multi-token intent must match description, not just a shared short name.
		if len(queryTokens) >= 2 && descScore == 0 && covered < len(queryTokens) {
			continue
		}
		if len(queryTokens) >= 2 && covered*2 < len(queryTokens) {
			continue
		}
		score := descScore*3 + nameScore
		siblings := append([]string(nil), byName[tool.Name]...)
		sameName := []string{}
		for _, other := range siblings {
			if other != tool.ID {
				sameName = append(sameName, other)
			}
		}
		hits = append(hits, toolHit{
			ID:          tool.ID,
			Library:     tool.Library,
			Name:        tool.Name,
			Description: mcpSummary(tool.Description),
			Score:       score,
			Ambiguous:   len(sameName) > 0,
			SameName:    sameName,
		})
	}
	sort.Slice(hits, func(i, j int) bool {
		if hits[i].Score != hits[j].Score {
			return hits[i].Score > hits[j].Score
		}
		if hits[i].Library != hits[j].Library {
			return hits[i].Library < hits[j].Library
		}
		return hits[i].ID < hits[j].ID
	})
	if topK > 0 && len(hits) > topK {
		hits = hits[:topK]
	}
	return hits
}

func mcpSummaries(tools []toolDocument) []map[string]string {
	out := make([]map[string]string, 0, len(tools))
	for _, tool := range tools {
		out = append(out, map[string]string{
			"id":          tool.ID,
			"library":     tool.Library,
			"name":        tool.Name,
			"description": mcpSummary(tool.Description),
		})
	}
	return out
}

func mcpQueryTokens(text string) map[string]bool {
	tokens := mcpTokens(text)
	filtered := map[string]bool{}
	for token := range tokens {
		if mcpStopwords[token] {
			continue
		}
		filtered[token] = true
	}
	if len(filtered) == 0 {
		return tokens
	}
	return filtered
}

func mcpTokens(text string) map[string]bool {
	tokens := map[string]bool{}
	normalized := strings.ToLower(text)
	normalized = strings.NewReplacer("-", " ", "_", " ", "/", " ", ".", " ").Replace(normalized)
	for _, field := range strings.Fields(normalized) {
		tokens[strings.Trim(field, ".,;:!?")] = true
	}
	delete(tokens, "")
	return tokens
}

func mcpMergeTokens(parts ...map[string]bool) map[string]bool {
	out := map[string]bool{}
	for _, part := range parts {
		for token := range part {
			out[token] = true
		}
	}
	return out
}

func mcpOverlap(query, doc map[string]bool) int {
	score := 0
	for token := range query {
		if doc[token] {
			score++
		}
	}
	return score
}

var mcpStopwords = map[string]bool{
	"a": true, "an": true, "the": true, "to": true, "for": true, "of": true,
	"and": true, "or": true, "in": true, "on": true, "with": true, "into": true,
}

func mcpSummary(text string) string {
	cleaned := strings.Map(func(r rune) rune {
		if r == '\n' || r == '\t' {
			return ' '
		}
		if unicode.IsControl(r) {
			return -1
		}
		return r
	}, text)
	runes := []rune(cleaned)
	if len(runes) <= maxMCPSummaryRunes {
		return cleaned
	}
	return string(runes[:maxMCPSummaryRunes])
}

func mcpIntArg(args map[string]any, key string, fallback int) (int, error) {
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

func mcpHitIDs(tools []toolDocument) []string {
	out := make([]string, 0, len(tools))
	for _, tool := range tools {
		if tool.ID != "" {
			out = append(out, tool.ID)
		}
	}
	return out
}

func mcpHitIDsFromHits(hits []toolHit) []string {
	out := make([]string, 0, len(hits))
	for _, hit := range hits {
		if hit.ID != "" {
			out = append(out, hit.ID)
		}
	}
	return out
}

func mcpJSONResult(value any, metadata map[string]any) (core.CapabilityResult, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return core.CapabilityResult{}, err
	}
	return core.CapabilityResult{Content: string(encoded), OK: true, Metadata: metadata}, nil
}

func mcpDocumentsToRecords(tools []toolDocument) []toollib.Record {
	out := make([]toollib.Record, 0, len(tools))
	for _, tool := range tools {
		out = append(out, toollib.Record{
			ID: tool.ID, Library: tool.Library, Name: tool.Name,
			Description: tool.Description, Schema: tool.InputSchema, RemoteName: tool.Name,
			Triggers: tool.Triggers, NotFor: tool.NotFor,
		})
	}
	return out
}
