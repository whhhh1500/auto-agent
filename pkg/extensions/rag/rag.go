// Package rag provides optional scoped retrieval implementations and a search
// capability for the core runtime.
package rag

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"unicode"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
	"github.com/cc-auto-agent/harness-core/pkg/extensions/internal/support"
)

var errStandardSearch = errors.New("standard rag search failed")

const ContractSearchV1 = "harness.rag.search/v1"

const (
	MaxDocumentContentBytes = 1 << 20
	MaxDocumentIDBytes      = 256
	MaxDocumentSourceBytes  = 4096
	MaxDocumentTags         = 128
	MaxSearchTopK           = 100
	MaxQueryRunes           = 4096
	MaxTagRunes             = 256
	MaxQueryTokens          = 128
	MaxQueryTags            = MaxDocumentTags
	MaxDocumentsPerScope    = 1024
	MaxDocumentTokens       = 8192
	MaxScopes               = 4096
)

type Document = core.RagDocument
type Query = core.RagQuery
type Chunk = core.RagChunk

// Index ingests document chunks and searches within the caller's visible
// scope prefixes.
type Index interface {
	Ingest(ctx context.Context, scope core.ScopePath, document Document) error
	Search(ctx context.Context, scope core.ScopePath, query Query) ([]Chunk, error)
}

type KeywordIndex struct {
	mu           sync.RWMutex
	scopes       map[string][]indexedChunk
	maxDocuments int
	maxScopes    int
}

type indexedChunk struct {
	doc    Document
	tokens map[string]bool
}

func NewKeywordIndex() *KeywordIndex {
	return &KeywordIndex{scopes: map[string][]indexedChunk{}}
}

// ValidateScope validates the ownership scope required by RAG operations.
func ValidateScope(scope core.ScopePath) error {
	if scope.Depth() == 0 {
		return fmt.Errorf("rag scope is empty")
	}
	return nil
}

func validateText(value string) bool {
	for _, char := range value {
		if unicode.IsControl(char) {
			return false
		}
	}
	return true
}

// ValidateDocument validates a document before indexing.
func ValidateDocument(document Document) error {
	if document.ID == "" {
		return fmt.Errorf("rag document requires an id")
	}
	if len(document.ID) > MaxDocumentIDBytes {
		return fmt.Errorf("rag document id exceeds maximum of %d bytes", MaxDocumentIDBytes)
	}
	if !validateText(document.ID) {
		return fmt.Errorf("rag document id contains a control character")
	}
	if len(document.Source) > MaxDocumentSourceBytes {
		return fmt.Errorf("rag document source exceeds maximum of %d bytes", MaxDocumentSourceBytes)
	}
	if !validateText(document.Source) {
		return fmt.Errorf("rag document source contains a control character")
	}
	if strings.TrimSpace(document.Content) == "" {
		return fmt.Errorf("rag document requires content")
	}
	if len(document.Content) > MaxDocumentContentBytes {
		return fmt.Errorf("rag document exceeds content or tag limits")
	}
	if strings.ContainsRune(document.Content, '\x00') {
		return fmt.Errorf("rag document content contains NUL")
	}
	if len(Tokenize(document.Content)) > MaxDocumentTokens {
		return fmt.Errorf("rag document exceeds %d unique tokens", MaxDocumentTokens)
	}
	return ValidateTags(document.Tags)
}

// cjkFullwidthTokenSeparators is deliberately limited to CJK/fullwidth
// sentence and paired punctuation. It is not a Chinese word segmenter: text
// without a separator remains one keyword token. ASCII punctuation and Latin
// apostrophes stay within tokens so identifiers, email addresses, URLs, and
// words such as l'été retain their existing keyword contract.
const cjkFullwidthTokenSeparators = "，、。；：！？（）［］【】「」『』《》〈〉"

// Tokenize is the keyword tokenizer shared by the reference index, SQL ingest,
// and projection rebuild. It lower-cases, splits on Unicode whitespace plus
// the fixed CJK/fullwidth delimiters, and trims the same ASCII punctuation
// from each token.
func Tokenize(text string) map[string]bool {
	tokens := map[string]bool{}
	lower := strings.ToLower(text)
	if !containsCJKFullwidthTokenSeparator(lower) {
		for _, field := range strings.Fields(lower) {
			tokens[strings.Trim(field, ".,;:!?")] = true
		}
		delete(tokens, "")
		return tokens
	}
	for _, field := range strings.Fields(lower) {
		for token := range strings.FieldsFuncSeq(field, isCJKFullwidthTokenSeparator) {
			tokens[strings.Trim(token, ".,;:!?")] = true
		}
	}
	delete(tokens, "")
	return tokens
}

func isCJKFullwidthTokenSeparator(char rune) bool {
	switch char {
	case '，', '、', '。', '；', '：', '！', '？', '（', '）', '［', '］', '【', '】', '「', '」', '『', '』', '《', '》', '〈', '〉':
		return true
	default:
		return false
	}
}

func containsCJKFullwidthTokenSeparator(text string) bool {
	for _, char := range text {
		if char > unicode.MaxASCII && isCJKFullwidthTokenSeparator(char) {
			return true
		}
	}
	return false
}

// ValidateQuery checks the size and unique token bounds for one RAG query.
func ValidateQuery(query string) error {
	if len([]rune(query)) > MaxQueryRunes {
		return fmt.Errorf("rag query exceeds %d runes", MaxQueryRunes)
	}
	if strings.ContainsRune(query, '\x00') {
		return fmt.Errorf("rag query contains NUL")
	}
	if len(Tokenize(query)) > MaxQueryTokens {
		return fmt.Errorf("rag query exceeds %d unique tokens", MaxQueryTokens)
	}
	return nil
}

// ValidateTag checks the size bound for one RAG tag. Empty tags remain valid.
func ValidateTag(tag string) error {
	if len([]rune(tag)) > MaxTagRunes {
		return fmt.Errorf("rag tag exceeds %d runes", MaxTagRunes)
	}
	if strings.ContainsRune(tag, '\x00') {
		return fmt.Errorf("rag tag contains NUL")
	}
	return nil
}

// ValidateTags checks the size and item-count bounds for RAG tags.
func ValidateTags(tags []string) error {
	if len(tags) > MaxQueryTags {
		return fmt.Errorf("rag tags exceed %d items", MaxQueryTags)
	}
	for _, tag := range tags {
		if err := ValidateTag(tag); err != nil {
			return err
		}
	}
	return nil
}

// ValidateSearch validates a complete RAG search request and returns its tokens.
func ValidateSearch(query Query) (map[string]bool, error) {
	if err := ValidateQuery(query.Query); err != nil {
		return nil, err
	}
	if err := ValidateTags(query.Tags); err != nil {
		return nil, err
	}
	if query.TopK < 0 {
		return nil, fmt.Errorf("rag search top_k must not be negative")
	}
	if query.TopK > MaxSearchTopK {
		return nil, fmt.Errorf("rag search top_k exceeds %d", MaxSearchTopK)
	}
	queryTokens := Tokenize(query.Query)
	if len(queryTokens) == 0 {
		return nil, fmt.Errorf("rag query is empty")
	}
	return queryTokens, nil
}

func (k *KeywordIndex) Ingest(_ context.Context, scope core.ScopePath, document Document) error {
	if err := ValidateScope(scope); err != nil {
		return err
	}
	if err := ValidateDocument(document); err != nil {
		return err
	}
	document.Tags = append([]string(nil), document.Tags...)
	k.mu.Lock()
	defer k.mu.Unlock()
	key := scope.String()
	if _, exists := k.scopes[key]; !exists && len(k.scopes) >= k.scopeCap() {
		return fmt.Errorf("rag index exceeds maximum of %d scopes", k.scopeCap())
	}
	bucket := k.scopes[key]
	replaced := false
	for i, existing := range bucket {
		if existing.doc.ID == document.ID {
			bucket = append(bucket[:i], bucket[i+1:]...)
			replaced = true
			break
		}
	}
	if !replaced && len(bucket) >= k.documentCap() {
		return fmt.Errorf("rag scope exceeds maximum of %d documents", k.documentCap())
	}
	k.scopes[key] = append(bucket, indexedChunk{doc: document, tokens: Tokenize(document.Content)})
	return nil
}

func (k *KeywordIndex) documentCap() int {
	if k != nil && k.maxDocuments > 0 {
		return k.maxDocuments
	}
	return MaxDocumentsPerScope
}

func (k *KeywordIndex) scopeCap() int {
	if k != nil && k.maxScopes > 0 {
		return k.maxScopes
	}
	return MaxScopes
}

func (k *KeywordIndex) Search(_ context.Context, scope core.ScopePath, query Query) ([]Chunk, error) {
	if err := ValidateScope(scope); err != nil {
		return nil, err
	}
	queryTokens, err := ValidateSearch(query)
	if err != nil {
		return nil, err
	}
	wantTags := map[string]bool{}
	for _, tag := range query.Tags {
		wantTags[tag] = true
	}

	k.mu.RLock()
	defer k.mu.RUnlock()
	type rankedChunk struct {
		chunk     Chunk
		hierarchy int
	}
	hits := []rankedChunk{}
	for hierarchy, prefix := range scope.Prefixes() {
		for _, indexed := range k.scopes[prefix.String()] {
			if len(wantTags) > 0 {
				match := false
				for _, tag := range indexed.doc.Tags {
					if wantTags[tag] {
						match = true
						break
					}
				}
				if !match {
					continue
				}
			}
			overlap := 0.0
			for token := range queryTokens {
				if indexed.tokens[token] {
					overlap++
				}
			}
			if overlap == 0 {
				continue
			}
			hits = append(hits, rankedChunk{
				chunk: Chunk{
					ID: indexed.doc.ID, Source: indexed.doc.Source, Content: indexed.doc.Content,
					Score: overlap / float64(len(queryTokens)),
				},
				hierarchy: hierarchy,
			})
		}
	}
	sort.Slice(hits, func(i, j int) bool {
		if hits[i].chunk.Score != hits[j].chunk.Score {
			return hits[i].chunk.Score > hits[j].chunk.Score
		}
		if hits[i].hierarchy != hits[j].hierarchy {
			return hits[i].hierarchy < hits[j].hierarchy
		}
		return hits[i].chunk.ID < hits[j].chunk.ID
	})
	topK := query.TopK
	if topK == 0 {
		topK = 5
	}
	if len(hits) > topK {
		hits = hits[:topK]
	}
	out := make([]Chunk, len(hits))
	for i, hit := range hits {
		out[i] = hit.chunk
	}
	return out, nil
}

type SearchCapability struct {
	index           Index
	id              string
	requireAccepted bool
}

func (*SearchCapability) ArtifactRevision() string { return "rag-search/v3-keyword-contract" }

func NewSearchCapability(id string, index Index) (*SearchCapability, error) {
	return newSearchCapability(id, index, false)
}

// NewStandardSearchCapability creates the guarded variant intended for a
// general runtime composition. NewSearchCapability remains source-compatible
// for existing integrations that own their own invocation boundary.
func NewStandardSearchCapability(id string, index Index) (*SearchCapability, error) {
	return newSearchCapability(id, index, true)
}

func newSearchCapability(id string, index Index, requireAccepted bool) (*SearchCapability, error) {
	if err := support.ValidateCapabilityID(id); err != nil {
		return nil, err
	}
	if index == nil {
		return nil, fmt.Errorf("rag capability %q requires an index", id)
	}
	return &SearchCapability{index: index, id: id, requireAccepted: requireAccepted}, nil
}

func (r *SearchCapability) Manifest() core.CapabilityManifest {
	parameters := support.ObjectSchema(map[string]any{
		"query": map[string]any{"type": "string", "description": "Search query.", "minLength": 1, "maxLength": MaxQueryRunes},
		"top_k": map[string]any{"type": "integer", "description": "Maximum passages to return.", "minimum": 1, "maximum": MaxSearchTopK},
		"tags": map[string]any{
			"type": "array", "maxItems": MaxQueryTags,
			"items": map[string]any{"type": "string", "maxLength": MaxTagRunes},
		},
	})
	parameters["required"] = []any{"query"}
	return core.CapabilityManifest{
		ID: r.id, Version: "1.0.0", Name: r.id,
		Description:         "Search the knowledge base visible to this user and return the most relevant passages.",
		Kind:                core.KindRAG,
		Contract:            ContractSearchV1,
		RequiredPermissions: []core.Permission{core.PermRead},
		Tool:                &core.ToolExposure{Parameters: parameters},
	}
}

func (r *SearchCapability) Execute(ctx context.Context, request core.CapabilityRequest) (core.CapabilityResult, error) {
	if r.requireAccepted {
		if err := core.RequireAcceptedInvocation(request); err != nil {
			return core.CapabilityResult{}, err
		}
	}
	return r.execute(ctx, request)
}

// execute contains the capability behavior after the standard invocation gate.
// It is intentionally separate so the guarded variant can contain failures
// from an extension-owned Index without changing the legacy constructor's
// long-standing diagnostic behavior.
func (r *SearchCapability) execute(ctx context.Context, request core.CapabilityRequest) (core.CapabilityResult, error) {
	if err := core.ValidateArgs(r.Manifest().Tool.Parameters, request.Args); err != nil {
		return r.searchFailure(err), nil
	}
	query, _ := request.Args["query"].(string)
	topK := 0
	switch value := request.Args["top_k"].(type) {
	case float64:
		topK = int(value)
	case int:
		topK = value
	}
	tags := support.StringSlice(request.Args["tags"])
	if _, err := ValidateSearch(Query{Query: query, TopK: topK, Tags: tags}); err != nil {
		return r.searchFailure(err), nil
	}
	hits, err := r.search(ctx, request.Context.Principal.Scope, Query{
		Query: query, TopK: topK, Tags: tags,
	})
	if err != nil {
		return r.searchFailure(err), nil
	}
	return support.JSONResult(map[string]any{"chunks": hits})
}

func (r *SearchCapability) search(ctx context.Context, scope core.ScopePath, query Query) (hits []Chunk, err error) {
	if !r.requireAccepted {
		return r.index.Search(ctx, scope, query)
	}
	defer func() {
		if recover() != nil {
			hits, err = nil, errStandardSearch
		}
	}()
	hits, err = r.index.Search(ctx, scope, query)
	if err != nil {
		return nil, errStandardSearch
	}
	return hits, nil
}

func (r *SearchCapability) searchFailure(err error) core.CapabilityResult {
	if r.requireAccepted {
		return support.DeniedResult("rag_search_failed", "knowledge search is unavailable")
	}
	return support.DeniedResult("rag_search_failed", err.Error())
}
