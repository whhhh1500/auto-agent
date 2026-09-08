package rag

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	core "github.com/whhhh1500/auto-agent/pkg/core"
)

func TestStandardSearchCapabilityRequiresAcceptedInvocationWithoutChangingLegacyConstructor(t *testing.T) {
	index := NewKeywordIndex()
	standard, err := NewStandardSearchCapability("rag.search", index)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := standard.Execute(context.Background(), core.CapabilityRequest{}); !errors.Is(err, core.ErrAcceptedInvocationRequired) {
		t.Fatalf("standard gate error=%v", err)
	}
	legacy, err := NewSearchCapability("rag.legacy", index)
	if err != nil {
		t.Fatal(err)
	}
	result, err := legacy.Execute(context.Background(), core.CapabilityRequest{Args: map[string]any{"query": "x"}, Context: core.CapabilityContext{Principal: core.Principal{Scope: core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})}}})
	if err != nil || !result.OK {
		t.Fatalf("legacy constructor changed: result=%#v err=%v", result, err)
	}
}

func TestStandardSearchCapabilitySanitizesIndexErrorsAndPanics(t *testing.T) {
	scope := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	request := core.CapabilityRequest{
		Args:    map[string]any{"query": "match"},
		Context: core.CapabilityContext{Principal: core.Principal{Scope: scope}},
	}
	for name, index := range map[string]Index{
		"error": secretErrorIndex{},
		"panic": secretPanicIndex{},
	} {
		t.Run(name, func(t *testing.T) {
			standard, err := NewStandardSearchCapability("rag.search", index)
			if err != nil {
				t.Fatal(err)
			}
			// execute is the post-accepted-invocation path. The accepted gate is
			// separately covered above; this isolates extension failure handling.
			result, err := standard.execute(context.Background(), request)
			if err != nil || result.OK || result.Metadata["code"] != "rag_search_failed" || result.Content != "knowledge search is unavailable" || strings.Contains(result.Content, "TOP-SECRET") {
				t.Fatalf("standard failure leaked: result=%#v err=%v", result, err)
			}
		})
	}

	legacy, err := NewSearchCapability("rag.legacy", secretErrorIndex{})
	if err != nil {
		t.Fatal(err)
	}
	result, err := legacy.Execute(context.Background(), request)
	if err != nil || result.OK || !strings.Contains(result.Content, "TOP-SECRET") {
		t.Fatalf("legacy error compatibility changed: result=%#v err=%v", result, err)
	}
}

type secretErrorIndex struct{}

func (secretErrorIndex) Ingest(context.Context, core.ScopePath, Document) error { return nil }
func (secretErrorIndex) Search(context.Context, core.ScopePath, Query) ([]Chunk, error) {
	return nil, errors.New("TOP-SECRET credential material")
}

type secretPanicIndex struct{}

func (secretPanicIndex) Ingest(context.Context, core.ScopePath, Document) error { return nil }
func (secretPanicIndex) Search(context.Context, core.ScopePath, Query) ([]Chunk, error) {
	panic("TOP-SECRET credential material")
}

func uniqueTokenQuery(count int) string {
	tokens := make([]string, count)
	for i := range tokens {
		tokens[i] = fmt.Sprintf("token-%d", i)
	}
	return strings.Join(tokens, " ")
}

func repeatedTags(count int) []string {
	tags := make([]string, count)
	for i := range tags {
		tags[i] = "duplicate"
	}
	return tags
}

func stringsAsAny(values []string) []any {
	out := make([]any, len(values))
	for i, value := range values {
		out[i] = value
	}
	return out
}

func TestTokenizeUsesOnlyFixedCJKFullwidthSeparators(t *testing.T) {
	for _, separator := range cjkFullwidthTokenSeparators {
		t.Run(string(separator), func(t *testing.T) {
			if got := Tokenize("alpha" + string(separator) + "bravo"); !reflect.DeepEqual(got, map[string]bool{"alpha": true, "bravo": true}) {
				t.Fatalf("tokens for separator %q = %#v", separator, got)
			}
		})
	}

	for text, want := range map[string]map[string]bool{
		"用户，保留凭证":                              {"用户": true, "保留凭证": true},
		"（保留凭证）":                               {"保留凭证": true},
		"用户保留凭证":                               {"用户保留凭证": true},
		"retry_policy_v2 owner@example.com":    {"retry_policy_v2": true, "owner@example.com": true},
		"https://api.example.com/v1?mode=fast": {"https://api.example.com/v1?mode=fast": true},
		"l’été retention":                      {"l’été": true, "retention": true},
		"...alpha, bravo!?":                    {"alpha": true, "bravo": true},
	} {
		if got := Tokenize(text); !reflect.DeepEqual(got, want) {
			t.Fatalf("tokens for %q = %#v, want %#v", text, got, want)
		}
	}
}

func cjkSeparatedTokenText(count int) string {
	parts := make([]string, count)
	for i := range parts {
		parts[i] = fmt.Sprintf("token-%d", i)
	}
	return strings.Join(parts, "，")
}

func TestValidateDocumentCountsCJKFullwidthSeparatedTokens(t *testing.T) {
	for name, count := range map[string]int{
		"at cap":   MaxDocumentTokens,
		"over cap": MaxDocumentTokens + 1,
	} {
		t.Run(name, func(t *testing.T) {
			err := ValidateDocument(Document{ID: "cjk-token-cap", Content: cjkSeparatedTokenText(count)})
			if count == MaxDocumentTokens && err != nil {
				t.Fatalf("document at CJK-separated token cap rejected: %v", err)
			}
			if count > MaxDocumentTokens && err == nil {
				t.Fatal("document above CJK-separated token cap was accepted")
			}
		})
	}
}

func legacyTokenizeForBenchmark(text string) map[string]bool {
	tokens := map[string]bool{}
	for _, field := range strings.Fields(strings.ToLower(text)) {
		tokens[strings.Trim(field, ".,;:!?")] = true
	}
	delete(tokens, "")
	return tokens
}

func BenchmarkTokenize(b *testing.B) {
	inputs := map[string]string{
		"ascii_document": strings.Repeat("retention policy evidence billing deployment ", 24),
		"cjk_document":   strings.Repeat("用户，保留凭证；审计记录。", 48),
		"cjk_query":      "保留凭证",
	}
	for name, text := range inputs {
		b.Run("legacy/"+name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = legacyTokenizeForBenchmark(text)
			}
		})
		b.Run("v47/"+name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = Tokenize(text)
			}
		})
	}
}

func TestValidateQueryAndTagsUseRuneAndRawItemLimits(t *testing.T) {
	if err := ValidateQuery(strings.Repeat("界", MaxQueryRunes)); err != nil {
		t.Fatalf("query at rune limit rejected: %v", err)
	}
	if err := ValidateQuery(strings.Repeat("界", MaxQueryRunes+1)); err == nil {
		t.Fatal("query above rune limit was accepted")
	}
	if err := ValidateQuery(uniqueTokenQuery(MaxQueryTokens)); err != nil {
		t.Fatalf("query at token limit rejected: %v", err)
	}
	if err := ValidateQuery(uniqueTokenQuery(MaxQueryTokens + 1)); err == nil {
		t.Fatal("query above unique token limit was accepted")
	}
	if err := ValidateQuery(strings.TrimSpace(strings.Repeat("duplicate ", MaxQueryTokens+1))); err != nil {
		t.Fatalf("duplicate query tokens must not count toward the unique token limit: %v", err)
	}

	if err := ValidateTag(strings.Repeat("界", MaxTagRunes)); err != nil {
		t.Fatalf("tag at rune limit rejected: %v", err)
	}
	if err := ValidateTag(strings.Repeat("界", MaxTagRunes+1)); err == nil {
		t.Fatal("tag above rune limit was accepted")
	}
	if err := ValidateTags(make([]string, MaxQueryTags)); err != nil {
		t.Fatalf("empty tags at item limit rejected: %v", err)
	}
	if err := ValidateTags(repeatedTags(MaxQueryTags + 1)); err == nil {
		t.Fatal("duplicate tags above raw item limit were accepted")
	}
}

func TestKeywordIndexEnforcesBoundedSearchAndIngest(t *testing.T) {
	scope := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	index := NewKeywordIndex()
	if err := index.Ingest(context.Background(), scope, Document{
		ID: "unicode-tag", Content: "match", Tags: []string{strings.Repeat("界", MaxTagRunes)},
	}); err != nil {
		t.Fatalf("document tag at rune limit rejected: %v", err)
	}
	if err := index.Ingest(context.Background(), scope, Document{
		ID: "empty-tag", Content: "match", Tags: []string{""},
	}); err != nil {
		t.Fatalf("empty document tag rejected: %v", err)
	}
	if err := index.Ingest(context.Background(), scope, Document{
		ID: "oversized-tag", Content: "match", Tags: []string{strings.Repeat("界", MaxTagRunes+1)},
	}); err == nil {
		t.Fatal("document tag above rune limit was accepted")
	}

	for name, query := range map[string]Query{
		"exact query rune limit": {Query: strings.Repeat("界", MaxQueryRunes)},
		"exact tag rune limit":   {Query: "match", Tags: []string{strings.Repeat("界", MaxTagRunes)}},
		"exact token limit":      {Query: uniqueTokenQuery(MaxQueryTokens)},
		"maximum top k":          {Query: "match", TopK: MaxSearchTopK},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := index.Search(context.Background(), scope, query); err != nil {
				t.Fatalf("search at exact limit rejected: %v", err)
			}
		})
	}

	for name, query := range map[string]Query{
		"oversized query":         {Query: strings.Repeat("界", MaxQueryRunes+1)},
		"oversized tag":           {Query: "match", Tags: []string{strings.Repeat("界", MaxTagRunes+1)}},
		"too many duplicate tags": {Query: "match", Tags: repeatedTags(MaxQueryTags + 1)},
		"too many unique tokens":  {Query: uniqueTokenQuery(MaxQueryTokens + 1)},
		"negative top k":          {Query: "match", TopK: -1},
		"oversized top k":         {Query: "match", TopK: MaxSearchTopK + 1},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := index.Search(context.Background(), scope, query); err == nil {
				t.Fatal("out-of-bounds search was accepted")
			}
		})
	}
}

func TestSearchCapabilityPublishesBoundedToolSchema(t *testing.T) {
	capability, err := NewSearchCapability("crypto.kb.search", NewKeywordIndex())
	if err != nil {
		t.Fatal(err)
	}
	if got := capability.ArtifactRevision(); got != "rag-search/v3-keyword-contract" {
		t.Fatalf("artifact revision = %q", got)
	}
	manifest := capability.Manifest()
	if manifest.Tool == nil {
		t.Fatal("search capability has no tool exposure")
	}
	schema := manifest.Tool.Parameters
	valid := map[string]any{
		"query": strings.Repeat("界", MaxQueryRunes),
		"top_k": float64(MaxSearchTopK),
		"tags":  []any{strings.Repeat("界", MaxTagRunes)},
	}
	if err := core.ValidateArgs(schema, valid); err != nil {
		t.Fatalf("tool schema rejected exact limits: %v", err)
	}

	for name, args := range map[string]map[string]any{
		"query is required":       {},
		"empty query":             {"query": ""},
		"oversized unicode query": {"query": strings.Repeat("界", MaxQueryRunes+1)},
		"too many tags":           {"query": "match", "tags": stringsAsAny(repeatedTags(MaxQueryTags + 1))},
		"oversized unicode tag":   {"query": "match", "tags": []any{strings.Repeat("界", MaxTagRunes+1)}},
		"top k below minimum":     {"query": "match", "top_k": float64(0)},
		"top k above maximum":     {"query": "match", "top_k": float64(MaxSearchTopK + 1)},
	} {
		t.Run(name, func(t *testing.T) {
			if err := core.ValidateArgs(schema, args); err == nil {
				t.Fatal("out-of-bounds tool arguments were accepted")
			}
		})
	}
}

func TestKeywordIndexUsesDeterministicScopeAndIDTieOrder(t *testing.T) {
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	product, _ := global.Child(core.ScopeRef{Kind: core.ScopeProduct, ID: "product"})
	tenant, _ := product.Child(core.ScopeRef{Kind: core.ScopeTenant, ID: "acme"})
	user, _ := tenant.Child(core.ScopeRef{Kind: core.ScopeUser, ID: "alice"})
	index := NewKeywordIndex()
	for _, row := range []struct {
		scope core.ScopePath
		id    string
	}{
		{tenant, "b-target"},
		{global, "z-root"},
		{tenant, "a-target"},
	} {
		if err := index.Ingest(context.Background(), row.scope, Document{ID: row.id, Content: "tie"}); err != nil {
			t.Fatal(err)
		}
	}
	hits, err := index.Search(context.Background(), user, Query{Query: "tie"})
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]string, len(hits))
	for i, hit := range hits {
		ids[i] = hit.ID
	}
	if !reflect.DeepEqual(ids, []string{"z-root", "a-target", "b-target"}) {
		t.Fatalf("tie order = %#v", ids)
	}
}

func TestKeywordIndexDefaultTopKIsFive(t *testing.T) {
	scope := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	index := NewKeywordIndex()
	for _, id := range []string{"f", "e", "d", "c", "b", "a"} {
		if err := index.Ingest(context.Background(), scope, Document{ID: id, Content: "match"}); err != nil {
			t.Fatal(err)
		}
	}
	hits, err := index.Search(context.Background(), scope, Query{Query: "match", TopK: 0})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 5 || hits[0].ID != "a" || hits[4].ID != "e" {
		t.Fatalf("default topK hits = %#v", hits)
	}
}

func TestKeywordIndexRejectsEmptyScopeControlIDsAndNUL(t *testing.T) {
	scope := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	index := NewKeywordIndex()
	ctx := context.Background()
	if err := index.Ingest(ctx, core.ScopePath{}, Document{ID: "doc", Content: "match"}); err == nil {
		t.Fatal("empty rag scope ingest was accepted")
	}
	if _, err := index.Search(ctx, core.ScopePath{}, Query{Query: "match"}); err == nil {
		t.Fatal("empty rag scope search was accepted")
	}
	if err := index.Ingest(ctx, scope, Document{ID: "doc\n", Content: "match"}); err == nil {
		t.Fatal("control character document id was accepted")
	}
	if err := index.Ingest(ctx, scope, Document{ID: strings.Repeat("a", MaxDocumentIDBytes+1), Content: "match"}); err == nil {
		t.Fatal("oversized document id was accepted")
	}
	if err := index.Ingest(ctx, scope, Document{ID: "doc", Source: "src\t", Content: "match"}); err == nil {
		t.Fatal("control character document source was accepted")
	}
	if err := index.Ingest(ctx, scope, Document{ID: "doc", Source: strings.Repeat("s", MaxDocumentSourceBytes+1), Content: "match"}); err == nil {
		t.Fatal("oversized document source was accepted")
	}
	if err := index.Ingest(ctx, scope, Document{ID: "nul", Content: "ma\x00tch"}); err == nil {
		t.Fatal("NUL document content was accepted")
	}
	if _, err := index.Search(ctx, scope, Query{Query: "ma\x00tch"}); err == nil {
		t.Fatal("NUL rag query was accepted")
	}
	if err := index.Ingest(ctx, scope, Document{ID: "nul-tag", Content: "match", Tags: []string{"ta\x00g"}}); err == nil {
		t.Fatal("NUL document tag was accepted")
	}
	if err := index.Ingest(ctx, scope, Document{ID: "newline", Content: "line1\nline2"}); err != nil {
		t.Fatalf("newline document content was rejected: %v", err)
	}
	hits, err := index.Search(ctx, scope, Query{Query: "line1"})
	if err != nil || len(hits) != 1 || hits[0].ID != "newline" {
		t.Fatalf("newline document search = %#v err=%v", hits, err)
	}
}

func TestKeywordIndexRejectsDocumentTokenOverflow(t *testing.T) {
	index := NewKeywordIndex()
	scope := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	ctx := context.Background()
	if err := index.Ingest(ctx, scope, Document{ID: "at-cap", Content: uniqueTokenQuery(MaxDocumentTokens)}); err != nil {
		t.Fatalf("document at unique token limit rejected: %v", err)
	}
	if err := index.Ingest(ctx, scope, Document{ID: "over", Content: uniqueTokenQuery(MaxDocumentTokens + 1)}); err == nil {
		t.Fatal("document above unique token limit was accepted")
	}
	if err := index.Ingest(ctx, scope, Document{
		ID: "duplicates", Content: strings.TrimSpace(strings.Repeat("duplicate ", MaxDocumentTokens+1)),
	}); err != nil {
		t.Fatalf("duplicate document tokens must not count toward the unique token limit: %v", err)
	}
	if err := index.Ingest(ctx, scope, Document{ID: "at-cap", Content: uniqueTokenQuery(MaxDocumentTokens + 1)}); err == nil {
		t.Fatal("replacing an existing document above the unique token limit was accepted")
	}
	if err := index.Ingest(ctx, scope, Document{ID: "at-cap", Content: uniqueTokenQuery(MaxDocumentTokens)}); err != nil {
		t.Fatalf("replacing an existing document at the unique token limit rejected: %v", err)
	}
}

func TestKeywordIndexRejectsScopeOverflow(t *testing.T) {
	index := NewKeywordIndex()
	index.maxDocuments = 2
	scope := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	ctx := context.Background()
	if err := index.Ingest(ctx, scope, Document{ID: "a", Content: "one alpha"}); err != nil {
		t.Fatal(err)
	}
	if err := index.Ingest(ctx, scope, Document{ID: "b", Content: "two beta"}); err != nil {
		t.Fatal(err)
	}
	if err := index.Ingest(ctx, scope, Document{ID: "c", Content: "three gamma"}); err == nil {
		t.Fatal("rag scope overflow was accepted")
	}
	if err := index.Ingest(ctx, scope, Document{ID: "a", Content: "one alpha updated"}); err != nil {
		t.Fatalf("replacing an existing document must not count as overflow: %v", err)
	}
}

func TestKeywordIndexRejectsTooManyScopes(t *testing.T) {
	index := NewKeywordIndex()
	index.maxScopes = 2
	ctx := context.Background()
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	first, err := global.Child(core.ScopeRef{Kind: core.ScopeProduct, ID: "one"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := global.Child(core.ScopeRef{Kind: core.ScopeProduct, ID: "two"})
	if err != nil {
		t.Fatal(err)
	}
	third, err := global.Child(core.ScopeRef{Kind: core.ScopeProduct, ID: "three"})
	if err != nil {
		t.Fatal(err)
	}
	if err := index.Ingest(ctx, first, Document{ID: "a", Content: "one alpha"}); err != nil {
		t.Fatal(err)
	}
	if err := index.Ingest(ctx, second, Document{ID: "b", Content: "two beta"}); err != nil {
		t.Fatal(err)
	}
	if err := index.Ingest(ctx, third, Document{ID: "c", Content: "three gamma"}); err == nil {
		t.Fatal("rag index scope overflow was accepted")
	}
	if err := index.Ingest(ctx, first, Document{ID: "a", Content: "one alpha updated"}); err != nil {
		t.Fatalf("replacing an existing scope must not count as overflow: %v", err)
	}
}
