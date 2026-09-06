package core

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"unicode"
)

func TestDiscloseHidesNonHotToolsAndSearchesByMeaning(t *testing.T) {
	_, _, _, user := testScopes()
	principal := testPrincipal(user)
	registry := NewCapabilityRegistry()
	mail := toolManifest("notify.send", "1.0.0")
	mail.Description = "Send an email to an address"
	mail.Tool = &ToolExposure{Description: "Send an email to an address", Parameters: map[string]any{"type": "object"}}
	slack := toolManifest("chat.send", "1.0.0")
	slack.Description = "Send a message to a Slack channel"
	slack.Tool = &ToolExposure{Description: "Send a message to a Slack channel", Parameters: map[string]any{"type": "object"}}
	if err := registry.Register(user, staticTool{manifest: mail, content: "email-sent"}); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(user, staticTool{manifest: slack, content: "slack-sent"}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := (CapabilityResolver{Registry: registry}).Resolve(principal, user)
	if err != nil {
		t.Fatal(err)
	}
	wrapped := discloseToolRuntime(snapshot)
	if wrapped == snapshot {
		t.Fatal("expected a disclosed runtime")
	}
	for _, schema := range wrapped.Schemas() {
		if schema.Name == "notify.send" || schema.Name == "chat.send" {
			t.Fatalf("hidden tool leaked into model schemas: %#v", schema)
		}
	}
	foundLibrary := false
	for _, schema := range wrapped.Schemas() {
		if schema.Name == disclosedLibraryID {
			foundLibrary = true
		}
	}
	if !foundLibrary {
		t.Fatal("tool library was not disclosed")
	}
	searched, err := wrapped.Execute(context.Background(), ToolCall{
		ID: "c1", Name: disclosedLibraryID,
		Args: map[string]any{"action": "search", "query": "send slack channel"},
	})
	if err != nil || !searched.OK {
		t.Fatalf("search failed: %#v %v", searched, err)
	}
	if !strings.Contains(searched.Content, `"id":"chat.send"`) || strings.Contains(searched.Content, `"id":"notify.send"`) {
		t.Fatalf("semantic search leaked the email tool as a hit: %s", searched.Content)
	}
	if searched.Metadata["library.action"] != "search" {
		t.Fatalf("search is not observable: %#v", searched.Metadata)
	}
	described, err := wrapped.Execute(context.Background(), ToolCall{
		ID: "c2", Name: disclosedLibraryID,
		Args: map[string]any{"action": "describe", "name": "send"},
	})
	if err != nil || !described.OK || !strings.Contains(described.Content, `"status":"explore"`) {
		t.Fatalf("shared short name should stay explorable: %#v %v", described, err)
	}
	called, err := wrapped.Execute(context.Background(), ToolCall{ID: "c3", Name: "chat.send"})
	if err != nil || !called.OK || called.Content != "slack-sent" {
		t.Fatalf("calling a discovered id should still execute: %#v %v", called, err)
	}
}

func TestDiscloseLeavesSmallHotCatalogAlone(t *testing.T) {
	_, _, _, user := testScopes()
	principal := testPrincipal(user)
	registry := NewCapabilityRegistry()
	memory := toolManifest("user.memory", "1.0.0")
	memory.Kind = KindMemory
	if err := registry.Register(user, staticTool{manifest: memory, content: "ok"}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := (CapabilityResolver{Registry: registry}).Resolve(principal, user)
	if err != nil {
		t.Fatal(err)
	}
	if discloseToolRuntime(snapshot) != snapshot {
		t.Fatal("a hot-only catalog should not wrap")
	}
}

func TestDiscloseSiblingNotForExcludesWrongSense(t *testing.T) {
	_, _, _, user := testScopes()
	principal := testPrincipal(user)
	registry := NewCapabilityRegistry()
	mail := toolManifest("notify.send", "1.0.0")
	mail.Tool = &ToolExposure{Description: "Send an email to an address", Parameters: map[string]any{"type": "object"}}
	slack := toolManifest("chat.send", "1.0.0")
	slack.Tool = &ToolExposure{Description: "Send a message to a Slack channel", Parameters: map[string]any{"type": "object"}}
	if err := registry.Register(user, staticTool{manifest: mail, content: "email-sent"}); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(user, staticTool{manifest: slack, content: "slack-sent"}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := (CapabilityResolver{Registry: registry}).Resolve(principal, user)
	if err != nil {
		t.Fatal(err)
	}
	wrapped := discloseToolRuntime(snapshot)
	result, err := wrapped.Execute(context.Background(), ToolCall{
		ID: "c1", Name: disclosedLibraryID,
		Args: map[string]any{"action": "search", "query": "send email address"},
	})
	if err != nil || !result.OK {
		t.Fatalf("search failed: %#v %v", result, err)
	}
	if !strings.Contains(result.Content, `"id":"notify.send"`) || strings.Contains(result.Content, `"id":"chat.send"`) {
		t.Fatalf("sibling NotFor failed: %s", result.Content)
	}
}

func TestDiscloseSummaryBoundedPreservesUnicodeAndControls(t *testing.T) {
	tests := []struct {
		name, input, want string
	}{
		{name: "short", input: "a\nb\tc\x00d🙂", want: "a b cd🙂"},
		{name: "bounded", input: strings.Repeat("界", discloseSummaryRunes+1), want: strings.Repeat("界", discloseSummaryRunes)},
		{name: "controls-do-not-count", input: strings.Repeat("界", discloseSummaryRunes-1) + "\x00🙂", want: strings.Repeat("界", discloseSummaryRunes-1) + "🙂"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := discloseSummary(test.input); got != test.want {
				t.Fatalf("summary = %q, want %q", got, test.want)
			}
		})
	}
}

func TestAgentToolDisclosureIsOptIn(t *testing.T) {
	_, _, _, user := testScopes()
	principal := testPrincipal(user)
	registry := NewCapabilityRegistry()
	mail := toolManifest("notify.send", "1.0.0")
	mail.Tool = &ToolExposure{Description: "Send an email", Parameters: map[string]any{"type": "object"}}
	chat := toolManifest("chat.send", "1.0.0")
	chat.Tool = &ToolExposure{Description: "Send a Slack message", Parameters: map[string]any{"type": "object"}}
	if err := registry.Register(user, staticTool{manifest: mail, content: "email-sent"}); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(user, staticTool{manifest: chat, content: "slack-sent"}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := (CapabilityResolver{Registry: registry}).Resolve(principal, user)
	if err != nil {
		t.Fatal(err)
	}
	session := mustSession(t, user, principal)

	direct, err := NewAgent(AgentOptions{LLM: MockLlmAdapter{}, Tools: snapshot, Session: session})
	if err != nil {
		t.Fatal(err)
	}
	if hasToolSchema(direct.opts.Tools.Schemas(), disclosedLibraryID) ||
		!hasToolSchema(direct.opts.Tools.Schemas(), "notify.send") ||
		!hasToolSchema(direct.opts.Tools.Schemas(), "chat.send") {
		t.Fatalf("default agent tool schemas changed: %#v", direct.opts.Tools.Schemas())
	}

	disclosed, err := NewAgent(AgentOptions{
		LLM: MockLlmAdapter{}, Tools: snapshot, Session: session, DiscloseTools: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !hasToolSchema(disclosed.opts.Tools.Schemas(), disclosedLibraryID) ||
		hasToolSchema(disclosed.opts.Tools.Schemas(), "notify.send") ||
		hasToolSchema(disclosed.opts.Tools.Schemas(), "chat.send") {
		t.Fatalf("explicit disclosure schemas wrong: %#v", disclosed.opts.Tools.Schemas())
	}
	searched, err := disclosed.opts.Tools.Execute(context.Background(), ToolCall{
		ID: "search", Name: disclosedLibraryID,
		Args: map[string]any{"action": "search", "query": "send Slack message"},
	})
	if err != nil || !searched.OK || !strings.Contains(searched.Content, `"id":"chat.send"`) {
		t.Fatalf("explicit library search failed: %#v %v", searched, err)
	}
	described, err := disclosed.opts.Tools.Execute(context.Background(), ToolCall{
		ID: "describe", Name: disclosedLibraryID,
		Args: map[string]any{"action": "describe", "id": "chat.send"},
	})
	if err != nil || !described.OK || !strings.Contains(described.Content, `"id":"chat.send"`) {
		t.Fatalf("explicit library describe failed: %#v %v", described, err)
	}
	called, err := disclosed.opts.Tools.Execute(context.Background(), ToolCall{ID: "call", Name: "chat.send"})
	if err != nil || !called.OK || called.Content != "slack-sent" {
		t.Fatalf("discovered direct call failed: %#v %v", called, err)
	}
}

func TestMockLlmAdapterSkipsUnknownRequiredToolArguments(t *testing.T) {
	chunks := []StreamChunk{}
	err := (MockLlmAdapter{}).Stream(context.Background(), GenerateOptions{
		Messages: []ChatMessage{{Role: RoleUser, Content: "test"}},
		Tools: []ToolSchema{{
			Name: "requires.args", Parameters: map[string]any{
				"type": "object", "required": []any{"action"},
			},
		}},
	}, func(chunk StreamChunk) {
		chunks = append(chunks, chunk)
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, chunk := range chunks {
		if chunk.ToolCall != nil {
			t.Fatalf("mock called a tool with unknown required arguments: %#v", chunk.ToolCall)
		}
	}
}

func TestMockLlmAdapterUsesValidLibraryAction(t *testing.T) {
	chunks := []StreamChunk{}
	err := (MockLlmAdapter{}).Stream(context.Background(), GenerateOptions{
		Messages: []ChatMessage{{Role: RoleUser, Content: "test"}},
		Tools:    []ToolSchema{{Name: disclosedLibraryID, Parameters: discloseLibrarySchema()}},
	}, func(chunk StreamChunk) {
		chunks = append(chunks, chunk)
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, chunk := range chunks {
		if chunk.ToolCall != nil {
			if chunk.ToolCall.Args["action"] != "list" {
				t.Fatalf("mock library call missing valid action: %#v", chunk.ToolCall)
			}
			return
		}
	}
	t.Fatal("mock did not call the library")
}

func hasToolSchema(schemas []ToolSchema, name string) bool {
	for _, schema := range schemas {
		if schema.Name == name {
			return true
		}
	}
	return false
}

func TestLibraryIndexSearchIsConcurrentAndUsesStableSnapshot(t *testing.T) {
	docs := []libraryDoc{{ID: "image.render", Name: "render", Description: "render an image"}}
	index := newLibraryIndex(docs)
	if len(index.docs[0].nameTokens) == 0 || len(index.docs[0].descTokens) == 0 {
		t.Fatal("library index did not precompute metadata tokens")
	}
	t.Run("parallel", func(t *testing.T) {
		t.Parallel()
		for i := 0; i < 100; i++ {
			hits := index.search("render image", 8)
			if len(hits) != 1 || hits[0].ID != "image.render" {
				t.Fatalf("unexpected concurrent search result: %#v", hits)
			}
		}
	})
	updated := newLibraryIndex([]libraryDoc{{ID: "video.render", Name: "render", Description: "render a video"}})
	if hits := updated.search("video render", 8); len(hits) != 1 || hits[0].ID != "video.render" {
		t.Fatalf("updated index did not replace old metadata: %#v", hits)
	}
	if hits := index.search("video", 8); len(hits) != 0 {
		t.Fatalf("old index was mutated by replacement: %#v", hits)
	}
}

func BenchmarkLibrarySearchIndexedVsLegacy(b *testing.B) {
	docs := make([]libraryDoc, 0, 500)
	for i := 0; i < 500; i++ {
		docs = append(docs, libraryDoc{
			ID: fmt.Sprintf("tool.%03d", i), Name: fmt.Sprintf("render-%03d", i),
			Description: "render an image with a bounded output",
		})
	}
	index := newLibraryIndex(docs)
	b.Run("indexed", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = index.search("render image", 8)
		}
	})
	b.Run("legacy-tokenize-full-catalog", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = legacySearchLibraryDocsForBenchmark(docs, "render image", 8)
		}
	})
}

func BenchmarkDisclosedSchemasHotOnlyVsFullSnapshot(b *testing.B) {
	snapshot := benchmarkCatalogSnapshot(500)
	disclosed := discloseToolRuntime(snapshot).(*disclosedRuntime)
	hot := map[string]bool{"memory.hot": true}
	b.Run("hot-only-lazy", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = disclosed.Schemas()
		}
	})
	b.Run("legacy-copy-all-then-filter", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			all := snapshot.Schemas()
			out := make([]ToolSchema, 0, len(all))
			for _, schema := range all {
				if hot[schema.Name] {
					out = append(out, schema)
				}
			}
			_ = out
		}
	})
}

func BenchmarkDiscloseSummaryBoundedVsLegacy(b *testing.B) {
	input := strings.Repeat("long description with unicode 界\n", 1024)
	b.Run("bounded", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = discloseSummary(input)
		}
	})
	b.Run("legacy-full-clean-and-runes", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = legacyDiscloseSummaryForBenchmark(input)
		}
	})
}

func legacyDiscloseSummaryForBenchmark(text string) string {
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
	if len(runes) <= discloseSummaryRunes {
		return cleaned
	}
	return string(runes[:discloseSummaryRunes])
}

func benchmarkCatalogSnapshot(count int) *CapabilitySnapshot {
	capabilities := make([]SnapshotCapability, 0, count+1)
	providers := make(map[string]any, count+1)
	for i := 0; i < count; i++ {
		id := fmt.Sprintf("tool.%03d", i)
		manifest := CapabilityManifest{ID: id, Version: "1.0.0", Kind: KindConnector,
			Tool: &ToolExposure{Description: "catalog tool", Parameters: map[string]any{
				"type": "object", "properties": map[string]any{"value": map[string]any{"type": "string"}},
			}}}
		capabilities = append(capabilities, SnapshotCapability{Manifest: manifest})
		providers[id] = staticTool{manifest: manifest}
	}
	hotManifest := CapabilityManifest{ID: "memory.hot", Version: "1.0.0", Kind: KindMemory,
		Tool: &ToolExposure{Description: "hot tool", Parameters: map[string]any{"type": "object"}}}
	capabilities = append(capabilities, SnapshotCapability{Manifest: hotManifest})
	providers[hotManifest.ID] = staticTool{manifest: hotManifest}
	return &CapabilitySnapshot{capabilities: capabilities, providers: providers}
}

func legacySearchLibraryDocsForBenchmark(docs []libraryDoc, query string, topK int) []libraryHit {
	queryTokens := discloseQueryTokens(query)
	byName := map[string][]string{}
	for _, doc := range docs {
		byName[doc.Name] = append(byName[doc.Name], doc.ID)
	}
	hits := []libraryHit{}
	for _, doc := range docs {
		nameTokens := discloseTokens(doc.Name)
		descTokens := discloseTokens(doc.Description + " " + strings.Join(doc.Triggers, " "))
		notTokens := discloseTokens(strings.Join(doc.NotFor, " "))
		docTokens := discloseMergeTokens(nameTokens, descTokens)
		nameScore := discloseOverlap(queryTokens, nameTokens)
		descScore := discloseOverlap(queryTokens, descTokens)
		covered := discloseOverlap(queryTokens, docTokens)
		notScore := discloseOverlap(queryTokens, notTokens)
		if notScore > 0 && notScore >= descScore && notScore >= nameScore || covered <= 0 {
			continue
		}
		same := []string{}
		for _, other := range byName[doc.Name] {
			if other != doc.ID {
				same = append(same, other)
			}
		}
		hits = append(hits, libraryHit{ID: doc.ID, Library: doc.Library, Name: doc.Name,
			Description: discloseSummary(doc.Description), Score: descScore*3 + nameScore,
			Ambiguous: len(same) > 0, SameName: same})
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
