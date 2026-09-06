package toollib

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"unicode"
)

func TestCatalogApplyAddsUpdatesDeletesAndSkipsUnchanged(t *testing.T) {
	cat := NewCatalog()
	mail := Record{Library: "mcp.mail", Name: "send", Description: "Send an email"}
	slack := Record{Library: "mcp.slack", Name: "send", Description: "Send a Slack message"}
	stats, err := cat.Apply([]Record{mail, slack})
	if err != nil || stats.Added != 2 {
		t.Fatalf("first apply = %+v err=%v", stats, err)
	}
	if cat.Len() != 2 {
		t.Fatalf("len = %d", cat.Len())
	}
	same, err := cat.Apply([]Record{mail, slack})
	if err != nil || same.Unchanged != 2 || same.Added != 0 || same.Updated != 0 {
		t.Fatalf("unchanged apply = %+v err=%v", same, err)
	}
	mail.Description = "Send an email to an address"
	updated, err := cat.Apply([]Record{mail, slack})
	if err != nil || updated.Updated != 1 || updated.Unchanged != 1 {
		t.Fatalf("description change = %+v err=%v", updated, err)
	}
	removed, err := cat.Apply([]Record{slack})
	if err != nil || removed.Removed != 1 || cat.Len() != 1 {
		t.Fatalf("delete missing = %+v len=%d err=%v", removed, cat.Len(), err)
	}
	if _, ok := cat.Get("mcp.mail/send"); ok {
		t.Fatal("removed id still present")
	}
	if _, err := cat.Apply(nil); err == nil {
		t.Fatal("empty listing wiped the catalog")
	}
	if cat.Len() != 1 {
		t.Fatalf("empty apply must keep previous catalog, len=%d", cat.Len())
	}
}

func TestCatalogSearchIndexRefreshesAndSupportsConcurrentReaders(t *testing.T) {
	cat := NewCatalog()
	if _, err := cat.Apply([]Record{
		{ID: "tool.email", Name: "send", Description: "send an email"},
		{ID: "tool.slack", Name: "send", Description: "send a Slack message"},
	}); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				hits := cat.Search("email", 8)
				if len(hits) != 1 || hits[0].ID != "tool.email" {
					t.Errorf("concurrent search = %#v", hits)
					return
				}
			}
		}()
	}
	wg.Wait()
	if _, err := cat.Apply([]Record{{ID: "tool.weather", Name: "forecast", Description: "weather forecast"}}); err != nil {
		t.Fatal(err)
	}
	if hits := cat.Search("weather forecast", 8); len(hits) != 1 || hits[0].ID != "tool.weather" {
		t.Fatalf("updated index = %#v", hits)
	}
	if hits := cat.Search("email", 8); len(hits) != 0 {
		t.Fatalf("stale index hit after replacement = %#v", hits)
	}
}

func BenchmarkCatalogSearchIndexedVsLegacy(b *testing.B) {
	records := make([]Record, 0, 500)
	for i := 0; i < 500; i++ {
		records = append(records, Record{ID: fmt.Sprintf("tool.%03d", i), Name: fmt.Sprintf("render-%03d", i), Description: "render an image with bounded output"})
	}
	cat := NewCatalog()
	if _, err := cat.Apply(records); err != nil {
		b.Fatal(err)
	}
	b.Run("indexed", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = cat.Search("render image", 8)
		}
	})
	b.Run("legacy-snapshot-and-tokenize", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = SearchRecords(records, "render image", 8)
		}
	})
}

func TestSummaryBoundedPreservesUnicodeAndControls(t *testing.T) {
	tests := []struct {
		name, input, want string
	}{
		{name: "short", input: "a\nb\tc\x00d🙂", want: "a b cd🙂"},
		{name: "bounded", input: strings.Repeat("界", MaxSummaryRunes+1), want: strings.Repeat("界", MaxSummaryRunes)},
		{name: "controls-do-not-count", input: strings.Repeat("界", MaxSummaryRunes-1) + "\x00🙂", want: strings.Repeat("界", MaxSummaryRunes-1) + "🙂"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := summary(test.input); got != test.want {
				t.Fatalf("summary = %q, want %q", got, test.want)
			}
		})
	}
}

func BenchmarkSummaryBoundedVsLegacy(b *testing.B) {
	input := strings.Repeat("long description with unicode 界\n", 1024)
	b.Run("bounded", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = summary(input)
		}
	})
	b.Run("legacy-full-clean-and-runes", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = legacySummaryForBenchmark(input)
		}
	})
}

func legacySummaryForBenchmark(text string) string {
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
	if len(runes) <= MaxSummaryRunes {
		return cleaned
	}
	return string(runes[:MaxSummaryRunes])
}

func TestCatalogSearchPrefersDescriptionOverSharedName(t *testing.T) {
	cat := NewCatalog()
	if _, err := cat.Apply([]Record{
		{Library: "mcp.mail", Name: "send", Description: "Send an email to an address"},
		{Library: "mcp.slack", Name: "send", Description: "Send a message to a Slack channel"},
	}); err != nil {
		t.Fatal(err)
	}
	hits := cat.Search("send slack channel message", 8)
	if len(hits) == 0 || hits[0].ID != "mcp.slack/send" {
		t.Fatalf("hits = %#v", hits)
	}
	for _, hit := range hits {
		if hit.ID == "mcp.mail/send" {
			t.Fatalf("email send ranked for slack intent: %#v", hits)
		}
	}
}

func TestCatalogNotForExcludesWrongSense(t *testing.T) {
	cat := NewCatalog()
	if _, err := cat.Apply([]Record{
		{Library: "mcp.mail", Name: "send", Description: "Send an email to an address", Triggers: []string{"gmail", "inbox"}},
		{Library: "mcp.slack", Name: "send", Description: "Send a message to a Slack channel", NotFor: []string{"email", "gmail"}},
	}); err != nil {
		t.Fatal(err)
	}
	hits := cat.Search("send gmail inbox", 8)
	if len(hits) == 0 || hits[0].ID != "mcp.mail/send" {
		t.Fatalf("gmail intent = %#v", hits)
	}
	for _, hit := range hits {
		if hit.ID == "mcp.slack/send" {
			t.Fatalf("slack send should be excluded by NotFor: %#v", hits)
		}
	}
}

func TestSearchSiblingDescriptionsActAsNotFor(t *testing.T) {
	cat := NewCatalog()
	if _, err := cat.Apply([]Record{
		{Library: "mcp.mail", Name: "send", Description: "Send an email to an address"},
		{Library: "mcp.slack", Name: "send", Description: "Send a message to a Slack channel"},
	}); err != nil {
		t.Fatal(err)
	}
	hits := cat.Search("send email address", 8)
	if len(hits) == 0 || hits[0].ID != "mcp.mail/send" {
		t.Fatalf("email intent = %#v", hits)
	}
	for _, hit := range hits {
		if hit.ID == "mcp.slack/send" {
			t.Fatalf("slack send should be excluded by sibling description tokens: %#v", hits)
		}
	}
}

type stubSearcher struct{}

func (stubSearcher) Search(docs []Record, query string, topK int) []Hit {
	return []Hit{{ID: "mcp.forced/tool", Name: query, Score: 99}}
}

func TestCatalogSetSearcherOverridesKeyword(t *testing.T) {
	cat := NewCatalog()
	if _, err := cat.Apply([]Record{
		{Library: "mcp.mail", Name: "send", Description: "Send an email to an address"},
	}); err != nil {
		t.Fatal(err)
	}
	cat.SetSearcher(stubSearcher{})
	hits := cat.Search("send email", 8)
	if len(hits) != 1 || hits[0].ID != "mcp.forced/tool" {
		t.Fatalf("custom searcher not used: %#v", hits)
	}
	cat.SetSearcher(nil)
	hits = cat.Search("send email", 8)
	if len(hits) == 0 || hits[0].ID != "mcp.mail/send" {
		t.Fatalf("keyword searcher not restored: %#v", hits)
	}
}

func TestHybridSearcherKeepsNotForAndReranks(t *testing.T) {
	cat := NewCatalog()
	if _, err := cat.Apply([]Record{
		{Library: "mcp.mail", Name: "send", Description: "Send an email to an address"},
		{Library: "mcp.slack", Name: "send", Description: "Send a message to a Slack channel"},
		{Library: "mcp.weather", Name: "get-forecast", Description: "Weather forecast for a city"},
	}); err != nil {
		t.Fatal(err)
	}
	cat.SetSearcher(HybridSearcher{})
	hits := cat.Search("send email address", 8)
	if len(hits) == 0 || hits[0].ID != "mcp.mail/send" {
		t.Fatalf("hybrid email intent = %#v", hits)
	}
	for _, hit := range hits {
		if hit.ID == "mcp.slack/send" {
			t.Fatalf("hybrid reintroduced NotFor sibling: %#v", hits)
		}
	}
	forecast := cat.Search("weather city forecast", 8)
	if len(forecast) == 0 || forecast[0].ID != "mcp.weather/get-forecast" {
		t.Fatalf("hybrid forecast = %#v", forecast)
	}
}

func TestHashingSearcherKeepsNotFor(t *testing.T) {
	cat := NewCatalog()
	if _, err := cat.Apply([]Record{
		{Library: "mcp.mail", Name: "send", Description: "Send an email to an address"},
		{Library: "mcp.slack", Name: "send", Description: "Send a message to a Slack channel"},
	}); err != nil {
		t.Fatal(err)
	}
	cat.SetSearcher(HashingSearcher{})
	hits := cat.Search("send email address", 8)
	if len(hits) == 0 || hits[0].ID != "mcp.mail/send" {
		t.Fatalf("hashing email intent = %#v", hits)
	}
	for _, hit := range hits {
		if hit.ID == "mcp.slack/send" {
			t.Fatalf("hashing reintroduced NotFor sibling: %#v", hits)
		}
	}
}
