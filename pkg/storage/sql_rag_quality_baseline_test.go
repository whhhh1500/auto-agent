package storage

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
	"github.com/cc-auto-agent/harness-core/pkg/extensions/rag"
)

// TestSQLRagQualityBaseline records the current deterministic lexical baseline
// against fixed relevance labels. qrels are deliberately independent from the
// current ranker: the semantic case remains labelled relevant even when the
// keyword retrieval returns no hit, while the CJK punctuation case validates
// the fixed delimiter tokenizer and its SQL projection.
//
// Recall and precision here are micro metrics across all returned top-five
// chunks: relevant returned chunks divided by total qrels and returned chunks,
// respectively. MRR is macro across all queries, including zero for a query
// with no relevant retrieval. The expected IDs and metrics form a fixed
// regression baseline, not a production-quality threshold or evidence of a
// quality improvement. These are traditional document-qrel metrics, not an
// implementation or score of RAGChecker.
func TestSQLRagQualityBaseline(t *testing.T) {
	runSQLRagQualityBaseline(t, SQLDialectSQLite, newSQLMemoryRagV26DB(t))
}

// TestPostgresSQLRagQualityBaseline is intentionally runnable through the
// repository's HARNESS_TEST_PG_DSN test-db helper. It naturally skips when the
// disposable PostgreSQL test environment is unavailable.
func TestPostgresSQLRagQualityBaseline(t *testing.T) {
	runSQLRagQualityBaseline(t, SQLDialectPostgres, newPostgresTestDB(t))
}

type ragQualityBaseline struct {
	Documents []ragQualityDocument `json:"documents"`
	Queries   []ragQualityQuery    `json:"queries"`
	Expected  ragQualityMetrics    `json:"expected_baseline"`
}

type ragQualityDocument struct {
	Scope   []core.ScopeRef `json:"scope"`
	ID      string          `json:"id"`
	Source  string          `json:"source"`
	Content string          `json:"content"`
	Tags    []string        `json:"tags"`
}

type ragQualityQuery struct {
	Name            string          `json:"name"`
	InformationNeed string          `json:"information_need"`
	Scope           []core.ScopeRef `json:"scope"`
	Query           string          `json:"query"`
	Tags            []string        `json:"tags"`
	TopK            int             `json:"top_k"`
	Qrels           []string        `json:"qrels"`
	AllowedScopes   []string        `json:"allowed_scopes"`
	ForbiddenIDs    []string        `json:"forbidden_ids"`
	ExpectedIDs     []string        `json:"expected_ids"`
}

type ragQualityMetrics struct {
	RelevantRetrieved int      `json:"relevant_retrieved_at_5"`
	RelevantTotal     int      `json:"relevant_total"`
	RetrievedTotal    int      `json:"retrieved_total_at_5"`
	MRR               float64  `json:"mrr_at_5"`
	DiagnosticMisses  []string `json:"diagnostic_missed_qrels"`
}

func runSQLRagQualityBaseline(t *testing.T, dialect SQLDialect, db *sql.DB) {
	t.Helper()
	fixture := loadRagQualityBaseline(t)
	ctx := context.Background()
	if _, err := OpenSQLSessionStore(ctx, db, dialect); err != nil {
		t.Fatalf("open canonical %s schema: %v", dialect, err)
	}
	keyword := rag.NewKeywordIndex()
	sqlIndex, err := NewSQLRagIndex(db, dialect)
	if err != nil {
		t.Fatal(err)
	}
	documentScopes := make(map[string]string, len(fixture.Documents))
	for _, document := range fixture.Documents {
		scope := mustRagQualityScope(t, document.Scope)
		if _, exists := documentScopes[document.ID]; exists {
			t.Fatalf("fixture document id %q is not globally unique", document.ID)
		}
		documentScopes[document.ID] = scope.String()
		entry := core.RagDocument{ID: document.ID, Source: document.Source, Content: document.Content, Tags: document.Tags}
		if err := keyword.Ingest(ctx, scope, entry); err != nil {
			t.Fatalf("keyword ingest %q: %v", document.ID, err)
		}
		if err := sqlIndex.Ingest(ctx, scope, entry); err != nil {
			t.Fatalf("sql ingest %q: %v", document.ID, err)
		}
	}

	var got ragQualityMetrics
	for _, query := range fixture.Queries {
		t.Run(query.Name, func(t *testing.T) {
			if strings.TrimSpace(query.InformationNeed) == "" {
				t.Fatal("fixture information_need is empty")
			}
			if query.TopK != 5 {
				t.Fatalf("fixture top_k=%d, want fixed baseline top_k=5", query.TopK)
			}
			scope := mustRagQualityScope(t, query.Scope)
			request := core.RagQuery{Query: query.Query, Tags: query.Tags, TopK: query.TopK}
			reference, err := keyword.Search(ctx, scope, request)
			if err != nil {
				t.Fatalf("keyword search: %v", err)
			}
			actual, err := sqlIndex.Search(ctx, scope, request)
			if err != nil {
				t.Fatalf("sql search: %v", err)
			}
			assertRagQualityParity(t, reference, actual)
			ids := ragChunkIDs(actual)
			if !reflect.DeepEqual(ids, query.ExpectedIDs) {
				t.Fatalf("baseline ordered IDs = %#v, want fixture %#v", ids, query.ExpectedIDs)
			}
			assertRagQualityVisibility(t, ids, documentScopes, query.AllowedScopes, query.ForbiddenIDs)

			validateRagQualityQrels(t, query.Qrels, documentScopes, query.AllowedScopes, query.ForbiddenIDs)
			qrels := stringSet(query.Qrels)
			firstRank := 0
			for rank, id := range ids {
				got.RetrievedTotal++
				if qrels[id] {
					got.RelevantRetrieved++
					if firstRank == 0 {
						firstRank = rank + 1
					}
				}
			}
			got.RelevantTotal += len(qrels)
			if firstRank > 0 {
				got.MRR += 1 / float64(firstRank)
			}
			for id := range qrels {
				if !ragQualityContains(ids, id) {
					got.DiagnosticMisses = append(got.DiagnosticMisses, id)
				}
			}
		})
	}
	got.MRR /= float64(len(fixture.Queries))
	sort.Strings(got.DiagnosticMisses)
	sort.Strings(fixture.Expected.DiagnosticMisses)
	if !reflect.DeepEqual(got, fixture.Expected) {
		t.Fatalf("quality baseline metrics=%+v, want fixture %+v", got, fixture.Expected)
	}
	t.Logf("RAG deterministic baseline: micro_recall@5=%d/%d micro_precision@5=%d/%d macro_mrr@5=%.3f misses=%v",
		got.RelevantRetrieved, got.RelevantTotal, got.RelevantRetrieved, got.RetrievedTotal, got.MRR, got.DiagnosticMisses)
}

func loadRagQualityBaseline(t *testing.T) ragQualityBaseline {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "rag_quality_baseline.json"))
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var fixture ragQualityBaseline
	if err := decoder.Decode(&fixture); err != nil {
		t.Fatalf("decode quality fixture: %v", err)
	}
	if len(fixture.Documents) == 0 || len(fixture.Queries) == 0 || fixture.Expected.RelevantTotal == 0 {
		t.Fatal("quality fixture is incomplete")
	}
	return fixture
}

func mustRagQualityScope(t *testing.T, refs []core.ScopeRef) core.ScopePath {
	t.Helper()
	scope, err := core.NewScopePath(refs...)
	if err != nil {
		t.Fatalf("fixture scope %v: %v", refs, err)
	}
	return scope
}

func validateRagQualityQrels(t *testing.T, qrels []string, documentScopes map[string]string, allowedScopes, forbiddenIDs []string) {
	t.Helper()
	if len(qrels) == 0 {
		t.Fatal("fixture qrels is empty")
	}
	allowed := stringSet(allowedScopes)
	forbidden := stringSet(forbiddenIDs)
	seen := make(map[string]bool, len(qrels))
	for _, id := range qrels {
		if id == "" {
			t.Fatal("fixture qrel is empty")
		}
		if seen[id] {
			t.Fatalf("fixture qrel %q is duplicated", id)
		}
		seen[id] = true
		scope, exists := documentScopes[id]
		if !exists {
			t.Fatalf("fixture qrel %q has no document", id)
		}
		if forbidden[id] {
			t.Fatalf("fixture qrel %q is forbidden", id)
		}
		if !allowed[scope] {
			t.Fatalf("fixture qrel %q uses unapproved scope %q", id, scope)
		}
	}
}

func assertRagQualityParity(t *testing.T, reference []core.RagChunk, actual []core.RagChunk) {
	t.Helper()
	if len(reference) != len(actual) {
		t.Fatalf("reference/sql result lengths %d/%d", len(reference), len(actual))
	}
	for i := range reference {
		if reference[i].ID != actual[i].ID || reference[i].Source != actual[i].Source || reference[i].Content != actual[i].Content || math.Abs(reference[i].Score-actual[i].Score) > 1e-12 {
			t.Fatalf("reference/sql result %d differ: reference=%+v sql=%+v", i, reference[i], actual[i])
		}
	}
}

func assertRagQualityVisibility(t *testing.T, ids []string, documentScopes map[string]string, allowedScopes, forbiddenIDs []string) {
	t.Helper()
	allowed := stringSet(allowedScopes)
	forbidden := stringSet(forbiddenIDs)
	for _, id := range ids {
		if forbidden[id] {
			t.Fatalf("forbidden document %q was returned", id)
		}
		if !allowed[documentScopes[id]] {
			t.Fatalf("document %q from unapproved fixture scope %q was returned", id, documentScopes[id])
		}
	}
}

func ragQualityContains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
