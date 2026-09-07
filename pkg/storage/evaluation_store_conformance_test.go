package storage

import (
	"context"
	"errors"
	"math"
	"reflect"
	"testing"
	"time"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
	"github.com/cc-auto-agent/harness-core/pkg/evaluation"
)

// TestEvaluationStoreConformance exercises the public evaluation.Store contract
// through both in-process implementations. It deliberately calls Store methods
// directly: Runner behaviour is covered separately and must not be the only
// guard against fabricated durable evaluation evidence.
func TestEvaluationStoreConformance(t *testing.T) {
	for _, factory := range []struct {
		name string
		new  func(*testing.T) evaluation.Store
	}{
		{
			name: "memory",
			new: func(t *testing.T) evaluation.Store {
				t.Helper()
				return evaluation.NewMemoryStore()
			},
		},
		{
			name: "sqlite",
			new: func(t *testing.T) evaluation.Store {
				t.Helper()
				sessions := newTestSQLStore(t)
				store, err := NewSQLEvaluationStore(sessions.db, SQLDialectSQLite)
				if err != nil {
					t.Fatal(err)
				}
				return store
			},
		},
	} {
		t.Run(factory.name, func(t *testing.T) {
			exerciseEvaluationStoreConformance(t, factory.new(t))
		})
	}
}

func TestPostgresEvaluationStoreConformance(t *testing.T) {
	db := newPostgresTestDB(t)
	if _, err := OpenSQLSessionStore(context.Background(), db, SQLDialectPostgres); err != nil {
		t.Fatalf("initialize PostgreSQL storage schema: %v", err)
	}
	store, err := NewSQLEvaluationStore(db, SQLDialectPostgres)
	if err != nil {
		t.Fatal(err)
	}
	exerciseEvaluationStoreConformance(t, store)
}

func exerciseEvaluationStoreConformance(t *testing.T, store evaluation.Store) {
	t.Helper()
	ctx := context.Background()
	dataset, _, err := store.PutDataset(ctx, evaluationStoreConformanceDataset())
	if err != nil {
		t.Fatal(err)
	}

	t.Run("CreateBindsTheWholeImmutableDataset", func(t *testing.T) {
		for _, test := range []struct {
			name     string
			idSuffix string
			mutate   func(*evaluation.RunResult)
		}{
			{
				name: "missing dataset", idSuffix: "missing_dataset",
				mutate: func(run *evaluation.RunResult) {
					run.DatasetID = "evaluation.missing"
				},
			},
			{
				name: "different revision", idSuffix: "different_revision",
				mutate: func(run *evaluation.RunResult) {
					run.DatasetRevision = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
				},
			},
			{
				name: "partial total", idSuffix: "partial_total",
				mutate: func(run *evaluation.RunResult) {
					run.TotalCases--
				},
			},
			{
				name: "oversized total", idSuffix: "oversized_total",
				mutate: func(run *evaluation.RunResult) {
					run.TotalCases++
				},
			},
		} {
			t.Run(test.name, func(t *testing.T) {
				run := evaluationStoreConformanceRun(t, dataset, "eval_conformance_create_"+test.idSuffix)
				test.mutate(&run)
				if err := store.CreateRun(ctx, run); err == nil {
					t.Fatal("CreateRun accepted a run not bound to the complete immutable dataset")
				}
				if _, err := store.GetRun(ctx, run.ID); !errors.Is(err, evaluation.ErrRunNotFound) {
					t.Fatalf("rejected CreateRun left a durable run: %v", err)
				}
			})
		}
	})

	t.Run("AllowsAnExplicitProfileOverride", func(t *testing.T) {
		run := evaluationStoreConformanceRun(t, dataset, "eval_conformance_profile_override")
		run.ProfileID = "evaluation.override"
		if err := store.CreateRun(ctx, run); err != nil {
			t.Fatalf("CreateRun rejected valid explicit profile override: %v", err)
		}
	})

	t.Run("CreateReplayIgnoresTransportTimestampOnly", func(t *testing.T) {
		run := evaluationStoreConformanceRun(t, dataset, "eval_conformance_create_replay")
		if err := store.CreateRun(ctx, run); err != nil {
			t.Fatal(err)
		}
		replay := run
		replay.CreatedAt = replay.CreatedAt.Add(time.Hour)
		if err := store.CreateRun(ctx, replay); err != nil {
			t.Fatalf("canonical CreateRun replay failed: %v", err)
		}
		stored, err := store.GetRun(ctx, run.ID)
		if err != nil {
			t.Fatal(err)
		}
		if !stored.CreatedAt.Equal(run.CreatedAt) {
			t.Fatalf("CreateRun replay rewrote durable created_at: got %s want %s", stored.CreatedAt, run.CreatedAt)
		}
	})

	t.Run("RecordRequiresAValidBoundRunningCase", func(t *testing.T) {
		run := evaluationStoreConformanceRun(t, dataset, "eval_conformance_record")
		if err := store.CreateRun(ctx, run); err != nil {
			t.Fatal(err)
		}
		invalid := evaluation.CaseResult{CaseID: dataset.Cases[0].ID}
		if err := store.RecordCaseResult(ctx, run.ID, invalid); err == nil {
			t.Fatal("RecordCaseResult accepted an invalid case result")
		}
		evaluationStoreConformanceAssertCases(t, ctx, store, run.ID)
		invented := evaluationStoreConformanceCase(dataset.Cases[0].ID)
		invented.CaseID = "invented-case"
		if err := store.RecordCaseResult(ctx, run.ID, invented); err == nil {
			t.Fatal("RecordCaseResult accepted a case outside the bound dataset")
		}
		evaluationStoreConformanceAssertCases(t, ctx, store, run.ID)
		first := evaluationStoreConformanceCase(dataset.Cases[0].ID)
		if err := store.RecordCaseResult(ctx, run.ID, first); err != nil {
			t.Fatal(err)
		}
		if err := store.RecordCaseResult(ctx, run.ID, first); err != nil {
			t.Fatalf("canonical running case replay failed: %v", err)
		}
	})

	t.Run("CanonicalCaseReplayKeepsInt64Precision", func(t *testing.T) {
		run := evaluationStoreConformanceRun(t, dataset, "eval_conformance_int64_precision")
		if err := store.CreateRun(ctx, run); err != nil {
			t.Fatal(err)
		}
		first := evaluationStoreConformanceCase(dataset.Cases[0].ID)
		first.DurationMS = int64(1 << 53)
		if err := store.RecordCaseResult(ctx, run.ID, first); err != nil {
			t.Fatal(err)
		}
		conflict := first
		conflict.DurationMS++
		if err := store.RecordCaseResult(ctx, run.ID, conflict); err == nil {
			t.Fatal("RecordCaseResult accepted a distinct int64 case-result replay")
		}
		evaluationStoreConformanceAssertCases(t, ctx, store, run.ID, first)
	})

	t.Run("CanonicalCaseReplayKeepsNanosecondPrecision", func(t *testing.T) {
		run := evaluationStoreConformanceRun(t, dataset, "eval_conformance_nanosecond_precision")
		if err := store.CreateRun(ctx, run); err != nil {
			t.Fatal(err)
		}
		first := evaluationStoreConformanceCase(dataset.Cases[0].ID)
		first.CompletedAt = time.Unix(1_700_000_051, 17).UTC()
		if err := store.RecordCaseResult(ctx, run.ID, first); err != nil {
			t.Fatal(err)
		}
		if err := store.RecordCaseResult(ctx, run.ID, first); err != nil {
			t.Fatalf("canonical nanosecond case replay failed: %v", err)
		}
		conflict := first
		conflict.CompletedAt = conflict.CompletedAt.Add(time.Nanosecond)
		if err := store.RecordCaseResult(ctx, run.ID, conflict); err == nil {
			t.Fatal("RecordCaseResult accepted a distinct nanosecond case-result replay")
		}
		evaluationStoreConformanceAssertCases(t, ctx, store, run.ID, first)
	})

	t.Run("RejectsNonFiniteScoresWithoutChangingDurableState", func(t *testing.T) {
		caseRun := evaluationStoreConformanceRun(t, dataset, "eval_conformance_nonfinite_case")
		if err := store.CreateRun(ctx, caseRun); err != nil {
			t.Fatal(err)
		}
		for _, test := range []struct {
			name   string
			mutate func(*evaluation.CaseResult)
		}{
			{
				name: "case score NaN",
				mutate: func(result *evaluation.CaseResult) {
					result.Score = math.NaN()
				},
			},
			{
				name: "assertion score NaN",
				mutate: func(result *evaluation.CaseResult) {
					result.Assertions[0].Score = math.NaN()
				},
			},
		} {
			t.Run(test.name, func(t *testing.T) {
				result := evaluationStoreConformanceCase(dataset.Cases[0].ID)
				test.mutate(&result)
				if err := store.RecordCaseResult(ctx, caseRun.ID, result); err == nil {
					t.Fatal("RecordCaseResult accepted a non-finite score")
				}
				evaluationStoreConformanceAssertCases(t, ctx, store, caseRun.ID)
			})
		}

		for _, test := range []struct {
			name     string
			idSuffix string
			score    float64
		}{
			{name: "NaN", idSuffix: "nan", score: math.NaN()},
			{name: "positive infinity", idSuffix: "positive_infinity", score: math.Inf(1)},
			{name: "negative infinity", idSuffix: "negative_infinity", score: math.Inf(-1)},
		} {
			t.Run(test.name, func(t *testing.T) {
				run := evaluationStoreConformanceRun(t, dataset, "eval_conformance_nonfinite_"+test.idSuffix)
				if err := store.CreateRun(ctx, run); err != nil {
					t.Fatal(err)
				}
				first := evaluationStoreConformanceCase(dataset.Cases[0].ID)
				second := evaluationStoreConformanceCase(dataset.Cases[1].ID)
				if err := store.RecordCaseResult(ctx, run.ID, first); err != nil {
					t.Fatal(err)
				}
				if err := store.RecordCaseResult(ctx, run.ID, second); err != nil {
					t.Fatal(err)
				}
				finish := run
				finish.Status, finish.Score, finish.Passed, finish.PassedCases = evaluation.RunCompleted, test.score, true, 2
				finish.CompletedAt = time.Unix(1_700_000_175, 0).UTC()
				if err := store.FinishRun(ctx, finish); err == nil {
					t.Fatal("FinishRun accepted a non-finite score")
				}
				stored, err := store.GetRun(ctx, run.ID)
				if err != nil {
					t.Fatal(err)
				}
				if stored.Status != evaluation.RunRunning {
					t.Fatalf("rejected non-finite finish changed status: got %q", stored.Status)
				}
				evaluationStoreConformanceAssertCases(t, ctx, store, run.ID, first, second)
			})
		}
	})

	t.Run("CompletedRunRequiresTheExactDatasetCaseSet", func(t *testing.T) {
		run := evaluationStoreConformanceRun(t, dataset, "eval_conformance_full_set")
		if err := store.CreateRun(ctx, run); err != nil {
			t.Fatal(err)
		}
		first := evaluationStoreConformanceCase(dataset.Cases[0].ID)
		invented := evaluationStoreConformanceCase("invented-case")
		if err := store.RecordCaseResult(ctx, run.ID, first); err != nil {
			t.Fatal(err)
		}
		if err := store.RecordCaseResult(ctx, run.ID, invented); err == nil {
			t.Fatal("RecordCaseResult accepted an invented case before completion")
		}
		evaluationStoreConformanceAssertCases(t, ctx, store, run.ID, first)
		run.Status, run.CompletedAt = evaluation.RunCompleted, time.Unix(1_700_000_100, 0).UTC()
		if err := store.FinishRun(ctx, run); err == nil {
			t.Fatal("FinishRun completed without the exact dataset case set")
		}
		stored, err := store.GetRun(ctx, run.ID)
		if err != nil {
			t.Fatal(err)
		}
		if stored.Status != evaluation.RunRunning {
			t.Fatalf("rejected completed run changed status: got %q", stored.Status)
		}
		evaluationStoreConformanceAssertCases(t, ctx, store, run.ID, first)
	})

	t.Run("TerminalStateKeepsTheStoredHeaderAndCases", func(t *testing.T) {
		run := evaluationStoreConformanceRun(t, dataset, "eval_conformance_terminal")
		if err := store.CreateRun(ctx, run); err != nil {
			t.Fatal(err)
		}
		first := evaluationStoreConformanceCase(dataset.Cases[0].ID)
		second := evaluationStoreConformanceCase(dataset.Cases[1].ID)
		if err := store.RecordCaseResult(ctx, run.ID, first); err != nil {
			t.Fatal(err)
		}
		if err := store.RecordCaseResult(ctx, run.ID, second); err != nil {
			t.Fatal(err)
		}
		run.Status, run.Score, run.Passed, run.PassedCases = evaluation.RunCompleted, 1, true, 2
		run.CompletedAt = time.Unix(1_700_000_200, 0).UTC()
		run.Cases = []evaluation.CaseResult{second, first} // Order is not authoritative.
		if err := store.FinishRun(ctx, run); err != nil {
			t.Fatalf("FinishRun rejected the same stored case map in another order: %v", err)
		}
		stored, err := store.GetRun(ctx, run.ID)
		if err != nil {
			t.Fatal(err)
		}
		if !stored.CreatedAt.Equal(evaluationStoreConformanceCreatedAt) || len(stored.Cases) != 2 {
			t.Fatalf("FinishRun did not preserve the original header/cases: %#v", stored)
		}

		headerChanged := run
		headerChanged.TenantID = "other-tenant"
		if err := store.FinishRun(ctx, headerChanged); err == nil {
			t.Fatal("terminal replay rewrote or accepted a different immutable header")
		}
		evaluationStoreConformanceAssertRun(t, ctx, store, run.ID, stored)
		caseChanged := run
		caseChanged.Cases = []evaluation.CaseResult{run.Cases[0], run.Cases[1]}
		caseChanged.Cases[0].Assertions = append([]evaluation.AssertionResult(nil), run.Cases[0].Assertions...)
		caseChanged.Cases[0].Score, caseChanged.Cases[0].Passed = 0, false
		caseChanged.Cases[0].Assertions[0].Score, caseChanged.Cases[0].Assertions[0].Passed = 0, false
		if err := store.FinishRun(ctx, caseChanged); err == nil {
			t.Fatal("terminal replay accepted a changed supplied case result")
		}
		evaluationStoreConformanceAssertRun(t, ctx, store, run.ID, stored)
		replay := run
		replay.Cases = nil
		replay.CreatedAt = replay.CreatedAt.Add(time.Hour)
		replay.CompletedAt = replay.CompletedAt.Add(time.Hour)
		if err := store.FinishRun(ctx, replay); err != nil {
			t.Fatalf("canonical terminal replay failed: %v", err)
		}
		stored, err = store.GetRun(ctx, run.ID)
		if err != nil {
			t.Fatal(err)
		}
		if !stored.CreatedAt.Equal(evaluationStoreConformanceCreatedAt) || !stored.CompletedAt.Equal(run.CompletedAt) {
			t.Fatalf("terminal replay rewrote durable timestamps: %#v", stored)
		}
	})

	t.Run("FailedRunMayRemainPartialButCannotAcceptNewEvidence", func(t *testing.T) {
		run := evaluationStoreConformanceRun(t, dataset, "eval_conformance_failed")
		if err := store.CreateRun(ctx, run); err != nil {
			t.Fatal(err)
		}
		first := evaluationStoreConformanceCase(dataset.Cases[0].ID)
		if err := store.RecordCaseResult(ctx, run.ID, first); err != nil {
			t.Fatal(err)
		}
		run.Status, run.Error, run.CompletedAt = evaluation.RunFailed, "fixture interruption", time.Unix(1_700_000_300, 0).UTC()
		if err := store.FinishRun(ctx, run); err != nil {
			t.Fatalf("failed partial run was rejected: %v", err)
		}
		if err := store.RecordCaseResult(ctx, run.ID, first); err != nil {
			t.Fatalf("canonical case replay after terminal failed: %v", err)
		}
		if err := store.RecordCaseResult(ctx, run.ID, evaluationStoreConformanceCase(dataset.Cases[1].ID)); err == nil {
			t.Fatal("terminal run accepted new case evidence")
		}
		evaluationStoreConformanceAssertCases(t, ctx, store, run.ID, first)
	})
}

func evaluationStoreConformanceAssertCases(t *testing.T, ctx context.Context, store evaluation.Store, runID string, want ...evaluation.CaseResult) {
	t.Helper()
	stored, err := store.GetRun(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	if len(stored.Cases) != len(want) || !reflect.DeepEqual(stored.Cases, want) && len(want) != 0 {
		t.Fatalf("rejected operation changed durable cases: got %#v want %#v", stored.Cases, want)
	}
}

func evaluationStoreConformanceAssertRun(t *testing.T, ctx context.Context, store evaluation.Store, runID string, want evaluation.RunResult) {
	t.Helper()
	stored, err := store.GetRun(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(stored, want) {
		t.Fatalf("rejected operation changed durable run: got %#v want %#v", stored, want)
	}
}

var evaluationStoreConformanceCreatedAt = time.Unix(1_700_000_000, 0).UTC()

func evaluationStoreConformanceDataset() evaluation.Dataset {
	return evaluation.Dataset{
		ID: "evaluation.conformance", Version: 1, Name: "Evaluation Store Conformance", ProfileID: "evaluation.default",
		Cases: []evaluation.Case{
			{ID: "case-one", Input: "one", Assertions: []evaluation.Assertion{{ID: "status", Kind: evaluation.AssertRunStatus, ExpectedStatus: core.RunCompleted}}},
			{ID: "case-two", Input: "two", Assertions: []evaluation.Assertion{{ID: "status", Kind: evaluation.AssertRunStatus, ExpectedStatus: core.RunCompleted}}},
		},
	}
}

func evaluationStoreConformanceRun(t *testing.T, dataset evaluation.Dataset, id string) evaluation.RunResult {
	t.Helper()
	composition := map[string]string{"harness.assignment.id": "evaluation-store-conformance"}
	assignment, err := core.CompositionMetadataRevision(composition)
	if err != nil {
		t.Fatal(err)
	}
	return evaluation.RunResult{
		ID: id, DatasetID: dataset.ID, DatasetVersion: dataset.Version, DatasetRevision: dataset.Revision,
		TenantID: "acme", SubjectID: "alice", ProfileID: dataset.ProfileID, AssignmentRevision: assignment,
		Status: evaluation.RunRunning, TotalCases: len(dataset.Cases), AllowCapabilities: []string{"eval.lookup"},
		CompositionMetadata: composition, Metadata: map[string]string{"fixture": "evaluation-store-conformance"},
		CreatedAt: evaluationStoreConformanceCreatedAt,
	}
}

func evaluationStoreConformanceCase(id string) evaluation.CaseResult {
	return evaluation.CaseResult{
		CaseID: id, SessionID: "evalsess_conformance", AgentRunID: "evalcase_conformance",
		Status: core.RunCompleted, Answer: "done", Score: 1, Passed: true,
		Assertions: []evaluation.AssertionResult{{AssertionID: "status", Kind: evaluation.AssertRunStatus, Score: 1, Passed: true}},
		DurationMS: 1, CompletedAt: time.Unix(1_700_000_050, 0).UTC(),
	}
}
