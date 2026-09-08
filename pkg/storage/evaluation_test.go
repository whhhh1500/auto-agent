package storage

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	core "github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/evaluation"
)

func sqlEvaluationDataset() evaluation.Dataset {
	return evaluation.Dataset{
		ID: "evaluation.sql", Version: 1, Name: "SQL Evaluation", ProfileID: "evaluation.agent",
		Cases: []evaluation.Case{{
			ID: "case-sql", Input: "test",
			Assertions: []evaluation.Assertion{{ID: "status", Kind: evaluation.AssertRunStatus, ExpectedStatus: core.RunCompleted}},
		}},
	}
}

func sqlEvaluationRun(dataset evaluation.Dataset) evaluation.RunResult {
	return evaluation.RunResult{
		ID: "eval_sql_run", DatasetID: dataset.ID, DatasetVersion: dataset.Version,
		DatasetRevision: dataset.Revision, TenantID: "acme", SubjectID: "alice",
		ProfileID: dataset.ProfileID, Status: evaluation.RunRunning,
		TotalCases: len(dataset.Cases), AllowCapabilities: []string{"eval.lookup"},
		Metadata: map[string]string{"branch": "candidate"}, CreatedAt: time.Now().UTC(),
	}
}

func sqlEvaluationCaseResult() evaluation.CaseResult {
	return evaluation.CaseResult{
		CaseID: "case-sql", SessionID: "evalsess_sql", AgentRunID: "evalcase_sql",
		Status: core.RunCompleted, Answer: "done", Score: 1, Passed: true,
		Assertions: []evaluation.AssertionResult{{AssertionID: "status", Kind: evaluation.AssertRunStatus, Score: 1, Passed: true}},
		Artifacts:  evaluation.ArtifactSnapshot{ProfileSnapshotID: "profile-revision", CapabilitySnapshotID: "cap-revision"},
		DurationMS: 12, ToolCalls: []string{"eval.lookup"}, CompletedAt: time.Now().UTC(),
	}
}

func TestSQLEvaluationStoreLifecycleAndIncrementalCases(t *testing.T) {
	sessions := newTestSQLStore(t)
	store, err := NewSQLEvaluationStore(sessions.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	dataset, created, err := store.PutDataset(ctx, sqlEvaluationDataset())
	if err != nil || !created || dataset.Revision == "" || dataset.CreatedAt.IsZero() {
		t.Fatalf("dataset create wrong: %#v created=%t err=%v", dataset, created, err)
	}
	replayed, created, err := store.PutDataset(ctx, dataset)
	if err != nil || created || replayed.Revision != dataset.Revision {
		t.Fatalf("dataset replay wrong: %#v created=%t err=%v", replayed, created, err)
	}
	changed := dataset
	changed.Cases[0].Input = "changed"
	changed.Revision = ""
	if _, _, err := store.PutDataset(ctx, changed); err == nil {
		t.Fatal("dataset version was overwritten with another revision")
	}
	listed, err := store.ListDatasets(ctx, dataset.ID, 10)
	if err != nil || len(listed) != 1 || listed[0].Revision != dataset.Revision || listed[0].CaseCount != 1 {
		t.Fatalf("dataset list wrong: %#v err=%v", listed, err)
	}
	run := sqlEvaluationRun(dataset)
	run.CompositionMetadata = map[string]string{
		"harness.assignment.id":        "sql-evaluation-assignment",
		"harness.assignment.candidate": "true",
	}
	if err := store.CreateRun(ctx, run); err != nil {
		t.Fatal(err)
	}
	caseResult := sqlEvaluationCaseResult()
	if err := store.RecordCaseResult(ctx, run.ID, caseResult); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordCaseResult(ctx, run.ID, caseResult); err != nil {
		t.Fatalf("idempotent case result replay failed: %v", err)
	}
	inProgress, err := store.GetRun(ctx, run.ID)
	if err != nil || inProgress.Status != evaluation.RunRunning || len(inProgress.Cases) != 1 ||
		inProgress.CompositionMetadata["harness.assignment.id"] != "sql-evaluation-assignment" {
		t.Fatalf("incremental run wrong: %#v err=%v", inProgress, err)
	}
	run.Cases = []evaluation.CaseResult{caseResult}
	run.Status, run.Score, run.Passed, run.PassedCases = evaluation.RunCompleted, 1, true, 1
	run.CompletedAt = time.Now().UTC()
	if err := store.FinishRun(ctx, run); err != nil {
		t.Fatal(err)
	}
	if err := store.FinishRun(ctx, run); err != nil {
		t.Fatalf("idempotent finish failed: %v", err)
	}
	finished, err := store.GetRun(ctx, run.ID)
	if err != nil || finished.Status != evaluation.RunCompleted || !finished.Passed || finished.Score != 1 || len(finished.Cases) != 1 {
		t.Fatalf("finished run wrong: %#v err=%v", finished, err)
	}
	if finished.CompositionMetadata["harness.assignment.candidate"] != "true" {
		t.Fatalf("finished evaluation lost composition metadata: %#v", finished)
	}
	if err := evaluation.ValidateCaseResult(evaluation.CaseResult{
		CaseID: "case-sql", SessionID: "evalsess_sql", AgentRunID: "evalcase_sql",
		Status: core.RunCompleted, Score: 1, Passed: true, CompletedAt: time.Now().UTC(),
		Artifacts: evaluation.ArtifactSnapshot{
			CompositionRevision: strings.Repeat("a", 64), AssignmentRevision: strings.Repeat("b", 64),
		},
	}); err != nil {
		t.Fatalf("valid evaluation artifact rejected: %v", err)
	}
	runs, err := store.ListRuns(ctx, dataset.ID, "acme", 10)
	if err != nil || len(runs) != 1 || runs[0].ID != run.ID || len(runs[0].Cases) != 0 {
		t.Fatalf("run list wrong: %#v err=%v", runs, err)
	}
	if _, err := store.GetRun(ctx, "eval_missing"); !errors.Is(err, evaluation.ErrRunNotFound) {
		t.Fatalf("missing run error=%v", err)
	}
}

func TestSQLEvaluationStoreRefusesCompletedRunWithMissingCases(t *testing.T) {
	sessions := newTestSQLStore(t)
	store, _ := NewSQLEvaluationStore(sessions.db, SQLDialectSQLite)
	dataset, _, err := store.PutDataset(context.Background(), sqlEvaluationDataset())
	if err != nil {
		t.Fatal(err)
	}
	run := sqlEvaluationRun(dataset)
	run.ID = "eval_missing_cases"
	if err := store.CreateRun(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	run.Status, run.CompletedAt = evaluation.RunCompleted, time.Now().UTC()
	if err := store.FinishRun(context.Background(), run); err == nil {
		t.Fatal("evaluation run completed without all case results")
	}
}

func TestSQLEvaluationStoreQueriesRunsByArtifactRevision(t *testing.T) {
	sessions := newTestSQLStore(t)
	store, err := NewSQLEvaluationStore(sessions.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	dataset, _, err := store.PutDataset(ctx, sqlEvaluationDataset())
	if err != nil {
		t.Fatal(err)
	}
	compositionA := strings.Repeat("a", 64)
	compositionB := strings.Repeat("b", 64)
	assignmentA := strings.Repeat("c", 64)
	assignmentB := strings.Repeat("d", 64)
	makeRun := func(id, assignment string) evaluation.RunResult {
		run := sqlEvaluationRun(dataset)
		run.ID = id
		run.AssignmentRevision = assignment
		run.CompositionMetadata = map[string]string{"assignment": id}
		return run
	}
	first, second := makeRun("eval_query_first", assignmentA), makeRun("eval_query_second", assignmentB)
	if err := store.CreateRun(ctx, first); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateRun(ctx, second); err != nil {
		t.Fatal(err)
	}
	firstCase, secondCase := sqlEvaluationCaseResult(), sqlEvaluationCaseResult()
	firstCase.Artifacts.CompositionRevision, firstCase.Artifacts.AssignmentRevision = compositionA, assignmentA
	secondCase.Artifacts.CompositionRevision, secondCase.Artifacts.AssignmentRevision = compositionB, assignmentB
	firstCase.CaseID, secondCase.CaseID = "case-sql", "case-sql"
	if err := store.RecordCaseResult(ctx, first.ID, firstCase); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordCaseResult(ctx, second.ID, secondCase); err != nil {
		t.Fatal(err)
	}
	for name, query := range map[string]evaluation.RunQuery{
		"composition": {DatasetID: dataset.ID, TenantID: "acme", CompositionRevision: compositionA},
		"assignment":  {DatasetID: dataset.ID, TenantID: "acme", AssignmentRevision: assignmentA},
	} {
		result, err := store.QueryRuns(ctx, query)
		if err != nil || len(result) != 1 || result[0].ID != first.ID {
			t.Fatalf("%s SQL query wrong: %#v err=%v", name, result, err)
		}
	}
	if _, err := store.QueryRuns(ctx, evaluation.RunQuery{CompositionRevision: "not-a-digest"}); err == nil {
		t.Fatal("invalid SQL revision query was accepted")
	}
	if _, err := store.QueryRuns(ctx, evaluation.RunQuery{TenantID: "acme\x00"}); err == nil {
		t.Fatal("NUL SQL evaluation tenant query was accepted")
	}
	if _, err := store.GetDataset(ctx, "bad id", 1); err == nil {
		t.Fatal("invalid dataset id was accepted")
	}
	if _, err := store.ListDatasets(ctx, "nodot", 10); err == nil {
		t.Fatal("non-namespaced dataset list filter was accepted")
	}
}

func TestSQLEvaluationStoreRejectsInvalidRunID(t *testing.T) {
	store, err := NewSQLEvaluationStore(newTestSQLStore(t).db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := store.GetRun(ctx, "bad id"); err == nil {
		t.Fatal("invalid evaluation run id was accepted")
	}
}

func TestSQLEvaluationStoreRejectsInvalidCaseRunID(t *testing.T) {
	store, err := NewSQLEvaluationStore(newTestSQLStore(t).db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := store.RecordCaseResult(ctx, "bad id", evaluation.CaseResult{CaseID: "case-1"}); err == nil {
		t.Fatal("invalid case run id was accepted")
	}
	if err := store.RecordRunError(ctx, "bad id", "boom"); err == nil {
		t.Fatal("invalid error run id was accepted")
	}
}

func TestSameEvaluationRunHeaderReplayDefinition(t *testing.T) {
	base := evaluation.RunResult{
		ID:                 "eval_sql_header",
		DatasetID:          "evaluation.sql",
		DatasetVersion:     1,
		DatasetRevision:    strings.Repeat("a", 64),
		TenantID:           "acme",
		SubjectID:          "alice",
		ProfileID:          "evaluation.agent",
		BaselineRunID:      "eval_sql_baseline",
		AssignmentRevision: strings.Repeat("b", 64),
		Status:             evaluation.RunRunning,
		TotalCases:         2,
		AllowCapabilities:  []string{"eval.lookup"},
		CompositionMetadata: map[string]string{
			"harness.assignment.id": "assignment-1",
		},
		Metadata:  map[string]string{"branch": "candidate"},
		CreatedAt: time.Unix(1, 2).UTC(),
	}

	createdAtReplay := base
	createdAtReplay.CreatedAt = time.Unix(99, 100).UTC()
	if !sameEvaluationRunHeader(base, createdAtReplay) {
		t.Fatal("replay with a different CreatedAt must match the same run definition")
	}

	definitionChanges := []struct {
		name   string
		change func(*evaluation.RunResult)
	}{
		{"id", func(run *evaluation.RunResult) { run.ID = "eval_sql_other" }},
		{"dataset id", func(run *evaluation.RunResult) { run.DatasetID = "evaluation.other" }},
		{"dataset version", func(run *evaluation.RunResult) { run.DatasetVersion = 2 }},
		{"dataset revision", func(run *evaluation.RunResult) { run.DatasetRevision = strings.Repeat("c", 64) }},
		{"tenant", func(run *evaluation.RunResult) { run.TenantID = "other-tenant" }},
		{"subject", func(run *evaluation.RunResult) { run.SubjectID = "bob" }},
		{"profile", func(run *evaluation.RunResult) { run.ProfileID = "evaluation.other" }},
		{"baseline", func(run *evaluation.RunResult) { run.BaselineRunID = "eval_sql_other_baseline" }},
		{"assignment", func(run *evaluation.RunResult) { run.AssignmentRevision = strings.Repeat("d", 64) }},
		{"allow capabilities", func(run *evaluation.RunResult) { run.AllowCapabilities = []string{"eval.other"} }},
		{"composition metadata", func(run *evaluation.RunResult) {
			run.CompositionMetadata = map[string]string{"harness.assignment.id": "assignment-2"}
		}},
		{"metadata", func(run *evaluation.RunResult) {
			run.Metadata = map[string]string{"branch": "stable"}
		}},
		{"total cases", func(run *evaluation.RunResult) { run.TotalCases = 3 }},
	}
	for _, test := range definitionChanges {
		t.Run(test.name, func(t *testing.T) {
			candidate := base
			test.change(&candidate)
			if sameEvaluationRunHeader(base, candidate) {
				t.Fatalf("definition change %q was accepted", test.name)
			}
		})
	}
}

func TestSQLEvaluationStoreRejectsRunningOverflow(t *testing.T) {
	sessions := newTestSQLStore(t)
	store, err := NewSQLEvaluationStore(sessions.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	store.maxRunning = 2
	ctx := context.Background()
	dataset, _, err := store.PutDataset(ctx, sqlEvaluationDataset())
	if err != nil {
		t.Fatal(err)
	}
	run := func(id string) evaluation.RunResult {
		item := sqlEvaluationRun(dataset)
		item.ID = id
		return item
	}
	if err := store.CreateRun(ctx, run("eval_sql_run_1")); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateRun(ctx, run("eval_sql_run_2")); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateRun(ctx, run("eval_sql_run_3")); err == nil {
		t.Fatal("sql running evaluation overflow was accepted")
	}
	if err := store.CreateRun(ctx, run("eval_sql_run_1")); err != nil {
		t.Fatalf("idempotent running replay must still work at cap: %v", err)
	}
	finished := run("eval_sql_run_1")
	finished.Status = evaluation.RunFailed
	finished.Error = "aborted"
	finished.CompletedAt = time.Now().UTC()
	if err := store.FinishRun(ctx, finished); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateRun(ctx, run("eval_sql_run_3")); err != nil {
		t.Fatalf("failed evaluation run did not free a slot: %v", err)
	}
}

func TestSQLEvaluationStoreRejectsDatasetVersionOverflow(t *testing.T) {
	sessions := newTestSQLStore(t)
	store, err := NewSQLEvaluationStore(sessions.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	store.maxDatasetVersions = 2
	ctx := context.Background()
	first, created, err := store.PutDataset(ctx, sqlEvaluationDataset())
	if err != nil || !created {
		t.Fatalf("first dataset version failed: %#v created=%t err=%v", first, created, err)
	}
	second := sqlEvaluationDataset()
	second.Version = 2
	if _, _, err := store.PutDataset(ctx, second); err != nil {
		t.Fatal(err)
	}
	third := sqlEvaluationDataset()
	third.Version = 3
	if _, _, err := store.PutDataset(ctx, third); err == nil {
		t.Fatal("dataset version overflow was accepted")
	} else if !strings.Contains(err.Error(), "versions exceed maximum of 2") {
		t.Fatalf("unexpected overflow error: %v", err)
	}
	replayed, created, err := store.PutDataset(ctx, first)
	if err != nil || created || replayed.Revision != first.Revision {
		t.Fatalf("existing dataset version was blocked by the cap: %#v created=%t err=%v", replayed, created, err)
	}
}

func TestSQLEvaluationStoreRejectsDatasetIDOverflow(t *testing.T) {
	sessions := newTestSQLStore(t)
	store, err := NewSQLEvaluationStore(sessions.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	store.maxDatasetIDs = 2
	ctx := context.Background()
	put := func(id string) error {
		dataset := sqlEvaluationDataset()
		dataset.ID = id
		_, _, err := store.PutDataset(ctx, dataset)
		return err
	}
	if err := put("evaluation.one"); err != nil {
		t.Fatal(err)
	}
	if err := put("evaluation.two"); err != nil {
		t.Fatal(err)
	}
	if err := put("evaluation.three"); err == nil {
		t.Fatal("dataset id overflow was accepted")
	} else if !strings.Contains(err.Error(), "dataset ids exceed maximum of 2") {
		t.Fatalf("unexpected overflow error: %v", err)
	}
	first := sqlEvaluationDataset()
	first.ID = "evaluation.one"
	replayed, created, err := store.PutDataset(ctx, first)
	if err != nil || created || replayed.ID != "evaluation.one" {
		t.Fatalf("existing dataset id was blocked by the cap: %#v created=%t err=%v", replayed, created, err)
	}
}

func TestSQLEvaluationStoreRejectsStoredRunOverflow(t *testing.T) {
	sessions := newTestSQLStore(t)
	store, err := NewSQLEvaluationStore(sessions.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	store.maxRuns = 2
	ctx := context.Background()
	dataset, _, err := store.PutDataset(ctx, sqlEvaluationDataset())
	if err != nil {
		t.Fatal(err)
	}
	run := func(id string) evaluation.RunResult {
		item := sqlEvaluationRun(dataset)
		item.ID = id
		return item
	}
	first := run("eval_sql_stored_1")
	if err := store.CreateRun(ctx, first); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateRun(ctx, run("eval_sql_stored_2")); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateRun(ctx, run("eval_sql_stored_3")); err == nil {
		t.Fatal("stored evaluation run overflow was accepted")
	} else if !strings.Contains(err.Error(), "evaluation runs exceed maximum of 2") {
		t.Fatalf("unexpected overflow error: %v", err)
	}
	if err := store.CreateRun(ctx, first); err != nil {
		t.Fatalf("existing evaluation run was blocked by the cap: %v", err)
	}
}

func TestSQLEvaluationStoreRejectsCaseOverflow(t *testing.T) {
	sessions := newTestSQLStore(t)
	store, err := NewSQLEvaluationStore(sessions.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	store.maxCases = 2
	ctx := context.Background()
	dataset := sqlEvaluationDataset()
	dataset.Cases = []evaluation.Case{
		dataset.Cases[0],
		{ID: "case-sql-2", Input: "two", Assertions: dataset.Cases[0].Assertions},
		{ID: "case-sql-3", Input: "three", Assertions: dataset.Cases[0].Assertions},
	}
	stored, _, err := store.PutDataset(ctx, dataset)
	if err != nil {
		t.Fatal(err)
	}
	run := sqlEvaluationRun(stored)
	run.TotalCases = 3
	if err := store.CreateRun(ctx, run); err != nil {
		t.Fatal(err)
	}
	first := sqlEvaluationCaseResult()
	if err := store.RecordCaseResult(ctx, run.ID, first); err != nil {
		t.Fatal(err)
	}
	second := sqlEvaluationCaseResult()
	second.CaseID = "case-sql-2"
	if err := store.RecordCaseResult(ctx, run.ID, second); err != nil {
		t.Fatal(err)
	}
	third := sqlEvaluationCaseResult()
	third.CaseID = "case-sql-3"
	if err := store.RecordCaseResult(ctx, run.ID, third); err == nil {
		t.Fatal("evaluation case overflow was accepted")
	} else if !strings.Contains(err.Error(), "case results exceed maximum of 2") {
		t.Fatalf("unexpected overflow error: %v", err)
	}
	if err := store.RecordCaseResult(ctx, run.ID, first); err != nil {
		t.Fatalf("existing case result was blocked by the cap: %v", err)
	}
}
