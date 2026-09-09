package main

import (
	"context"
	"testing"
)

func TestRunUsesCatalogThenProgramExecuteAndPreservesAggregate(t *testing.T) {
	report, err := Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !report.SameValue || !sameStrings(report.Direct.FinalValue, expectedValue(2)) || !sameStrings(report.Programmatic.FinalValue, expectedValue(2)) {
		t.Fatalf("final values=%#v", report)
	}
	if report.Direct.ModelRounds != 4 || report.Programmatic.ModelRounds != 3 {
		t.Fatalf("model rounds direct=%d programmatic=%d", report.Direct.ModelRounds, report.Programmatic.ModelRounds)
	}
	if report.Direct.FixtureToolCalls != 3 || report.Programmatic.FixtureToolCalls != 3 {
		t.Fatalf("fixture tools direct=%d programmatic=%d", report.Direct.FixtureToolCalls, report.Programmatic.FixtureToolCalls)
	}
	if report.Direct.JournalInvocations != 3 || report.Programmatic.JournalInvocations != 5 {
		t.Fatalf("journal calls direct=%d programmatic=%d", report.Direct.JournalInvocations, report.Programmatic.JournalInvocations)
	}
	if report.Direct.ModelHistoryBytes <= 0 || report.Programmatic.ModelHistoryBytes <= 0 {
		t.Fatalf("history bytes=%#v", report)
	}
	if report.Direct.ModelRequestBytes <= report.Direct.ModelHistoryBytes || report.Programmatic.ModelRequestBytes <= report.Programmatic.ModelHistoryBytes {
		t.Fatalf("request bytes should include more than message history: %#v", report)
	}
}

func TestRunWithActiveRowsShowsSimpleAndBatchShapes(t *testing.T) {
	for _, activeRows := range []int{1, 8} {
		report, err := RunWithActiveRows(context.Background(), activeRows)
		if err != nil {
			t.Fatalf("rows=%d: %v", activeRows, err)
		}
		if !report.SameValue || !sameStrings(report.Direct.FinalValue, expectedValue(activeRows)) {
			t.Fatalf("rows=%d report=%#v", activeRows, report)
		}
		t.Logf("active_rows=%d direct=%+v programmatic=%+v same_value=%v", activeRows, report.Direct, report.Programmatic, report.SameValue)
		if want := activeRows + 2; report.Direct.ModelRounds != want || report.Programmatic.ModelRounds != 3 {
			t.Fatalf("rows=%d rounds direct=%d programmatic=%d", activeRows, report.Direct.ModelRounds, report.Programmatic.ModelRounds)
		}
		if want := activeRows + 1; report.Direct.FixtureToolCalls != want || report.Programmatic.FixtureToolCalls != want {
			t.Fatalf("rows=%d fixture tools direct=%d programmatic=%d", activeRows, report.Direct.FixtureToolCalls, report.Programmatic.FixtureToolCalls)
		}
	}
}

// BenchmarkLocalProtocolOverhead measures only this process's deterministic
// Runtime, journal, adapters, and fixture tools. It deliberately excludes a
// provider request, model quality, and network latency.
func BenchmarkLocalProtocolOverhead(b *testing.B) {
	ctx := context.Background()
	b.Run("direct_successive_decisions", func(b *testing.B) {
		for index := 0; index < b.N; index++ {
			if _, err := runDirect(ctx, 8); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("programmatic_catalog_execute", func(b *testing.B) {
		for index := 0; index < b.N; index++ {
			if _, err := runProgrammatic(ctx, 8); err != nil {
				b.Fatal(err)
			}
		}
	})
}
