package server

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"testing"
	"time"

	core "github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/storage"
)

type captureRunStats struct {
	stat  storage.RunStat
	calls int
}

func (s *captureRunStats) RecordRunStat(_ context.Context, stat storage.RunStat) error {
	s.stat, s.calls = stat, s.calls+1
	return nil
}
func (*captureRunStats) Metrics(context.Context, string) (storage.RunMetrics, error) {
	return storage.RunMetrics{}, nil
}

type failingRunStats struct{ calls int }

func (s *failingRunStats) RecordRunStat(context.Context, storage.RunStat) error {
	s.calls++
	return errors.New("metrics store unavailable")
}
func (*failingRunStats) Metrics(context.Context, string) (storage.RunMetrics, error) {
	return storage.RunMetrics{}, nil
}

func TestRecordRunStatSumsLegacyAndIdentifiedUsage(t *testing.T) {
	fixture := newRunWorkerFixture(t)
	const runID = "run-usage-stat"
	for _, entry := range []struct {
		kind core.SessionEventType
		data any
	}{
		{core.EvRunStart, core.RunStartData{}},
		{core.EvRunUsage, core.RunUsageData{InputTokens: 2, OutputTokens: 1}},
		{core.EvRunUsage, core.RunUsageData{InputTokens: 3, OutputTokens: 2, InvocationID: "model:1"}},
		{core.EvRunUsage, core.RunUsageData{InputTokens: 5, OutputTokens: 4, InvocationID: "summary:0:1:2"}},
	} {
		if _, err := fixture.session.Append(runID, entry.kind, entry.data); err != nil {
			t.Fatal(err)
		}
	}
	stats := &captureRunStats{}
	fixture.server.runStats = stats
	fixture.server.recordRunStat(context.Background(), fixture.session, runID, fixture.principal, core.RunCompleted, time.Now())
	if stats.calls != 1 || stats.stat.InputTokens != 10 || stats.stat.OutputTokens != 7 {
		t.Fatalf("stat=%#v calls=%d", stats.stat, stats.calls)
	}
}

func TestSumRunUsageEventsRejectsOverflowWithoutProducingWrappedStat(t *testing.T) {
	encode := func(t *testing.T, usage core.RunUsageData) json.RawMessage {
		t.Helper()
		data, err := json.Marshal(usage)
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	_, err := sumRunUsageEvents([]core.SessionEvent{
		{RunID: "run-overflow", Type: core.EvRunUsage, Data: encode(t, core.RunUsageData{InputTokens: math.MaxInt64})},
		{RunID: "run-overflow", Type: core.EvRunUsage, Data: encode(t, core.RunUsageData{InputTokens: 1})},
	}, "run-overflow")
	if err == nil {
		t.Fatal("overflowed historical usage produced a wrapped stat")
	}
}

func TestQueuedRunStatFailureDoesNotChangeTerminalControl(t *testing.T) {
	fixture := newRunWorkerFixture(t)
	stats := &failingRunStats{}
	fixture.server.runStats = stats
	record := enqueueRunHTTP(t, fixture, "stat failure")
	claimed, err := fixture.server.RunWorkerOnce(context.Background(), "worker-stat-failure")
	if err != nil || !claimed {
		t.Fatalf("claimed=%t err=%v", claimed, err)
	}
	if stats.calls != 1 {
		t.Fatalf("run-stat calls=%d, want 1", stats.calls)
	}
	terminal, err := fixture.queue.GetRun(context.Background(), record.RunID)
	if err != nil || terminal.Status != string(core.RunCompleted) || terminal.ErrorCode != "" || terminal.CompletedAt.IsZero() {
		t.Fatalf("terminal run control=%#v err=%v", terminal, err)
	}
}
