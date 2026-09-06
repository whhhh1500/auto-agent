package perfp0

import (
	"context"
	"errors"
	"fmt"
	"math"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
)

func TestRunConcurrentExecutesEveryWorker(t *testing.T) {
	var calls atomic.Int64
	result, err := runConcurrent(context.Background(), time.Millisecond, "test", "small", 1, 10, func(context.Context, int) error {
		calls.Add(1)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 10 || result.Operations != 10 || result.Errors != 0 {
		t.Fatalf("calls=%d result=%+v", calls.Load(), result)
	}
	if result.Latency.Samples != 10 {
		t.Fatalf("latency=%+v", result.Latency)
	}
}

func TestErrorSamplesAreBoundedAndRedacted(t *testing.T) {
	result, err := runConcurrent(context.Background(), time.Millisecond, "control_health", "", 0, 100, func(context.Context, int) error {
		return errors.New("private endpoint token=should-not-appear")
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Errors != 100 || result.ErrorKinds["cold_start_transport"] != 100 || len(result.ErrorSamples) != 1 {
		t.Fatalf("error evidence=%+v", result)
	}
	for _, sample := range result.ErrorSamples {
		if sample != "cold_start_transport" || strings.Contains(sample, "token") {
			t.Fatalf("unredacted or unstable error sample=%q", sample)
		}
	}
}

func TestPreparedWorkersAllExistBeforeMeasuredRun(t *testing.T) {
	const workers = 8
	var prepared atomic.Int64
	var executed atomic.Int64
	result, err := runPreparedConcurrent(context.Background(), time.Millisecond, "prepared-test", "small", 1024, workers, func(worker int) (func(context.Context) error, error) {
		if worker < 0 || worker >= workers {
			return nil, fmt.Errorf("unexpected worker %d", worker)
		}
		prepared.Add(1)
		fixture := make([]byte, 1<<20)
		return func(context.Context) error {
			if prepared.Load() != workers {
				return fmt.Errorf("worker ran before all fixtures were prepared")
			}
			_ = fixture[0]
			executed.Add(1)
			return nil
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if prepared.Load() != workers || executed.Load() != workers || result.Operations != workers || result.Errors != 0 {
		t.Fatalf("prepared=%d executed=%d result=%+v", prepared.Load(), executed.Load(), result)
	}
	if result.Fixture.Workers != workers || result.Fixture.HeapRetainedApprox == 0 {
		t.Fatalf("fixture baseline missing: %+v", result.Fixture)
	}
	// The 8 MiB fixture is retained before the sampler's runtime baseline;
	// setup must not be charged as operation allocation. The harness itself has
	// a small measurement overhead, so use a conservative upper bound.
	if result.Runtime.TotalAllocDelta >= 8<<20 {
		t.Fatalf("fixture allocation charged to RunTurn interval: %+v", result.Runtime)
	}
}

// Production-equivalent max fixture: setup is excluded, but the session and
// agent remain live for the measured operation, matching the perf harness.
func BenchmarkProductionMaxRunTurn(b *testing.B) {
	s := Scenario{Name: "max", PayloadBytes: 1 << 20, Messages: 16, Tools: 512}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := runProductionAgent(context.Background(), s, i); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPreparedProductionMaxRunTurn excludes Session/Agent/TurnInput
// construction from the timed interval, matching the prepared matrix case.
func BenchmarkPreparedProductionMaxRunTurn(b *testing.B) {
	prepared, err := prepareProductionAgent(Scenario{Name: "prepared-max", PayloadBytes: 1 << 20, Messages: 4}, 0)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		input := prepared.input
		input.RunID = fmt.Sprintf("prepared-bench-%d", i)
		if result, err := prepared.agent.RunTurn(context.Background(), input); err != nil {
			b.Fatal(err)
		} else if result.Status != core.RunCompleted {
			b.Fatalf("status=%s", result.Status)
		}
	}
}

func BenchmarkPreparedProductionTypical500RunTurn(b *testing.B) {
	benchmarkPreparedProduction500(b, Scenario{Name: "prepared-typical", PayloadBytes: 16 << 10, Messages: 32, Tools: 128})
}

func BenchmarkPreparedProductionMax500RunTurn(b *testing.B) {
	benchmarkPreparedProduction500(b, Scenario{Name: "prepared-max-500", PayloadBytes: 1 << 20, Messages: 4, Tools: 512})
}

func benchmarkPreparedProduction500(b *testing.B, scenario Scenario) {
	const workers = 500
	prepared := make([]*preparedProductionAgent, workers)
	for worker := range prepared {
		var err error
		prepared[worker], err = prepareProductionAgent(scenario, worker)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		start := make(chan struct{})
		errors := make(chan error, workers)
		var group sync.WaitGroup
		for worker, fixture := range prepared {
			group.Add(1)
			go func(worker int, fixture *preparedProductionAgent) {
				defer group.Done()
				<-start
				input := fixture.input
				input.RunID = fmt.Sprintf("prepared-bench-%d-%d", iteration, worker)
				result, err := fixture.agent.RunTurn(context.Background(), input)
				if err == nil && result.Status != core.RunCompleted {
					err = fmt.Errorf("status=%s", result.Status)
				}
				if err != nil {
					errors <- err
				}
			}(worker, fixture)
		}
		close(start)
		group.Wait()
		close(errors)
		for err := range errors {
			b.Fatal(err)
		}
	}
}

func TestRuntimeStatsUsesPauseTotalNanoseconds(t *testing.T) {
	before := runtime.MemStats{HeapAlloc: 2, TotalAlloc: 10, Mallocs: 4, Frees: 1, PauseTotalNs: 100, NumGC: 7}
	after := runtime.MemStats{HeapAlloc: 9, TotalAlloc: 30, Mallocs: 10, Frees: 4, PauseTotalNs: 350, NumGC: 9}
	stats := runtimeStats(caseMeasurement{RuntimeBefore: before, RuntimeAfter: after, HeapMaxSeen: 11, GoroutineMax: 12})
	if stats.PauseTotalNsDelta != 250 || stats.NumGCDelta != 2 {
		t.Fatalf("runtime stats=%+v", stats)
	}
}

func TestCaseSamplerTracksSampledPeaks(t *testing.T) {
	sampler := newCaseSampler(time.Millisecond)
	payload := make([]byte, 1<<20)
	for index := range payload {
		payload[index] = byte(index)
	}
	time.Sleep(3 * time.Millisecond)
	measurement := sampler.Stop()
	if measurement.HeapMaxSeen < measurement.RuntimeBefore.HeapAlloc {
		t.Fatalf("heap max=%d before=%d", measurement.HeapMaxSeen, measurement.RuntimeBefore.HeapAlloc)
	}
	if measurement.GoroutineMax < 1 {
		t.Fatalf("goroutine peak=%d", measurement.GoroutineMax)
	}
	_ = payload[0]
}

func TestProcessStatsUsesBeforeAfterPeakAndCPUTime(t *testing.T) {
	stats := processStatsFrom(
		processSample{RSSBytes: 100, CPUKernelNanoseconds: 4, CPUUserNanoseconds: 6, CPUTimeNanoseconds: 10},
		processSample{RSSBytes: 200, CPUKernelNanoseconds: 12, CPUUserNanoseconds: 18, CPUTimeNanoseconds: 30},
		processSample{RSSBytes: 250, CPUTimeNanoseconds: 40}, nil, nil, 100*time.Millisecond,
	)
	if !stats.Supported || stats.RSSBeforeBytes == nil || *stats.RSSBeforeBytes != 100 ||
		stats.RSSAfterBytes == nil || *stats.RSSAfterBytes != 200 || stats.RSSPeakBytes == nil || *stats.RSSPeakBytes != 250 ||
		stats.CPUTimeDeltaNanoseconds == nil || *stats.CPUTimeDeltaNanoseconds != 20 || stats.CPUKernelTimeDeltaNs == nil || *stats.CPUKernelTimeDeltaNs != 8 || stats.CPUUserTimeDeltaNs == nil || *stats.CPUUserTimeDeltaNs != 12 || stats.CPUPercent == nil || math.Abs(*stats.CPUPercent-0.00002) > 1e-12 {
		t.Fatalf("process stats before=%v after=%v peak=%v cpu=%v cpu%%=%v", *stats.RSSBeforeBytes, *stats.RSSAfterBytes, *stats.RSSPeakBytes, *stats.CPUTimeDeltaNanoseconds, *stats.CPUPercent)
	}
}

func TestBuildSessionProjectsAndCompacts(t *testing.T) {
	session, err := buildSession(Scenario{Name: "test", PayloadBytes: 4096, Messages: 16}, 1)
	if err != nil {
		t.Fatal(err)
	}
	messages, err := session.DeriveMessages()
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) == 0 {
		t.Fatal("projection is empty")
	}
	compacted := core.RecentTurnsCompactor{MaxMessages: 8, MaxToolResultChars: 512}.Compact(messages)
	if len(compacted) == 0 || len(compacted) >= len(messages) {
		t.Fatalf("compaction did not reduce projection: before=%d after=%d", len(messages), len(compacted))
	}
}

func TestRunProductionAgentUsesFusedRecentCompaction(t *testing.T) {
	if err := runProductionAgent(context.Background(), Scenario{Name: "production", PayloadBytes: 1024, Messages: 16}, 1); err != nil {
		t.Fatal(err)
	}
}

func TestReportFormatsContainUnsupportedProcessEvidence(t *testing.T) {
	report := Report{Version: reportVersion, Process: ProcessStats{Unsupported: "portable baseline"}}
	markdown := report.Markdown()
	if !strings.Contains(markdown, "unsupported") || !strings.Contains(markdown, "no admission") {
		t.Fatalf("markdown=%s", markdown)
	}
	encoded, err := report.JSON()
	if err != nil || !strings.Contains(string(encoded), "unsupported_reason") {
		t.Fatalf("json=%s err=%v", encoded, err)
	}
}

func TestFixtureRSSDeltaPreservesSignedDecrease(t *testing.T) {
	delta := int64(-128)
	report := Report{Version: reportVersion, Process: ProcessStats{Supported: true}, Cases: []CaseResult{{Fixture: FixtureStats{RSSDeltaBytes: &delta, RSSSupported: true}}}}
	markdown := report.Markdown()
	if !strings.Contains(markdown, "| -128 |") {
		t.Fatalf("signed fixture delta was lost: %s", markdown)
	}
}
