// Package perfp0 contains the deterministic, local-only Perf-P0 baseline.
//
// The package deliberately measures work directly. It has no admission,
// queue, backpressure, rate-limit, or load-shed behavior; a concurrency value
// means that many goroutines execute the operation at the same time.
package perfp0

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	core "github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/extensions/toollib"
	"github.com/whhhh1500/auto-agent/pkg/server"
)

const reportVersion = 3

// minMeasuredDuration avoids reporting zero-duration work as infinite
// throughput on platforms whose timer resolution is coarser than the tiny
// operation being measured. Such samples are reported at the floor instead
// of claiming false nanosecond precision.
const minMeasuredDuration = time.Microsecond

// Scenario is one bounded input shape. MaxPayloadBytes is the largest legal
// tool argument payload (core.MaxToolArgumentBytes), not the larger durable
// session event hard cap. This keeps the default max case executable at 500
// genuine concurrent workers while still exercising a production limit.
type Scenario struct {
	Name         string `json:"name"`
	PayloadBytes int    `json:"payload_bytes"`
	Messages     int    `json:"messages"`
	Tools        int    `json:"tools"`
}

// DefaultScenarios returns the reproducible small, typical, and max inputs.
func DefaultScenarios() []Scenario {
	return []Scenario{
		{Name: "small", PayloadBytes: 1 << 10, Messages: 8, Tools: 32},
		{Name: "typical", PayloadBytes: 16 << 10, Messages: 32, Tools: 128},
		{Name: "max", PayloadBytes: core.MaxToolArgumentBytes, Messages: 4, Tools: 512},
	}
}

// Config controls one baseline run. Every concurrency value is executed as a
// real concurrent batch; no worker is hidden behind a scheduler gate.
type Config struct {
	Concurrency    []int
	Scenarios      []Scenario
	Soak           time.Duration
	SoakWorkers    int
	SampleInterval time.Duration
}

// DefaultConfig returns the P0 matrix required by the performance plan.
func DefaultConfig() Config {
	return Config{Concurrency: []int{10, 100, 500}, Scenarios: DefaultScenarios(), SampleInterval: time.Millisecond}
}

// Report is both the JSON contract and the input to Markdown.
type Report struct {
	Version         int                `json:"version"`
	StartedAt       time.Time          `json:"started_at"`
	Duration        time.Duration      `json:"duration_ns"`
	Cases           []CaseResult       `json:"cases"`
	Process         ProcessStats       `json:"process"`
	Catalogs        []CatalogFootprint `json:"tool_catalogs"`
	ProcessIsolated bool               `json:"process_isolated"`
}

// CatalogFootprint separates resident tool metadata/index cost from search
// throughput. Values are approximate heap observations after an explicit GC;
// allocator and runtime noise can still affect them.
type CatalogFootprint struct {
	Scenario       string `json:"scenario"`
	ToolCount      int    `json:"tool_count"`
	HeapBeforeGC   uint64 `json:"heap_before_gc_bytes"`
	HeapAfterBuild uint64 `json:"heap_after_build_bytes"`
	HeapAfterGC    uint64 `json:"heap_after_gc_bytes"`
	RetainedApprox uint64 `json:"retained_approx_bytes"`
	Approximate    bool   `json:"approximate"`
}

// CaseResult records one workload and one concurrency level.
type CaseResult struct {
	Workload     string         `json:"workload"`
	Scenario     string         `json:"scenario,omitempty"`
	Concurrency  int            `json:"concurrency"`
	PayloadBytes int            `json:"payload_bytes"`
	Operations   int            `json:"operations"`
	Errors       int            `json:"errors"`
	ErrorKinds   map[string]int `json:"error_kinds,omitempty"`
	ErrorSamples []string       `json:"error_samples,omitempty"`
	Duration     time.Duration  `json:"duration_ns"`
	Throughput   float64        `json:"throughput_ops_sec"`
	Latency      LatencyStats   `json:"latency"`
	Runtime      RuntimeStats   `json:"runtime"`
	Process      ProcessStats   `json:"process"`
	Fixture      FixtureStats   `json:"fixture"`
	Warmup       WarmupStats    `json:"warmup,omitempty"`
}

// WarmupStats records the bounded connection warmup that precedes a soak's
// measured keepalive window. Warmup is observable but excluded from measured
// operations, latency, runtime, and process deltas.
type WarmupStats struct {
	Operations   int            `json:"operations"`
	Errors       int            `json:"errors"`
	ErrorKinds   map[string]int `json:"error_kinds,omitempty"`
	ErrorSamples []string       `json:"error_samples,omitempty"`
	Duration     time.Duration  `json:"duration_ns"`
}

// FixtureStats describes the resident setup retained before the measured
// operation begins. It is deliberately separate from RuntimeStats so setup
// allocations cannot be mistaken for RunTurn allocations.
type FixtureStats struct {
	Workers             int     `json:"workers"`
	HeapBeforeSetup     uint64  `json:"heap_before_setup_bytes"`
	HeapAfterSetup      uint64  `json:"heap_after_setup_bytes"`
	HeapRetainedApprox  uint64  `json:"heap_retained_approx_bytes"`
	RSSBeforeSetupBytes *uint64 `json:"rss_before_setup_bytes"`
	RSSAfterSetupBytes  *uint64 `json:"rss_after_setup_bytes"`
	RSSDeltaBytes       *int64  `json:"rss_delta_bytes"`
	RSSSupported        bool    `json:"rss_supported"`
	RSSUnsupported      string  `json:"rss_unsupported_reason,omitempty"`
}

// LatencyStats uses nearest-rank percentiles over recorded operations. The raw
// sample list is intentionally not retained.
type LatencyStats struct {
	Samples int           `json:"samples"`
	P50     time.Duration `json:"p50_ns"`
	P95     time.Duration `json:"p95_ns"`
	P99     time.Duration `json:"p99_ns"`
}

// RuntimeStats is process-local Go runtime evidence. Heap and goroutine maxima
// are sampled during the case and may miss a transient between ticks.
type RuntimeStats struct {
	HeapAllocBefore   uint64 `json:"heap_alloc_before_bytes"`
	HeapAllocAfter    uint64 `json:"heap_alloc_after_bytes"`
	HeapAllocMaxSeen  uint64 `json:"heap_alloc_max_seen_bytes"`
	TotalAllocDelta   uint64 `json:"total_alloc_delta_bytes"`
	MallocsDelta      uint64 `json:"mallocs_delta"`
	FreesDelta        uint64 `json:"frees_delta"`
	NumGoroutinePeak  int    `json:"goroutines_peak"`
	PauseTotalNsDelta uint64 `json:"gc_pause_total_ns_delta"`
	NumGCDelta        uint32 `json:"gc_delta"`
}

type ProcessStats struct {
	RSSBeforeBytes          *uint64  `json:"rss_before_bytes"`
	RSSAfterBytes           *uint64  `json:"rss_after_bytes"`
	RSSPeakBytes            *uint64  `json:"rss_peak_bytes"`
	CPUPercent              *float64 `json:"cpu_percent"`
	CPUTimeDeltaNanoseconds *uint64  `json:"cpu_time_delta_nanoseconds"`
	CPUKernelTimeDeltaNs    *uint64  `json:"cpu_kernel_time_delta_nanoseconds"`
	CPUUserTimeDeltaNs      *uint64  `json:"cpu_user_time_delta_nanoseconds"`
	Supported               bool     `json:"supported"`
	Unsupported             string   `json:"unsupported_reason,omitempty"`
}

// Run executes the local deterministic matrix. The only external-facing
// request is an httptest server bound to loopback by the standard library.
func Run(ctx context.Context, cfg Config) (Report, error) {
	cfg = normalizeConfig(cfg)
	started := time.Now().UTC()
	api, err := newControlPlane()
	if err != nil {
		return Report{}, err
	}
	defer api.Close()
	maxConcurrency := 1
	for _, n := range cfg.Concurrency {
		if n > maxConcurrency {
			maxConcurrency = n
		}
	}
	controlTransport := http.DefaultTransport.(*http.Transport).Clone()
	controlTransport.MaxIdleConns = maxConcurrency
	controlTransport.MaxIdleConnsPerHost = maxConcurrency
	controlTransport.MaxConnsPerHost = maxConcurrency
	controlClient := &http.Client{Transport: controlTransport, Timeout: 10 * time.Second}
	defer controlTransport.CloseIdleConnections()
	runSampler := newCaseSampler(cfg.SampleInterval)
	defer func() {
		if runSampler != nil {
			runSampler.Stop()
		}
	}()

	report := Report{
		Version: reportVersion, StartedAt: started,
	}
	for _, concurrency := range cfg.Concurrency {
		result, err := runConcurrent(ctx, cfg.SampleInterval, "control_health", "", 0, concurrency, func(ctx context.Context, _ int) error {
			request, err := http.NewRequestWithContext(ctx, http.MethodGet, api.URL+"/healthz", nil)
			if err != nil {
				return err
			}
			response, err := controlClient.Do(request)
			if err != nil {
				return err
			}
			defer response.Body.Close()
			_, _ = io.Copy(io.Discard, response.Body)
			if response.StatusCode != http.StatusOK {
				return fmt.Errorf("health status %d", response.StatusCode)
			}
			return nil
		})
		if err != nil {
			return Report{}, err
		}
		report.Cases = append(report.Cases, result)
	}

	for _, scenario := range cfg.Scenarios {
		catalog, footprint := measureCatalog(scenario)
		report.Catalogs = append(report.Catalogs, footprint)
		for _, concurrency := range cfg.Concurrency {
			result, err := runConcurrent(ctx, cfg.SampleInterval, "legacy_session_projection_compaction", scenario.Name, scenario.PayloadBytes, concurrency, func(_ context.Context, worker int) error {
				session, err := buildSession(scenario, worker)
				if err != nil {
					return err
				}
				messages, err := session.DeriveMessages()
				if err != nil {
					return err
				}
				compacted := core.RecentTurnsCompactor{MaxMessages: 8, MaxToolResultChars: 2048}.Compact(messages)
				if len(compacted) == 0 {
					return fmt.Errorf("empty compacted projection")
				}
				return nil
			})
			if err != nil {
				return Report{}, err
			}
			report.Cases = append(report.Cases, result)

			result, err = runConcurrent(ctx, cfg.SampleInterval, "agent_production_recent_compaction_setup_inclusive", scenario.Name, scenario.PayloadBytes, concurrency, func(ctx context.Context, worker int) error {
				return runProductionAgent(ctx, scenario, worker)
			})
			if err != nil {
				return Report{}, err
			}
			report.Cases = append(report.Cases, result)

			result, err = runPreparedConcurrent(ctx, cfg.SampleInterval, "agent_production_recent_compaction", scenario.Name, scenario.PayloadBytes, concurrency, func(worker int) (func(context.Context) error, error) {
				prepared, err := prepareProductionAgent(scenario, worker)
				if err != nil {
					return nil, err
				}
				return prepared.run, nil
			})
			if err != nil {
				return Report{}, err
			}
			report.Cases = append(report.Cases, result)

			result, err = runConcurrent(ctx, cfg.SampleInterval, "tool_search", scenario.Name, scenario.PayloadBytes, concurrency, func(_ context.Context, _ int) error {
				hits := catalog.Search("send email to an address", 8)
				runtime.KeepAlive(catalog)
				if len(hits) == 0 {
					return fmt.Errorf("tool search returned no hits")
				}
				return nil
			})
			if err != nil {
				return Report{}, err
			}
			report.Cases = append(report.Cases, result)
		}
	}

	if cfg.Soak > 0 {
		workers := cfg.SoakWorkers
		if workers <= 0 {
			workers = 100
		}
		result, err := runSoak(ctx, cfg.SampleInterval, api.URL+"/healthz", workers, cfg.Soak)
		if err != nil {
			return Report{}, err
		}
		report.Cases = append(report.Cases, result)
	}
	report.Duration = time.Since(started)
	runMeasurement := runSampler.Stop()
	runSampler = nil
	report.Process = processStatsFrom(runMeasurement.ProcessBefore, runMeasurement.ProcessAfter, runMeasurement.ProcessPeak, runMeasurement.ProcessBeforeErr, runMeasurement.ProcessAfterErr, report.Duration)
	return report, nil
}

func normalizeConfig(cfg Config) Config {
	if len(cfg.Concurrency) == 0 {
		cfg.Concurrency = DefaultConfig().Concurrency
	}
	if len(cfg.Scenarios) == 0 {
		cfg.Scenarios = DefaultScenarios()
	}
	if cfg.SampleInterval <= 0 {
		cfg.SampleInterval = time.Millisecond
	}
	for i := range cfg.Concurrency {
		if cfg.Concurrency[i] < 1 {
			cfg.Concurrency[i] = 1
		}
	}
	return cfg
}

func newControlPlane() (*httptest.Server, error) {
	api, err := server.New(server.Config{
		Runtime:  &core.Runtime{},
		Sessions: core.NewMemorySessionStore(),
		Authenticator: server.AuthenticatorFunc(func(*http.Request) (core.Principal, error) {
			return core.Principal{}, nil
		}),
	})
	if err != nil {
		return nil, err
	}
	return httptest.NewServer(api.Handler()), nil
}

func runConcurrent(ctx context.Context, sampleInterval time.Duration, workload, scenario string, payloadBytes, concurrency int, operation func(context.Context, int) error) (CaseResult, error) {
	return runConcurrentWithFixture(ctx, sampleInterval, workload, scenario, payloadBytes, concurrency, FixtureStats{}, operation)
}

func runConcurrentWithFixture(ctx context.Context, sampleInterval time.Duration, workload, scenario string, payloadBytes, concurrency int, fixture FixtureStats, operation func(context.Context, int) error) (CaseResult, error) {
	if concurrency < 1 {
		return CaseResult{}, fmt.Errorf("concurrency must be positive")
	}
	// Isolate Go live-heap observations from garbage left by the preceding
	// workload. This GC is deliberately outside case timing; RSS still reflects
	// process-wide allocator/OS retention and is not isolated by this step.
	runtime.GC()
	runtime.KeepAlive(operation)
	sampler := newCaseSampler(sampleInterval)
	started := time.Now()
	start := make(chan struct{})
	latencies := make(chan time.Duration, concurrency)
	errorKinds := make(chan string, concurrency)
	var errorsCount atomic.Int64
	var wg sync.WaitGroup
	for worker := 0; worker < concurrency; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			begin := time.Now()
			if err := operation(ctx, worker); err != nil {
				errorsCount.Add(1)
				errorKinds <- classifyWorkloadError(workload, err)
			}
			latency := time.Since(begin)
			if latency < minMeasuredDuration {
				latency = minMeasuredDuration
			}
			latencies <- latency
		}()
	}
	close(start)
	wg.Wait()
	close(latencies)
	close(errorKinds)
	measurement := sampler.Stop()

	samples := make([]time.Duration, 0, concurrency)
	for latency := range latencies {
		samples = append(samples, latency)
	}
	kinds := map[string]int{}
	errorSamples := make([]string, 0, 8)
	for kind := range errorKinds {
		kinds[kind]++
		errorSamples = appendErrorSample(errorSamples, kind)
	}
	if len(kinds) == 0 {
		kinds = nil
	}
	if len(errorSamples) == 0 {
		errorSamples = nil
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	duration := time.Since(started)
	if duration < minMeasuredDuration {
		duration = minMeasuredDuration
	}
	result := CaseResult{
		Workload: workload, Scenario: scenario, Concurrency: concurrency,
		PayloadBytes: payloadBytes, Operations: concurrency,
		Errors: int(errorsCount.Load()), ErrorKinds: kinds, ErrorSamples: errorSamples, Duration: duration,
		Throughput: operationsPerSecond(int64(concurrency), duration),
		Latency:    percentileStats(samples),
		Runtime:    runtimeStats(measurement),
		Process:    processStatsFrom(measurement.ProcessBefore, measurement.ProcessAfter, measurement.ProcessPeak, measurement.ProcessBeforeErr, measurement.ProcessAfterErr, duration),
		Fixture:    fixture,
	}
	return result, nil
}

// runPreparedConcurrent builds every worker fixture before sampling starts,
// then releases all prepared workers through the same barrier used by the
// ordinary concurrent harness. The measured interval contains only the
// operation returned by prepare; setup is reported in FixtureStats.
func runPreparedConcurrent(ctx context.Context, sampleInterval time.Duration, workload, scenario string, payloadBytes, concurrency int, prepare func(int) (func(context.Context) error, error)) (CaseResult, error) {
	if concurrency < 1 {
		return CaseResult{}, fmt.Errorf("concurrency must be positive")
	}
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	processBefore, processBeforeErr := readProcessSample()
	operations := make([]func(context.Context) error, concurrency)
	for worker := 0; worker < concurrency; worker++ {
		operation, err := prepare(worker)
		if err != nil {
			return CaseResult{}, err
		}
		if operation == nil {
			return CaseResult{}, fmt.Errorf("prepared worker %d has nil operation", worker)
		}
		operations[worker] = operation
	}
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	processAfter, processAfterErr := readProcessSample()
	fixture := FixtureStats{Workers: concurrency, HeapBeforeSetup: before.HeapAlloc, HeapAfterSetup: after.HeapAlloc}
	if after.HeapAlloc >= before.HeapAlloc {
		fixture.HeapRetainedApprox = after.HeapAlloc - before.HeapAlloc
	}
	if processBeforeErr == nil && processAfterErr == nil {
		beforeRSS, afterRSS := processBefore.RSSBytes, processAfter.RSSBytes
		fixture.RSSBeforeSetupBytes = &beforeRSS
		fixture.RSSAfterSetupBytes = &afterRSS
		delta := int64(afterRSS) - int64(beforeRSS)
		fixture.RSSDeltaBytes = &delta
		fixture.RSSSupported = true
	} else {
		fixture.RSSUnsupported = processErrorText(firstError(processBeforeErr, processAfterErr))
	}
	return runConcurrentWithFixture(ctx, sampleInterval, workload, scenario, payloadBytes, concurrency, fixture, func(ctx context.Context, worker int) error {
		return operations[worker](ctx)
	})
}

func classifyError(err error) string {
	if err == nil {
		return "none"
	}
	message := strings.ToLower(err.Error())
	switch {
	case strings.Contains(message, "status "):
		return "http_status"
	case strings.Contains(message, "context"):
		return "context"
	default:
		return "transport_or_operation"
	}
}

// classifyWorkloadError keeps transport failures observable without exposing
// provider or socket text. A control_health case is a fresh child-process
// connection burst; control_health_soak reuses keepalive connections.
func classifyWorkloadError(workload string, err error) string {
	kind := classifyError(err)
	if kind != "transport_or_operation" {
		return kind
	}
	switch workload {
	case "control_health":
		return "cold_start_transport"
	case "control_health_warmup":
		return "warmup_transport"
	case "control_health_soak":
		return "steady_keepalive_transport"
	default:
		return kind
	}
}

func appendErrorSample(samples []string, kind string) []string {
	if len(samples) >= 8 {
		return samples
	}
	for _, sample := range samples {
		if sample == kind {
			return samples
		}
	}
	return append(samples, kind)
}

const soakWarmupDuration = 250 * time.Millisecond

type soakPhaseResult struct {
	Operations   int64
	Errors       int64
	ErrorKinds   map[string]int
	ErrorSamples []string
	Samples      []time.Duration
	Duration     time.Duration
}

// runSoakPhase uses the caller's transport and a fixed worker batch. Keeping
// the transport alive across warmup and measurement makes the second phase a
// keepalive observation instead of another connection cold-start test.
func runSoakPhase(ctx context.Context, client *http.Client, endpoint string, concurrency int, duration time.Duration, collectLatency bool, workload string) soakPhaseResult {
	started := time.Now()
	deadline := started.Add(duration)
	var operations atomic.Int64
	var errorsCount atomic.Int64
	var samplesMu sync.Mutex
	var samples []time.Duration
	errorKinds := map[string]int{}
	errorSamples := make([]string, 0, 8)
	var errorsMu sync.Mutex
	var wg sync.WaitGroup
	for worker := 0; worker < concurrency; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			const sampleCap = 256
			localSamples := make([]time.Duration, 0, sampleCap)
			var localCount uint64
			for time.Now().Before(deadline) && ctx.Err() == nil {
				operationStarted := time.Now()
				request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
				if err != nil {
					errorsCount.Add(1)
					errorsMu.Lock()
					kind := classifyWorkloadError(workload, err)
					errorKinds[kind]++
					errorSamples = appendErrorSample(errorSamples, kind)
					errorsMu.Unlock()
					continue
				}
				response, err := client.Do(request)
				if err != nil {
					errorsCount.Add(1)
					errorsMu.Lock()
					kind := classifyWorkloadError(workload, err)
					errorKinds[kind]++
					errorSamples = appendErrorSample(errorSamples, kind)
					errorsMu.Unlock()
					continue
				}
				if response.StatusCode != http.StatusOK {
					errorsCount.Add(1)
					errorsMu.Lock()
					errorKinds["http_status"]++
					errorSamples = appendErrorSample(errorSamples, "http_status")
					errorsMu.Unlock()
				}
				_, _ = io.Copy(io.Discard, response.Body)
				_ = response.Body.Close()
				operations.Add(1)
				if collectLatency {
					latency := time.Since(operationStarted)
					if latency < minMeasuredDuration {
						latency = minMeasuredDuration
					}
					localCount++
					if len(localSamples) < sampleCap {
						localSamples = append(localSamples, latency)
					} else {
						localSamples[localCount%sampleCap] = latency
					}
				}
			}
			if collectLatency {
				samplesMu.Lock()
				samples = append(samples, localSamples...)
				samplesMu.Unlock()
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(started)
	if elapsed < minMeasuredDuration {
		elapsed = minMeasuredDuration
	}
	if len(errorKinds) == 0 {
		errorKinds = nil
	}
	if len(errorSamples) == 0 {
		errorSamples = nil
	}
	return soakPhaseResult{Operations: operations.Load(), Errors: errorsCount.Load(), ErrorKinds: errorKinds, ErrorSamples: errorSamples, Samples: samples, Duration: elapsed}
}

func runSoak(ctx context.Context, sampleInterval time.Duration, endpoint string, concurrency int, duration time.Duration) (CaseResult, error) {
	if ctx == nil || concurrency < 1 || duration <= 0 {
		return CaseResult{}, fmt.Errorf("soak context, concurrency and duration must be positive")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = concurrency
	transport.MaxIdleConnsPerHost = concurrency
	transport.MaxConnsPerHost = concurrency
	transport.IdleConnTimeout = 30 * time.Second
	client := &http.Client{Transport: transport, Timeout: 10 * time.Second}
	defer transport.CloseIdleConnections()
	// Warmup is bounded and observable. It uses the same client/transport so
	// measured requests exercise a warmed connection pool where possible.
	warmup := runSoakPhase(ctx, client, endpoint, concurrency, soakWarmupDuration, false, "control_health_warmup")
	if ctx.Err() != nil {
		return CaseResult{Workload: "control_health_soak", Concurrency: concurrency, Warmup: WarmupStats{
			Operations: int(warmup.Operations), Errors: int(warmup.Errors), ErrorKinds: warmup.ErrorKinds, ErrorSamples: warmup.ErrorSamples, Duration: warmup.Duration,
		}}, ctx.Err()
	}
	// Keep the measured Go live-heap baseline independent of warmup. The
	// transport and its idle connections remain alive, so this does not turn
	// the measured phase back into a cold-start test.
	runtime.GC()
	sampler := newCaseSampler(sampleInterval)
	measured := runSoakPhase(ctx, client, endpoint, concurrency, duration, true, "control_health_soak")
	measurement := sampler.Stop()
	return CaseResult{
		Workload: "control_health_soak", Concurrency: concurrency,
		Operations: int(measured.Operations), Errors: int(measured.Errors), ErrorKinds: measured.ErrorKinds, ErrorSamples: measured.ErrorSamples,
		Duration: measured.Duration, Throughput: operationsPerSecond(measured.Operations, measured.Duration),
		Latency: percentileStats(measured.Samples), Runtime: runtimeStats(measurement),
		Process: processStatsFrom(measurement.ProcessBefore, measurement.ProcessAfter, measurement.ProcessPeak, measurement.ProcessBeforeErr, measurement.ProcessAfterErr, measured.Duration),
		Warmup:  WarmupStats{Operations: int(warmup.Operations), Errors: int(warmup.Errors), ErrorKinds: warmup.ErrorKinds, ErrorSamples: warmup.ErrorSamples, Duration: warmup.Duration},
	}, nil
}

func operationsPerSecond(operations int64, elapsed time.Duration) float64 {
	seconds := elapsed.Seconds()
	if seconds <= 0 {
		return 0
	}
	return float64(operations) / seconds
}

func buildSession(scenario Scenario, worker int) (*core.Session, error) {
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "perf"})
	tenant, err := global.Child(core.ScopeRef{Kind: core.ScopeTenant, ID: "baseline"})
	if err != nil {
		return nil, err
	}
	id := fmt.Sprintf("perf-%s-%d", strings.ReplaceAll(scenario.Name, "_", "-"), worker)
	scope, err := tenant.Child(core.ScopeRef{Kind: core.ScopeSession, ID: id})
	if err != nil {
		return nil, err
	}
	principal := core.Principal{SubjectID: "perf", TenantID: "baseline", Scope: tenant}
	session, err := core.NewSession(core.SessionOptions{ID: id, ProfileID: "perf.agent", Principal: principal, Scope: scope})
	if err != nil {
		return nil, err
	}
	payload := strings.Repeat("p", scenario.PayloadBytes)
	runID := "run-" + id
	if _, err := session.Append(runID, core.EvRunStart, core.RunStartData{}); err != nil {
		return nil, err
	}
	turns := scenario.Messages / 4
	if turns < 1 {
		turns = 1
	}
	for turn := 0; turn < turns; turn++ {
		if _, err := session.Append(runID, core.EvUserMessage, core.UserMessageData{Text: payload}); err != nil {
			return nil, err
		}
		callID := fmt.Sprintf("call-%d", turn)
		call := core.ToolCall{ID: callID, Name: "perf.lookup", Args: map[string]any{"payload": payload}}
		if _, err := session.Append(runID, core.EvAssistantMessage, core.AssistantMessageData{Text: "lookup", ToolCall: &call}); err != nil {
			return nil, err
		}
		if _, err := session.Append(runID, core.EvToolResult, core.ToolResultData{CallID: callID, Content: payload, OK: true}); err != nil {
			return nil, err
		}
	}
	if _, err := session.Append(runID, core.EvUserMessage, core.UserMessageData{Text: "latest request"}); err != nil {
		return nil, err
	}
	return session, nil
}

// deterministicToolRuntime is deliberately tiny: it gives the deterministic
// local model one valid tool to call, so the benchmark exercises Agent's full
// model/tool/event loop without network access or provider state.
type deterministicToolRuntime struct{}

func (deterministicToolRuntime) Schemas() []core.ToolSchema {
	return []core.ToolSchema{{Name: "perf.lookup", Description: "deterministic local lookup", Parameters: map[string]any{"type": "object"}}}
}

func (deterministicToolRuntime) Execute(context.Context, core.ToolCall) (core.CapabilityResult, error) {
	return core.CapabilityResult{Content: "deterministic result", OK: true}, nil
}

func (deterministicToolRuntime) Authorized(name string) bool { return name == "perf.lookup" }

func (deterministicToolRuntime) MaxCallBudget() int { return 2 }

type preparedProductionAgent struct {
	agent *core.Agent
	input core.TurnInput
}

func (p *preparedProductionAgent) run(ctx context.Context) error {
	result, err := p.agent.RunTurn(ctx, p.input)
	if err != nil {
		return err
	}
	if result.Status != core.RunCompleted {
		return fmt.Errorf("production agent returned status %s", result.Status)
	}
	return nil
}

func prepareProductionAgent(scenario Scenario, worker int) (*preparedProductionAgent, error) {
	session, err := buildSession(scenario, worker)
	if err != nil {
		return nil, err
	}
	agent, err := core.NewAgent(core.AgentOptions{
		LLM: core.MockLlmAdapter{}, Tools: deterministicToolRuntime{}, Session: session,
		MaxSteps: 2, MaxToolCalls: 1,
		Compactor: core.RecentTurnsCompactor{MaxMessages: 8, MaxToolResultChars: 2048},
	})
	if err != nil {
		return nil, err
	}
	return &preparedProductionAgent{agent: agent, input: core.TurnInput{
		RunID: fmt.Sprintf("agent-run-%s-%d", strings.ReplaceAll(scenario.Name, "_", "-"), worker),
		Text:  strings.Repeat("q", scenario.PayloadBytes),
	}}, nil
}

// runProductionAgent retains the setup-inclusive path for attribution and
// compatibility with the focused benchmark. Production matrix cases use the
// prepared path above.
func runProductionAgent(ctx context.Context, scenario Scenario, worker int) error {
	prepared, err := prepareProductionAgent(scenario, worker)
	if err != nil {
		return err
	}
	return prepared.run(ctx)
}

func buildCatalog(count, payloadBytes int) *toollib.Catalog {
	if count < 1 {
		count = 1
	}
	listing := make([]toollib.Record, 0, count)
	for i := 0; i < count; i++ {
		name := fmt.Sprintf("tool-%04d", i)
		description := "General automation capability"
		if i == 0 {
			name, description = "send-email", "Send an email to an address"
			if payloadBytes > 0 {
				description += " " + strings.Repeat("x", payloadBytes)
			}
		}
		listing = append(listing, toollib.Record{Library: "perf", Name: name, Description: description, Triggers: []string{"email", "address"}})
	}
	catalog := toollib.NewCatalog()
	_, _ = catalog.Apply(listing)
	return catalog
}

func measureCatalog(scenario Scenario) (*toollib.Catalog, CatalogFootprint) {
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	catalog := buildCatalog(scenario.Tools, scenario.PayloadBytes)
	var afterBuild runtime.MemStats
	runtime.ReadMemStats(&afterBuild)
	runtime.GC()
	var afterGC runtime.MemStats
	runtime.ReadMemStats(&afterGC)
	runtime.KeepAlive(catalog)
	retained := uint64(0)
	if afterGC.HeapAlloc >= before.HeapAlloc {
		retained = afterGC.HeapAlloc - before.HeapAlloc
	}
	return catalog, CatalogFootprint{
		Scenario: scenario.Name, ToolCount: scenario.Tools,
		HeapBeforeGC: before.HeapAlloc, HeapAfterBuild: afterBuild.HeapAlloc,
		HeapAfterGC: afterGC.HeapAlloc, RetainedApprox: retained, Approximate: true,
	}
}

func percentileStats(samples []time.Duration) LatencyStats {
	if len(samples) == 0 {
		return LatencyStats{}
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	return LatencyStats{
		Samples: len(samples),
		P50:     samples[nearestRank(len(samples), 0.50)],
		P95:     samples[nearestRank(len(samples), 0.95)],
		P99:     samples[nearestRank(len(samples), 0.99)],
	}
}

func nearestRank(length int, percentile float64) int {
	index := int(float64(length)*percentile) - 1
	if index < 0 {
		index = 0
	}
	if index >= length {
		index = length - 1
	}
	return index
}

// JSON returns stable indented machine-readable output.
func (r Report) JSON() ([]byte, error) { return json.MarshalIndent(r, "", "  ") }

// Markdown returns a concise human-readable report without raw latency
// samples or payload content.
func (r Report) Markdown() string {
	var out strings.Builder
	fmt.Fprintf(&out, "# Perf-P0 baseline\n\nVersion: %d  \nStarted: %s  \nDuration: %s\n\n", r.Version, r.StartedAt.Format(time.RFC3339), r.Duration)
	if r.ProcessIsolated {
		out.WriteString("Case process isolation: enabled; RSS/heap process values are per-case child processes.\n\n")
	} else {
		out.WriteString("Case process isolation: disabled; RSS/heap process values are process-wide and may be contaminated by earlier cases.\n\n")
	}
	if r.Process.Supported {
		fmt.Fprint(&out, "Process RSS/CPU: supported; report before/after/peak working set and CPU time.\n\n")
	} else {
		fmt.Fprintf(&out, "Process RSS/CPU: unsupported (%s).\n\n", r.Process.Unsupported)
	}
	out.WriteString("| Workload | Scenario | Concurrency | Payload | Ops | Errors | Warmup ops/errors | Error kinds | Error samples | Attempt ops/s | p50 | p95 | p99 | Fixture heap | Fixture RSS delta | Heap after | Heap max seen | Alloc delta | GC pause ns | GCs | Goroutines peak | RSS peak | CPU % |\n")
	out.WriteString("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n")
	for _, c := range r.Cases {
		fmt.Fprintf(&out, "| %s | %s | %d | %d | %d | %d | %d/%d | %v | %v | %.2f | %s | %s | %s | %d | %s | %d | %d | %d | %d | %d | %d | %s | %s |\n",
			c.Workload, c.Scenario, c.Concurrency, c.PayloadBytes, c.Operations, c.Errors, c.Warmup.Operations, c.Warmup.Errors, c.ErrorKinds, c.ErrorSamples, c.Throughput,
			c.Latency.P50, c.Latency.P95, c.Latency.P99, c.Fixture.HeapAfterSetup, formatInt64(c.Fixture.RSSDeltaBytes), c.Runtime.HeapAllocAfter, c.Runtime.HeapAllocMaxSeen,
			c.Runtime.TotalAllocDelta, c.Runtime.PauseTotalNsDelta, c.Runtime.NumGCDelta, c.Runtime.NumGoroutinePeak,
			formatUint(c.Process.RSSPeakBytes), formatFloat(c.Process.CPUPercent))
	}
	out.WriteString("\n## Tool catalog retained footprint (approximate)\n\n| Scenario | Tools | Heap before GC | Heap after build | Heap after GC | Retained approx |\n|---|---:|---:|---:|---:|---:|\n")
	for _, catalog := range r.Catalogs {
		fmt.Fprintf(&out, "| %s | %d | %d | %d | %d | %d |\n", catalog.Scenario, catalog.ToolCount, catalog.HeapBeforeGC, catalog.HeapAfterBuild, catalog.HeapAfterGC, catalog.RetainedApprox)
	}
	out.WriteString("\nNotes:\n\n- Concurrency is genuine simultaneous goroutine execution; this harness adds no admission, queue, backpressure, rate-limit, or 429 behavior.\n- Ops and Attempt ops/s count completed operation attempts, including attempts that return an error. Errors is reported separately; compare throughput only when Errors == 0.\n- `legacy_session_projection_compaction` intentionally measures the older `DeriveMessages` then `Compact` path for diagnostic attribution; it is not the current default Agent path.\n- `agent_production_recent_compaction_setup_inclusive` retains the previous setup-inclusive Agent path for attribution only.\n- `agent_production_recent_compaction` prepares every Session, Agent, and TurnInput before the measured barrier; Runtime/Process values cover RunTurn only, while Fixture values expose the retained setup baseline.\n")
	if r.ProcessIsolated {
		out.WriteString("- Case RSS is sampled in a fresh child process for each CLI case; within that case, allocator/arena retention still means max_seen is an observation, not an absolute peak.\n")
	} else {
		out.WriteString("- Case RSS is sampled in one library process; preceding cases and Go arena retention can contaminate it, so it is not an isolated per-case resident-size measurement.\n")
	}
	out.WriteString("- CPU% is cumulative across logical processors and may exceed 100%; very short cases are affected by Windows timer granularity.\n- Samples below the portable 1µs reporting floor are shown at that floor; they are not nanosecond-precision claims.\n- The case sampler defaults to 1ms and adds one goroutine plus runtime/process sampling work while a case runs; max_seen values can still miss a transient between ticks.\n- Tool catalog footprint is measured separately with an explicit GC before/after build; it is approximate and affected by allocator/runtime noise, so search throughput cases exclude catalog construction.\n- The max payload is the 1 MiB tool-argument limit. The separate 16 MiB session-event cap is intentionally not allocated 500 times by the default run.\n- Provisional memory targets remain unverified until compared with these measurements; misses must be reported by scenario.\n")
	return out.String()
}

func formatUint(value *uint64) string {
	if value == nil {
		return "unsupported"
	}
	return fmt.Sprintf("%d", *value)
}

func formatInt64(value *int64) string {
	if value == nil {
		return "unsupported"
	}
	return fmt.Sprintf("%d", *value)
}

func formatFloat(value *float64) string {
	if value == nil {
		return "unsupported"
	}
	return fmt.Sprintf("%.2f", *value)
}
