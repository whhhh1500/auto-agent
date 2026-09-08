package perfp0

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"runtime"
	"time"

	core "github.com/whhhh1500/auto-agent/pkg/core"
)

// CaseRequest is the small stdin protocol used by the process-isolated
// command. The child receives one case only, so its RSS and heap samples are
// not contaminated by preceding matrix cases.
type CaseRequest struct {
	Workload       string        `json:"workload"`
	Scenario       Scenario      `json:"scenario"`
	Concurrency    int           `json:"concurrency"`
	SampleInterval time.Duration `json:"sample_interval_ns"`
	Soak           time.Duration `json:"soak_ns,omitempty"`
	SoakWorkers    int           `json:"soak_workers,omitempty"`
}

// ChildResponse is the stdout protocol returned by one isolated case.
// Error is a bounded, non-secret diagnostic; normal reports use Result.
type ChildResponse struct {
	Result *CaseResult `json:"result,omitempty"`
	Error  string      `json:"error,omitempty"`
}

const maxChildJSONBytes = 4 << 20

func decodeChildResponse(output []byte) (CaseResult, error) {
	if len(output) > maxChildJSONBytes {
		return CaseResult{}, fmt.Errorf("isolated case protocol exceeded limit")
	}
	var response ChildResponse
	if err := json.Unmarshal(output, &response); err != nil {
		return CaseResult{}, fmt.Errorf("isolated case protocol failed")
	}
	if response.Error != "" {
		// Child diagnostics are not trusted provider output. Keep the parent
		// error fixed so a plugin cannot exfiltrate secrets through the protocol.
		return CaseResult{}, fmt.Errorf("isolated case child failed")
	}
	if response.Result == nil {
		return CaseResult{}, fmt.Errorf("isolated case returned no result")
	}
	return *response.Result, nil
}

// RunChild executes exactly one bounded case for the command child process.
// It intentionally does not run the full matrix or write files.
func RunChild(request CaseRequest) (CaseResult, error) {
	if request.SampleInterval <= 0 {
		request.SampleInterval = time.Millisecond
	}
	if request.Workload == "control_health_soak" {
		api, err := newControlPlane()
		if err != nil {
			return CaseResult{}, err
		}
		defer api.Close()
		return runSoak(context.Background(), request.SampleInterval, api.URL+"/healthz", request.SoakWorkers, request.Soak)
	}
	return runIsolatedCase(context.Background(), request)
}

func runIsolatedCase(ctx context.Context, request CaseRequest) (CaseResult, error) {
	scenario := request.Scenario
	if request.Concurrency < 1 {
		return CaseResult{}, fmt.Errorf("concurrency must be positive")
	}
	switch request.Workload {
	case "control_health":
		api, err := newControlPlane()
		if err != nil {
			return CaseResult{}, err
		}
		defer api.Close()
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.MaxIdleConns = request.Concurrency
		transport.MaxIdleConnsPerHost = request.Concurrency
		transport.MaxConnsPerHost = request.Concurrency
		client := &http.Client{Transport: transport, Timeout: 10 * time.Second}
		defer transport.CloseIdleConnections()
		return runConcurrent(ctx, request.SampleInterval, request.Workload, "", 0, request.Concurrency, func(ctx context.Context, _ int) error {
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, api.URL+"/healthz", nil)
			if err != nil {
				return err
			}
			response, err := client.Do(req)
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
	case "legacy_session_projection_compaction":
		return runConcurrent(ctx, request.SampleInterval, request.Workload, scenario.Name, scenario.PayloadBytes, request.Concurrency, func(_ context.Context, worker int) error {
			session, err := buildSession(scenario, worker)
			if err != nil {
				return err
			}
			messages, err := session.DeriveMessages()
			if err != nil {
				return err
			}
			compacted := coreRecentTurnsCompact(messages)
			if len(compacted) == 0 {
				return fmt.Errorf("empty compacted projection")
			}
			return nil
		})
	case "agent_production_recent_compaction_setup_inclusive":
		return runConcurrent(ctx, request.SampleInterval, request.Workload, scenario.Name, scenario.PayloadBytes, request.Concurrency, func(ctx context.Context, worker int) error {
			return runProductionAgent(ctx, scenario, worker)
		})
	case "agent_production_recent_compaction":
		return runPreparedConcurrent(ctx, request.SampleInterval, request.Workload, scenario.Name, scenario.PayloadBytes, request.Concurrency, func(worker int) (func(context.Context) error, error) {
			prepared, err := prepareProductionAgent(scenario, worker)
			if err != nil {
				return nil, err
			}
			return prepared.run, nil
		})
	case "tool_search":
		catalog := buildCatalog(scenario.Tools, scenario.PayloadBytes)
		return runConcurrent(ctx, request.SampleInterval, request.Workload, scenario.Name, scenario.PayloadBytes, request.Concurrency, func(_ context.Context, _ int) error {
			hits := catalog.Search("send email to an address", 8)
			if len(hits) == 0 {
				return fmt.Errorf("tool search returned no hits")
			}
			return nil
		})
	default:
		return CaseResult{}, fmt.Errorf("unknown workload %q", request.Workload)
	}
}

func coreRecentTurnsCompact(messages []core.ChatMessage) []core.ChatMessage {
	return core.RecentTurnsCompactor{MaxMessages: 8, MaxToolResultChars: 2048}.Compact(messages)
}

// RunIsolated composes the same matrix as Run, but starts a fresh child
// process for every case. executable must be the already-built command path;
// this avoids invoking a compiler for each case.
func RunIsolated(ctx context.Context, cfg Config, executable string) (Report, error) {
	if ctx == nil {
		return Report{}, fmt.Errorf("context is nil")
	}
	if executable == "" {
		return Report{}, fmt.Errorf("isolated executable is empty")
	}
	cfg = normalizeConfig(cfg)
	started := time.Now().UTC()
	report := Report{Version: reportVersion, StartedAt: started, ProcessIsolated: true, Process: ProcessStats{Unsupported: "case-isolated; inspect each case process"}}
	for _, concurrency := range cfg.Concurrency {
		result, err := runChildCase(ctx, executable, CaseRequest{Workload: "control_health", Concurrency: concurrency, SampleInterval: cfg.SampleInterval})
		if err != nil {
			return Report{}, err
		}
		report.Cases = append(report.Cases, result)
	}
	for _, scenario := range cfg.Scenarios {
		catalog, footprint := measureCatalog(scenario)
		report.Catalogs = append(report.Catalogs, footprint)
		runtime.KeepAlive(catalog)
		for _, concurrency := range cfg.Concurrency {
			for _, workload := range []string{"legacy_session_projection_compaction", "agent_production_recent_compaction_setup_inclusive", "agent_production_recent_compaction", "tool_search"} {
				result, err := runChildCase(ctx, executable, CaseRequest{Workload: workload, Scenario: scenario, Concurrency: concurrency, SampleInterval: cfg.SampleInterval})
				if err != nil {
					return Report{}, err
				}
				report.Cases = append(report.Cases, result)
			}
		}
	}
	if cfg.Soak > 0 {
		result, err := runChildCase(ctx, executable, CaseRequest{Workload: "control_health_soak", SampleInterval: cfg.SampleInterval, Soak: cfg.Soak, SoakWorkers: cfg.SoakWorkers})
		if err != nil {
			return Report{}, err
		}
		report.Cases = append(report.Cases, result)
	}
	report.Duration = time.Since(started)
	return report, nil
}

func runChildCase(ctx context.Context, executable string, request CaseRequest) (CaseResult, error) {
	encoded, err := json.Marshal(request)
	if err != nil {
		return CaseResult{}, err
	}
	if len(encoded) > maxChildJSONBytes {
		return CaseResult{}, fmt.Errorf("isolated case request exceeds protocol limit")
	}
	command := exec.CommandContext(ctx, executable, "-perf-child")
	command.Stdin = bytes.NewReader(encoded)
	stdout, err := command.StdoutPipe()
	if err != nil {
		return CaseResult{}, fmt.Errorf("isolated case failed")
	}
	if err := command.Start(); err != nil {
		return CaseResult{}, fmt.Errorf("isolated case failed")
	}
	output, readErr := io.ReadAll(io.LimitReader(stdout, maxChildJSONBytes+1))
	if len(output) > maxChildJSONBytes {
		if command.Process != nil {
			_ = command.Process.Kill()
		}
		_ = command.Wait()
		return CaseResult{}, fmt.Errorf("isolated case protocol exceeded limit")
	}
	waitErr := command.Wait()
	if readErr != nil || waitErr != nil {
		return CaseResult{}, fmt.Errorf("isolated case failed")
	}
	return decodeChildResponse(output)
}
