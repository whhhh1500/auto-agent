package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/whhhh1500/auto-agent/internal/perfp0"
)

func main() {
	defaultConfig := perfp0.DefaultConfig()
	concurrencyFlag := flag.String("concurrency", "10,100,500", "comma-separated genuine concurrency levels")
	scenarioFlag := flag.String("scenarios", "small,typical,max", "comma-separated scenarios")
	soakFlag := flag.Duration("soak", 0, "optional control-plane soak duration, for example 30s")
	soakWorkersFlag := flag.Int("soak-workers", 100, "genuine concurrent workers used by soak")
	sampleIntervalFlag := flag.Duration("sample-interval", time.Millisecond, "case sampler interval, for example 1ms")
	outFlag := flag.String("out", "data/perf-p0-baseline", "output path prefix; .json and .md are written")
	childFlag := flag.Bool("perf-child", false, "internal one-case process protocol")
	flag.Parse()
	if *childFlag {
		runChild()
		return
	}

	concurrency, err := parseConcurrency(*concurrencyFlag)
	if err != nil {
		fatal(err)
	}
	scenarios, err := selectScenarios(*scenarioFlag, defaultConfig.Scenarios)
	if err != nil {
		fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		fatal(err)
	}
	report, err := perfp0.RunIsolated(context.Background(), perfp0.Config{
		Concurrency: concurrency, Scenarios: scenarios,
		Soak: *soakFlag, SoakWorkers: *soakWorkersFlag, SampleInterval: *sampleIntervalFlag,
	}, executable)
	if err != nil {
		fatal(err)
	}
	jsonBytes, err := report.JSON()
	if err != nil {
		fatal(err)
	}
	markdown := report.Markdown()
	if *outFlag == "-" {
		fmt.Println(string(jsonBytes))
		fmt.Println(markdown)
		return
	}
	if err := os.MkdirAll(filepath.Dir(*outFlag), 0o755); err != nil {
		fatal(err)
	}
	if err := os.WriteFile(*outFlag+".json", jsonBytes, 0o644); err != nil {
		fatal(err)
	}
	if err := os.WriteFile(*outFlag+".md", []byte(markdown), 0o644); err != nil {
		fatal(err)
	}
	fmt.Printf("wrote %s.json and %s.md\n", *outFlag, *outFlag)
}

func runChild() {
	var request perfp0.CaseRequest
	decoder := json.NewDecoder(io.LimitReader(os.Stdin, 1<<20))
	if err := decoder.Decode(&request); err != nil {
		_ = json.NewEncoder(os.Stdout).Encode(perfp0.ChildResponse{Error: "invalid child request"})
		return
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		_ = json.NewEncoder(os.Stdout).Encode(perfp0.ChildResponse{Error: "invalid child request"})
		return
	}
	result, err := perfp0.RunChild(request)
	if err != nil {
		// Do not put provider or transport diagnostics on the machine-readable
		// stdout channel; the parent deliberately receives only a fixed class.
		_ = json.NewEncoder(os.Stdout).Encode(perfp0.ChildResponse{Error: "child case failed"})
		return
	}
	_ = json.NewEncoder(os.Stdout).Encode(perfp0.ChildResponse{Result: &result})
}

func parseConcurrency(raw string) ([]int, error) {
	parts := strings.Split(raw, ",")
	values := make([]int, 0, len(parts))
	for _, part := range parts {
		value, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil || value < 1 {
			return nil, fmt.Errorf("invalid concurrency %q", part)
		}
		values = append(values, value)
	}
	if len(values) == 0 {
		return nil, fmt.Errorf("concurrency list is empty")
	}
	return values, nil
}

func selectScenarios(raw string, available []perfp0.Scenario) ([]perfp0.Scenario, error) {
	byName := make(map[string]perfp0.Scenario, len(available))
	for _, scenario := range available {
		byName[scenario.Name] = scenario
	}
	var selected []perfp0.Scenario
	for _, name := range strings.Split(raw, ",") {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		scenario, ok := byName[name]
		if !ok {
			return nil, fmt.Errorf("unknown scenario %q", name)
		}
		selected = append(selected, scenario)
	}
	if len(selected) == 0 {
		return nil, fmt.Errorf("scenario list is empty")
	}
	return selected, nil
}

func fatal(err error) {
	_, _ = fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
