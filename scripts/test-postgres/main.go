// Command test-postgres executes the complete PostgreSQL integration gate.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"
)

type testEvent struct {
	Time    time.Time `json:",omitempty"`
	Elapsed float64   `json:",omitempty"`
	Action  string
	Package string
	Test    string
	Output  string `json:",omitempty"`
}

type summary struct {
	Tests, Subtests int
	Packages        map[string]bool
}

type testKey struct {
	packageName string
	testName    string
}

type testLifecycle struct {
	started  bool
	terminal bool
}

func verifyEvents(reader io.Reader, log io.Writer, secret string) (summary, error) {
	result := summary{Packages: make(map[string]bool)}
	lifecycles := make(map[testKey]testLifecycle)
	decoder := json.NewDecoder(reader)
	encoder := json.NewEncoder(log)
	var failures []error
	for {
		var event testEvent
		if err := decoder.Decode(&event); err != nil {
			if err == io.EOF {
				break
			}
			return result, errors.New("invalid or truncated go test JSON output")
		}
		if secret != "" {
			event.Output = strings.ReplaceAll(event.Output, secret, "[REDACTED_DSN]")
		}
		if err := encoder.Encode(event); err != nil {
			return result, fmt.Errorf("write test evidence: %w", err)
		}
		if event.Action == "fail" {
			failures = append(failures, fmt.Errorf("failed: %s %s", event.Package, event.Test))
		}
		if event.Test == "" {
			continue
		}
		key := testKey{packageName: event.Package, testName: event.Test}
		lifecycle := lifecycles[key]
		switch event.Action {
		case "run":
			if lifecycle.started {
				failures = append(failures, fmt.Errorf("selected test ran more than once: %s %s", event.Package, event.Test))
				continue
			}
			lifecycle.started = true
			lifecycles[key] = lifecycle
		case "pass", "skip", "fail":
			if !lifecycle.started {
				failures = append(failures, fmt.Errorf("selected test ended without running: %s %s", event.Package, event.Test))
				continue
			}
			if lifecycle.terminal {
				failures = append(failures, fmt.Errorf("selected test ended more than once: %s %s", event.Package, event.Test))
				continue
			}
			lifecycle.terminal = true
			lifecycles[key] = lifecycle
			if event.Action == "skip" {
				failures = append(failures, fmt.Errorf("selected test skipped: %s %s", event.Package, event.Test))
			}
			if event.Action == "pass" {
				if strings.Contains(event.Test, "/") {
					result.Subtests++
				} else {
					result.Tests++
					result.Packages[event.Package] = true
				}
			}
		}
	}
	if result.Tests == 0 {
		failures = append(failures, errors.New("no PostgreSQL tests executed"))
	}
	for _, lifecycle := range lifecycles {
		if lifecycle.started && !lifecycle.terminal {
			failures = append(failures, errors.New("selected tests did not finish"))
			break
		}
	}
	return result, errors.Join(failures...)
}

func run() error {
	logPath := flag.String("log", "postgres-test.jsonl", "new JSON test evidence path (must not exist)")
	flag.Parse()
	dsn := os.Getenv("HARNESS_TEST_PG_DSN")
	if strings.TrimSpace(dsn) == "" {
		return errors.New("HARNESS_TEST_PG_DSN is required; skipped database tests cannot pass this gate")
	}
	file, err := openEvidenceLog(*logPath)
	if err != nil {
		return err
	}
	defer file.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	command := exec.CommandContext(ctx, "go", "test", "-p", "4", "-json", "-count=1", "-timeout", "600s", "./...", "-run", "^TestPostgres")
	command.Stderr = os.Stderr
	stdout, err := command.StdoutPipe()
	if err != nil {
		return err
	}
	if err := command.Start(); err != nil {
		return err
	}
	result, verifyErr := verifyEvents(stdout, file, dsn)
	if verifyErr != nil {
		// A decoder/log error may stop reading before the child finishes.
		_, _ = io.Copy(io.Discard, stdout)
	}
	waitErr := command.Wait()
	if err := errors.Join(verifyErr, waitErr, file.Sync()); err != nil {
		return fmt.Errorf("PostgreSQL gate failed (see %s): %w", *logPath, err)
	}
	fmt.Printf("PostgreSQL gate passed: %d tests, %d subtests, %d packages; zero skips\n", result.Tests, result.Subtests, len(result.Packages))
	return nil
}

// openEvidenceLog keeps evidence-file creation in one place so the command
// and its tests apply the same writer ownership rule.
func openEvidenceLog(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
