package main

import (
	"bytes"
	"io"
	"os"
	"strings"
	"testing"
)

func TestEvidenceRejectsFalseGreenResults(t *testing.T) {
	passed := `{"Action":"pass","Package":"adapter","Test":"TestPostgresCAS"}`
	for name, input := range map[string]string{
		"no tests":        `{"Action":"pass","Package":"adapter"}`,
		"skip":            passed + `{"Action":"skip","Package":"storage","Test":"TestPostgresRows"}`,
		"package fail":    passed + `{"Action":"fail","Package":"storage"}`,
		"unfinished":      passed + `{"Action":"run","Package":"storage","Test":"TestPostgresRows"}`,
		"truncated":       passed + `{"Action":`,
		"orphan terminal": `{"Action":"pass","Package":"storage","Test":"TestPostgresRows"}`,
		"orphan subtest terminal": `{"Action":"run","Package":"storage","Test":"TestPostgresRows"}
{"Action":"pass","Package":"storage","Test":"TestPostgresRows/atomic"}
{"Action":"pass","Package":"storage","Test":"TestPostgresRows"}`,
		"duplicate terminal": `{"Action":"run","Package":"storage","Test":"TestPostgresRows"}
{"Action":"pass","Package":"storage","Test":"TestPostgresRows"}
{"Action":"pass","Package":"storage","Test":"TestPostgresRows"}`,
		"duplicate run": `{"Action":"run","Package":"storage","Test":"TestPostgresRows"}
{"Action":"run","Package":"storage","Test":"TestPostgresRows"}
{"Action":"pass","Package":"storage","Test":"TestPostgresRows"}`,
		"run after terminal": `{"Action":"run","Package":"storage","Test":"TestPostgresRows"}
{"Action":"pass","Package":"storage","Test":"TestPostgresRows"}
{"Action":"run","Package":"storage","Test":"TestPostgresRows"}
{"Action":"pass","Package":"storage","Test":"TestPostgresRows"}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := verifyEvents(strings.NewReader(input), io.Discard, ""); err == nil {
				t.Fatal("false-green test evidence accepted")
			}
		})
	}
}

func TestEvidenceCountsPackagesAndRedactsDSN(t *testing.T) {
	input := `{"Action":"run","Package":"adapter","Test":"TestPostgresCAS"}
{"Action":"output","Package":"adapter","Output":"fixture-dsn"}
{"Action":"run","Package":"adapter","Test":"TestPostgresCAS/atomic"}
{"Action":"pass","Package":"adapter","Test":"TestPostgresCAS/atomic"}
{"Action":"pass","Package":"adapter","Test":"TestPostgresCAS"}
{"Action":"run","Package":"storage","Test":"TestPostgresRows"}
{"Action":"pass","Package":"storage","Test":"TestPostgresRows"}`
	var log bytes.Buffer
	result, err := verifyEvents(strings.NewReader(input), &log, "fixture-dsn")
	if err != nil || result.Tests != 2 || result.Subtests != 1 || len(result.Packages) != 2 {
		t.Fatalf("summary=%+v err=%v", result, err)
	}
	if strings.Contains(log.String(), "fixture-dsn") {
		t.Fatal("DSN escaped into evidence")
	}
}

func TestEvidenceLogRejectsSecondWriterWithoutTruncatingFirstEvidence(t *testing.T) {
	path := t.TempDir() + "/postgres-test.jsonl"
	first, err := openEvidenceLog(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.WriteString("first-evidence\n"); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second, err := openEvidenceLog(path)
	if err == nil {
		_ = second.Close()
		t.Fatal("second evidence writer opened an existing log")
	}
	if !os.IsExist(err) {
		t.Fatalf("second writer error = %v, want already exists", err)
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != "first-evidence\n" {
		t.Fatalf("first evidence changed: %q err=%v", data, err)
	}
}
