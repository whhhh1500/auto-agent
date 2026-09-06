package main

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

func TestEvidenceRejectsFalseGreenResults(t *testing.T) {
	passed := `{"Action":"pass","Package":"adapter","Test":"TestPostgresCAS"}`
	for name, input := range map[string]string{
		"no tests":     `{"Action":"pass","Package":"adapter"}`,
		"skip":         passed + `{"Action":"skip","Package":"storage","Test":"TestPostgresRows"}`,
		"package fail": passed + `{"Action":"fail","Package":"storage"}`,
		"unfinished":   passed + `{"Action":"run","Package":"storage","Test":"TestPostgresRows"}`,
		"truncated":    passed + `{"Action":`,
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
{"Action":"pass","Package":"adapter","Test":"TestPostgresCAS/atomic"}
{"Action":"pass","Package":"adapter","Test":"TestPostgresCAS"}
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
