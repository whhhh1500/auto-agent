package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadDotEnvMissingAndPrecedence(t *testing.T) {
	dir := t.TempDir()
	if err := loadDotEnv(filepath.Join(dir, "missing.env")); err != nil {
		t.Fatal(err)
	}
	const key = "HARNESS_DOTENV_PRECEDENCE_TEST"
	t.Setenv(key, "process")
	path := filepath.Join(dir, ".env")
	if err := os.WriteFile(path, []byte("\ufeff"+key+"=file\nHARNESS_DOTENV_NEW_TEST='from file'\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Unsetenv("HARNESS_DOTENV_NEW_TEST") })
	if err := loadDotEnv(path); err != nil {
		t.Fatal(err)
	}
	if got := os.Getenv(key); got != "process" {
		t.Fatalf("process environment replaced: %q", got)
	}
	if got := os.Getenv("HARNESS_DOTENV_NEW_TEST"); got != "from file" {
		t.Fatalf("dotenv value = %q", got)
	}
}

func TestParseDotEnvValues(t *testing.T) {
	for _, test := range []struct {
		line, key, value string
		present          bool
	}{
		{line: "", present: false},
		{line: " # comment", present: false},
		{line: "A=plain", key: "A", value: "plain", present: true},
		{line: "export A = value # comment", key: "A", value: "value", present: true},
		{line: `A="line\nvalue"`, key: "A", value: "line\nvalue", present: true},
		{line: "A='literal # value'", key: "A", value: "literal # value", present: true},
	} {
		key, value, present, err := parseDotEnvLine(test.line)
		if err != nil || key != test.key || value != test.value || present != test.present {
			t.Fatalf("parse %q = %q, %q, %t, %v", test.line, key, value, present, err)
		}
	}
}

func TestLoadDotEnvErrorsDoNotEchoSecrets(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".env")
	const secret = "do-not-echo-this-secret"
	if err := os.WriteFile(path, []byte("HARNESS_LLM_API_KEY='"+secret+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := loadDotEnv(path)
	if err == nil || !strings.Contains(err.Error(), "line 1") {
		t.Fatalf("dotenv error = %v", err)
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("dotenv error leaked secret: %v", err)
	}
}
