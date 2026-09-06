package execution

import (
	"context"
	"strings"
	"testing"
)

func TestValidateLocalExecPath(t *testing.T) {
	if err := validateLocalExecPath("modules/calc.wasm"); err != nil {
		t.Fatal(err)
	}
	if err := validateLocalExecPath("/bin/sh"); err != nil {
		t.Fatal(err)
	}
	if err := validateLocalExecPath("../etc/passwd"); err == nil {
		t.Fatal("parent path segment was accepted")
	}
	if err := validateLocalExecPath(""); err == nil {
		t.Fatal("empty path was accepted")
	}
	if err := validateLocalExecPath("mod\x00ule.wasm"); err == nil {
		t.Fatal("NUL path was accepted")
	}
}

func TestArgsToStrsRejectsNUL(t *testing.T) {
	if _, err := argsToStrs(map[string]any{"input": "ok"}); err != nil {
		t.Fatal(err)
	}
	if _, err := argsToStrs(map[string]any{"input": "bad\x00arg"}); err == nil {
		t.Fatal("NUL argument was accepted")
	}
	if _, err := argsToStrs(map[string]any{"inputs": []any{"ok", "bad\x00arg"}}); err == nil {
		t.Fatal("NUL list argument was accepted")
	}
}

func TestWazeroRunRejectsParentModulePath(t *testing.T) {
	_, err := (WazeroExecutor{}).Run(context.Background(), "../calc.wasm", nil)
	if err == nil || !strings.Contains(err.Error(), "parent") {
		t.Fatalf("parent wasm path was read: %v", err)
	}
}

func TestArgsToStrsRejectsNestedValues(t *testing.T) {
	if _, err := argsToStrs(map[string]any{"inputs": []any{map[string]any{"nested": true}}}); err == nil {
		t.Fatal("nested execution argument was accepted")
	}
	if _, err := argsToStrs(map[string]any{"input": []any{"x"}}); err == nil {
		t.Fatal("non-scalar input was accepted")
	}
	got, err := argsToStrs(map[string]any{"inputs": []any{"ok", 2, true}})
	if err != nil || len(got) != 3 || got[0] != "ok" || got[1] != "2" || got[2] != "true" {
		t.Fatalf("scalar list encoding failed: %#v err=%v", got, err)
	}
}

func TestValidateExecArgsRejectsOverflow(t *testing.T) {
	tooMany := make([]string, maxExecArgs+1)
	for i := range tooMany {
		tooMany[i] = "a"
	}
	if err := validateExecArgs(tooMany); err == nil {
		t.Fatal("oversized argv was accepted")
	}
	if err := validateExecArgs([]string{strings.Repeat("x", maxExecArgBytes+1)}); err == nil {
		t.Fatal("oversized argument was accepted")
	}
}
