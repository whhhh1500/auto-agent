package core

import (
	"errors"
	"testing"
)

func TestSafeCallContainment(t *testing.T) {
	want := errors.New("original")
	if err := safeCallError("ignored", func() error { return want }); !errors.Is(err, want) {
		t.Fatalf("safeCallError returned %v, want %v", err, want)
	}
	if err := safeCallError("call panicked", func() error { panic("boom") }); err == nil || err.Error() != "call panicked" {
		t.Fatalf("safeCallError panic result = %v", err)
	}

	if got := safeCallValue("fallback", func() string { panic("boom") }); got != "fallback" {
		t.Fatalf("safeCallValue panic result = %q", got)
	}

	value, err := safeCallValueError("ignored", func() (string, error) { return "value", want })
	if value != "value" || !errors.Is(err, want) {
		t.Fatalf("safeCallValueError result = %q, %v", value, err)
	}
	value, err = safeCallValueError("call panicked", func() (string, error) { panic("boom") })
	if value != "" || err == nil || err.Error() != "call panicked" {
		t.Fatalf("safeCallValueError panic result = %q, %v", value, err)
	}

	called := false
	safeCallNotify(func() { called = true })
	if !called {
		t.Fatal("safeCallNotify did not call the function")
	}
	safeCallNotify(func() { panic("boom") })
}
