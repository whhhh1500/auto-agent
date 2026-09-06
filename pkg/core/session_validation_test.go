package core

import (
	"encoding/json"
	"testing"
)

func TestTypedAndRawSessionValidationErrorsMatch(t *testing.T) {
	cases := []struct {
		name string
		typ  SessionEventType
		data any
	}{
		{"run error", EvRunError, RuntimeErrorData{}},
		{"step error", EvStepError, RuntimeErrorData{}},
		{"step start", EvStepStart, StepData{Index: -2}},
		{"step end", EvStepEnd, StepData{Index: -2}},
		{"nil pointer", EvUserMessage, (*UserMessageData)(nil)},
		{"unknown", SessionEventType("future/unknown"), map[string]any{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			encoded, err := json.Marshal(tc.data)
			if err != nil {
				t.Fatal(err)
			}
			typed, typedErr := validateSessionEventValue(tc.typ, tc.data)
			rawErr := validateSessionEvent(SessionEvent{RunID: "run-validation", Type: tc.typ, Data: encoded})
			if typed && tc.name == "unknown" {
				t.Fatal("unknown event unexpectedly recognized as canonical")
			}
			if !typed && tc.name != "unknown" && tc.name != "nil pointer" {
				t.Fatal("canonical event was not recognized")
			}
			if tc.name == "unknown" {
				if rawErr == nil {
					t.Fatal("unknown event unexpectedly validated")
				}
				return
			}
			if (typedErr == nil) != (rawErr == nil) || (typedErr != nil && typedErr.Error() != rawErr.Error()) {
				t.Fatalf("typed=%v typedErr=%v rawErr=%v", typed, typedErr, rawErr)
			}
		})
	}
}

func TestAppendCanonicalEventWireBytesRemainMarshalOutput(t *testing.T) {
	_, _, _, user := testScopes()
	scope, _ := user.Child(ScopeRef{Kind: ScopeSession, ID: "session-validation"})
	session, err := NewSession(SessionOptions{ID: "session-validation", ProfileID: "profile-validation", Scope: scope, Principal: testPrincipal(user)})
	if err != nil {
		t.Fatal(err)
	}
	data := RunStartData{}
	event, err := session.Append("run-validation", EvRunStart, data)
	if err != nil {
		t.Fatal(err)
	}
	want, _ := json.Marshal(data)
	if string(event.Data) != string(want) {
		t.Fatalf("wire bytes changed: got %s want %s", event.Data, want)
	}
	if event.Seq != 0 {
		t.Fatalf("unexpected event sequence %d", event.Seq)
	}
}
