package modelexecution

import (
	"strings"
	"testing"
)

func TestContinuationOnlyDeltaIsBoundedAndImmutable(t *testing.T) {
	validator := NewStreamValidator()
	first := Event{Kind: EventToolCallDelta, ToolCall: &ToolCallDelta{Index: 0, ID: "call-a", Name: "lookup", ArgumentsFragment: []byte("{}")}}
	if _, err := validator.Accept(first); err != nil {
		t.Fatal(err)
	}
	delta := Event{Kind: EventToolCallDelta, ToolCall: &ToolCallDelta{Index: 0, Continuation: "opaque"}}
	if _, err := validator.Accept(delta); err != nil {
		t.Fatal(err)
	}
	if _, err := validator.Accept(delta); err != nil {
		t.Fatal("identical repeated continuation rejected")
	}
	delta.ToolCall.Continuation = "changed"
	if _, err := validator.Accept(delta); err == nil {
		t.Fatal("conflicting continuation accepted")
	}
	delta.ToolCall.Continuation = strings.Repeat("x", DefaultMaxContinuationBytes+1)
	if _, err := NewStreamValidator().Accept(delta); err == nil {
		t.Fatal("unbounded continuation accepted")
	}
}
