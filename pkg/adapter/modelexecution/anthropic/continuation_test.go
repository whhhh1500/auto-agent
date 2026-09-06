package anthropic

import (
	"testing"

	"github.com/cc-auto-agent/harness-core/pkg/app/modelexecution"
)

func TestForeignToolContinuationIsRejected(t *testing.T) {
	request := modelexecution.Request{Messages: []modelexecution.Message{{Role: "assistant", ToolCalls: []modelexecution.ToolCall{{ID: "call-a", Name: "lookup", Arguments: []byte(`{}`), Continuation: "opaque-foreign-state"}}}}}
	if _, err := marshalMessagesRequest(request, 100); err == nil {
		t.Fatal("foreign protocol state silently discarded")
	}
}
