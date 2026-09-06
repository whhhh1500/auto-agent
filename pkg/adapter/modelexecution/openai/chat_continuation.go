package openai

import (
	"encoding/json"
	"fmt"

	"github.com/cc-auto-agent/harness-core/pkg/app/modelexecution"
)

// The protocol envelope prevents opaque continuation data from becoming tool
// arguments or being interpreted as arbitrary top-level request parameters.
type chatContinuation struct {
	Protocol     string          `json:"protocol"`
	ExtraContent json.RawMessage `json:"extra_content"`
}

func encodeChatContinuation(extra json.RawMessage) (string, error) {
	if len(extra) == 0 || string(extra) == "null" {
		return "", nil
	}
	if !continuationObject(extra) {
		return "", fmt.Errorf("invalid chat tool continuation")
	}
	encoded, err := json.Marshal(chatContinuation{Protocol: "openai.chat/v1", ExtraContent: extra})
	if err != nil || len(encoded) > modelexecution.DefaultMaxContinuationBytes {
		return "", fmt.Errorf("chat tool continuation exceeds limit")
	}
	return string(encoded), nil
}

func decodeChatContinuation(value string) (json.RawMessage, error) {
	if len(value) > modelexecution.DefaultMaxContinuationBytes {
		return nil, fmt.Errorf("chat tool continuation exceeds limit")
	}
	var continuation chatContinuation
	if json.Unmarshal([]byte(value), &continuation) != nil || continuation.Protocol != "openai.chat/v1" || !continuationObject(continuation.ExtraContent) {
		return nil, fmt.Errorf("incompatible chat tool continuation")
	}
	return continuation.ExtraContent, nil
}

func continuationObject(value []byte) bool {
	var object map[string]json.RawMessage
	return json.Unmarshal(value, &object) == nil && object != nil
}
