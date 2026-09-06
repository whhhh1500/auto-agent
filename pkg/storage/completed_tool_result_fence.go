package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
)

// ErrCompletedToolResultProofInvalid reports that an exact completed journal
// result cannot safely be delivered into the fenced Session. Missing and
// non-completed rows deliberately share this result so callers cannot infer
// durable journal state from recovery probing.
var ErrCompletedToolResultProofInvalid = errors.New("completed tool result proof invalid")

// FencedCompletedToolResultAppender is an optional SQL-only extension for a
// queued recovery coordinator. It atomically rechecks a live SessionWriteFence,
// an exact completed journal identity, and the canonical result before adding
// exactly one tool/result event. Stores backed by separate queue, Session, or
// journal databases must not emulate this contract with best-effort checks.
//
// expectedResultDigest is CanonicalCapabilityResultDigest of the canonical
// result observed during caller preflight. event is the durable event written
// by this call or, when appended is false, the one exact result already present
// at expectedVersion after a response-lost retry.
type FencedCompletedToolResultAppender interface {
	AppendCompletedToolResultFenced(ctx context.Context, fence SessionWriteFence, expectedVersion int64, invocation core.ToolInvocation, expectedResultDigest string) (event core.SessionEvent, appended bool, err error)
}

// CanonicalCapabilityResultDigest returns the lowercase SHA-256 digest of the
// standard-library JSON encoding of one validated capability result. The
// encoding is deterministic for this wire value: struct fields are fixed and
// encoding/json sorts string map keys.
func CanonicalCapabilityResultDigest(result core.CapabilityResult) (string, error) {
	if err := core.ValidateCapabilityResult(result); err != nil {
		return "", err
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		return "", fmt.Errorf("encode capability result: %w", err)
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

func validCapabilityResultDigest(value string) bool {
	if len(value) != sha256.Size*2 || strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func completedToolResultProofInvalid() error { return ErrCompletedToolResultProofInvalid }
