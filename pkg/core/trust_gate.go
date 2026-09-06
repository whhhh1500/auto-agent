package core

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
)

var ErrAcceptedInvocationRequired = errors.New("accepted tool invocation is required")

type acceptedInvocationSeal struct{}

// AcceptedInvocation is a kernel-minted proof that one canonical tool
// invocation passed the guard funnel. Its fields intentionally have no
// external constructor or exported accessors; providers can only validate it
// through RequireAcceptedInvocation.
type AcceptedInvocation struct {
	invocation ToolInvocation
	scope      ScopePath
	principal  Principal
	seal       *acceptedInvocationSeal
}

func mintAcceptedInvocation(invocation ToolInvocation, scope ScopePath, principal Principal) AcceptedInvocation {
	return AcceptedInvocation{invocation: invocation, scope: scope, principal: clonePrincipal(principal), seal: &acceptedInvocationSeal{}}
}

func acceptedInvocationFromContext(ctx context.Context) AcceptedInvocation {
	value, _ := ctx.Value(acceptedInvocationContextKey{}).(AcceptedInvocation)
	return value
}

type acceptedInvocationContextKey struct{}

func withAcceptedInvocation(ctx context.Context, accepted AcceptedInvocation) context.Context {
	return context.WithValue(ctx, acceptedInvocationContextKey{}, accepted)
}

// RequireAcceptedInvocation verifies the guard proof against the complete
// provider request, including principal, scope, call identity and canonical
// argument digest. It is safe for external providers to call directly.
func RequireAcceptedInvocation(request CapabilityRequest) error {
	accepted := request.Context.Accepted
	if accepted.seal == nil {
		return ErrAcceptedInvocationRequired
	}
	if err := ValidateToolInvocation(accepted.invocation); err != nil {
		return fmt.Errorf("%w: invalid proof", ErrAcceptedInvocationRequired)
	}
	if accepted.scope.Depth() == 0 || request.Context.Scope.Depth() == 0 {
		return fmt.Errorf("%w: scope is empty", ErrAcceptedInvocationRequired)
	}
	if !accepted.scope.Equal(request.Context.Scope) {
		return fmt.Errorf("%w: scope mismatch", ErrAcceptedInvocationRequired)
	}
	if !samePrincipal(accepted.principal, request.Context.Principal) {
		return fmt.Errorf("%w: principal mismatch", ErrAcceptedInvocationRequired)
	}
	if request.Context.CapabilityID == "" || request.CallID == "" {
		return fmt.Errorf("%w: request identity is incomplete", ErrAcceptedInvocationRequired)
	}
	if accepted.invocation.TenantID != request.Context.Principal.TenantID ||
		accepted.invocation.SubjectID != request.Context.Principal.SubjectID ||
		accepted.invocation.SessionID != request.Context.Invocation.SessionID ||
		accepted.invocation.RunID != request.Context.Invocation.RunID ||
		accepted.invocation.CallID != request.CallID ||
		accepted.invocation.CapabilityID != request.Context.CapabilityID {
		return fmt.Errorf("%w: invocation identity mismatch", ErrAcceptedInvocationRequired)
	}
	encoded, err := json.Marshal(normalizeToolArgs(request.Args))
	if err != nil {
		return fmt.Errorf("%w: arguments are not canonical", ErrAcceptedInvocationRequired)
	}
	if subtle.ConstantTimeCompare([]byte(accepted.invocation.ArgsDigest), digestArgs(encoded)) != 1 {
		return fmt.Errorf("%w: argument digest mismatch", ErrAcceptedInvocationRequired)
	}
	return nil
}

func digestArgs(encoded []byte) []byte {
	// NewToolInvocation uses SHA-256 followed by lowercase hex. Keeping the
	// conversion here identical makes the comparison constant-time while the
	// canonical JSON encoding remains the single digest input.
	sum := sha256.Sum256(encoded)
	result := make([]byte, hex.EncodedLen(len(sum)))
	hex.Encode(result, sum[:])
	return result
}

func normalizeToolArgs(args map[string]any) map[string]any {
	if args == nil {
		return map[string]any{}
	}
	return args
}
