package programmatic

import (
	"context"

	"github.com/whhhh1500/auto-agent/pkg/core"
)

// CodePTCToolAccess reserves the future trusted host's run-bound tool surface.
// It uses the shared ExposureKey, ExposureVersion, and Descriptor contract.
// ListTools returns defensive copies of the currently approved descriptors.
// CallTool must use the active run's protected tool invocation path.
//
// The host owns principal, scope, session, run, binding and stable call identity;
// untrusted code must use a restricted protocol and never receive this Go
// interface directly. This reservation supplies no CodePTC runner, isolation,
// process transport, or replay implementation and grants no execution access.
type CodePTCToolAccess interface {
	ListTools(context.Context) ([]Descriptor, error)
	CallTool(context.Context, string, map[string]any) (core.CapabilityResult, error)
}
