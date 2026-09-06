// Package runner owns the private runner HTTP request models.
//
// These DTOs deliberately stay at the transport boundary. They translate
// JSON requests into the provider-neutral runner commands used by the
// runtime, while keeping HTTP compatibility details out of the queue state
// machine and durable storage adapters.
package runner

import (
	"fmt"
	"sort"
	"strings"
	"time"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
	runtime "github.com/cc-auto-agent/harness-core/pkg/extensions/runner"
)

// ClaimRequest selects the capabilities a private runner is willing to
// execute. An empty body or an empty selector claims from all capabilities,
// subject to the authenticated runner grant enforced by the server.
type ClaimRequest struct {
	Capabilities []string `json:"capabilities"`
}

// Validate checks and canonicalizes the request's capability selector without
// applying the authenticated principal's capability grant.
func (request ClaimRequest) Validate() error {
	_, err := request.CanonicalCapabilities()
	return err
}

// CanonicalCapabilities returns the selector in the same form expected by
// the runner queue: trimmed, de-duplicated and sorted. It intentionally does
// not authorize the selector; authorization belongs to the HTTP server after
// it authenticates the runner principal.
func (request ClaimRequest) CanonicalCapabilities() ([]string, error) {
	if len(request.Capabilities) == 0 {
		return nil, nil
	}
	if len(request.Capabilities) > runtime.MaxClaimCapabilities {
		return nil, fmt.Errorf("runner claim supports at most %d capabilities", runtime.MaxClaimCapabilities)
	}
	unique := make(map[string]struct{}, len(request.Capabilities))
	for _, value := range request.Capabilities {
		capability := strings.TrimSpace(value)
		if capability == "*" {
			return nil, fmt.Errorf("runner claim capability selector cannot include %q", capability)
		}
		if err := core.ValidateNamespacedID(capability); err != nil {
			return nil, fmt.Errorf("invalid runner claim capability %q: %w", capability, err)
		}
		unique[capability] = struct{}{}
	}
	capabilities := make([]string, 0, len(unique))
	for capability := range unique {
		capabilities = append(capabilities, capability)
	}
	sort.Strings(capabilities)
	return capabilities, nil
}

// ToCommand maps the transport request to the existing runner queue command.
// The authenticated principal's grant is intentionally checked by the
// server before the returned command is submitted.
func (request ClaimRequest) ToCommand(workerID string, leaseTTL time.Duration) (runtime.ClaimOptions, error) {
	capabilities, err := request.CanonicalCapabilities()
	if err != nil {
		return runtime.ClaimOptions{}, err
	}
	return runtime.ClaimOptions{WorkerID: workerID, Capabilities: capabilities, LeaseTTL: leaseTTL}, nil
}

// GenerationRequest is shared by renew and complete requests.
type GenerationRequest struct {
	Generation int64 `json:"generation"`
}

// Validate rejects missing and stale fencing generations at the HTTP
// boundary. The runner state machine still validates the command as defense
// in depth.
func (request GenerationRequest) Validate() error {
	if request.Generation < 1 {
		return fmt.Errorf("runner claim generation must be positive")
	}
	return nil
}

// CompletionRequest delivers a runner outcome for one fenced claim.
type CompletionRequest struct {
	Generation int64          `json:"generation"`
	Content    string         `json:"content"`
	OK         bool           `json:"ok"`
	Metadata   map[string]any `json:"metadata,omitempty"`
}

// Validate preserves the provider-neutral result limits before completion is
// sent to the queue and durable adapter.
func (request CompletionRequest) Validate() error {
	if err := (GenerationRequest{Generation: request.Generation}).Validate(); err != nil {
		return err
	}
	return core.ValidateCapabilityResult(request.ToResult())
}

// ToResult maps the HTTP payload to the existing provider-neutral result
// command. It does not clone or persist the result.
func (request CompletionRequest) ToResult() core.CapabilityResult {
	return core.CapabilityResult{Content: request.Content, OK: request.OK, Metadata: request.Metadata}
}
