package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

var ErrAcceptedModelCallRequired = errors.New("accepted model call is required")

const MaxModelCallPrincipalMapItems = 64

// ModelCallBudget is non-secret budget evidence supplied to a model gate.
type ModelCallBudget struct {
	MaxInputTokens  int64 `json:"max_input_tokens,omitempty"`
	MaxOutputTokens int64 `json:"max_output_tokens,omitempty"`
}

// ModelCallRequest is the exact identity a model adapter is authorized to
// serve. It deliberately contains no credentials or prompt content.
type ModelCallRequest struct {
	Principal Principal       `json:"principal"`
	Scope     ScopePath       `json:"scope"`
	SessionID string          `json:"session_id"`
	RunID     string          `json:"run_id"`
	Step      int             `json:"step"`
	Provider  string          `json:"provider"`
	Model     string          `json:"model"`
	Deadline  time.Time       `json:"deadline,omitempty"`
	Budget    ModelCallBudget `json:"budget,omitempty"`
}

func (request ModelCallRequest) Validate() error {
	if request.Scope.Depth() == 0 {
		return fmt.Errorf("model call scope is empty")
	}
	if err := ValidateSessionID(request.SessionID); err != nil {
		return err
	}
	if err := ValidateRunID(request.RunID); err != nil {
		return err
	}
	if request.Step < 0 || request.Step > HardMaxSteps {
		return fmt.Errorf("model call step must be between 0 and %d", HardMaxSteps)
	}
	for label, value := range map[string]string{"provider": request.Provider, "model": request.Model} {
		if strings.TrimSpace(value) == "" || len(value) > 256 || strings.ContainsAny(value, "\r\n\x00") {
			return fmt.Errorf("model call %s is empty, too long, or contains control characters", label)
		}
	}
	if request.Budget.MaxInputTokens < 0 || request.Budget.MaxOutputTokens < 0 {
		return fmt.Errorf("model call budget cannot be negative")
	}
	for label, value := range map[string]string{"tenant id": request.Principal.TenantID, "subject id": request.Principal.SubjectID} {
		if strings.TrimSpace(value) == "" || len(value) > 512 || strings.ContainsAny(value, "\r\n\x00") {
			return fmt.Errorf("model call principal %s is empty, too long, or contains control characters", label)
		}
	}
	if err := validateModelCallPrincipalClaims(request.Principal); err != nil {
		return err
	}
	if !request.Principal.Scope.IsAncestorOf(request.Scope) {
		return fmt.Errorf("model call scope is outside principal scope")
	}
	return nil
}

func validateModelCallPrincipalClaims(principal Principal) error {
	if len(principal.Grants) > MaxModelCallPrincipalMapItems {
		return fmt.Errorf("model call principal grants exceed %d items", MaxModelCallPrincipalMapItems)
	}
	if len(principal.Attributes) > MaxModelCallPrincipalMapItems {
		return fmt.Errorf("model call principal attributes exceed %d items", MaxModelCallPrincipalMapItems)
	}

	for grant := range principal.Grants {
		key := string(grant)
		if invalidModelCallClaimKey(key) {
			return fmt.Errorf("model call principal grant is empty, too long, or contains control characters")
		}
	}
	for key, value := range principal.Attributes {
		if invalidModelCallClaimKey(key) {
			return fmt.Errorf("model call principal attribute key is empty, too long, or contains control characters")
		}
		if len(value) > MaxRunCompositionMetadataValueBytes || !utf8.ValidString(value) || containsUnicodeControl(value) {
			return fmt.Errorf("model call principal attribute value for %q is too long or contains control characters", key)
		}
	}
	encoded, err := json.Marshal(struct {
		Grants     PermissionSet     `json:"grants"`
		Attributes map[string]string `json:"attributes,omitempty"`
	}{Grants: principal.Grants, Attributes: principal.Attributes})
	if err != nil {
		return fmt.Errorf("model call principal claims could not be encoded: %w", err)
	}
	if len(encoded) > MaxRunCompositionMetadataBytes {
		return fmt.Errorf("model call principal claims exceed %d bytes", MaxRunCompositionMetadataBytes)
	}
	return nil
}

func invalidModelCallClaimKey(value string) bool {
	return strings.TrimSpace(value) == "" || len(value) > MaxRunCompositionMetadataKeyBytes || !utf8.ValidString(value) || containsUnicodeControl(value)
}

func containsUnicodeControl(value string) bool {
	for _, character := range value {
		if unicode.IsControl(character) {
			return true
		}
	}
	return false
}

// ModelCallGate authorizes one exact model adapter invocation.
type ModelCallGate interface {
	AuthorizeModelCall(context.Context, ModelCallRequest) error
}

// PrepareAcceptedModelCall validates principal claims and, when a gate is
// supplied, authorizes and mints the unforgeable proof for one model call.
// A nil gate preserves legacy behavior and returns an empty proof. Callers
// should add their own domain-specific error context at the boundary.
func PrepareAcceptedModelCall(ctx context.Context, gate ModelCallGate, request ModelCallRequest) (AcceptedModelCall, error) {
	if err := request.Validate(); err != nil {
		return AcceptedModelCall{}, fmt.Errorf("model call request: %w", err)
	}
	if ctx == nil {
		return AcceptedModelCall{}, fmt.Errorf("model call request: nil context")
	}
	if err := ctx.Err(); err != nil {
		return AcceptedModelCall{}, err
	}
	if gate == nil {
		return AcceptedModelCall{}, nil
	}
	if err := safeAuthorizeModelCall(gate, ctx, request); err != nil {
		return AcceptedModelCall{}, fmt.Errorf("model call gate: %w", err)
	}
	return mintAcceptedModelCall(request), nil
}

type acceptedModelCallSeal struct{}

// AcceptedModelCall is minted only after ModelCallGate authorization. Its
// fields are private so adapters cannot forge or alter the proof.
type AcceptedModelCall struct {
	request ModelCallRequest
	seal    *acceptedModelCallSeal
}

func mintAcceptedModelCall(request ModelCallRequest) AcceptedModelCall {
	return AcceptedModelCall{request: cloneModelCallRequest(request), seal: &acceptedModelCallSeal{}}
}

// RequireAcceptedModelCall verifies adapter-visible options against the exact
// request a gate authorized. The request is carried as non-secret evidence in
// GenerateOptions, so adapters never need to self-report hidden run state.
func RequireAcceptedModelCall(options GenerateOptions) error {
	if options.AcceptedCall.seal == nil {
		return ErrAcceptedModelCallRequired
	}
	if err := options.ModelCall.Validate(); err != nil {
		return fmt.Errorf("%w: expected request is invalid", ErrAcceptedModelCallRequired)
	}
	if options.Provider != options.ModelCall.Provider || options.Model != options.ModelCall.Model {
		return fmt.Errorf("%w: adapter binding mismatch", ErrAcceptedModelCallRequired)
	}
	if !sameModelCallRequest(options.AcceptedCall.request, options.ModelCall) {
		return fmt.Errorf("%w: model call identity mismatch", ErrAcceptedModelCallRequired)
	}
	return nil
}

func sameModelCallRequest(left, right ModelCallRequest) bool {
	return left.SessionID == right.SessionID && left.RunID == right.RunID && left.Step == right.Step &&
		left.Provider == right.Provider && left.Model == right.Model && left.Scope.Equal(right.Scope) &&
		samePrincipal(left.Principal, right.Principal) &&
		left.Deadline.Equal(right.Deadline) && left.Budget == right.Budget
}

func samePrincipal(left, right Principal) bool {
	if left.SubjectID != right.SubjectID || left.TenantID != right.TenantID || !left.Scope.Equal(right.Scope) {
		return false
	}
	if len(left.Grants) != len(right.Grants) || len(left.Attributes) != len(right.Attributes) {
		return false
	}
	for permission, allowed := range left.Grants {
		if right.Grants[permission] != allowed {
			return false
		}
	}
	for key, value := range left.Attributes {
		if right.Attributes[key] != value {
			return false
		}
	}
	return true
}

func cloneModelCallRequest(request ModelCallRequest) ModelCallRequest {
	request.Principal = clonePrincipal(request.Principal)
	return request
}

func safeAuthorizeModelCall(gate ModelCallGate, ctx context.Context, request ModelCallRequest) (err error) {
	defer func() {
		if recover() != nil {
			err = fmt.Errorf("model call gate panicked")
		}
	}()
	if gate == nil {
		return fmt.Errorf("model call gate is nil")
	}
	return gate.AuthorizeModelCall(ctx, cloneModelCallRequest(request))
}
