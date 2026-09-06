package core

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

func validModelCallRequest(t *testing.T) ModelCallRequest {
	t.Helper()
	_, _, _, user := testScopes()
	return ModelCallRequest{
		Principal: testPrincipal(user), Scope: user,
		SessionID: "model-validation-session", RunID: "model-validation-run",
		Provider: "provider", Model: "model",
	}
}

func TestModelCallRequestBoundsPrincipalClaims(t *testing.T) {
	t.Run("valid claims", func(t *testing.T) {
		request := validModelCallRequest(t)
		request.Principal.Grants = NewPermissionSet(PermRead, PermWrite)
		request.Principal.Attributes = map[string]string{"role": "operator"}
		if err := request.Validate(); err != nil {
			t.Fatal(err)
		}
	})

	for _, test := range []struct {
		name   string
		mutate func(*ModelCallRequest)
	}{
		{name: "too many grants", mutate: func(request *ModelCallRequest) {
			request.Principal.Grants = NewPermissionSet()
			for index := 0; index < MaxModelCallPrincipalMapItems+1; index++ {
				request.Principal.Grants[Permission(fmt.Sprintf("grant.%d", index))] = true
			}
		}},
		{name: "too many attributes", mutate: func(request *ModelCallRequest) {
			request.Principal.Attributes = make(map[string]string, MaxModelCallPrincipalMapItems+1)
			for index := 0; index < MaxModelCallPrincipalMapItems+1; index++ {
				request.Principal.Attributes[fmt.Sprintf("claim.%d", index)] = "value"
			}
		}},
		{name: "grant key too long", mutate: func(request *ModelCallRequest) {
			request.Principal.Grants = NewPermissionSet(Permission(strings.Repeat("g", MaxRunCompositionMetadataKeyBytes+1)))
		}},
		{name: "grant invalid utf8", mutate: func(request *ModelCallRequest) {
			request.Principal.Grants = NewPermissionSet(Permission(string([]byte{0xff})))
		}},
		{name: "grant unicode control", mutate: func(request *ModelCallRequest) {
			request.Principal.Grants = NewPermissionSet(Permission("grant\u0085name"))
		}},
		{name: "attribute key invalid utf8", mutate: func(request *ModelCallRequest) {
			request.Principal.Attributes = map[string]string{string([]byte{0xff}): "value"}
		}},
		{name: "attribute key unicode control", mutate: func(request *ModelCallRequest) {
			request.Principal.Attributes = map[string]string{"claim\u0085name": "value"}
		}},
		{name: "attribute value too long", mutate: func(request *ModelCallRequest) {
			request.Principal.Attributes = map[string]string{"claim": strings.Repeat("v", MaxRunCompositionMetadataValueBytes+1)}
		}},
		{name: "attribute value invalid utf8", mutate: func(request *ModelCallRequest) {
			request.Principal.Attributes = map[string]string{"claim": string([]byte{0xff})}
		}},
		{name: "attribute value unicode control", mutate: func(request *ModelCallRequest) {
			request.Principal.Attributes = map[string]string{"claim": "operator\u0085admin"}
		}},
		{name: "attribute control character", mutate: func(request *ModelCallRequest) {
			request.Principal.Attributes = map[string]string{"claim": "operator\nadmin"}
		}},
		{name: "claims too large", mutate: func(request *ModelCallRequest) {
			request.Principal.Attributes = map[string]string{}
			for index := 0; index < 16; index++ {
				request.Principal.Attributes[fmt.Sprintf("claim.%d", index)] = strings.Repeat("v", MaxRunCompositionMetadataValueBytes)
			}
		}},
		{name: "encoded claims too large", mutate: func(request *ModelCallRequest) {
			request.Principal.Attributes = make(map[string]string, 32)
			for index := 0; index < 32; index++ {
				request.Principal.Attributes[fmt.Sprintf("claim.%d", index)] = strings.Repeat("\"", 400)
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := validModelCallRequest(t)
			test.mutate(&request)
			if err := request.Validate(); err == nil {
				t.Fatal("invalid principal claims were accepted")
			}
		})
	}
}

func TestAgentRejectsOversizedPrincipalClaimsBeforeAdapter(t *testing.T) {
	_, _, _, user := testScopes()
	principal := testPrincipal(user)
	principal.Attributes = make(map[string]string, MaxModelCallPrincipalMapItems+1)
	for index := 0; index < MaxModelCallPrincipalMapItems+1; index++ {
		principal.Attributes[fmt.Sprintf("claim.%d", index)] = "value"
	}
	var calls int
	adapter := streamAdapterFunc(func(_ context.Context, _ GenerateOptions, emit func(StreamChunk)) error {
		calls++
		emit(StreamChunk{Kind: StreamKindAssistant, Text: "unexpected"})
		emit(StreamChunk{Kind: StreamKindFinish, FinishKind: FinishStop})
		return nil
	})
	if _, err := runStrictAgent(t, nil, principal, nil, adapter); err == nil || !strings.Contains(err.Error(), "principal attributes exceed") {
		t.Fatalf("oversized principal was not rejected before adapter: %v", err)
	}
	if calls != 0 {
		t.Fatalf("adapter calls=%d, want 0", calls)
	}
}
