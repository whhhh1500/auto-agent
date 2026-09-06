package core

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

type stubFastTools struct {
	authorized bool
	last       ToolCall
}

func (s *stubFastTools) Schemas() []ToolSchema  { return nil }
func (s *stubFastTools) Authorized(string) bool { return s.authorized }
func (s *stubFastTools) MaxCallBudget() int     { return 1 }
func (s *stubFastTools) Execute(_ context.Context, call ToolCall) (CapabilityResult, error) {
	s.last = call
	if call.Args != nil {
		call.Args["mutated"] = true
	}
	return CapabilityResult{Content: "ok", OK: true}, nil
}

func TestFastRouterClonesSharedArgs(t *testing.T) {
	shared := map[string]any{"symbol": "BTC"}
	router := &FastRouter{}
	router.Add(FastRule{
		Match:      func(string) bool { return true },
		Capability: "market.quote",
		Args:       func(string) map[string]any { return shared },
		Answer:     func(result CapabilityResult) string { return "quote: " + result.Content },
	})
	tools := &stubFastTools{authorized: true}
	dispatch, err := router.Dispatch(context.Background(), "quote", tools)
	if err != nil || !dispatch.Matched || dispatch.Blocked {
		t.Fatalf("dispatch failed: %#v err=%v", dispatch, err)
	}
	if _, ok := shared["mutated"]; ok {
		t.Fatal("fast route mutated the shared argument map")
	}
	if tools.last.Args["symbol"] != "BTC" {
		t.Fatalf("cloned args missing: %#v", tools.last.Args)
	}
	if dispatch.Answer != "quote: ok" {
		t.Fatalf("custom fast answer = %q", dispatch.Answer)
	}
}

func TestFastRouterBlocksInvalidCapability(t *testing.T) {
	router := &FastRouter{}
	router.Add(FastRule{Match: func(string) bool { return true }, Capability: "nodot"})
	tools := &stubFastTools{authorized: true}
	dispatch, err := router.Dispatch(context.Background(), "quote", tools)
	if err != nil || !dispatch.Matched || !dispatch.Blocked {
		t.Fatalf("invalid capability was executed: %#v err=%v", dispatch, err)
	}
	if tools.last.Name != "" {
		t.Fatalf("invalid capability reached execute: %#v", tools.last)
	}
}

func TestFastRouterAgentAppendFailureStopsBeforeToolExecution(t *testing.T) {
	_, _, _, user := testScopes()
	principal := testPrincipal(user)
	tools := &stubFastTools{authorized: true}
	llm := &neverLLM{}
	router := &FastRouter{}
	router.Add(FastRule{Match: func(string) bool { return true }, Capability: "market.quote"})
	session := mustSession(t, user, principal)
	agent, err := NewAgent(AgentOptions{
		LLM: llm, Tools: tools, Session: session, Fast: router,
		OnEvent: func(event SessionEvent) {
			if event.Type == EvToolCall {
				panic("tool-call transport failed")
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := agent.RunTurn(context.Background(), TurnInput{RunID: "run-fast-append", Text: "quote"})
	if err == nil || result.Status != RunFailed || !errors.Is(err, errFastToolCallAppend) || !strings.Contains(err.Error(), "event callback panicked") {
		t.Fatalf("fast append failure classification: result=%#v err=%v", result, err)
	}
	if tools.last.Name != "" {
		t.Fatalf("tool executed after tool-call append failure: %#v", tools.last)
	}
	if llm.called {
		t.Fatal("fast append failure fell through to the model")
	}
	failureCode := ""
	for _, event := range session.Events() {
		if event.Type != EvRunError {
			continue
		}
		var data RuntimeErrorData
		if err := json.Unmarshal(event.Data, &data); err != nil {
			t.Fatal(err)
		}
		failureCode = data.Code
	}
	if failureCode != "event_append_failed" {
		t.Fatalf("fast append failure event code=%q", failureCode)
	}
}

func TestFastRouterApprovalPendingResumesSameCall(t *testing.T) {
	runtime, principal, session, approver, providerCalls := durableApprovalFixture(t)
	journal := &fastCallOrderJournal{session: session}
	runtime.ToolJournal = journal
	llm := &neverLLM{}
	runtime.Models = ModelResolverFunc(func(context.Context, ModelSelection) (LlmAdapter, error) { return llm, nil })
	runtime.FastRouters = FastRouterResolverFunc(func(context.Context, *AgentProfileSnapshot) (*FastRouter, error) {
		router := &FastRouter{}
		router.Add(FastRule{Match: func(string) bool { return true }, Capability: "payment.release"})
		return router, nil
	})
	result, err := runtime.RunTurn(context.Background(), principal, session, TurnInput{RunID: "run-fast-approval", Text: "release"}, nil)
	if err != nil || result.Status != RunWaitingApproval || providerCalls.Load() != 0 || journal.begins != 0 {
		t.Fatalf("fast approval did not pause before journal begin: result=%#v provider=%d journal=%d err=%v", result, providerCalls.Load(), journal.begins, err)
	}
	pending, found, err := session.PendingApproval("run-fast-approval")
	if err != nil || !found || !pending.Fast || pending.ToolCall.ID == "" {
		t.Fatalf("fast approval checkpoint=%#v found=%t err=%v", pending, found, err)
	}
	approver.decide(ApprovalApproved)
	result, err = runtime.ResumeTurn(context.Background(), principal, session, ResumeInput{RunID: "run-fast-approval"}, nil)
	if err != nil || result.Status != RunCompleted || providerCalls.Load() != 1 || journal.begins != 1 || llm.called {
		t.Fatalf("fast approval resume failed: result=%#v provider=%d journal=%d llm=%t err=%v", result, providerCalls.Load(), journal.begins, llm.called, err)
	}
	calls := 0
	recordedCallID := ""
	for _, event := range session.Events() {
		if event.Type == EvToolCall {
			calls++
			var data ToolCallData
			if err := json.Unmarshal(event.Data, &data); err != nil {
				t.Fatal(err)
			}
			recordedCallID = data.CallID
		}
	}
	if calls != 1 {
		t.Fatalf("fast approval recorded %d tool calls, want one stable call", calls)
	}
	if recordedCallID != pending.ToolCall.ID || recordedCallID != pending.ResumeCall.ID {
		t.Fatalf("fast approval call IDs changed: event=%q request=%q resume=%q", recordedCallID, pending.ToolCall.ID, pending.ResumeCall.ID)
	}
}
