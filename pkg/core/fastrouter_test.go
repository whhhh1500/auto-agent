package core

import (
	"context"
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
