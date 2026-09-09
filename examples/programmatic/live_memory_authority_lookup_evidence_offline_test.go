package main

import (
	"context"
	"testing"

	core "github.com/whhhh1500/auto-agent/pkg/core"
)

func TestMemoryAuthorityLookupEvidenceRejectsOtherAuthorityCode(t *testing.T) {
	values, err := newMemoryAuthorityValuesForEntity("Node-7.Alpha")
	if err != nil {
		t.Fatal(err)
	}
	other, err := memoryNonce()
	if err != nil || other == values.Current || other == values.Historical {
		t.Fatal("could not create a distinct malicious authority sidecar value")
	}
	model := newLiveModel(t, &memoryAuthorityLookupProbe{}, "offline-memory-authority-lookup-malicious-authority", 2, nil)
	observed := &memoryObservedModel{inner: model}
	fixture, err := newMemoryAuthorityLookupFixture(observed, "offline-memory-authority-lookup", values, "offline-malicious-authority")
	if err != nil {
		t.Fatal(err)
	}
	fixture.authority.mu.Lock()
	fixture.authority.code = other
	fixture.authority.mu.Unlock()
	result, err := fixture.runtime.RunTurn(context.Background(), fixture.principal, fixture.session, core.TurnInput{RunID: "offline-memory-authority-lookup-malicious-authority", Text: memoryAuthorityLookupPrompt(values.Entity)}, nil)
	if err != nil || result.Status != core.RunCompleted {
		t.Fatal("malicious authority sidecar fixture did not complete")
	}
	if err := assertMemoryAuthorityLookupRun(fixture, observed, model, fixture.session.Events(), result, values); err == nil {
		t.Fatal("acceptance accepted a structured authority response with the wrong fixture current value")
	}
	record := liveMemoryAuthorityLookupRecord(liveCase{name: "malicious-authority"}, result, "offline-model", model.Provider(), values.Entity, fixture.session.Events(), model, fixture, observed, false, 0)
	if record.AuthorityStructured || record.ConflictRecognized || record.CurrentAnswer || record.CurrentDurableAnswer || record.AcceptancePassed {
		t.Fatal("wrong authority code self-certified as current evidence")
	}
}
