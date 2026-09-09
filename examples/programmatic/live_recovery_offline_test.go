package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	core "github.com/whhhh1500/auto-agent/pkg/core"
)

func TestSQLiteRecoveryContextFixtureOffline(t *testing.T) {
	ctx := context.Background()
	value, err := recoveryNonce()
	if err != nil {
		t.Fatal("could not create opaque recovery fixture value")
	}
	firstPrompt := recoveryFirstPrompt(value)
	probe := &recoveryOfflineProbe{value: value}
	observed := newRecoveryObservedAdapter(probe, 2)
	fixture, err := newRecoveryFixture(observed, "offline-sqlite-recovery")
	if err != nil {
		t.Fatal("could not construct recovery fixture")
	}
	runIDs := []string{"offline-recovery-first", "offline-recovery-final"}
	var results [2]core.TurnResult
	path := filepath.Join(t.TempDir(), "recovery.sqlite")
	db1, store1 := openRecoverySQLite(t, ctx, path)
	if err := store1.Create(ctx, fixture.session); err != nil {
		t.Fatal("could not create SQL session")
	}
	baseVersion := fixture.session.Version()
	first, err := fixture.runtime.RunTurn(ctx, fixture.principal, fixture.session, core.TurnInput{RunID: runIDs[0], Text: firstPrompt}, nil)
	results[0] = first
	if err != nil || first.Status != core.RunCompleted || strings.TrimSpace(first.Answer) != "ACK" {
		t.Fatal("offline first turn did not acknowledge")
	}
	if err := store1.Save(ctx, fixture.session, baseVersion); err != nil {
		t.Fatal("could not persist first turn")
	}
	persistedPrefix := fixture.session.Events()
	if err := db1.Close(); err != nil {
		t.Fatal("could not close first SQLite handle")
	}
	db2, store2 := openRecoverySQLite(t, ctx, path)
	defer db2.Close()
	loaded, err := store2.Load(ctx, fixture.session.ID())
	if err != nil {
		t.Fatal("could not load SQL session through fresh handle")
	}
	if !reflect.DeepEqual(persistedPrefix, loaded.Events()) {
		t.Fatal("fresh SQLite load did not restore exact first-turn prefix")
	}
	recoveredPrefix, err := loaded.DeriveMessages()
	if err != nil {
		t.Fatal("could not derive restored context prefix")
	}
	secondExpectedVersion := loaded.Version()
	secondRuntime, err := newRecoveryRuntime(observed, "offline-sqlite-recovery", fixture.product, loaded.ProfileID())
	if err != nil {
		t.Fatal("could not reconstruct runtime")
	}
	second, err := secondRuntime.RunTurn(ctx, loaded.Principal(), loaded, core.TurnInput{RunID: runIDs[1], Text: recoveryFinalPrompt}, nil)
	results[1] = second
	if err != nil || second.Status != core.RunCompleted {
		t.Fatal("offline recovered turn did not complete")
	}
	if err := store2.Save(ctx, loaded, secondExpectedVersion); err != nil {
		t.Fatal("could not persist recovered turn")
	}
	loaded, err = store2.Load(ctx, fixture.session.ID())
	if err != nil {
		t.Fatal("could not reload final SQL session")
	}
	events := loaded.Events()
	assertRecoveryAcceptance(t, fixture, observed, runIDs, results, events, loaded, db1 != db2 && store1 != store2, persistedPrefix, recoveredPrefix, firstPrompt, value)
	evidence := recoveryEvidenceFromRun("offline_sqlite_recovery", fixture, observed, runIDs, results, events, loaded, db1 != db2 && store1 != store2, persistedPrefix, recoveredPrefix, firstPrompt, value)
	evidence.AcceptancePassed = true
	if evidence.RequestedModel != "offline-sqlite-recovery" || evidence.SourceRevision == "" || !evidence.AcceptancePassed {
		t.Fatalf("offline recovery evidence incomplete: %+v", evidence)
	}
	directory := t.TempDir()
	if err := writeRecoveryEvidence(directory, evidence); err != nil {
		t.Fatal("could not write recovery evidence")
	}
	entries, err := os.ReadDir(directory)
	if err != nil || len(entries) != 1 {
		t.Fatal("recovery evidence did not create one file")
	}
	payload, err := os.ReadFile(filepath.Join(directory, entries[0].Name()))
	if err != nil || strings.Contains(string(payload), value) || strings.Contains(string(payload), firstPrompt) {
		t.Fatal("recovery evidence exposed fixture content")
	}
}

type recoveryOfflineProbe struct {
	value string
	mu    sync.Mutex
	phase int
}

func (*recoveryOfflineProbe) Provider() string { return "offline-sqlite-recovery" }
func (m *recoveryOfflineProbe) Stream(_ context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	m.mu.Lock()
	phase := m.phase
	m.phase++
	m.mu.Unlock()
	text := "ACK"
	if phase == 1 {
		if !recoveryMessagesContain(options.Messages, recoveryFirstPrompt(m.value)) {
			return errors.New("restored model context lacks exact durable first prompt")
		}
		text = "CURRENT: " + m.value
	} else if phase > 1 {
		return errors.New("offline recovery probe received unexpected model call")
	}
	emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: text, Usage: &core.TokenUsage{InputTokens: 13, OutputTokens: 2}})
	emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop})
	return nil
}
