package server

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
	"github.com/cc-auto-agent/harness-core/pkg/storage"
)

// nativeQueuedModelSessionStore keeps ordinary fenced appends and native
// queued model outcomes on the same SQLSessionStore. It is deliberately
// private: accepting a separately supplied appender would permit the attempt,
// outcome, and Session tip to cross persistence domains.
type nativeQueuedModelSessionStore struct {
	*storage.SQLSessionStore
}

func (s *nativeQueuedModelSessionStore) AppendEventsFenced(ctx context.Context, fence storage.SessionWriteFence, expectedVersion int64, events []core.SessionEvent) error {
	if s == nil || s.SQLSessionStore == nil {
		return fmt.Errorf("native queued model append requires the native SQL session store")
	}
	outcomeEnd, err := nativeQueuedModelOutcomeEnd(events)
	if err != nil {
		return err
	}
	if outcomeEnd == 0 {
		return s.SQLSessionStore.AppendEventsFenced(ctx, fence, expectedVersion, events)
	}
	// A flush may also contain step/end, tool events, or run/end after the
	// assistant outcome. The model outcome evidence and the complete Session
	// suffix must commit together: a split prefix/suffix write would leave
	// WriteBehind unable to advance its saved version after a suffix failure.
	_, err = s.SQLSessionStore.AppendNativeQueuedModelOutcomeBatchFenced(ctx, fence, expectedVersion, outcomeEnd, events)
	return err
}

func nativeQueuedModelOutcomeEnd(events []core.SessionEvent) (int, error) {
	outcomeEnd, modelUsages := 0, 0
	for index, event := range events {
		if event.Type != core.EvRunUsage {
			continue
		}
		var usage core.RunUsageData
		if json.Unmarshal(event.Data, &usage) != nil {
			return 0, fmt.Errorf("native queued run usage is malformed")
		}
		if !strings.HasPrefix(usage.InvocationID, "model:") {
			continue
		}
		modelUsages++
		if index == 0 || events[index-1].Type != core.EvAssistantMessage || events[index-1].RunID != event.RunID {
			continue
		}
		if outcomeEnd != 0 {
			return 0, fmt.Errorf("native queued append contains multiple model outcomes")
		}
		outcomeEnd = index + 1
	}
	if outcomeEnd == 0 {
		// A provider can report usage with an error before it yields any assistant
		// outcome. That has no v46 delivery record and remains a normal fenced
		// failure suffix; v45 is its durable no-replay evidence.
		return 0, nil
	}
	if modelUsages != 1 {
		return 0, fmt.Errorf("native queued append contains an unpaired model usage")
	}
	return outcomeEnd, nil
}

type nativeQueuedModelCallGate struct {
	server                *Server
	store                 *storage.SQLSessionStore
	writer                *storage.WriteBehind
	cancel                context.CancelFunc
	failure               *toolCheckpointFailure
	fence                 storage.SessionWriteFence
	session               *core.Session
	principal             core.Principal
	recoveredContinuation bool
}

func (g *nativeQueuedModelCallGate) AuthorizeModelCall(ctx context.Context, request core.ModelCallRequest) error {
	if g == nil || g.server == nil || g.store == nil || g.writer == nil || g.session == nil || g.server.nativeStrict == nil {
		return g.fail(fmt.Errorf("native queued model admission is not configured"))
	}
	if err := g.writer.Checkpoint(ctx); err != nil {
		return g.fail(err)
	}
	version := g.writer.SavedVersion()
	if version != g.session.Version() {
		return g.fail(fmt.Errorf("native queued model checkpoint is not the complete session prefix"))
	}
	if request.SessionID != g.fence.SessionID || request.RunID != g.fence.RunID || !request.Scope.Equal(g.session.Scope()) || !reflect.DeepEqual(request.Principal, g.principal) {
		return g.fail(fmt.Errorf("native queued model request does not match the queued execution owner"))
	}
	lease, err := g.server.acquireExecutionProjection(ctx)
	if err != nil {
		return g.fail(err)
	}
	defer lease.Release()
	epoch, err := lease.appliedEpoch()
	if err != nil {
		return g.fail(err)
	}
	resolver, ok := g.server.runPrincipal.(*storage.SQLQueuedPrincipalResolver)
	if !ok {
		return g.fail(fmt.Errorf("native queued model admission requires the native SQL principal resolver"))
	}
	currentPrincipal, err := resolver.ResolveRunPrincipal(ctx, request.Principal.TenantID, request.Principal.SubjectID)
	if err != nil {
		return g.fail(err)
	}
	if !reflect.DeepEqual(currentPrincipal, g.principal) {
		return g.fail(fmt.Errorf("native queued principal changed before model admission"))
	}
	admitted, err := g.store.BeginNativeQueuedModelInvocationFenced(ctx, g.fence, version, storage.NativeQueuedModelInvocationInput{
		Request: request, AuthorizationEpoch: epoch, BootstrapRevision: g.server.nativeStrict.bootstrapRevision,
	})
	if err != nil {
		return g.fail(err)
	}
	if !admitted {
		return g.fail(fmt.Errorf("native queued model attempt already exists; provider replay is forbidden"))
	}
	if g.recoveredContinuation && g.server.nativeQueuedRecoveryTestHooks != nil && g.server.nativeQueuedRecoveryTestHooks.afterRecoveredModelAttempt != nil {
		g.server.nativeQueuedRecoveryTestHooks.afterRecoveredModelAttempt()
	}
	return nil
}

func (g *nativeQueuedModelCallGate) fail(err error) error {
	if err == nil {
		err = fmt.Errorf("native queued model admission failed")
	}
	if g != nil {
		if g.failure != nil {
			g.failure.record(err)
		}
		if g.cancel != nil {
			g.cancel()
		}
	}
	return err
}

func (s *Server) nativeQueuedWriteBehindStore() (core.SessionStore, error) {
	if s == nil {
		return nil, fmt.Errorf("native queued model persistence requires a server")
	}
	if s.nativeStrict == nil {
		return s.sessions, nil
	}
	store, ok := s.sessions.(*storage.SQLSessionStore)
	if !ok || store == nil {
		return nil, fmt.Errorf("native queued model persistence requires the native SQL session store")
	}
	return &nativeQueuedModelSessionStore{SQLSessionStore: store}, nil
}
