package server

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	core "github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/storage"
)

// runtimeWithToolCheckpoint gives one run a private journal boundary without
// mutating the server's shared Runtime or the public executor contract.
func runtimeWithToolCheckpoint(runtime *core.Runtime, writer *storage.WriteBehind, cancel context.CancelFunc) (*core.Runtime, *toolCheckpointFailure) {
	copyOf := *runtime
	failure := &toolCheckpointFailure{}
	if copyOf.ToolJournal != nil {
		journal := &checkpointToolInvocationJournal{
			ToolInvocationJournal: copyOf.ToolJournal,
			checkpoint:            writer.Checkpoint,
			cancel:                cancel,
			failure:               failure,
		}
		if reader, ok := copyOf.ToolJournal.(core.ToolInvocationReader); ok && reader != nil {
			copyOf.ToolJournal = &checkpointToolInvocationJournalWithReader{checkpointToolInvocationJournal: journal, reader: reader}
		} else {
			copyOf.ToolJournal = journal
		}
	}
	return &copyOf, failure
}

func (s *Server) runtimeWithQueuedToolCheckpoint(runtime *core.Runtime, writer *storage.WriteBehind, cancel context.CancelFunc, fence storage.SessionWriteFence, session *core.Session, principal core.Principal, recoveredContinuation bool) (*core.Runtime, *toolCheckpointFailure, error) {
	if s.nativeStrict == nil {
		copyOf, failure := runtimeWithToolCheckpoint(runtime, writer, cancel)
		return copyOf, failure, nil
	}
	store, ok := s.sessions.(*storage.SQLSessionStore)
	if !ok || runtime == nil || runtime.ToolJournal == nil || runtime.ModelCallGate != nil {
		return nil, nil, fmt.Errorf("native queued admission requires the unwrapped native SQL runtime")
	}
	copyOf := *runtime
	failure := &toolCheckpointFailure{}
	journal := &nativeQueuedToolInvocationJournal{
		ToolInvocationJournal: runtime.ToolJournal, server: s, store: store, writer: writer,
		cancel: cancel, failure: failure, fence: fence, session: session, principal: principal, runtime: runtime,
	}
	if reader, ok := runtime.ToolJournal.(core.ToolInvocationReader); ok && reader != nil {
		copyOf.ToolJournal = &nativeQueuedToolInvocationJournalWithReader{nativeQueuedToolInvocationJournal: journal, reader: reader}
	} else {
		copyOf.ToolJournal = journal
	}
	copyOf.ModelCallGate = &nativeQueuedModelCallGate{
		server: s, store: store, writer: writer, cancel: cancel, failure: failure,
		fence: fence, session: session, principal: principal, recoveredContinuation: recoveredContinuation,
	}
	return &copyOf, failure, nil
}

type toolCheckpointFailure struct {
	mu  sync.Mutex
	err error
}

func (f *toolCheckpointFailure) record(err error) {
	if err == nil {
		return
	}
	f.mu.Lock()
	if f.err == nil {
		f.err = err
	}
	f.mu.Unlock()
}

func (f *toolCheckpointFailure) Err() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.err
}

func (f *toolCheckpointFailure) Failed() bool { return f.Err() != nil }

type checkpointToolInvocationJournal struct {
	core.ToolInvocationJournal
	checkpoint func(context.Context) error
	cancel     context.CancelFunc
	failure    *toolCheckpointFailure
}

// checkpointToolInvocationJournalWithReader preserves the optional read-only
// journal capability only when the wrapped journal supplied it. This keeps
// selection fail-closed for routes that require durable catalog evidence.
type checkpointToolInvocationJournalWithReader struct {
	*checkpointToolInvocationJournal
	reader core.ToolInvocationReader
}

func (j *checkpointToolInvocationJournalWithReader) GetToolInvocation(ctx context.Context, invocation core.ToolInvocation) (core.ToolInvocationRecord, bool, error) {
	return j.reader.GetToolInvocation(ctx, invocation)
}

type nativeQueuedToolInvocationJournal struct {
	core.ToolInvocationJournal
	server    *Server
	store     *storage.SQLSessionStore
	writer    *storage.WriteBehind
	cancel    context.CancelFunc
	failure   *toolCheckpointFailure
	fence     storage.SessionWriteFence
	session   *core.Session
	principal core.Principal
	runtime   *core.Runtime
}

type nativeQueuedToolInvocationJournalWithReader struct {
	*nativeQueuedToolInvocationJournal
	reader core.ToolInvocationReader
}

func (j *nativeQueuedToolInvocationJournalWithReader) GetToolInvocation(ctx context.Context, invocation core.ToolInvocation) (core.ToolInvocationRecord, bool, error) {
	return j.reader.GetToolInvocation(ctx, invocation)
}

func (j *nativeQueuedToolInvocationJournal) CompleteToolInvocation(ctx context.Context, invocation core.ToolInvocation, result core.CapabilityResult) (core.ToolInvocationRecord, error) {
	record, err := j.ToolInvocationJournal.CompleteToolInvocation(ctx, invocation, result)
	if err != nil {
		return record, err
	}
	if j.server != nil && j.server.nativeQueuedRecoveryTestHooks != nil && j.server.nativeQueuedRecoveryTestHooks.afterToolJournalComplete != nil {
		j.server.nativeQueuedRecoveryTestHooks.afterToolJournalComplete()
	}
	return record, nil
}

func (j *checkpointToolInvocationJournal) BeginToolInvocation(ctx context.Context, invocation core.ToolInvocation) (core.ToolInvocationRecord, core.ToolInvocationDecision, error) {
	if err := j.checkpoint(ctx); err != nil {
		if j.failure != nil {
			j.failure.record(err)
		}
		j.cancel()
		return core.ToolInvocationRecord{}, "", err
	}
	return j.ToolInvocationJournal.BeginToolInvocation(ctx, invocation)
}

func (j *nativeQueuedToolInvocationJournal) BeginToolInvocation(ctx context.Context, invocation core.ToolInvocation) (core.ToolInvocationRecord, core.ToolInvocationDecision, error) {
	if err := j.writer.Checkpoint(ctx); err != nil {
		return j.fail(err)
	}
	version := j.writer.SavedVersion()
	if j.session == nil || version != j.session.Version() {
		return j.fail(fmt.Errorf("native queued tool checkpoint is not the complete session prefix"))
	}
	lease, err := j.server.acquireExecutionProjection(ctx)
	if err != nil {
		return j.fail(err)
	}
	defer lease.Release()
	epoch, err := lease.appliedEpoch()
	if err != nil {
		return j.fail(err)
	}
	resolver, ok := j.server.runPrincipal.(*storage.SQLQueuedPrincipalResolver)
	if !ok {
		return j.fail(fmt.Errorf("native queued tool admission requires the native SQL principal resolver"))
	}
	currentPrincipal, err := resolver.ResolveRunPrincipal(ctx, invocation.TenantID, invocation.SubjectID)
	if err != nil {
		return j.fail(err)
	}
	if !reflect.DeepEqual(currentPrincipal, j.principal) {
		return j.fail(fmt.Errorf("native queued principal changed before tool admission"))
	}
	snapshot, err := (core.CapabilityResolver{Registry: j.runtime.Capabilities}).Resolve(j.principal, j.session.Scope())
	if err != nil {
		return j.fail(err)
	}
	var expected *core.SnapshotCapability
	for _, capability := range snapshot.Capabilities() {
		if capability.Manifest.ID == invocation.CapabilityID {
			copyOf := capability
			expected = &copyOf
			break
		}
	}
	if expected == nil {
		return j.fail(fmt.Errorf("native queued capability %q is unavailable", invocation.CapabilityID))
	}
	if expected.Manifest.RequiresApproval {
		return j.ToolInvocationJournal.BeginToolInvocation(ctx, invocation)
	}
	record, decision, err := j.store.BeginNativeQueuedToolEffectFenced(ctx, j.fence, version, storage.NativeQueuedToolEffectWitnessInput{
		Invocation: invocation, AuthorizationEpoch: epoch, BootstrapRevision: j.server.nativeStrict.bootstrapRevision, ExpectedCapability: *expected,
	})
	if err != nil {
		return j.fail(err)
	}
	return record, decision, nil
}

func (j *nativeQueuedToolInvocationJournal) fail(err error) (core.ToolInvocationRecord, core.ToolInvocationDecision, error) {
	if j.failure != nil {
		j.failure.record(err)
	}
	if j.cancel != nil {
		j.cancel()
	}
	return core.ToolInvocationRecord{}, "", err
}
