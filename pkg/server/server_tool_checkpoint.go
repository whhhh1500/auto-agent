package server

import (
	"context"
	"sync"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
	"github.com/cc-auto-agent/harness-core/pkg/storage"
)

// runtimeWithToolCheckpoint gives one run a private journal boundary without
// mutating the server's shared Runtime or the public executor contract.
func runtimeWithToolCheckpoint(runtime *core.Runtime, writer *storage.WriteBehind, cancel context.CancelFunc) (*core.Runtime, *toolCheckpointFailure) {
	copyOf := *runtime
	failure := &toolCheckpointFailure{}
	if copyOf.ToolJournal != nil {
		copyOf.ToolJournal = &checkpointToolInvocationJournal{
			ToolInvocationJournal: copyOf.ToolJournal,
			checkpoint:            writer.Checkpoint,
			cancel:                cancel,
			failure:               failure,
		}
	}
	return &copyOf, failure
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
