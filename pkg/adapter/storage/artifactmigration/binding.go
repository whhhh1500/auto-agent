// Package artifactmigration adapts concrete object-store migration primitives
// to the storage-neutral application contract.
package artifactmigration

import (
	"context"
	"errors"
	"fmt"

	app "github.com/cc-auto-agent/harness-core/pkg/app/artifactmigration"
	"github.com/cc-auto-agent/harness-core/pkg/storage"
)

// Binding owns one concrete migration target. It is intentionally composed at
// the server boundary so the application coordinator never sees object-store
// implementations or copy policy types.
type Binding struct {
	router  *storage.DynamicObjectStore
	target  storage.ObjectStore
	label   string
	options storage.ArtifactCopyOptions
}

// NewBinding creates a runtime binding for one candidate target. It performs
// no route change and starts no work.
func NewBinding(router *storage.DynamicObjectStore, target storage.ObjectStore, label string, options storage.ArtifactCopyOptions) (*Binding, error) {
	if router == nil || target == nil || label == "" {
		return nil, errors.New("artifact migration binding requires router, target, and label")
	}
	return &Binding{router: router, target: target, label: label, options: options}, nil
}

var _ app.ArtifactRuntime = (*Binding)(nil)

// Bind attaches a new migration or resumes an already attached generation.
func (b *Binding) Bind(ctx context.Context, id string, recorder app.MutationRecorder) (app.ArtifactCopy, error) {
	if b == nil || b.router == nil || b.target == nil || id == "" {
		return nil, errors.New("artifact migration binding is not initialized")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var sink storage.ArtifactMutationRecorder
	if recorder != nil {
		sink = mutationRecorder{sink: recorder}
	}
	source, target, err := b.router.BeginArtifactMigration(id, b.target, b.label, sink)
	if err != nil {
		// A process restart or a retry may already have installed this exact
		// generation. Attach to its stable source/target pair instead.
		source, target, err = b.router.ArtifactMigrationStores(id)
		if err != nil {
			return nil, err
		}
	}
	copier, err := storage.NewArtifactCopier(source, target, b.options)
	if err != nil {
		return nil, err
	}
	return &copyPort{copier: copier}, nil
}

// RestoreActive makes the configured target route active after durable state
// says the migration was already completed.
func (b *Binding) RestoreActive(ctx context.Context) error {
	if b == nil || b.router == nil || b.target == nil {
		return errors.New("artifact migration binding is not initialized")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return b.router.TrySwap(b.target, b.label)
}

// Activate executes the caller's finite final-delta callback under the
// DynamicObjectStore barrier and atomically switches the route on success.
func (b *Binding) Activate(ctx context.Context, id string, apply func(context.Context, app.ArtifactCopy) error) error {
	if b == nil || b.router == nil || b.target == nil || id == "" || apply == nil {
		return errors.New("artifact migration activation is not initialized")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return b.router.WithArtifactActivationBarrier(ctx, id, func(barrier context.Context, source, target storage.ObjectStore) error {
		copier, err := storage.NewArtifactCopier(source, target, b.options)
		if err != nil {
			return err
		}
		return apply(barrier, &copyPort{copier: copier})
	})
}

// Cancel abandons a candidate without changing the active route.
func (b *Binding) Cancel(id string) {
	if b != nil && b.router != nil {
		b.router.CancelArtifactMigration(id)
	}
}

type copyPort struct{ copier *storage.ArtifactCopier }

var _ app.ArtifactCopy = (*copyPort)(nil)

func (p *copyPort) Copy(ctx context.Context, prefix, startAfter string, progress app.ProgressFunc) (app.TransferStats, error) {
	stats, err := p.copier.Copy(ctx, prefix, startAfter, progressRecorder{fn: progress})
	return app.TransferStats{LastKey: stats.LastKey, Objects: stats.Objects, Bytes: stats.Bytes}, err
}

func (p *copyPort) Reconcile(ctx context.Context, prefix string, progress app.ProgressFunc) (app.TransferStats, error) {
	stats, err := p.copier.ReconcileWithProgress(ctx, prefix, progressRecorder{fn: progress})
	return app.TransferStats{Objects: stats.Objects, Bytes: stats.Bytes}, err
}

func (p *copyPort) ApplyAndVerify(ctx context.Context, changes []app.KeyChange) error {
	converted := make([]storage.ArtifactKeyChange, len(changes))
	for i, change := range changes {
		converted[i] = storage.ArtifactKeyChange{Key: change.Key, Deleted: change.Deleted}
	}
	return p.copier.ApplyAndVerifyKeys(ctx, converted)
}

type progressRecorder struct{ fn app.ProgressFunc }

func (r progressRecorder) RecordArtifactCopyProgress(ctx context.Context, progress storage.ArtifactCopyProgress) error {
	if r.fn == nil {
		return nil
	}
	return r.fn(ctx, app.TransferProgress{LastKey: progress.LastKey, Objects: progress.Objects, Bytes: progress.Bytes})
}

func (r progressRecorder) RecordArtifactReconcileProgress(ctx context.Context, progress storage.ArtifactReconcileProgress) error {
	if r.fn == nil {
		return nil
	}
	return r.fn(ctx, app.TransferProgress{LastKey: progress.LastKey, Objects: progress.Objects, Bytes: progress.Bytes})
}

type mutationRecorder struct{ sink app.MutationRecorder }

func (r mutationRecorder) RecordArtifactMutation(ctx context.Context, mutation storage.ArtifactMutation) error {
	if r.sink == nil {
		return nil
	}
	var operation app.MutationOperation
	switch mutation.Operation {
	case storage.ArtifactMutationPut:
		operation = app.MutationPut
	case storage.ArtifactMutationDelete:
		operation = app.MutationDelete
	default:
		return fmt.Errorf("unsupported artifact mutation operation %q", mutation.Operation)
	}
	return r.sink.Record(ctx, app.MutationEvidence{
		Operation: operation,
		Key:       mutation.Key,
		ETag:      mutation.ETag,
		Digest:    mutation.Digest,
		Size:      mutation.Size,
	})
}
