package server

import (
	"context"
	"errors"
	"testing"

	core "github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/storage"
)

var _ RunPrincipalResolver = (*storage.SQLQueuedPrincipalResolver)(nil)

func TestQueuedWorkerTreatsStoragePrincipalIdentityErrorsAsPermanent(t *testing.T) {
	for _, principalErr := range []error{
		storage.ErrQueuedPrincipalNotFound,
		storage.ErrQueuedPrincipalIdentityMismatch,
		storage.ErrQueuedPrincipalInactive,
		storage.ErrQueuedPrincipalInvalid,
	} {
		t.Run(principalErr.Error(), func(t *testing.T) {
			fixture := newRunWorkerFixture(t)
			record := enqueueRunHTTP(t, fixture, "storage principal state")
			fixture.server.runPrincipal = RunPrincipalResolverFunc(func(context.Context, string, string) (core.Principal, error) {
				return core.Principal{}, principalErr
			})
			claimed, err := fixture.server.RunWorkerOnce(context.Background(), "worker-storage-principal")
			if err == nil || !claimed {
				t.Fatalf("permanent storage principal error was not surfaced: claimed=%t err=%v", claimed, err)
			}
			if !errors.Is(err, principalErr) {
				t.Fatalf("worker error=%v, want %v", err, principalErr)
			}
			terminal, getErr := fixture.queue.GetRun(context.Background(), record.RunID)
			if getErr != nil || terminal.Status != string(core.RunFailed) || terminal.ErrorCode != "principal_resolution_failed" {
				t.Fatalf("principal failure state=%#v err=%v", terminal, getErr)
			}
			if fixture.modelRuns.Load() != 0 {
				t.Fatalf("permanent principal error reached model: calls=%d", fixture.modelRuns.Load())
			}
		})
	}
}

func TestQueuedWorkerKeepsPrincipalInfrastructureErrorsRetryable(t *testing.T) {
	fixture := newRunWorkerFixture(t)
	record := enqueueRunHTTP(t, fixture, "storage principal infrastructure")
	fixture.server.runPrincipal = RunPrincipalResolverFunc(func(context.Context, string, string) (core.Principal, error) {
		return core.Principal{}, context.DeadlineExceeded
	})
	claimed, err := fixture.server.RunWorkerOnce(context.Background(), "worker-storage-principal-retry")
	if !claimed || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("retryable principal failure claimed=%t err=%v", claimed, err)
	}
	queued, getErr := fixture.queue.GetRun(context.Background(), record.RunID)
	if getErr != nil || queued.Status != storage.RunStatusQueued || queued.ErrorCode != "principal_resolution_failed" {
		t.Fatalf("retryable principal state=%#v err=%v", queued, getErr)
	}
	if fixture.modelRuns.Load() != 0 {
		t.Fatalf("retryable principal error reached model: calls=%d", fixture.modelRuns.Load())
	}
}
