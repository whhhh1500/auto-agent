package server

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestNativeQueuedRetentionWorkerLifecycleStartsOnceAndShutdownWaits(t *testing.T) {
	fixture := newNativeStrictWitnessFixture(t, false)
	server := fixture.api
	server.runWorkerCount = 0
	server.nativeQueuedRetentionEvery = time.Millisecond
	server.nativeQueuedRetentionWindow = time.Nanosecond

	var calls atomic.Int32
	entered := make(chan struct{}, 1)
	cancelled := make(chan struct{}, 1)
	release := make(chan struct{})
	server.nativeQueuedRetentionTestHooks = &nativeQueuedRetentionTestHooks{
		beforePrune: func(ctx context.Context) {
			calls.Add(1)
			select {
			case entered <- struct{}{}:
			default:
			}
			<-ctx.Done()
			select {
			case cancelled <- struct{}{}:
			default:
			}
			<-release
		},
	}
	serviceCtx, stopService := context.WithCancel(context.Background())
	defer stopService()
	if err := server.StartRunWorkers(serviceCtx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("native retention loop did not tick")
	}
	if err := server.StartRunWorkers(serviceCtx); err != nil {
		t.Fatalf("second StartRunWorkers: %v", err)
	}
	time.Sleep(10 * time.Millisecond)
	if got := calls.Load(); got != 1 {
		t.Fatalf("native retention loops=%d, want one", got)
	}

	shutdownDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		shutdownDone <- server.Shutdown(ctx)
	}()
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("Shutdown did not cancel the native retention context")
	}
	select {
	case err := <-shutdownDone:
		t.Fatalf("Shutdown returned before native retention left its worker wait group: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-shutdownDone; err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	time.Sleep(10 * time.Millisecond)
	if got := calls.Load(); got != 1 {
		t.Fatalf("native retention ticked after Shutdown cancellation: %d", got)
	}
}

func TestNativeQueuedRetentionDoesNotStartForGenericOrRunWorkerOnce(t *testing.T) {
	generic := newRunWorkerFixture(t)
	generic.server.runWorkerCount = 0
	generic.server.nativeQueuedRetentionEvery = time.Millisecond
	var genericTicks atomic.Int32
	generic.server.nativeQueuedRetentionTestHooks = &nativeQueuedRetentionTestHooks{
		beforePrune: func(context.Context) { genericTicks.Add(1) },
	}
	serviceCtx, stopService := context.WithCancel(context.Background())
	defer stopService()
	if err := generic.server.StartRunWorkers(serviceCtx); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	if err := generic.server.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := genericTicks.Load(); got != 0 {
		t.Fatalf("generic worker started native retention ticks=%d", got)
	}

	native := newNativeStrictWitnessFixture(t, false)
	native.api.nativeQueuedRetentionEvery = time.Millisecond
	var manualTicks atomic.Int32
	native.api.nativeQueuedRetentionTestHooks = &nativeQueuedRetentionTestHooks{
		beforePrune: func(context.Context) { manualTicks.Add(1) },
	}
	claimed, err := native.api.RunWorkerOnce(context.Background(), "worker-native-retention-manual")
	if err != nil || !claimed {
		t.Fatalf("RunWorkerOnce claimed=%t err=%v", claimed, err)
	}
	time.Sleep(20 * time.Millisecond)
	if got := manualTicks.Load(); got != 0 {
		t.Fatalf("RunWorkerOnce started native retention ticks=%d", got)
	}
}
