package sandbox

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestProcessLifecycleCloseCancelsBeforeLatePublish(t *testing.T) {
	canceled := make(chan struct{})
	lifecycle := processLifecycle{}
	run, err := lifecycle.begin(func() { close(canceled) })
	if err != nil {
		t.Fatal(err)
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- lifecycle.close(context.Background(), nil) }()
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("close did not cancel a not-yet-started run")
	}
	if lifecycle.publish(run, "late-process") {
		t.Fatal("closed lifecycle accepted a late process")
	}
	if !lifecycle.finish(run) {
		t.Fatal("finished run was not observed as closed")
	}
	if err := <-closeDone; err != nil {
		t.Fatalf("close returned %v", err)
	}
}

func TestProcessLifecycleCloseWaitsForPublishedProcess(t *testing.T) {
	lifecycle := processLifecycle{}
	run, err := lifecycle.begin(func() {})
	if err != nil {
		t.Fatal(err)
	}
	if !lifecycle.publish(run, struct{}{}) {
		t.Fatal("publish failed")
	}
	killed := make(chan struct{})
	closeDone := make(chan error, 1)
	go func() {
		closeDone <- lifecycle.close(context.Background(), func(any) { close(killed) })
	}()
	select {
	case <-killed:
	case <-time.After(time.Second):
		t.Fatal("close did not invoke process kill")
	}
	select {
	case err := <-closeDone:
		t.Fatalf("close returned before run finished: %v", err)
	default:
	}
	lifecycle.finish(run)
	if err := <-closeDone; err != nil {
		t.Fatalf("close returned %v", err)
	}
}

func TestProcessLifecycleSecondCloseStillWaitsAfterTimeout(t *testing.T) {
	lifecycle := processLifecycle{}
	run, err := lifecycle.begin(func() {})
	if err != nil {
		t.Fatal(err)
	}
	if !lifecycle.publish(run, struct{}{}) {
		t.Fatal("publish failed")
	}
	firstCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := lifecycle.close(firstCtx, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("first close error=%v", err)
	}
	secondDone := make(chan error, 1)
	killed := make(chan struct{})
	go func() { secondDone <- lifecycle.close(context.Background(), func(any) { close(killed) }) }()
	select {
	case <-killed:
	case <-time.After(time.Second):
		t.Fatal("second close did not inspect the active process")
	}
	select {
	case err := <-secondDone:
		t.Fatalf("second close returned before active run finished: %v", err)
	default:
	}
	lifecycle.finish(run)
	if err := <-secondDone; err != nil {
		t.Fatalf("second close returned %v", err)
	}
}

func TestProcessLifecycleCloseMarksClosedWhenCallerCanceled(t *testing.T) {
	lifecycle := processLifecycle{}
	run, err := lifecycle.begin(func() {})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := lifecycle.close(ctx, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("close error=%v", err)
	}
	if !lifecycle.isClosed() {
		t.Fatal("canceled close did not fence future runs")
	}
	lifecycle.finish(run)
}

func TestLocalRunContextHonorsWallDeadline(t *testing.T) {
	ctx, cancel := localRunContext(context.Background(), time.Millisecond)
	defer cancel()
	select {
	case <-ctx.Done():
		if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
			t.Fatalf("wall context error=%v", ctx.Err())
		}
	case <-time.After(time.Second):
		t.Fatal("wall deadline did not fire")
	}
}
