//go:build windows

package sandbox

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

func TestWindowsJobConfiguresAndReadsBackEveryLimit(t *testing.T) {
	api := &fakeWindowsJobAPI{handle: 7}
	job, err := newWindowsJob(api, windowsJobLimits{MemoryBytes: 1024, CPUTime: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if api.setCount != 1 || api.queryCount != 1 || api.last.BasicLimitInformation.ActiveProcessLimit != windowsJobActiveProcesses || api.last.BasicLimitInformation.LimitFlags&(windows.JOB_OBJECT_LIMIT_BREAKAWAY_OK|windows.JOB_OBJECT_LIMIT_SILENT_BREAKAWAY_OK) != 0 {
		t.Fatalf("job limits=%#v set=%d query=%d", api.last, api.setCount, api.queryCount)
	}
	if err := job.Close(context.Background()); err != nil || api.terminateCount != 1 || api.waitCount != 1 || api.closeCount != 1 {
		t.Fatalf("close err=%v terminate=%d wait=%d close=%d", err, api.terminateCount, api.waitCount, api.closeCount)
	}
	if err := job.Close(context.Background()); err != nil || api.closeCount != 1 {
		t.Fatalf("idempotent close err=%v close=%d", err, api.closeCount)
	}
}

func TestWindowsJobRejectsSetAndReadbackFailures(t *testing.T) {
	for name, api := range map[string]*fakeWindowsJobAPI{
		"create":   {createErr: errors.New("create")},
		"set":      {handle: 7, setErr: errors.New("set")},
		"query":    {handle: 7, queryErr: errors.New("query")},
		"mismatch": {handle: 7, mutateQuery: true},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := newWindowsJob(api, windowsJobLimits{MemoryBytes: 1024, CPUTime: time.Second}); !errors.Is(err, errWindowsJob) {
				t.Fatalf("new job err=%v", err)
			}
		})
	}
}

func TestWindowsJobPreservesContextIdentityAndConcurrentClose(t *testing.T) {
	api := &fakeWindowsJobAPI{handle: 7, waitBlock: make(chan struct{})}
	job, err := newWindowsJob(api, windowsJobLimits{MemoryBytes: 1024, CPUTime: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := job.Terminate(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("terminate err=%v", err)
	}
	if err := job.WaitEmpty(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("wait err=%v", err)
	}
	result := make(chan error, 2)
	go func() { result <- job.Close(context.Background()) }()
	for deadline := time.Now().Add(time.Second); !api.waitStarted() && time.Now().Before(deadline); {
		runtime.Gosched()
	}
	go func() { result <- job.Close(context.Background()) }()
	close(api.waitBlock)
	if err := <-result; err != nil {
		t.Fatalf("first close err=%v", err)
	}
	if err := <-result; err != nil {
		t.Fatalf("second close err=%v", err)
	}
	if api.closeCount != 1 || api.terminateCount != 1 {
		t.Fatalf("close=%d terminate=%d", api.closeCount, api.terminateCount)
	}
}

func TestWindowsJobCloseRetainsHandleUntilTreeIsProvenEmpty(t *testing.T) {
	for name, configure := range map[string]func(*fakeWindowsJobAPI){
		"terminate": func(api *fakeWindowsJobAPI) { api.terminateErr = errors.New("terminate") },
		"wait":      func(api *fakeWindowsJobAPI) { api.waitErr = errors.New("wait") },
		"close":     func(api *fakeWindowsJobAPI) { api.closeErr = errors.New("close") },
	} {
		t.Run(name, func(t *testing.T) {
			api := &fakeWindowsJobAPI{handle: 7}
			configure(api)
			job, err := newWindowsJob(api, windowsJobLimits{MemoryBytes: 1024, CPUTime: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			if err := job.Close(context.Background()); !errors.Is(err, errWindowsJob) {
				t.Fatalf("first close=%v", err)
			}
			wantCloseAttempts := 0
			if name == "close" {
				// Closing the native handle was attempted only after Terminate and
				// WaitEmpty succeeded, but failure must preserve it for retry.
				wantCloseAttempts = 1
			}
			if api.closeCount != wantCloseAttempts || job.closed || job.handle == 0 {
				t.Fatalf("failure released unproven job: close=%d want=%d closed=%v handle=%d", api.closeCount, wantCloseAttempts, job.closed, job.handle)
			}
			api.terminateErr = nil
			api.waitErr = nil
			api.closeErr = nil
			if err := job.Close(context.Background()); err != nil {
				t.Fatalf("retry close=%v", err)
			}
			wantCloseAfterRetry := 1
			if name == "close" {
				wantCloseAfterRetry = 2
			}
			if api.closeCount != wantCloseAfterRetry || !job.closed || job.handle != 0 {
				t.Fatalf("retry did not close proven-empty job: close=%d want=%d closed=%v handle=%d", api.closeCount, wantCloseAfterRetry, job.closed, job.handle)
			}
		})
	}
}

func TestWindowsJobABILayout(t *testing.T) {
	if got := unsafe.Sizeof(windowsJobBasicAccountingInformation{}); got != 48 || unsafe.Offsetof(windowsJobBasicAccountingInformation{}.ActiveProcesses) != 40 {
		t.Fatalf("SDK accounting ABI size=%d active offset=%d", got, unsafe.Offsetof(windowsJobBasicAccountingInformation{}.ActiveProcesses))
	}
	if got := unsafe.Sizeof(windows.JOBOBJECT_BASIC_LIMIT_INFORMATION{}); (strconvIntSize() == 64 && got != 64) || (strconvIntSize() == 32 && got != 48) {
		t.Fatalf("SDK basic limit ABI size=%d arch=%d", got, strconvIntSize())
	}
	if got := unsafe.Sizeof(windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION{}); (strconvIntSize() == 64 && got != 144) || (strconvIntSize() == 32 && got != 112) {
		t.Fatalf("SDK extended limit ABI size=%d arch=%d", got, strconvIntSize())
	}
}

func strconvIntSize() int { return 32 << (^uint(0) >> 63) }

type fakeWindowsJobAPI struct {
	mu             sync.Mutex
	handle         windows.Handle
	createErr      error
	setErr         error
	queryErr       error
	mutateQuery    bool
	last           windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION
	setCount       int
	queryCount     int
	terminateCount int
	waitCount      int
	closeCount     int
	waitBlock      chan struct{}
	terminateErr   error
	waitErr        error
	closeErr       error
}

func (api *fakeWindowsJobAPI) Create() (windows.Handle, error) { return api.handle, api.createErr }
func (api *fakeWindowsJobAPI) SetExtendedLimits(_ windows.Handle, limits windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION) error {
	api.mu.Lock()
	defer api.mu.Unlock()
	api.last, api.setCount = limits, api.setCount+1
	return api.setErr
}
func (api *fakeWindowsJobAPI) QueryExtendedLimits(windows.Handle) (windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION, error) {
	api.mu.Lock()
	defer api.mu.Unlock()
	api.queryCount++
	value := api.last
	if api.mutateQuery {
		value.JobMemoryLimit++
	}
	return value, api.queryErr
}
func (api *fakeWindowsJobAPI) Terminate(windows.Handle) error {
	api.mu.Lock()
	defer api.mu.Unlock()
	api.terminateCount++
	return api.terminateErr
}
func (api *fakeWindowsJobAPI) WaitEmpty(ctx context.Context, _ windows.Handle) error {
	api.mu.Lock()
	api.waitCount++
	block := api.waitBlock
	waitErr := api.waitErr
	api.mu.Unlock()
	if block == nil {
		return waitErr
	}
	select {
	case <-block:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (api *fakeWindowsJobAPI) Close(windows.Handle) error {
	api.mu.Lock()
	defer api.mu.Unlock()
	api.closeCount++
	return api.closeErr
}

func (api *fakeWindowsJobAPI) waitStarted() bool {
	api.mu.Lock()
	defer api.mu.Unlock()
	return api.waitCount > 0
}
