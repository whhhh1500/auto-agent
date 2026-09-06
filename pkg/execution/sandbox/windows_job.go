//go:build windows

package sandbox

import (
	"context"
	"errors"
	"sync"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

var errWindowsJob = errors.New("windows sandbox job unavailable")

type windowsJobLimits struct {
	MemoryBytes int64
	CPUTime     time.Duration
}

type windowsJobAPI interface {
	Create() (windows.Handle, error)
	SetExtendedLimits(windows.Handle, windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION) error
	QueryExtendedLimits(windows.Handle) (windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION, error)
	Terminate(windows.Handle) error
	WaitEmpty(context.Context, windows.Handle) error
	Close(windows.Handle) error
}

type windowsJob struct {
	mu         sync.Mutex
	api        windowsJobAPI
	handle     windows.Handle
	expected   windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION
	terminated bool
	closing    bool
	closed     bool
	done       chan struct{}
	closeErr   error
}

func newWindowsJob(api windowsJobAPI, limits windowsJobLimits) (*windowsJob, error) {
	if api == nil || !limits.valid() {
		return nil, errWindowsJob
	}
	handle, err := api.Create()
	if err != nil {
		return nil, errWindowsJob
	}
	expected := windowsJobExtendedLimits(limits)
	if err := api.SetExtendedLimits(handle, expected); err != nil {
		_ = api.Close(handle)
		return nil, errWindowsJob
	}
	actual, err := api.QueryExtendedLimits(handle)
	if err != nil || !sameWindowsJobExtendedLimits(expected, actual) {
		_ = api.Terminate(handle)
		_ = api.Close(handle)
		return nil, errWindowsJob
	}
	return &windowsJob{api: api, handle: handle, expected: expected, done: make(chan struct{})}, nil
}

func (job *windowsJob) jobHandle() windows.Handle {
	if job == nil {
		return 0
	}
	job.mu.Lock()
	defer job.mu.Unlock()
	if job.closed {
		return 0
	}
	return job.handle
}

func (job *windowsJob) verifyLimits() error {
	if job == nil {
		return errWindowsJob
	}
	job.mu.Lock()
	defer job.mu.Unlock()
	if job.closed {
		return errWindowsJob
	}
	actual, err := job.api.QueryExtendedLimits(job.handle)
	if err != nil || !sameWindowsJobExtendedLimits(job.expected, actual) {
		return errWindowsJob
	}
	return nil
}

func (limits windowsJobLimits) valid() bool {
	return limits.MemoryBytes > 0 && limits.MemoryBytes <= DefaultExecMemoryBytes && limits.CPUTime > 0 && limits.CPUTime <= DefaultExecWallTime && limits.CPUTime%100 == 0
}

func windowsJobExtendedLimits(limits windowsJobLimits) windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION {
	return windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION{BasicLimitInformation: windows.JOBOBJECT_BASIC_LIMIT_INFORMATION{
		PerJobUserTimeLimit: int64(limits.CPUTime / 100), // Windows uses 100ns units.
		LimitFlags: windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE |
			windows.JOB_OBJECT_LIMIT_DIE_ON_UNHANDLED_EXCEPTION |
			windows.JOB_OBJECT_LIMIT_ACTIVE_PROCESS |
			windows.JOB_OBJECT_LIMIT_PROCESS_MEMORY |
			windows.JOB_OBJECT_LIMIT_JOB_MEMORY |
			windows.JOB_OBJECT_LIMIT_JOB_TIME,
		ActiveProcessLimit: windowsJobActiveProcesses,
	}, ProcessMemoryLimit: uintptr(limits.MemoryBytes), JobMemoryLimit: uintptr(limits.MemoryBytes)}
}

const windowsJobActiveProcesses = 32

func sameWindowsJobExtendedLimits(expected, actual windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION) bool {
	return expected.BasicLimitInformation.PerJobUserTimeLimit == actual.BasicLimitInformation.PerJobUserTimeLimit &&
		expected.BasicLimitInformation.LimitFlags == actual.BasicLimitInformation.LimitFlags &&
		expected.BasicLimitInformation.ActiveProcessLimit == actual.BasicLimitInformation.ActiveProcessLimit &&
		expected.ProcessMemoryLimit == actual.ProcessMemoryLimit && expected.JobMemoryLimit == actual.JobMemoryLimit
}

func (job *windowsJob) Terminate(ctx context.Context) error {
	if ctx == nil {
		return context.Canceled
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	job.mu.Lock()
	defer job.mu.Unlock()
	if job.closed || job.terminated {
		return nil
	}
	if err := job.api.Terminate(job.handle); err != nil {
		return errWindowsJob
	}
	job.terminated = true
	return nil
}

func (job *windowsJob) WaitEmpty(ctx context.Context) error {
	if ctx == nil {
		return context.Canceled
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	job.mu.Lock()
	if job.closed {
		job.mu.Unlock()
		return nil
	}
	handle := job.handle
	job.mu.Unlock()
	if err := job.api.WaitEmpty(ctx, handle); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return errWindowsJob
	}
	return nil
}

func (job *windowsJob) Close(ctx context.Context) error {
	if ctx == nil {
		return context.Canceled
	}
	job.mu.Lock()
	if job.closed {
		job.mu.Unlock()
		return nil
	}
	if job.closing {
		done := job.done
		job.mu.Unlock()
		select {
		case <-done:
			job.mu.Lock()
			err := job.closeErr
			job.mu.Unlock()
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	job.closing = true
	job.done = make(chan struct{})
	handle := job.handle
	terminated := job.terminated
	job.mu.Unlock()

	result := error(nil)
	if !terminated {
		if err := job.api.Terminate(handle); err != nil {
			result = errWindowsJob
		} else {
			terminated = true
		}
	}
	// Never surrender the sole Job handle before a successful tree-empty proof.
	// KILL_ON_JOB_CLOSE is a backstop, not evidence that descendants are gone.
	if result == nil {
		if waitErr := job.api.WaitEmpty(ctx, handle); waitErr != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				result = ctxErr
			} else {
				result = errWindowsJob
			}
		}
	}
	closed := false
	if result == nil {
		if closeErr := job.api.Close(handle); closeErr != nil {
			result = errWindowsJob
		} else {
			closed = true
		}
	}
	if ctxErr := ctx.Err(); ctxErr != nil && closed {
		result = ctxErr
	}

	job.mu.Lock()
	job.terminated = terminated
	job.closed = closed
	if closed {
		job.handle = 0
	}
	job.closing = false
	job.closeErr = result
	close(job.done)
	job.mu.Unlock()
	return result
}

type windowsNativeJobAPI struct{}

var _ windowsJobAPI = windowsNativeJobAPI{}

func (windowsNativeJobAPI) Create() (windows.Handle, error) {
	handle, err := windows.CreateJobObject(nil, nil)
	if err != nil {
		return 0, err
	}
	if err := windows.SetHandleInformation(handle, windows.HANDLE_FLAG_INHERIT, 0); err != nil {
		_ = windows.CloseHandle(handle)
		return 0, err
	}
	return handle, nil
}

func (windowsNativeJobAPI) SetExtendedLimits(handle windows.Handle, limits windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION) error {
	_, err := windows.SetInformationJobObject(handle, windows.JobObjectExtendedLimitInformation, uintptr(unsafe.Pointer(&limits)), uint32(unsafe.Sizeof(limits)))
	return err
}

func (windowsNativeJobAPI) QueryExtendedLimits(handle windows.Handle) (windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION, error) {
	var limits windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION
	err := windows.QueryInformationJobObject(handle, windows.JobObjectExtendedLimitInformation, uintptr(unsafe.Pointer(&limits)), uint32(unsafe.Sizeof(limits)), nil)
	return limits, err
}

func (windowsNativeJobAPI) Terminate(handle windows.Handle) error {
	return windows.TerminateJobObject(handle, 1)
}
func (windowsNativeJobAPI) Close(handle windows.Handle) error { return windows.CloseHandle(handle) }

// This mirrors JOBOBJECT_BASIC_ACCOUNTING_INFORMATION from the Windows SDK.
// It has no pointer fields, so the layout is 48 bytes on both x86 and amd64.
type windowsJobBasicAccountingInformation struct {
	TotalUserTime             int64
	TotalKernelTime           int64
	ThisPeriodTotalUserTime   int64
	ThisPeriodTotalKernelTime int64
	TotalPageFaultCount       uint32
	TotalProcesses            uint32
	ActiveProcesses           uint32
	TotalTerminatedProcesses  uint32
}

func (windowsNativeJobAPI) WaitEmpty(ctx context.Context, handle windows.Handle) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		var accounting windowsJobBasicAccountingInformation
		if err := windows.QueryInformationJobObject(handle, windows.JobObjectBasicAccountingInformation, uintptr(unsafe.Pointer(&accounting)), uint32(unsafe.Sizeof(accounting)), nil); err != nil {
			return err
		}
		if accounting.ActiveProcesses == 0 {
			return nil
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
}
