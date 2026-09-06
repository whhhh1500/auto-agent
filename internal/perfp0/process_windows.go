//go:build windows

package perfp0

import (
	"errors"
	"unsafe"

	"golang.org/x/sys/windows"
)

type processSample struct {
	RSSBytes             uint64
	CPUKernelNanoseconds uint64
	CPUUserNanoseconds   uint64
	CPUTimeNanoseconds   uint64
}

type processMemoryCounters struct {
	CB                         uint32
	PageFaultCount             uint32
	PeakWorkingSetSize         uintptr
	WorkingSetSize             uintptr
	QuotaPeakPagedPoolUsage    uintptr
	QuotaPagedPoolUsage        uintptr
	QuotaPeakNonPagedPoolUsage uintptr
	QuotaNonPagedPoolUsage     uintptr
	PagefileUsage              uintptr
	PeakPagefileUsage          uintptr
}

var psapi = windows.NewLazySystemDLL("psapi.dll")
var getProcessMemoryInfo = psapi.NewProc("GetProcessMemoryInfo")

func readProcessSample() (processSample, error) {
	handle := windows.CurrentProcess()
	var creation, exit, kernel, user windows.Filetime
	if err := windows.GetProcessTimes(handle, &creation, &exit, &kernel, &user); err != nil {
		return processSample{}, err
	}
	kernelNanoseconds := filetime100ns(kernel) * 100
	userNanoseconds := filetime100ns(user) * 100
	cpu := kernelNanoseconds + userNanoseconds
	var counters processMemoryCounters
	counters.CB = uint32(unsafe.Sizeof(counters))
	ret, _, callErr := getProcessMemoryInfo.Call(
		uintptr(handle), uintptr(unsafe.Pointer(&counters)), uintptr(counters.CB),
	)
	if ret == 0 {
		if callErr == nil {
			callErr = errors.New("GetProcessMemoryInfo returned false")
		}
		return processSample{}, callErr
	}
	return processSample{RSSBytes: uint64(counters.WorkingSetSize), CPUKernelNanoseconds: kernelNanoseconds, CPUUserNanoseconds: userNanoseconds, CPUTimeNanoseconds: cpu}, nil
}

func filetime100ns(value windows.Filetime) uint64 {
	return uint64(value.HighDateTime)<<32 | uint64(value.LowDateTime)
}
