//go:build !windows

package perfp0

import "errors"

type processSample struct {
	RSSBytes             uint64
	CPUKernelNanoseconds uint64
	CPUUserNanoseconds   uint64
	CPUTimeNanoseconds   uint64
}

func readProcessSample() (processSample, error) {
	return processSample{}, errors.New("process RSS/CPU sampler is only implemented for Windows")
}
