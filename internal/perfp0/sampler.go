package perfp0

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// caseMeasurement contains runtime and process observations for one case.
// Peak values are sampled while the case is running, then finalized once
// after the sampler goroutine stops.
type caseMeasurement struct {
	RuntimeBefore runtime.MemStats
	RuntimeAfter  runtime.MemStats
	HeapMaxSeen   uint64
	GoroutineMax  int64

	ProcessBefore    processSample
	ProcessAfter     processSample
	ProcessPeak      processSample
	ProcessBeforeErr error
	ProcessAfterErr  error
	ProcessSupported bool
}

type caseSampler struct {
	stop     chan struct{}
	done     chan struct{}
	interval time.Duration

	before runtime.MemStats
	heap   atomic.Uint64
	goMax  atomic.Int64

	processMu     sync.Mutex
	processBefore processSample
	processAfter  processSample
	processPeak   processSample
	processErr    error
	processOK     bool
}

func newCaseSampler(interval time.Duration) *caseSampler {
	if interval <= 0 {
		interval = time.Millisecond
	}
	sampler := &caseSampler{
		stop: make(chan struct{}), done: make(chan struct{}), interval: interval,
	}
	runtime.ReadMemStats(&sampler.before)
	sampler.heap.Store(sampler.before.HeapAlloc)
	sampler.record()
	go sampler.loop()
	return sampler
}

func (s *caseSampler) loop() {
	defer close(s.done)
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.record()
		case <-s.stop:
			return
		}
	}
}

func (s *caseSampler) record() {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	for {
		old := s.heap.Load()
		if stats.HeapAlloc <= old || s.heap.CompareAndSwap(old, stats.HeapAlloc) {
			break
		}
	}
	current := int64(runtime.NumGoroutine())
	for {
		old := s.goMax.Load()
		if current <= old || s.goMax.CompareAndSwap(old, current) {
			break
		}
	}
	process, err := readProcessSample()
	if err != nil {
		s.processMu.Lock()
		if s.processErr == nil {
			s.processErr = err
		}
		s.processMu.Unlock()
		return
	}
	s.processMu.Lock()
	defer s.processMu.Unlock()
	if !s.processOK {
		s.processBefore = process
		s.processPeak = process
		s.processOK = true
	}
	if process.RSSBytes > s.processPeak.RSSBytes {
		s.processPeak.RSSBytes = process.RSSBytes
	}
	if process.CPUTimeNanoseconds > s.processPeak.CPUTimeNanoseconds {
		s.processPeak.CPUTimeNanoseconds = process.CPUTimeNanoseconds
		s.processPeak.CPUKernelNanoseconds = process.CPUKernelNanoseconds
		s.processPeak.CPUUserNanoseconds = process.CPUUserNanoseconds
	}
	s.processAfter = process
}

func (s *caseSampler) Stop() caseMeasurement {
	close(s.stop)
	<-s.done
	// Capture the final boundary after the ticker has stopped. This catches a
	// final allocation but cannot prove that a sub-interval transient was seen.
	s.record()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	s.processMu.Lock()
	processBefore, processAfter, processPeak := s.processBefore, s.processAfter, s.processPeak
	processOK, processErr := s.processOK, s.processErr
	s.processMu.Unlock()
	if !processOK && processErr == nil {
		_, processErr = readProcessSample()
	}
	return caseMeasurement{
		RuntimeBefore: s.before, RuntimeAfter: after,
		HeapMaxSeen: s.heap.Load(), GoroutineMax: s.goMax.Load(),
		ProcessBefore: processBefore, ProcessAfter: processAfter, ProcessPeak: processPeak,
		ProcessBeforeErr: processErr, ProcessAfterErr: processErr, ProcessSupported: processOK,
	}
}

func runtimeStats(measurement caseMeasurement) RuntimeStats {
	before, after := measurement.RuntimeBefore, measurement.RuntimeAfter
	var allocDelta, mallocDelta, freesDelta uint64
	if after.TotalAlloc >= before.TotalAlloc {
		allocDelta = after.TotalAlloc - before.TotalAlloc
	}
	if after.Mallocs >= before.Mallocs {
		mallocDelta = after.Mallocs - before.Mallocs
	}
	if after.Frees >= before.Frees {
		freesDelta = after.Frees - before.Frees
	}
	var pauseDelta uint64
	if after.PauseTotalNs >= before.PauseTotalNs {
		pauseDelta = after.PauseTotalNs - before.PauseTotalNs
	}
	var gcDelta uint32
	if after.NumGC >= before.NumGC {
		gcDelta = after.NumGC - before.NumGC
	}
	return RuntimeStats{
		HeapAllocBefore: before.HeapAlloc, HeapAllocAfter: after.HeapAlloc,
		HeapAllocMaxSeen: measurement.HeapMaxSeen, TotalAllocDelta: allocDelta,
		MallocsDelta: mallocDelta, FreesDelta: freesDelta,
		NumGoroutinePeak: int(measurement.GoroutineMax), PauseTotalNsDelta: pauseDelta,
		NumGCDelta: gcDelta,
	}
}

func processStatsFrom(before, after, peak processSample, beforeErr, afterErr error, elapsed time.Duration) ProcessStats {
	if beforeErr != nil || afterErr != nil || before.RSSBytes == 0 || after.RSSBytes == 0 {
		return ProcessStats{Unsupported: processErrorText(firstError(beforeErr, afterErr))}
	}
	rssBefore, rssAfter, rssPeak := before.RSSBytes, after.RSSBytes, peak.RSSBytes
	cpuDelta := uint64(0)
	if after.CPUTimeNanoseconds >= before.CPUTimeNanoseconds {
		cpuDelta = after.CPUTimeNanoseconds - before.CPUTimeNanoseconds
	}
	kernelDelta := uint64(0)
	if after.CPUKernelNanoseconds >= before.CPUKernelNanoseconds {
		kernelDelta = after.CPUKernelNanoseconds - before.CPUKernelNanoseconds
	}
	userDelta := uint64(0)
	if after.CPUUserNanoseconds >= before.CPUUserNanoseconds {
		userDelta = after.CPUUserNanoseconds - before.CPUUserNanoseconds
	}
	var cpuPercent float64
	if elapsed > 0 {
		cpuPercent = float64(cpuDelta) / float64(elapsed.Nanoseconds()) * 100
	}
	return ProcessStats{
		RSSBeforeBytes: &rssBefore, RSSAfterBytes: &rssAfter, RSSPeakBytes: &rssPeak,
		CPUPercent: &cpuPercent, CPUTimeDeltaNanoseconds: &cpuDelta,
		CPUKernelTimeDeltaNs: &kernelDelta, CPUUserTimeDeltaNs: &userDelta, Supported: true,
	}
}

func firstError(values ...error) error {
	for _, err := range values {
		if err != nil {
			return err
		}
	}
	return nil
}

func processErrorText(err error) string {
	if err == nil {
		return "process sampler unavailable"
	}
	return fmt.Sprintf("%v", err)
}
