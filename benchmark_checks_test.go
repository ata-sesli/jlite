package jlite

import (
	"testing"
	"time"
)

func TestBenchmarkLatencySummary(t *testing.T) {
	values := make([]time.Duration, 20)
	for i := range values {
		values[i] = time.Duration(20-i) * time.Millisecond
	}
	s := benchmarkLatency(values)
	if s.P50MS != 10 || s.P95MS != 19 || s.P99MS != 20 {
		t.Fatalf("nearest-rank percentiles: %+v", s)
	}
}

func TestBenchmarkProcMemory(t *testing.T) {
	rss, hwm := benchmarkProcMemory("Name:\tjlite\nVmRSS:\t1024 kB\nVmHWM:\t2048 kB\n")
	if rss != 1<<20 || hwm != 2<<20 {
		t.Fatalf("memory unit conversion: %d %d", rss, hwm)
	}
}

func TestBenchmarkHarnessChecksDurabilityAndConvergence(t *testing.T) {
	for _, mode := range []string{"single", "batch"} {
		t.Run(mode, func(t *testing.T) {
			result := runOperatingBenchmark(t, benchmarkSpec{Mode: mode, PayloadBytes: 64, Operations: 16, Workers: 4, Round: 1})
			if result.Committed != 16 || result.Published != 16 || result.Replicated != 16 || result.Transactions < 1 || result.OutboxPeakBytes < 1 || result.LocalLatency.P50MS <= 0 || result.CatchupMS <= 0 {
				t.Fatalf("incomplete measurements: %+v", result)
			}
			if mode == "single" && (result.Transactions != 16 || result.BatchMeanOperations != 1) {
				t.Fatalf("baseline batched accidentally: %+v", result)
			}
			if mode == "batch" && result.BatchMeanOperations <= 1 {
				t.Fatalf("batched workload never grouped: %+v", result)
			}
		})
	}
}

func TestBenchmarkOverloadRetainsCommittedPrefix(t *testing.T) {
	result := runOperatingBenchmark(t, benchmarkSpec{Mode: "batch", PayloadBytes: 16384, Workers: 128, Round: 1, Overload: true, PressureMS: 100})
	if result.Committed <= 0 || result.OutboxRejected+result.AdmissionRejected == 0 || result.OutboxPeakBytes > result.ConfigMaxOutboxBytes || result.PendingPeakBytes > result.ConfigMaxPendingBytes || result.Committed != result.Published || result.Committed != result.Replicated {
		t.Fatalf("unbounded/incomplete overload result: %+v", result)
	}
}
