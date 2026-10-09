package jlite

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type benchmarkSpec struct {
	Mode                                           string
	PayloadBytes, Operations, Workers, Rate, Round int
	Overload                                       bool
	PressureMS                                     int
}
type benchmarkPercentiles struct{ P50MS, P95MS, P99MS float64 }
type benchmarkResult struct {
	Spec                                                                          benchmarkSpec
	GoVersion, OS, Architecture, CPUModel, MemoryLimit                            string
	GoMaxProcs                                                                    int
	Attempts, Committed, Published, Replicated, AdmissionRejected, OutboxRejected int
	LocalSeconds, LocalOpsPerSecond, OfferedOpsPerSecond                          float64
	LocalLatency, PublicationAge, ReplicaAge                                      benchmarkPercentiles
	DrainMS, CatchupMS                                                            float64
	Transactions                                                                  int64
	BatchMeanOperations                                                           float64
	BatchMaxOperations, BatchMaxBytes                                             int64
	ConfigMaxBatchOperations, ConfigMaxBatchBytes                                 int
	ConfigLingerMS                                                                float64
	ConfigMaxPendingOperations, ConfigMaxPendingBytes                             int
	ConfigMaxOutboxBytes                                                          int64
	PendingPeakOperations, PendingPeakBytes                                       int
	OutboxPeakBytes                                                               int64
	BaselineRSSBytes, PeakRSSBytes, BaselineHeapBytes, PeakHeapBytes              uint64
	OwnerDBBytes, OwnerWALBytes                                                   int64
	StreamBytes                                                                   uint64
}

func benchmarkLatency(values []time.Duration) benchmarkPercentiles {
	if len(values) == 0 {
		return benchmarkPercentiles{}
	}
	copy := append([]time.Duration(nil), values...)
	sort.Slice(copy, func(i, j int) bool { return copy[i] < copy[j] })
	rank := func(p int) float64 {
		i := (len(copy)*p+99)/100 - 1
		return float64(copy[i]) / float64(time.Millisecond)
	}
	return benchmarkPercentiles{rank(50), rank(95), rank(99)}
}

func benchmarkProcMemory(data string) (rss, hwm uint64) {
	for _, line := range strings.Split(data, "\n") {
		fields := strings.Fields(line)
		if len(fields) != 3 || fields[2] != "kB" {
			continue
		}
		value, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			continue
		}
		switch fields[0] {
		case "VmRSS:":
			rss = value * 1024
		case "VmHWM:":
			hwm = value * 1024
		}
	}
	return
}

func benchmarkMemory() (rss, hwm, heap uint64) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	data, _ := os.ReadFile("/proc/self/status")
	rss, hwm = benchmarkProcMemory(string(data))
	return rss, hwm, m.HeapAlloc
}

type benchmarkSamples struct {
	PendingOps, PendingBytes int
	OutboxBytes              int64
	RSS, Heap                uint64
	Err                      error
}

func sampleOperatingBenchmark(ctx context.Context, w *Writer) <-chan benchmarkSamples {
	done := make(chan benchmarkSamples, 1)
	go func() {
		var result benchmarkSamples
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			s, err := w.State()
			if err != nil {
				result.Err = err
				done <- result
				return
			}
			if s.PendingOperations > w.options.MaxPendingOperations || s.PendingBytes > w.options.MaxPendingBytes || s.OutboxBytes > w.options.MaxOutboxBytes {
				result.Err = errors.New("measured capacities exceeded")
				done <- result
				return
			}
			result.PendingOps = max(result.PendingOps, s.PendingOperations)
			result.PendingBytes = max(result.PendingBytes, s.PendingBytes)
			result.OutboxBytes = max(result.OutboxBytes, s.OutboxBytes)
			_, hwm, heap := benchmarkMemory()
			result.RSS = max(result.RSS, hwm)
			result.Heap = max(result.Heap, heap)
			select {
			case <-ctx.Done():
				done <- result
				return
			case <-ticker.C:
			}
		}
	}()
	return done
}

func runOperatingBenchmark(t *testing.T, spec benchmarkSpec) benchmarkResult {
	t.Helper()
	if spec.Mode != "single" && spec.Mode != "batch" || spec.PayloadBytes < 1 || spec.PayloadBytes > 64<<10 || spec.Workers < 1 || spec.Operations < 0 || spec.Rate < 0 {
		t.Fatal("invalid benchmark spec")
	}
	cfg := replicaConfig("owner")
	if spec.Mode == "single" {
		cfg.Limits.MaxBatchOperations = 1
	}
	options := DefaultWriterOptions()
	// Keep both modes' capacities identical and accommodate the high-arrival
	// 512-writer workload even at 16 KiB. Defaults remain unchanged in jlite.
	options.MaxPendingBytes = 16 << 20
	if spec.Overload {
		options.MaxPendingOperations = 64
		options.MaxPendingBytes = 1 << 20
		options.MaxOutboxBytes = 1 << 20
		if spec.PressureMS <= 0 {
			t.Fatal("invalid pressure duration")
		}
	}
	path := filepath.Join(t.TempDir(), "owner.zova")
	w := openTestWriter(t, path, cfg, options)
	s, js := publisherServer(t, t.TempDir(), cfg)
	o := testPublisherOptions(s)
	p := openTestPublisher(t, w, o)
	value := bytes.Repeat([]byte{0x5a}, spec.PayloadBytes)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	const warm = 8
	for i := range warm {
		req := WriteRequest{RequestID: fmt.Sprintf("warm-%d", i), Namespace: w.namespace, Operation: Put, Key: fmt.Sprintf("warm-%d", i), Value: value}
		if _, err := w.Write(ctx, req); err != nil {
			t.Fatal(err)
		}
		if ok, err := p.PublishNext(ctx); !ok || err != nil {
			t.Fatalf("warm publication: %v %v", ok, err)
		}
	}
	var replicas []*Replica
	var consumers []*ReplicaConsumer
	for _, node := range []string{"replica", "other"} {
		local := cfg
		local.NodeID = node
		r, err := OpenReplica(filepath.Join(t.TempDir(), node+".zova"), w.namespace, local)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { r.Close() })
		ro := DefaultReplicaOptions()
		ro.Connection = o
		ro.Connection.Username, ro.Connection.Password = node, "test-"+node
		c, err := ConnectReplicaConsumer(ctx, r, ro)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(c.Close)
		drainReplica(t, r, c)
		replicas = append(replicas, r)
		consumers = append(consumers, c)
	}
	consumers[1].Close() // second replica stays offline during measured ingestion/drain
	w.mu.Lock()
	err := w.db.Exec(`CREATE TABLE bench_batches(operations INTEGER, bytes INTEGER);
 CREATE TRIGGER bench_commit AFTER UPDATE OF sequence ON jlite_state WHEN NEW.sequence>OLD.sequence BEGIN
 INSERT INTO bench_batches VALUES(NEW.sequence-OLD.sequence,(SELECT sum(length(payload)) FROM jlite_outbox WHERE sequence>OLD.sequence AND sequence<=NEW.sequence)); END;`)
	w.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	result := benchmarkResult{Spec: spec, GoVersion: runtime.Version(), OS: runtime.GOOS, Architecture: runtime.GOARCH, GoMaxProcs: runtime.GOMAXPROCS(0), ConfigMaxBatchOperations: cfg.Limits.MaxBatchOperations, ConfigMaxBatchBytes: cfg.Limits.MaxBatchBytes, ConfigLingerMS: float64(cfg.Limits.BatchWait) / float64(time.Millisecond), ConfigMaxPendingOperations: options.MaxPendingOperations, ConfigMaxPendingBytes: options.MaxPendingBytes, ConfigMaxOutboxBytes: options.MaxOutboxBytes}
	cpu, _ := os.ReadFile("/proc/cpuinfo")
	for _, line := range strings.Split(string(cpu), "\n") {
		if strings.HasPrefix(line, "model name") {
			parts := strings.SplitN(line, ":", 2)
			if len(parts) == 2 {
				result.CPUModel = strings.TrimSpace(parts[1])
			}
			break
		}
	}
	limit, _ := os.ReadFile("/sys/fs/cgroup/memory.max")
	result.MemoryLimit = strings.TrimSpace(string(limit))
	result.BaselineRSSBytes, _, result.BaselineHeapBytes = benchmarkMemory()
	sampleCtx, stopSamples := context.WithCancel(ctx)
	defer stopSamples()
	samples := sampleOperatingBenchmark(sampleCtx, w)
	commits := make(map[uint64]time.Time)
	keys := make(map[string]bool)
	var latencies []time.Duration
	var mu sync.Mutex
	var writeErr error
	attempt := func(req WriteRequest) {
		start := time.Now()
		out, err := w.Write(ctx, req)
		end := time.Now()
		mu.Lock()
		defer mu.Unlock()
		result.Attempts++
		switch {
		case err == nil:
			result.Committed++
			commits[out.Sequence] = end
			keys[req.Key] = true
			latencies = append(latencies, end.Sub(start))
		case errors.Is(err, ErrAdmissionFull):
			result.AdmissionRejected++
		case errors.Is(err, ErrOutboxFull):
			result.OutboxRejected++
		default:
			writeErr = err
		}
	}
	started := time.Now()
	var workers sync.WaitGroup
	if spec.Overload {
		until := started.Add(time.Duration(spec.PressureMS) * time.Millisecond)
		for worker := range spec.Workers {
			workers.Add(1)
			go func() {
				defer workers.Done()
				for i := 0; time.Now().Before(until); i++ {
					attempt(WriteRequest{RequestID: fmt.Sprintf("pressure-%d-%d", worker, i), Namespace: w.namespace, Operation: Put, Key: fmt.Sprintf("pressure-%d", worker), Value: value})
				}
			}()
		}
	} else {
		jobs := make(chan int)
		for range spec.Workers {
			workers.Add(1)
			go func() {
				defer workers.Done()
				for i := range jobs {
					attempt(WriteRequest{RequestID: fmt.Sprintf("measured-%d", i), Namespace: w.namespace, Operation: Put, Key: fmt.Sprintf("measured-%d", i), Value: value})
				}
			}()
		}
		var ticker *time.Ticker
		if spec.Rate > 0 {
			ticker = time.NewTicker(time.Second / time.Duration(spec.Rate))
			defer ticker.Stop()
		}
		for i := range spec.Operations {
			if ticker != nil {
				select {
				case <-ticker.C:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			select {
			case jobs <- i:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		}
		close(jobs)
	}
	workers.Wait()
	result.LocalSeconds = time.Since(started).Seconds()
	result.LocalOpsPerSecond = float64(result.Committed) / result.LocalSeconds
	result.OfferedOpsPerSecond = float64(result.Attempts) / result.LocalSeconds
	result.LocalLatency = benchmarkLatency(latencies)
	if writeErr != nil {
		t.Fatal(writeErr)
	}
	if !spec.Overload && (result.Committed != spec.Operations || result.AdmissionRejected+result.OutboxRejected != 0) {
		t.Fatalf("ordinary workload dropped writes: %+v", result)
	}
	if spec.Overload && (result.Committed == 0 || result.AdmissionRejected+result.OutboxRejected == 0) {
		t.Fatalf("overload never filled capacity: %+v", result)
	}
	state, err := w.State()
	if err != nil {
		t.Fatal(err)
	}
	result.OutboxPeakBytes = state.OutboxBytes
	if state.Sequence != uint64(warm+result.Committed) || state.OutboxCount != int64(result.Committed) || state.OutboxBytes > options.MaxOutboxBytes {
		t.Fatalf("invalid local prefix/backlog: %+v", state)
	}
	result.Transactions = queryInt(t, w.db, "SELECT count(*) FROM bench_batches")
	result.BatchMaxOperations = queryInt(t, w.db, "SELECT max(operations) FROM bench_batches")
	result.BatchMaxBytes = queryInt(t, w.db, "SELECT max(bytes) FROM bench_batches")
	if result.Transactions == 0 || queryInt(t, w.db, "SELECT sum(operations) FROM bench_batches") != int64(result.Committed) || result.BatchMaxOperations > int64(cfg.Limits.MaxBatchOperations) || result.BatchMaxBytes > int64(cfg.Limits.MaxBatchBytes) {
		t.Fatal("invalid observed transaction grouping")
	}
	result.BatchMeanOperations = float64(result.Committed) / float64(result.Transactions)
	// Ingestion was local-only. Now drain publication while replica 1 consumes.
	type drained struct {
		ages []time.Duration
		err  error
	}
	applied := make(chan drained, 1)
	target := uint64(warm + result.Committed)
	drainStart := time.Now()
	go func() {
		previous := uint64(warm)
		var ages []time.Duration
		for previous < target {
			if _, err := consumers[0].ConsumeNext(ctx); err != nil {
				applied <- drained{err: err}
				return
			}
			st, err := replicas[0].State()
			if err != nil {
				applied <- drained{err: err}
				return
			}
			now := time.Now()
			for seq := previous + 1; seq <= st.AppliedSequence; seq++ {
				ages = append(ages, now.Sub(commits[seq]))
			}
			previous = st.AppliedSequence
		}
		applied <- drained{ages: ages}
	}()
	var pubAges []time.Duration
	for seq := uint64(warm + 1); seq <= target; seq++ {
		if ok, err := p.PublishNext(ctx); !ok || err != nil {
			cancel()
			<-applied
			t.Fatalf("measured publication: %v %v", ok, err)
		}
		pubAges = append(pubAges, time.Since(commits[seq]))
		result.Published++
	}
	online := <-applied
	if online.err != nil {
		t.Fatal(online.err)
	}
	result.Replicated = len(online.ages)
	result.DrainMS = float64(time.Since(drainStart)) / float64(time.Millisecond)
	result.PublicationAge = benchmarkLatency(pubAges)
	result.ReplicaAge = benchmarkLatency(online.ages)
	catchupStart := time.Now()
	ro := DefaultReplicaOptions()
	ro.Connection = o
	ro.Connection.Username, ro.Connection.Password = "other", "test-other"
	c, err := ConnectReplicaConsumer(ctx, replicas[1], ro)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	for {
		st, err := replicas[1].State()
		if err != nil {
			t.Fatal(err)
		}
		if st.AppliedSequence == target {
			break
		}
		if _, err := c.ConsumeNext(ctx); err != nil {
			t.Fatal(err)
		}
	}
	result.CatchupMS = float64(time.Since(catchupStart)) / float64(time.Millisecond)
	stopSamples()
	sample := <-samples
	if sample.Err != nil {
		t.Fatal(sample.Err)
	}
	result.PendingPeakOperations, result.PendingPeakBytes = sample.PendingOps, sample.PendingBytes
	result.OutboxPeakBytes = max(result.OutboxPeakBytes, sample.OutboxBytes)
	result.PeakRSSBytes = sample.RSS
	result.PeakHeapBytes = max(result.BaselineHeapBytes, sample.Heap)
	state, err = w.State()
	if err != nil || state.PublishedSequence != target || state.OutboxBytes != 0 || state.OutboxCount != 0 || state.PendingOperations != 0 {
		t.Fatalf("incomplete publication prefix: %+v %v", state, err)
	}
	if queryInt(t, w.db, "SELECT count(*) FROM jlite_requests") != int64(target) {
		t.Fatal("request metadata count differs from committed prefix")
	}
	for i := range warm {
		keys[fmt.Sprintf("warm-%d", i)] = true
	}
	if queryInt(t, w.db, "SELECT count(*) FROM jlite_records") != int64(len(keys)) {
		t.Fatal("owner record cardinality differs")
	}
	reads := []func(ReadRequest) (ReadResult, error){w.Get}
	for _, r := range replicas {
		st, err := r.State()
		if err != nil || !st.Ready || st.AppliedSequence != target || queryInt(t, r.db, "SELECT count(*) FROM jlite_applied") != int64(target) {
			t.Fatalf("replica did not converge: %+v %v", st, err)
		}
		if queryInt(t, r.db, "SELECT count(*) FROM jlite_records") != int64(len(keys)) {
			t.Fatal("replica record cardinality differs")
		}
		reads = append(reads, r.Get)
	}
	for _, read := range reads {
		for key := range keys {
			out, err := read(ReadRequest{Namespace: w.namespace, Key: key})
			if err != nil || !out.Found || !bytes.Equal(out.Value, value) || out.AppliedSequence != target {
				t.Fatalf("record validation %s: %+v %v", key, out, err)
			}
		}
	}
	if info, err := os.Stat(path); err == nil {
		result.OwnerDBBytes = info.Size()
	}
	if info, err := os.Stat(path + "-wal"); err == nil {
		result.OwnerWALBytes = info.Size()
	}
	streamName, _ := StreamName(w.namespace)
	stream, err := js.Stream(ctx, streamName)
	if err != nil {
		t.Fatal(err)
	}
	info, err := stream.Info(ctx)
	if err != nil || info.State.Msgs != target {
		t.Fatalf("retained history differs: %+v %v", info, err)
	}
	result.StreamBytes = info.State.Bytes
	return result
}

func TestBenchmarkCase(t *testing.T) {
	data := os.Getenv("JLITE_BENCH_CASE")
	if data == "" {
		t.Skip("benchmark subprocess only")
	}
	var spec benchmarkSpec
	if err := json.Unmarshal([]byte(data), &spec); err != nil {
		t.Fatal(err)
	}
	result := runOperatingBenchmark(t, spec)
	wire, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(os.Getenv("JLITE_BENCH_CASE_OUTPUT"), wire, 0600); err != nil {
		t.Fatal(err)
	}
}

func TestOperatingBenchmark(t *testing.T) {
	output := os.Getenv("JLITE_BENCH_OUTPUT")
	if output == "" {
		t.Skip("explicit benchmark run only")
	}
	if runtime.GOOS != "linux" {
		t.Fatal("use the Podman Linux benchmark runner")
	}
	rounds := 3
	if raw := os.Getenv("JLITE_BENCH_ROUNDS"); raw != "" {
		var err error
		rounds, err = strconv.Atoi(raw)
		if err != nil || rounds < 1 || rounds > 10 {
			t.Fatal("rounds must be 1..10")
		}
	}
	type report struct {
		Schema               int
		Revision, StartedUTC string
		Results              []benchmarkResult
	}
	reportData := report{Schema: 1, Revision: os.Getenv("JLITE_BENCH_REVISION"), StartedUTC: time.Now().UTC().Format(time.RFC3339)}
	var specs []benchmarkSpec
	for round := 1; round <= rounds; round++ {
		for _, size := range []int{64, 1024, 16384} {
			for _, rate := range []int{100, 0} {
				modes := []string{"single", "batch"}
				if round%2 == 0 {
					modes = []string{"batch", "single"}
				}
				for _, mode := range modes {
					n, workers := 1024, 512
					if rate > 0 {
						n = 128
						workers = 32
					}
					specs = append(specs, benchmarkSpec{Mode: mode, PayloadBytes: size, Operations: n, Workers: workers, Rate: rate, Round: round})
				}
			}
		}
		for _, mode := range []string{"single", "batch"} {
			specs = append(specs, benchmarkSpec{Mode: mode, PayloadBytes: 16384, Workers: 128, Round: round, Overload: true, PressureMS: 1500})
		}
	}
	for i, spec := range specs {
		path := filepath.Join(t.TempDir(), "case.json")
		wire, _ := json.Marshal(spec)
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestBenchmarkCase$", "-test.timeout=75s")
		cmd.Env = append(os.Environ(), "JLITE_BENCH_CASE="+string(wire), "JLITE_BENCH_CASE_OUTPUT="+path)
		logs, err := cmd.CombinedOutput()
		cancel()
		if err != nil {
			t.Fatalf("case %+v failed: %v\n%s", spec, err, logs)
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var result benchmarkResult
		if err := json.Unmarshal(raw, &result); err != nil {
			t.Fatal(err)
		}
		reportData.Results = append(reportData.Results, result)
		t.Logf("%d/%d round=%d mode=%s bytes=%d rate=%d overload=%t local=%.0f ops/s p95=%.2fms batch=%.1f drain=%.0fms catchup=%.0fms", i+1, len(specs), spec.Round, spec.Mode, spec.PayloadBytes, spec.Rate, spec.Overload, result.LocalOpsPerSecond, result.LocalLatency.P95MS, result.BatchMeanOperations, result.DrainMS, result.CatchupMS)
		raw, err = json.MarshalIndent(reportData, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(output, append(raw, '\n'), 0600); err != nil {
			t.Fatal(err)
		}
	}
}
