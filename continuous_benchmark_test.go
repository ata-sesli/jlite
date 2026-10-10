package jlite

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type continuousSpec struct{ Rate, Operations, PayloadBytes, Round int }
type continuousProgress struct {
	Seconds                  float64
	Committed, Published     uint64
	Applied                  [2]uint64
	OutboxCount, OutboxBytes int64
}
type callTiming struct {
	Calls   int
	TotalMS float64
}

// Interface wrappers measure broker calls without changing production code.
type timedStream struct {
	jetstream.Stream
	InfoTiming, GetTiming callTiming
}

func (s *timedStream) Info(ctx context.Context, opts ...jetstream.StreamInfoOpt) (*jetstream.StreamInfo, error) {
	start := time.Now()
	info, err := s.Stream.Info(ctx, opts...)
	s.InfoTiming.Calls++
	s.InfoTiming.TotalMS += float64(time.Since(start)) / float64(time.Millisecond)
	return info, err
}
func (s *timedStream) GetMsg(ctx context.Context, seq uint64, opts ...jetstream.GetMsgOpt) (*jetstream.RawStreamMsg, error) {
	start := time.Now()
	msg, err := s.Stream.GetMsg(ctx, seq, opts...)
	s.GetTiming.Calls++
	s.GetTiming.TotalMS += float64(time.Since(start)) / float64(time.Millisecond)
	return msg, err
}

type timedJetStream struct {
	jetstream.JetStream
	Timing callTiming
}

func (s *timedJetStream) Publish(ctx context.Context, subject string, data []byte, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	start := time.Now()
	ack, err := s.JetStream.Publish(ctx, subject, data, opts...)
	s.Timing.Calls++
	s.Timing.TotalMS += float64(time.Since(start)) / float64(time.Millisecond)
	return ack, err
}

type continuousResult struct {
	Spec                                                                             continuousSpec
	GoVersion, OS, Architecture                                                      string
	GoMaxProcs                                                                       int
	Committed, Published                                                             int
	Replicated                                                                       [2]int
	PublishedDuringIngestion                                                         int
	ReplicatedDuringIngestion                                                        [2]int
	IngestionSeconds, TotalSeconds, DrainMS, LocalOpsPerSecond, EndToEndOpsPerSecond float64
	LocalLatency, PublicationAge                                                     benchmarkPercentiles
	ReplicaAge                                                                       [2]benchmarkPercentiles
	OutboxPeakBytes                                                                  int64
	OutboxPeakCount                                                                  int64
	PeakRSSBytes, PeakHeapBytes                                                      uint64
	Info, GetMsg, Publish, PublisherTotal                                            callTiming
	Progress                                                                         []continuousProgress
}

func runContinuousBenchmark(t *testing.T, spec continuousSpec) continuousResult {
	t.Helper()
	if spec.Rate <= 0 || spec.Operations <= 0 || spec.PayloadBytes < 1 || spec.PayloadBytes > 64<<10 {
		t.Fatal("invalid continuous spec")
	}
	cfg := replicaConfig("owner")
	w := openTestWriter(t, filepath.Join(t.TempDir(), "owner.zova"), cfg, DefaultWriterOptions())
	s, _ := publisherServer(t, t.TempDir(), cfg)
	o := testPublisherOptions(s)
	p := openTestPublisher(t, w, o)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	value := bytes.Repeat([]byte{0x5a}, spec.PayloadBytes)
	const warm = 8
	for i := range warm {
		_, err := w.Write(ctx, WriteRequest{RequestID: fmt.Sprintf("warm-%d", i), Namespace: w.namespace, Operation: Put, Key: fmt.Sprintf("warm-%d", i), Value: value})
		if err != nil {
			t.Fatal(err)
		}
		if ok, err := p.PublishNext(ctx); !ok || err != nil {
			t.Fatalf("warm publish: %v %v", ok, err)
		}
	}
	var replicas [2]*Replica
	var consumers [2]*ReplicaConsumer
	for i, node := range []string{"replica", "other"} {
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
		replicas[i], consumers[i] = r, c
	}
	ts := &timedStream{Stream: p.stream}
	tj := &timedJetStream{JetStream: p.js}
	p.stream, p.js = ts, tj
	result := continuousResult{Spec: spec, GoVersion: runtime.Version(), OS: runtime.GOOS, Architecture: runtime.GOARCH, GoMaxProcs: runtime.GOMAXPROCS(0)}
	// Event timestamps are joined by logical sequence after all goroutines stop.
	// A publication can precede delivery of the local Write response; clamp that
	// response-to-progress interval to zero rather than reporting negative lag.
	commits := make(map[uint64]time.Time)
	pubs := make(map[uint64]time.Time)
	var apps [2]map[uint64]time.Time
	for i := range apps {
		apps[i] = make(map[uint64]time.Time)
	}
	var mu sync.Mutex
	var latencies []time.Duration
	errCh := make(chan error, 35) // one possible failure per driver or write worker
	var loops sync.WaitGroup
	loops.Add(3)
	go func() {
		defer loops.Done()
		for ctx.Err() == nil {
			start := time.Now()
			ok, err := p.PublishNext(ctx)
			elapsed := time.Since(start)
			result.PublisherTotal.Calls++
			result.PublisherTotal.TotalMS += float64(elapsed) / float64(time.Millisecond)
			if err != nil {
				if ctx.Err() == nil {
					errCh <- err
				}
				return
			}
			if ok {
				st, err := w.State()
				if err != nil {
					errCh <- err
					return
				}
				mu.Lock()
				pubs[st.PublishedSequence] = time.Now()
				mu.Unlock()
			} else {
				select {
				case <-time.After(time.Millisecond):
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	for i := range replicas {
		go func() {
			defer loops.Done()
			previous := uint64(warm)
			for ctx.Err() == nil {
				if _, err := consumers[i].ConsumeNext(ctx); err != nil {
					if ctx.Err() == nil {
						errCh <- err
					}
					return
				}
				st, err := replicas[i].State()
				if err != nil {
					errCh <- err
					return
				}
				now := time.Now()
				mu.Lock()
				for seq := previous + 1; seq <= st.AppliedSequence; seq++ {
					apps[i][seq] = now
				}
				mu.Unlock()
				previous = st.AppliedSequence
			}
		}()
	}
	// Always join drivers before closing their database/connection resources.
	defer func() { cancel(); loops.Wait() }()
	started := time.Now()
	sampleCtx, stopSamples := context.WithCancel(ctx)
	samples := sampleOperatingBenchmark(sampleCtx, w)
	defer stopSamples()
	snapshot := func() {
		st, err := w.State()
		if err != nil {
			t.Fatal(err)
		}
		x := continuousProgress{Seconds: time.Since(started).Seconds(), Committed: st.Sequence - warm, Published: st.PublishedSequence - warm, OutboxCount: st.OutboxCount, OutboxBytes: st.OutboxBytes}
		for i, r := range replicas {
			rs, err := r.State()
			if err != nil {
				t.Fatal(err)
			}
			x.Applied[i] = rs.AppliedSequence - warm
		}
		result.Progress = append(result.Progress, x)
		result.OutboxPeakCount = max(result.OutboxPeakCount, x.OutboxCount)
	}
	jobs := make(chan int, 64)
	jobsClosed := false
	var workers sync.WaitGroup
	defer func() {
		cancel()
		if !jobsClosed {
			close(jobs)
		}
		workers.Wait()
	}()
	for range 32 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for i := range jobs {
				begin := time.Now()
				out, err := w.Write(ctx, WriteRequest{RequestID: fmt.Sprintf("live-%d", i), Namespace: w.namespace, Operation: Put, Key: fmt.Sprintf("live-%d", i), Value: value})
				end := time.Now()
				if err != nil {
					errCh <- err
					return
				}
				mu.Lock()
				commits[out.Sequence] = end
				latencies = append(latencies, end.Sub(begin))
				mu.Unlock()
			}
		}()
	}
	ticker := time.NewTicker(time.Second / time.Duration(spec.Rate))
	for i := range spec.Operations {
		select {
		case <-ticker.C:
		case err := <-errCh:
			t.Fatal(err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		select {
		case jobs <- i:
		case err := <-errCh:
			t.Fatal(err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		if i%spec.Rate == 0 {
			snapshot()
		}
	}
	ticker.Stop()
	close(jobs)
	jobsClosed = true
	workers.Wait()
	ingested := time.Now()
	result.IngestionSeconds = ingested.Sub(started).Seconds()
	snapshot()
	result.PublishedDuringIngestion = int(result.Progress[len(result.Progress)-1].Published)
	for i := range replicas {
		result.ReplicatedDuringIngestion[i] = int(result.Progress[len(result.Progress)-1].Applied[i])
	}
	target := uint64(warm + spec.Operations)
	for {
		mu.Lock()
		done := len(pubs) == spec.Operations && len(apps[0]) == spec.Operations && len(apps[1]) == spec.Operations
		mu.Unlock()
		if done {
			break
		}
		select {
		case err := <-errCh:
			t.Fatal(err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
	finished := time.Now()
	snapshot()
	cancel()
	loops.Wait()
	stopSamples()
	sample := <-samples
	if sample.Err != nil {
		t.Fatal(sample.Err)
	}
	result.TotalSeconds = finished.Sub(started).Seconds()
	result.DrainMS = float64(finished.Sub(ingested)) / float64(time.Millisecond)
	result.Committed = len(commits)
	result.Published = len(pubs)
	result.Replicated = [2]int{len(apps[0]), len(apps[1])}
	result.LocalOpsPerSecond = float64(result.Committed) / result.IngestionSeconds
	result.EndToEndOpsPerSecond = float64(result.Committed) / result.TotalSeconds
	result.LocalLatency = benchmarkLatency(latencies)
	var pubAges []time.Duration
	var replicaAges [2][]time.Duration
	for seq, commit := range commits {
		pubAges = append(pubAges, max(time.Duration(0), pubs[seq].Sub(commit)))
		for i := range replicas {
			replicaAges[i] = append(replicaAges[i], max(time.Duration(0), apps[i][seq].Sub(commit)))
		}
	}
	result.PublicationAge = benchmarkLatency(pubAges)
	for i := range replicas {
		result.ReplicaAge[i] = benchmarkLatency(replicaAges[i])
	}
	result.OutboxPeakBytes = sample.OutboxBytes
	result.PeakRSSBytes = sample.RSS
	result.PeakHeapBytes = sample.Heap
	result.Info, result.GetMsg, result.Publish = ts.InfoTiming, ts.GetTiming, tj.Timing
	st, err := w.State()
	if err != nil || st.Sequence != target || st.PublishedSequence != target || st.OutboxCount != 0 || st.PendingOperations != 0 {
		t.Fatalf("incomplete owner: %+v %v", st, err)
	}
	if queryInt(t, w.db, "SELECT count(*) FROM jlite_requests") != int64(target) {
		t.Fatal("owner retry prefix differs")
	}
	reads := []func(ReadRequest) (ReadResult, error){w.Get}
	for _, r := range replicas {
		st, err := r.State()
		if err != nil || !st.Ready || st.AppliedSequence != target || queryInt(t, r.db, "SELECT count(*) FROM jlite_applied") != int64(target) {
			t.Fatalf("incomplete replica: %+v %v", st, err)
		}
		reads = append(reads, r.Get)
	}
	for _, read := range reads {
		for i := range spec.Operations {
			out, err := read(ReadRequest{Namespace: w.namespace, Key: fmt.Sprintf("live-%d", i)})
			if err != nil || !out.Found || !bytes.Equal(out.Value, value) || out.AppliedSequence != target {
				t.Fatalf("record %d differs: %v", i, err)
			}
		}
	}
	return result
}

func TestContinuousBenchmarkCase(t *testing.T) {
	if os.Getenv("JLITE_CONTINUOUS_CASE") == "" {
		t.Skip("benchmark subprocess only")
	}
	var spec continuousSpec
	if err := json.Unmarshal([]byte(os.Getenv("JLITE_CONTINUOUS_CASE")), &spec); err != nil {
		t.Fatal(err)
	}
	result := runContinuousBenchmark(t, spec)
	raw, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(os.Getenv("JLITE_CONTINUOUS_CASE_OUTPUT"), raw, 0600); err != nil {
		t.Fatal(err)
	}
}
func TestContinuousBenchmark(t *testing.T) {
	output := os.Getenv("JLITE_CONTINUOUS_OUTPUT")
	if output == "" {
		t.Skip("explicit Podman benchmark only")
	}
	if runtime.GOOS != "linux" {
		t.Fatal("use Podman Linux runner")
	}
	rounds := 3
	if s := os.Getenv("JLITE_BENCH_ROUNDS"); s != "" {
		var err error
		rounds, err = strconv.Atoi(s)
		if err != nil || rounds < 1 || rounds > 10 {
			t.Fatal("rounds must be 1..10")
		}
	}
	report := struct {
		Schema               int
		Revision, StartedUTC string
		Results              []continuousResult
	}{Schema: 1, Revision: os.Getenv("JLITE_BENCH_REVISION"), StartedUTC: time.Now().UTC().Format(time.RFC3339)}
	for round := 1; round <= rounds; round++ {
		for _, rate := range []int{20, 80, 200} {
			spec := continuousSpec{Rate: rate, Operations: rate * 8, PayloadBytes: 1024, Round: round}
			wire, _ := json.Marshal(spec)
			path := filepath.Join(t.TempDir(), "case.json")
			args := []string{"-test.run=^TestContinuousBenchmarkCase$", "-test.timeout=4m"}
			if round == 1 && rate == 200 {
				args = append(args, "-test.blockprofile="+output+".block", "-test.blockprofilerate=1000000", "-test.cpuprofile="+output+".cpu")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
			cmd := exec.CommandContext(ctx, os.Args[0], args...)
			cmd.Env = append(os.Environ(), "JLITE_CONTINUOUS_CASE="+string(wire), "JLITE_CONTINUOUS_CASE_OUTPUT="+path)
			logs, err := cmd.CombinedOutput()
			cancel()
			if err != nil {
				t.Fatalf("case %+v: %v\n%s", spec, err, logs)
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var r continuousResult
			if err := json.Unmarshal(raw, &r); err != nil {
				t.Fatal(err)
			}
			report.Results = append(report.Results, r)
			t.Logf("round=%d rate=%d local=%.1f end-to-end=%.1f ops/s local-p95=%.2fms replica-p95=%.2f/%.2fms drain=%.0fms outbox-peak=%dbytes", round, rate, r.LocalOpsPerSecond, r.EndToEndOpsPerSecond, r.LocalLatency.P95MS, r.ReplicaAge[0].P95MS, r.ReplicaAge[1].P95MS, r.DrainMS, r.OutboxPeakBytes)
			raw, err = json.MarshalIndent(report, "", "  ")
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(output, append(raw, '\n'), 0600); err != nil {
				t.Fatal(err)
			}
		}
	}
}
