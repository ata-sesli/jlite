package jlite

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// These observers wrap existing interfaces. They do not change stream policy,
// retained-history validation, replica application, or acknowledgement semantics.
type diagnosticTiming struct {
	callTiming
	Latency benchmarkPercentiles
}

type diagnosticStages struct {
	Info, History, Fetch, ApplyAndLocal, Ack, Cycle diagnosticTiming
	Messages                                        int
}

type diagnosticReplicaSummary struct {
	Ingestion, Drain diagnosticStages
}

type diagnosticMemory struct {
	Label                             string
	BeforeGC, AfterGC                 runtime.MemStats
	GoroutinesBefore, GoroutinesAfter int
	RSSBefore, RSSAfter               uint64
}

type replicationDiagnosticResult struct {
	Mode     string
	Replicas [2]diagnosticReplicaSummary
	Memory   []diagnosticMemory
}

type replicationDiagnostics struct {
	mode      string
	dir       string
	ingesting atomic.Bool
	replicas  [2]*diagnosticReplica
	memory    []diagnosticMemory
}

func startReplicationDiagnostics(ctx context.Context, mode string, consumers [2]*ReplicaConsumer) *replicationDiagnostics {
	if mode == "" {
		return nil
	}
	d := &replicationDiagnostics{mode: mode, dir: os.Getenv("JLITE_DIAGNOSTIC_DIR")}
	d.ingesting.Store(true)
	parallel := mode == "parallel-acks" || mode == "batch32-parallel-acks"
	for i, c := range consumers {
		d.replicas[i] = newDiagnosticReplica(ctx, &d.ingesting, parallel)
		c.stream = diagnosticStream{c.stream, d.replicas[i]}
		c.consumer = diagnosticConsumer{c.consumer, d.replicas[i]}
	}
	if d.dir != "" {
		// Set directly after server startup; profiles must cover actual blocking,
		// including broker store locks, rather than relying on test flag defaults.
		runtime.SetBlockProfileRate(1)
		runtime.SetMutexProfileFraction(5)
	}
	return d
}

func (d *replicationDiagnostics) consume(ctx context.Context, index int, c *ReplicaConsumer) (int, error) {
	if d == nil {
		return c.ConsumeNext(ctx)
	}
	start := time.Now()
	n, err := c.ConsumeNext(ctx)
	d.replicas[index].record("cycle", time.Since(start), 0)
	return n, err
}

func (d *replicationDiagnostics) capture(t *testing.T, label string) {
	if d == nil || d.dir == "" {
		return
	}
	t.Helper()
	if err := os.MkdirAll(d.dir, 0700); err != nil {
		t.Fatal(err)
	}
	write := func(name string, profile *pprof.Profile, debug int) {
		f, err := os.Create(filepath.Join(d.dir, label+"."+name))
		if err != nil {
			t.Fatal(err)
		}
		err = profile.WriteTo(f, debug)
		closeErr := f.Close()
		if err != nil || closeErr != nil {
			t.Fatalf("profile %s: %v %v", name, err, closeErr)
		}
	}
	x := diagnosticMemory{Label: label, GoroutinesBefore: runtime.NumGoroutine()}
	runtime.ReadMemStats(&x.BeforeGC)
	x.RSSBefore, _, _ = benchmarkMemory()
	write("goroutines.txt", pprof.Lookup("goroutine"), 2)
	runtime.GC()
	runtime.ReadMemStats(&x.AfterGC)
	x.GoroutinesAfter = runtime.NumGoroutine()
	x.RSSAfter, _, _ = benchmarkMemory()
	write("heap", pprof.Lookup("heap"), 0)
	write("allocs", pprof.Lookup("allocs"), 0)
	write("mutex", pprof.Lookup("mutex"), 0)
	write("block", pprof.Lookup("block"), 0)
	d.memory = append(d.memory, x)
}

func (d *replicationDiagnostics) finish() *replicationDiagnosticResult {
	if d == nil {
		return nil
	}
	r := &replicationDiagnosticResult{Mode: d.mode, Memory: d.memory}
	for i, x := range d.replicas {
		x.workers.Wait()
		r.Replicas[i] = x.summary()
	}
	if d.dir != "" {
		runtime.SetBlockProfileRate(0)
		runtime.SetMutexProfileFraction(0)
	}
	return r
}

type diagnosticStageValues struct {
	Info, History, Fetch, ApplyAndLocal, Ack, Cycle []time.Duration
	Messages                                        int
}

type diagnosticReplica struct {
	ctx       context.Context
	ingesting *atomic.Bool
	parallel  bool
	mu        sync.Mutex
	values    [2]diagnosticStageValues
	workers   sync.WaitGroup
}

func newDiagnosticReplica(ctx context.Context, phase *atomic.Bool, parallel bool) *diagnosticReplica {
	return &diagnosticReplica{ctx: ctx, ingesting: phase, parallel: parallel}
}

func (d *diagnosticReplica) record(stage string, elapsed time.Duration, messages int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	phase := 1
	if d.ingesting.Load() {
		phase = 0
	}
	v := &d.values[phase]
	switch stage {
	case "info":
		v.Info = append(v.Info, elapsed)
	case "history":
		v.History = append(v.History, elapsed)
	case "fetch":
		v.Fetch = append(v.Fetch, elapsed)
		v.Messages += messages
	case "apply":
		v.ApplyAndLocal = append(v.ApplyAndLocal, elapsed)
	case "ack":
		v.Ack = append(v.Ack, elapsed)
	case "cycle":
		v.Cycle = append(v.Cycle, elapsed)
	}
}

func summarizeDiagnosticTiming(values []time.Duration) diagnosticTiming {
	var total time.Duration
	for _, x := range values {
		total += x
	}
	return diagnosticTiming{callTiming{len(values), float64(total) / float64(time.Millisecond)}, benchmarkLatency(values)}
}

func (d *diagnosticReplica) summary() diagnosticReplicaSummary {
	d.mu.Lock()
	defer d.mu.Unlock()
	var phases [2]diagnosticStages
	for i, v := range d.values {
		phases[i] = diagnosticStages{Info: summarizeDiagnosticTiming(v.Info), History: summarizeDiagnosticTiming(v.History), Fetch: summarizeDiagnosticTiming(v.Fetch), ApplyAndLocal: summarizeDiagnosticTiming(v.ApplyAndLocal), Ack: summarizeDiagnosticTiming(v.Ack), Cycle: summarizeDiagnosticTiming(v.Cycle), Messages: v.Messages}
	}
	return diagnosticReplicaSummary{phases[0], phases[1]}
}

type diagnosticStream struct {
	jetstream.Stream
	stats *diagnosticReplica
}

func (s diagnosticStream) Info(ctx context.Context, opts ...jetstream.StreamInfoOpt) (*jetstream.StreamInfo, error) {
	start := time.Now()
	info, err := s.Stream.Info(ctx, opts...)
	s.stats.record("info", time.Since(start), 0)
	return info, err
}

func (s diagnosticStream) GetMsg(ctx context.Context, sequence uint64, opts ...jetstream.GetMsgOpt) (*jetstream.RawStreamMsg, error) {
	start := time.Now()
	msg, err := s.Stream.GetMsg(ctx, sequence, opts...)
	s.stats.record("history", time.Since(start), 0)
	return msg, err
}

type diagnosticConsumer struct {
	jetstream.Consumer
	stats *diagnosticReplica
}

type diagnosticBatch struct {
	jetstream.MessageBatch
	stats    *diagnosticReplica
	channel  chan jetstream.Msg
	fetched  time.Time
	apply    sync.Once
	ackStart sync.Once
	msgs     []jetstream.Msg
	acks     []chan error
}

func (b *diagnosticBatch) Messages() <-chan jetstream.Msg { return b.channel }

func (c diagnosticConsumer) Fetch(count int, opts ...jetstream.FetchOpt) (jetstream.MessageBatch, error) {
	start := time.Now()
	batch, err := c.Consumer.Fetch(count, opts...)
	if err != nil {
		c.stats.record("fetch", time.Since(start), 0)
		return nil, err
	}
	b := &diagnosticBatch{MessageBatch: batch, stats: c.stats, channel: make(chan jetstream.Msg, count)}
	c.stats.workers.Add(1)
	go func() {
		defer c.stats.workers.Done()
		defer close(b.channel)
		for {
			select {
			case <-c.stats.ctx.Done():
				return
			case m, ok := <-batch.Messages():
				if !ok {
					b.fetched = time.Now()
					c.stats.record("fetch", b.fetched.Sub(start), len(b.msgs))
					return
				}
				index := len(b.msgs)
				b.msgs = append(b.msgs, m)
				b.channel <- diagnosticMsg{m, b, index}
			}
		}
	}()
	return b, nil
}

type diagnosticMsg struct {
	jetstream.Msg
	batch *diagnosticBatch
	index int
}

func (m diagnosticMsg) DoubleAck(ctx context.Context) error {
	b := m.batch
	b.apply.Do(func() { b.stats.record("apply", time.Since(b.fetched), 0) })
	if !b.stats.parallel {
		start := time.Now()
		err := m.Msg.DoubleAck(ctx)
		b.stats.record("ack", time.Since(start), 0)
		return err
	}
	// Diagnostic intervention: every real explicit DoubleAck is still confirmed,
	// and none starts until ConsumeNext calls its first ACK after database commit.
	b.ackStart.Do(func() {
		b.acks = make([]chan error, len(b.msgs))
		for i, original := range b.msgs {
			b.acks[i] = make(chan error, 1)
			b.stats.workers.Add(1)
			go func() {
				defer b.stats.workers.Done()
				start := time.Now()
				err := original.DoubleAck(ctx)
				b.stats.record("ack", time.Since(start), 0)
				b.acks[i] <- err
			}()
		}
	})
	select {
	case err := <-b.acks[m.index]:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

type diagnosticTestMsg struct {
	jetstream.Msg
	acks *atomic.Int32
}

func (m diagnosticTestMsg) DoubleAck(context.Context) error {
	m.acks.Add(1)
	return nil
}

type diagnosticTestBatch struct{ messages <-chan jetstream.Msg }

func (b diagnosticTestBatch) Messages() <-chan jetstream.Msg { return b.messages }
func (b diagnosticTestBatch) Error() error                   { return nil }

type diagnosticTestConsumer struct {
	jetstream.Consumer
	batch jetstream.MessageBatch
}

func (c diagnosticTestConsumer) Fetch(int, ...jetstream.FetchOpt) (jetstream.MessageBatch, error) {
	return c.batch, nil
}

func TestReplicaDiagnosticObservesFetchAndCommitBeforeAcks(t *testing.T) {
	for _, parallel := range []bool{false, true} {
		t.Run(map[bool]string{false: "serial", true: "parallel"}[parallel], func(t *testing.T) {
			var phase atomic.Bool
			phase.Store(true)
			stats := newDiagnosticReplica(context.Background(), &phase, parallel)
			var acks atomic.Int32
			ch := make(chan jetstream.Msg, 2)
			for range 2 {
				ch <- diagnosticTestMsg{acks: &acks}
			}
			close(ch)
			c := diagnosticConsumer{diagnosticTestConsumer{batch: diagnosticTestBatch{ch}}, stats}
			batch, err := c.Fetch(2)
			if err != nil {
				t.Fatal(err)
			}
			var messages []jetstream.Msg
			for m := range batch.Messages() {
				messages = append(messages, m)
			}
			if acks.Load() != 0 {
				t.Fatal("diagnostic acknowledged before application completed")
			}
			for _, m := range messages {
				if err := m.DoubleAck(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			stats.workers.Wait()
			s := stats.summary().Ingestion
			if acks.Load() != 2 || s.Fetch.Calls != 1 || s.ApplyAndLocal.Calls != 1 || s.Ack.Calls != 2 || s.Messages != 2 {
				t.Fatalf("missing or duplicated stage observations: %+v", s)
			}
		})
	}
}

func TestReplicaDiagnosticsConvergeWithRealStorageAndExplicitAcks(t *testing.T) {
	for _, mode := range []string{"profile", "parallel-acks", "batch32", "batch32-parallel-acks"} {
		t.Run(mode, func(t *testing.T) {
			r := runContinuousBenchmark(t, continuousSpec{Rate: 80, Operations: 32, PayloadBytes: 1024, DiagnosticMode: mode})
			if r.Diagnostics == nil || r.Committed != 32 || r.Replicated != [2]int{32, 32} {
				t.Fatalf("diagnostic did not converge: %+v", r)
			}
			for _, replica := range r.Diagnostics.Replicas {
				if replica.Ingestion.Ack.Calls+replica.Drain.Ack.Calls != 32 || replica.Ingestion.Messages+replica.Drain.Messages != 32 || replica.Ingestion.ApplyAndLocal.Calls+replica.Drain.ApplyAndLocal.Calls == 0 {
					t.Fatalf("real fetch/commit/ACK timing incomplete: %+v", replica)
				}
			}
		})
	}
}
