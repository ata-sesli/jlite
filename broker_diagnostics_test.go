package jlite

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// Isolate the same authenticated, file-backed, sync-always broker and bounded
// ordered publication window. No owner writes, consumer traffic, history reads
// or database checkpoints run inside the measurement. This is a component
// ceiling, not a substitute for the continuous replication benchmark.
func TestDurableBrokerDiagnostic(t *testing.T) {
	operations := 32
	if raw := os.Getenv("JLITE_BROKER_DIAGNOSTIC_OPERATIONS"); raw != "" {
		var err error
		operations, err = strconv.Atoi(raw)
		if err != nil || operations < 1 || operations > 24000 {
			t.Fatal("invalid broker diagnostic operation count")
		}
	}
	cfg := replicaConfig("owner")
	w := openTestWriter(t, filepath.Join(t.TempDir(), "owner.zova"), cfg, DefaultWriterOptions())
	s, _ := publisherServer(t, t.TempDir(), cfg)
	p := openTestPublisher(t, w, testPublisherOptions(s))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	subject, err := ChangeSubject(w.namespace)
	if err != nil {
		t.Fatal(err)
	}
	info, err := p.stream.Info(ctx)
	if err != nil || info.Config.Storage != jetstream.FileStorage || info.Config.Replicas != 1 || info.State.Msgs != 0 {
		t.Fatalf("unexpected broker policy/state: %+v %v", info, err)
	}
	type entry struct {
		id      string
		payload []byte
	}
	entries := make([]entry, operations)
	for i := range entries {
		c, err := cfg.NewChange(WriteRequest{RequestID: fmt.Sprintf("broker-%d", i), Namespace: w.namespace, Operation: Put, Key: fmt.Sprintf("broker-%d", i), Value: bytes.Repeat([]byte{0x5a}, 1024)}, uint64(i+1))
		if err != nil {
			t.Fatal(err)
		}
		payload, err := c.Encode(cfg)
		if err != nil {
			t.Fatal(err)
		}
		entries[i] = entry{c.ID, payload}
	}
	window := publicationWindow(cfg.Limits)
	futures := make([]jetstream.PubAckFuture, window)
	startedAt := make([]time.Time, window)
	var latency, waits []time.Duration
	sent := 0
	started := time.Now()
	for i := range entries {
		for sent < len(entries) && sent-i < window {
			e := entries[sent]
			startedAt[sent%window] = time.Now()
			future, err := p.js.PublishAsync(subject, e.payload, jetstream.WithMsgID(e.id), jetstream.WithExpectLastSequence(uint64(sent)), jetstream.WithRetryAttempts(0), jetstream.WithStallWait(p.options.RequestTimeout))
			if err != nil {
				t.Fatal(err)
			}
			futures[sent%window] = future
			sent++
		}
		waitStarted := time.Now()
		var ack *jetstream.PubAck
		select {
		case ack = <-futures[i%window].Ok():
		case err := <-futures[i%window].Err():
			t.Fatal(err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		waits = append(waits, time.Since(waitStarted))
		latency = append(latency, time.Since(startedAt[i%window]))
		if ack == nil || ack.Stream != info.Config.Name || ack.Sequence != uint64(i+1) || ack.Duplicate {
			t.Fatalf("unexpected ordered ACK: %+v", ack)
		}
		if (i+1)%1000 == 0 {
			t.Logf("broker progress: confirmed=%d elapsed=%.1fs", i+1, time.Since(started).Seconds())
		}
	}
	elapsed := time.Since(started)
	// Verify all retained bytes and order outside the timed interval.
	for i, e := range entries {
		msg, err := p.stream.GetMsg(ctx, uint64(i+1))
		if err != nil || msg.Subject != subject || !bytes.Equal(msg.Data, e.payload) {
			t.Fatalf("retained message %d differs: %v", i+1, err)
		}
	}
	info, err = p.stream.Info(ctx)
	if err != nil || info.State.Msgs != uint64(operations) || info.State.LastSeq != uint64(operations) {
		t.Fatalf("incomplete broker stream: %+v %v", info, err)
	}
	result := struct {
		Operations, Window    int
		GoVersion             string
		Seconds, OpsPerSecond float64
		AckWait               diagnosticTiming
		AckLatency            benchmarkPercentiles
	}{operations, window, runtime.Version(), elapsed.Seconds(), float64(operations) / elapsed.Seconds(), summarizeDiagnosticTiming(waits), benchmarkLatency(latency)}
	t.Logf("broker result: %+v", result)
	if path := os.Getenv("JLITE_BROKER_DIAGNOSTIC_OUTPUT"); path != "" {
		data, err := json.MarshalIndent(result, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, append(data, '\n'), 0600); err != nil {
			t.Fatal(err)
		}
	}
}

// Measure the underlying durability boundary with one append and one fsync per
// operation, using the same TMPDIR/data volume as the broker. This does not
// emulate broker metadata, consumer state, SQLite or their shared I/O workload.
func TestDurableStorageDiagnostic(t *testing.T) {
	if os.Getenv("JLITE_STORAGE_DIAGNOSTIC_OUTPUT") == "" {
		t.Skip("opt-in storage component measurement")
	}
	f, err := os.Create(filepath.Join(t.TempDir(), "sync.dat"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	const operations = 1000
	payload := bytes.Repeat([]byte{0x5a}, 1640)
	latency := make([]time.Duration, 0, operations)
	started := time.Now()
	for i := range operations {
		before := time.Now()
		if _, err := f.Write(payload); err != nil {
			t.Fatal(err)
		}
		if err := f.Sync(); err != nil {
			t.Fatal(err)
		}
		latency = append(latency, time.Since(before))
		if (i+1)%250 == 0 {
			t.Logf("storage progress: synced=%d elapsed=%.1fs", i+1, time.Since(started).Seconds())
		}
	}
	elapsed := time.Since(started)
	result := struct {
		Operations, PayloadBytes int
		Seconds, OpsPerSecond    float64
		WriteAndSync             diagnosticTiming
	}{operations, len(payload), elapsed.Seconds(), float64(operations) / elapsed.Seconds(), summarizeDiagnosticTiming(latency)}
	t.Logf("storage result: %+v", result)
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(os.Getenv("JLITE_STORAGE_DIAGNOSTIC_OUTPUT"), append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}
