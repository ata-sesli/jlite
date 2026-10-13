package jlite

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	zova "github.com/ata-sesli/zova/bindings/go"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func writePublicationBatch(t *testing.T, w *Writer, count, valueBytes int) []OutboxEntry {
	t.Helper()
	var entries []OutboxEntry
	for i := range count {
		r := WriteRequest{RequestID: fmt.Sprintf("batch-%d", i), Namespace: w.namespace, Operation: Put, Key: fmt.Sprintf("batch-%d", i), Value: bytes.Repeat([]byte("b"), valueBytes)}
		out, err := w.Write(context.Background(), r)
		if err != nil {
			t.Fatal(err)
		}
		change, err := w.cfg.NewChange(r, out.Sequence)
		if err != nil {
			t.Fatal(err)
		}
		wire, err := change.Encode(w.cfg)
		if err != nil {
			t.Fatal(err)
		}
		entries = append(entries, OutboxEntry{Sequence: out.Sequence, ID: change.ID, Payload: wire})
	}
	return entries
}

type readyPublishAck struct {
	ack      <-chan *jetstream.PubAck
	original jetstream.PubAckFuture
}

type delayedHistoryStream struct{ jetstream.Stream }

func (s delayedHistoryStream) GetMsg(ctx context.Context, seq uint64, opts ...jetstream.GetMsgOpt) (*jetstream.RawStreamMsg, error) {
	select {
	case <-time.After(30 * time.Millisecond):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return s.Stream.GetMsg(ctx, seq, opts...)
}

func TestPublisherBatchSuffixRecoveryUsesPerRequestTimeout(t *testing.T) {
	for _, single := range []bool{true, false} {
		t.Run(fmt.Sprint(single), func(t *testing.T) {
			cfg := replicaConfig("owner")
			s, js := publisherServer(t, t.TempDir(), cfg)
			w := openTestWriter(t, filepath.Join(t.TempDir(), "owner.zova"), cfg, DefaultWriterOptions())
			entries := writePublicationBatch(t, w, 5, 64)
			p := openTestPublisher(t, w, testPublisherOptions(s))
			subject, _ := ChangeSubject(w.namespace)
			for _, entry := range entries {
				if _, err := js.Publish(context.Background(), subject, entry.Payload, jetstream.WithMsgID(entry.ID)); err != nil {
					t.Fatal(err)
				}
			}
			p.options.RequestTimeout = 100 * time.Millisecond
			p.stream = delayedHistoryStream{p.stream}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			want := 5
			if single {
				want = 1
				if ok, err := p.PublishNext(ctx); !ok || err != nil {
					t.Fatalf("single-entry recovery of batch suffix: %v %v", ok, err)
				}
			} else {
				if n, err := p.PublishBatch(ctx); n != 5 || err != nil {
					t.Fatalf("batched recovery: %d %v", n, err)
				}
			}
			st, _ := w.State()
			if st.PublishedSequence != uint64(want) {
				t.Fatalf("recovery prefix: %+v", st)
			}
		})
	}
}

func (f readyPublishAck) Ok() <-chan *jetstream.PubAck { return f.ack }
func (f readyPublishAck) Err() <-chan error            { return f.original.Err() }
func (f readyPublishAck) Msg() *nats.Msg               { return f.original.Msg() }

// Inject a send failure only after the first two real broker ACKs, making the
// accepted partial prefix deterministic without replacing persistence with a mock.
type interruptedBatchPublisher struct {
	jetstream.JetStream
	sends         int
	AfterAccepted func()
	FailAt        int
}

type unconfirmedRecoveryPublisher struct{ jetstream.JetStream }

func (s unconfirmedRecoveryPublisher) Publish(context.Context, string, []byte, ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	return nil, nats.ErrTimeout
}

func TestPublisherBatchRecoveryRequiresDurableTailAck(t *testing.T) {
	cfg := replicaConfig("owner")
	s, js := publisherServer(t, t.TempDir(), cfg)
	w := openTestWriter(t, filepath.Join(t.TempDir(), "owner.zova"), cfg, DefaultWriterOptions())
	entries := writePublicationBatch(t, w, 3, 64)
	p := openTestPublisher(t, w, testPublisherOptions(s))
	subject, _ := ChangeSubject(w.namespace)
	for _, entry := range entries[:2] {
		if _, err := js.Publish(context.Background(), subject, entry.Payload, jetstream.WithMsgID(entry.ID)); err != nil {
			t.Fatal(err)
		}
	}
	p.js = unconfirmedRecoveryPublisher{p.js}
	if n, err := p.PublishBatch(context.Background()); n != 0 || !errors.Is(err, nats.ErrTimeout) {
		t.Fatalf("unconfirmed recovery: %d %v", n, err)
	}
	st, _ := w.State()
	if st.PublishedSequence != 0 || st.OutboxCount != 3 {
		t.Fatalf("read-back alone reclaimed outbox: %+v", st)
	}
}

func (s *interruptedBatchPublisher) PublishAsync(subject string, data []byte, opts ...jetstream.PublishOpt) (jetstream.PubAckFuture, error) {
	s.sends++
	if s.sends == max(3, s.FailAt) {
		return nil, nats.ErrConnectionClosed
	}
	f, err := s.JetStream.PublishAsync(subject, data, opts...)
	if err != nil {
		return nil, err
	}
	select {
	case ack := <-f.Ok():
		if s.sends == 2 && s.AfterAccepted != nil {
			s.AfterAccepted()
		}
		ch := make(chan *jetstream.PubAck, 1)
		ch <- ack
		return readyPublishAck{ch, f}, nil
	case err := <-f.Err():
		return nil, err
	case <-time.After(2 * time.Second):
		return nil, nats.ErrTimeout
	}
}

func TestPublisherBatchPartialSendRetainsOutboxAndReconciles(t *testing.T) {
	cfg := replicaConfig("owner")
	s, js := publisherServer(t, t.TempDir(), cfg)
	path := filepath.Join(t.TempDir(), "owner.zova")
	w := openTestWriter(t, path, cfg, DefaultWriterOptions())
	writePublicationBatch(t, w, 5, 64)
	o := testPublisherOptions(s)
	o.DuplicateWindow = 100 * time.Millisecond
	p := openTestPublisher(t, w, o)
	p.js = &interruptedBatchPublisher{JetStream: p.js}
	if n, err := p.PublishBatch(context.Background()); n != 0 || !errors.Is(err, nats.ErrConnectionClosed) {
		t.Fatalf("partial send: %d %v", n, err)
	}
	st, _ := w.State()
	if st.PublishedSequence != 0 || st.OutboxCount != 5 || p.nc.IsConnected() {
		t.Fatalf("ambiguous send discarded state: %+v", st)
	}
	name, _ := StreamName(w.namespace)
	stream, _ := js.Stream(context.Background(), name)
	info, _ := stream.Info(context.Background())
	if info.State.Msgs != 2 {
		t.Fatalf("wrong accepted partial prefix: %+v", info.State)
	}
	p.Close()
	w.Close()
	time.Sleep(200 * time.Millisecond)
	w = openTestWriter(t, path, cfg, DefaultWriterOptions())
	p = openTestPublisher(t, w, o)
	if n, err := p.PublishBatch(context.Background()); n != 2 || err != nil {
		t.Fatalf("reconcile partial prefix: %d %v", n, err)
	}
	if n, err := p.PublishBatch(context.Background()); n != 3 || err != nil {
		t.Fatalf("remaining suffix: %d %v", n, err)
	}
	info, _ = stream.Info(context.Background())
	st, _ = w.State()
	if info.State.Msgs != 6 || st.PublishedSequence != 5 || st.OutboxCount != 0 {
		t.Fatalf("partial retry changed prefix: %+v %+v", info.State, st)
	}
}

func TestPublisherBatchCancellationRetainsWholePrefix(t *testing.T) {
	for _, afterSend := range []bool{false, true} {
		t.Run(fmt.Sprint(afterSend), func(t *testing.T) {
			cfg := replicaConfig("owner")
			s, _ := publisherServer(t, t.TempDir(), cfg)
			w := openTestWriter(t, filepath.Join(t.TempDir(), "owner.zova"), cfg, DefaultWriterOptions())
			writePublicationBatch(t, w, 3, 64)
			p := openTestPublisher(t, w, testPublisherOptions(s))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if afterSend {
				p.js = &interruptedBatchPublisher{JetStream: p.js, AfterAccepted: cancel}
			} else {
				cancel()
			}
			if n, err := p.PublishBatch(ctx); n != 0 || !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled batch: %d %v", n, err)
			}
			st, _ := w.State()
			if st.PublishedSequence != 0 || st.OutboxCount != 3 {
				t.Fatalf("cancellation advanced prefix: %+v", st)
			}
			if afterSend && p.nc.IsConnected() {
				t.Fatal("uncertain cancellation left async transport active")
			}
		})
	}
}

func TestFailurePublicationBatchCrashRecovery(t *testing.T) {
	for _, mode := range []string{"owner-batch-published", "owner-batch-checkpointed"} {
		t.Run(mode, func(t *testing.T) {
			cfg := replicaConfig("owner")
			s, js := publisherServer(t, t.TempDir(), cfg)
			path := filepath.Join(t.TempDir(), "owner.zova")
			w := openTestWriter(t, path, cfg, DefaultWriterOptions())
			o := testPublisherOptions(s)
			o.DuplicateWindow = 100 * time.Millisecond
			p := openTestPublisher(t, w, o)
			p.Close()
			w.Close()
			kill := startFailureProcess(t, failureProcess{mode, path, s.ClientURL()})
			name, _ := StreamName("project:alpha")
			stream, _ := js.Stream(context.Background(), name)
			info, err := stream.Info(context.Background())
			if err != nil || info.State.Msgs != 3 {
				t.Fatalf("wrong batch barrier: %+v %v", info, err)
			}
			kill()
			db, err := zova.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			st, err := readWriterState(db)
			published, pending := uint64(3), int64(0)
			if mode == "owner-batch-published" {
				published, pending = 0, 3
			}
			if err != nil || st.Sequence != 3 || st.PublishedSequence != published || st.OutboxCount != pending {
				t.Fatalf("batch atomicity after SIGKILL: %+v %v", st, err)
			}
			if pending > 0 {
				if err := db.Exec("DROP TRIGGER fail_checkpoint"); err != nil {
					t.Fatal(err)
				}
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			time.Sleep(200 * time.Millisecond)
			w = openTestWriter(t, path, cfg, DefaultWriterOptions())
			p = openTestPublisher(t, w, o)
			requests := failureRequests()[:6]
			for i, req := range requests[:3] {
				if out, err := w.Write(context.Background(), req); err != nil || out.Sequence != uint64(i+1) {
					t.Fatalf("retry changed sequence: %+v %v", out, err)
				}
			}
			if pending > 0 {
				if n, err := p.PublishBatch(context.Background()); n != 3 || err != nil {
					t.Fatalf("recover verified suffix: %d %v", n, err)
				}
			}
			for _, req := range requests[3:] {
				if _, err := w.Write(context.Background(), req); err != nil {
					t.Fatal(err)
				}
			}
			if n, err := p.PublishBatch(context.Background()); n != 3 || err != nil {
				t.Fatalf("new batch after crash: %d %v", n, err)
			}
			info, err = stream.Info(context.Background())
			wantPhysical := uint64(6)
			if pending > 0 {
				wantPhysical++
			}
			if err != nil || info.State.Msgs != wantPhysical {
				t.Fatalf("crash replay duplicated entries: %+v %v", info, err)
			}
			var replicas []*Replica
			for _, node := range []string{"replica", "other"} {
				r := openTestReplica(t, filepath.Join(t.TempDir(), node+".zova"), node)
				ro := DefaultReplicaOptions()
				ro.Connection = o
				ro.Connection.Username, ro.Connection.Password = node, "test-"+node
				c, err := ConnectReplicaConsumer(context.Background(), r, ro)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(c.Close)
				drainReplica(t, r, c)
				replicas = append(replicas, r)
			}
			assertFailureConvergence(t, w, replicas, requests)
		})
	}
}

func TestPublisherBatchBoundsAndOneCheckpoint(t *testing.T) {
	for _, byteBound := range []bool{false, true} {
		t.Run(fmt.Sprint(byteBound), func(t *testing.T) {
			cfg := replicaConfig("owner")
			cfg.Limits.MaxBatchOperations = 3
			size, want := 64, 3
			if byteBound {
				cfg.Limits.MaxBatchBytes = cfg.Limits.MaxChangeBytes
				size, want = 32<<10, 2
			}
			s, js := publisherServer(t, t.TempDir(), cfg)
			w := openTestWriter(t, filepath.Join(t.TempDir(), "owner.zova"), cfg, DefaultWriterOptions())
			entries := writePublicationBatch(t, w, 5, size)
			p := openTestPublisher(t, w, testPublisherOptions(s))
			w.mu.Lock()
			err := w.db.Exec(`CREATE TABLE pub_commits(n INTEGER); CREATE TRIGGER pub_commit AFTER UPDATE OF published ON jlite_state WHEN NEW.published>OLD.published BEGIN INSERT INTO pub_commits VALUES(NEW.published-OLD.published); END;`)
			w.mu.Unlock()
			if err != nil {
				t.Fatal(err)
			}
			if n, err := p.PublishBatch(context.Background()); err != nil || n != want {
				t.Fatalf("batch: %d %v want %d", n, err, want)
			}
			st, err := w.State()
			if err != nil || st.PublishedSequence != uint64(want) || st.OutboxCount != int64(5-want) || queryInt(t, w.db, "SELECT count(*) FROM pub_commits") != 1 || queryInt(t, w.db, "SELECT n FROM pub_commits") != int64(want) {
				t.Fatalf("batch checkpoint: %+v %v", st, err)
			}
			name, _ := StreamName(w.namespace)
			stream, _ := js.Stream(context.Background(), name)
			for i := range want {
				msg, err := stream.GetMsg(context.Background(), uint64(i+1))
				if err != nil || !bytes.Equal(msg.Data, entries[i].Payload) {
					t.Fatalf("ordered entry %d: %v", i, err)
				}
			}
		})
	}
}

func TestPublisherBatchBoundsInflightWindow(t *testing.T) {
	cfg := replicaConfig("owner")
	s, _ := publisherServer(t, t.TempDir(), cfg)
	w := openTestWriter(t, filepath.Join(t.TempDir(), "owner.zova"), cfg, DefaultWriterOptions())
	writePublicationBatch(t, w, 32, 1024)
	p := openTestPublisher(t, w, testPublisherOptions(s))
	timed := &timedJetStream{JetStream: p.js}
	p.js = timed
	if n, err := p.PublishBatch(context.Background()); n != 32 || err != nil {
		t.Fatalf("batch: %d %v", n, err)
	}
	window := cfg.Limits.MaxBatchBytes / cfg.Limits.MaxChangeBytes
	if timed.PendingPeak > window {
		t.Fatalf("inflight peak %d exceeds conservative window %d", timed.PendingPeak, window)
	}
}

type failedWindowStream struct {
	jetstream.Stream
	failure string
	cancel  context.CancelFunc
}

func (s failedWindowStream) GetMsg(ctx context.Context, seq uint64, opts ...jetstream.GetMsgOpt) (*jetstream.RawStreamMsg, error) {
	if seq == 9 {
		switch s.failure {
		case "timeout":
			return nil, nats.ErrTimeout
		case "cancel":
			s.cancel()
			return nil, context.Canceled
		case "corrupt":
			m, err := s.Stream.GetMsg(ctx, seq, opts...)
			if err == nil {
				m.Data = []byte(`{"bad":true}`)
			}
			return m, err
		}
	}
	return s.Stream.GetMsg(ctx, seq, opts...)
}

func TestPublisherBatchFailureInLaterWindowRetainsWholeGroup(t *testing.T) {
	for _, failure := range []string{"timeout", "cancel", "corrupt", "send"} {
		t.Run(failure, func(t *testing.T) {
			cfg := replicaConfig("owner")
			s, js := publisherServer(t, t.TempDir(), cfg)
			path := filepath.Join(t.TempDir(), "owner.zova")
			w := openTestWriter(t, path, cfg, DefaultWriterOptions())
			writePublicationBatch(t, w, 32, 1024)
			o := testPublisherOptions(s)
			o.DuplicateWindow = 100 * time.Millisecond
			p := openTestPublisher(t, w, o)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			p.stream = failedWindowStream{p.stream, failure, cancel}
			if failure == "send" {
				p.js = &interruptedBatchPublisher{JetStream: p.js, FailAt: 10}
			}
			want := nats.ErrTimeout
			if failure == "cancel" {
				want = context.Canceled
			} else if failure == "corrupt" {
				want = ErrStreamIdentity
			} else if failure == "send" {
				want = nats.ErrConnectionClosed
			}
			if n, err := p.PublishBatch(ctx); n != 0 || !errors.Is(err, want) {
				t.Fatalf("failure in second window: %d %v", n, err)
			}
			st, _ := w.State()
			if st.PublishedSequence != 0 || st.OutboxCount != 32 || p.nc.IsConnected() {
				t.Fatalf("failed batch reclaimed prefix: %+v", st)
			}
			p.Close()
			w.Close()
			time.Sleep(200 * time.Millisecond)
			name, _ := StreamName("project:alpha")
			stream, _ := js.Stream(context.Background(), name)
			info, err := stream.Info(context.Background())
			// The first window is verified; the second is sent and acknowledged
			// before its read-back fails.
			physical := uint64(16)
			if failure == "send" {
				physical = 9
			}
			if err != nil || info.State.Msgs != physical {
				t.Fatalf("accepted physical prefix: %+v %v", info, err)
			}
			accepted := int(info.State.Msgs)
			w = openTestWriter(t, path, cfg, DefaultWriterOptions())
			p = openTestPublisher(t, w, o)
			if n, err := p.PublishBatch(context.Background()); n != accepted || err != nil {
				t.Fatalf("recover accepted prefix: %d %v", n, err)
			}
			if n, err := p.PublishBatch(context.Background()); n != 32-accepted || err != nil {
				t.Fatalf("finish remaining prefix: %d %v", n, err)
			}
			for _, node := range []string{"replica", "other"} {
				r := openTestReplica(t, filepath.Join(t.TempDir(), node+".zova"), node)
				ro := DefaultReplicaOptions()
				ro.Connection = o
				ro.Connection.Username, ro.Connection.Password = node, "test-"+node
				c, err := ConnectReplicaConsumer(context.Background(), r, ro)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(c.Close)
				drainReplica(t, r, c)
				if st, err := r.State(); err != nil || st.AppliedSequence != 32 || queryInt(t, r.db, "SELECT count(*) FROM jlite_applied") != 32 {
					t.Fatalf("recovery did not converge: %+v %v", st, err)
				}
			}
		})
	}
}

func TestPublisherBatchRecoversAcceptedSuffixAfterWindowExpiry(t *testing.T) {
	cfg := replicaConfig("owner")
	s, js := publisherServer(t, t.TempDir(), cfg)
	path := filepath.Join(t.TempDir(), "owner.zova")
	w := openTestWriter(t, path, cfg, DefaultWriterOptions())
	entries := writePublicationBatch(t, w, 5, 64)
	o := testPublisherOptions(s)
	o.DuplicateWindow = 100 * time.Millisecond
	p := openTestPublisher(t, w, o)
	subject, _ := ChangeSubject(w.namespace)
	for _, entry := range entries[:3] {
		if _, err := js.Publish(context.Background(), subject, entry.Payload, jetstream.WithMsgID(entry.ID)); err != nil {
			t.Fatal(err)
		}
	}
	p.Close()
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	w = openTestWriter(t, path, cfg, DefaultWriterOptions())
	p = openTestPublisher(t, w, o)
	if n, err := p.PublishBatch(context.Background()); n != 3 || err != nil {
		t.Fatalf("recover accepted prefix: %d %v", n, err)
	}
	if n, err := p.PublishBatch(context.Background()); n != 2 || err != nil {
		t.Fatalf("publish remaining prefix: %d %v", n, err)
	}
	name, _ := StreamName(w.namespace)
	stream, _ := js.Stream(context.Background(), name)
	info, err := stream.Info(context.Background())
	if err != nil || info.State.Msgs != 6 {
		t.Fatalf("recovery duplicated history: %+v %v", info, err)
	}
	for _, node := range []string{"replica", "other"} {
		r := openTestReplica(t, filepath.Join(t.TempDir(), node+".zova"), node)
		ro := DefaultReplicaOptions()
		ro.Connection = o
		ro.Connection.Username, ro.Connection.Password = node, "test-"+node
		c, err := ConnectReplicaConsumer(context.Background(), r, ro)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(c.Close)
		drainReplica(t, r, c)
		if st, err := r.State(); err != nil || st.AppliedSequence != 5 {
			t.Fatalf("replica prefix: %+v %v", st, err)
		}
	}
}

func TestNodePublishesBacklogWithGroupedCheckpoints(t *testing.T) {
	cfg := replicaConfig("owner")
	cfg.Limits.MaxBatchOperations = 3
	s, _ := publisherServer(t, t.TempDir(), cfg)
	path := filepath.Join(t.TempDir(), "owner.zova")
	w := openTestWriter(t, path, cfg, DefaultWriterOptions())
	writePublicationBatch(t, w, 6, 64)
	if err := w.db.Exec(`CREATE TABLE pub_commits(n INTEGER); CREATE TRIGGER pub_commit AFTER UPDATE OF published ON jlite_state WHEN NEW.published>OLD.published BEGIN INSERT INTO pub_commits VALUES(NEW.published-OLD.published); END;`); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	n, err := OpenNode(path, "project:alpha", cfg, testNodeOptions(s.ClientURL(), "owner"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { n.Close() })
	waitNode(t, n, func(s NodeStatus) bool { return s.PublishedSequence == 6 })
	n.writer.mu.Lock()
	commits := queryInt(t, n.writer.db, "SELECT count(*) FROM pub_commits")
	n.writer.mu.Unlock()
	if commits != 2 {
		t.Fatalf("six writes should checkpoint in two groups, got %d commits", commits)
	}
}

func TestPublisherBatchRejectsCorruptMiddleOfAcceptedSuffix(t *testing.T) {
	cfg := replicaConfig("owner")
	s, js := publisherServer(t, t.TempDir(), cfg)
	w := openTestWriter(t, filepath.Join(t.TempDir(), "owner.zova"), cfg, DefaultWriterOptions())
	entries := writePublicationBatch(t, w, 3, 64)
	p := openTestPublisher(t, w, testPublisherOptions(s))
	subject, _ := ChangeSubject(w.namespace)
	for i, entry := range entries {
		data := entry.Payload
		if i == 1 {
			data = []byte(`{"bad":true}`)
		}
		if _, err := js.Publish(context.Background(), subject, data, jetstream.WithMsgID(entry.ID)); err != nil {
			t.Fatal(err)
		}
	}
	if n, err := p.PublishBatch(context.Background()); n != 0 || !errors.Is(err, ErrStreamIdentity) {
		t.Fatalf("corrupt suffix: %d %v", n, err)
	}
	st, _ := w.State()
	if st.PublishedSequence != 0 || st.OutboxCount != 3 {
		t.Fatalf("corruption advanced checkpoint: %+v", st)
	}
}
