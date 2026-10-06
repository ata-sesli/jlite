package jlite

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func replicaConfig(node string) Config {
	c := testConfig()
	c.NodeID = node
	c.Namespaces[0].Replicas = []string{"replica", "other"}
	return c
}

func openTestReplica(t *testing.T, path, node string) *Replica {
	t.Helper()
	r, err := OpenReplica(path, "project:alpha", replicaConfig(node))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := r.Close(); err != nil {
			t.Error(err)
		}
	})
	return r
}

func openTestConsumer(t *testing.T, r *Replica, url string, windows ...time.Duration) *ReplicaConsumer {
	t.Helper()
	o := DefaultReplicaOptions()
	o.Connection.URL = url
	o.Connection.Username = r.cfg.NodeID
	o.Connection.Password = "test-" + r.cfg.NodeID
	if len(windows) > 0 {
		o.Connection.DuplicateWindow = windows[0]
	}
	c, err := ConnectReplicaConsumer(context.Background(), r, o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

func drainReplica(t *testing.T, r *Replica, c *ReplicaConsumer) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for {
		s, err := r.State()
		if err != nil {
			t.Fatal(err)
		}
		if s.Ready {
			return
		}
		if _, err := c.ConsumeNext(ctx); err != nil {
			t.Fatal(err)
		}
	}
}

func TestReplicaFullReplayAndIndependentConsumers(t *testing.T) {
	s, _ := publisherServer(t, t.TempDir(), replicaConfig("owner"))
	w := openTestWriter(t, filepath.Join(t.TempDir(), "owner.zova"), replicaConfig("owner"), DefaultWriterOptions())
	p := openTestPublisher(t, w, testPublisherOptions(s))
	for i, id := range []string{"req-1", "req-2", "req-3"} {
		r := testWrite()
		r.RequestID = id
		r.Value = []byte{byte(i)}
		if _, err := w.Write(context.Background(), r); err != nil {
			t.Fatal(err)
		}
		if _, err := p.PublishNext(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	r1 := openTestReplica(t, filepath.Join(t.TempDir(), "one.zova"), "replica")
	c1 := openTestConsumer(t, r1, s.ClientURL())
	if _, err := r1.Get(ReadRequest{Namespace: w.namespace, Key: "document"}); !errors.Is(err, ErrReplicaNotReady) {
		t.Fatalf("unsynchronized read: %v", err)
	}
	r2 := openTestReplica(t, filepath.Join(t.TempDir(), "two.zova"), "other")
	c2 := openTestConsumer(t, r2, s.ClientURL())
	drainReplica(t, r1, c1)
	drainReplica(t, r2, c2)
	s1, _ := r1.State()
	s2, _ := r2.State()
	if s1.Consumer == s2.Consumer || s1.AppliedSequence != 3 || s2.AppliedSequence != 3 {
		t.Fatalf("shared or incomplete consumers: %+v %+v", s1, s2)
	}
	for _, r := range []*Replica{r1, r2} {
		got, err := r.Get(ReadRequest{Namespace: w.namespace, Key: "document"})
		if err != nil || !got.Found || len(got.Value) != 1 || got.Value[0] != 2 || got.AppliedSequence != 3 {
			t.Fatalf("replay result: %+v %v", got, err)
		}
	}
}

func TestReplicaCheckpointPrecedesAckAndRejoinCatchesUp(t *testing.T) {
	s, _ := publisherServer(t, t.TempDir(), replicaConfig("owner"))
	w := openTestWriter(t, filepath.Join(t.TempDir(), "owner.zova"), replicaConfig("owner"), DefaultWriterOptions())
	p := openTestPublisher(t, w, testPublisherOptions(s))
	if _, err := w.Write(context.Background(), testWrite()); err != nil {
		t.Fatal(err)
	}
	if _, err := p.PublishNext(context.Background()); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "replica.zova")
	r := openTestReplica(t, path, "replica")
	c := openTestConsumer(t, r, s.ClientURL())
	batch, err := c.consumer.Fetch(1, jetstream.FetchMaxWait(c.options.FetchWait))
	if err != nil {
		t.Fatal(err)
	}
	var messages []jetstream.Msg
	for m := range batch.Messages() {
		messages = append(messages, m)
	}
	if err := batch.Error(); err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 {
		t.Fatal("missing replay message")
	}
	// Commit through the real storage path without acknowledging, simulating
	// interruption between transaction commit and transport acknowledgement.
	if _, err := r.applyMessages(messages, c.name); err != nil {
		t.Fatal(err)
	}
	info, err := c.consumer.Info(context.Background())
	if err != nil || info.AckFloor.Stream != 0 {
		t.Fatal("ack preceded durable apply")
	}
	c.Close()
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	request := testWrite()
	request.RequestID = "req-2"
	request.Value = []byte("new")
	if _, err := w.Write(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if _, err := p.PublishNext(context.Background()); err != nil {
		t.Fatal(err)
	}
	r = openTestReplica(t, path, "replica")
	c = openTestConsumer(t, r, s.ClientURL())
	drainReplica(t, r, c)
	state, _ := r.State()
	if state.AppliedSequence != 2 || state.LogSequence != 2 {
		t.Fatalf("rejoin checkpoint: %+v", state)
	}
	r.mu.Lock()
	count := queryInt(t, r.db, "SELECT count(*) FROM jlite_applied")
	r.mu.Unlock()
	if count != 2 {
		t.Fatalf("duplicate application on rejoin: %d", count)
	}
}

func TestReplicaDuplicateLogicalChangeAndGapBlock(t *testing.T) {
	for _, gap := range []bool{false, true} {
		t.Run(map[bool]string{false: "duplicate", true: "gap"}[gap], func(t *testing.T) {
			s, js := publisherServer(t, t.TempDir(), replicaConfig("owner"))
			w := openTestWriter(t, filepath.Join(t.TempDir(), "owner.zova"), replicaConfig("owner"), DefaultWriterOptions())
			o := testPublisherOptions(s)
			o.DuplicateWindow = 100 * time.Millisecond
			p := openTestPublisher(t, w, o)
			_ = p
			subject, _ := ChangeSubject(w.namespace)
			first, _ := w.cfg.NewChange(testWrite(), 1)
			wire, _ := first.Encode(w.cfg)
			if _, err := js.Publish(context.Background(), subject, wire, jetstream.WithMsgID(first.ID)); err != nil {
				t.Fatal(err)
			}
			r := openTestReplica(t, filepath.Join(t.TempDir(), "replica.zova"), "replica")
			c := openTestConsumer(t, r, s.ClientURL(), o.DuplicateWindow)
			if gap {
				request := testWrite()
				request.RequestID = "req-3"
				change, _ := w.cfg.NewChange(request, 3)
				wire, _ = change.Encode(w.cfg)
				if _, err := js.Publish(context.Background(), subject, wire, jetstream.WithMsgID(change.ID)); err != nil {
					t.Fatal(err)
				}
			} else {
				time.Sleep(200 * time.Millisecond)
				if _, err := js.Publish(context.Background(), subject, wire, jetstream.WithMsgID(first.ID), jetstream.WithExpectLastSequence(1)); err != nil {
					t.Fatal(err)
				}
			}
			_, err := c.ConsumeNext(context.Background())
			if gap {
				if !errors.Is(err, ErrSequenceGap) {
					t.Fatalf("gap accepted: %v", err)
				}
				state, _ := r.State()
				if state.Ready || state.Blocked == nil || state.AppliedSequence != 0 {
					t.Fatalf("gap advanced state: %+v", state)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				state, _ := r.State()
				if !state.Ready || state.AppliedSequence != 1 || state.LogSequence != 2 {
					t.Fatalf("duplicate state: %+v", state)
				}
			}
		})
	}
}

func TestReplicaMalformedHistoryAndMissingStream(t *testing.T) {
	s, js := publisherServer(t, t.TempDir(), replicaConfig("owner"))
	w := openTestWriter(t, filepath.Join(t.TempDir(), "owner.zova"), replicaConfig("owner"), DefaultWriterOptions())
	p := openTestPublisher(t, w, testPublisherOptions(s))
	_ = p
	r := openTestReplica(t, filepath.Join(t.TempDir(), "replica.zova"), "replica")
	c := openTestConsumer(t, r, s.ClientURL())
	other := openTestReplica(t, filepath.Join(t.TempDir(), "missing.zova"), "other")
	otherConsumer := openTestConsumer(t, other, s.ClientURL())
	otherConsumer.Close()
	subject, _ := ChangeSubject(w.namespace)
	if _, err := js.Publish(context.Background(), subject, []byte(`{"version":99}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ConsumeNext(context.Background()); err == nil {
		t.Fatal("malformed history applied")
	}
	state, _ := r.State()
	if state.Blocked == nil || state.Ready || state.AppliedSequence != 0 {
		t.Fatalf("bad message state: %+v", state)
	}
	c.Close()
	name, _ := StreamName(w.namespace)
	if err := js.DeleteStream(context.Background(), name); err != nil {
		t.Fatal(err)
	}
	o := DefaultReplicaOptions()
	o.Connection = testPublisherOptions(s)
	o.Connection.Username, o.Connection.Password = "other", "test-other"
	if connected, err := ConnectReplicaConsumer(context.Background(), other, o); !errors.Is(err, ErrStreamIdentity) {
		if connected != nil {
			connected.Close()
		}
		t.Fatalf("missing history: %v", err)
	}
}

func TestReplicaLocalCheckpointOverridesAckFloorAndFixedTarget(t *testing.T) {
	s, _ := publisherServer(t, t.TempDir(), replicaConfig("owner"))
	w := openTestWriter(t, filepath.Join(t.TempDir(), "owner.zova"), replicaConfig("owner"), DefaultWriterOptions())
	p := openTestPublisher(t, w, testPublisherOptions(s))
	if _, err := w.Write(context.Background(), testWrite()); err != nil {
		t.Fatal(err)
	}
	if _, err := p.PublishNext(context.Background()); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "replica.zova")
	r := openTestReplica(t, path, "replica")
	c := openTestConsumer(t, r, s.ClientURL())
	batch, err := c.consumer.Fetch(1, jetstream.FetchMaxWait(c.options.FetchWait))
	if err != nil {
		t.Fatal(err)
	}
	for m := range batch.Messages() {
		if err := m.DoubleAck(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if err := batch.Error(); err != nil {
		t.Fatal(err)
	}
	info, err := c.consumer.Info(context.Background())
	if err != nil || info.AckFloor.Stream != 1 {
		t.Fatalf("test requires advanced remote ack: %+v %v", info, err)
	}
	c.Close()
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	r = openTestReplica(t, path, "replica")
	o := DefaultReplicaOptions()
	o.Connection = testPublisherOptions(s)
	o.Connection.Username, o.Connection.Password = "replica", "test-replica"
	o.MaxFetchBytes = r.cfg.Limits.MaxChangeBytes + 4096 // exactly one worst-case message
	c, err = ConnectReplicaConsumer(context.Background(), r, o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	request := testWrite()
	request.RequestID, request.Operation, request.Value = "req-2", Delete, nil
	if _, err := w.Write(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if _, err := p.PublishNext(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n, err := c.ConsumeNext(context.Background()); err != nil || n != 1 {
		t.Fatalf("replay skipped acked message: %d %v", n, err)
	}
	state, _ := r.State()
	if !state.Ready || state.BootstrapTarget != 1 || state.AppliedSequence != 1 {
		t.Fatalf("moving target or unbounded fetch: %+v", state)
	}
	if n, err := c.ConsumeNext(context.Background()); err != nil || n != 1 {
		t.Fatalf("later change: %d %v", n, err)
	}
	got, err := r.Get(ReadRequest{Namespace: w.namespace, Key: "document"})
	if err != nil || got.Found || got.AppliedSequence != 2 {
		t.Fatalf("delete replay: %+v %v", got, err)
	}
}

func TestReplicaRejectsCrossNamespaceChange(t *testing.T) {
	cfg := replicaConfig("owner")
	cfg.Namespaces = append(cfg.Namespaces, Assignment{Namespace: "project:beta", Owner: "owner", Replicas: []string{"replica"}})
	s, js := publisherServer(t, t.TempDir(), cfg)
	w := openTestWriter(t, filepath.Join(t.TempDir(), "owner.zova"), cfg, DefaultWriterOptions())
	openTestPublisher(t, w, testPublisherOptions(s))
	cfg.NodeID = "replica"
	r, err := OpenReplica(filepath.Join(t.TempDir(), "replica.zova"), w.namespace, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.Close() })
	c := openTestConsumer(t, r, s.ClientURL())
	request := testWrite()
	request.Namespace = "project:beta"
	change, err := w.cfg.NewChange(request, 1)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := change.Encode(w.cfg)
	if err != nil {
		t.Fatal(err)
	}
	subject, _ := ChangeSubject(w.namespace)
	if _, err := js.Publish(context.Background(), subject, wire, jetstream.WithMsgID(change.ID)); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ConsumeNext(context.Background()); err == nil {
		t.Fatal("cross-namespace change accepted")
	}
	state, _ := r.State()
	if state.AppliedSequence != 0 || state.Blocked == nil {
		t.Fatalf("cross-namespace state: %+v", state)
	}
}

func TestReplicaStorageFailureRollsBackBatchWithoutAck(t *testing.T) {
	s, _ := publisherServer(t, t.TempDir(), replicaConfig("owner"))
	w := openTestWriter(t, filepath.Join(t.TempDir(), "owner.zova"), replicaConfig("owner"), DefaultWriterOptions())
	p := openTestPublisher(t, w, testPublisherOptions(s))
	for _, id := range []string{"req-1", "req-2"} {
		request := testWrite()
		request.RequestID = id
		if _, err := w.Write(context.Background(), request); err != nil {
			t.Fatal(err)
		}
		if _, err := p.PublishNext(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	r := openTestReplica(t, filepath.Join(t.TempDir(), "replica.zova"), "replica")
	c := openTestConsumer(t, r, s.ClientURL())
	if err := executeSQL(r.db, "CREATE TRIGGER fail_checkpoint BEFORE UPDATE ON jlite_replica BEGIN SELECT RAISE(ABORT,'storage failure'); END"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ConsumeNext(context.Background()); err == nil {
		t.Fatal("storage failure ignored")
	}
	state, _ := r.State()
	if state.AppliedSequence != 0 || state.LogSequence != 0 || state.Blocked == nil {
		t.Fatalf("failed checkpoint advanced: %+v", state)
	}
	if n := queryInt(t, r.db, "SELECT count(*) FROM jlite_records"); n != 0 {
		t.Fatalf("mutations escaped rollback: %d", n)
	}
	info, err := c.consumer.Info(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if info.AckFloor.Stream != 0 {
		t.Fatalf("ack before commit: %+v", info.AckFloor)
	}
}
