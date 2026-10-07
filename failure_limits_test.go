package jlite

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func TestFailureBoundedOverloadAndRecovery(t *testing.T) {
	for _, mode := range []string{"count", "bytes"} {
		t.Run(mode, func(t *testing.T) {
			cfg := replicaConfig("owner")
			cfg.Limits.MaxChangeBytes = 512
			cfg.Limits.BatchWait = time.Hour
			opts := DefaultWriterOptions()
			opts.MaxPendingOperations = 4
			opts.MaxPendingBytes = 1024
			opts.MaxOutboxBytes = 600
			if mode == "bytes" {
				opts.MaxPendingOperations = 16
				opts.MaxPendingBytes = 512
			}
			path := filepath.Join(t.TempDir(), "owner.zova")
			w := openTestWriter(t, path, cfg, opts)
			var pending []*queuedWrite
			var submitted []WriteRequest
			for i := range 16 {
				r := testWrite()
				r.RequestID = fmt.Sprintf("queued-%d", i)
				r.Key = fmt.Sprintf("key-%d", i)
				q, err := w.admit(context.Background(), r)
				if errors.Is(err, ErrAdmissionFull) {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				pending = append(pending, q)
				submitted = append(submitted, r)
			}
			if len(pending) == 0 || len(pending) > opts.MaxPendingOperations {
				t.Fatal("invalid admitted count")
			}
			// Sustained rejected traffic must not accumulate queue/in-flight entries.
			for i := range 1000 {
				r := testWrite()
				r.RequestID = fmt.Sprintf("rejected-%d", i)
				if _, err := w.Write(context.Background(), r); !errors.Is(err, ErrAdmissionFull) {
					t.Fatalf("overload admission: %v", err)
				}
			}
			state, err := w.State()
			if err != nil || state.PendingOperations != len(pending) || state.PendingBytes > opts.MaxPendingBytes || state.Sequence != 0 {
				t.Fatalf("queue bounds: %+v %v", state, err)
			}
			if mode == "count" && len(pending) != opts.MaxPendingOperations {
				t.Fatalf("count saturation not reached: %d", len(pending))
			}
			if mode == "bytes" && len(pending) >= opts.MaxPendingOperations {
				t.Fatal("test did not saturate byte limit first")
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			var accepted []WriteRequest
			for i, q := range pending {
				outcome := <-q.reply
				if errors.Is(outcome.err, ErrOutboxFull) {
					continue
				}
				if outcome.err != nil || outcome.result.Sequence != uint64(len(accepted)+1) {
					t.Fatalf("batch saturation result: %+v", outcome)
				}
				accepted = append(accepted, submitted[i])
			}
			cfg.Limits.BatchWait = time.Millisecond
			w = openTestWriter(t, path, cfg, opts)
			for i := range 100 {
				r := testWrite()
				r.RequestID = fmt.Sprintf("full-%d", i)
				r.Key = "rejected-key"
				if _, err := w.Write(context.Background(), r); !errors.Is(err, ErrOutboxFull) {
					t.Fatalf("outbox overload: %v", err)
				}
			}
			state, err = w.State()
			if err != nil || state.Sequence != uint64(len(accepted)) || state.OutboxBytes > opts.MaxOutboxBytes || state.OutboxCount != int64(len(accepted)) || state.PendingOperations != 0 || state.PendingBytes != 0 {
				t.Fatalf("durable backlog bounds: %+v %v", state, err)
			}
			if queryInt(t, w.db, "SELECT sum(length(payload)) FROM jlite_outbox") != state.OutboxBytes {
				t.Fatal("outbox byte counter diverged")
			}
			if result, err := w.Get(ReadRequest{Namespace: w.namespace, Key: "rejected-key"}); err != nil || result.Found {
				t.Fatalf("rejected mutation applied: %+v %v", result, err)
			}
			s, js := publisherServer(t, t.TempDir(), cfg)
			o := testPublisherOptions(s)
			o.MaxStreamBytes = 100
			p := openTestPublisher(t, w, o)
			for range 3 {
				if ok, err := p.PublishNext(context.Background()); ok || err == nil {
					t.Fatalf("full stream dropped/accepted work: %v %v", ok, err)
				}
			}
			name, _ := StreamName(w.namespace)
			stream, err := js.Stream(context.Background(), name)
			if err != nil {
				t.Fatal(err)
			}
			info, err := stream.Info(context.Background())
			if err != nil || info.State.Msgs != 0 || info.State.Bytes > uint64(o.MaxStreamBytes) {
				t.Fatalf("stream capacity: %+v %v", info, err)
			}
			after, err := w.State()
			if err != nil || after.PublishedSequence != 0 || after.OutboxBytes != state.OutboxBytes || after.OutboxCount != state.OutboxCount {
				t.Fatalf("rejection lost acknowledged local work: %+v %v", after, err)
			}
			// Explicit operator capacity increase; retain the same history/identity and
			// stop publication before maintenance. There is no automatic eviction.
			p.Close()
			o.MaxStreamBytes = 1 << 20
			info.Config.MaxBytes = o.MaxStreamBytes
			if _, err := js.UpdateStream(context.Background(), info.Config); err != nil {
				t.Fatal(err)
			}
			p = openTestPublisher(t, w, o)
			for range len(accepted) {
				if ok, err := p.PublishNext(context.Background()); !ok || err != nil {
					t.Fatalf("capacity recovery: %v %v", ok, err)
				}
			}
			var replicas []*Replica
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
				c, err := ConnectReplicaConsumer(context.Background(), r, ro)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(c.Close)
				drainReplica(t, r, c)
				replicas = append(replicas, r)
			}
			assertFailureConvergence(t, w, replicas, accepted)
		})
	}
}
