package jlite

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	zova "github.com/ata-sesli/zova/bindings/go"
	"github.com/nats-io/nats.go/jetstream"
)

type failureProcess struct{ Mode, Path, URL string }

// The child reaches an explicit boundary, reports readiness through FD 3,
// then waits on stdin. Only its parent kills it, and Wait proves SIGKILL.
func startFailureProcess(t *testing.T, args failureProcess) func() {
	t.Helper()
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestFailureProcess$", "-test.timeout=20s")
	cmd.Env = append(os.Environ(), "JLITE_FAILURE_CHILD=1")
	cmd.ExtraFiles = []*os.File{write}
	var logs bytes.Buffer
	cmd.Stdout, cmd.Stderr = &logs, &logs
	input, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		read.Close()
		write.Close()
		input.Close()
		t.Fatal(err)
	}
	write.Close()
	waited := false
	kill := func() error {
		if waited {
			return nil
		}
		waited = true
		_ = cmd.Process.Kill()
		err := cmd.Wait()
		input.Close()
		read.Close()
		return err
	}
	t.Cleanup(func() { _ = kill() })
	if err := json.NewEncoder(input).Encode(args); err != nil {
		_ = kill()
		t.Fatal(err)
	}
	ready := make(chan error, 1)
	go func() {
		var b [1]byte
		_, err := io.ReadFull(read, b[:])
		if err == nil && b[0] != 1 {
			err = errors.New("invalid barrier")
		}
		ready <- err
	}()
	select {
	case err := <-ready:
		if err != nil {
			_ = kill()
			t.Fatalf("child failed before %s barrier: %v\n%s", args.Mode, err, logs.String())
		}
	case <-time.After(10 * time.Second):
		_ = kill()
		t.Fatalf("child timed out before %s barrier:\n%s", args.Mode, logs.String())
	}
	t.Logf("child paused at %s", args.Mode)
	return func() {
		err := kill()
		var exit *exec.ExitError
		if !errors.As(err, &exit) {
			t.Fatalf("child was not killed: %v\n%s", err, logs.String())
		}
		status, ok := exit.Sys().(syscall.WaitStatus)
		if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
			t.Fatalf("unexpected child exit: %v\n%s", err, logs.String())
		}
	}
}

func TestFailureProcess(t *testing.T) {
	if os.Getenv("JLITE_FAILURE_CHILD") != "1" {
		t.Skip("subprocess helper")
	}
	var args failureProcess
	if err := json.NewDecoder(os.Stdin).Decode(&args); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if args.Mode == "owner-commit" || args.Mode == "owner-published" {
		w := openTestWriter(t, args.Path, replicaConfig("owner"), DefaultWriterOptions())
		if result, err := w.Write(ctx, failureRequests()[0]); err != nil || result.Sequence != 1 {
			t.Fatalf("child commit: %+v %v", result, err)
		}
		if args.Mode == "owner-published" {
			o := DefaultPublisherOptions()
			o.URL = args.URL
			o.Username, o.Password = "owner", "test-owner"
			o.DuplicateWindow = 100 * time.Millisecond
			p := openTestPublisher(t, w, o)
			// Real SQLite failure after server acceptance, before checkpoint commit.
			w.mu.Lock()
			err := w.db.Exec("CREATE TRIGGER fail_checkpoint BEFORE UPDATE OF published ON jlite_state BEGIN SELECT RAISE(ABORT,'checkpoint failure'); END")
			w.mu.Unlock()
			if err != nil {
				t.Fatal(err)
			}
			if ok, err := p.PublishNext(ctx); ok || !errors.Is(err, ErrWriterFailed) {
				t.Fatalf("checkpoint barrier: %v %v", ok, err)
			}
		}
	} else {
		r := openTestReplica(t, args.Path, "replica")
		c := openTestConsumer(t, r, args.URL)
		batch, err := c.consumer.Fetch(3, jetstream.FetchMaxWait(c.options.FetchWait))
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
		if len(messages) != 3 {
			t.Fatalf("expected three deliveries, got %d", len(messages))
		}
		switch args.Mode {
		case "replica-before-commit":
			r.mu.Lock()
			if err := r.db.BeginImmediate(); err != nil {
				t.Fatal(err)
			}
			if n, err := r.mutateMessages(messages, c.name); err != nil || n != 3 {
				t.Fatalf("uncommitted batch: %d %v", n, err)
			}
			// Keep the real transaction and mutex open until abrupt termination.
		case "replica-before-ack":
			if n, err := r.applyMessages(messages, c.name); err != nil || n != 3 {
				t.Fatalf("committed batch: %d %v", n, err)
			}
		default:
			t.Fatal("unknown crash mode")
		}
	}
	barrier := os.NewFile(3, "failure-barrier")
	if _, err := barrier.Write([]byte{1}); err != nil {
		t.Fatal(err)
	}
	barrier.Close()
	// No Close, Commit, Rollback, ACK or deferred cleanup before the kill.
	if _, err := io.Copy(io.Discard, os.Stdin); err != nil {
		t.Fatal(err)
	}
	t.Fatal("parent released barrier without killing child")
}

func failureRequests() []WriteRequest {
	specs := []struct {
		op         Operation
		key, value string
	}{
		{Put, "document", "first"}, {Put, "a", "A"}, {Put, "document", "updated"},
		{Delete, "a", ""}, {Put, "empty", ""}, {Delete, "missing", ""},
		{Put, "a", "again"}, {Put, "b", "B"}, {Delete, "document", ""}, {Put, "document", "last"},
	}
	requests := make([]WriteRequest, len(specs))
	for i, s := range specs {
		requests[i] = WriteRequest{RequestID: fmt.Sprintf("failure-%d", i+1), Namespace: "project:alpha", Operation: s.op, Key: s.key}
		if s.op == Put {
			requests[i].Value = []byte(s.value)
		}
	}
	return requests
}

func publishFailureRequests(t *testing.T, w *Writer, p *Publisher, requests []WriteRequest, start uint64) {
	t.Helper()
	for i, request := range requests {
		result, err := w.Write(context.Background(), request)
		if err != nil || result.Sequence != start+uint64(i) {
			t.Fatalf("write prefix: %+v %v", result, err)
		}
		retry, err := w.Write(context.Background(), request)
		if err != nil || retry != result {
			t.Fatalf("retry altered result: %+v %v", retry, err)
		}
		if ok, err := p.PublishNext(context.Background()); err != nil || !ok {
			t.Fatalf("publication: %v %v", ok, err)
		}
	}
}

func assertFailureConvergence(t *testing.T, w *Writer, replicas []*Replica, requests []WriteRequest) {
	t.Helper()
	want := make(map[string][]byte)
	keys := make(map[string]bool)
	for _, r := range requests {
		keys[r.Key] = true
		if r.Operation == Delete {
			delete(want, r.Key)
		} else {
			want[r.Key] = r.Value
		}
	}
	state, err := w.State()
	n := uint64(len(requests))
	if err != nil || state.Sequence != n || state.PublishedSequence != n || state.OutboxCount != 0 || state.OutboxBytes != 0 {
		t.Fatalf("owner prefix: %+v %v", state, err)
	}
	reads := []func(ReadRequest) (ReadResult, error){w.Get}
	databases := []*zova.DB{w.db}
	for _, r := range replicas {
		st, err := r.State()
		if err != nil || !st.Ready || st.AppliedSequence != n || st.Blocked != nil {
			t.Fatalf("replica prefix: %+v %v", st, err)
		}
		if count := queryInt(t, r.db, "SELECT count(*) FROM jlite_applied"); count != int64(n) {
			t.Fatalf("duplicate/missing logical effects: %d want %d", count, n)
		}
		for i, request := range requests {
			change, err := w.cfg.NewChange(request, uint64(i+1))
			if err != nil {
				t.Fatal(err)
			}
			id, found, err := r.appliedID(uint64(i + 1))
			if err != nil || !found || id != change.ID {
				t.Fatalf("logical ledger differs at %d: %s %v", i+1, id, err)
			}
		}
		reads = append(reads, r.Get)
		databases = append(databases, r.db)
	}
	for i, read := range reads {
		if count := queryInt(t, databases[i], "SELECT count(*) FROM jlite_records"); count != int64(len(want)) {
			t.Fatalf("node %d extra/missing records: %d", i, count)
		}
		for key := range keys {
			result, err := read(ReadRequest{Namespace: w.namespace, Key: key})
			value, found := want[key]
			if err != nil || result.Found != found || !bytes.Equal(result.Value, value) || result.AppliedSequence != n {
				t.Fatalf("node %d key %s: %+v %v", i, key, result, err)
			}
		}
	}
}

func TestFailureOwnerCrashRecovery(t *testing.T) {
	for _, mode := range []string{"owner-commit", "owner-published"} {
		t.Run(mode, func(t *testing.T) {
			s, js := publisherServer(t, t.TempDir(), replicaConfig("owner"))
			path := filepath.Join(t.TempDir(), "owner.zova")
			w := openTestWriter(t, path, replicaConfig("owner"), DefaultWriterOptions())
			o := testPublisherOptions(s)
			o.DuplicateWindow = 100 * time.Millisecond
			p := openTestPublisher(t, w, o)
			p.Close()
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			kill := startFailureProcess(t, failureProcess{mode, path, s.ClientURL()})
			name, _ := StreamName("project:alpha")
			stream, err := js.Stream(context.Background(), name)
			if err != nil {
				t.Fatal(err)
			}
			info, err := stream.Info(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			expected := uint64(0)
			if mode == "owner-published" {
				expected = 1
			}
			if info.State.Msgs != expected {
				t.Fatalf("wrong publication barrier: %+v", info.State)
			}
			kill()
			db, err := zova.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			st, err := readWriterState(db)
			if err != nil || st.Sequence != 1 || st.PublishedSequence != 0 || st.OutboxCount != 1 {
				t.Fatalf("crash lost local commit: %+v %v", st, err)
			}
			if mode == "owner-published" {
				if err := db.Exec("DROP TRIGGER fail_checkpoint"); err != nil {
					t.Fatal(err)
				}
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			if mode == "owner-published" {
				time.Sleep(200 * time.Millisecond)
			} // expire server dedup
			w = openTestWriter(t, path, replicaConfig("owner"), DefaultWriterOptions())
			// The original caller did not receive a result; resolve with the same ID.
			requests := failureRequests()
			result, err := w.Write(context.Background(), requests[0])
			if err != nil || result.Sequence != 1 {
				t.Fatalf("lost response retry: %+v %v", result, err)
			}
			conflict := requests[0]
			conflict.Value = []byte("conflict")
			if _, err := w.Write(context.Background(), conflict); !errors.Is(err, ErrRequestConflict) {
				t.Fatalf("retry conflict: %v", err)
			}
			p = openTestPublisher(t, w, o)
			if ok, err := p.PublishNext(context.Background()); !ok || err != nil {
				t.Fatalf("recover publication: %v %v", ok, err)
			}
			publishFailureRequests(t, w, p, requests[1:], 2)
			info, err = stream.Info(context.Background())
			if err != nil || info.State.Msgs != uint64(len(requests))+expected {
				t.Fatalf("physical retry history: %+v %v", info, err)
			}
			var replicas []*Replica
			for _, node := range []string{"replica", "other"} {
				r := openTestReplica(t, filepath.Join(t.TempDir(), node+".zova"), node)
				c := openTestConsumer(t, r, s.ClientURL(), o.DuplicateWindow)
				drainReplica(t, r, c)
				replicas = append(replicas, r)
			}
			assertFailureConvergence(t, w, replicas, requests)
		})
	}
}

func TestFailureReplicaCrashRecovery(t *testing.T) {
	for _, mode := range []string{"replica-before-commit", "replica-before-ack"} {
		t.Run(mode, func(t *testing.T) {
			s, js := publisherServer(t, t.TempDir(), replicaConfig("owner"))
			w := openTestWriter(t, filepath.Join(t.TempDir(), "owner.zova"), replicaConfig("owner"), DefaultWriterOptions())
			p := openTestPublisher(t, w, testPublisherOptions(s))
			requests := failureRequests()
			publishFailureRequests(t, w, p, requests, 1)
			path := filepath.Join(t.TempDir(), "replica.zova")
			kill := startFailureProcess(t, failureProcess{mode, path, s.ClientURL()})
			name, _ := StreamName(w.namespace)
			stream, err := js.Stream(context.Background(), name)
			if err != nil {
				t.Fatal(err)
			}
			consumerName, _ := ConsumerName(w.namespace, "replica")
			consumer, err := stream.Consumer(context.Background(), consumerName)
			if err != nil {
				t.Fatal(err)
			}
			info, err := consumer.Info(context.Background())
			if err != nil || info.AckFloor.Stream != 0 || info.NumAckPending != 3 {
				t.Fatalf("ack before kill: %+v %v", info, err)
			}
			kill()
			r := openTestReplica(t, path, "replica")
			st, err := r.State()
			expected := uint64(0)
			if mode == "replica-before-ack" {
				expected = 3
			}
			if err != nil || st.AppliedSequence != expected || st.LogSequence != expected {
				t.Fatalf("crash transaction boundary: %+v %v", st, err)
			}
			if count := queryInt(t, r.db, "SELECT count(*) FROM jlite_applied"); count != int64(expected) {
				t.Fatalf("partial transaction ledger: %d", count)
			}
			if expected == 0 && queryInt(t, r.db, "SELECT count(*) FROM jlite_records") != 0 {
				t.Fatal("uncommitted mutations survived kill")
			}
			// More owner writes arrive while this replica is offline.
			extra := WriteRequest{RequestID: "failure-11", Namespace: w.namespace, Operation: Put, Key: "document", Value: []byte("after crash")}
			publishFailureRequests(t, w, p, []WriteRequest{extra}, 11)
			requests = append(requests, extra)
			c := openTestConsumer(t, r, s.ClientURL())
			drainReplica(t, r, c)
			other := openTestReplica(t, filepath.Join(t.TempDir(), "fresh.zova"), "other")
			c2 := openTestConsumer(t, other, s.ClientURL())
			drainReplica(t, other, c2)
			assertFailureConvergence(t, w, []*Replica{r, other}, requests)
		})
	}
}
