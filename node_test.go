package jlite

import (
	"context"
	"errors"
	urlpkg "net/url"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"
)

func testNodeOptions(url, node string) NodeOptions {
	o := DefaultNodeOptions()
	o.Replica.Connection.URL = url
	o.Replica.Connection.Username = node
	o.Replica.Connection.Password = "test-" + node
	o.RetryWait = 20 * time.Millisecond
	return o
}

func TestNodeCloseDrainsAcceptedWritesAndReportsFinalPosition(t *testing.T) {
	cfg := replicaConfig("owner")
	cfg.Limits.BatchWait = 200 * time.Millisecond
	o := testNodeOptions("nats://127.0.0.1:1", "owner")
	o.Replica.Connection.RequestTimeout = 100 * time.Millisecond
	o.Replica.FetchWait = 50 * time.Millisecond
	n, err := OpenNode(filepath.Join(t.TempDir(), "owner.zova"), "project:alpha", cfg, o)
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	var wg sync.WaitGroup
	results := make(chan error, 10)
	for i := range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := testWrite()
			req.RequestID = "drain-" + strconv.Itoa(i)
			_, err := n.Write(context.Background(), req)
			results <- err
		}()
	}
	deadline := time.Now().Add(time.Second)
	for {
		n.writer.mu.Lock()
		pending := n.writer.pendingCount
		n.writer.mu.Unlock()
		if pending == 10 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("writes were not admitted")
		}
		time.Sleep(time.Millisecond)
	}
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatal(err)
		}
	}
	st, err := n.Status()
	if err != nil || st.LocalSequence != 10 || st.OutboxCount != 10 || st.PendingOperations != 0 || st.Ready || st.Connected {
		t.Fatalf("final shutdown progress: %+v %v", st, err)
	}
}

func waitNode(t *testing.T, n *Node, check func(NodeStatus) bool) NodeStatus {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		s, err := n.Status()
		if err != nil {
			t.Fatal(err)
		}
		if s.Blocked != "" {
			t.Fatalf("blocked node: %+v", s)
		}
		if check(s) {
			return s
		}
		time.Sleep(10 * time.Millisecond)
	}
	s, _ := n.Status()
	t.Fatalf("node did not converge: %+v", s)
	return s
}

func TestNodeOfflineWritesReconnectAndFreshReplay(t *testing.T) {
	dir := t.TempDir()
	s, _ := publisherServer(t, dir, replicaConfig("owner"))
	url := s.ClientURL()
	parsed, _ := urlpkg.Parse(url)
	port, _ := strconv.Atoi(parsed.Port())
	path := filepath.Join(t.TempDir(), "owner.zova")
	owner, err := OpenNode(path, "project:alpha", replicaConfig("owner"), testNodeOptions(url, "owner"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { owner.Close() })
	waitNode(t, owner, func(s NodeStatus) bool { return s.Connected })
	replica, err := OpenNode(filepath.Join(t.TempDir(), "replica.zova"), "project:alpha", replicaConfig("replica"), testNodeOptions(url, "replica"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { replica.Close() })
	if _, err := owner.Write(context.Background(), testWrite()); err != nil {
		t.Fatal(err)
	}
	waitNode(t, replica, func(s NodeStatus) bool { return s.Ready && s.AppliedSequence == 1 })
	s.Shutdown()
	s.WaitForShutdown()
	waitNode(t, owner, func(s NodeStatus) bool { return !s.Connected && s.LastError != "" })
	req := testWrite()
	req.RequestID = "offline"
	req.Value = []byte("offline")
	if _, err := owner.Write(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	st, err := owner.Status()
	if err != nil || st.LocalSequence != 2 || st.PublishedSequence != 1 || st.OutboxCount != 1 || st.OutboxBytes == 0 {
		t.Fatalf("offline progress: %+v %v", st, err)
	}
	got, err := replica.Get(ReadRequest{Namespace: "project:alpha", Key: "document"})
	if err != nil || got.AppliedSequence != 1 {
		t.Fatalf("stale local read: %+v %v", got, err)
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.Write(context.Background(), req); !errors.Is(err, ErrNodeClosed) {
		t.Fatalf("admission after shutdown: %v", err)
	}
	owner, err = OpenNode(path, "project:alpha", replicaConfig("owner"), testNodeOptions(url, "owner"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { owner.Close() })
	// Restart the same real file-backed server at the same address.
	s, _ = publisherServerOnPort(t, dir, port, replicaConfig("owner"))
	waitNode(t, owner, func(s NodeStatus) bool { return s.PublishedSequence == 2 })
	waitNode(t, replica, func(s NodeStatus) bool { return s.Ready && s.AppliedSequence == 2 })
	other, err := OpenNode(filepath.Join(t.TempDir(), "other.zova"), "project:alpha", replicaConfig("other"), testNodeOptions(url, "other"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { other.Close() })
	waitNode(t, other, func(s NodeStatus) bool { return s.Ready && s.AppliedSequence == 2 })
	if _, err := other.Write(context.Background(), req); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("replica write: %v", err)
	}
}

func TestNodeBlockedHistoryRemainsVisible(t *testing.T) {
	s, js := publisherServer(t, t.TempDir(), replicaConfig("owner"))
	w := openTestWriter(t, filepath.Join(t.TempDir(), "owner.zova"), replicaConfig("owner"), DefaultWriterOptions())
	openTestPublisher(t, w, testPublisherOptions(s))
	n, err := OpenNode(filepath.Join(t.TempDir(), "replica.zova"), w.namespace, replicaConfig("replica"), testNodeOptions(s.ClientURL(), "replica"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { n.Close() })
	waitNode(t, n, func(s NodeStatus) bool { return s.Ready })
	subject, _ := ChangeSubject(w.namespace)
	if _, err := js.Publish(context.Background(), subject, []byte(`{"version":99}`)); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		st, err := n.Status()
		if err != nil {
			t.Fatal(err)
		}
		if st.Blocked != "" {
			if st.Ready || st.AppliedSequence != 0 {
				t.Fatalf("blocked progress: %+v", st)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("bad history not reported")
}

func TestNodeRetriesTransportFailureDuringIdentityRead(t *testing.T) {
	// Replica checkpoint-anchor reads join their identity context with the
	// underlying transport cause. A timeout does not prove lost history.
	if permanentNodeError(errors.Join(ErrStreamIdentity, context.DeadlineExceeded)) {
		t.Fatal("transport timeout classified as permanent lost history")
	}
	if !permanentNodeError(ErrStreamIdentity) {
		t.Fatal("actual lost history not blocked")
	}
}
