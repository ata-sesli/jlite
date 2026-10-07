package jlite

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func waitFailureBlocked(t *testing.T, n *Node, cause string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		s, err := n.Status()
		if err != nil {
			t.Fatal(err)
		}
		if s.Blocked != "" {
			if s.Ready || !strings.Contains(s.Blocked, cause) {
				t.Fatalf("wrong blocking cause: %+v", s)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	s, _ := n.Status()
	t.Fatalf("failure not exposed: %+v", s)
}

func TestFailureNodesRejectBadOrMissingHistory(t *testing.T) {
	for _, mode := range []string{"malformed", "version", "origin", "missing"} {
		t.Run(mode, func(t *testing.T) {
			s, js := publisherServer(t, t.TempDir(), replicaConfig("owner"))
			w := openTestWriter(t, filepath.Join(t.TempDir(), "owner.zova"), replicaConfig("owner"), DefaultWriterOptions())
			p := openTestPublisher(t, w, testPublisherOptions(s))
			requests := failureRequests()
			publishFailureRequests(t, w, p, requests[:1], 1)
			var replicas []*Node
			for _, node := range []string{"replica", "other"} {
				n, err := OpenNode(filepath.Join(t.TempDir(), node+".zova"), w.namespace, replicaConfig(node), testNodeOptions(s.ClientURL(), node))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { n.Close() })
				waitNode(t, n, func(st NodeStatus) bool { return st.Ready && st.AppliedSequence == 1 })
				replicas = append(replicas, n)
			}
			cause := ErrInvalidChange.Error()
			if mode == "missing" {
				name, _ := StreamName(w.namespace)
				if err := js.DeleteStream(context.Background(), name); err != nil {
					t.Fatal(err)
				}
				cause = ErrStreamIdentity.Error()
			} else {
				change, err := w.cfg.NewChange(requests[1], 2)
				if err != nil {
					t.Fatal(err)
				}
				switch mode {
				case "version":
					change.Version++
					cause = ErrUnsupportedVersion.Error()
				case "origin":
					change.Owner = "other"
				}
				wire, err := json.Marshal(change)
				if err != nil {
					t.Fatal(err)
				}
				if mode == "malformed" {
					wire = []byte(`{"version":1,`)
				}
				subject, _ := ChangeSubject(w.namespace)
				// Admin deliberately injects corrupt history. Ordinary replica credentials
				// cannot publish this subject (covered by the real ACL integration test).
				if _, err := js.Publish(context.Background(), subject, wire, jetstream.WithMsgID(change.ID)); err != nil {
					t.Fatal(err)
				}
			}
			for _, n := range replicas {
				waitFailureBlocked(t, n, cause)
				st, err := n.Status()
				if err != nil || st.AppliedSequence != 1 {
					t.Fatalf("bad history advanced prefix: %+v %v", st, err)
				}
				if _, err := n.Get(ReadRequest{Namespace: w.namespace, Key: "document"}); !errors.Is(err, ErrNodeBlocked) {
					t.Fatalf("blocked namespace served reads: %v", err)
				}
				if mode != "missing" {
					name, _ := StreamName(w.namespace)
					stream, err := js.Stream(context.Background(), name)
					if err != nil {
						t.Fatal(err)
					}
					consumerName, _ := ConsumerName(w.namespace, st.NodeID)
					consumer, err := stream.Consumer(context.Background(), consumerName)
					if err != nil {
						t.Fatal(err)
					}
					info, err := consumer.Info(context.Background())
					if err != nil || info.AckFloor.Stream > 1 {
						t.Fatalf("bad message acknowledged: %+v %v", info, err)
					}
				}
			}
			if mode == "missing" {
				if _, err := w.Write(context.Background(), requests[1]); err != nil {
					t.Fatal(err)
				}
				if ok, err := p.PublishNext(context.Background()); ok || !errors.Is(err, ErrStreamIdentity) {
					t.Fatalf("lost history silently rebuilt: %v %v", ok, err)
				}
				st, err := w.State()
				if err != nil || st.Sequence != 2 || st.PublishedSequence != 1 || st.OutboxCount != 1 {
					t.Fatalf("lost history discarded local commit: %+v %v", st, err)
				}
			}
		})
	}
}

func TestFailureReplacementOwnerCannotAdoptHistory(t *testing.T) {
	s, _ := publisherServer(t, t.TempDir(), replicaConfig("owner"))
	path := filepath.Join(t.TempDir(), "owner.zova")
	owner, err := OpenNode(path, "project:alpha", replicaConfig("owner"), testNodeOptions(s.ClientURL(), "owner"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { owner.Close() })
	requests := failureRequests()
	if _, err := owner.Write(context.Background(), requests[0]); err != nil {
		t.Fatal(err)
	}
	waitNode(t, owner, func(s NodeStatus) bool { return s.PublishedSequence == 1 })
	var replicas []*Node
	for _, node := range []string{"replica", "other"} {
		n, err := OpenNode(filepath.Join(t.TempDir(), node+".zova"), "project:alpha", replicaConfig(node), testNodeOptions(s.ClientURL(), node))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { n.Close() })
		waitNode(t, n, func(s NodeStatus) bool { return s.Ready && s.AppliedSequence == 1 })
		replicas = append(replicas, n)
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	replacement, err := OpenNode(filepath.Join(t.TempDir(), "replacement.zova"), "project:alpha", replicaConfig("owner"), testNodeOptions(s.ClientURL(), "owner"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { replacement.Close() })
	waitFailureBlocked(t, replacement, ErrStreamIdentity.Error())
	if _, err := replacement.Write(context.Background(), requests[1]); !errors.Is(err, ErrNodeBlocked) {
		t.Fatalf("replacement owner forked namespace: %v", err)
	}
	if err := replacement.Close(); err != nil {
		t.Fatal(err)
	}
	owner, err = OpenNode(path, "project:alpha", replicaConfig("owner"), testNodeOptions(s.ClientURL(), "owner"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { owner.Close() })
	if result, err := owner.Write(context.Background(), requests[1]); err != nil || result.Sequence != 2 {
		t.Fatalf("original owner recovery: %+v %v", result, err)
	}
	waitNode(t, owner, func(s NodeStatus) bool { return s.PublishedSequence == 2 })
	for _, n := range replicas {
		waitNode(t, n, func(s NodeStatus) bool { return s.Ready && s.AppliedSequence == 2 })
		if result, err := n.Get(ReadRequest{Namespace: "project:alpha", Key: "a"}); err != nil || !result.Found || string(result.Value) != "A" {
			t.Fatalf("history changed after replacement refusal: %+v %v", result, err)
		}
	}
}
