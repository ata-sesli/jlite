package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"jlite"
)

func TestLocalSocketWriteStatusReadAndShutdown(t *testing.T) {
	opts, err := brokerOptions(t.TempDir(), map[string]string{"owner": "test-owner", "replica": "test-replica", "other": "test-other"})
	if err != nil {
		t.Fatal(err)
	}
	opts.Port = -1
	s, err := server.NewServer(opts)
	if err != nil {
		t.Fatal(err)
	}
	s.Start()
	defer s.Shutdown()
	if !s.ReadyForConnections(3 * time.Second) {
		t.Fatal("broker not ready")
	}
	o := jlite.DefaultNodeOptions()
	o.Replica.Connection.URL = s.ClientURL()
	o.Replica.Connection.Username = "owner"
	o.Replica.Connection.Password = "test-owner"
	n, err := jlite.OpenNode(filepath.Join(t.TempDir(), "owner.zova"), demoNamespace, demoConfig("owner"), o)
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dir, err := os.MkdirTemp("", "jlite-sock-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	socket := filepath.Join(dir, "control.sock")
	done := make(chan error, 1)
	go func() { done <- serveSocket(ctx, socket, n) }()
	deadline := time.Now().Add(2 * time.Second)
	for {
		select {
		case err := <-done:
			t.Fatalf("socket startup: %v", err)
		default:
		}
		if _, err := os.Stat(socket); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("missing socket")
		}
		time.Sleep(10 * time.Millisecond)
	}
	stat, err := os.Stat(socket)
	if err != nil || stat.Mode().Perm() != 0600 {
		t.Fatalf("socket permissions: %v %v", stat, err)
	}
	reply, err := requestSocket(socket, command{Operation: "put", RequestID: "test", Key: "doc", Value: "hello"})
	if err != nil || reply.Error != "" || reply.Write == nil || reply.Write.Sequence != 1 {
		t.Fatalf("write response: %+v %v", reply, err)
	}
	reply, err = requestSocket(socket, command{Operation: "get", Key: "doc"})
	if err != nil || reply.Error != "" || reply.Read == nil || string(reply.Read.Value) != "hello" {
		t.Fatalf("read response: %+v %v", reply, err)
	}
	reply, err = requestSocket(socket, command{Operation: "status"})
	if err != nil || reply.Status == nil || reply.Status.LocalSequence != 1 {
		t.Fatalf("status: %+v %v", reply, err)
	}
	reply, err = requestSocket(socket, command{Operation: "invalid"})
	if err != nil || reply.Error == "" {
		t.Fatalf("invalid request: %+v %v", reply, err)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("socket shutdown hung")
	}
	if _, err := os.Stat(socket); !os.IsNotExist(err) {
		t.Fatalf("socket not removed: %v", err)
	}
}
