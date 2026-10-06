package jlite

import (
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
)

// startJetStream owns a real, isolated server for this test. A caller can reuse
// its temporary store directory after shutdown to exercise durable recovery.
func startJetStream(t *testing.T, storeDir string) *server.Server {
	t.Helper()
	s, err := server.NewServer(&server.Options{
		Host: "127.0.0.1", Port: -1,
		JetStream: true, StoreDir: storeDir,
		JetStreamMaxMemory: 8 << 20, JetStreamMaxStore: 16 << 20,
		NoLog: true, NoSigs: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		s.Shutdown()
		s.WaitForShutdown()
	})
	go s.Start()
	if !s.ReadyForConnections(5 * time.Second) {
		t.Fatal("temporary JetStream server did not become ready")
	}
	t.Logf("NATS %s ready on %s", server.VERSION, s.ClientURL())
	return s
}
