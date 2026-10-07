package jlite

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	zova "github.com/ata-sesli/zova/bindings/go"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func publisherServer(t *testing.T, dir string, configs ...Config) (*server.Server, jetstream.JetStream) {
	return publisherServerOnPort(t, dir, -1, configs...)
}

func publisherServerOnPort(t *testing.T, dir string, port int, configs ...Config) (*server.Server, jetstream.JetStream) {
	t.Helper()
	cfg := testConfig()
	if len(configs) > 0 {
		cfg = configs[0]
	}
	options := &server.Options{}
	if err := options.ProcessConfigString("jetstream { sync_interval: always }"); err != nil {
		t.Fatal(err)
	}
	if !options.SyncAlways {
		t.Fatal("test server must fsync publications")
	}
	options.Host, options.Port = "127.0.0.1", port
	options.JetStream, options.StoreDir = true, dir
	options.NoLog, options.NoSigs = true, true
	options.Users = []*server.User{{Username: "admin", Password: "test-admin"}}
	for _, node := range cfg.Nodes {
		cfg.NodeID = node
		permissions, err := PermissionsFor(cfg, "project:alpha")
		if errors.Is(err, ErrNotAssigned) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		options.Users = append(options.Users, &server.User{Username: node, Password: "test-" + node, Permissions: &server.Permissions{Publish: &server.SubjectPermission{Allow: permissions.Publish}, Subscribe: &server.SubjectPermission{Allow: permissions.Subscribe}}})
	}
	s, err := server.NewServer(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Shutdown(); s.WaitForShutdown() })
	go s.Start()
	if !s.ReadyForConnections(5 * time.Second) {
		t.Fatal("authenticated server not ready")
	}
	nc, err := nats.Connect(s.ClientURL(), nats.UserInfo("admin", "test-admin"), nats.NoReconnect())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	return s, js
}

func TestPublisherRestartAndCancellation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(t.TempDir(), "owner.zova")
	s, _ := publisherServer(t, dir)
	w := openTestWriter(t, path, testConfig(), DefaultWriterOptions())
	r := testWrite()
	if _, err := w.Write(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	p := openTestPublisher(t, w, testPublisherOptions(s))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if ok, err := p.PublishNext(ctx); ok || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled publish: %v %v", ok, err)
	}
	state, _ := w.State()
	if state.PublishedSequence != 0 || state.OutboxCount != 1 {
		t.Fatal("cancellation advanced checkpoint")
	}
	if ok, err := p.PublishNext(context.Background()); !ok || err != nil {
		t.Fatalf("initial publish: %v %v", ok, err)
	}
	p.Close()
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	s.Shutdown()
	s.WaitForShutdown()
	s, js := publisherServer(t, dir)
	w = openTestWriter(t, path, testConfig(), DefaultWriterOptions())
	p = openTestPublisher(t, w, testPublisherOptions(s))
	r.RequestID = "req-2"
	if _, err := w.Write(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	if ok, err := p.PublishNext(context.Background()); !ok || err != nil {
		t.Fatalf("restart publish: %v %v", ok, err)
	}
	name, _ := StreamName(w.namespace)
	stream, _ := js.Stream(context.Background(), name)
	info, err := stream.Info(context.Background())
	state, _ = w.State()
	if err != nil || info.State.Msgs != 2 || state.PublishedSequence != 2 || state.OutboxCount != 0 {
		t.Fatalf("restart lost progress: %+v %+v %v", info, state, err)
	}
}

func TestPublisherWrongCredentialsAndSinglePublisher(t *testing.T) {
	s, _ := publisherServer(t, t.TempDir())
	w := openTestWriter(t, filepath.Join(t.TempDir(), "owner.zova"), testConfig(), DefaultWriterOptions())
	o := testPublisherOptions(s)
	o.Password = "incorrect"
	if p, err := ConnectPublisher(context.Background(), w, o); err == nil {
		p.Close()
		t.Fatal("wrong credentials accepted")
	}
	p := openTestPublisher(t, w, testPublisherOptions(s))
	_ = p
	if another, err := ConnectPublisher(context.Background(), w, testPublisherOptions(s)); !errors.Is(err, ErrPublisherBusy) {
		if another != nil {
			another.Close()
		}
		t.Fatalf("two publishers attached: %v", err)
	}
}

func testPublisherOptions(s *server.Server) PublisherOptions {
	o := DefaultPublisherOptions()
	o.URL = s.ClientURL()
	o.Username = "owner"
	o.Password = "test-owner"
	return o
}

func openTestPublisher(t *testing.T, w *Writer, o PublisherOptions) *Publisher {
	t.Helper()
	p, err := ConnectPublisher(context.Background(), w, o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return p
}

func TestPublisherOrderedPublicationAndAtomicProgress(t *testing.T) {
	s, js := publisherServer(t, t.TempDir())
	w := openTestWriter(t, filepath.Join(t.TempDir(), "owner.zova"), testConfig(), DefaultWriterOptions())
	for _, id := range []string{"req-1", "req-2"} {
		r := testWrite()
		r.RequestID = id
		if _, err := w.Write(context.Background(), r); err != nil {
			t.Fatal(err)
		}
	}
	p := openTestPublisher(t, w, testPublisherOptions(s))
	for range 2 {
		published, err := p.PublishNext(context.Background())
		if err != nil || !published {
			t.Fatalf("publish = %v %v", published, err)
		}
	}
	if published, err := p.PublishNext(context.Background()); err != nil || published {
		t.Fatalf("empty publish = %v %v", published, err)
	}
	state, err := w.State()
	if err != nil || state.PublishedSequence != 2 || state.OutboxCount != 0 || state.OutboxBytes != 0 {
		t.Fatalf("checkpoint = %+v %v", state, err)
	}
	name, _ := StreamName(w.namespace)
	stream, err := js.Stream(context.Background(), name)
	if err != nil {
		t.Fatal(err)
	}
	info, err := stream.Info(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if info.Config.Retention != jetstream.LimitsPolicy || info.Config.Discard != jetstream.DiscardNew || info.Config.MaxAge != 0 || info.State.Msgs != 2 {
		t.Fatalf("stream: %+v", info)
	}
	for seq := uint64(1); seq <= 2; seq++ {
		m, err := stream.GetMsg(context.Background(), seq)
		if err != nil {
			t.Fatal(err)
		}
		c, err := DecodeChange(m.Data, w.cfg)
		if err != nil || c.Sequence != seq || m.Header.Get(jetstream.MsgIDHeader) != c.ID {
			t.Fatalf("message %d = %+v %v", seq, c, err)
		}
	}
}

func TestPublisherReplicaCannotPublishOwnerEnvelope(t *testing.T) {
	s, js := publisherServer(t, t.TempDir())
	w := openTestWriter(t, filepath.Join(t.TempDir(), "owner.zova"), testConfig(), DefaultWriterOptions())
	p := openTestPublisher(t, w, testPublisherOptions(s))
	_ = p
	cfg := testConfig()
	cfg.NodeID = "replica"
	permissions, _ := PermissionsFor(cfg, w.namespace)
	nc, err := nats.Connect(s.ClientURL(), nats.UserInfo("replica", "test-replica"), nats.CustomInboxPrefix(permissions.InboxPrefix), nats.NoReconnect(), nats.ErrorHandler(func(*nats.Conn, *nats.Subscription, error) {}))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	replica, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	change, _ := w.cfg.NewChange(testWrite(), 1)
	wire, _ := change.Encode(w.cfg)
	subject, _ := ChangeSubject(w.namespace)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := replica.Publish(ctx, subject, wire); err == nil {
		t.Fatal("replica published a forged owner envelope")
	}
	name, _ := StreamName(w.namespace)
	stream, _ := js.Stream(context.Background(), name)
	info, err := stream.Info(context.Background())
	if err != nil || info.State.Msgs != 0 {
		t.Fatalf("unauthorized mutation retained: %+v %v", info, err)
	}
}

func TestPublisherRequiresAuthenticationAndSafeExistingPolicy(t *testing.T) {
	s := startJetStream(t, t.TempDir())
	w := openTestWriter(t, filepath.Join(t.TempDir(), "owner.zova"), testConfig(), DefaultWriterOptions())
	if p, err := ConnectPublisher(context.Background(), w, testPublisherOptions(s)); !errors.Is(err, ErrPublisherAuth) {
		if p != nil {
			p.Close()
		}
		t.Fatalf("unauthenticated server accepted: %v", err)
	}
	s2, js := publisherServer(t, t.TempDir())
	o := testPublisherOptions(s2)
	p := openTestPublisher(t, w, o)
	p.Close()
	name, _ := StreamName(w.namespace)
	stream, err := js.Stream(context.Background(), name)
	if err != nil {
		t.Fatal(err)
	}
	info, err := stream.Info(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	bad := info.Config
	bad.MaxAge = 10 * time.Minute
	if _, err := js.UpdateStream(context.Background(), bad); err != nil {
		t.Fatal(err)
	}
	if p, err := ConnectPublisher(context.Background(), w, o); !errors.Is(err, ErrStreamPolicy) {
		if p != nil {
			p.Close()
		}
		t.Fatalf("unsafe stream accepted: %v", err)
	}
	info, err = stream.Info(context.Background())
	if err != nil || info.Config.MaxAge != 10*time.Minute {
		t.Fatal("publisher silently changed stream policy")
	}
}

func TestPublisherFullStreamAndDisconnectLeaveOutbox(t *testing.T) {
	s, _ := publisherServer(t, t.TempDir())
	w := openTestWriter(t, filepath.Join(t.TempDir(), "owner.zova"), testConfig(), DefaultWriterOptions())
	if _, err := w.Write(context.Background(), testWrite()); err != nil {
		t.Fatal(err)
	}
	o := testPublisherOptions(s)
	o.MaxStreamBytes = 100
	p := openTestPublisher(t, w, o)
	if ok, err := p.PublishNext(context.Background()); err == nil || ok {
		t.Fatalf("full stream accepted: %v %v", ok, err)
	}
	state, _ := w.State()
	if state.PublishedSequence != 0 || state.OutboxCount != 1 {
		t.Fatalf("rejection advanced state: %+v", state)
	}
	s.Shutdown()
	s.WaitForShutdown()
	if ok, err := p.PublishNext(context.Background()); err == nil || ok {
		t.Fatalf("disconnect advanced: %v %v", ok, err)
	}
	state, _ = w.State()
	if state.PublishedSequence != 0 || state.OutboxCount != 1 {
		t.Fatal("disconnect discarded outbox")
	}
}

func TestPublisherRetriesAcceptedMessageAfterCheckpointFailure(t *testing.T) {
	for _, expired := range []bool{false, true} {
		t.Run(map[bool]string{false: "deduplicated", true: "expired-window"}[expired], func(t *testing.T) {
			s, js := publisherServer(t, t.TempDir())
			o := testPublisherOptions(s)
			if expired {
				o.DuplicateWindow = 100 * time.Millisecond
			}
			path := filepath.Join(t.TempDir(), "owner.zova")
			w := openTestWriter(t, path, testConfig(), DefaultWriterOptions())
			if _, err := w.Write(context.Background(), testWrite()); err != nil {
				t.Fatal(err)
			}
			original, _, _ := w.NextOutbox()
			p := openTestPublisher(t, w, o)
			w.mu.Lock()
			err := w.db.Exec(`CREATE TRIGGER fail_checkpoint BEFORE UPDATE OF published ON jlite_state BEGIN SELECT RAISE(ABORT,'checkpoint failure'); END;`)
			w.mu.Unlock()
			if err != nil {
				t.Fatal(err)
			}
			if ok, err := p.PublishNext(context.Background()); ok || !errors.Is(err, ErrWriterFailed) {
				t.Fatalf("checkpoint failure = %v %v", ok, err)
			}
			p.Close()
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			db, err := zova.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			if got := queryInt(t, db, "SELECT published FROM jlite_state"); got != 0 {
				t.Fatal("failed checkpoint advanced")
			}
			if err := db.Exec("DROP TRIGGER fail_checkpoint"); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			if expired {
				time.Sleep(200 * time.Millisecond)
			}
			w = openTestWriter(t, path, testConfig(), DefaultWriterOptions())
			p = openTestPublisher(t, w, o)
			if ok, err := p.PublishNext(context.Background()); !ok || err != nil {
				t.Fatalf("retry = %v %v", ok, err)
			}
			name, _ := StreamName(w.namespace)
			stream, _ := js.Stream(context.Background(), name)
			info, err := stream.Info(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			want := uint64(1)
			if expired {
				want = 2
			}
			if info.State.Msgs != want {
				t.Fatalf("message count=%d want %d", info.State.Msgs, want)
			}
			for seq := uint64(1); seq <= want; seq++ {
				msg, err := stream.GetMsg(context.Background(), seq)
				if err != nil || !bytes.Equal(msg.Data, original.Payload) || msg.Header.Get(jetstream.MsgIDHeader) != original.ID {
					t.Fatal("retry changed immutable bytes or identity")
				}
			}
			state, _ := w.State()
			if state.PublishedSequence != 1 || state.OutboxCount != 0 {
				t.Fatalf("recovered checkpoint: %+v", state)
			}
			r := testWrite()
			r.RequestID = "req-2"
			if _, err := w.Write(context.Background(), r); err != nil {
				t.Fatal(err)
			}
			if ok, err := p.PublishNext(context.Background()); !ok || err != nil {
				t.Fatalf("next logical sequence after retry: %v %v", ok, err)
			}
			last, err := stream.GetMsg(context.Background(), want+1)
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := DecodeChange(last.Data, w.cfg)
			if err != nil || decoded.Sequence != 2 {
				t.Fatalf("logical vs stream sequence: %+v %v", decoded, err)
			}
		})
	}
}

func TestPublisherRejectsLostOrRecreatedStream(t *testing.T) {
	for _, recreate := range []bool{false, true} {
		t.Run(map[bool]string{false: "missing", true: "recreated"}[recreate], func(t *testing.T) {
			s, js := publisherServer(t, t.TempDir())
			w := openTestWriter(t, filepath.Join(t.TempDir(), "owner.zova"), testConfig(), DefaultWriterOptions())
			o := testPublisherOptions(s)
			p := openTestPublisher(t, w, o)
			p.Close()
			name, _ := StreamName(w.namespace)
			stream, _ := js.Stream(context.Background(), name)
			info, err := stream.Info(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if err := js.DeleteStream(context.Background(), name); err != nil {
				t.Fatal(err)
			}
			if recreate {
				if _, err := js.CreateStream(context.Background(), info.Config); err != nil {
					t.Fatal(err)
				}
			}
			if p, err := ConnectPublisher(context.Background(), w, o); !errors.Is(err, ErrStreamIdentity) {
				if p != nil {
					p.Close()
				}
				t.Fatalf("lost/recreated log adopted: %v", err)
			}
		})
	}
}
