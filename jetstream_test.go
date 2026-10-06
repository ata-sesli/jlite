package jlite

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestJetStreamDurableReplay(t *testing.T) {
	storeDir := t.TempDir()
	s := startJetStream(t, storeDir)
	nc, js := connectJetStream(t, s.ClientURL())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name: "SMOKE", Subjects: []string{"smoke.changes"},
		Storage: jetstream.FileStorage, Retention: jetstream.LimitsPolicy,
		Discard: jetstream.DiscardNew, MaxBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	consumer, err := stream.CreateConsumer(ctx, jetstream.ConsumerConfig{
		Durable: "replica", AckPolicy: jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, data := range []string{"first", "second"} {
		if _, err := js.Publish(ctx, "smoke.changes", []byte(data)); err != nil {
			t.Fatal(err)
		}
	}
	msg := fetchOne(t, consumer)
	if string(msg.Data()) != "first" {
		t.Fatalf("first delivery = %q", msg.Data())
	}
	if err := msg.DoubleAck(ctx); err != nil {
		t.Fatal(err)
	}
	nc.Close()
	s.Shutdown()
	s.WaitForShutdown()

	// Reuse only this test's store directory to verify persisted stream and
	// durable consumer progress across a real server restart.
	s = startJetStream(t, storeDir)
	_, js = connectJetStream(t, s.ClientURL())
	stream, err = js.Stream(ctx, "SMOKE")
	if err != nil {
		t.Fatal(err)
	}
	consumer, err = stream.Consumer(ctx, "replica")
	if err != nil {
		t.Fatal(err)
	}
	msg = fetchOne(t, consumer)
	if string(msg.Data()) != "second" {
		t.Fatalf("delivery after restart = %q, want second", msg.Data())
	}
	if err := msg.DoubleAck(ctx); err != nil {
		t.Fatal(err)
	}
	info, err := consumer.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.AckFloor.Stream != 2 || info.NumAckPending != 0 {
		t.Fatalf("consumer progress after restart: %+v", info)
	}
	// A new replica must still receive history acknowledged by the first.
	consumer, err = stream.CreateConsumer(ctx, jetstream.ConsumerConfig{
		Durable: "new_replica", AckPolicy: jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
	})
	if err != nil {
		t.Fatal(err)
	}
	msg = fetchOne(t, consumer)
	if string(msg.Data()) != "first" {
		t.Fatalf("new replica replay = %q, want first", msg.Data())
	}
	if err := msg.DoubleAck(ctx); err != nil {
		t.Fatal(err)
	}
}

func connectJetStream(t *testing.T, url string) (*nats.Conn, jetstream.JetStream) {
	t.Helper()
	nc, err := nats.Connect(url, nats.Timeout(2*time.Second), nats.NoReconnect())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	return nc, js
}

func fetchOne(t *testing.T, consumer jetstream.Consumer) jetstream.Msg {
	t.Helper()
	batch, err := consumer.Fetch(1, jetstream.FetchMaxWait(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	var result jetstream.Msg
	for msg := range batch.Messages() {
		result = msg
	}
	if err := batch.Error(); err != nil {
		t.Fatal(err)
	}
	if result == nil {
		t.Fatal("fetch returned no message")
	}
	return result
}
