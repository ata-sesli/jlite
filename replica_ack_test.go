package jlite

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
)

// Wrap the real pull/ACK boundary; validation, database transactions, retained
// history and explicit broker acknowledgements still use their production paths.
type ackWrappingConsumer struct {
	jetstream.Consumer
	wrap func(jetstream.Msg) jetstream.Msg
}

func TestReplicaAckFailureRetriesFromCommittedState(t *testing.T) {
	for _, lostReply := range []bool{false, true} {
		name := map[bool]string{false: "before-broker-ACK", true: "lost-confirmation"}[lostReply]
		t.Run(name, func(t *testing.T) {
			w, p, r, c, path := ackFixture(t)
			originalConsumer := c.consumer
			failure := errors.New("injected ACK uncertainty")
			var failedMessage jetstream.Msg
			c.consumer = ackWrappingConsumer{c.consumer, func(m jetstream.Msg) jetstream.Msg {
				meta, err := m.Metadata()
				if err != nil {
					return ackWrappingMsg{m, func(context.Context, jetstream.Msg) error { return err }}
				}
				if meta.Sequence.Stream != 2 {
					return m
				}
				failedMessage = m
				return ackWrappingMsg{m, func(ctx context.Context, original jetstream.Msg) error {
					if lostReply {
						if err := original.DoubleAck(ctx); err != nil {
							return err
						}
					}
					return failure
				}}
			}}
			n, err := c.ConsumeNext(context.Background())
			if n != 8 || !errors.Is(err, failure) {
				t.Fatalf("uncertain confirmation lost applied count/error: %d %v", n, err)
			}
			st, stateErr := r.State()
			if stateErr != nil || st.AppliedSequence != 8 || st.LogSequence != 8 || st.Blocked != nil {
				t.Fatalf("ACK failure changed committed state: %+v %v", st, stateErr)
			}
			// Only the first delivery was confirmed before the failure.
			info, infoErr := c.consumer.Info(context.Background())
			pending := 7
			if lostReply {
				pending = 6
			}
			if infoErr != nil || info.NumAckPending != pending {
				t.Fatalf("unexpected ACK uncertainty boundary: %+v %v", info, infoErr)
			}
			c.consumer = originalConsumer
			if !lostReply {
				// Ask the real broker to redeliver the still-unacknowledged message,
				// without waiting thirty seconds for the default AckWait.
				if err := failedMessage.Nak(); err != nil {
					t.Fatal(err)
				}
				if n, err := c.ConsumeNext(context.Background()); n != 0 || err != nil {
					t.Fatalf("committed redelivery was applied twice: %d %v", n, err)
				}
			}
			url := c.options.Connection.URL
			c.Close()
			if err := r.Close(); err != nil {
				t.Fatal(err)
			}
			publishFailureRequests(t, w, p, failureRequests()[8:], 9)
			r = openTestReplica(t, path, "replica")
			c = openTestConsumer(t, r, url)
			drainReplica(t, r, c)
			assertFailureConvergence(t, w, []*Replica{r}, failureRequests())
		})
	}
}

type ackWrappingBatch struct {
	jetstream.MessageBatch
	channel <-chan jetstream.Msg
}

func (b ackWrappingBatch) Messages() <-chan jetstream.Msg { return b.channel }

func (c ackWrappingConsumer) Fetch(count int, opts ...jetstream.FetchOpt) (jetstream.MessageBatch, error) {
	b, err := c.Consumer.Fetch(count, opts...)
	if err != nil {
		return nil, err
	}
	ch := make(chan jetstream.Msg, count)
	for m := range b.Messages() {
		ch <- c.wrap(m)
	}
	close(ch)
	return ackWrappingBatch{b, ch}, nil
}

type ackWrappingMsg struct {
	jetstream.Msg
	confirm func(context.Context, jetstream.Msg) error
}

func (m ackWrappingMsg) DoubleAck(ctx context.Context) error { return m.confirm(ctx, m.Msg) }

func ackFixture(t *testing.T) (*Writer, *Publisher, *Replica, *ReplicaConsumer, string) {
	t.Helper()
	s, _ := publisherServer(t, t.TempDir(), replicaConfig("owner"))
	w := openTestWriter(t, filepath.Join(t.TempDir(), "owner.zova"), replicaConfig("owner"), DefaultWriterOptions())
	p := openTestPublisher(t, w, testPublisherOptions(s))
	publishFailureRequests(t, w, p, failureRequests()[:8], 1)
	path := filepath.Join(t.TempDir(), "replica.zova")
	r := openTestReplica(t, path, "replica")
	c := openTestConsumer(t, r, s.ClientURL())
	return w, p, r, c, path
}
