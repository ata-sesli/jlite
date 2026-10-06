package jlite

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// ReplicaOptions bounds pull requests and acknowledgement timing. Connection
// carries the same explicit stream policy/authentication settings as the owner.
type ReplicaOptions struct {
	Connection    PublisherOptions
	FetchWait     time.Duration
	AckWait       time.Duration
	MaxFetchBytes int
}

// DefaultReplicaOptions bounds each fetch to at most 2 MiB of message/header
// budget and waits 100 ms for a batch. Application batches use Config.Limits.
func DefaultReplicaOptions() ReplicaOptions {
	return ReplicaOptions{Connection: DefaultPublisherOptions(), FetchWait: 100 * time.Millisecond, AckWait: 30 * time.Second, MaxFetchBytes: 2 << 20}
}

// ReplicaConsumer owns the authenticated pull connection for one replica.
// Calls to ConsumeNext are serialized; no background catch-up loop is hidden.
type ReplicaConsumer struct {
	mu       sync.Mutex
	r        *Replica
	nc       *nats.Conn
	stream   jetstream.Stream
	consumer jetstream.Consumer
	name     string
	options  ReplicaOptions
	count    int
	closed   bool
}

// ConnectReplicaConsumer pins stream identity and a fixed bootstrap target,
// and starts its own durable explicit-ack consumer from the local checkpoint.
func ConnectReplicaConsumer(ctx context.Context, r *Replica, o ReplicaOptions) (_ *ReplicaConsumer, err error) {
	if r == nil {
		return nil, ErrInvalidConfig
	}
	if err := validateConnectionOptions(o.Connection); err != nil {
		return nil, err
	}
	if o.FetchWait <= 0 || o.FetchWait > o.Connection.RequestTimeout || o.AckWait <= o.FetchWait+o.Connection.RequestTimeout || o.MaxFetchBytes < r.cfg.Limits.MaxChangeBytes+4096 {
		return nil, ErrInvalidConfig
	}
	r.mu.Lock()
	if e := r.available(); e != nil {
		r.mu.Unlock()
		return nil, e
	}
	if r.consumerAttached {
		r.mu.Unlock()
		return nil, ErrReplicaBusy
	}
	r.consumerAttached = true
	r.mu.Unlock()
	count := min(r.cfg.Limits.MaxBatchOperations, r.cfg.Limits.MaxBatchBytes/r.cfg.Limits.MaxChangeBytes, o.MaxFetchBytes/(r.cfg.Limits.MaxChangeBytes+4096))
	c := &ReplicaConsumer{r: r, options: o, count: count}
	c.name, _ = ConsumerName(r.namespace, r.cfg.NodeID)
	defer func() {
		if err != nil {
			if errors.Is(err, ErrStreamIdentity) || errors.Is(err, ErrStreamPolicy) || errors.Is(err, ErrSequenceGap) || errors.Is(err, ErrInvalidChange) || errors.Is(err, ErrUnsupportedVersion) || errors.Is(err, ErrIdentityMismatch) {
				r.mu.Lock()
				r.blocked = err
				r.mu.Unlock()
			}
			c.Close()
		}
	}()
	var js jetstream.JetStream
	c.nc, js, err = connectNamespace(r.cfg, r.namespace, o.Connection)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, o.Connection.RequestTimeout)
	defer cancel()
	name, _ := StreamName(r.namespace)
	c.stream, err = js.Stream(ctx, name)
	if err != nil {
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			return nil, errors.Join(ErrStreamIdentity, err)
		}
		return nil, err
	}
	info, err := c.stream.Info(ctx)
	if err != nil {
		return nil, err
	}
	state, err := r.State()
	if err != nil {
		return nil, err
	}
	if err := c.checkStream(ctx, info, state); err != nil {
		return nil, err
	}
	target := uint64(0)
	if info.State.LastSeq > 0 {
		tail, e := c.stream.GetMsg(ctx, info.State.LastSeq)
		if e != nil {
			return nil, e
		}
		change, e := c.decodeRaw(tail)
		if e != nil {
			return nil, e
		}
		target = change.Sequence
	}
	if target > info.State.LastSeq {
		return nil, ErrSequenceGap
	}
	if err := r.bindReplay(info, target); err != nil {
		return nil, err
	}
	state, err = r.State()
	if err != nil {
		return nil, err
	}
	previous, e := c.stream.Consumer(ctx, c.name)
	if e == nil {
		ci, e := previous.Info(ctx)
		if e != nil {
			return nil, e
		}
		if ci.Config.Metadata["jlite_replica_database_id"] != state.DatabaseID || ci.Config.Metadata["jlite_stream_id"] != state.StreamID {
			return nil, ErrIdentityMismatch
		}
		if ci.Config.AckPolicy != jetstream.AckExplicitPolicy || ci.Config.DeliverSubject != "" || ci.Config.MemoryStorage || ci.Config.FilterSubject != "" || len(ci.Config.FilterSubjects) > 0 {
			return nil, ErrStreamPolicy
		}
		// Never trust an ack floor over the local transaction checkpoint. Reset
		// only this node's identified consumer, preserving all retained history.
		if err := c.stream.DeleteConsumer(ctx, c.name); err != nil {
			return nil, err
		}
	} else if !errors.Is(e, jetstream.ErrConsumerNotFound) {
		return nil, e
	}
	c.consumer, err = c.stream.CreateConsumer(ctx, jetstream.ConsumerConfig{Name: c.name, Durable: c.name, AckPolicy: jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverByStartSequencePolicy, OptStartSeq: state.LogSequence + 1, AckWait: o.AckWait, MaxDeliver: -1,
		MaxAckPending: count, MaxWaiting: 1, MaxRequestBatch: count, MaxRequestExpires: o.FetchWait, MaxRequestMaxBytes: o.MaxFetchBytes, Replicas: o.Connection.Replicas,
		Metadata: map[string]string{"jlite_replica_database_id": state.DatabaseID, "jlite_stream_id": state.StreamID}})
	if err != nil {
		return nil, errors.Join(err, c.nc.LastError())
	}
	c.options.Connection.Username, c.options.Connection.Password = "", ""
	return c, nil
}

func (c *ReplicaConsumer) checkStream(ctx context.Context, info *jetstream.StreamInfo, s ReplicaState) error {
	ownerID := s.OwnerDatabaseID
	if ownerID == "" {
		ownerID = info.Config.Metadata["jlite_database_id"]
	}
	name, _ := StreamName(c.r.namespace)
	bound := s.StreamID != ""
	if err := checkNamespaceStream(info, WriterState{Namespace: s.Namespace, Owner: s.Owner, DatabaseID: ownerID}, streamBinding{Name: name, ID: s.StreamID, Created: s.StreamCreated}, bound, c.r.cfg.Limits, c.options.Connection); err != nil {
		return err
	}
	if info.State.LastSeq < s.LogSequence || info.State.LastSeq > maxSequence {
		return ErrStreamIdentity
	}
	if s.LogSequence > 0 {
		anchor, err := c.stream.GetMsg(ctx, s.LogSequence)
		if err != nil {
			return errors.Join(ErrStreamIdentity, err)
		}
		change, err := c.decodeRaw(anchor)
		if err != nil || change.ID != s.LastChangeID || change.Sequence > s.AppliedSequence {
			return ErrStreamIdentity
		}
		c.r.mu.Lock()
		id, found, err := c.r.appliedID(change.Sequence)
		c.r.mu.Unlock()
		if err != nil {
			return err
		}
		if !found || id != change.ID {
			return ErrStreamIdentity
		}
	}
	return nil
}

func (c *ReplicaConsumer) decodeRaw(m *jetstream.RawStreamMsg) (Change, error) {
	subject, _ := ChangeSubject(c.r.namespace)
	change, err := DecodeChange(m.Data, c.r.cfg)
	if err != nil {
		return Change{}, err
	}
	if change.Request.Namespace != c.r.namespace || m.Subject != subject || m.Header.Get(jetstream.MsgIDHeader) != change.ID {
		return Change{}, ErrInvalidChange
	}
	return change, nil
}

func (r *Replica) bindReplay(info *jetstream.StreamInfo, target uint64) (err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.available(); err != nil {
		return err
	}
	s, err := r.readState()
	if err != nil {
		return err
	}
	ownerID, streamID := info.Config.Metadata["jlite_database_id"], info.Config.Metadata["jlite_stream_id"]
	created := info.Created.UTC().Format(time.RFC3339Nano)
	if s.OwnerDatabaseID != "" && (s.OwnerDatabaseID != ownerID || s.StreamID != streamID || s.StreamCreated != created) {
		return ErrStreamIdentity
	}
	ready := int64(0)
	if s.LogSequence >= info.State.LastSeq && s.AppliedSequence >= target {
		ready = 1
	}
	if err = r.db.BeginImmediate(); err == nil {
		err = executeSQL(r.db, "UPDATE jlite_replica SET owner_database_id=?,stream_id=?,stream_created=?,target=?,log_target=?,ready=? WHERE singleton=1", ownerID, streamID, created, int64(target), int64(info.State.LastSeq), ready)
	}
	if err == nil {
		err = r.db.Commit()
	}
	if err != nil {
		err = errors.Join(err, r.db.Rollback())
		r.blocked = err
	}
	return err
}

// ConsumeNext fetches a bounded batch, commits it and its checkpoint together,
// then explicitly double-acknowledges each delivery. A failed ack leaves the
// committed local position authoritative for redelivery/reconnection.
func (c *ReplicaConsumer) ConsumeNext(ctx context.Context) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return 0, ErrReplicaClosed
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if !c.nc.IsConnected() {
		return 0, nats.ErrConnectionClosed
	}
	s, err := c.r.State()
	if err != nil {
		return 0, err
	}
	if s.Blocked != nil {
		return 0, errors.Join(ErrReplicaBlocked, s.Blocked)
	}
	rpc, cancel := context.WithTimeout(ctx, c.options.Connection.RequestTimeout)
	defer cancel()
	info, err := c.stream.Info(rpc)
	if err != nil {
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			return 0, c.block(errors.Join(ErrStreamIdentity, err))
		}
		return 0, err
	}
	if err := c.checkStream(rpc, info, s); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, nats.ErrConnectionClosed) || errors.Is(err, nats.ErrTimeout) {
			return 0, err
		}
		return 0, c.block(err)
	}
	batch, err := c.consumer.Fetch(c.count, jetstream.FetchMaxWait(c.options.FetchWait))
	if err != nil {
		return 0, errors.Join(err, c.nc.LastError())
	}
	var messages []jetstream.Msg
	for {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case m, ok := <-batch.Messages():
			if !ok {
				goto fetched
			}
			messages = append(messages, m)
		}
	}
fetched:
	if err := batch.Error(); err != nil && !errors.Is(err, nats.ErrTimeout) {
		return 0, err
	}
	if len(messages) == 0 {
		return 0, nil
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	applied, err := c.r.applyMessages(messages, c.name)
	if err != nil {
		return 0, err
	}
	ackctx, ackcancel := context.WithTimeout(ctx, c.options.Connection.RequestTimeout)
	defer ackcancel()
	for _, message := range messages {
		if err := message.DoubleAck(ackctx); err != nil {
			return applied, errors.Join(err, c.nc.LastError())
		}
	}
	return applied, nil
}

func (c *ReplicaConsumer) block(err error) error {
	c.r.mu.Lock()
	defer c.r.mu.Unlock()
	c.r.blocked = err
	return err
}

// Close releases the transport and attachment; it does not delete history.
func (c *ReplicaConsumer) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	c.closed = true
	if c.nc != nil {
		c.nc.Close()
	}
	c.r.mu.Lock()
	c.r.consumerAttached = false
	c.r.mu.Unlock()
}
