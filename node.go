package jlite

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

var (
	ErrNodeClosed  = errors.New("node is closed")
	ErrNodeBlocked = errors.New("namespace is blocked")
)

// NodeOptions configures one hosted namespace and its reconnect/poll interval.
// Replica.Connection is also used by an owner's publisher.
type NodeOptions struct {
	Writer    WriterOptions
	Replica   ReplicaOptions
	RetryWait time.Duration
}

// DefaultNodeOptions returns bounded writer/replica settings and a 250 ms retry.
func DefaultNodeOptions() NodeOptions {
	return NodeOptions{Writer: DefaultWriterOptions(), Replica: DefaultReplicaOptions(), RetryWait: 250 * time.Millisecond}
}

// NodeStatus contains this node's local knowledge, never other nodes' progress.
// Connected describes the most recent transport observation, not freshness.
type NodeStatus struct {
	NodeID            string
	Namespace         string
	Role              string
	DatabaseID        string
	LocalSequence     uint64
	PublishedSequence uint64
	AppliedSequence   uint64
	BootstrapTarget   uint64
	OutboxCount       int64
	OutboxBytes       int64
	PendingOperations int
	PendingBytes      int
	Ready             bool
	Connected         bool
	Closing           bool
	LastError         string
	Blocked           string
}

// Node owns one database and a serial replication loop with explicit shutdown.
type Node struct {
	mu        sync.Mutex
	writer    *Writer
	replica   *Replica
	options   NodeOptions
	status    NodeStatus
	cancel    context.CancelFunc
	done      chan struct{}
	closeDone chan struct{}
	closeErr  error
}

// OpenNode opens local storage and starts replication asynchronously. Transport
// downtime does not prevent owner writes within configured outbox capacity.
func OpenNode(path, namespace string, cfg Config, o NodeOptions) (*Node, error) {
	if o.RetryWait <= 0 || o.RetryWait > time.Minute {
		return nil, ErrInvalidConfig
	}
	if err := validateConnectionOptions(o.Replica.Connection); err != nil {
		return nil, err
	}
	if o.Replica.FetchWait <= 0 || o.Replica.FetchWait > o.Replica.Connection.RequestTimeout || o.Replica.AckWait <= o.Replica.FetchWait+o.Replica.Connection.RequestTimeout || o.Replica.MaxFetchBytes < cfg.Limits.MaxChangeBytes+4096 {
		return nil, ErrInvalidConfig
	}
	a, err := cfg.assignment(namespace)
	if err != nil {
		return nil, err
	}
	n := &Node{options: o, done: make(chan struct{}), closeDone: make(chan struct{}), status: NodeStatus{NodeID: cfg.NodeID, Namespace: namespace}}
	if a.Owner == cfg.NodeID {
		n.writer, err = OpenWriter(path, namespace, cfg, o.Writer)
		n.status.Role = "owner"
	} else {
		n.replica, err = OpenReplica(path, namespace, cfg)
		n.status.Role = "replica"
	}
	if err != nil {
		return nil, err
	}
	if o.Replica.Connection.TLSConfig != nil {
		n.options.Replica.Connection.TLSConfig = o.Replica.Connection.TLSConfig.Clone()
	}
	ctx, cancel := context.WithCancel(context.Background())
	n.cancel = cancel
	go n.run(ctx)
	return n, nil
}

// Write returns a local commit result. Shutdown stops admission before draining
// accepted writes; cancellation after admission retains Writer's unknown outcome.
func (n *Node) Write(ctx context.Context, r WriteRequest) (WriteResult, error) {
	n.mu.Lock()
	if n.status.Closing {
		n.mu.Unlock()
		return WriteResult{}, ErrNodeClosed
	}
	if n.status.Blocked != "" {
		n.mu.Unlock()
		return WriteResult{}, ErrNodeBlocked
	}
	if n.writer == nil {
		n.mu.Unlock()
		return WriteResult{}, ErrNotOwner
	}
	q, err := n.writer.admit(ctx, r)
	n.mu.Unlock()
	if err != nil {
		return WriteResult{}, err
	}
	select {
	case result := <-q.reply:
		return result.result, result.err
	case <-ctx.Done():
		return WriteResult{}, ctx.Err()
	}
}

// Get returns local data and applied position; replicas may be stale while
// disconnected. Readiness does not assert equality with the owner's current head.
func (n *Node) Get(r ReadRequest) (ReadResult, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.status.Closing {
		return ReadResult{}, ErrNodeClosed
	}
	if n.status.Blocked != "" {
		return ReadResult{}, ErrNodeBlocked
	}
	if n.writer != nil {
		return n.writer.Get(r)
	}
	return n.replica.Get(r)
}

// Status remains inspectable during blocking and after shutdown. Durable fields
// are sampled under the database lock, separately from transport observations.
func (n *Node) Status() (NodeStatus, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.status.Closing {
		return n.status, nil
	}
	return n.snapshotLocked()
}

func (n *Node) snapshotLocked() (NodeStatus, error) {
	s := n.status
	if n.writer != nil {
		n.writer.mu.Lock()
		st, err := readWriterState(n.writer.db)
		failure := n.writer.failed
		s.PendingOperations, s.PendingBytes = n.writer.pendingCount, n.writer.pendingBytes
		n.writer.mu.Unlock()
		if err != nil {
			return s, err
		}
		s.DatabaseID, s.LocalSequence, s.AppliedSequence = st.DatabaseID, st.Sequence, st.Sequence
		s.PublishedSequence, s.OutboxCount, s.OutboxBytes = st.PublishedSequence, st.OutboxCount, st.OutboxBytes
		s.Ready = s.Blocked == "" && failure == nil
		if failure != nil {
			s.Blocked = failure.Error()
		}
	} else {
		st, err := n.replica.State()
		if err != nil {
			return s, err
		}
		s.DatabaseID, s.AppliedSequence, s.BootstrapTarget = st.DatabaseID, st.AppliedSequence, st.BootstrapTarget
		s.Ready = st.Ready && s.Blocked == ""
		if st.Blocked != nil {
			s.Blocked = st.Blocked.Error()
			s.Ready = false
		}
	}
	n.status = s
	return s, nil
}

// Close stops admission, cancels and joins replication, drains accepted writes,
// then releases storage. Unpublished commits remain in the durable outbox.
func (n *Node) Close() error {
	n.mu.Lock()
	if n.status.Closing {
		n.mu.Unlock()
		<-n.closeDone
		return n.closeErr
	}
	n.mu.Unlock()
	_, _ = n.Status()
	n.mu.Lock()
	if n.status.Closing {
		n.mu.Unlock()
		<-n.closeDone
		return n.closeErr
	}
	n.status.Closing = true
	n.status.Ready = false
	n.cancel()
	n.mu.Unlock()
	<-n.done
	var err error
	if n.writer != nil {
		// Stop the underlying writer's admission and join its drain before
		// sampling final progress; Close then releases its already-idle handle.
		n.writer.mu.Lock()
		n.writer.closing = true
		close(n.writer.stop)
		n.writer.mu.Unlock()
		<-n.writer.done
		n.mu.Lock()
		_, err = n.snapshotLocked()
		n.status.Ready = false
		n.mu.Unlock()
		err = errors.Join(err, n.writer.Close())
	} else {
		n.mu.Lock()
		_, err = n.snapshotLocked()
		n.status.Ready = false
		n.mu.Unlock()
		err = errors.Join(err, n.replica.Close())
	}
	n.mu.Lock()
	n.status.Connected = false
	n.closeErr = err
	close(n.closeDone)
	n.mu.Unlock()
	return err
}

func (n *Node) observe(connected bool, err error, blocked bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.status.Connected = connected
	if err == nil {
		n.status.LastError = ""
	} else {
		n.status.LastError = err.Error()
	}
	if blocked {
		n.status.Blocked = err.Error()
		n.status.Ready = false
	}
}

func permanentNodeError(err error) bool {
	if errors.Is(err, nats.ErrAuthorization) || errors.Is(err, nats.ErrPermissionViolation) {
		return true
	}
	if transientNodeError(err) {
		return false
	}
	for _, target := range []error{ErrStreamIdentity, ErrStreamPolicy, ErrIdentityMismatch, ErrInvalidChange, ErrUnsupportedVersion, ErrSequenceGap, ErrWriterFailed, ErrReplicaBlocked, ErrPublisherAuth, nats.ErrAuthorization, nats.ErrPermissionViolation} {
		if errors.Is(err, target) {
			return true
		}
	}
	return false
}

func transientNodeError(err error) bool {
	var network net.Error
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, nats.ErrTimeout) || errors.Is(err, nats.ErrConnectionClosed) || errors.Is(err, nats.ErrNoServers) || errors.Is(err, nats.ErrNoResponders) || errors.As(err, &network)
}

func (n *Node) run(ctx context.Context) {
	defer close(n.done)
	var p *Publisher
	var c *ReplicaConsumer
	defer func() {
		if p != nil {
			p.Close()
		}
		if c != nil {
			c.Close()
		}
	}()
	for ctx.Err() == nil {
		var err error
		work := false
		if n.writer != nil {
			if p == nil {
				p, err = ConnectPublisher(ctx, n.writer, n.options.Replica.Connection)
			}
			if err == nil {
				work, err = p.PublishNext(ctx)
			}
		} else {
			if c == nil {
				c, err = ConnectReplicaConsumer(ctx, n.replica, n.options.Replica)
			}
			if err == nil {
				var count int
				count, err = c.ConsumeNext(ctx)
				work = count > 0
			}
		}
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			// A not-yet-provisioned stream is expected for a fresh replica started
			// before its owner. A pinned stream disappearing remains a hard failure.
			blocked := permanentNodeError(err)
			if n.replica != nil && transientNodeError(err) && !blocked {
				n.replica.mu.Lock()
				if transientNodeError(n.replica.blocked) {
					n.replica.blocked = nil
				}
				n.replica.mu.Unlock()
			}
			if n.replica != nil && errors.Is(err, jetstream.ErrStreamNotFound) {
				n.replica.mu.Lock()
				st, e := n.replica.readState()
				if e == nil && st.StreamID == "" {
					n.replica.blocked = nil
					blocked = false
				}
				n.replica.mu.Unlock()
			}
			if p != nil {
				p.Close()
				p = nil
			}
			if c != nil {
				c.Close()
				c = nil
			}
			n.observe(false, err, blocked)
			if blocked {
				return
			}
		} else {
			n.observe(true, nil, false)
		}
		if work && err == nil {
			continue
		}
		timer := time.NewTimer(n.options.RetryWait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}
