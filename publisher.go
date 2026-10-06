package jlite

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

var (
	ErrPublisherAuth   = errors.New("publisher requires an authenticated NATS server")
	ErrPublisherBusy   = errors.New("writer already has a publisher")
	ErrPublisherClosed = errors.New("publisher is closed")
	ErrStreamPolicy    = errors.New("incompatible stream policy")
	ErrStreamIdentity  = errors.New("stream identity or retained history does not match database")
)

// SubjectPermissions contains NATS server allowlists and the matching client
// reply prefix. Install these on the server; a client cannot enforce server ACLs.
type SubjectPermissions struct {
	Publish     []string
	Subscribe   []string
	InboxPrefix string
}

// StreamName deterministically names a namespace stream; metadata checks retain
// the full namespace identity in addition to this compact name.
func StreamName(namespace string) (string, error) {
	if _, err := ChangeSubject(namespace); err != nil {
		return "", err
	}
	digest := sha256.Sum256([]byte(namespace))
	return fmt.Sprintf("JLITE_%x", digest[:16]), nil
}

// PermissionsFor permits only the owner to publish changes or provision its
// stream. Replicas can inspect/read it, but consumer permissions are Item 5.
func PermissionsFor(c Config, namespace string) (SubjectPermissions, error) {
	a, err := c.assignment(namespace)
	if err != nil {
		return SubjectPermissions{}, err
	}
	if !a.hosts(c.NodeID) {
		return SubjectPermissions{}, ErrNotAssigned
	}
	name, _ := StreamName(namespace)
	subject, _ := ChangeSubject(namespace)
	prefix := "_JLITE_REPLY." + c.NodeID + "." + name
	p := SubjectPermissions{Publish: []string{"$JS.API.STREAM.INFO." + name, "$JS.API.STREAM.MSG.GET." + name}, Subscribe: []string{prefix + ".>"}, InboxPrefix: prefix}
	if a.Owner == c.NodeID {
		p.Publish = append(p.Publish, subject, "$JS.API.STREAM.CREATE."+name)
	}
	return p, nil
}

// PublisherOptions selects explicit stream policy and authenticated connection
// settings. Supply URL, Username and Password; use TLS outside loopback.
type PublisherOptions struct {
	URL             string
	Username        string
	Password        string
	TLSConfig       *tls.Config
	MaxStreamBytes  int64
	Replicas        int
	DuplicateWindow time.Duration
	RequestTimeout  time.Duration
}

// DefaultPublisherOptions uses one file-backed replica and finite retention
// capacity. Server fsync settings remain operator-managed.
func DefaultPublisherOptions() PublisherOptions {
	return PublisherOptions{MaxStreamBytes: 64 << 20, Replicas: 1, DuplicateWindow: 2 * time.Minute, RequestTimeout: 2 * time.Second}
}

// Publisher serially publishes one namespace's durable outbox. The caller drives
// PublishNext and owns retry scheduling; there is no hidden ingestion loop.
type Publisher struct {
	mu      sync.Mutex
	w       *Writer
	nc      *nats.Conn
	js      jetstream.JetStream
	stream  jetstream.Stream
	options PublisherOptions
	closed  bool
}

// ConnectPublisher validates or provisions retained history, then persists its
// incarnation before any publication. It never updates an existing stream.
func ConnectPublisher(ctx context.Context, w *Writer, o PublisherOptions) (_ *Publisher, err error) {
	u, e := url.Parse(o.URL)
	if w == nil || e != nil || u.Hostname() == "" || u.User != nil || o.Username == "" || o.Password == "" || o.MaxStreamBytes <= 0 || o.Replicas < 1 || o.Replicas > 5 || o.DuplicateWindow < 100*time.Millisecond || o.RequestTimeout <= 0 {
		return nil, ErrInvalidConfig
	}
	ip := net.ParseIP(u.Hostname())
	loopback := u.Hostname() == "localhost" || ip != nil && ip.IsLoopback()
	if u.Scheme != "tls" && (u.Scheme != "nats" || (!loopback && o.TLSConfig == nil)) {
		return nil, fmt.Errorf("%w: TLS required outside loopback", ErrInvalidConfig)
	}
	w.mu.Lock()
	if e := w.available(); e != nil {
		w.mu.Unlock()
		return nil, e
	}
	if w.publisherAttached {
		w.mu.Unlock()
		return nil, ErrPublisherBusy
	}
	w.publisherAttached = true
	w.mu.Unlock()
	p := &Publisher{w: w, options: o}
	defer func() {
		if err != nil {
			p.Close()
		}
	}()
	permissions, e := PermissionsFor(w.cfg, w.namespace)
	if e != nil {
		return nil, e
	}
	connectOptions := []nats.Option{nats.UserInfo(o.Username, o.Password), nats.CustomInboxPrefix(permissions.InboxPrefix), nats.Timeout(o.RequestTimeout), nats.NoReconnect(), nats.ErrorHandler(func(*nats.Conn, *nats.Subscription, error) {})}
	if o.TLSConfig != nil {
		connectOptions = append(connectOptions, nats.Secure(o.TLSConfig.Clone()))
	}
	p.nc, err = nats.Connect(o.URL, connectOptions...)
	if err != nil {
		return nil, err
	}
	if !p.nc.AuthRequired() {
		return nil, ErrPublisherAuth
	}
	p.js, err = jetstream.New(p.nc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, o.RequestTimeout)
	defer cancel()
	state, b, bound, err := w.publisherSnapshot()
	if err != nil {
		return nil, err
	}
	name, _ := StreamName(w.namespace)
	p.stream, err = p.js.Stream(ctx, name)
	if errors.Is(err, jetstream.ErrStreamNotFound) {
		if bound || state.PublishedSequence > 0 {
			return nil, ErrStreamIdentity
		}
		var id [16]byte
		if _, err = rand.Read(id[:]); err != nil {
			return nil, err
		}
		p.stream, err = p.js.CreateStream(ctx, p.streamConfig(state, hex.EncodeToString(id[:])))
	}
	if err != nil {
		return nil, p.connectionError(err)
	}
	info, err := p.stream.Info(ctx)
	if err != nil {
		return nil, p.connectionError(err)
	}
	if err := p.checkStream(info, state, b, bound); err != nil {
		return nil, err
	}
	if !bound {
		b = streamBinding{Name: name, ID: info.Config.Metadata["jlite_stream_id"], Created: info.Created.UTC().Format(time.RFC3339Nano)}
		if err := w.bindStream(b); err != nil {
			return nil, err
		}
	}
	if _, err := p.validateHistory(ctx, info, state, b); err != nil {
		return nil, err
	}
	// Credentials are only needed by the owned NATS connection.
	p.options.Username, p.options.Password = "", ""
	return p, nil
}

func (p *Publisher) streamConfig(state WriterState, id string) jetstream.StreamConfig {
	name, _ := StreamName(state.Namespace)
	subject, _ := ChangeSubject(state.Namespace)
	return jetstream.StreamConfig{Name: name, Subjects: []string{subject}, Storage: jetstream.FileStorage, Retention: jetstream.LimitsPolicy, Discard: jetstream.DiscardNew,
		MaxBytes: p.options.MaxStreamBytes, MaxMsgs: -1, MaxMsgsPerSubject: -1, MaxMsgSize: int32(p.w.cfg.Limits.MaxChangeBytes + 4096), Replicas: p.options.Replicas,
		Duplicates: p.options.DuplicateWindow, DenyDelete: true, DenyPurge: true,
		Metadata: map[string]string{"jlite_database_id": state.DatabaseID, "jlite_namespace": state.Namespace, "jlite_owner": state.Owner, "jlite_protocol": strconv.Itoa(ProtocolVersion), "jlite_stream_id": id}}
}

func (p *Publisher) checkStream(info *jetstream.StreamInfo, state WriterState, b streamBinding, bound bool) error {
	c := info.Config
	expected := p.streamConfig(state, c.Metadata["jlite_stream_id"])
	if c.Name != expected.Name || len(c.Subjects) != 1 || c.Subjects[0] != expected.Subjects[0] || c.Storage != expected.Storage || c.Retention != expected.Retention || c.Discard != expected.Discard ||
		c.MaxBytes != expected.MaxBytes || c.Replicas != expected.Replicas || c.MaxMsgSize != expected.MaxMsgSize || c.MaxAge != 0 || c.MaxMsgs != -1 || c.MaxMsgsPerSubject != -1 || c.Duplicates != expected.Duplicates ||
		c.NoAck || c.Sealed || !c.DenyDelete || !c.DenyPurge || c.AllowRollup || c.AllowMsgTTL || c.DiscardNewPerSubject || c.Mirror != nil || len(c.Sources) > 0 || c.SubjectTransform != nil || c.RePublish != nil || c.Template != "" || c.AllowMsgCounter || c.AllowMsgSchedules || c.AllowAtomicPublish || c.AllowBatchPublish || c.SubjectDeleteMarkerTTL != 0 {
		return ErrStreamPolicy
	}
	for key, value := range expected.Metadata {
		if value == "" || c.Metadata[key] != value {
			return ErrStreamIdentity
		}
	}
	if bound && (b.Name != c.Name || b.ID != c.Metadata["jlite_stream_id"] || b.Created != info.Created.UTC().Format(time.RFC3339Nano)) {
		return ErrStreamIdentity
	}
	if info.State.Msgs != info.State.LastSeq || info.State.NumDeleted != 0 || (info.State.Msgs > 0 && info.State.FirstSeq != 1) {
		return ErrStreamIdentity
	}
	return nil
}

func (p *Publisher) validateHistory(ctx context.Context, info *jetstream.StreamInfo, state WriterState, b streamBinding) (*jetstream.RawStreamMsg, error) {
	if info.State.LastSeq < b.LogSequence {
		return nil, ErrStreamIdentity
	}
	if state.PublishedSequence > 0 {
		anchor, err := p.stream.GetMsg(ctx, b.LogSequence)
		if err != nil {
			return nil, p.connectionError(err)
		}
		c, err := p.decodeMessage(anchor)
		if err != nil || c.Sequence != state.PublishedSequence || c.ID != b.ChangeID {
			return nil, ErrStreamIdentity
		}
	}
	if info.State.LastSeq == 0 {
		if state.PublishedSequence != 0 {
			return nil, ErrStreamIdentity
		}
		return nil, nil
	}
	tail, err := p.stream.GetMsg(ctx, info.State.LastSeq)
	if err != nil {
		return nil, p.connectionError(err)
	}
	c, err := p.decodeMessage(tail)
	if err != nil {
		return nil, ErrStreamIdentity
	}
	if c.Sequence == state.PublishedSequence {
		if c.ID != b.ChangeID {
			return nil, ErrStreamIdentity
		}
	} else if c.Sequence == state.PublishedSequence+1 {
		entry, found, err := p.w.NextOutbox()
		if err != nil {
			return nil, err
		}
		if !found || entry.Sequence != c.Sequence || entry.ID != c.ID || !bytes.Equal(entry.Payload, tail.Data) {
			return nil, ErrStreamIdentity
		}
	} else {
		return nil, ErrStreamIdentity
	}
	return tail, nil
}

func (p *Publisher) decodeMessage(m *jetstream.RawStreamMsg) (Change, error) {
	subject, _ := ChangeSubject(p.w.namespace)
	c, err := DecodeChange(m.Data, p.w.cfg)
	if err != nil || m.Subject != subject || m.Header.Get(jetstream.MsgIDHeader) != c.ID {
		return Change{}, ErrStreamIdentity
	}
	return c, nil
}

// PublishNext publishes at most the oldest outbox entry and checkpoints only a
// verified acknowledgement. Errors leave pending bytes for an immutable retry.
func (p *Publisher) PublishNext(ctx context.Context) (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return false, ErrPublisherClosed
	}
	if !p.nc.IsConnected() {
		return false, nats.ErrConnectionClosed
	}
	if !p.nc.AuthRequired() {
		return false, ErrPublisherAuth
	}
	ctx, cancel := context.WithTimeout(ctx, p.options.RequestTimeout)
	defer cancel()
	state, b, bound, err := p.w.publisherSnapshot()
	if err != nil {
		return false, err
	}
	if !bound {
		return false, ErrStreamIdentity
	}
	info, err := p.stream.Info(ctx)
	if err != nil {
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			return false, ErrStreamIdentity
		}
		return false, p.connectionError(err)
	}
	if err := p.checkStream(info, state, b, true); err != nil {
		return false, err
	}
	if _, err := p.validateHistory(ctx, info, state, b); err != nil {
		return false, err
	}
	entry, found, err := p.w.NextOutbox()
	if err != nil || !found {
		return false, err
	}
	change, err := DecodeChange(entry.Payload, p.w.cfg)
	if err != nil || change.ID != entry.ID || change.Sequence != state.PublishedSequence+1 {
		return false, ErrStreamIdentity
	}
	subject, _ := ChangeSubject(p.w.namespace)
	ack, err := p.js.Publish(ctx, subject, entry.Payload, jetstream.WithMsgID(entry.ID), jetstream.WithExpectLastSequence(info.State.LastSeq))
	if err != nil {
		return false, p.connectionError(err)
	}
	if ack.Stream != b.Name || ack.Sequence <= b.LogSequence || ack.Sequence > maxSequence {
		return false, ErrStreamIdentity
	}
	stored, err := p.stream.GetMsg(ctx, ack.Sequence)
	if err != nil {
		return false, p.connectionError(err)
	}
	if c, err := p.decodeMessage(stored); err != nil || c.ID != entry.ID || !bytes.Equal(stored.Data, entry.Payload) {
		return false, ErrStreamIdentity
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := p.w.acknowledgePublication(ctx, entry, ack.Sequence, b); err != nil {
		return false, err
	}
	return true, nil
}

func (p *Publisher) connectionError(err error) error { return errors.Join(err, p.nc.LastError()) }

// Close releases the connection and publisher slot. Close it before the Writer.
func (p *Publisher) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	p.closed = true
	if p.nc != nil {
		p.nc.Close()
	}
	p.w.mu.Lock()
	p.w.publisherAttached = false
	p.w.mu.Unlock()
}
