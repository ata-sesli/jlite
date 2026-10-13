package jlite

import (
	"bytes"
	"context"
	"errors"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type publication struct {
	Entry       OutboxEntry
	LogSequence uint64
}

type publicationHistory struct {
	Tail        *jetstream.RawStreamMsg
	Recovered   []publication
	LastLogical uint64
}

func publicationWindow(l Limits) int {
	return min(l.MaxBatchOperations, l.MaxBatchBytes/l.MaxChangeBytes)
}

func (p *Publisher) historyMessage(ctx context.Context, sequence uint64) (*jetstream.RawStreamMsg, error) {
	opCtx, cancel := context.WithTimeout(ctx, p.options.RequestTimeout)
	defer cancel()
	msg, err := p.stream.GetMsg(opCtx, sequence)
	if err != nil {
		return nil, p.connectionError(err)
	}
	return msg, nil
}

// Confirm the retained suffix through a publication ACK, rather than assuming
// a read-back proves the broker's durability policy has completed. Retrying only
// the verified tail preserves nondecreasing logical order even after dedup expiry.
func (p *Publisher) checkpointRecovered(ctx context.Context, info *jetstream.StreamInfo, b streamBinding, history publicationHistory) error {
	if len(history.Recovered) == 0 || history.Tail == nil {
		return ErrStreamIdentity
	}
	tail, err := p.decodeMessage(history.Tail)
	if err != nil {
		return ErrStreamIdentity
	}
	opCtx, cancel := context.WithTimeout(ctx, p.options.RequestTimeout)
	ack, err := p.js.Publish(opCtx, history.Tail.Subject, history.Tail.Data, jetstream.WithMsgID(tail.ID), jetstream.WithExpectLastSequence(info.State.LastSeq), jetstream.WithRetryAttempts(0))
	cancel()
	if err != nil {
		return p.connectionError(err)
	}
	if ack == nil || ack.Stream != b.Name || ack.Sequence <= b.LogSequence || ack.Sequence > info.State.LastSeq+1 || ack.Sequence > maxSequence {
		return ErrStreamIdentity
	}
	stored, err := p.historyMessage(ctx, ack.Sequence)
	if err != nil {
		return err
	}
	if c, err := p.decodeMessage(stored); err != nil || c.ID != tail.ID || !bytes.Equal(stored.Data, history.Tail.Data) {
		return ErrStreamIdentity
	}
	last := &history.Recovered[len(history.Recovered)-1]
	if last.Entry.Sequence == history.LastLogical {
		last.LogSequence = ack.Sequence
	} else if last.LogSequence >= ack.Sequence {
		return ErrStreamIdentity
	}
	return p.w.acknowledgePublications(ctx, history.Recovered, b)
}

// inspectHistory verifies every uncheckpointed physical message against the
// immutable outbox. Only a bounded leading logical prefix is retained in memory.
// Physical duplicates must repeat the latest logical change, as produced by
// single-entry retries and recovery confirmation after deduplication expiry.
func (p *Publisher) inspectHistory(ctx context.Context, info *jetstream.StreamInfo, state WriterState, b streamBinding, maxOps int) (publicationHistory, error) {
	h := publicationHistory{LastLogical: state.PublishedSequence}
	if info.State.LastSeq < b.LogSequence || info.State.LastSeq > maxSequence {
		return h, ErrStreamIdentity
	}
	previousID := b.ChangeID
	var previousData []byte
	if state.PublishedSequence > 0 {
		anchor, err := p.historyMessage(ctx, b.LogSequence)
		if err != nil {
			return h, err
		}
		c, err := p.decodeMessage(anchor)
		if err != nil || c.Sequence != state.PublishedSequence || c.ID != b.ChangeID {
			return h, ErrStreamIdentity
		}
		h.Tail, previousData = anchor, anchor.Data
	}
	bytesCollected, collecting := 0, maxOps > 0
	for seq := b.LogSequence + 1; seq <= info.State.LastSeq; seq++ {
		if err := ctx.Err(); err != nil {
			return h, err
		}
		msg, err := p.historyMessage(ctx, seq)
		if err != nil {
			return h, err
		}
		c, err := p.decodeMessage(msg)
		if err != nil {
			return h, ErrStreamIdentity
		}
		switch {
		case c.Sequence == h.LastLogical && c.Sequence > 0:
			if c.ID != previousID || !bytes.Equal(msg.Data, previousData) {
				return h, ErrStreamIdentity
			}
			if n := len(h.Recovered); n > 0 && h.Recovered[n-1].Entry.Sequence == c.Sequence {
				h.Recovered[n-1].LogSequence = seq
			}
		case c.Sequence == h.LastLogical+1:
			entry, found, err := p.w.outboxEntry(c.Sequence)
			if err != nil {
				return h, err
			}
			if !found || entry.ID != c.ID || !bytes.Equal(entry.Payload, msg.Data) {
				return h, ErrStreamIdentity
			}
			h.LastLogical, previousID, previousData = c.Sequence, c.ID, entry.Payload
			if collecting && (len(h.Recovered) == maxOps || len(entry.Payload) > p.w.cfg.Limits.MaxBatchBytes-bytesCollected) {
				collecting = false
			}
			if collecting {
				h.Recovered = append(h.Recovered, publication{entry, seq})
				bytesCollected += len(entry.Payload)
			}
		default:
			return h, ErrStreamIdentity
		}
		h.Tail = msg
	}
	return h, nil
}

// PublishBatch publishes a bounded, ordered outbox prefix and commits all its
// verified acknowledgements in one transaction. Count and bytes use namespace
// batch limits; only currently available entries are collected, without linger.
// A previously accepted suffix is verified and checkpointed before new sends.
// RequestTimeout bounds individual broker/ACK waits, not the whole batch; the
// caller context bounds total duration. Errors return zero and retain the entire
// uncheckpointed prefix. After an uncertain async failure, reconnect the publisher.
func (p *Publisher) PublishBatch(ctx context.Context) (count int, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return 0, ErrPublisherClosed
	}
	if !p.nc.IsConnected() {
		return 0, nats.ErrConnectionClosed
	}
	if !p.nc.AuthRequired() {
		return 0, ErrPublisherAuth
	}
	checkCtx, cancel := context.WithTimeout(ctx, p.options.RequestTimeout)
	state, b, bound, err := p.w.publisherSnapshot()
	if err != nil {
		cancel()
		return 0, err
	}
	if !bound {
		cancel()
		return 0, ErrStreamIdentity
	}
	info, err := p.stream.Info(checkCtx)
	if err != nil {
		cancel()
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			return 0, ErrStreamIdentity
		}
		return 0, p.connectionError(err)
	}
	if err := p.checkStream(info, state, b, true); err != nil {
		cancel()
		return 0, err
	}
	cancel()
	history, err := p.inspectHistory(ctx, info, state, b, p.w.cfg.Limits.MaxBatchOperations)
	if err != nil {
		return 0, err
	}
	if len(history.Recovered) > 0 {
		if err := p.checkpointRecovered(ctx, info, b, history); err != nil {
			return 0, err
		}
		return len(history.Recovered), nil
	}
	entries, err := p.w.outboxBatch()
	if err != nil || len(entries) == 0 {
		return 0, err
	}
	if uint64(len(entries)) > maxSequence-info.State.LastSeq {
		return 0, ErrStreamIdentity
	}
	for i, entry := range entries {
		c, err := DecodeChange(entry.Payload, p.w.cfg)
		if err != nil || c.ID != entry.ID || c.Sequence != state.PublishedSequence+uint64(i)+1 {
			return 0, ErrStreamIdentity
		}
	}
	// One connection and explicit expected physical sequences preserve append
	// order. Disable SDK retries: ambiguous results are reconciled from retained
	// history, never by resending an uncontrolled async request.
	started := false
	defer func() {
		if err != nil && started {
			p.nc.Close()
			p.js.CleanupPublisher()
		}
	}()
	window := publicationWindow(p.w.cfg.Limits)
	verified := make([]publication, 0, len(entries))
	subject, _ := ChangeSubject(p.w.namespace)
	// Each window is acknowledged and read back before the next is sent.
	for offset := 0; offset < len(entries); offset += window {
		end := min(offset+window, len(entries))
		futures := make([]jetstream.PubAckFuture, 0, end-offset)
		for i, entry := range entries[offset:end] {
			if err = ctx.Err(); err != nil {
				return 0, err
			}
			started = true
			future, e := p.js.PublishAsync(subject, entry.Payload, jetstream.WithMsgID(entry.ID), jetstream.WithExpectLastSequence(info.State.LastSeq+uint64(offset+i)), jetstream.WithRetryAttempts(0), jetstream.WithStallWait(p.options.RequestTimeout))
			if e != nil {
				return 0, e
			}
			futures = append(futures, future)
		}
		for i, future := range futures {
			timer := time.NewTimer(p.options.RequestTimeout)
			var ack *jetstream.PubAck
			select {
			case ack = <-future.Ok():
			case err = <-future.Err():
			case <-ctx.Done():
				err = ctx.Err()
			case <-timer.C:
				err = nats.ErrTimeout
			}
			timer.Stop()
			if err != nil {
				return 0, p.connectionError(err)
			}
			if ack == nil || ack.Stream != b.Name || ack.Sequence != info.State.LastSeq+uint64(offset+i)+1 {
				return 0, ErrStreamIdentity
			}
			verified = append(verified, publication{entries[offset+i], ack.Sequence})
		}
		for _, item := range verified[offset:end] {
			msg, e := p.historyMessage(ctx, item.LogSequence)
			if e != nil {
				return 0, p.connectionError(e)
			}
			if c, e := p.decodeMessage(msg); e != nil || c.ID != item.Entry.ID || !bytes.Equal(msg.Data, item.Entry.Payload) {
				return 0, ErrStreamIdentity
			}
		}
	}
	if err = p.w.acknowledgePublications(ctx, verified, b); err != nil {
		return 0, err
	}
	return len(verified), nil
}
