package jlite

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	zova "github.com/ata-sesli/zova/bindings/go"
)

var (
	ErrWriterClosed     = errors.New("writer is closed")
	ErrWriterFailed     = errors.New("writer failed; close and reopen before retrying")
	ErrWriterBusy       = errors.New("database already has a writer")
	ErrAdmissionFull    = errors.New("writer admission capacity exhausted")
	ErrOutboxFull       = errors.New("durable outbox capacity exhausted")
	ErrIdentityMismatch = errors.New("database identity or state does not match")
)

// WriterOptions bounds admitted work (including in-flight batches) and durable
// unpublished wire bytes. These are not limits on the physical SQLite file.
type WriterOptions struct {
	MaxPendingOperations int
	MaxPendingBytes      int
	MaxOutboxBytes       int64
}

// DefaultWriterOptions returns explicit bounded capacities.
func DefaultWriterOptions() WriterOptions {
	return WriterOptions{MaxPendingOperations: 1024, MaxPendingBytes: 4 << 20, MaxOutboxBytes: 64 << 20}
}

// Writer owns one owner namespace database, its admission queue and batch worker.
// All transaction calls and local reads are serialized on this handle.
type Writer struct {
	mu                sync.Mutex
	db                *zova.DB
	lock              *os.File
	cfg               Config
	namespace         string
	options           WriterOptions
	queue             chan *queuedWrite
	stop              chan struct{}
	done              chan struct{}
	closing           bool
	failed            error
	closeErr          error
	pendingCount      int
	pendingBytes      int
	publisherAttached bool
}

type writeOutcome struct {
	result WriteResult
	err    error
}
type queuedWrite struct {
	request WriteRequest
	bytes   int
	reply   chan writeOutcome
}

// OpenWriter creates or reopens a v0 owner database with WAL/FULL durability.
// Its advisory lock coordinates jlite writers on Unix, not distributed owners
// or programs bypassing jlite. It never changes persisted ownership.
func OpenWriter(path, namespace string, cfg Config, options WriterOptions) (*Writer, error) {
	a, err := cfg.assignment(namespace)
	if err != nil {
		return nil, err
	}
	if a.Owner != cfg.NodeID {
		return nil, ErrNotOwner
	}
	if options.MaxPendingOperations <= 0 || options.MaxPendingBytes < cfg.Limits.MaxChangeBytes || options.MaxOutboxBytes <= 0 {
		return nil, fmt.Errorf("%w: invalid writer capacities", ErrInvalidConfig)
	}
	// Own all configuration slices rather than retaining caller-owned memory.
	cfg.Nodes = append([]string(nil), cfg.Nodes...)
	cfg.Namespaces = append([]Assignment(nil), cfg.Namespaces...)
	for i := range cfg.Namespaces {
		cfg.Namespaces[i].Replicas = append([]string(nil), cfg.Namespaces[i].Replicas...)
	}
	db, lock, err := openWriterDB(path, namespace, cfg)
	if err != nil {
		return nil, err
	}
	w := &Writer{db: db, lock: lock, cfg: cfg, namespace: namespace, options: options,
		queue: make(chan *queuedWrite, options.MaxPendingOperations), stop: make(chan struct{}), done: make(chan struct{})}
	go w.run()
	return w, nil
}

// Write returns only after local commit, or an error. Cancellation after
// admission leaves the outcome unknown: retry the same request ID and payload.
// The caller must not modify input bytes concurrently with this call.
func (w *Writer) Write(ctx context.Context, r WriteRequest) (WriteResult, error) {
	q, err := w.admit(ctx, r)
	if err != nil {
		return WriteResult{}, err
	}
	select {
	case reply := <-q.reply:
		return reply.result, reply.err
	case <-ctx.Done():
		return WriteResult{}, ctx.Err()
	}
}

func (w *Writer) admit(ctx context.Context, r WriteRequest) (*queuedWrite, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if r.Namespace != w.namespace {
		return nil, ErrUnknownNamespace
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.available(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if w.pendingCount >= w.options.MaxPendingOperations {
		return nil, ErrAdmissionFull
	}
	// Serialize preflight allocation as well as admission; rejected concurrent
	// callers must not each allocate a maximum-sized wire message first.
	change, err := w.cfg.NewChange(r, maxSequence)
	if err != nil {
		return nil, err
	}
	wire, err := change.Encode(w.cfg)
	if err != nil {
		return nil, err
	}
	if len(wire) > w.options.MaxPendingBytes-w.pendingBytes {
		return nil, ErrAdmissionFull
	}
	r.Value = normalizedValue(r.Value)
	q := &queuedWrite{request: r, bytes: len(wire), reply: make(chan writeOutcome, 1)}
	w.pendingCount++
	w.pendingBytes += q.bytes
	w.queue <- q
	return q, nil
}

func (w *Writer) available() error {
	if w.closing {
		return ErrWriterClosed
	}
	if w.failed != nil {
		return errors.Join(ErrWriterFailed, w.failed)
	}
	return nil
}

// Close stops admission, drains accepted work and closes the database and lock.
// It is safe to call more than once. No goroutine remains owned by the writer.
func (w *Writer) Close() error {
	w.mu.Lock()
	if !w.closing {
		w.closing = true
		close(w.stop)
	}
	w.mu.Unlock()
	<-w.done
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.db != nil {
		w.closeErr = errors.Join(w.db.Close(), w.lock.Close())
		w.db, w.lock = nil, nil
	}
	return w.closeErr
}

func (w *Writer) run() {
	defer close(w.done)
	var carry *queuedWrite
	for {
		first := carry
		carry = nil
		if first == nil {
			select {
			case first = <-w.queue:
			case <-w.stop:
				select {
				case first = <-w.queue:
				default:
					return
				}
			}
		}
		batch := []*queuedWrite{first}
		bytes := first.bytes
		timer := time.NewTimer(w.cfg.Limits.BatchWait)
	collect:
		for len(batch) < w.cfg.Limits.MaxBatchOperations {
			select {
			case next := <-w.queue:
				if next.bytes > w.cfg.Limits.MaxBatchBytes-bytes {
					carry = next
					break collect
				}
				batch = append(batch, next)
				bytes += next.bytes
			case <-timer.C:
				break collect
			case <-w.stop:
				break collect
			}
		}
		timer.Stop()
		w.mu.Lock()
		outcomes := w.applyBatch(batch)
		for i, q := range batch {
			w.pendingCount--
			w.pendingBytes -= q.bytes
			q.reply <- outcomes[i]
		}
		w.mu.Unlock()
	}
}

func (w *Writer) applyBatch(batch []*queuedWrite) []writeOutcome {
	outcomes := make([]writeOutcome, len(batch))
	var err error
	if w.failed != nil {
		err = w.failed
	} else {
		err = w.db.BeginImmediate()
	}
	if err == nil {
		err = w.mutateBatch(batch, outcomes)
	}
	if err == nil {
		err = w.db.Commit()
	}
	if err != nil {
		if w.failed == nil {
			err = errors.Join(err, w.db.Rollback())
			w.failed = err
		}
		for i := range outcomes {
			outcomes[i] = writeOutcome{err: errors.Join(ErrWriterFailed, err)}
		}
	}
	return outcomes
}

func (w *Writer) mutateBatch(batch []*queuedWrite, outcomes []writeOutcome) error {
	state, err := readWriterState(w.db)
	if err != nil {
		return err
	}
	for i, q := range batch {
		r := q.request
		stored, found, err := findStoredWrite(w.db, w.namespace, r.RequestID)
		if err != nil {
			return err
		}
		if found {
			outcomes[i].result, outcomes[i].err = RetryResult(r, stored, w.cfg.Limits)
			continue
		}
		if state.Sequence == maxSequence {
			outcomes[i].err = ErrInvalidChange
			continue
		}
		change, err := w.cfg.NewChange(r, state.Sequence+1)
		if err != nil {
			outcomes[i].err = err
			continue
		}
		payload, err := change.Encode(w.cfg)
		if err != nil {
			outcomes[i].err = err
			continue
		}
		if int64(len(payload)) > w.options.MaxOutboxBytes-state.OutboxBytes {
			outcomes[i].err = ErrOutboxFull
			continue
		}
		if r.Operation == Put {
			err = executeSQL(w.db, "INSERT INTO jlite_records(key,value) VALUES(?,coalesce(?,x'')) ON CONFLICT(key) DO UPDATE SET value=excluded.value", r.Key, r.Value)
		} else {
			err = executeSQL(w.db, "DELETE FROM jlite_records WHERE key=?", r.Key)
		}
		if err != nil {
			return err
		}
		fingerprint, err := r.Fingerprint(w.cfg.Limits)
		if err != nil {
			return err
		}
		seq := int64(change.Sequence)
		if err := executeSQL(w.db, "INSERT INTO jlite_requests VALUES(?,?,?)", r.RequestID, fingerprint, seq); err != nil {
			return err
		}
		if err := executeSQL(w.db, "INSERT INTO jlite_outbox VALUES(?,?,?)", seq, change.ID, payload); err != nil {
			return err
		}
		state.Sequence++
		state.OutboxBytes += int64(len(payload))
		outcomes[i].result = WriteResult{Namespace: w.namespace, Sequence: state.Sequence}
	}
	return executeSQL(w.db, "UPDATE jlite_state SET sequence=?,outbox_bytes=? WHERE singleton=1", int64(state.Sequence), state.OutboxBytes)
}

// Get reads a record and its applied position in one SQL snapshot.
func (w *Writer) Get(r ReadRequest) (result ReadResult, err error) {
	if r.Namespace != w.namespace {
		return result, ErrUnknownNamespace
	}
	if err := w.cfg.ValidateRead(r); err != nil {
		return result, err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.available(); err != nil {
		return result, err
	}
	s, err := prepareSQL(w.db, `SELECT s.sequence,r.value,r.key IS NOT NULL FROM jlite_state s LEFT JOIN jlite_records r ON r.key=? WHERE s.singleton=1`, r.Key)
	if err != nil {
		return result, err
	}
	defer func() { err = errors.Join(err, s.Close()) }()
	step, err := s.Step()
	if err != nil {
		return result, err
	}
	if step != zova.StepRow {
		return result, ErrIdentityMismatch
	}
	seq, err := s.ColumnInt64(0)
	if err != nil {
		return result, err
	}
	found, err := s.ColumnInt64(2)
	if err != nil {
		return result, err
	}
	result.AppliedSequence, result.Found = uint64(seq), found == 1
	if result.Found {
		result.Value, _, err = s.ColumnBlob(1)
	}
	return result, err
}

// State returns durable progress and bounded in-memory admission usage.
func (w *Writer) State() (WriterState, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.available(); err != nil {
		return WriterState{}, err
	}
	s, err := readWriterState(w.db)
	s.PendingOperations, s.PendingBytes = w.pendingCount, w.pendingBytes
	return s, err
}

// NextOutbox reads the oldest unpublished entry without modifying it.
func (w *Writer) NextOutbox() (entry OutboxEntry, found bool, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.available(); err != nil {
		return entry, false, err
	}
	s, err := prepareSQL(w.db, "SELECT sequence,id,payload FROM jlite_outbox ORDER BY sequence LIMIT 1")
	if err != nil {
		return entry, false, err
	}
	defer func() { err = errors.Join(err, s.Close()) }()
	step, err := s.Step()
	if err != nil || step == zova.StepDone {
		return entry, false, err
	}
	seq, err := s.ColumnInt64(0)
	if err != nil {
		return entry, false, err
	}
	entry.Sequence = uint64(seq)
	entry.ID, _, err = s.ColumnText(1)
	if err != nil {
		return entry, false, err
	}
	entry.Payload, _, err = s.ColumnBlob(2)
	return entry, true, err
}
