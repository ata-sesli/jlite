package jlite

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	zova "github.com/ata-sesli/zova/bindings/go"
)

func openTestWriter(t *testing.T, path string, cfg Config, options WriterOptions) *Writer {
	t.Helper()
	w, err := OpenWriter(path, "project:alpha", cfg, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := w.Close(); err != nil {
			t.Error(err)
		}
	})
	return w
}

func TestWriterSameBatchDuplicatesAndConflicts(t *testing.T) {
	w := openTestWriter(t, filepath.Join(t.TempDir(), "batch.zova"), testConfig(), DefaultWriterOptions())
	r := testWrite()
	conflict := r
	conflict.Value = []byte("different")
	other := r
	other.RequestID, other.Key = "req-2", "other"
	w.mu.Lock()
	results := w.applyBatch([]*queuedWrite{{request: r}, {request: r}, {request: conflict}, {request: other}})
	w.mu.Unlock()
	if results[0].err != nil || results[1].err != nil || results[0].result != results[1].result ||
		!errors.Is(results[2].err, ErrRequestConflict) || results[3].err != nil || results[3].result.Sequence != 2 {
		t.Fatalf("same batch results = %+v", results)
	}
	s, err := w.State()
	if err != nil || s.Sequence != 2 || s.OutboxCount != 2 {
		t.Fatalf("same-batch duplicates changed state: %+v, %v", s, err)
	}
}

func TestWriterRollsBackWholeBatchOnStorageError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rollback.zova")
	w := openTestWriter(t, path, testConfig(), DefaultWriterOptions())
	w.mu.Lock()
	err := w.db.Exec(`CREATE TRIGGER fail_second BEFORE INSERT ON jlite_outbox WHEN NEW.sequence=2 BEGIN SELECT RAISE(ABORT,'storage failure'); END;`)
	if err != nil {
		w.mu.Unlock()
		t.Fatal(err)
	}
	r := testWrite()
	second := r
	second.RequestID, second.Key = "req-2", "other"
	results := w.applyBatch([]*queuedWrite{{request: r}, {request: second}})
	w.mu.Unlock()
	for _, result := range results {
		if result.result.Sequence != 0 || !errors.Is(result.err, ErrWriterFailed) {
			t.Fatalf("rollback returned success: %+v", result)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := zova.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, table := range []string{"jlite_records", "jlite_requests", "jlite_outbox"} {
		if got := queryInt(t, db, "SELECT count(*) FROM "+table); got != 0 {
			t.Fatalf("rollback left %d %s rows", got, table)
		}
	}
	if got := queryInt(t, db, "SELECT outbox_bytes FROM jlite_state"); got != 0 {
		t.Fatalf("rollback left outbox bytes %d", got)
	}
}

func TestWriterCopiesConfigAndRejectsBadOptions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "copied.zova")
	cfg := testConfig()
	options := DefaultWriterOptions()
	options.MaxOutboxBytes = 0
	if _, err := OpenWriter(path, "project:alpha", cfg, options); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("invalid capacity: %v", err)
	}
	w := openTestWriter(t, path, cfg, DefaultWriterOptions())
	cfg.Nodes[0] = "changed"
	cfg.Namespaces[0].Owner = "other"
	cfg.Namespaces[0].Replicas[0] = "other"
	if _, err := w.Write(context.Background(), testWrite()); err != nil {
		t.Fatalf("writer retained caller configuration: %v", err)
	}
}

func waitPending(t *testing.T, w *Writer, count int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		s, err := w.State()
		if err != nil {
			t.Fatal(err)
		}
		if s.PendingOperations == count {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("admission did not reach expected pending count")
}

func TestWriterAdmissionCancellationAndCloseDrain(t *testing.T) {
	for _, mode := range []string{"count", "bytes"} {
		t.Run(mode, func(t *testing.T) {
			cfg := testConfig()
			cfg.Limits.BatchWait = time.Hour
			cfg.Limits.MaxChangeBytes = 256
			opts := DefaultWriterOptions()
			if mode == "count" {
				opts.MaxPendingOperations = 1
			} else {
				opts.MaxPendingBytes = 256
			}
			path := filepath.Join(t.TempDir(), "admission.zova")
			w := openTestWriter(t, path, cfg, opts)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			response := make(chan error, 1)
			go func() { _, err := w.Write(ctx, testWrite()); response <- err }()
			waitPending(t, w, 1)
			r := testWrite()
			r.RequestID = "req-2"
			if _, err := w.Write(context.Background(), r); !errors.Is(err, ErrAdmissionFull) {
				t.Fatalf("unbounded admission: %v", err)
			}
			cancel()
			if err := <-response; !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled response: %v", err)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := w.Write(context.Background(), testWrite()); !errors.Is(err, ErrWriterClosed) {
				t.Fatalf("closed writer: %v", err)
			}
			// The canceled response does not revoke an admitted write.
			cfg.Limits.BatchWait = time.Millisecond
			w = openTestWriter(t, path, cfg, opts)
			result, err := w.Write(context.Background(), testWrite())
			if err != nil || result.Sequence != 1 {
				t.Fatalf("lost response retry: %+v %v", result, err)
			}
			s, err := w.State()
			if err != nil || s.OutboxCount != 1 || s.PendingOperations != 0 || s.PendingBytes != 0 {
				t.Fatalf("drain state: %+v %v", s, err)
			}
		})
	}
}

func TestWriterBatchesRespectOperationAndByteBounds(t *testing.T) {
	for _, mode := range []string{"count", "bytes"} {
		t.Run(mode, func(t *testing.T) {
			cfg := testConfig()
			cfg.Limits.BatchWait = 100 * time.Millisecond
			if mode == "count" {
				cfg.Limits.MaxBatchOperations = 2
			} else {
				cfg.Limits.MaxChangeBytes = 256
				cfg.Limits.MaxBatchBytes = 512
			}
			w := openTestWriter(t, filepath.Join(t.TempDir(), "batches.zova"), cfg, DefaultWriterOptions())
			w.mu.Lock()
			err := w.db.Exec(`CREATE TABLE batch_sizes(operations INTEGER, bytes INTEGER);
			CREATE TRIGGER capture_batch AFTER UPDATE OF sequence ON jlite_state WHEN NEW.sequence>OLD.sequence BEGIN
			INSERT INTO batch_sizes VALUES(NEW.sequence-OLD.sequence,(SELECT sum(length(payload)) FROM jlite_outbox WHERE sequence>OLD.sequence AND sequence<=NEW.sequence)); END;`)
			w.mu.Unlock()
			if err != nil {
				t.Fatal(err)
			}
			responses := make(chan writeOutcome, 6)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			for i := range 6 {
				go func() {
					r := testWrite()
					r.RequestID = fmt.Sprintf("req-%d", i)
					r.Key = fmt.Sprintf("key-%d", i)
					result, err := w.Write(ctx, r)
					responses <- writeOutcome{result, err}
				}()
			}
			seen := make(map[uint64]bool)
			for range 6 {
				o := <-responses
				if o.err != nil {
					t.Fatal(o.err)
				}
				if seen[o.result.Sequence] {
					t.Fatal("duplicate sequence")
				}
				seen[o.result.Sequence] = true
			}
			w.mu.Lock()
			maxOps := queryInt(t, w.db, "SELECT max(operations) FROM batch_sizes")
			maxBytes := queryInt(t, w.db, "SELECT max(bytes) FROM batch_sizes")
			batches := queryInt(t, w.db, "SELECT count(*) FROM batch_sizes")
			w.mu.Unlock()
			if maxOps > int64(cfg.Limits.MaxBatchOperations) || maxBytes > int64(cfg.Limits.MaxBatchBytes) || batches >= 6 {
				t.Fatalf("batch bounds/grouping: ops=%d bytes=%d transactions=%d", maxOps, maxBytes, batches)
			}
		})
	}
}

func TestWriterSingleHandleAndUnownedDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "single.zova")
	w := openTestWriter(t, path, testConfig(), DefaultWriterOptions())
	if other, err := OpenWriter(path, "project:alpha", testConfig(), DefaultWriterOptions()); !errors.Is(err, ErrWriterBusy) {
		if other != nil {
			_ = other.Close()
		}
		t.Fatalf("second writer: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	w = openTestWriter(t, path, testConfig(), DefaultWriterOptions())
	path = filepath.Join(t.TempDir(), "unowned.zova")
	db, err := zova.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if other, err := OpenWriter(path, "project:alpha", testConfig(), DefaultWriterOptions()); !errors.Is(err, ErrIdentityMismatch) {
		if other != nil {
			_ = other.Close()
		}
		t.Fatalf("unowned database: %v", err)
	}
}

func TestWriterCommitRetryAndReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "owner.zova")
	cfg := testConfig()
	w := openTestWriter(t, path, cfg, DefaultWriterOptions())
	r := testWrite()
	result, err := w.Write(context.Background(), r)
	if err != nil || result.Sequence != 1 {
		t.Fatalf("write = %+v, %v", result, err)
	}
	read, err := w.Get(ReadRequest{Namespace: r.Namespace, Key: r.Key})
	if err != nil || !read.Found || !bytes.Equal(read.Value, r.Value) || read.AppliedSequence != 1 {
		t.Fatalf("read = %+v, %v", read, err)
	}
	entry, found, err := w.NextOutbox()
	if err != nil || !found || entry.Sequence != 1 {
		t.Fatalf("outbox = %+v, %v", entry, err)
	}
	change, err := DecodeChange(entry.Payload, cfg)
	if err != nil || change.ID != entry.ID || change.Request.RequestID != r.RequestID {
		t.Fatalf("change = %+v, %v", change, err)
	}
	before, err := w.State()
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	w = openTestWriter(t, path, cfg, DefaultWriterOptions())
	retry, err := w.Write(context.Background(), r)
	if err != nil || retry != result {
		t.Fatalf("retry = %+v, %v", retry, err)
	}
	after, err := w.State()
	if err != nil || before.DatabaseID != after.DatabaseID || after.Sequence != 1 || after.OutboxCount != 1 || after.OutboxBytes != int64(len(entry.Payload)) {
		t.Fatalf("reopened state = %+v, %v", after, err)
	}
	r.Value = []byte("conflict")
	if _, err := w.Write(context.Background(), r); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("conflict = %v", err)
	}
	entry2, _, err := w.NextOutbox()
	if err != nil || !bytes.Equal(entry.Payload, entry2.Payload) {
		t.Fatal("retry changed immutable outbox payload")
	}
}

func TestWriterDeleteEmptyValueAndInvalidIsolation(t *testing.T) {
	w := openTestWriter(t, filepath.Join(t.TempDir(), "owner.zova"), testConfig(), DefaultWriterOptions())
	r := testWrite()
	r.Value = nil
	if _, err := w.Write(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	got, err := w.Get(ReadRequest{Namespace: r.Namespace, Key: r.Key})
	if err != nil || !got.Found || len(got.Value) != 0 {
		t.Fatalf("empty put = %+v, %v", got, err)
	}
	r.RequestID = "bad"
	r.Operation = "unsupported"
	if _, err := w.Write(context.Background(), r); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("invalid = %v", err)
	}
	r.Operation, r.RequestID = Delete, "delete-1"
	result, err := w.Write(context.Background(), r)
	if err != nil || result.Sequence != 2 {
		t.Fatalf("delete = %+v, %v", result, err)
	}
	retry, err := w.Write(context.Background(), r)
	if err != nil || retry != result {
		t.Fatalf("delete retry = %+v, %v", retry, err)
	}
	got, err = w.Get(ReadRequest{Namespace: r.Namespace, Key: r.Key})
	if err != nil || got.Found || got.AppliedSequence != 2 {
		t.Fatalf("deleted read = %+v, %v", got, err)
	}
}

func TestWriterOutboxLimitPersistsAndAllowsRetries(t *testing.T) {
	cfg := testConfig()
	r := testWrite()
	change, _ := cfg.NewChange(r, 1)
	payload, _ := change.Encode(cfg)
	opts := DefaultWriterOptions()
	opts.MaxOutboxBytes = int64(len(payload))
	path := filepath.Join(t.TempDir(), "limited.zova")
	w := openTestWriter(t, path, cfg, opts)
	first, err := w.Write(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	r.RequestID = "req-2"
	if _, err := w.Write(context.Background(), r); !errors.Is(err, ErrOutboxFull) {
		t.Fatalf("full outbox = %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	w = openTestWriter(t, path, cfg, opts)
	if _, err := w.Write(context.Background(), r); !errors.Is(err, ErrOutboxFull) {
		t.Fatalf("reopened full outbox = %v", err)
	}
	r.RequestID = "req-1"
	if result, err := w.Write(context.Background(), r); err != nil || result != first {
		t.Fatalf("full outbox retry = %+v, %v", result, err)
	}
	s, err := w.State()
	if err != nil || s.Sequence != 1 || s.OutboxCount != 1 {
		t.Fatalf("failed admission mutated state: %+v %v", s, err)
	}
}

func TestWriterRejectsReopenIdentityMismatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "identity.zova")
	cfg := testConfig()
	w := openTestWriter(t, path, cfg, DefaultWriterOptions())
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.NodeID = "other"
	cfg.Namespaces[0].Owner = "other"
	if _, err := OpenWriter(path, "project:alpha", cfg, DefaultWriterOptions()); !errors.Is(err, ErrIdentityMismatch) {
		t.Fatalf("wrong node = %v", err)
	}
	cfg = testConfig()
	cfg.Namespaces[0].Namespace = "different"
	if _, err := OpenWriter(path, "different", cfg, DefaultWriterOptions()); !errors.Is(err, ErrIdentityMismatch) {
		t.Fatalf("wrong namespace = %v", err)
	}
	cfg = testConfig()
	cfg.NodeID = "replica"
	if _, err := OpenWriter(path, "project:alpha", cfg, DefaultWriterOptions()); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("replica writer = %v", err)
	}
}

func TestWriterCommitFailureRollsBackAndRequiresReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "failure.zova")
	w := openTestWriter(t, path, testConfig(), DefaultWriterOptions())
	// Real deferred constraint: the mutations succeed, but COMMIT fails.
	w.mu.Lock()
	err := w.db.Exec(`CREATE TABLE failure_parent(id INTEGER PRIMARY KEY);
		CREATE TABLE failure_child(id INTEGER REFERENCES failure_parent(id) DEFERRABLE INITIALLY DEFERRED);
		CREATE TRIGGER fail_commit AFTER INSERT ON jlite_records BEGIN INSERT INTO failure_child VALUES(99); END;`)
	w.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if result, err := w.Write(context.Background(), testWrite()); !errors.Is(err, ErrWriterFailed) || result.Sequence != 0 {
		t.Fatalf("commit failure = %+v %v", result, err)
	}
	if _, err := w.Write(context.Background(), testWrite()); !errors.Is(err, ErrWriterFailed) {
		t.Fatalf("failed writer accepted work: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := zova.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"jlite_records", "jlite_requests", "jlite_outbox", "failure_child"} {
		if got := queryInt(t, db, "SELECT count(*) FROM "+table); got != 0 {
			t.Fatalf("rollback left %d rows in %s", got, table)
		}
	}
	if got := queryInt(t, db, "SELECT sequence FROM jlite_state"); got != 0 {
		t.Fatalf("rollback left sequence %d", got)
	}
	if err := db.Exec("DROP TRIGGER fail_commit"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	w = openTestWriter(t, path, testConfig(), DefaultWriterOptions())
	if result, err := w.Write(context.Background(), testWrite()); err != nil || result.Sequence != 1 {
		t.Fatalf("recovered write = %+v %v", result, err)
	}
}
