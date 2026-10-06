package jlite

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	zova "github.com/ata-sesli/zova/bindings/go"
	"golang.org/x/sys/unix"
)

const writerSchema = `
CREATE TABLE jlite_state (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1), version INTEGER NOT NULL,
 namespace TEXT NOT NULL, node TEXT NOT NULL, owner TEXT NOT NULL, database_id TEXT NOT NULL,
 sequence INTEGER NOT NULL CHECK(sequence>=0), published INTEGER NOT NULL CHECK(published>=0 AND published<=sequence),
 outbox_bytes INTEGER NOT NULL CHECK(outbox_bytes>=0));
CREATE TABLE jlite_records (key TEXT PRIMARY KEY, value BLOB NOT NULL);
CREATE TABLE jlite_requests (request_id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL, sequence INTEGER NOT NULL CHECK(sequence>0));
CREATE TABLE jlite_outbox (sequence INTEGER PRIMARY KEY CHECK(sequence>0), id TEXT NOT NULL UNIQUE, payload BLOB NOT NULL);`

// WriterState reports durable namespace progress and current admission usage.
type WriterState struct {
	DatabaseID        string
	Namespace         string
	NodeID            string
	Owner             string
	Sequence          uint64
	PublishedSequence uint64
	OutboxBytes       int64
	OutboxCount       int64
	PendingOperations int
	PendingBytes      int
}

// OutboxEntry contains the exact immutable bytes to publish after local commit.
type OutboxEntry struct {
	Sequence uint64
	ID       string
	Payload  []byte
}

func openWriterDB(path, namespace string, cfg Config) (*zova.DB, *os.File, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil, nil, err
	}
	if resolved, err := filepath.EvalSymlinks(absolute); err == nil {
		absolute = resolved
	} else {
		parent, err := filepath.EvalSymlinks(filepath.Dir(absolute))
		if err != nil {
			return nil, nil, err
		}
		absolute = filepath.Join(parent, filepath.Base(absolute))
	}
	lock, err := os.OpenFile(absolute+".lock", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, nil, err
	}
	if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = lock.Close()
		return nil, nil, fmt.Errorf("%w: %v", ErrWriterBusy, err)
	}
	var db *zova.DB
	_, statErr := os.Stat(absolute)
	created := errors.Is(statErr, os.ErrNotExist)
	if statErr != nil && !created {
		err = statErr
	} else if created {
		db, err = zova.Create(absolute)
	} else {
		db, err = zova.Open(absolute)
	}
	if err == nil {
		err = db.SetBusyTimeout(1000)
	}
	if err == nil && created {
		err = db.Exec("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL")
	}
	if err == nil && created {
		var id [16]byte
		if _, err = rand.Read(id[:]); err == nil {
			err = db.BeginImmediate()
		}
		if err == nil {
			err = db.Exec(writerSchema)
		}
		if err == nil {
			err = executeSQL(db, "INSERT INTO jlite_state VALUES(1,1,?,?,?,?,0,0,0)", namespace, cfg.NodeID, cfg.NodeID, hex.EncodeToString(id[:]))
		}
		if err == nil {
			err = db.Commit()
		}
		if err != nil {
			_ = db.Rollback()
		}
	}
	if err == nil {
		var s WriterState
		s, err = readWriterState(db)
		if err == nil && (s.Namespace != namespace || s.NodeID != cfg.NodeID || s.Owner != cfg.NodeID) {
			err = ErrIdentityMismatch
		}
		if err == nil {
			err = verifyOutboxState(db, s)
		}
	}
	if err == nil && !created {
		err = db.Exec("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL")
	}
	if err != nil {
		if db != nil {
			_ = db.Close()
		}
		_ = lock.Close()
		return nil, nil, err
	}
	return db, lock, nil
}

func prepareSQL(db *zova.DB, sql string, args ...any) (*zova.Stmt, error) {
	s, err := db.Prepare(sql)
	if err != nil {
		return nil, err
	}
	for i, arg := range args {
		switch v := arg.(type) {
		case string:
			err = s.BindText(i+1, v)
		case []byte:
			err = s.BindBlob(i+1, v)
		case int64:
			err = s.BindInt64(i+1, v)
		default:
			err = errors.New("unsupported SQL argument")
		}
		if err != nil {
			_ = s.Close()
			return nil, err
		}
	}
	return s, nil
}

func executeSQL(db *zova.DB, sql string, args ...any) (err error) {
	s, err := prepareSQL(db, sql, args...)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, s.Close()) }()
	step, err := s.Step()
	if err == nil && step != zova.StepDone {
		err = errors.New("SQL mutation did not complete")
	}
	return err
}

func readWriterState(db *zova.DB) (state WriterState, err error) {
	s, err := prepareSQL(db, `SELECT version,namespace,node,owner,database_id,sequence,published,outbox_bytes
 FROM jlite_state WHERE singleton=1`)
	if err != nil {
		return state, fmt.Errorf("%w: %v", ErrIdentityMismatch, err)
	}
	defer func() { err = errors.Join(err, s.Close()) }()
	step, err := s.Step()
	if err != nil {
		return state, err
	}
	if step != zova.StepRow {
		return state, ErrIdentityMismatch
	}
	version, err := s.ColumnInt64(0)
	if err != nil {
		return state, err
	}
	if version != 1 {
		return state, ErrIdentityMismatch
	}
	for i, target := range []*string{&state.Namespace, &state.NodeID, &state.Owner, &state.DatabaseID} {
		value, ok, e := s.ColumnText(i + 1)
		if e != nil {
			return state, e
		}
		if !ok || value == "" {
			return state, ErrIdentityMismatch
		}
		*target = value
	}
	var values [3]int64
	for i := range values {
		values[i], err = s.ColumnInt64(i + 5)
		if err != nil {
			return state, err
		}
		if values[i] < 0 {
			return state, ErrIdentityMismatch
		}
	}
	if values[1] > values[0] {
		return state, ErrIdentityMismatch
	}
	state.Sequence, state.PublishedSequence = uint64(values[0]), uint64(values[1])
	state.OutboxBytes, state.OutboxCount = values[2], values[0]-values[1]
	return state, nil
}

// Verify the complete outbox once on reopen, not on every batch. Thereafter
// transactionally maintained counters make admission independent of log size.
func verifyOutboxState(db *zova.DB, state WriterState) (err error) {
	s, err := prepareSQL(db, "SELECT count(*),coalesce(sum(length(payload)),0),coalesce(min(sequence),0),coalesce(max(sequence),0) FROM jlite_outbox")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, s.Close()) }()
	step, err := s.Step()
	if err != nil {
		return err
	}
	if step != zova.StepRow {
		return ErrIdentityMismatch
	}
	var values [4]int64
	for i := range values {
		values[i], err = s.ColumnInt64(i)
		if err != nil {
			return err
		}
	}
	if values[0] != state.OutboxCount || values[1] != state.OutboxBytes {
		return ErrIdentityMismatch
	}
	if state.OutboxCount > 0 && (uint64(values[2]) != state.PublishedSequence+1 || uint64(values[3]) != state.Sequence) {
		return ErrIdentityMismatch
	}
	return nil
}

func findStoredWrite(db *zova.DB, namespace, id string) (stored StoredWrite, found bool, err error) {
	s, err := prepareSQL(db, "SELECT fingerprint,sequence FROM jlite_requests WHERE request_id=?", id)
	if err != nil {
		return stored, false, err
	}
	defer func() { err = errors.Join(err, s.Close()) }()
	step, err := s.Step()
	if err != nil || step == zova.StepDone {
		return stored, false, err
	}
	fingerprint, ok, err := s.ColumnText(0)
	if err != nil {
		return stored, false, err
	}
	if !ok {
		return stored, false, ErrIdentityMismatch
	}
	seq, err := s.ColumnInt64(1)
	if err != nil {
		return stored, false, err
	}
	return StoredWrite{RequestID: id, Fingerprint: fingerprint, Result: WriteResult{Namespace: namespace, Sequence: uint64(seq)}}, true, nil
}
