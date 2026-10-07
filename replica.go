package jlite

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"sync"

	zova "github.com/ata-sesli/zova/bindings/go"
	"github.com/nats-io/nats.go/jetstream"
)

var (
	ErrReplicaClosed   = errors.New("replica is closed")
	ErrReplicaNotReady = errors.New("replica has not reached its bootstrap target")
	ErrReplicaBlocked  = errors.New("replica is blocked; inspect cause and reopen before retrying")
	ErrSequenceGap     = errors.New("noncontiguous logical change sequence")
	ErrReplicaBusy     = errors.New("replica already has a consumer connection")
)

// ReplicaState reports local application, physical replay and fixed catch-up
// targets. Blocked is a visible runtime error and suppresses readiness.
type ReplicaState struct {
	DatabaseID         string
	Namespace          string
	NodeID             string
	Owner              string
	OwnerDatabaseID    string
	Consumer           string
	StreamID           string
	StreamCreated      string
	AppliedSequence    uint64
	LogSequence        uint64
	LastChangeID       string
	BootstrapTarget    uint64
	BootstrapLogTarget uint64
	Ready              bool
	Blocked            error
}

// Replica owns one read materialization and serializes its transactions/reads.
// It can serve previously synchronized local state while transport is offline.
type Replica struct {
	mu               sync.Mutex
	db               *zova.DB
	lock             *os.File
	cfg              Config
	namespace        string
	closed           bool
	blocked          error
	consumerAttached bool
}

const replicaSchema = `CREATE TABLE jlite_replica (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1), version INTEGER NOT NULL,
 database_id TEXT NOT NULL, namespace TEXT NOT NULL, node TEXT NOT NULL, owner TEXT NOT NULL,
 owner_database_id TEXT NOT NULL, stream_id TEXT NOT NULL, stream_created TEXT NOT NULL,
 applied INTEGER NOT NULL CHECK(applied>=0), log_sequence INTEGER NOT NULL CHECK(log_sequence>=applied), last_change_id TEXT NOT NULL,
 target INTEGER NOT NULL CHECK(target>=0), log_target INTEGER NOT NULL CHECK(log_target>=target), ready INTEGER NOT NULL CHECK(ready IN(0,1)));
CREATE TABLE jlite_records(key TEXT PRIMARY KEY,value BLOB NOT NULL);
CREATE TABLE jlite_applied(sequence INTEGER PRIMARY KEY CHECK(sequence>0),id TEXT NOT NULL UNIQUE,request_id TEXT NOT NULL UNIQUE);`

// ConsumerName is stable and distinct for every node/namespace assignment.
func ConsumerName(namespace, node string) (string, error) {
	if !validID(namespace) || !validID(node) {
		return "", ErrInvalidConfig
	}
	digest := sha256.Sum256([]byte(namespace + "\x00" + node))
	return fmt.Sprintf("R_%x", digest[:16]), nil
}

// OpenReplica creates or reopens a WAL/FULL local replica. Owners must use
// OpenWriter instead. It never adopts an owner database or changes identity.
func OpenReplica(path, namespace string, cfg Config) (*Replica, error) {
	a, err := cfg.assignment(namespace)
	if err != nil {
		return nil, err
	}
	if !a.hosts(cfg.NodeID) || a.Owner == cfg.NodeID {
		return nil, ErrNotAssigned
	}
	cfg.Nodes = append([]string(nil), cfg.Nodes...)
	cfg.Namespaces = append([]Assignment(nil), cfg.Namespaces...)
	for i := range cfg.Namespaces {
		cfg.Namespaces[i].Replicas = append([]string(nil), cfg.Namespaces[i].Replicas...)
	}
	absolute, lock, err := lockDatabase(path)
	if err != nil {
		return nil, err
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
	r := &Replica{db: db, lock: lock, cfg: cfg, namespace: namespace}
	if err == nil {
		err = db.SetBusyTimeout(1000)
	}
	if err == nil && created {
		err = db.Exec("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL")
		var id [16]byte
		if err == nil {
			_, err = rand.Read(id[:])
		}
		if err == nil {
			err = db.BeginImmediate()
		}
		if err == nil {
			err = db.Exec(replicaSchema)
		}
		if err == nil {
			err = executeSQL(db, "INSERT INTO jlite_replica VALUES(1,1,?,?,?,?,'','','',0,0,'',0,0,0)", hex.EncodeToString(id[:]), namespace, cfg.NodeID, a.Owner)
		}
		if err == nil {
			err = db.Commit()
		}
		if err != nil {
			_ = db.Rollback()
		}
	}
	if err == nil {
		var state ReplicaState
		state, err = r.readState()
		if err == nil && (state.Namespace != namespace || state.NodeID != cfg.NodeID || state.Owner != a.Owner) {
			err = ErrIdentityMismatch
		}
		if err == nil {
			err = r.verifyLedger(state)
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
		return nil, err
	}
	return r, nil
}

func (r *Replica) readState() (state ReplicaState, err error) {
	s, err := prepareSQL(r.db, "SELECT version,database_id,namespace,node,owner,owner_database_id,stream_id,stream_created,applied,log_sequence,last_change_id,target,log_target,ready FROM jlite_replica WHERE singleton=1")
	if err != nil {
		return state, errors.Join(ErrIdentityMismatch, err)
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
	for i, target := range []*string{&state.DatabaseID, &state.Namespace, &state.NodeID, &state.Owner, &state.OwnerDatabaseID, &state.StreamID, &state.StreamCreated} {
		value, ok, e := s.ColumnText(i + 1)
		if e != nil {
			return state, e
		}
		if !ok || i < 4 && value == "" {
			return state, ErrIdentityMismatch
		}
		*target = value
	}
	for i, target := range []*uint64{&state.AppliedSequence, &state.LogSequence, &state.BootstrapTarget, &state.BootstrapLogTarget} {
		index := 8 + i
		if i >= 2 {
			index++
		}
		value, e := s.ColumnInt64(index)
		if e != nil {
			return state, e
		}
		if value < 0 {
			return state, ErrIdentityMismatch
		}
		*target = uint64(value)
	}
	state.LastChangeID, _, err = s.ColumnText(10)
	if err != nil {
		return state, err
	}
	ready, err := s.ColumnInt64(13)
	if err != nil {
		return state, err
	}
	state.Ready = ready == 1
	state.Consumer, _ = ConsumerName(state.Namespace, state.NodeID)
	if state.LogSequence < state.AppliedSequence || state.BootstrapLogTarget < state.BootstrapTarget || state.LogSequence > 0 && state.LastChangeID == "" {
		return state, ErrIdentityMismatch
	}
	return state, nil
}

func (r *Replica) verifyLedger(state ReplicaState) (err error) {
	s, err := prepareSQL(r.db, "SELECT count(*),coalesce(max(sequence),0) FROM jlite_applied")
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
	count, err := s.ColumnInt64(0)
	if err != nil {
		return err
	}
	max, err := s.ColumnInt64(1)
	if err != nil {
		return err
	}
	if uint64(count) != state.AppliedSequence || uint64(max) != state.AppliedSequence {
		return ErrIdentityMismatch
	}
	return nil
}

func (r *Replica) available() error {
	if r.closed {
		return ErrReplicaClosed
	}
	if r.blocked != nil {
		return errors.Join(ErrReplicaBlocked, r.blocked)
	}
	return nil
}

// State remains inspectable when blocked. No blocked replica is reported ready.
func (r *Replica) State() (ReplicaState, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ReplicaState{}, ErrReplicaClosed
	}
	s, err := r.readState()
	s.Blocked = r.blocked
	if r.blocked != nil {
		s.Ready = false
	}
	return s, err
}

// Get serves only synchronized, unblocked local state in one SQL snapshot.
func (r *Replica) Get(request ReadRequest) (result ReadResult, err error) {
	if request.Namespace != r.namespace {
		return result, ErrUnknownNamespace
	}
	if err := r.cfg.ValidateRead(request); err != nil {
		return result, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.available(); err != nil {
		return result, err
	}
	s, err := prepareSQL(r.db, "SELECT s.applied,s.ready,v.value,v.key IS NOT NULL FROM jlite_replica s LEFT JOIN jlite_records v ON v.key=? WHERE s.singleton=1", request.Key)
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
	ready, err := s.ColumnInt64(1)
	if err != nil {
		return result, err
	}
	if ready != 1 {
		return result, ErrReplicaNotReady
	}
	seq, err := s.ColumnInt64(0)
	if err != nil {
		return result, err
	}
	found, err := s.ColumnInt64(3)
	if err != nil {
		return result, err
	}
	result.AppliedSequence, result.Found = uint64(seq), found == 1
	if result.Found {
		result.Value, _, err = s.ColumnBlob(2)
	}
	return result, err
}

// Close releases the local database/lease. Close its consumer connection first.
func (r *Replica) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	return errors.Join(r.db.Close(), r.lock.Close())
}

func (r *Replica) appliedID(sequence uint64) (id string, found bool, err error) {
	s, err := prepareSQL(r.db, "SELECT id FROM jlite_applied WHERE sequence=?", int64(sequence))
	if err != nil {
		return "", false, err
	}
	defer func() { err = errors.Join(err, s.Close()) }()
	step, err := s.Step()
	if err != nil || step == zova.StepDone {
		return "", false, err
	}
	id, found, err = s.ColumnText(0)
	return id, found, err
}

func (r *Replica) applyMessages(messages []jetstream.Msg, consumer string) (applied int, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.available(); err != nil {
		return 0, err
	}
	if err = r.db.BeginImmediate(); err == nil {
		applied, err = r.mutateMessages(messages, consumer)
	}
	if err == nil {
		err = r.db.Commit()
	}
	if err != nil {
		err = errors.Join(err, r.db.Rollback())
		r.blocked = err
		return 0, err
	}
	return applied, nil
}

// mutateMessages validates and applies a batch and its checkpoint inside the
// caller's open transaction. The caller holds mu and owns commit/rollback.
func (r *Replica) mutateMessages(messages []jetstream.Msg, consumer string) (applied int, err error) {
	var state ReplicaState
	state, err = r.readState()
	bytes := 0
	if len(messages) > r.cfg.Limits.MaxBatchOperations {
		err = ErrInvalidChange
	}
	for _, m := range messages {
		if err != nil {
			break
		}
		bytes += len(m.Data())
		if bytes > r.cfg.Limits.MaxBatchBytes {
			err = ErrInvalidChange
			break
		}
		var change Change
		change, err = DecodeChange(m.Data(), r.cfg)
		if err != nil {
			break
		}
		if change.Request.Namespace != r.namespace {
			err = ErrInvalidChange
			break
		}
		var meta *jetstream.MsgMetadata
		meta, err = m.Metadata()
		if err != nil {
			break
		}
		name, _ := StreamName(r.namespace)
		subject, _ := ChangeSubject(r.namespace)
		if meta.Stream != name || meta.Consumer != consumer || m.Subject() != subject || m.Headers().Get(jetstream.MsgIDHeader) != change.ID || meta.Sequence.Stream == 0 || meta.Sequence.Stream > maxSequence {
			err = ErrStreamIdentity
			break
		}
		if meta.Sequence.Stream > state.LogSequence+1 || change.Sequence > state.AppliedSequence+1 {
			err = ErrSequenceGap
			break
		}
		if change.Sequence <= state.AppliedSequence {
			var id string
			var found bool
			id, found, err = r.appliedID(change.Sequence)
			if err == nil && (!found || id != change.ID) {
				err = ErrInvalidChange
			}
		} else if meta.Sequence.Stream <= state.LogSequence {
			err = ErrInvalidChange
		} else {
			if change.Request.Operation == Put {
				err = executeSQL(r.db, "INSERT INTO jlite_records VALUES(?,coalesce(?,x'')) ON CONFLICT(key) DO UPDATE SET value=excluded.value", change.Request.Key, change.Request.Value)
			} else {
				err = executeSQL(r.db, "DELETE FROM jlite_records WHERE key=?", change.Request.Key)
			}
			if err == nil {
				err = executeSQL(r.db, "INSERT INTO jlite_applied VALUES(?,?,?)", int64(change.Sequence), change.ID, change.Request.RequestID)
			}
			if err == nil {
				state.AppliedSequence++
				applied++
			}
		}
		if err == nil && meta.Sequence.Stream > state.LogSequence {
			state.LogSequence = meta.Sequence.Stream
			state.LastChangeID = change.ID
		}
	}
	if err == nil {
		ready := int64(0)
		if state.LogSequence >= state.BootstrapLogTarget && state.AppliedSequence >= state.BootstrapTarget {
			ready = 1
		}
		err = executeSQL(r.db, "UPDATE jlite_replica SET applied=?,log_sequence=?,last_change_id=?,ready=? WHERE singleton=1", int64(state.AppliedSequence), int64(state.LogSequence), state.LastChangeID, ready)
	}
	return applied, err
}
