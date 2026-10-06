package jlite

import (
	"bytes"
	"context"
	"errors"

	zova "github.com/ata-sesli/zova/bindings/go"
)

type streamBinding struct {
	Name        string
	ID          string
	Created     string
	LogSequence uint64
	ChangeID    string
}

const bindingSchema = `CREATE TABLE IF NOT EXISTS jlite_stream (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1), name TEXT NOT NULL,
 incarnation TEXT NOT NULL, created TEXT NOT NULL, log_sequence INTEGER NOT NULL CHECK(log_sequence>=0),
 change_id TEXT NOT NULL)`

func (w *Writer) publisherSnapshot() (WriterState, streamBinding, bool, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.available(); err != nil {
		return WriterState{}, streamBinding{}, false, err
	}
	if err := w.db.Exec(bindingSchema); err != nil {
		w.failed = err
		return WriterState{}, streamBinding{}, false, errors.Join(ErrWriterFailed, err)
	}
	state, err := readWriterState(w.db)
	if err != nil {
		return state, streamBinding{}, false, err
	}
	b, found, err := readBinding(w.db)
	if err == nil && ((state.PublishedSequence > 0 && (!found || b.LogSequence < state.PublishedSequence || b.ChangeID == "")) || (state.PublishedSequence == 0 && found && (b.LogSequence != 0 || b.ChangeID != ""))) {
		err = ErrStreamIdentity
	}
	return state, b, found, err
}

func readBinding(db *zova.DB) (b streamBinding, found bool, err error) {
	s, err := prepareSQL(db, "SELECT name,incarnation,created,log_sequence,change_id FROM jlite_stream WHERE singleton=1")
	if err != nil {
		return b, false, err
	}
	defer func() { err = errors.Join(err, s.Close()) }()
	step, err := s.Step()
	if err != nil || step == zova.StepDone {
		return b, false, err
	}
	for i, target := range []*string{&b.Name, &b.ID, &b.Created} {
		value, ok, e := s.ColumnText(i)
		if e != nil {
			return b, false, e
		}
		if !ok || value == "" {
			return b, false, ErrStreamIdentity
		}
		*target = value
	}
	seq, err := s.ColumnInt64(3)
	if err != nil {
		return b, false, err
	}
	if seq < 0 {
		return b, false, ErrStreamIdentity
	}
	b.LogSequence = uint64(seq)
	b.ChangeID, _, err = s.ColumnText(4)
	return b, true, err
}

func (w *Writer) bindStream(b streamBinding) (err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.available(); err != nil {
		return err
	}
	if err = w.db.BeginImmediate(); err == nil {
		var old streamBinding
		var found bool
		old, found, err = readBinding(w.db)
		if err == nil && found {
			if old.Name != b.Name || old.ID != b.ID || old.Created != b.Created {
				err = ErrStreamIdentity
			}
		} else if err == nil {
			err = executeSQL(w.db, "INSERT INTO jlite_stream VALUES(1,?,?,?,0,'')", b.Name, b.ID, b.Created)
		}
	}
	if err == nil {
		err = w.db.Commit()
	}
	if err != nil {
		rollback := w.db.Rollback()
		if errors.Is(err, ErrStreamIdentity) && rollback == nil {
			return err
		}
		w.failed = errors.Join(err, rollback)
		return errors.Join(ErrWriterFailed, w.failed)
	}
	return nil
}

func (w *Writer) acknowledgePublication(ctx context.Context, entry OutboxEntry, logSequence uint64, b streamBinding) (err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.available(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err = w.db.BeginImmediate(); err == nil {
		var state WriterState
		var current streamBinding
		var found bool
		state, err = readWriterState(w.db)
		if err == nil {
			current, found, err = readBinding(w.db)
		}
		if err == nil && (!found || current.Name != b.Name || current.ID != b.ID || current.Created != b.Created || entry.Sequence != state.PublishedSequence+1 || logSequence <= current.LogSequence || logSequence > maxSequence || int64(len(entry.Payload)) > state.OutboxBytes) {
			err = ErrStreamIdentity
		}
		if err == nil {
			err = checkOutboxEntry(w.db, entry)
		}
		if err == nil {
			err = executeSQL(w.db, "DELETE FROM jlite_outbox WHERE sequence=? AND id=?", int64(entry.Sequence), entry.ID)
		}
		if err == nil {
			err = executeSQL(w.db, "UPDATE jlite_state SET published=?,outbox_bytes=? WHERE singleton=1", int64(entry.Sequence), state.OutboxBytes-int64(len(entry.Payload)))
		}
		if err == nil {
			err = executeSQL(w.db, "UPDATE jlite_stream SET log_sequence=?,change_id=? WHERE singleton=1", int64(logSequence), entry.ID)
		}
		if err == nil {
			err = ctx.Err()
		}
	}
	if err == nil {
		err = w.db.Commit()
	}
	if err != nil {
		rollback := w.db.Rollback()
		if (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrStreamIdentity)) && rollback == nil {
			return err
		}
		w.failed = errors.Join(err, rollback)
		return errors.Join(ErrWriterFailed, w.failed)
	}
	return nil
}

func checkOutboxEntry(db *zova.DB, entry OutboxEntry) (err error) {
	s, err := prepareSQL(db, "SELECT sequence,id,payload FROM jlite_outbox ORDER BY sequence LIMIT 1")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, s.Close()) }()
	step, err := s.Step()
	if err != nil {
		return err
	}
	if step != zova.StepRow {
		return ErrStreamIdentity
	}
	seq, err := s.ColumnInt64(0)
	if err != nil {
		return err
	}
	id, _, err := s.ColumnText(1)
	if err != nil {
		return err
	}
	wire, _, err := s.ColumnBlob(2)
	if err != nil {
		return err
	}
	if uint64(seq) != entry.Sequence || id != entry.ID || !bytes.Equal(wire, entry.Payload) {
		return ErrStreamIdentity
	}
	return nil
}
