package jlite

import (
	"errors"
	"path/filepath"
	"testing"

	zova "github.com/ata-sesli/zova/bindings/go"
)

func TestZovaTransactionsSurviveReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "transactions.zova")
	db, err := zova.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if db != nil {
			if err := db.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	t.Logf("Zova ABI: %s", zova.ABIVersion())
	if got := zova.ABIVersion(); got != "1.1.0" {
		t.Fatalf("Zova ABI = %q, want 1.1.0", got)
	}
	info, err := zova.ProbeFormat(path)
	if err != nil || info.Compatibility != zova.FormatCurrent {
		t.Fatalf("current Zova format: %+v, %v", info, err)
	}
	for _, sql := range []string{
		"PRAGMA journal_mode = WAL",
		"PRAGMA synchronous = FULL",
		"CREATE TABLE records (id INTEGER PRIMARY KEY, value BLOB NOT NULL)",
		"CREATE TABLE progress (sequence INTEGER NOT NULL)",
		"INSERT INTO progress VALUES (0)",
	} {
		if err := db.Exec(sql); err != nil {
			t.Fatal(err)
		}
	}
	if got := queryInt(t, db, "PRAGMA synchronous"); got != 2 {
		t.Fatalf("synchronous = %d, want FULL (2)", got)
	}
	if err := db.BeginImmediate(); err != nil {
		t.Fatal(err)
	}
	insertRecord(t, db, 1, []byte{0, 1, 255})
	if err := db.Exec("UPDATE progress SET sequence = 1"); err != nil {
		t.Fatal(err)
	}
	if err := db.Rollback(); err != nil {
		t.Fatal(err)
	}
	if got := queryInt(t, db, "SELECT count(*) FROM records"); got != 0 {
		t.Fatalf("rollback left %d records", got)
	}
	if got := queryInt(t, db, "SELECT sequence FROM progress"); got != 0 {
		t.Fatalf("rollback left sequence %d", got)
	}
	if err := db.BeginImmediate(); err != nil {
		t.Fatal(err)
	}
	insertRecord(t, db, 1, []byte{0, 1, 255})
	if err := db.Exec("UPDATE progress SET sequence = 1"); err != nil {
		t.Fatal(err)
	}
	if err := db.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = nil
	db, err = zova.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := queryInt(t, db, "SELECT sequence FROM progress"); got != 1 {
		t.Fatalf("reopened sequence = %d, want 1", got)
	}
	if got := queryInt(t, db, "SELECT count(*) FROM records WHERE id = 1 AND value = x'0001ff'"); got != 1 {
		t.Fatalf("reopened matching records = %d, want 1", got)
	}
}

func TestZovaReturnsTypedErrors(t *testing.T) {
	db, err := zova.Create(filepath.Join(t.TempDir(), "errors.zova"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.Exec("CREATE TABLE records (id INTEGER PRIMARY KEY); INSERT INTO records VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	err = db.Exec("INSERT INTO records VALUES (1)")
	var zerr *zova.Error
	if !errors.As(err, &zerr) || zerr.Status != zova.StatusConstraint {
		t.Fatalf("duplicate key error = %v, want typed constraint error", err)
	}
	if got := queryInt(t, db, "SELECT count(*) FROM records"); got != 1 {
		t.Fatalf("failed insert changed record count to %d", got)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	err = db.Exec("SELECT 1")
	if !errors.As(err, &zerr) || zerr.Status != zova.StatusMisuse {
		t.Fatalf("closed database error = %v, want typed misuse error", err)
	}
}

func insertRecord(t *testing.T, db *zova.DB, id int64, value []byte) {
	t.Helper()
	stmt, err := db.Prepare("INSERT INTO records (id, value) VALUES (?, ?)")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := stmt.Close(); err != nil {
			t.Error(err)
		}
	}()
	if err := stmt.BindInt64(1, id); err != nil {
		t.Fatal(err)
	}
	if err := stmt.BindBlob(2, value); err != nil {
		t.Fatal(err)
	}
	step, err := stmt.Step()
	if err != nil || step != zova.StepDone {
		t.Fatalf("insert step = %v, %v", step, err)
	}
}

func queryInt(t *testing.T, db *zova.DB, sql string) int64 {
	t.Helper()
	stmt, err := db.Prepare(sql)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := stmt.Close(); err != nil {
			t.Error(err)
		}
	}()
	step, err := stmt.Step()
	if err != nil || step != zova.StepRow {
		t.Fatalf("query step = %v, %v", step, err)
	}
	value, err := stmt.ColumnInt64(0)
	if err != nil {
		t.Fatal(err)
	}
	return value
}
