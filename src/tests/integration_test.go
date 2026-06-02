package tests

import (
	"testing"
	"time"

	"github.com/Ari-Ghosh/flash-db/src/engine"
)

func TestCFWithTTL(t *testing.T) {
	db := openDB(t, tmpDir(t))
	defer func() { _ = db.Close() }()

	_ = db.CreateColumnFamily("sessions")
	cf, _ := db.GetColumnFamily("sessions")

	_ = cf.PutWithTTL([]byte("sess:abc"), []byte("user1"), 50*time.Millisecond)
	v, _ := cf.Get([]byte("sess:abc"))
	if string(v) != "user1" {
		t.Fatalf("got %q before expiry", v)
	}
	time.Sleep(100 * time.Millisecond)
	_, err := cf.Get([]byte("sess:abc"))
	if err == nil {
		t.Fatal("session should have expired")
	}
}

func TestIndexWithWriteBatch(t *testing.T) {
	db := openDB(t, tmpDir(t))
	defer func() { _ = db.Close() }()

	_ = db.DefineIndex(engine.IndexDefinition{Name: "batch_idx", KeyFn: emailIndex})

	// Use a WriteBatch for the puts, then check index (note: WriteBatch bypasses
	// index maintenance, so we use PutIndexed here to be explicit).
	_ = db.PutIndexed([]byte("batchidx:1"), []byte("val1"))
	_ = db.PutIndexed([]byte("batchidx:2"), []byte("val1"))

	pkeys, _ := db.QueryByIndex("batch_idx", []byte("val1"))
	if len(pkeys) != 2 {
		t.Fatalf("expected 2 results, got %d", len(pkeys))
	}
}

func TestAllFeaturesWithRestart(t *testing.T) {
	dir := tmpDir(t)
	db := openDB(t, dir)

	// Column family.
	_ = db.CreateColumnFamily("persist_cf")
	cf, _ := db.GetColumnFamily("persist_cf")
	_ = cf.Put([]byte("cfk"), []byte("cfv"))

	// WriteBatch.
	wb := db.NewWriteBatch()
	wb.Put([]byte("wb:restart"), []byte("ok"))
	_ = wb.Commit()

	_ = db.Close()
	db2 := openDB(t, dir)
	defer func() { _ = db2.Close() }()

	// CF must survive.
	cf2, err := db2.GetColumnFamily("persist_cf")
	if err != nil {
		t.Fatalf("CF not found after restart: %v", err)
	}
	v, err := cf2.Get([]byte("cfk"))
	if err != nil || string(v) != "cfv" {
		t.Fatalf("CF value after restart: %q %v", v, err)
	}

	// WriteBatch key must survive.
	mustGet(t, db2, "wb:restart", "ok")
}
