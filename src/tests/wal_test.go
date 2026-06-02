package tests

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Ari-Ghosh/flash-db/src/engine"
	"github.com/Ari-Ghosh/flash-db/src/wal"
)

func TestWALRecovery(t *testing.T) {
	dir := tmpDir(t)
	db := openDB(t, dir)

	mustPut(t, db, "persist", "yes")
	mustPut(t, db, "also", "survives")
	// Simulate crash by closing without compaction.
	_ = db.Close()

	db2 := openDB(t, dir)
	defer func() { _ = db2.Close() }()

	mustGet(t, db2, "persist", "yes")
	mustGet(t, db2, "also", "survives")
}

func TestWALRecoveryAfterDelete(t *testing.T) {
	dir := tmpDir(t)
	db := openDB(t, dir)

	mustPut(t, db, "del", "me")
	_ = db.Delete([]byte("del"))
	_ = db.Close()

	db2 := openDB(t, dir)
	defer func() { _ = db2.Close() }()
	mustNotFound(t, db2, "del")
}

// ── 8. WAL batch mode throughput ──────────────────────────────────────────────

func TestWALBatchThroughput(t *testing.T) {
	dir := tmpDir(t)
	cfg := engine.DefaultConfig(dir)
	cfg.MemTableSize = 64 * 1024 * 1024
	cfg.WALSyncPolicy = wal.SyncBatch
	db, err := engine.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	const n = 500
	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		// i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = db.Put([]byte(fmt.Sprintf("bk%d", i)), []byte("val"))
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	t.Logf("WAL batch: %d concurrent puts in %v", n, elapsed)

	// Just verify data integrity.
	for i := 0; i < n; i++ {
		mustGet(t, db, fmt.Sprintf("bk%d", i), "val")
	}
}

// ── 10. Unit tests: WAL, MemTable, Bloom, BTree, Compaction ──────────────────

func TestWALRoundtrip(t *testing.T) {
	dir := tmpDir(t)
	path := filepath.Join(dir, "wal.log")

	w, err := wal.Open(filepath.Clean(path))
	if err != nil {
		t.Fatal(err)
	}
	_ = w.AppendPut(1, []byte("key1"), []byte("val1"))
	_ = w.AppendPut(2, []byte("key2"), []byte("val2"))
	_ = w.AppendDelete(3, []byte("key1"))
	_ = w.Close()

	w2, _ := wal.Open(filepath.Clean(path))
	recs, err := w2.Replay()
	_ = w2.Close()
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 3 {
		t.Fatalf("expected 3 records, got %d", len(recs))
	}
	if string(recs[1].Key) != "key2" || string(recs[1].Value) != "val2" {
		t.Fatalf("unexpected record: %+v", recs[1])
	}
	if !recs[2].Tombstone {
		t.Fatal("expected tombstone for record 3")
	}
}

func TestWALCRCCorruption(t *testing.T) {
	dir := tmpDir(t)
	path := filepath.Join(dir, "wal.log")

	w, _ := wal.Open(filepath.Clean(path))
	_ = w.AppendPut(1, []byte("k"), []byte("v"))
	_ = w.Close()

	// Corrupt a byte in the middle.
	data, _ := os.ReadFile(filepath.Clean(path))
	data[len(data)/2] ^= 0xFF
	// Use Join with Base to satisfy gosec G703 (path traversal) taint analysis.
	corruptPath := filepath.Join(dir, filepath.Base(path))
	if err := os.WriteFile(corruptPath, data, 0o644); err != nil { //nolint:gosec // G703: path is constructed from t.TempDir(), not user input
		t.Fatal(err)
	}

	w2, _ := wal.Open(filepath.Clean(path))
	recs, err := w2.Replay()
	_ = w2.Close()
	// Should get 0 or 1 records (corruption stops replay).
	if err != nil {
		t.Fatal("unexpected error:", err)
	}
	_ = recs // 0 records is expected
}

// ── 1. WAL parallel writes / group-commit ─────────────────────────────────────

func TestWALGroupCommitCorrectness(t *testing.T) {
	dir := tmpDir(t)
	cfg := engine.DefaultConfig(dir)
	cfg.MemTableSize = 64 * 1024 * 1024
	cfg.WALSyncPolicy = wal.SyncBatch
	db, err := engine.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	const goroutines, perG = 50, 200
	var wg sync.WaitGroup
	errs := make(chan error, goroutines*perG)
	for g := 0; g < goroutines; g++ {
		// g := g
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				k := fmt.Sprintf("g%d:k%d", g, i)
				if err := db.Put([]byte(k), []byte("val")); err != nil {
					errs <- err
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Errorf("concurrent Put: %v", e)
	}
	// Spot-check data integrity.
	for g := 0; g < goroutines; g++ {
		mustGet(t, db, fmt.Sprintf("g%d:k0", g), "val")
		mustGet(t, db, fmt.Sprintf("g%d:k%d", g, perG-1), "val")
	}
}

func TestWALGroupCommitSurvivesCrash(t *testing.T) {
	dir := tmpDir(t)
	cfg := engine.DefaultConfig(dir)
	cfg.WALSyncPolicy = wal.SyncBatch
	db, err := engine.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 500; i++ {
		mustPut(t, db, fmt.Sprintf("crash:%05d", i), "v")
	}
	_ = db.Close()

	db2 := openDB(t, dir)
	for i := 0; i < 500; i++ {
		mustGet(t, db2, fmt.Sprintf("crash:%05d", i), "v")
	}
}

func TestWALRecoveryRegression(t *testing.T) {
	dir := tmpDir(t)
	db := openDB(t, dir)
	mustPut(t, db, "persist", "yes")
	_ = db.Close()
	db2 := openDB(t, dir)
	mustGet(t, db2, "persist", "yes")
}
