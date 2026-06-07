package tests

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/Ari-Ghosh/flash-db/src/btree"
	"github.com/Ari-Ghosh/flash-db/src/compaction"
	"github.com/Ari-Ghosh/flash-db/src/logging"
	"github.com/Ari-Ghosh/flash-db/src/sstable"
	types "github.com/Ari-Ghosh/flash-db/src/types"
)

func TestFlushAndCompaction(t *testing.T) {
	dir := tmpDir(t)
	db := openDB(t, dir)
	defer func() { _ = db.Close() }()

	// Write enough to trigger multiple flushes and a compaction.
	for i := 0; i < 3000; i++ {
		k := fmt.Sprintf("k%06d", i)
		v := fmt.Sprintf("v%d", i)
		mustPut(t, db, k, v)
	}
	time.Sleep(400 * time.Millisecond)

	for _, i := range []int{0, 1000, 2999} {
		k := fmt.Sprintf("k%06d", i)
		v := fmt.Sprintf("v%d", i)
		mustGet(t, db, k, v)
	}
}

func TestCompactionPreservesLatestVersion(t *testing.T) {
	dir := tmpDir(t)
	db := openDB(t, dir)
	defer func() { _ = db.Close() }()

	// Write key multiple times, then flush to L0.
	for v := 0; v < 10; v++ {
		mustPut(t, db, "key", fmt.Sprintf("v%d", v))
	}
	for i := 0; i < 2000; i++ {
		mustPut(t, db, fmt.Sprintf("filler%05d", i), "x")
	}
	time.Sleep(400 * time.Millisecond)
	mustGet(t, db, "key", "v9")
}

// ── 7. Tombstone GC after compaction ──────────────────────────────────────────

func TestTombstoneGCAfterCompaction(t *testing.T) {
	dir := tmpDir(t)
	db := openDB(t, dir)

	mustPut(t, db, "tgc", "v")
	// Ensure in L0 by writing enough to flush.
	for i := 0; i < 2000; i++ {
		mustPut(t, db, fmt.Sprintf("filler%05d", i), "x")
	}
	time.Sleep(200 * time.Millisecond)

	_ = db.Delete([]byte("tgc"))
	for i := 2000; i < 4000; i++ {
		mustPut(t, db, fmt.Sprintf("filler%05d", i), "x")
	}
	time.Sleep(400 * time.Millisecond)

	mustNotFound(t, db, "tgc")
	_ = db.Close()

	// Reopen: key should still be absent.
	db2 := openDB(t, dir)
	defer func() { _ = db2.Close() }()
	mustNotFound(t, db2, "tgc")
}

func TestCompactionMergeTwo(t *testing.T) {
	dir := tmpDir(t)

	// Build two L0 SSTables with overlapping keys.
	path1 := filepath.Join(dir, "l0_1.sst")
	path2 := filepath.Join(dir, "l0_2.sst")

	w1, _ := sstable.NewWriter(filepath.Clean(path1), 5)
	_ = w1.Add(types.Entry{Key: []byte("a"), Value: []byte("old"), SeqNum: 1})
	_ = w1.Add(types.Entry{Key: []byte("b"), Value: []byte("b1"), SeqNum: 1})
	_ = w1.Close()

	w2, _ := sstable.NewWriter(filepath.Clean(path2), 5)
	_ = w2.Add(types.Entry{Key: []byte("a"), Value: []byte("new"), SeqNum: 5})
	_ = w2.Add(types.Entry{Key: []byte("c"), Value: []byte("c1"), SeqNum: 5})
	_ = w2.Close()

	l1Tree, _ := btree.Open(filepath.Clean(filepath.Join(dir, "l1.bt")))
	l2Tree, _ := btree.Open(filepath.Clean(filepath.Join(dir, "l2.bt")))
	defer func() { _ = l1Tree.Close() }()
	defer func() { _ = l2Tree.Close() }()

	tracker := types.NewSnapshotTracker()
	eng := compaction.New(compaction.Config{
		L0Threshold:     2,
		L1SizeThreshold: 1024 * 1024 * 1024,
	}, l1Tree, l2Tree, tracker, nil, logging.New(logging.LevelInfo))
	eng.Start()
	eng.Trigger([]string{path1, path2})
	time.Sleep(200 * time.Millisecond)
	eng.Stop()

	// After compaction, "a" should have the newer value.
	e, err := l1Tree.Get([]byte("a"))
	if err != nil {
		t.Fatalf("Get(a): %v", err)
	}
	if string(e.Value) != "new" {
		t.Fatalf("compaction: a = %q, want 'new'", e.Value)
	}
	// "b" and "c" should both be present.
	if _, err := l1Tree.Get([]byte("b")); err != nil {
		t.Fatalf("Get(b): %v", err)
	}
	if _, err := l1Tree.Get([]byte("c")); err != nil {
		t.Fatalf("Get(c): %v", err)
	}
}

func TestPrefixScanPostCompaction(t *testing.T) {
	db := openDB(t, tmpDir(t))
	// Trigger flush + compaction by writing enough data.
	for i := 0; i < 3000; i++ {
		mustPut(t, db, fmt.Sprintf("metric:%08d", i), fmt.Sprintf("v%d", i))
	}
	time.Sleep(400 * time.Millisecond)

	iter, err := db.PrefixScan([]byte("metric:"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = iter.Close() }()

	count := 0
	for iter.Valid() {
		count++
		iter.Next()
	}
	if count != 3000 {
		t.Fatalf("post-compaction prefix scan: got %d keys, want 3000", count)
	}
}
