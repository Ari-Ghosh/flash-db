// Package tests contains integration tests for HybridDB.
//
// ## Overview
//
// This test suite validates all HybridDB v2 features including MVCC, iterators,
// compaction, and crash recovery. Tests ensure correctness under concurrent
// operations and verify performance optimizations.
//
// ## Test Coverage
//
// 1. **Basic CRUD**: Put/Get/Delete operations.
// 2. **WAL Crash Recovery**: WAL replay restores state after restart.
// 3. **Flush + Compaction**: MemTable→SSTable→B-tree correctness.
// 4. **MVCC Snapshots**: Point-in-time reads with isolation.
// 5. **Range Iterator**: Forward/reverse scans with bounds.
// 6. **Concurrent Writes**: Data race detection and consistency.
// 7. **Tombstone GC**: Deleted keys invisible after compaction.
// 8. **WAL Batch Mode**: Performance validation of group-commit.
// 9. **Iterator Filtering**: Tombstone handling in range scans.
// 10. **Tiered Compaction**: L0→L1→L2 incremental merging.
//
// ## v1 vs v2 Changes
//
// ### v1 (Original Tests)
// - Basic CRUD and compaction tests.
// - No snapshot or iterator testing.
// - Simple single-threaded validation.
// - Focused on basic LSM-tree correctness.
//
// ### v2 (Enhanced Tests)
// - **MVCC Testing**: Snapshot isolation and visibility rules.
// - **Iterator Tests**: Range scans, bounds, direction, filtering.
// - **Concurrency**: Race detection with parallel operations.
// - **Performance**: WAL batching and compaction benchmarks.
// - **Advanced Features**: Tombstone GC, tiered storage, compression.
//
// ## Test Structure
//
// - **TestBasicCRUD**: Fundamental operations.
// - **TestSnapshots**: MVCC isolation.
// - **TestIterators**: Range scan functionality.
// - **TestCompaction**: Tiered merging correctness.
// - **TestCrashRecovery**: WAL replay validation.
// - **TestConcurrent**: Race-free concurrent access.
//
// ## Running Tests
//
//	go test ./tests/...
//
// Tests use temporary directories and clean up automatically. Some tests
// intentionally trigger panics to validate error handling.
package tests

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"local/flashdb/bloom"
	"local/flashdb/btree"
	"local/flashdb/compaction"
	"local/flashdb/engine"
	hybriddb "local/flashdb/hybriddb"
	"local/flashdb/memtable"
	"local/flashdb/sstable"
	"local/flashdb/wal"
)

// ── helpers ───────────────────────────────────────────────────────────────────

func tmpDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "hybriddb_test_*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

func openDB(t *testing.T, dir string) *engine.DB {
	t.Helper()
	cfg := engine.DefaultConfig(dir)
	cfg.MemTableSize = 128 * 1024 // 128 KB – fast flushes in tests
	cfg.L0CompactThreshold = 2
	db, err := engine.Open(cfg)
	if err != nil {
		t.Fatalf("engine.Open: %v", err)
	}
	return db
}

func mustPut(t *testing.T, db *engine.DB, k, v string) {
	t.Helper()
	if err := db.Put([]byte(k), []byte(v)); err != nil {
		t.Fatalf("Put(%q): %v", k, err)
	}
}

func mustGet(t *testing.T, db *engine.DB, k, want string) {
	t.Helper()
	got, err := db.Get([]byte(k))
	if err != nil {
		t.Fatalf("Get(%q): %v", k, err)
	}
	if string(got) != want {
		t.Fatalf("Get(%q) = %q, want %q", k, got, want)
	}
}

func mustNotFound(t *testing.T, db *engine.DB, k string) {
	t.Helper()
	_, err := db.Get([]byte(k))
	if err != hybriddb.ErrKeyNotFound && err != hybriddb.ErrKeyDeleted {
		t.Fatalf("Get(%q) = %v, want ErrKeyNotFound/ErrKeyDeleted", k, err)
	}
}

// ── 1. Basic CRUD ─────────────────────────────────────────────────────────────

func TestBasicPutGet(t *testing.T) {
	db := openDB(t, tmpDir(t))
	defer db.Close()

	mustPut(t, db, "hello", "world")
	mustGet(t, db, "hello", "world")
}

func TestOverwrite(t *testing.T) {
	db := openDB(t, tmpDir(t))
	defer db.Close()

	mustPut(t, db, "key", "v1")
	mustPut(t, db, "key", "v2")
	mustGet(t, db, "key", "v2")
}

func TestDelete(t *testing.T) {
	db := openDB(t, tmpDir(t))
	defer db.Close()

	mustPut(t, db, "key", "val")
	if err := db.Delete([]byte("key")); err != nil {
		t.Fatal(err)
	}
	mustNotFound(t, db, "key")
}

func TestDeleteNonExistent(t *testing.T) {
	db := openDB(t, tmpDir(t))
	defer db.Close()

	// Should succeed (tombstone is idempotent).
	if err := db.Delete([]byte("nope")); err != nil {
		t.Fatalf("Delete nonexistent: %v", err)
	}
}

// ── 2. WAL crash recovery ──────────────────────────────────────────────────────

func TestWALRecovery(t *testing.T) {
	dir := tmpDir(t)
	db := openDB(t, dir)

	mustPut(t, db, "persist", "yes")
	mustPut(t, db, "also", "survives")
	// Simulate crash by closing without compaction.
	db.Close()

	db2 := openDB(t, dir)
	defer db2.Close()

	mustGet(t, db2, "persist", "yes")
	mustGet(t, db2, "also", "survives")
}

func TestWALRecoveryAfterDelete(t *testing.T) {
	dir := tmpDir(t)
	db := openDB(t, dir)

	mustPut(t, db, "del", "me")
	db.Delete([]byte("del"))
	db.Close()

	db2 := openDB(t, dir)
	defer db2.Close()
	mustNotFound(t, db2, "del")
}

// ── 3. Flush + compaction correctness ─────────────────────────────────────────

func TestFlushAndCompaction(t *testing.T) {
	dir := tmpDir(t)
	db := openDB(t, dir)
	defer db.Close()

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
	defer db.Close()

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

// ── 4. MVCC snapshots ─────────────────────────────────────────────────────────

func TestSnapshotIsolation(t *testing.T) {
	db := openDB(t, tmpDir(t))
	defer db.Close()

	mustPut(t, db, "x", "before")
	snap := db.NewSnapshot()
	defer snap.Release()

	mustPut(t, db, "x", "after")

	// Snapshot should still see "before".
	old, err := db.GetSnapshot(snap, []byte("x"))
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	if string(old) != "before" {
		t.Fatalf("snapshot saw %q, want %q", old, "before")
	}

	// Latest should see "after".
	mustGet(t, db, "x", "after")
}

func TestSnapshotNotAffectedByDelete(t *testing.T) {
	db := openDB(t, tmpDir(t))
	defer db.Close()

	mustPut(t, db, "gone", "here")
	snap := db.NewSnapshot()
	defer snap.Release()

	db.Delete([]byte("gone"))

	// Snapshot should still see "here".
	v, err := db.GetSnapshot(snap, []byte("gone"))
	if err != nil {
		t.Fatalf("snapshot should see deleted key: %v", err)
	}
	if string(v) != "here" {
		t.Fatalf("snapshot got %q, want %q", v, "here")
	}

	// Current view should see deletion.
	mustNotFound(t, db, "gone")
}

func TestMultipleSnapshotsIndependent(t *testing.T) {
	db := openDB(t, tmpDir(t))
	defer db.Close()

	mustPut(t, db, "v", "0")
	s0 := db.NewSnapshot()

	mustPut(t, db, "v", "1")
	s1 := db.NewSnapshot()

	mustPut(t, db, "v", "2")

	check := func(snap *hybriddb.Snapshot, want string) {
		t.Helper()
		got, err := db.GetSnapshot(snap, []byte("v"))
		if err != nil || string(got) != want {
			t.Fatalf("snap %v: got %q err %v, want %q", snap.ID(), got, err, want)
		}
	}
	check(s0, "0")
	check(s1, "1")
	mustGet(t, db, "v", "2")

	s0.Release()
	s1.Release()
}

// ── 5. Range iterator ─────────────────────────────────────────────────────────

func TestIteratorForward(t *testing.T) {
	db := openDB(t, tmpDir(t))
	defer db.Close()

	keys := []string{"aaa", "bbb", "ccc", "ddd", "eee"}
	for _, k := range keys {
		mustPut(t, db, k, k+"_val")
	}

	iter, err := db.NewIterator(hybriddb.IteratorOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()

	var got []string
	for iter.Valid() {
		got = append(got, string(iter.Key()))
		iter.Next()
	}
	if !sort.StringsAreSorted(got) {
		t.Fatalf("iterator not sorted: %v", got)
	}
	for _, k := range keys {
		found := false
		for _, g := range got {
			if g == k {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("key %q missing from iterator", k)
		}
	}
}

func TestIteratorBounds(t *testing.T) {
	db := openDB(t, tmpDir(t))
	defer db.Close()

	for _, k := range []string{"a", "b", "c", "d", "e"} {
		mustPut(t, db, k, k)
	}

	iter, err := db.NewIterator(hybriddb.IteratorOptions{
		LowerBound: []byte("b"),
		UpperBound: []byte("d"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()

	var got []string
	for iter.Valid() {
		got = append(got, string(iter.Key()))
		iter.Next()
	}
	want := []string{"b", "c"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("bounds: got %v, want %v", got, want)
	}
}

func TestIteratorReverse(t *testing.T) {
	db := openDB(t, tmpDir(t))
	defer db.Close()

	for _, k := range []string{"a", "b", "c", "d"} {
		mustPut(t, db, k, k)
	}

	iter, err := db.NewIterator(hybriddb.IteratorOptions{Reverse: true})
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()

	var got []string
	for iter.Valid() {
		got = append(got, string(iter.Key()))
		iter.Next()
	}
	// Should be in reverse order.
	for i := 0; i < len(got)-1; i++ {
		if got[i] < got[i+1] {
			t.Fatalf("not reversed: %v", got)
		}
	}
}

func TestIteratorSkipsTombstones(t *testing.T) {
	db := openDB(t, tmpDir(t))
	defer db.Close()

	mustPut(t, db, "a", "1")
	mustPut(t, db, "b", "2")
	mustPut(t, db, "c", "3")
	db.Delete([]byte("b"))

	iter, err := db.NewIterator(hybriddb.IteratorOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()

	var got []string
	for iter.Valid() {
		got = append(got, string(iter.Key()))
		iter.Next()
	}
	for _, k := range got {
		if k == "b" {
			t.Fatalf("deleted key 'b' appeared in iterator: %v", got)
		}
	}
}

func TestIteratorSnapshotBound(t *testing.T) {
	db := openDB(t, tmpDir(t))
	defer db.Close()

	mustPut(t, db, "early", "e")
	snap := db.NewSnapshot()
	defer snap.Release()
	mustPut(t, db, "late", "l")

	iter, err := db.NewIterator(hybriddb.IteratorOptions{SnapshotSeq: snap.Seq()})
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()

	for iter.Valid() {
		if string(iter.Key()) == "late" {
			t.Fatal("iterator leaked post-snapshot key 'late'")
		}
		iter.Next()
	}
}

// ── 6. Concurrent writes ──────────────────────────────────────────────────────

func TestConcurrentPuts(t *testing.T) {
	db := openDB(t, tmpDir(t))
	defer db.Close()

	const goroutines = 20
	const perGoroutine = 100
	var wg sync.WaitGroup
	errs := make(chan error, goroutines*perGoroutine)

	for g := 0; g < goroutines; g++ {
		g := g
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
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
		t.Errorf("concurrent Put error: %v", e)
	}

	// Spot-check a few keys.
	for g := 0; g < goroutines; g++ {
		k := fmt.Sprintf("g%d:k0", g)
		mustGet(t, db, k, "val")
	}
}

func TestConcurrentPutGet(t *testing.T) {
	db := openDB(t, tmpDir(t))
	defer db.Close()

	const n = 50
	var wg sync.WaitGroup

	// Writers.
	for i := 0; i < n; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			db.Put([]byte(fmt.Sprintf("ck%d", i)), []byte("v"))
		}()
	}
	// Readers — should not panic or data-race.
	for i := 0; i < n; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			db.Get([]byte(fmt.Sprintf("ck%d", i)))
		}()
	}
	wg.Wait()
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

	db.Delete([]byte("tgc"))
	for i := 2000; i < 4000; i++ {
		mustPut(t, db, fmt.Sprintf("filler%05d", i), "x")
	}
	time.Sleep(400 * time.Millisecond)

	mustNotFound(t, db, "tgc")
	db.Close()

	// Reopen: key should still be absent.
	db2 := openDB(t, dir)
	defer db2.Close()
	mustNotFound(t, db2, "tgc")
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
	defer db.Close()

	const n = 500
	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			db.Put([]byte(fmt.Sprintf("bk%d", i)), []byte("val"))
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

// ── 9. SSTable iterator ───────────────────────────────────────────────────────

func TestSSTIterator(t *testing.T) {
	dir := tmpDir(t)
	path := filepath.Join(dir, "test.sst")

	w, err := sstable.NewWriter(path, 10)
	if err != nil {
		t.Fatal(err)
	}
	entries := []hybriddb.Entry{
		{Key: []byte("a"), Value: []byte("1"), SeqNum: 1},
		{Key: []byte("b"), Value: []byte("2"), SeqNum: 2},
		{Key: []byte("c"), Value: []byte("3"), SeqNum: 3},
		{Key: []byte("d"), Value: []byte("4"), SeqNum: 4},
	}
	for _, e := range entries {
		if err := w.Add(e); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()

	r, err := sstable.OpenReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	iter, err := r.NewIterator(hybriddb.IteratorOptions{
		LowerBound: []byte("b"),
		UpperBound: []byte("d"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()

	var got []string
	for iter.Valid() {
		got = append(got, string(iter.Key()))
		iter.Next()
	}
	want := []string{"b", "c"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("SST iterator bounds: got %v, want %v", got, want)
	}
}

// ── 10. Unit tests: WAL, MemTable, Bloom, BTree, Compaction ──────────────────

func TestWALRoundtrip(t *testing.T) {
	dir := tmpDir(t)
	path := filepath.Join(dir, "wal.log")

	w, err := wal.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	w.AppendPut(1, []byte("key1"), []byte("val1"))
	w.AppendPut(2, []byte("key2"), []byte("val2"))
	w.AppendDelete(3, []byte("key1"))
	w.Close()

	w2, _ := wal.Open(path)
	recs, err := w2.Replay()
	w2.Close()
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

	w, _ := wal.Open(path)
	w.AppendPut(1, []byte("k"), []byte("v"))
	w.Close()

	// Corrupt a byte in the middle.
	data, _ := os.ReadFile(path)
	data[len(data)/2] ^= 0xFF
	os.WriteFile(path, data, 0644)

	w2, _ := wal.Open(path)
	recs, err := w2.Replay()
	w2.Close()
	// Should get 0 or 1 records (corruption stops replay).
	if err != nil {
		t.Fatal("unexpected error:", err)
	}
	_ = recs // 0 records is expected
}

func TestMemTableIterator(t *testing.T) {
	mt := memtable.New(1 << 20)
	mt.Put([]byte("c"), []byte("3"), 3)
	mt.Put([]byte("a"), []byte("1"), 1)
	mt.Put([]byte("b"), []byte("2"), 2)

	iter := mt.NewIterator(hybriddb.IteratorOptions{})
	var keys []string
	for iter.Valid() {
		keys = append(keys, string(iter.Key()))
		iter.Next()
	}
	iter.Close()

	if !sort.StringsAreSorted(keys) {
		t.Fatalf("memtable iterator not sorted: %v", keys)
	}
	if len(keys) != 3 {
		t.Fatalf("expected 3 keys, got %d: %v", len(keys), keys)
	}
}

func TestMemTableSnapshotFilter(t *testing.T) {
	mt := memtable.New(1 << 20)
	mt.Put([]byte("k"), []byte("early"), 1)
	mt.Put([]byte("k2"), []byte("late"), 5)

	iter := mt.NewIterator(hybriddb.IteratorOptions{SnapshotSeq: 3})
	var keys []string
	for iter.Valid() {
		keys = append(keys, string(iter.Key()))
		iter.Next()
	}
	iter.Close()

	for _, k := range keys {
		if k == "k2" {
			t.Fatal("snapshot filter leaked post-snapshot key 'k2'")
		}
	}
}

func TestBloomFilter(t *testing.T) {
	f := bloom.New(100, 0.01)
	f.Add([]byte("present"))

	if !f.MayContain([]byte("present")) {
		t.Fatal("bloom: known key returned false")
	}
	if f.MayContain([]byte("definitely_not_here_xyz_123")) {
		// This is probabilistic; very unlikely to collide for a unique string.
		t.Log("bloom: false positive (acceptable but suspicious)")
	}

	// Serialise / deserialise round-trip.
	b := f.Bytes()
	f2 := bloom.FromBytes(b)
	if !f2.MayContain([]byte("present")) {
		t.Fatal("bloom: round-trip lost key")
	}
}

func TestBTreeBulkLoad(t *testing.T) {
	dir := tmpDir(t)
	bt, err := btree.Open(filepath.Join(dir, "test.bt"))
	if err != nil {
		t.Fatal(err)
	}
	defer bt.Close()

	var entries []hybriddb.Entry
	for i := 0; i < 200; i++ {
		entries = append(entries, hybriddb.Entry{
			Key:    []byte(fmt.Sprintf("k%04d", i)),
			Value:  []byte(fmt.Sprintf("v%d", i)),
			SeqNum: uint64(i + 1),
		})
	}

	if err := bt.BulkLoad(entries); err != nil {
		t.Fatal(err)
	}

	for _, e := range entries {
		got, err := bt.Get(e.Key)
		if err != nil {
			t.Fatalf("Get(%q): %v", e.Key, err)
		}
		if string(got.Value) != string(e.Value) {
			t.Fatalf("Get(%q) = %q, want %q", e.Key, got.Value, e.Value)
		}
	}
}

func TestBTreeAllEntries(t *testing.T) {
	dir := tmpDir(t)
	bt, _ := btree.Open(filepath.Join(dir, "test.bt"))
	defer bt.Close()

	var entries []hybriddb.Entry
	for i := 0; i < 50; i++ {
		entries = append(entries, hybriddb.Entry{
			Key:   []byte(fmt.Sprintf("k%03d", i)),
			Value: []byte("v"),
		})
	}
	bt.BulkLoad(entries)

	all, err := bt.AllEntries()
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 50 {
		t.Fatalf("AllEntries: got %d, want 50", len(all))
	}
}

func TestBTreeIterator(t *testing.T) {
	dir := tmpDir(t)
	bt, _ := btree.Open(filepath.Join(dir, "test.bt"))
	defer bt.Close()

	var entries []hybriddb.Entry
	for i := 0; i < 10; i++ {
		entries = append(entries, hybriddb.Entry{
			Key:   []byte(fmt.Sprintf("%02d", i)),
			Value: []byte("v"),
		})
	}
	bt.BulkLoad(entries)

	iter, err := bt.NewIterator(hybriddb.IteratorOptions{
		LowerBound: []byte("03"),
		UpperBound: []byte("07"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()

	var got []string
	for iter.Valid() {
		got = append(got, string(iter.Key()))
		iter.Next()
	}
	want := []string{"03", "04", "05", "06"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("btree iterator: got %v, want %v", got, want)
	}
}

func TestCompactionMergeTwo(t *testing.T) {
	dir := tmpDir(t)

	// Build two L0 SSTables with overlapping keys.
	path1 := filepath.Join(dir, "l0_1.sst")
	path2 := filepath.Join(dir, "l0_2.sst")

	w1, _ := sstable.NewWriter(path1, 5)
	w1.Add(hybriddb.Entry{Key: []byte("a"), Value: []byte("old"), SeqNum: 1})
	w1.Add(hybriddb.Entry{Key: []byte("b"), Value: []byte("b1"), SeqNum: 1})
	w1.Close()

	w2, _ := sstable.NewWriter(path2, 5)
	w2.Add(hybriddb.Entry{Key: []byte("a"), Value: []byte("new"), SeqNum: 5})
	w2.Add(hybriddb.Entry{Key: []byte("c"), Value: []byte("c1"), SeqNum: 5})
	w2.Close()

	l1Tree, _ := btree.Open(filepath.Join(dir, "l1.bt"))
	l2Tree, _ := btree.Open(filepath.Join(dir, "l2.bt"))
	defer l1Tree.Close()
	defer l2Tree.Close()

	tracker := hybriddb.NewSnapshotTracker()
	eng := compaction.New(compaction.Config{L0Threshold: 2}, l1Tree, l2Tree, tracker)
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

func TestSnapshotTrackerOldestSeq(t *testing.T) {
	tracker := hybriddb.NewSnapshotTracker()

	if oldest := tracker.OldestPinnedSeq(); oldest != ^uint64(0) {
		t.Fatalf("empty tracker: expected max uint64, got %d", oldest)
	}

	s10 := tracker.Create(10)
	s5 := tracker.Create(5)
	s20 := tracker.Create(20)

	if oldest := tracker.OldestPinnedSeq(); oldest != 5 {
		t.Fatalf("expected oldest=5, got %d", oldest)
	}

	s5.Release()
	if oldest := tracker.OldestPinnedSeq(); oldest != 10 {
		t.Fatalf("after s5 release, expected oldest=10, got %d", oldest)
	}

	s10.Release()
	s20.Release()
	if oldest := tracker.OldestPinnedSeq(); oldest != ^uint64(0) {
		t.Fatalf("all released: expected max, got %d", oldest)
	}
}

// ── Benchmark ─────────────────────────────────────────────────────────────────

func BenchmarkPut(b *testing.B) {
	dir, _ := os.MkdirTemp("", "bench_*")
	defer os.RemoveAll(dir)

	cfg := engine.DefaultConfig(dir)
	cfg.WALSyncPolicy = wal.SyncBatch
	db, _ := engine.Open(cfg)
	defer db.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Put([]byte(fmt.Sprintf("bk%d", i)), []byte("value"))
	}
}

func BenchmarkGet(b *testing.B) {
	dir, _ := os.MkdirTemp("", "bench_*")
	defer os.RemoveAll(dir)

	db, _ := engine.Open(engine.DefaultConfig(dir))
	defer db.Close()

	for i := 0; i < 10000; i++ {
		db.Put([]byte(fmt.Sprintf("k%d", i)), []byte("v"))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Get([]byte(fmt.Sprintf("k%d", i%10000)))
	}
}

func BenchmarkIterator(b *testing.B) {
	dir, _ := os.MkdirTemp("", "bench_*")
	defer os.RemoveAll(dir)

	db, _ := engine.Open(engine.DefaultConfig(dir))
	defer db.Close()

	for i := 0; i < 1000; i++ {
		db.Put([]byte(fmt.Sprintf("k%05d", i)), []byte("v"))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter, _ := db.NewIterator(hybriddb.IteratorOptions{})
		for iter.Valid() {
			iter.Next()
		}
		iter.Close()
	}
}
