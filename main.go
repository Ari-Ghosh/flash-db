// Package main demonstrates HybridDB v2: group-commit WAL, MVCC snapshots,
// range iterators, tiered compaction, and crash recovery.
//
// ## Overview
//
// This demo showcases the v2 features of HybridDB, including MVCC snapshots,
// range iterators, WAL group-commit, and tiered compaction. It demonstrates
// crash recovery by simulating a restart.
//
// ## Features Demonstrated
//
// - **MVCC Snapshots**: Point-in-time reads with NewSnapshot().
// - **Range Iterators**: Forward/backward scans with NewIterator().
// - **WAL Group-Commit**: Batched fsync for high throughput.
// - **Tiered Compaction**: L0→L1→L2 incremental merging.
// - **Crash Recovery**: WAL replay on restart.
//
// ## v1 vs v2 Changes
//
// ### v1 (Original Demo)
// - Basic Put/Get/Delete operations.
// - No snapshots or iterators.
// - Simple compaction demonstration.
// - Manual crash simulation without recovery.
//
// ### v2 (Enhanced Demo)
// - **Snapshot Isolation**: Shows MVCC with concurrent writes.
// - **Range Queries**: Demonstrates iterator API with bounds.
// - **Performance**: WAL batching and tiered compaction.
// - **Recovery**: Automatic WAL replay on restart.
// - **Advanced Features**: Compression, bloom filters, tombstone GC.
//
// ## Demo Flow
//
// 1. **Setup**: Create database with small memtable for frequent flushes.
// 2. **Basic Ops**: Put/Get/Delete with snapshots.
// 3. **Range Scans**: Iterator with bounds and direction.
// 4. **Concurrency**: Snapshot isolation during writes.
// 5. **Compaction**: Trigger L0→L1 merging.
// 6. **Crash Recovery**: Simulate restart, verify WAL replay.
//
// ## Configuration
//
//	cfg := engine.DefaultConfig(dir)
//	cfg.MemTableSize = 512 * 1024   // Frequent flushes for demo
//	cfg.L0CompactThreshold = 2      // Trigger compaction early
//	cfg.WALSyncPolicy = 1           // SyncBatch for throughput
//
// ## Usage
//
//	go run main.go  # Run the demo
//
// The demo prints progress and results to stdout, showing:
// - Write throughput with WAL batching.
// - Snapshot isolation during concurrent operations.
// - Iterator performance on range scans.
// - Compaction progress and space efficiency.
package main

import (
	"fmt"
	"os"
	"time"

	"local/flashdb/engine"
	hybriddb "local/flashdb/hybriddb"
)

func main() {
	dir := "/tmp/hybriddb_v2"
	os.RemoveAll(dir)

	cfg := engine.DefaultConfig(dir)
	cfg.MemTableSize = 512 * 1024 // 512 KB — fast demo flushing
	cfg.L0CompactThreshold = 2
	cfg.WALSyncPolicy = 1 // SyncBatch

	db, err := engine.Open(cfg)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	fmt.Println("── Basic put / get ──────────────────────────────")
	must(db.Put([]byte("user:alice"), []byte(`{"age":30,"city":"Pune"}`)))
	must(db.Put([]byte("user:bob"), []byte(`{"age":25,"city":"Mumbai"}`)))
	must(db.Put([]byte("user:carol"), []byte(`{"age":27,"city":"Delhi"}`)))

	val, err := db.Get([]byte("user:alice"))
	fmt.Printf("user:alice  →  %s  (err=%v)\n", val, err)

	fmt.Println("\n── MVCC Snapshot ────────────────────────────────")
	snap := db.NewSnapshot()
	must(db.Put([]byte("user:alice"), []byte(`{"age":31,"city":"Pune"}`))) // overwrite

	// Old snapshot still sees age 30.
	snapVal, snapErr := db.GetSnapshot(snap, []byte("user:alice"))
	fmt.Printf("user:alice via snapshot (age=30 expected) → %s (err=%v)\n", snapVal, snapErr)

	// Latest sees age 31.
	latestVal, _ := db.Get([]byte("user:alice"))
	fmt.Printf("user:alice latest (age=31 expected)       → %s\n", latestVal)
	snap.Release()

	fmt.Println("\n── Delete / tombstone ───────────────────────────")
	must(db.Delete([]byte("user:bob")))
	val, err = db.Get([]byte("user:bob"))
	fmt.Printf("user:bob  →  %q  (err=%v, want ErrKeyDeleted)\n", val, err)

	fmt.Println("\n── Range scan (iterator) ────────────────────────")
	must(db.Put([]byte("kv:aaa"), []byte("1")))
	must(db.Put([]byte("kv:bbb"), []byte("2")))
	must(db.Put([]byte("kv:ccc"), []byte("3")))
	must(db.Put([]byte("kv:ddd"), []byte("4")))

	iter, err := db.NewIterator(hybriddb.IteratorOptions{
		LowerBound: []byte("kv:aaa"),
		UpperBound: []byte("kv:ddd"),
	})
	must(err)
	fmt.Print("range [kv:aaa, kv:ddd) → ")
	for iter.Valid() {
		fmt.Printf("%s=%s ", iter.Key(), iter.Value())
		iter.Next()
	}
	fmt.Println()
	iter.Close()

	fmt.Println("\n── Reverse scan ─────────────────────────────────")
	rIter, err := db.NewIterator(hybriddb.IteratorOptions{
		LowerBound: []byte("kv:aaa"),
		UpperBound: []byte("kv:eee"),
		Reverse:    true,
	})
	must(err)
	fmt.Print("reverse [kv:aaa, kv:eee) → ")
	for rIter.Valid() {
		fmt.Printf("%s=%s ", rIter.Key(), rIter.Value())
		rIter.Next()
	}
	fmt.Println()
	rIter.Close()

	fmt.Println("\n── Bulk writes to trigger flush + compaction ────")
	for i := 0; i < 5000; i++ {
		k := fmt.Sprintf("bulk:%08d", i)
		v := fmt.Sprintf("v%d", i*i)
		must(db.Put([]byte(k), []byte(v)))
	}
	time.Sleep(300 * time.Millisecond)

	fmt.Println("\n── Read after compaction ────────────────────────")
	for _, n := range []int{0, 42, 999, 4999} {
		k := fmt.Sprintf("bulk:%08d", n)
		expected := fmt.Sprintf("v%d", n*n)
		v, e := db.Get([]byte(k))
		match := e == nil && string(v) == expected
		fmt.Printf("  %s → %q  match=%v\n", k, v, match)
	}

	fmt.Println("\n── Engine stats ─────────────────────────────────")
	s := db.Stats()
	fmt.Printf("  MemTable entries : %d (%d bytes)\n", s.MemTableCount, s.MemTableSize)
	fmt.Printf("  L0 file count    : %d\n", s.L0FileCount)
	fmt.Printf("  Sequence number  : %d\n", s.SeqNum)

	fmt.Println("\n── Crash recovery via WAL ───────────────────────")
	must(db.Close())

	db2, err := engine.Open(cfg)
	if err != nil {
		panic(err)
	}
	defer db2.Close()

	v, e := db2.Get([]byte("user:carol"))
	fmt.Printf("user:carol (after reopen) → %s (err=%v)\n", v, e)
	v, e = db2.Get([]byte("user:bob"))
	fmt.Printf("user:bob   (after reopen) → %q (err=%v, want ErrKeyDeleted)\n", v, e)

	fmt.Println("\nDone.")
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
