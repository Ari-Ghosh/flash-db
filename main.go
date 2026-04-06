// Package main demonstrates HybridDB: an LSM write path + B-tree read path.
package main

import (
	"fmt"
	"os"
	"time"

	"local/flashdb/engine"
)

func main() {
	dir := "/tmp/hybriddb_demo"
	os.RemoveAll(dir) // fresh start for demo

	cfg := engine.DefaultConfig(dir)
	cfg.MemTableSize = 1 * 1024 * 1024 // 1 MB for fast demo flushing
	cfg.L0CompactThreshold = 2         // compact after 2 L0 files

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

	fmt.Println("\n── Overwrite ────────────────────────────────────")
	must(db.Put([]byte("user:alice"), []byte(`{"age":31,"city":"Pune"}`))) // birthday!
	val, err = db.Get([]byte("user:alice"))
	fmt.Printf("user:alice  →  %s  (err=%v)\n", val, err)

	fmt.Println("\n── Delete / tombstone ───────────────────────────")
	must(db.Delete([]byte("user:bob")))
	val, err = db.Get([]byte("user:bob"))
	fmt.Printf("user:bob    →  %q  (err=%v)\n", val, err)

	fmt.Println("\n── Bulk writes to trigger flush + compaction ────")
	for i := 0; i < 5000; i++ {
		k := fmt.Sprintf("kv:%08d", i)
		v := fmt.Sprintf("value-%d", i*i)
		must(db.Put([]byte(k), []byte(v)))
	}

	// Give compaction a moment to finish in the background.
	time.Sleep(200 * time.Millisecond)

	fmt.Println("\n── Read after compaction ────────────────────────")
	for _, n := range []int{0, 42, 999, 4999} {
		k := fmt.Sprintf("kv:%08d", n)
		expected := fmt.Sprintf("value-%d", n*n)
		val, err := db.Get([]byte(k))
		match := err == nil && string(val) == expected
		fmt.Printf("  %s  →  %q  match=%v\n", k, val, match)
	}

	fmt.Println("\n── Engine stats ─────────────────────────────────")
	s := db.Stats()
	fmt.Printf("  MemTable entries : %d  (%d bytes)\n", s.MemTableCount, s.MemTableSize)
	fmt.Printf("  L0 file count    : %d\n", s.L0FileCount)
	fmt.Printf("  Sequence number  : %d\n", s.SeqNum)

	fmt.Println("\n── Reopen (crash recovery via WAL) ──────────────")
	must(db.Close())
	db2, err := engine.Open(cfg)
	if err != nil {
		panic(err)
	}
	defer db2.Close()

	val, err = db2.Get([]byte("user:carol"))
	fmt.Printf("user:carol (after reopen)  →  %s  (err=%v)\n", val, err)

	val, err = db2.Get([]byte("user:bob"))
	fmt.Printf("user:bob   (after reopen)  →  %q  (err=%v, should be ErrKeyDeleted)\n", val, err)

	fmt.Println("\nDone.")
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
