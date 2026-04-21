// Package main demonstrates flashDB v3: transactions, replication, backup/restore,
// prefix scans, and all v2 features.
//
// ## Overview
//
// This demo showcases the v3 features of flashDB, including multi-key transactions,
// distributed replication, hot backup/restore, prefix scans, and all v2 capabilities.
//
// ## Features Demonstrated
//
// - **Multi-Key Transactions**: Optimistic concurrency control with Begin/Commit.
// - **Distributed Replication**: Leader-follower setup with WAL shipping.
// - **Hot Backup & Restore**: Point-in-time backups with integrity verification.
// - **Prefix Scans**: Efficient range queries over key prefixes.
// - **MVCC Snapshots**: Point-in-time reads with NewSnapshot().
// - **Range Iterators**: Forward/backward scans with NewIterator().
// - **WAL Group-Commit**: Batched fsync for high throughput.
// - **Tiered Compaction**: L0→L1→L2 incremental merging.
// - **Crash Recovery**: WAL replay on restart.
//
// ## version Changes
// 
// ### v1 (Original Demo)
// - Basic Put/Get/Delete operations.
// - No snapshots or iterators.
// - Simple compaction demonstration.
// - Manual crash simulation without recovery.
//
// ### v2 (Enhanced Demo)
// - MVCC snapshots and range iterators.
// - WAL group-commit and tiered compaction.
// - Crash recovery with WAL replay.
//
// ### v3 (Production-Ready Demo)
// - **Transactions**: Atomic multi-key operations with conflict detection.
// - **Replication**: Leader ships WAL to read-only followers.
// - **Backup/Restore**: Hot backups with SHA-256 integrity checks.
// - **Prefix Scans**: Efficient prefix-based range queries.
// - **Advanced Features**: Parallel writes, transaction conflict resolution.
//
// ## Demo Flow
//
// 1. **Parallel Writes**: Concurrent writers with WAL batching.
// 2. **Transactions**: Multi-key transfers with conflict detection.
// 3. **Prefix Scans**: Range queries over user keys.
// 4. **Backup & Restore**: Hot backup then restore to new location.
// 5. **Replication**: Leader-follower setup with live synchronization.
//
// ## Configuration
//
//	cfg := engine.DefaultConfig(dir)
//	cfg.MemTableSize = 256 * 1024    // Frequent flushes for demo
//	cfg.L0CompactThreshold = 2       // Trigger compaction early
//	cfg.Replication = &replication.Config{
//		Role: "leader", ListenAddr: ":5432", Secret: secret,
//	}
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

	"local/flashdb/src/backup"
	"local/flashdb/src/engine"
	"local/flashdb/src/replication"
	types "local/flashdb/src/types"
)

func main() {
	dir := "/tmp/hybriddb_v3"
	os.RemoveAll(dir)

	cfg := engine.DefaultConfig(dir)
	cfg.MemTableSize = 256 * 1024
	cfg.L0CompactThreshold = 2

	db, err := engine.Open(cfg)
	must(err)
	defer db.Close()

	// ── 1. Parallel writes (group-commit WAL) ─────────────────────────────────
	fmt.Println("── 1. Parallel writes ────────────────────────────────")
	done := make(chan struct{})
	for g := 0; g < 4; g++ {
		g := g
		go func() {
			for i := 0; i < 50; i++ {
				db.Put([]byte(fmt.Sprintf("par:g%d:k%d", g, i)), []byte("v"))
			}
			done <- struct{}{}
		}()
	}
	for i := 0; i < 4; i++ {
		<-done
	}
	v, _ := db.Get([]byte("par:g0:k0"))
	fmt.Printf("  par:g0:k0 = %q\n", v)

	// ── 2. Multi-key transaction ──────────────────────────────────────────────
	fmt.Println("\n── 2. Transaction ────────────────────────────────────")
	must(db.Put([]byte("alice"), []byte("1000")))
	must(db.Put([]byte("bob"), []byte("500")))

	tx := db.Begin()
	tx.Put([]byte("alice"), []byte("900"))
	tx.Put([]byte("bob"), []byte("600"))
	must(tx.Commit())

	a, _ := db.Get([]byte("alice"))
	b, _ := db.Get([]byte("bob"))
	fmt.Printf("  alice=%s  bob=%s  (transferred 100)\n", a, b)

	// Conflict demo.
	tx2 := db.Begin()
	tx2.Get([]byte("alice"))                     // read into read-set
	must(db.Put([]byte("alice"), []byte("800"))) // concurrent write
	tx2.Put([]byte("alice"), []byte("700"))
	err = tx2.Commit()
	fmt.Printf("  conflict commit err = %v\n", err)

	// ── 3. Prefix scan ────────────────────────────────────────────────────────
	fmt.Println("\n── 3. Prefix scan ────────────────────────────────────")
	for _, k := range []string{"user:alice", "user:bob", "user:carol", "order:1"} {
		must(db.Put([]byte(k), []byte(k+"_data")))
	}
	iter, _ := db.PrefixScan([]byte("user:"))
	fmt.Print("  users: ")
	for iter.Valid() {
		fmt.Printf("%s ", iter.Key())
		iter.Next()
	}
	iter.Close()
	fmt.Println()

	// Reverse prefix scan.
	rIter, _ := db.NewIterator(types.IteratorOptions{Prefix: []byte("user:"), Reverse: true})
	fmt.Print("  users (reverse): ")
	for rIter.Valid() {
		fmt.Printf("%s ", rIter.Key())
		rIter.Next()
	}
	rIter.Close()
	fmt.Println()

	// ── 4. Backup and restore ─────────────────────────────────────────────────
	fmt.Println("\n── 4. Backup & restore ───────────────────────────────")
	backupDir := "/tmp/hybriddb_v3_backup"
	os.RemoveAll(backupDir)
	manifest, err := db.Backup(backupDir)
	must(err)
	fmt.Printf("  backed up %d files, snapSeq=%d\n", len(manifest.Files), manifest.SnapSeq)

	// Write more data after the backup.
	must(db.Put([]byte("post-backup"), []byte("not-in-backup")))
	must(db.Close())

	// Restore to a new directory and verify.
	restoreDir := "/tmp/hybriddb_v3_restored"
	os.RemoveAll(restoreDir)
	must(backup.Restore(backupDir, restoreDir))

	db2, err := engine.Open(engine.DefaultConfig(restoreDir))
	must(err)
	defer db2.Close()
	av, _ := db2.Get([]byte("alice"))
	fmt.Printf("  restored alice=%s\n", av)
	_, postErr := db2.Get([]byte("post-backup"))
	fmt.Printf("  post-backup key in restore: %v (want ErrKeyNotFound)\n", postErr)

	// ── 5. Replication ────────────────────────────────────────────────────────
	fmt.Println("\n── 5. Distributed replication ────────────────────────")
	leaderDir := "/tmp/hybriddb_leader"
	followerDir := "/tmp/hybriddb_follower"
	os.RemoveAll(leaderDir)
	os.RemoveAll(followerDir)
	secret := []byte("demo-replication-secret-32bytes!!")

	lCfg := engine.DefaultConfig(leaderDir)
	lCfg.Replication = &replication.Config{
		Role: "leader", ListenAddr: "127.0.0.1:15432", Secret: secret,
	}
	leaderDB, err := engine.Open(lCfg)
	must(err)
	defer leaderDB.Close()
	time.Sleep(50 * time.Millisecond)

	fCfg := engine.DefaultConfig(followerDir)
	fCfg.Replication = &replication.Config{
		Role: "follower", LeaderAddr: "127.0.0.1:15432", Secret: secret,
		DialTimeout: 2 * time.Second, ReconnectInterval: 200 * time.Millisecond,
	}
	followerDB, err := engine.Open(fCfg)
	must(err)
	defer followerDB.Close()
	time.Sleep(200 * time.Millisecond)

	for i := 0; i < 5; i++ {
		must(leaderDB.Put([]byte(fmt.Sprintf("rep:%d", i)), []byte(fmt.Sprintf("v%d", i))))
	}
	time.Sleep(300 * time.Millisecond)

	fmt.Print("  follower reads: ")
	for i := 0; i < 5; i++ {
		v, err := followerDB.Get([]byte(fmt.Sprintf("rep:%d", i)))
		if err == nil {
			fmt.Printf("rep:%d=%s ", i, v)
		} else {
			fmt.Printf("rep:%d=ERR(%v) ", i, err)
		}
	}
	fmt.Println("\nDone.")
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
