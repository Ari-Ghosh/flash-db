package tests

import (
	"errors"
	"net"
	"os"
	"testing"

	"github.com/Ari-Ghosh/flash-db/src/engine"
	types "github.com/Ari-Ghosh/flash-db/src/types"
)

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
func tmpDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "flashdb_test_*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
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
	if !errors.Is(err, types.ErrKeyNotFound) && !errors.Is(err, types.ErrKeyDeleted) {
		t.Fatalf("Get(%q) = %v, want ErrKeyNotFound/ErrKeyDeleted", k, err)
	}
}

func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	_, port, err := net.SplitHostPort(l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	return ":" + port
}
