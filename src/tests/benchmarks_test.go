package tests

import (
	"fmt"
	"os"
	"testing"

	"github.com/Ari-Ghosh/flash-db/src/arc"
	"github.com/Ari-Ghosh/flash-db/src/engine"
	types "github.com/Ari-Ghosh/flash-db/src/types"
	"github.com/Ari-Ghosh/flash-db/src/wal"
)

func BenchmarkWALGroupCommit(b *testing.B) {
	dir, _ := os.MkdirTemp("", "bench_wal_*")
	defer os.RemoveAll(dir)
	cfg := engine.DefaultConfig(dir)
	cfg.WALSyncPolicy = wal.SyncBatch
	db, _ := engine.Open(cfg)
	defer func() { _ = db.Close() }()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_ = db.Put([]byte(fmt.Sprintf("bk%d", i)), []byte("v"))
			i++
		}
	})
}

func BenchmarkPut(b *testing.B) {
	dir, _ := os.MkdirTemp("", "bench_*")
	defer os.RemoveAll(dir)

	cfg := engine.DefaultConfig(dir)
	cfg.WALSyncPolicy = wal.SyncBatch
	db, _ := engine.Open(cfg)
	defer func() { _ = db.Close() }()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = db.Put([]byte(fmt.Sprintf("bk%d", i)), []byte("value"))
	}
}

func BenchmarkGet(b *testing.B) {
	dir, _ := os.MkdirTemp("", "bench_*")
	defer os.RemoveAll(dir)

	db, _ := engine.Open(engine.DefaultConfig(dir))
	defer func() { _ = db.Close() }()

	for i := 0; i < 10000; i++ {
		_ = db.Put([]byte(fmt.Sprintf("k%d", i)), []byte("v"))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = db.Get([]byte(fmt.Sprintf("k%d", i%10000)))
	}
}

func BenchmarkIterator(b *testing.B) {
	dir, _ := os.MkdirTemp("", "bench_*")
	defer os.RemoveAll(dir)

	db, _ := engine.Open(engine.DefaultConfig(dir))
	defer func() { _ = db.Close() }()

	for i := 0; i < 1000; i++ {
		_ = db.Put([]byte(fmt.Sprintf("k%05d", i)), []byte("v"))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter, _ := db.NewIterator(types.IteratorOptions{})
		for iter.Valid() {
			iter.Next()
		}
		_ = iter.Close()
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Benchmarks
// ─────────────────────────────────────────────────────────────────────────────

func BenchmarkWriteBatch10(b *testing.B) {
	dir, _ := os.MkdirTemp("", "bench_wb_*")
	defer os.RemoveAll(dir)
	db, _ := engine.Open(engine.DefaultConfig(dir))
	defer func() { _ = db.Close() }()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wb := db.NewWriteBatch()
		for j := 0; j < 10; j++ {
			wb.Put([]byte(fmt.Sprintf("bk%d:%d", i, j)), []byte("v"))
		}
		_ = wb.Commit()
	}
}

func BenchmarkARCGet(b *testing.B) {
	c := arc.New[[]byte](1024)
	for i := 0; i < 1024; i++ {
		c.Put(uint64(i), []byte("value"))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Get(uint64(i % 1024))
	}
}

func BenchmarkIndexedPut(b *testing.B) {
	dir, _ := os.MkdirTemp("", "bench_idx_*")
	defer os.RemoveAll(dir)
	db, _ := engine.Open(engine.DefaultConfig(dir))
	defer func() { _ = db.Close() }()
	_ = db.DefineIndex(engine.IndexDefinition{Name: "bench", KeyFn: emailIndex})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = db.PutIndexed(
			[]byte(fmt.Sprintf("k%d", i)),
			[]byte(fmt.Sprintf("val%d@example.com", i)),
		)
	}
}
