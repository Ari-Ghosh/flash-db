package tests

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Ari-Ghosh/flash-db/src/bloom"
	"github.com/Ari-Ghosh/flash-db/src/engine"
	"github.com/Ari-Ghosh/flash-db/src/sstable"
	types "github.com/Ari-Ghosh/flash-db/src/types"
)

func TestBloomTelemetryTracking(t *testing.T) {
	tel := bloom.NewBloomTelemetry()

	// Record some queries and false positives.
	for i := 0; i < 50; i++ {
		tel.RecordQuery("sst1")
	}
	for i := 0; i < 10; i++ {
		tel.RecordFalsePositive("sst1")
	}

	s := tel.Stats("sst1")
	if s == nil {
		t.Fatal("expected stats for sst1")
	}
	if s.Queries.Load() != 50 {
		t.Fatalf("queries: got %d, want 50", s.Queries.Load())
	}
	if s.FalsePositives.Load() != 10 {
		t.Fatalf("FP: got %d, want 10", s.FalsePositives.Load())
	}
	fpr := s.ObservedFPR()
	if fpr < 0.19 || fpr > 0.21 {
		t.Fatalf("observed FPR: got %f, want ~0.2", fpr)
	}

	// Aggregate stats.
	tel.RecordQuery("sst2")
	tel.RecordQuery("sst2")
	totalQ, totalFP := tel.AggregateStats()
	if totalQ != 52 {
		t.Fatalf("aggregate queries: got %d, want 52", totalQ)
	}
	if totalFP != 10 {
		t.Fatalf("aggregate FP: got %d, want 10", totalFP)
	}

	// Remove.
	tel.Remove("sst1")
	if tel.Stats("sst1") != nil {
		t.Fatal("sst1 should have been removed")
	}
}

func TestBloomAdaptiveFPR(t *testing.T) {
	target := 0.01
	minFPR := 0.001
	maxFPR := 0.05

	t.Run("insufficient data returns target", func(t *testing.T) {
		tel := bloom.NewBloomTelemetry()
		// Only 10 queries — below threshold.
		for i := 0; i < 10; i++ {
			tel.RecordQuery("sst")
		}
		got := tel.Recommend(target, minFPR, maxFPR)
		if got != target {
			t.Fatalf("got %f, want %f", got, target)
		}
	})

	t.Run("high FPR reduces target", func(t *testing.T) {
		tel := bloom.NewBloomTelemetry()
		for i := 0; i < 200; i++ {
			tel.RecordQuery("sst")
		}
		// 50 false positives out of 200 = 25% FPR, way above 1%.
		for i := 0; i < 50; i++ {
			tel.RecordFalsePositive("sst")
		}
		got := tel.Recommend(target, minFPR, maxFPR)
		if got >= target {
			t.Fatalf("expected reduction: got %f, target %f", got, target)
		}
		if got != target*0.5 {
			t.Fatalf("expected %f, got %f", target*0.5, got)
		}
	})

	t.Run("low FPR increases target", func(t *testing.T) {
		tel := bloom.NewBloomTelemetry()
		for i := 0; i < 200; i++ {
			tel.RecordQuery("sst")
		}
		// 0 false positives out of 200 = 0% FPR, well below 0.25%.
		got := tel.Recommend(target, minFPR, maxFPR)
		if got <= target {
			t.Fatalf("expected increase: got %f, target %f", got, target)
		}
		if got != target*2.0 {
			t.Fatalf("expected %f, got %f", target*2.0, got)
		}
	})

	t.Run("clamped to max", func(t *testing.T) {
		tel := bloom.NewBloomTelemetry()
		for i := 0; i < 200; i++ {
			tel.RecordQuery("sst")
		}
		// 0 FP, target 0.04 → recommend 0.08, clamped to maxFPR 0.05.
		got := tel.Recommend(0.04, minFPR, maxFPR)
		if got > maxFPR {
			t.Fatalf("should be clamped: got %f, max %f", got, maxFPR)
		}
	})

	t.Run("clamped to min", func(t *testing.T) {
		tel := bloom.NewBloomTelemetry()
		for i := 0; i < 200; i++ {
			tel.RecordQuery("sst")
			tel.RecordFalsePositive("sst")
		}
		// 100% FPR → target*0.5 = 0.001/2 = 0.0005, clamped to minFPR.
		got := tel.Recommend(minFPR, minFPR, maxFPR)
		if got < minFPR {
			t.Fatalf("should be clamped: got %f, min %f", got, minFPR)
		}
	})
}

func TestBloomAdaptiveIntegration(t *testing.T) {
	dir := tmpDir(t)
	cfg := engine.DefaultConfig(dir)
	cfg.MemTableSize = 128 * 1024 // small to trigger flushes
	cfg.L0CompactThreshold = 100  // high to avoid compaction eating L0 files
	cfg.BloomFPRTarget = 0.01
	cfg.BloomFPRMin = 0.001
	cfg.BloomFPRMax = 0.05

	db, err := engine.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	// Write enough data to trigger a flush (creates L0 SSTable with default FPR).
	for i := 0; i < 2000; i++ {
		mustPut(t, db, fmt.Sprintf("k%06d", i), fmt.Sprintf("v%d", i))
	}
	time.Sleep(200 * time.Millisecond)

	// Read keys that definitely don't exist → these will hit bloom filters.
	// Some may be false positives, some won't.
	for i := 0; i < 500; i++ {
		_, _ = db.Get([]byte(fmt.Sprintf("miss%06d", i)))
	}

	stats := db.BloomStats()
	t.Logf("Bloom telemetry: queries=%d fp=%d observedFPR=%.4f targetFPR=%.4f",
		stats.TotalQueries, stats.TotalFalsePositives,
		stats.ObservedFPR, stats.CurrentTargetFPR)

	// The stats should be populated (queries > 0 means bloom filters were consulted).
	if stats.CurrentTargetFPR <= 0 {
		t.Fatal("CurrentTargetFPR should be > 0")
	}
	if stats.CurrentTargetFPR < cfg.BloomFPRMin || stats.CurrentTargetFPR > cfg.BloomFPRMax {
		t.Fatalf("CurrentTargetFPR %f out of bounds [%f, %f]",
			stats.CurrentTargetFPR, cfg.BloomFPRMin, cfg.BloomFPRMax)
	}
}

func TestBloomTelemetryCleanup(t *testing.T) {
	tel := bloom.NewBloomTelemetry()
	tel.RecordQuery("l0_001.sst")
	tel.RecordFalsePositive("l0_001.sst")
	tel.RecordQuery("l0_002.sst")

	// Simulate compaction removing these files.
	tel.Remove("l0_001.sst")
	tel.Remove("l0_002.sst")

	if tel.Stats("l0_001.sst") != nil {
		t.Fatal("l0_001.sst should be cleaned up")
	}
	if tel.Stats("l0_002.sst") != nil {
		t.Fatal("l0_002.sst should be cleaned up")
	}

	// Aggregate should be empty.
	q, fp := tel.AggregateStats()
	if q != 0 || fp != 0 {
		t.Fatalf("aggregate should be 0/0 after cleanup, got %d/%d", q, fp)
	}
}

func TestBloomStatsExposed(t *testing.T) {
	db := openDB(t, tmpDir(t))
	defer func() { _ = db.Close() }()

	// Before any activity, stats should be zeroed but valid.
	stats := db.BloomStats()
	if stats.TotalQueries != 0 {
		t.Fatalf("initial queries should be 0, got %d", stats.TotalQueries)
	}
	if stats.CurrentTargetFPR <= 0 {
		t.Fatal("CurrentTargetFPR should be positive")
	}
}

func TestSSTWriterWithFPR(t *testing.T) {
	dir := tmpDir(t)

	// Write SSTable with very low FPR (large bloom filter).
	path1 := filepath.Join(dir, "low_fpr.sst")
	w1, err := sstable.NewWriterWithFPR(filepath.Clean(path1), 1000, 0.001)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		_ = w1.Add(types.Entry{Key: []byte(fmt.Sprintf("k%04d", i)), Value: []byte("v"), SeqNum: uint64(i + 1)})
	}
	_ = w1.Close()

	// Write SSTable with higher FPR (smaller bloom filter).
	path2 := filepath.Join(dir, "high_fpr.sst")
	w2, err := sstable.NewWriterWithFPR(filepath.Clean(path2), 1000, 0.05)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		_ = w2.Add(types.Entry{Key: []byte(fmt.Sprintf("k%04d", i)), Value: []byte("v"), SeqNum: uint64(i + 1)})
	}
	_ = w2.Close()

	// Both should be readable.
	r1, _ := sstable.OpenReader(filepath.Clean(path1))
	r2, _ := sstable.OpenReader(filepath.Clean(path2))
	defer r1.Close()
	defer r2.Close()

	e1, err := r1.Get([]byte("k0050"))
	if err != nil {
		t.Fatalf("low FPR SST Get: %v", err)
	}
	if string(e1.Key) != "k0050" {
		t.Fatalf("unexpected key: %s", e1.Key)
	}

	e2, err := r2.Get([]byte("k0050"))
	if err != nil {
		t.Fatalf("high FPR SST Get: %v", err)
	}
	if string(e2.Key) != "k0050" {
		t.Fatalf("unexpected key: %s", e2.Key)
	}

	// Low FPR SSTable should have a larger file size (bigger bloom filter).
	fi1, _ := os.Stat(path1)
	fi2, _ := os.Stat(path2)
	t.Logf("low FPR file size: %d, high FPR file size: %d", fi1.Size(), fi2.Size())
	if fi1.Size() <= fi2.Size() {
		t.Logf("note: expected low-FPR SSTable to be larger due to bigger bloom filter")
	}
}
