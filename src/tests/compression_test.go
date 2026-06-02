package tests

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Ari-Ghosh/flash-db/src/btree"
	"github.com/Ari-Ghosh/flash-db/src/engine"
	"github.com/Ari-Ghosh/flash-db/src/sstable"
	types "github.com/Ari-Ghosh/flash-db/src/types"
)

func TestTieredCompression(t *testing.T) {
	dir := tmpDir(t)
	cfg := engine.DefaultConfig(dir)
	cfg.MemTableSize = 64 * 1024     // Small MemTable to trigger flushes
	cfg.L0CompactThreshold = 2       // 2 L0 files triggers L0->L1 compaction
	cfg.L1SizeThreshold = 128 * 1024 // Small L1 to trigger L1->L2 compaction

	db, err := engine.Open(cfg)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	// 1. Generate L0 SSTables (should use Snappy)
	for i := 0; i < 1000; i++ {
		mustPut(t, db, fmt.Sprintf("key%04d", i), "value"+fmt.Sprintf("%d", i))
	}

	// Wait for flushes and compactions
	time.Sleep(2 * time.Second)

	// Check L0 files (if any remain)
	matches, _ := filepath.Glob(filepath.Join(dir, "l0_*.sst"))
	for _, p := range matches {
		r, err := sstable.OpenReader(p)
		if err != nil {
			t.Errorf("failed to open SSTable %s: %v", p, err)
			continue
		}
		if r.Metadata().Codec != types.CodecSnappy {
			t.Errorf("L0 SSTable %s using codec %v, want Snappy", p, r.Metadata().Codec)
		}
		_ = r.Close()
	}

	// 2. Check if data is readable (verifies L1/L2 read/write)
	for i := 0; i < 1000; i++ {
		mustGet(t, db, fmt.Sprintf("key%04d", i), "value"+fmt.Sprintf("%d", i))
	}

	t.Log("Tiered compression test passed basic verification")
}

func TestBTreeCompression(t *testing.T) {
	dir := tmpDir(t)

	t.Run("L1-Snappy", func(t *testing.T) {
		path := filepath.Join(dir, "l1.bt")
		comp := types.NewCompressor(types.CodecSnappy)
		bt, err := btree.OpenWithCompressor(path, comp)
		if err != nil {
			t.Fatal(err)
		}

		var entries []types.Entry
		for i := 0; i < 100; i++ {
			entries = append(entries, types.Entry{
				Key:   []byte(fmt.Sprintf("k%04d", i)),
				Value: []byte("value" + fmt.Sprintf("%d", i)),
			})
		}
		if err := bt.BulkLoad(entries); err != nil {
			t.Fatal(err)
		}
		_ = bt.Close()

		// Read first page (after 512-byte header)
		f, err := os.Open(filepath.Clean(path))
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		buf := make([]byte, 8)
		_, _ = f.ReadAt(buf, 512)
		if buf[4] != byte(types.CodecSnappy) {
			t.Errorf("expected codec Snappy (1) in page header, got %d", buf[4])
		}
	})

	t.Run("L2-Zstd", func(t *testing.T) {
		path := filepath.Join(dir, "l2.bt")
		comp := types.NewCompressor(types.CodecZstd)
		bt, err := btree.OpenWithCompressor(path, comp)
		if err != nil {
			t.Fatal(err)
		}

		var entries []types.Entry
		for i := 0; i < 100; i++ {
			entries = append(entries, types.Entry{
				Key:   []byte(fmt.Sprintf("k%04d", i)),
				Value: []byte("value" + fmt.Sprintf("%d", i)),
			})
		}
		if err := bt.BulkLoad(entries); err != nil {
			t.Fatal(err)
		}
		_ = bt.Close()

		// Read first page (after 512-byte header)
		f, err := os.Open(filepath.Clean(path))
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		buf := make([]byte, 8)
		_, _ = f.ReadAt(buf, 512)
		if buf[4] != byte(types.CodecZstd) {
			t.Errorf("expected codec Zstd (2) in page header, got %d", buf[4])
		}
	})
}

func TestCompressors(t *testing.T) {
	data := []byte("this is some highly compressible data. " +
		"compress compress compress compress compress compress compress compress!")

	codecs := []types.Codec{types.CodecSnappy, types.CodecZstd}

	for _, codec := range codecs {
		t.Run(fmt.Sprintf("Codec%d", codec), func(t *testing.T) {
			comp := types.NewCompressor(codec)
			compressed, err := comp.Compress(nil, data)
			if err != nil {
				t.Fatalf("Compress failed: %v", err)
			}

			if len(compressed) >= len(data) && codec == types.CodecZstd {
				t.Errorf("Zstd failed to compress: original %d, compressed %d", len(data), len(compressed))
			}

			decompressed, err := comp.Decompress(nil, compressed)
			if err != nil {
				t.Fatalf("Decompress failed: %v", err)
			}

			if !bytes.Equal(decompressed, data) {
				t.Errorf("Decompressed data mismatch")
			}
		})
	}
}
