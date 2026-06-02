package tests

import (
	"bytes"
	"fmt"
	"path/filepath"
	"sort"
	"testing"

	"github.com/Ari-Ghosh/flash-db/src/bloom"
	"github.com/Ari-Ghosh/flash-db/src/btree"
	"github.com/Ari-Ghosh/flash-db/src/memtable"
	types "github.com/Ari-Ghosh/flash-db/src/types"
)

func TestMemTableIterator(t *testing.T) {
	mt := memtable.New(1 << 20)
	_ = mt.Put([]byte("c"), []byte("3"), 3)
	_ = mt.Put([]byte("a"), []byte("1"), 1)
	_ = mt.Put([]byte("b"), []byte("2"), 2)

	iter := mt.NewIterator(types.IteratorOptions{})
	var keys []string
	for iter.Valid() {
		keys = append(keys, string(iter.Key()))
		iter.Next()
	}
	_ = iter.Close()

	if !sort.StringsAreSorted(keys) {
		t.Fatalf("memtable iterator not sorted: %v", keys)
	}
	if len(keys) != 3 {
		t.Fatalf("expected 3 keys, got %d: %v", len(keys), keys)
	}
}

func TestMemTableSnapshotFilter(t *testing.T) {
	mt := memtable.New(1 << 20)
	_ = mt.Put([]byte("k"), []byte("early"), 1)
	_ = mt.Put([]byte("k2"), []byte("late"), 5)

	iter := mt.NewIterator(types.IteratorOptions{SnapshotSeq: 3})
	var keys []string
	for iter.Valid() {
		keys = append(keys, string(iter.Key()))
		iter.Next()
	}
	_ = iter.Close()

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
	bt, err := btree.Open(filepath.Clean(filepath.Join(dir, "test.bt")))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = bt.Close() }()

	var entries []types.Entry
	for i := 0; i < 200; i++ {
		entries = append(entries, types.Entry{
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
		if !bytes.Equal(got.Value, e.Value) {
			t.Fatalf("Get(%q) = %q, want %q", e.Key, got.Value, e.Value)
		}
	}
}

func TestBTreeAllEntries(t *testing.T) {
	dir := tmpDir(t)
	bt, _ := btree.Open(filepath.Clean(filepath.Join(dir, "test.bt")))
	defer func() { _ = bt.Close() }()

	var entries []types.Entry
	for i := 0; i < 50; i++ {
		entries = append(entries, types.Entry{
			Key:   []byte(fmt.Sprintf("k%03d", i)),
			Value: []byte("v"),
		})
	}
	_ = bt.BulkLoad(entries)

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
	bt, _ := btree.Open(filepath.Clean(filepath.Join(dir, "test.bt")))
	defer func() { _ = bt.Close() }()

	var entries []types.Entry
	for i := 0; i < 10; i++ {
		entries = append(entries, types.Entry{
			Key:   []byte(fmt.Sprintf("%02d", i)),
			Value: []byte("v"),
		})
	}
	_ = bt.BulkLoad(entries)

	iter, err := bt.NewIterator(types.IteratorOptions{
		LowerBound: []byte("03"),
		UpperBound: []byte("07"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = iter.Close() }()

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
