package tests

import (
	"fmt"
	"path/filepath"
	"sort"
	"testing"

	"github.com/Ari-Ghosh/flash-db/src/sstable"
	types "github.com/Ari-Ghosh/flash-db/src/types"
)

func TestIteratorForward(t *testing.T) {
	db := openDB(t, tmpDir(t))
	defer func() { _ = db.Close() }()

	keys := []string{"aaa", "bbb", "ccc", "ddd", "eee"}
	for _, k := range keys {
		mustPut(t, db, k, k+"_val")
	}

	iter, err := db.NewIterator(types.IteratorOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = iter.Close() }()

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
	defer func() { _ = db.Close() }()

	for _, k := range []string{"a", "b", "c", "d", "e"} {
		mustPut(t, db, k, k)
	}

	iter, err := db.NewIterator(types.IteratorOptions{
		LowerBound: []byte("b"),
		UpperBound: []byte("d"),
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
	want := []string{"b", "c"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("bounds: got %v, want %v", got, want)
	}
}

func TestIteratorReverse(t *testing.T) {
	db := openDB(t, tmpDir(t))
	defer func() { _ = db.Close() }()

	for _, k := range []string{"a", "b", "c", "d"} {
		mustPut(t, db, k, k)
	}

	iter, err := db.NewIterator(types.IteratorOptions{Reverse: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = iter.Close() }()

	var got []string
	for iter.Valid() {
		got = append(got, string(iter.Key()))
		iter.Next()
	}
	for i := 0; i < len(got)-1; i++ {
		if got[i] < got[i+1] {
			t.Fatalf("not reversed: %v", got)
		}
	}
}

func TestIteratorSkipsTombstones(t *testing.T) {
	db := openDB(t, tmpDir(t))
	defer func() { _ = db.Close() }()

	mustPut(t, db, "a", "1")
	mustPut(t, db, "b", "2")
	mustPut(t, db, "c", "3")
	_ = db.Delete([]byte("b"))

	iter, err := db.NewIterator(types.IteratorOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = iter.Close() }()

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
	defer func() { _ = db.Close() }()

	mustPut(t, db, "early", "e")
	snap := db.NewSnapshot()
	defer func() { snap.Release() }()
	mustPut(t, db, "late", "l")

	iter, err := db.NewIterator(types.IteratorOptions{SnapshotSeq: snap.Seq()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = iter.Close() }()

	for iter.Valid() {
		if string(iter.Key()) == "late" {
			t.Fatal("iterator leaked post-snapshot key 'late'")
		}
		iter.Next()
	}
}

// ── 9. SSTable iterator ───────────────────────────────────────────────────────

func TestSSTIterator(t *testing.T) {
	dir := tmpDir(t)
	path := filepath.Join(dir, "test.sst")

	w, err := sstable.NewWriter(filepath.Clean(path), 10)
	if err != nil {
		t.Fatal(err)
	}
	entries := []types.Entry{
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
	_ = w.Close()

	r, err := sstable.OpenReader(filepath.Clean(path))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()

	iter, err := r.NewIterator(types.IteratorOptions{
		LowerBound: []byte("b"),
		UpperBound: []byte("d"),
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
	want := []string{"b", "c"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("SST iterator bounds: got %v, want %v", got, want)
	}
}

// ── 3. Prefix scan ────────────────────────────────────────────────────────────

func TestPrefixScanBasic(t *testing.T) {
	db := openDB(t, tmpDir(t))
	for _, k := range []string{"user:alice", "user:bob", "user:carol", "order:1", "order:2"} {
		mustPut(t, db, k, k+"_val")
	}

	iter, err := db.PrefixScan([]byte("user:"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = iter.Close() }()

	var got []string
	for iter.Valid() {
		got = append(got, string(iter.Key()))
		iter.Next()
	}
	want := []string{"user:alice", "user:bob", "user:carol"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("prefix scan: got %v, want %v", got, want)
	}
}

func TestPrefixScanReverse(t *testing.T) {
	db := openDB(t, tmpDir(t))
	for _, k := range []string{"p:a", "p:b", "p:c", "q:x"} {
		mustPut(t, db, k, "v")
	}

	iter, err := db.NewIterator(types.IteratorOptions{
		Prefix:  []byte("p:"),
		Reverse: true,
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
	// Should be in reverse order and exclude q:x
	for _, k := range got {
		if string([]byte(k)[:2]) != "p:" {
			t.Fatalf("prefix leak: got key outside prefix: %s", k)
		}
	}
	if !sort.SliceIsSorted(got, func(i, j int) bool { return got[i] > got[j] }) {
		t.Fatalf("reverse prefix not sorted: %v", got)
	}
}

func TestPrefixScanAllFFBytes(t *testing.T) {
	// Prefix consisting entirely of 0xFF bytes — upper bound overflows to nil.
	db := openDB(t, tmpDir(t))
	ffKey := []byte{0xFF, 0x01}
	otherKey := []byte{0xFE, 0x00}
	_ = db.Put(ffKey, []byte("ff"))
	_ = db.Put(otherKey, []byte("other"))

	iter, err := db.NewIterator(types.IteratorOptions{Prefix: []byte{0xFF}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = iter.Close() }()

	var got []string
	for iter.Valid() {
		got = append(got, string(iter.Key()))
		iter.Next()
	}
	// Should only see keys starting with 0xFF
	for _, k := range got {
		if []byte(k)[0] != 0xFF {
			t.Fatalf("0xFF prefix scan: leaked non-matching key %x", k)
		}
	}
}

func TestPrefixScanEmpty(t *testing.T) {
	db := openDB(t, tmpDir(t))
	mustPut(t, db, "other:key", "v")

	iter, err := db.PrefixScan([]byte("missing:"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = iter.Close() }()

	if iter.Valid() {
		t.Fatalf("expected empty result, got key %s", iter.Key())
	}
}

func TestPrefixScanSkipsTombstones(t *testing.T) {
	db := openDB(t, tmpDir(t))
	mustPut(t, db, "ns:a", "1")
	mustPut(t, db, "ns:b", "2")
	mustPut(t, db, "ns:c", "3")
	_ = db.Delete([]byte("ns:b"))

	iter, err := db.PrefixScan([]byte("ns:"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = iter.Close() }()

	var got []string
	for iter.Valid() {
		got = append(got, string(iter.Key()))
		iter.Next()
	}
	for _, k := range got {
		if k == "ns:b" {
			t.Fatalf("prefix scan returned deleted key ns:b: %v", got)
		}
	}
}

func TestIteratorRegression(t *testing.T) {
	db := openDB(t, tmpDir(t))
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		mustPut(t, db, k, k)
	}
	iter, err := db.NewIterator(types.IteratorOptions{
		LowerBound: []byte("b"),
		UpperBound: []byte("d"),
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
	if fmt.Sprint(got) != fmt.Sprint([]string{"b", "c"}) {
		t.Fatalf("iterator regression: got %v", got)
	}
}
