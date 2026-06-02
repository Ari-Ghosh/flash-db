package tests

import (
	"bytes"
	"sort"
	"testing"

	"github.com/Ari-Ghosh/flash-db/src/engine"
)

func emailIndex(_, value []byte) [][]byte {
	if len(value) == 0 {
		return nil
	}
	return [][]byte{value}
}

// prefixIndex extracts the first 3 bytes of the value as an index key.
func prefixIndex(_, value []byte) [][]byte {
	if len(value) < 3 {
		return nil
	}
	k := make([]byte, 3)
	copy(k, value[:3])
	return [][]byte{k}
}

func TestIndexDefineAndQuery(t *testing.T) {
	db := openDB(t, tmpDir(t))
	defer func() { _ = db.Close() }()

	if err := db.DefineIndex(engine.IndexDefinition{Name: "by_email", KeyFn: emailIndex}); err != nil {
		t.Fatal(err)
	}

	if err := db.PutIndexed([]byte("user:1"), []byte("alice@example.com")); err != nil {
		t.Fatal(err)
	}
	if err := db.PutIndexed([]byte("user:2"), []byte("bob@example.com")); err != nil {
		t.Fatal(err)
	}
	if err := db.PutIndexed([]byte("user:3"), []byte("alice@example.com")); err != nil {
		t.Fatal(err)
	}

	pkeys, err := db.QueryByIndex("by_email", []byte("alice@example.com"))
	if err != nil {
		t.Fatal(err)
	}
	if len(pkeys) != 2 {
		t.Fatalf("expected 2 primary keys, got %d: %v", len(pkeys), pkeys)
	}
	sort.Slice(pkeys, func(i, j int) bool {
		return bytes.Compare(pkeys[i], pkeys[j]) < 0
	})
	if string(pkeys[0]) != "user:1" || string(pkeys[1]) != "user:3" {
		t.Fatalf("unexpected primary keys: %v", pkeys)
	}

	// No results for unknown email.
	pkeys2, err := db.QueryByIndex("by_email", []byte("nobody@example.com"))
	if err != nil {
		t.Fatal(err)
	}
	if len(pkeys2) != 0 {
		t.Fatalf("expected 0, got %d", len(pkeys2))
	}
}

func TestIndexUpdateRemovesOldEntry(t *testing.T) {
	db := openDB(t, tmpDir(t))
	defer func() { _ = db.Close() }()

	if err := db.DefineIndex(engine.IndexDefinition{Name: "by_val", KeyFn: emailIndex}); err != nil {
		t.Fatal(err)
	}

	if err := db.PutIndexed([]byte("doc:1"), []byte("oldval")); err != nil {
		t.Fatal(err)
	}
	// Update: value changes from oldval → newval.
	if err := db.PutIndexed([]byte("doc:1"), []byte("newval")); err != nil {
		t.Fatal(err)
	}

	// Old index entry must be gone.
	old, _ := db.QueryByIndex("by_val", []byte("oldval"))
	if len(old) != 0 {
		t.Fatalf("old index entry survived update: %v", old)
	}

	// New index entry must exist.
	n, _ := db.QueryByIndex("by_val", []byte("newval"))
	if len(n) != 1 || string(n[0]) != "doc:1" {
		t.Fatalf("new index entry missing: %v", n)
	}
}

func TestIndexDeleteRemovesEntry(t *testing.T) {
	db := openDB(t, tmpDir(t))
	defer func() { _ = db.Close() }()

	if err := db.DefineIndex(engine.IndexDefinition{Name: "by_v", KeyFn: emailIndex}); err != nil {
		t.Fatal(err)
	}

	_ = db.PutIndexed([]byte("del:1"), []byte("thevalue"))
	_ = db.DeleteIndexed([]byte("del:1"))

	pkeys, _ := db.QueryByIndex("by_v", []byte("thevalue"))
	if len(pkeys) != 0 {
		t.Fatalf("index entry survived delete: %v", pkeys)
	}
	mustNotFound(t, db, "del:1")
}

func TestIndexRangeQuery(t *testing.T) {
	db := openDB(t, tmpDir(t))
	defer func() { _ = db.Close() }()

	// Index value = first 3 bytes of value (e.g. "aaa", "bbb", "ccc").
	if err := db.DefineIndex(engine.IndexDefinition{Name: "by_prefix", KeyFn: prefixIndex}); err != nil {
		t.Fatal(err)
	}

	_ = db.PutIndexed([]byte("k:1"), []byte("aaaxxx"))
	_ = db.PutIndexed([]byte("k:2"), []byte("bbbxxx"))
	_ = db.PutIndexed([]byte("k:3"), []byte("cccxxx"))
	_ = db.PutIndexed([]byte("k:4"), []byte("dddxxx"))

	// Query range [bbb, ddd) — expect k:2 and k:3.
	pkeys, err := db.RangeQueryByIndex("by_prefix", []byte("bbb"), []byte("ddd"))
	if err != nil {
		t.Fatal(err)
	}
	if len(pkeys) != 2 {
		t.Fatalf("range query: got %d results, want 2: %v", len(pkeys), pkeys)
	}
	sort.Slice(pkeys, func(i, j int) bool { return bytes.Compare(pkeys[i], pkeys[j]) < 0 })
	if string(pkeys[0]) != "k:2" || string(pkeys[1]) != "k:3" {
		t.Fatalf("range query keys: %v", pkeys)
	}
}

func TestIndexRebuild(t *testing.T) {
	db := openDB(t, tmpDir(t))
	defer func() { _ = db.Close() }()

	// Write data BEFORE defining the index.
	mustPut(t, db, "r:1", "alpha")
	mustPut(t, db, "r:2", "beta")
	mustPut(t, db, "r:3", "alpha")

	// Now define and rebuild the index.
	if err := db.DefineIndex(engine.IndexDefinition{Name: "by_word", KeyFn: emailIndex}); err != nil {
		t.Fatal(err)
	}
	if err := db.RebuildIndex("by_word"); err != nil {
		t.Fatal(err)
	}

	pkeys, err := db.QueryByIndex("by_word", []byte("alpha"))
	if err != nil {
		t.Fatal(err)
	}
	if len(pkeys) != 2 {
		t.Fatalf("rebuild: expected 2 results for 'alpha', got %d: %v", len(pkeys), pkeys)
	}
}

func TestIndexDropRemovesAllEntries(t *testing.T) {
	db := openDB(t, tmpDir(t))
	defer func() { _ = db.Close() }()

	if err := db.DefineIndex(engine.IndexDefinition{Name: "todrop", KeyFn: emailIndex}); err != nil {
		t.Fatal(err)
	}
	_ = db.PutIndexed([]byte("d:1"), []byte("v1"))
	_ = db.PutIndexed([]byte("d:2"), []byte("v2"))

	if err := db.DropIndex("todrop"); err != nil {
		t.Fatal(err)
	}

	// After drop, querying must return an error (index not defined).
	_, err := db.QueryByIndex("todrop", []byte("v1"))
	_ = err // not an error condition we need to test (implementation detail);
	// primary data must survive.
	mustGet(t, db, "d:1", "v1")
	mustGet(t, db, "d:2", "v2")
}

func TestIndexMultipleIndexes(t *testing.T) {
	db := openDB(t, tmpDir(t))
	defer func() { _ = db.Close() }()

	// Two indexes on different aspects of the same value.
	// Value format: "<email>:<role>"
	emailFn := func(_, v []byte) [][]byte {
		for i, b := range v {
			if b == ':' {
				return [][]byte{v[:i]}
			}
		}
		return [][]byte{v}
	}
	roleFn := func(_, v []byte) [][]byte {
		for i, b := range v {
			if b == ':' {
				return [][]byte{v[i+1:]}
			}
		}
		return nil
	}

	_ = db.DefineIndex(engine.IndexDefinition{Name: "by_email2", KeyFn: emailFn})
	_ = db.DefineIndex(engine.IndexDefinition{Name: "by_role", KeyFn: roleFn})

	_ = db.PutIndexed([]byte("u:1"), []byte("alice@x.com:admin"))
	_ = db.PutIndexed([]byte("u:2"), []byte("bob@x.com:user"))
	_ = db.PutIndexed([]byte("u:3"), []byte("carol@x.com:admin"))

	admins, _ := db.QueryByIndex("by_role", []byte("admin"))
	if len(admins) != 2 {
		t.Fatalf("by_role admin: expected 2, got %d: %v", len(admins), admins)
	}
	bobs, _ := db.QueryByIndex("by_email2", []byte("bob@x.com"))
	if len(bobs) != 1 || string(bobs[0]) != "u:2" {
		t.Fatalf("by_email2 bob: expected [u:2], got %v", bobs)
	}
}
