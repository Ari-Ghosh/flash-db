package tests

import (
	"sort"
	"testing"

	types "github.com/Ari-Ghosh/flash-db/src/types"
)

func TestColumnFamilyBasic(t *testing.T) {
	db := openDB(t, tmpDir(t))
	defer func() { _ = db.Close() }()

	if err := db.CreateColumnFamily("users"); err != nil {
		t.Fatal(err)
	}
	cf, err := db.GetColumnFamily("users")
	if err != nil {
		t.Fatal(err)
	}

	if err := cf.Put([]byte("alice"), []byte("alice@example.com")); err != nil {
		t.Fatal(err)
	}
	v, err := cf.Get([]byte("alice"))
	if err != nil {
		t.Fatalf("cf.Get: %v", err)
	}
	if string(v) != "alice@example.com" {
		t.Fatalf("got %q, want alice@example.com", v)
	}
}

func TestColumnFamilyIsolation(t *testing.T) {
	db := openDB(t, tmpDir(t))
	defer func() { _ = db.Close() }()

	// Same key in two different column families must be independent.
	if err := db.CreateColumnFamily("cf1"); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateColumnFamily("cf2"); err != nil {
		t.Fatal(err)
	}
	cf1, _ := db.GetColumnFamily("cf1")
	cf2, _ := db.GetColumnFamily("cf2")

	if err := cf1.Put([]byte("shared"), []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if err := cf2.Put([]byte("shared"), []byte("v2")); err != nil {
		t.Fatal(err)
	}

	v1, _ := cf1.Get([]byte("shared"))
	v2, _ := cf2.Get([]byte("shared"))
	if string(v1) != "v1" {
		t.Fatalf("cf1 shared = %q, want v1", v1)
	}
	if string(v2) != "v2" {
		t.Fatalf("cf2 shared = %q, want v2", v2)
	}

	// Default namespace must also be unaffected.
	mustNotFound(t, db, "shared")
}

func TestColumnFamilyNotFoundBeforeCreate(t *testing.T) {
	db := openDB(t, tmpDir(t))
	defer func() { _ = db.Close() }()

	_, err := db.GetColumnFamily("nonexistent")
	if err == nil {
		t.Fatal("expected error accessing non-existent CF")
	}
}

func TestColumnFamilyDelete(t *testing.T) {
	db := openDB(t, tmpDir(t))
	defer func() { _ = db.Close() }()

	if err := db.CreateColumnFamily("orders"); err != nil {
		t.Fatal(err)
	}
	cf, _ := db.GetColumnFamily("orders")
	_ = cf.Put([]byte("order:1"), []byte("data1"))
	_ = cf.Delete([]byte("order:1"))

	_, err := cf.Get([]byte("order:1"))
	if err == nil {
		t.Fatal("expected key-not-found after CF delete")
	}
}

func TestColumnFamilyIterator(t *testing.T) {
	db := openDB(t, tmpDir(t))
	defer func() { _ = db.Close() }()

	if err := db.CreateColumnFamily("metrics"); err != nil {
		t.Fatal(err)
	}
	cf, _ := db.GetColumnFamily("metrics")

	keys := []string{"cpu", "disk", "mem", "net"}
	for _, k := range keys {
		if err := cf.Put([]byte(k), []byte(k+"_val")); err != nil {
			t.Fatal(err)
		}
	}
	// Put a key in the default namespace with the same name — must not appear.
	mustPut(t, db, "cpu", "default_cpu")

	iter, err := cf.NewIterator(types.IteratorOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = iter.Close() }()

	var got []string
	for iter.Valid() {
		got = append(got, string(iter.Key()))
		iter.Next()
	}
	sort.Strings(got)

	if len(got) != len(keys) {
		t.Fatalf("CF iterator: got %v, want %v", got, keys)
	}
	for i, k := range keys {
		if got[i] != k {
			t.Fatalf("CF iterator key[%d]: got %q, want %q", i, got[i], k)
		}
	}
}

func TestColumnFamilyPersists(t *testing.T) {
	dir := tmpDir(t)
	db := openDB(t, dir)

	if err := db.CreateColumnFamily("persistent"); err != nil {
		t.Fatal(err)
	}
	cf, _ := db.GetColumnFamily("persistent")
	_ = cf.Put([]byte("k"), []byte("v"))
	_ = db.Close()

	db2 := openDB(t, dir)
	defer func() { _ = db2.Close() }()

	// CF must still exist after restart.
	cf2, err := db2.GetColumnFamily("persistent")
	if err != nil {
		t.Fatalf("CF not found after restart: %v", err)
	}
	v, err := cf2.Get([]byte("k"))
	if err != nil {
		t.Fatalf("cf2.Get: %v", err)
	}
	if string(v) != "v" {
		t.Fatalf("got %q, want v", v)
	}
}

func TestColumnFamilyListAndDrop(t *testing.T) {
	db := openDB(t, tmpDir(t))
	defer func() { _ = db.Close() }()

	_ = db.CreateColumnFamily("alpha")
	_ = db.CreateColumnFamily("beta")
	_ = db.CreateColumnFamily("gamma")

	names := db.ListColumnFamilies()
	if len(names) != 3 {
		t.Fatalf("expected 3 CFs, got %v", names)
	}

	// Drop beta and verify its keys are gone.
	cfBeta, _ := db.GetColumnFamily("beta")
	_ = cfBeta.Put([]byte("key"), []byte("betaval"))

	if err := db.DropColumnFamily("beta"); err != nil {
		t.Fatal(err)
	}
	names2 := db.ListColumnFamilies()
	for _, n := range names2 {
		if n == "beta" {
			t.Fatal("beta should have been dropped")
		}
	}
	// beta key must be gone.
	_, err := db.GetColumnFamily("beta")
	if err == nil {
		t.Fatal("expected error accessing dropped CF")
	}
}

func TestColumnFamilyIdempotentCreate(t *testing.T) {
	db := openDB(t, tmpDir(t))
	defer func() { _ = db.Close() }()

	_ = db.CreateColumnFamily("idm")
	cf1, _ := db.GetColumnFamily("idm")
	_ = cf1.Put([]byte("x"), []byte("1"))

	// Calling CreateColumnFamily again must not wipe the data.
	if err := db.CreateColumnFamily("idm"); err != nil {
		t.Fatal(err)
	}
	cf2, _ := db.GetColumnFamily("idm")
	v, err := cf2.Get([]byte("x"))
	if err != nil || string(v) != "1" {
		t.Fatalf("idempotent create wiped data: got %q %v", v, err)
	}
}
