package tests

import (
	"testing"
)

func TestBasicPutGet(t *testing.T) {
	db := openDB(t, tmpDir(t))
	defer func() { _ = db.Close() }()

	mustPut(t, db, "hello", "world")
	mustGet(t, db, "hello", "world")
}

func TestOverwrite(t *testing.T) {
	db := openDB(t, tmpDir(t))
	defer func() { _ = db.Close() }()

	mustPut(t, db, "key", "v1")
	mustPut(t, db, "key", "v2")
	mustGet(t, db, "key", "v2")
}

func TestDelete(t *testing.T) {
	db := openDB(t, tmpDir(t))
	defer func() { _ = db.Close() }()

	mustPut(t, db, "key", "val")
	if err := db.Delete([]byte("key")); err != nil {
		t.Fatal(err)
	}
	mustNotFound(t, db, "key")
}

func TestDeleteNonExistent(t *testing.T) {
	db := openDB(t, tmpDir(t))
	defer func() { _ = db.Close() }()

	// Should succeed (tombstone is idempotent).
	if err := db.Delete([]byte("nope")); err != nil {
		t.Fatalf("Delete nonexistent: %v", err)
	}
}

// ── 6. Regressions ────────────────────────────────────────────────────────────

func TestBasicCRUDRegression(t *testing.T) {
	db := openDB(t, tmpDir(t))
	mustPut(t, db, "hello", "world")
	mustGet(t, db, "hello", "world")
	mustPut(t, db, "hello", "updated")
	mustGet(t, db, "hello", "updated")
	_ = db.Delete([]byte("hello"))
	mustNotFound(t, db, "hello")
}
