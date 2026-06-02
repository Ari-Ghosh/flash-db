package tests

import (
	"fmt"
	"sync"
	"testing"
)

func TestWriteBatchAtomic(t *testing.T) {
	db := openDB(t, tmpDir(t))
	defer func() { _ = db.Close() }()

	wb := db.NewWriteBatch()
	wb.Put([]byte("wb:a"), []byte("1"))
	wb.Put([]byte("wb:b"), []byte("2"))
	wb.Put([]byte("wb:c"), []byte("3"))
	if err := wb.Commit(); err != nil {
		t.Fatalf("WriteBatch.Commit: %v", err)
	}

	mustGet(t, db, "wb:a", "1")
	mustGet(t, db, "wb:b", "2")
	mustGet(t, db, "wb:c", "3")
}

func TestWriteBatchDeleteInBatch(t *testing.T) {
	db := openDB(t, tmpDir(t))
	defer func() { _ = db.Close() }()

	mustPut(t, db, "del:x", "exists")

	wb := db.NewWriteBatch()
	wb.Put([]byte("del:y"), []byte("new"))
	wb.Delete([]byte("del:x"))
	if err := wb.Commit(); err != nil {
		t.Fatal(err)
	}

	mustNotFound(t, db, "del:x")
	mustGet(t, db, "del:y", "new")
}

func TestWriteBatchConcurrent(t *testing.T) {
	db := openDB(t, tmpDir(t))
	defer func() { _ = db.Close() }()

	const goroutines = 30
	const perG = 50
	var wg sync.WaitGroup
	errs := make(chan error, goroutines*perG)

	for g := 0; g < goroutines; g++ {
		// g := g
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				wb := db.NewWriteBatch()
				wb.Put([]byte(fmt.Sprintf("pg%d:k%d:a", g, i)), []byte("va"))
				wb.Put([]byte(fmt.Sprintf("pg%d:k%d:b", g, i)), []byte("vb"))
				if err := wb.Commit(); err != nil {
					errs <- err
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Errorf("concurrent WriteBatch: %v", e)
	}

	// Spot-check a few keys.
	for g := 0; g < goroutines; g++ {
		mustGet(t, db, fmt.Sprintf("pg%d:k0:a", g), "va")
		mustGet(t, db, fmt.Sprintf("pg%d:k0:b", g), "vb")
	}
}

func TestWriteBatchEmptyIsNoop(t *testing.T) {
	db := openDB(t, tmpDir(t))
	defer func() { _ = db.Close() }()

	wb := db.NewWriteBatch()
	if err := wb.Commit(); err != nil {
		t.Fatalf("empty WriteBatch should be a no-op, got: %v", err)
	}
}

func TestWriteBatchSurvivesRestart(t *testing.T) {
	dir := tmpDir(t)
	db := openDB(t, dir)

	wb := db.NewWriteBatch()
	wb.Put([]byte("restart:a"), []byte("1"))
	wb.Put([]byte("restart:b"), []byte("2"))
	if err := wb.Commit(); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()

	db2 := openDB(t, dir)
	defer func() { _ = db2.Close() }()
	mustGet(t, db2, "restart:a", "1")
	mustGet(t, db2, "restart:b", "2")
}
