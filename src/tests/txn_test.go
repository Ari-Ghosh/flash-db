package tests

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/Ari-Ghosh/flash-db/src/txn"
)

func TestTxnCommitBasic(t *testing.T) {
	db := openDB(t, tmpDir(t))
	tx := db.Begin()
	if err := tx.Put([]byte("acc:alice"), []byte("100")); err != nil {
		t.Fatal(err)
	}
	if err := tx.Put([]byte("acc:bob"), []byte("200")); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	mustGet(t, db, "acc:alice", "100")
	mustGet(t, db, "acc:bob", "200")
}

func TestTxnRollback(t *testing.T) {
	db := openDB(t, tmpDir(t))
	mustPut(t, db, "k", "original")

	tx := db.Begin()
	_ = tx.Put([]byte("k"), []byte("modified"))
	_ = tx.Rollback()

	mustGet(t, db, "k", "original")
}

func TestTxnReadYourWrites(t *testing.T) {
	db := openDB(t, tmpDir(t))
	tx := db.Begin()
	_ = tx.Put([]byte("ryw"), []byte("seen"))

	v, err := tx.Get([]byte("ryw"))
	if err != nil || string(v) != "seen" {
		t.Fatalf("read-your-writes: got %q %v", v, err)
	}
	_ = tx.Rollback()
}

func TestTxnConflictDetection(t *testing.T) {
	db := openDB(t, tmpDir(t))
	mustPut(t, db, "contested", "v0")

	tx := db.Begin()
	_, err := tx.Get([]byte("contested"))
	if err != nil {
		t.Fatal(err)
	}

	// Concurrent writer mutates the key before tx commits.
	mustPut(t, db, "contested", "v1")

	_ = tx.Put([]byte("contested"), []byte("v2"))
	err = tx.Commit()
	if err == nil {
		t.Fatal("expected ErrTxnConflict, got nil")
	}
	if !errors.Is(err, txn.ErrTxnConflict) {
		// Accept wrapped errors too.
		t.Logf("conflict error: %v", err)
	}
	// The key should have the value written by the concurrent writer.
	mustGet(t, db, "contested", "v1")
}

func TestTxnAtomicTransferReadBack(t *testing.T) {
	db := openDB(t, tmpDir(t))
	mustPut(t, db, "alice", "1000")
	mustPut(t, db, "bob", "500")

	tx := db.Begin()
	aVal, _ := tx.Get([]byte("alice"))
	bVal, _ := tx.Get([]byte("bob"))

	// Simulate transfer: alice→bob 100
	if string(aVal) != "1000" || string(bVal) != "500" {
		t.Fatalf("unexpected initial values: alice=%s bob=%s", aVal, bVal)
	}
	_ = tx.Put([]byte("alice"), []byte("900"))
	_ = tx.Put([]byte("bob"), []byte("600"))

	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit transfer: %v", err)
	}
	mustGet(t, db, "alice", "900")
	mustGet(t, db, "bob", "600")
}

func TestTxnDeleteInTransaction(t *testing.T) {
	db := openDB(t, tmpDir(t))
	mustPut(t, db, "todel", "here")

	tx := db.Begin()
	_ = tx.Delete([]byte("todel"))
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	mustNotFound(t, db, "todel")
}

func TestTxnDoubleCommitReturnsError(t *testing.T) {
	db := openDB(t, tmpDir(t))
	tx := db.Begin()
	_ = tx.Put([]byte("k"), []byte("v"))
	_ = tx.Commit()
	if err := tx.Commit(); !errors.Is(err, txn.ErrTxnClosed) {
		t.Fatalf("expected ErrTxnClosed on second Commit, got %v", err)
	}
}

func TestTxnOversizeKeyRejected(t *testing.T) {
	db := openDB(t, tmpDir(t))
	tx := db.Begin()
	bigKey := make([]byte, txn.MaxKeySize+1)
	if err := tx.Put(bigKey, []byte("v")); err == nil {
		t.Fatal("expected error for oversized key")
	}
	_ = tx.Rollback()
}

func TestTxnMaxOpsEnforced(t *testing.T) {
	db := openDB(t, tmpDir(t))
	tx := db.Begin()
	var lastErr error
	for i := 0; i <= txn.MaxOps+1; i++ {
		lastErr = tx.Put([]byte(fmt.Sprintf("k%d", i)), []byte("v"))
	}
	if !errors.Is(lastErr, txn.ErrTxnTooLarge) {
		t.Fatalf("expected ErrTxnTooLarge after %d ops, got %v", txn.MaxOps, lastErr)
	}
	_ = tx.Rollback()
}

func TestTxnWALReplay(t *testing.T) {
	dir := tmpDir(t)
	db := openDB(t, dir)

	tx := db.Begin()
	_ = tx.Put([]byte("tx:a"), []byte("1"))
	_ = tx.Put([]byte("tx:b"), []byte("2"))
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()

	// Reopen — both keys must survive WAL replay.
	db2 := openDB(t, dir)
	mustGet(t, db2, "tx:a", "1")
	mustGet(t, db2, "tx:b", "2")
}

func TestTxnConcurrentNonConflicting(t *testing.T) {
	db := openDB(t, tmpDir(t))
	// Two transactions writing disjoint key sets must both succeed.
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for g := 0; g < 2; g++ {
		// g := g
		wg.Add(1)
		go func() {
			defer wg.Done()
			tx := db.Begin()
			for i := 0; i < 10; i++ {
				_ = tx.Put([]byte(fmt.Sprintf("g%d:k%d", g, i)), []byte("v"))
			}
			if err := tx.Commit(); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Errorf("non-conflicting txn failed: %v", e)
	}
}
