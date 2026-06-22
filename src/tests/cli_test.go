package tests

import (
	"os"
	"testing"

	"github.com/Ari-Ghosh/flash-db/src/backup"
)

// TestCLI_BasicOps tests the fundamental KV operations that the CLI wraps.
func TestCLI_BasicOps(t *testing.T) {
	dir := tmpDir(t)
	defer os.RemoveAll(dir)

	db := openDB(t, dir)
	defer db.Close()

	mustPut(t, db, "k", "v")
	mustGet(t, db, "k", "v")

	if err := db.Delete([]byte("k")); err != nil {
		t.Fatal(err)
	}
	mustNotFound(t, db, "k")

	s := db.Stats()
	if s.SeqNum < 2 {
		t.Fatalf("expected seq >= 2, got %d", s.SeqNum)
	}
}

func TestCLI_BackupRestore(t *testing.T) {
	srcDir := tmpDir(t)
	defer os.RemoveAll(srcDir)
	dstDir := tmpDir(t)
	defer os.RemoveAll(dstDir)
	restoreDir := tmpDir(t)
	defer os.RemoveAll(restoreDir)

	db := openDB(t, srcDir)
	mustPut(t, db, "a", "1")
	mustPut(t, db, "b", "2")

	manifest, err := db.Backup(dstDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.Files) == 0 {
		t.Fatal("expected at least 1 backup file")
	}
	db.Close()

	if err := backup.Restore(dstDir, restoreDir); err != nil {
		t.Fatal(err)
	}

	db2 := openDB(t, restoreDir)
	defer db2.Close()
	mustGet(t, db2, "a", "1")
	mustGet(t, db2, "b", "2")
}
