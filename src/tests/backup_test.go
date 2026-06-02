package tests

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/Ari-Ghosh/flash-db/src/backup"
)

func TestBackupAndRestore(t *testing.T) {
	srcDir := tmpDir(t)
	backupDir := tmpDir(t)
	restoreDir := tmpDir(t)
	_ = os.RemoveAll(restoreDir) // Restore requires empty / non-existent dest

	db := openDB(t, srcDir)
	for i := 0; i < 100; i++ {
		mustPut(t, db, fmt.Sprintf("bk:%04d", i), fmt.Sprintf("v%d", i))
	}

	manifest, err := db.Backup(backupDir)
	if err != nil {
		t.Fatalf("Backup: %v", err)
	}
	if manifest == nil || len(manifest.Files) == 0 {
		t.Fatal("Backup returned empty manifest")
	}
	if manifest.Version != 1 {
		t.Fatalf("unexpected manifest version: %d", manifest.Version)
	}
	_ = db.Close()

	// Restore into a fresh directory.
	if err := backup.Restore(backupDir, restoreDir); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	// Reopen the restored DB and verify data.
	db2 := openDB(t, restoreDir)
	for i := 0; i < 100; i++ {
		mustGet(t, db2, fmt.Sprintf("bk:%04d", i), fmt.Sprintf("v%d", i))
	}
}

func TestBackupManifestChecksums(t *testing.T) {
	srcDir := tmpDir(t)
	backupDir := tmpDir(t)

	db := openDB(t, srcDir)
	mustPut(t, db, "k", "v")
	manifest, err := db.Backup(backupDir)
	if err != nil {
		t.Fatal(err)
	}
	_ = db.Close()

	// Corrupt one file in the backup.
	targetFile := filepath.Join(backupDir, manifest.Files[0].Name)
	data, _ := os.ReadFile(filepath.Clean(targetFile))
	if len(data) > 0 {
		data[len(data)/2] ^= 0xFF
		// Use Join with Base to satisfy gosec G703 (path traversal) taint analysis.
		cleanTarget := filepath.Join(backupDir, filepath.Base(targetFile))
		if err := os.WriteFile(cleanTarget, data, 0o600); err != nil { //nolint:gosec // G703: path is constructed from t.TempDir(), not user input
			t.Fatal(err)
		}
	}

	restoreDir := tmpDir(t)
	_ = os.RemoveAll(restoreDir)
	err = backup.Restore(backupDir, restoreDir)
	if err == nil {
		t.Fatal("Restore should have failed with corrupted backup")
	}
}

func TestBackupRejectsNonEmptyDest(t *testing.T) {
	srcDir := tmpDir(t)
	backupDir := tmpDir(t)
	restoreDir := tmpDir(t) // already exists and has content (tmpDir writes nothing, but dir exists)

	db := openDB(t, srcDir)
	mustPut(t, db, "k", "v")
	_, _ = db.Backup(backupDir)
	_ = db.Close()

	// Create a file in restoreDir so it's non-empty.
	existingFile := filepath.Join(restoreDir, "existing.txt")
	if err := os.WriteFile(filepath.Clean(existingFile), []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := backup.Restore(backupDir, restoreDir)
	if err == nil {
		t.Fatal("Restore should reject non-empty destination")
	}
}

func TestBackupManifestReadable(t *testing.T) {
	srcDir := tmpDir(t)
	backupDir := tmpDir(t)

	db := openDB(t, srcDir)
	mustPut(t, db, "manifest-test", "ok")
	_, _ = db.Backup(backupDir)
	_ = db.Close()

	m, err := backup.ReadManifest(backupDir)
	if err != nil {
		t.Fatalf("ReadManifest: %v", err)
	}
	if m.SnapSeq == 0 {
		t.Fatal("manifest SnapSeq should be > 0")
	}
	if m.CreatedAt.IsZero() {
		t.Fatal("manifest CreatedAt should not be zero")
	}
}

func TestBackupMissingManifestRejected(t *testing.T) {
	dir := tmpDir(t)
	_, err := backup.ReadManifest(dir)
	if err == nil {
		t.Fatal("ReadManifest should fail for directory without manifest")
	}
}
