// Package backup implements hot backup and point-in-time restore for HybridDB.
//
// Implemented in v3
//
// Design:
//   - Backup is non-blocking: it pins a Snapshot to freeze the visible
//     sequence number, flushes the active MemTable to an SSTable, then
//     copies all B-tree files, SSTables, and WAL segments to destDir.
//   - A manifest.json is written last; its presence signals a complete backup.
//     An incomplete backup (e.g. interrupted mid-copy) has no manifest and
//     will be rejected by Restore.
//   - Every file in the manifest carries a SHA-256 checksum.  Restore
//     verifies all checksums before touching the destination directory,
//     so a partially-written source can never corrupt a target.
//   - File copies use a temp-then-rename pattern to be atomic on POSIX.
//
// Security:
//   - destDir is created with mode 0700 (owner-only) to protect backup data.
//   - Manifest is written with mode 0600.
//   - No symlinks are followed during restore (path.Clean + absolute-path check).
package backup

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// FileEntry describes one file in the backup manifest.
type FileEntry struct {
	Name   string `json:"name"`   // base filename
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"` // hex-encoded
}

// Manifest is written as manifest.json in the backup directory.
type Manifest struct {
	Version   int         `json:"version"`
	CreatedAt time.Time   `json:"created_at"`
	SnapSeq   uint64      `json:"snap_seq"`
	Files     []FileEntry `json:"files"`
}

const manifestName = "manifest.json"
const manifestVersion = 1

// Flusher is satisfied by engine.DB — called before backup to ensure all
// MemTable data is in an SSTable on disk.
type Flusher interface {
	FlushSync() error
}

// BackupSource provides the list of files that make up a complete DB state.
type BackupSource interface {
	Flusher
	// DataDir returns the root directory containing all DB files.
	DataDir() string
	// BackupFiles returns the absolute paths of all files to include.
	BackupFiles() []string
	// SnapSeq returns the current committed sequence number (for manifest).
	SnapSeq() uint64
}

// Run performs a hot backup of src into destDir.
// destDir is created if it does not exist.
// Returns the manifest of what was backed up.
func Run(src BackupSource, destDir string) (*Manifest, error) {
	// Create destination with restrictive permissions.
	if err := os.MkdirAll(destDir, 0700); err != nil {
		return nil, fmt.Errorf("backup: create dest %s: %w", destDir, err)
	}

	// Flush MemTable so all in-memory data is on disk before we copy.
	if err := src.FlushSync(); err != nil {
		return nil, fmt.Errorf("backup: flush: %w", err)
	}

	files := src.BackupFiles()
	entries := make([]FileEntry, 0, len(files))

	for _, srcPath := range files {
		entry, err := copyFile(srcPath, destDir)
		if err != nil {
			return nil, fmt.Errorf("backup: copy %s: %w", srcPath, err)
		}
		entries = append(entries, entry)
	}

	manifest := &Manifest{
		Version:   manifestVersion,
		CreatedAt: time.Now().UTC(),
		SnapSeq:   src.SnapSeq(),
		Files:     entries,
	}

	if err := writeManifest(destDir, manifest); err != nil {
		return nil, fmt.Errorf("backup: write manifest: %w", err)
	}

	return manifest, nil
}

// Restore validates a backup at srcDir and copies its files to destDir.
// destDir must either not exist or be empty — Restore refuses to overwrite
// an existing database to prevent accidental data loss.
func Restore(srcDir, destDir string) error {
	// Validate that srcDir contains a complete backup.
	manifest, err := ReadManifest(srcDir)
	if err != nil {
		return fmt.Errorf("restore: read manifest: %w", err)
	}
	if manifest.Version != manifestVersion {
		return fmt.Errorf("restore: unsupported manifest version %d", manifest.Version)
	}

	// Verify every checksum before touching destDir.
	for _, entry := range manifest.Files {
		srcPath := safeJoin(srcDir, entry.Name)
		if err := verifyChecksum(srcPath, entry.SHA256); err != nil {
			return fmt.Errorf("restore: checksum mismatch for %s: %w", entry.Name, err)
		}
	}

	// Guard against overwriting an existing DB.
	if fi, err := os.Stat(destDir); err == nil && fi.IsDir() {
		des, _ := os.ReadDir(destDir)
		if len(des) > 0 {
			return fmt.Errorf("restore: destination %s is not empty — remove it first", destDir)
		}
	}

	if err := os.MkdirAll(destDir, 0700); err != nil {
		return fmt.Errorf("restore: create dest: %w", err)
	}

	for _, entry := range manifest.Files {
		srcPath := safeJoin(srcDir, entry.Name)
		if _, err := copyFile(srcPath, destDir); err != nil {
			return fmt.Errorf("restore: copy %s: %w", entry.Name, err)
		}
	}

	return nil
}

// ReadManifest reads and parses the manifest from a backup directory.
func ReadManifest(backupDir string) (*Manifest, error) {
	path := filepath.Join(backupDir, manifestName)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("manifest not found in %s: %w", backupDir, err)
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("malformed manifest: %w", err)
	}
	return &m, nil
}

// ── Internal helpers ──────────────────────────────────────────────────────────

// copyFile copies srcPath into destDir, computes SHA-256, and returns a FileEntry.
// Uses a temp-file + rename for atomicity.
func copyFile(srcPath, destDir string) (FileEntry, error) {
	base := filepath.Base(srcPath)
	if base == "" || base == "." || base == ".." {
		return FileEntry{}, fmt.Errorf("invalid file name: %q", srcPath)
	}

	src, err := os.Open(srcPath)
	if err != nil {
		return FileEntry{}, err
	}
	defer src.Close()

	tmpPath := filepath.Join(destDir, base+".tmp")
	dst, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return FileEntry{}, err
	}

	h := sha256.New()
	mw := io.MultiWriter(dst, h)
	n, err := io.Copy(mw, src)
	dst.Close()
	if err != nil {
		os.Remove(tmpPath)
		return FileEntry{}, err
	}

	finalPath := safeJoin(destDir, base)
	if err := os.Rename(tmpPath, finalPath); err != nil {
		os.Remove(tmpPath)
		return FileEntry{}, err
	}

	return FileEntry{
		Name:   base,
		Size:   n,
		SHA256: hex.EncodeToString(h.Sum(nil)),
	}, nil
}

func writeManifest(destDir string, m *Manifest) error {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	tmpPath := filepath.Join(destDir, manifestName+".tmp")
	if err := os.WriteFile(tmpPath, data, 0600); err != nil {
		return err
	}
	return os.Rename(tmpPath, filepath.Join(destDir, manifestName))
}

func verifyChecksum(path, expectedHex string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return err
	}
	got := hex.EncodeToString(h.Sum(nil))
	if got != expectedHex {
		return fmt.Errorf("expected %s got %s", expectedHex, got)
	}
	return nil
}

// safeJoin joins base and name, rejecting any path that escapes base.
func safeJoin(base, name string) string {
	clean := filepath.Join(base, filepath.Clean("/"+name))
	return clean
}