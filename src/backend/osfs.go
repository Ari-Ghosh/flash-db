package backend

import (
	"io"
	"os"
	"path/filepath"
	"sort"
)

// osFile wraps *os.File to satisfy the File interface.
type osFile struct {
	f *os.File
}

func (o *osFile) Read(p []byte) (int, error)             { return o.f.Read(p) }
func (o *osFile) Write(p []byte) (int, error)            { return o.f.Write(p) }
func (o *osFile) ReadAt(p []byte, off int64) (int, error) { return o.f.ReadAt(p, off) }
func (o *osFile) WriteAt(p []byte, off int64) (int, error) { return o.f.WriteAt(p, off) }
func (o *osFile) Seek(offset int64, whence int) (int64, error) { return o.f.Seek(offset, whence) }
func (o *osFile) Close() error                           { return o.f.Close() }
func (o *osFile) Sync() error                            { return o.f.Sync() }
func (o *osFile) Stat() (os.FileInfo, error)             { return o.f.Stat() }

// OSFS implements FS using the local operating system.
type OSFS struct{}

func (OSFS) OpenRead(path string) (File, error) {
	f, err := os.Open(filepath.Clean(path))
	if err != nil {
		return nil, err
	}
	return &osFile{f: f}, nil
}

func (OSFS) Create(path string) (File, error) {
	f, err := os.OpenFile(filepath.Clean(path), os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, err
	}
	return &osFile{f: f}, nil
}

func (OSFS) OpenReadWrite(path string) (File, error) {
	f, err := os.OpenFile(filepath.Clean(path), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	return &osFile{f: f}, nil
}

func (OSFS) OpenAppend(path string) (File, error) {
	f, err := os.OpenFile(filepath.Clean(path), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	return &osFile{f: f}, nil
}

func (OSFS) Remove(path string) error {
	return os.Remove(filepath.Clean(path))
}

func (OSFS) MkdirAll(path string, perm os.FileMode) error {
	return os.MkdirAll(filepath.Clean(path), perm)
}

func (OSFS) Rename(old, new string) error {
	return os.Rename(filepath.Clean(old), filepath.Clean(new))
}

func (OSFS) List(pattern string) ([]string, error) {
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}
	sort.Strings(matches)
	return matches, nil
}

// ── Helpers ────────────────────────────────────────────────────────────────

// ReadFile reads an entire file through the FS.
func ReadFile(fs FS, path string) ([]byte, error) {
	f, err := fs.OpenRead(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return io.ReadAll(f)
}
