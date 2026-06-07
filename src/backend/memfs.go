package backend

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

// memFile is an in-memory file backed by a byte buffer.
type memFile struct {
	mu     sync.RWMutex
	buf    []byte
	pos    int64
	closed bool
	name   string
}

func (f *memFile) Read(p []byte) (int, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.closed {
		return 0, os.ErrClosed
	}
	if f.pos >= int64(len(f.buf)) {
		return 0, io.EOF
	}
	n := copy(p, f.buf[f.pos:])
	f.pos += int64(n)
	return n, nil
}

func (f *memFile) Write(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return 0, os.ErrClosed
	}
	writePos := f.pos
	end := writePos + int64(len(p))
	if end > int64(len(f.buf)) {
		newBuf := make([]byte, end)
		copy(newBuf, f.buf)
		f.buf = newBuf
	}
	n := copy(f.buf[writePos:], p)
	f.pos = end
	return n, nil
}

func (f *memFile) ReadAt(p []byte, off int64) (int, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.closed {
		return 0, os.ErrClosed
	}
	if off >= int64(len(f.buf)) {
		return 0, io.EOF
	}
	n := copy(p, f.buf[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (f *memFile) WriteAt(p []byte, off int64) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return 0, os.ErrClosed
	}
	end := off + int64(len(p))
	if end > int64(len(f.buf)) {
		newBuf := make([]byte, end)
		copy(newBuf, f.buf)
		f.buf = newBuf
	}
	n := copy(f.buf[off:], p)
	return n, nil
}

func (f *memFile) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

func (f *memFile) Sync() error { return nil }

func (f *memFile) Seek(offset int64, whence int) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return 0, os.ErrClosed
	}
	switch whence {
	case io.SeekStart:
		f.pos = offset
	case io.SeekCurrent:
		f.pos += offset
	case io.SeekEnd:
		f.pos = int64(len(f.buf)) + offset
	}
	if f.pos < 0 {
		f.pos = 0
	}
	return f.pos, nil
}

func (f *memFile) Stat() (os.FileInfo, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return &memFileInfo{name: f.name, size: int64(len(f.buf))}, nil
}

type memFileInfo struct {
	name string
	size int64
}

func (i *memFileInfo) Name() string       { return i.name }
func (i *memFileInfo) Size() int64        { return i.size }
func (i *memFileInfo) Mode() os.FileMode  { return 0o600 }
func (i *memFileInfo) ModTime() time.Time { return time.Time{} }
func (i *memFileInfo) IsDir() bool        { return false }
func (i *memFileInfo) Sys() any           { return nil }

// MemFS is an in-memory filesystem for testing and benchmarking.
// All data lives in a map protected by a mutex.
type MemFS struct {
	mu    sync.RWMutex
	files map[string][]byte // path → file contents
	dirs  map[string]bool   // path → true if directory exists
}

// NewMemFS creates a fresh in-memory filesystem.
func NewMemFS() *MemFS {
	return &MemFS{
		files: make(map[string][]byte),
		dirs:  make(map[string]bool),
	}
}

func (m *MemFS) OpenRead(path string) (File, error) {
	m.mu.RLock()
	data, ok := m.files[path]
	m.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("memfs: open %s: %w", path, os.ErrNotExist)
	}
	buf := make([]byte, len(data))
	copy(buf, data)
	return &memFile{buf: buf, name: path}, nil
}

func (m *MemFS) Create(path string) (File, error) {
	m.mu.Lock()
	m.files[path] = nil
	m.mu.Unlock()
	return &memFile{name: path}, nil
}

func (m *MemFS) OpenReadWrite(path string) (File, error) {
	m.mu.Lock()
	data, ok := m.files[path]
	if !ok {
		m.files[path] = nil
		data = nil
	}
	m.mu.Unlock()
	buf := make([]byte, len(data))
	copy(buf, data)
	return &memFile{buf: buf, name: path}, nil
}

func (m *MemFS) Remove(path string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.files[path]; !ok {
		return fmt.Errorf("memfs: remove %s: %w", path, os.ErrNotExist)
	}
	delete(m.files, path)
	return nil
}

func (m *MemFS) MkdirAll(path string, _ os.FileMode) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dirs[path] = true
	return nil
}

func (m *MemFS) Rename(old, new string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.files[old]
	if !ok {
		return fmt.Errorf("memfs: rename %s: %w", old, os.ErrNotExist)
	}
	m.files[new] = data
	delete(m.files, old)
	return nil
}

func (m *MemFS) List(pattern string) ([]string, error) {
	if !strings.Contains(pattern, "*") {
		return nil, errors.New("memfs: List only supports glob patterns")
	}
	prefix := pattern[:strings.Index(pattern, "*")]
	m.mu.RLock()
	defer m.mu.RUnlock()
	var matches []string
	for path := range m.files {
		if strings.HasPrefix(path, prefix) {
			matches = append(matches, path)
		}
	}
	sort.Strings(matches)
	return matches, nil
}
