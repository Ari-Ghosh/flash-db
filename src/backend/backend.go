// Package backend defines pluggable filesystem interfaces for flashDB.
//
// The FS interface abstracts local filesystem operations so the engine can
// run on different storage backends (local disk, in-memory, cloud object
// store, etc.) and enables fault injection for chaos testing.

package backend

import (
	"io"
	"os"
)

// File represents an open file supporting sequential and random I/O.
type File interface {
	io.Reader
	io.Writer
	io.ReaderAt
	io.WriterAt
	io.Seeker
	io.Closer
	Sync() error
	Stat() (os.FileInfo, error)
}

// FS is the pluggable filesystem abstraction used by WAL, SSTable, BTree,
// compaction cleanup, and backup operations.
type FS interface {
	OpenRead(path string) (File, error)
	Create(path string) (File, error)
	OpenReadWrite(path string) (File, error)
	OpenAppend(path string) (File, error)
	Remove(path string) error
	MkdirAll(path string, perm os.FileMode) error
	Rename(old, newname string) error
	List(pattern string) ([]string, error)
}
