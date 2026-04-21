// Package types defines core types and interfaces for the flashDB engine.
//
// ## Overview
//
// This package provides the foundational types, interfaces, and constants used
// throughout the flashDB system. It defines the Iterator abstraction for range
// scans, Snapshot for MVCC, and Codec for serialization.
//
// ## Architecture
//
// - **Iterator Interface**: Unified API for range scans across MemTable, B-tree, SSTable.
// - **Snapshot**: Sequence number for point-in-time reads.
// - **Codec**: Pluggable serialization (JSON, Gob, etc.).
// - **Entry**: Key-value pair with sequence number and tombstone flag.
//
// ## v1 vs v2 Changes
//
// ### v1 (Original Implementation)
// - Basic Entry struct with key, value, seqNum.
// - No Iterator interface; range queries handled per-component.
// - Simple Get/Put operations without MVCC.
// - Used for basic key-value storage with compaction.
//
// ### v2 (Enhanced Implementation)
// - **Iterator Interface**: Seek(), Next(), Prev(), Valid() for unified range scans.
// - **Snapshot Support**: NewSnapshot() for MVCC point-in-time reads.
// - **Codec Abstraction**: Pluggable serialization for flexible data formats.
// - **Enhanced Entry**: Tombstone flag for logical deletes.
// - **Iterator Options**: Bounds, direction, snapshot filtering.
// ## v3 (Refinements and Optimizations)
// - **Snapshot / MVCC support**: every write is tagged with a SeqNum;
//     a Snapshot pins a read-consistent view at a specific sequence number.
// - **Iterator API**: forward and backward range scans over a consistent view.
// - **Compression codec interface**: pluggable per-SSTable value compression.
// - **Tiered storage**: Entry carries a Level field for L0→L1→L2 tiers.
//
// ## Key Types
//
// - **Entry**: {Key, Value, SeqNum, Tombstone} – core data unit.
// - **Iterator**: Interface for range iteration with Seek/Next/Prev.
// - **Snapshot**: Sequence number for MVCC isolation.
// - **Codec**: Interface for key/value serialization.
//
// ## Iterator Interface
//
//	type Iterator interface {
//		Seek(key)     // Position at first >= key
//		Next()        // Advance to next entry
//		Prev()        // Retreat to previous entry (v2)
//		Valid() bool  // Check if positioned at valid entry
//		Key() []byte  // Current key
//		Value() []byte // Current value
//		Close()       // Release resources
//	}
//
// ## Usage in flashDB

//
// Types are used across all components:
// - Engine.NewIterator() merges MemTable + B-tree iterators.
// - Snapshots enable consistent reads during compaction.
// - Codec handles serialization in WAL and SSTable.

package types

import (
	"errors"
	"io"
	"sync/atomic"
)

var (
	ErrKeyNotFound   = errors.New("key not found")
	ErrKeyDeleted    = errors.New("key deleted")
	ErrIterExhausted = errors.New("iterator exhausted")
	ErrClosed        = errors.New("db is closed")
	ErrSnapshotStale = errors.New("snapshot released")
)

// Entry is a versioned key-value record.
type Entry struct {
	Key       []byte
	Value     []byte
	SeqNum    uint64
	Tombstone bool
	Level     uint8
}

// Reader can look up a key.
type Reader interface {
	Get(key []byte) (*Entry, error)
}

// Writer can accept a put or delete.
type Writer interface {
	Put(key, value []byte, seq uint64) error
	Delete(key []byte, seq uint64) error
}

// IteratorOptions configures an iterator scan.
type IteratorOptions struct {
	// LowerBound is the inclusive lower key bound (nil = unbounded).
	LowerBound []byte
	// UpperBound is the exclusive upper key bound (nil = unbounded).
	UpperBound []byte
	// Prefix, when set, overrides LowerBound/UpperBound to scan all keys
	// sharing this byte prefix.  The engine derives the correct exclusive
	// upper bound automatically, including the 0xFF overflow edge case.
	Prefix []byte
	// Reverse iterates in descending key order.
	Reverse bool
	// SnapshotSeq filters entries: only those with SeqNum <= SnapshotSeq
	// are visible.  Zero means "use the current committed seq".
	SnapshotSeq uint64
	// IncludeTombstones exposes deletion markers to the caller.
	IncludeTombstones bool
}

// Iterator provides ordered range iteration over a consistent view.
type Iterator interface {
	Valid() bool
	Next()
	Prev()
	Key() []byte
	Value() []byte
	SeqNum() uint64
	IsTombstone() bool
	Error() error
	Close() error
	io.Closer
}

// SnapshotID is an opaque identifier for a point-in-time read snapshot.
type SnapshotID uint64

// Snapshot is a read-consistent view of the database at a fixed SeqNum.
type Snapshot struct {
	id      SnapshotID
	seq     uint64
	refs    int64
	tracker *SnapshotTracker
}

func (s *Snapshot) Seq() uint64       { return s.seq }
func (s *Snapshot) ID() SnapshotID    { return s.id }
func (s *Snapshot) Retain() *Snapshot { atomic.AddInt64(&s.refs, 1); return s }

func (s *Snapshot) Release() {
	if atomic.AddInt64(&s.refs, -1) == 0 && s.tracker != nil {
		s.tracker.remove(s)
	}
}

// SnapshotTracker maintains the set of live snapshots.
type SnapshotTracker struct {
	mu     atomic.Pointer[snapshotList]
	nextID atomic.Uint64
}

type snapshotList struct {
	snaps []*Snapshot
}

func NewSnapshotTracker() *SnapshotTracker {
	t := &SnapshotTracker{}
	t.mu.Store(&snapshotList{})
	return t
}

func (t *SnapshotTracker) Create(seq uint64) *Snapshot {
	s := &Snapshot{id: SnapshotID(t.nextID.Add(1)), seq: seq, refs: 1, tracker: t}
	for {
		old := t.mu.Load()
		nl := &snapshotList{snaps: make([]*Snapshot, len(old.snaps)+1)}
		copy(nl.snaps, old.snaps)
		nl.snaps[len(old.snaps)] = s
		if t.mu.CompareAndSwap(old, nl) {
			break
		}
	}
	return s
}

// OldestPinnedSeq returns the minimum seq held by any live snapshot.
// Returns ^uint64(0) if no snapshots are alive.
func (t *SnapshotTracker) OldestPinnedSeq() uint64 {
	list := t.mu.Load()
	oldest := ^uint64(0)
	for _, s := range list.snaps {
		if s.seq < oldest {
			oldest = s.seq
		}
	}
	return oldest
}

func (t *SnapshotTracker) remove(s *Snapshot) {
	for {
		old := t.mu.Load()
		nl := &snapshotList{snaps: make([]*Snapshot, 0, len(old.snaps))}
		for _, snap := range old.snaps {
			if snap.id != s.id {
				nl.snaps = append(nl.snaps, snap)
			}
		}
		if t.mu.CompareAndSwap(old, nl) {
			break
		}
	}
}

// Codec identifies a compression algorithm.
type Codec uint8

const (
	CodecNone   Codec = 0
	CodecSnappy Codec = 1
	CodecZstd   Codec = 2
)

// Compressor compresses / decompresses SSTable data blocks.
type Compressor interface {
	Compress(dst, src []byte) ([]byte, error)
	Decompress(dst, src []byte) ([]byte, error)
	Codec() Codec
}

// NoopCompressor is a passthrough (CodecNone).
type NoopCompressor struct{}

func (NoopCompressor) Compress(_, src []byte) ([]byte, error)   { return src, nil }
func (NoopCompressor) Decompress(_, src []byte) ([]byte, error) { return src, nil }
func (NoopCompressor) Codec() Codec                             { return CodecNone }

// Storage tier constants.
const (
	LevelMem = uint8(0)
	LevelL0  = uint8(1)
	LevelL1  = uint8(2)
	LevelL2  = uint8(3)
)
