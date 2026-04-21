// Package memtable provides a concurrent in-memory buffer backed by a skip
// list with full Iterator support for range scans.
//
// ## Overview
//
// The MemTable serves as the write-optimized layer in the LSM architecture, buffering
// recent writes in memory before flushing to disk. It uses a skip list for O(log n)
// insertions and sorted iteration for efficient SSTable creation.
//
// ## Architecture
//
// - **Skip List**: Probabilistic data structure with multiple levels for fast search.
// - **Concurrency**: RWMutex for concurrent reads, exclusive writes.
// - **Iterator Support**: Snapshot-aware range scans with bounds and direction.
// - **Size Tracking**: Atomic counters for memory usage and entry count.
//
// ## v1 vs v2 Changes
//
// ### v1 (Original Implementation)
// - Basic skip list with Put/Delete/Get operations.
// - Simple Entries() for sorted iteration during flush.
// - No iterator API; range queries handled externally.
// - Used as temporary buffer before compaction to B-tree.
//
// ### v2 (Enhanced Implementation)
// - **NewIterator()**: Range-bounded, snapshot-filtered iterators.
// - **Backward Iteration**: Prev() method for reverse scans.
// - **MVCC Support**: Iterators filter by SnapshotSeq (only show visible versions).
// - **Atomic Tracking**: Size and count never go negative, thread-safe.
// - **Enhanced Entries**: Now part of merged read path with filtering.
//
// ## Key Methods
//
// - **Put/Delete()**: Insert key-value or tombstone with sequence number.
// - **Get()**: Lookup most recent entry for key.
// - **NewIterator()**: Create range iterator with bounds, direction, snapshot.
// - **Entries()**: Return all entries in sorted order (for flush).
// - **IsFull()**: Check if size exceeds threshold.
//
// ## Skip List Details
//
// - **Levels**: Up to 16 levels, probability 0.25 for promotion.
// - **Insertion**: Find position, promote to random levels.
// - **Search**: Descend levels until target key found.
// - **Iteration**: Level-0 linked list with prev pointers for reverse.
//
// ## Performance Characteristics
//
// - **Put/Delete**: O(log n) skip list traversals + level promotions.
// - **Get**: O(log n) search through levels.
// - **Iterator**: O(n) for full scan, O(k) for bounded ranges.
// - **Memory**: ~1.5x overhead for skip pointers, plus entry data.
//
// ## Usage in HybridDB
//
// MemTable is the first stop in both read and write paths:
// - Write: WAL → MemTable.Put() → flush when full → SSTable.
// - Read: Check MemTable first (highest priority), then L1/L2 B-trees.
// - Iterator: Merged with B-tree iterators for consistent range scans.
package memtable

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"

	types "local/flashdb/src/types"
)

const (
	maxLevel    = 16
	probability = 0.25
)

// node is one element in the skip list.
type node struct {
	entry types.Entry
	next  [maxLevel]*node
	prev  *node // for backward iteration
}

// MemTable is a sorted, concurrent in-memory store.
type MemTable struct {
	mu      sync.RWMutex
	head    *node
	tail    *node // pointer to last real node (for reverse iteration init)
	size    int64
	count   int64
	maxSize int64
}

// New creates a MemTable that will signal readiness to flush at maxSize bytes.
func New(maxSize int64) *MemTable {
	h := &node{}
	return &MemTable{head: h, maxSize: maxSize}
}

func (m *MemTable) randomLevel() int {
	lvl := 1
	for lvl < maxLevel && rand.Float32() < probability {
		lvl++
	}
	return lvl
}

// Put inserts or overwrites a key.
func (m *MemTable) Put(key, value []byte, seq uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.insert(types.Entry{Key: key, Value: value, SeqNum: seq})
	atomic.AddInt64(&m.size, int64(len(key)+len(value)+32))
	atomic.AddInt64(&m.count, 1)
	return nil
}

// Delete inserts a tombstone for key.
func (m *MemTable) Delete(key []byte, seq uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.insert(types.Entry{Key: key, SeqNum: seq, Tombstone: true})
	atomic.AddInt64(&m.size, int64(len(key)+32))
	atomic.AddInt64(&m.count, 1)
	return nil
}

func (m *MemTable) insert(e types.Entry) {
	update := [maxLevel]*node{}
	cur := m.head
	for i := maxLevel - 1; i >= 0; i-- {
		for cur.next[i] != nil && bytes.Compare(cur.next[i].entry.Key, e.Key) < 0 {
			cur = cur.next[i]
		}
		update[i] = cur
	}

	lvl := m.randomLevel()
	n := &node{entry: e}

	// Wire level-0 prev pointers for backward iteration.
	n.prev = update[0]
	if update[0].next[0] != nil {
		update[0].next[0].prev = n
	} else {
		m.tail = n
	}

	for i := 0; i < lvl; i++ {
		if update[i] == nil {
			update[i] = m.head
		}
		n.next[i] = update[i].next[i]
		update[i].next[i] = n
	}
	// Update tail if this is now the last element.
	if n.next[0] == nil {
		m.tail = n
	}
}

// GetAt returns the most recent entry for key with seq <= seqNum, or ErrKeyNotFound.
func (m *MemTable) GetAt(key []byte, seqNum uint64) (*types.Entry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	cur := m.head
	for i := maxLevel - 1; i >= 0; i-- {
		for cur.next[i] != nil && bytes.Compare(cur.next[i].entry.Key, key) < 0 {
			cur = cur.next[i]
		}
	}
	n := cur.next[0]
	if n == nil || !bytes.Equal(n.entry.Key, key) {
		return nil, types.ErrKeyNotFound
	}
	// Scan all entries for this key to find the latest seq <= seqNum
	var best *types.Entry
	for n != nil && bytes.Equal(n.entry.Key, key) {
		if n.entry.SeqNum <= seqNum && (best == nil || n.entry.SeqNum > best.SeqNum) {
			e := n.entry
			best = &e
		}
		n = n.next[0]
	}
	if best != nil {
		return best, nil
	}
	return nil, types.ErrKeyNotFound
}

// Entries returns all entries in sorted key order (used by flush path).
func (m *MemTable) Entries() []types.Entry {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []types.Entry
	cur := m.head.next[0]
	for cur != nil {
		out = append(out, cur.entry)
		cur = cur.next[0]
	}
	return out
}

// NewIterator returns a range-bounded, snapshot-aware iterator over this
// MemTable.  The snapshot of the skip list is taken at construction time;
// concurrent writes are not visible.
func (m *MemTable) NewIterator(opts types.IteratorOptions) types.Iterator {
	m.mu.RLock()
	// Collect a snapshot of entries matching the bounds.
	var entries []types.Entry
	cur := m.head.next[0]
	for cur != nil {
		e := cur.entry
		if opts.LowerBound != nil && bytes.Compare(e.Key, opts.LowerBound) < 0 {
			cur = cur.next[0]
			continue
		}
		if opts.UpperBound != nil && bytes.Compare(e.Key, opts.UpperBound) >= 0 {
			break
		}
		if opts.SnapshotSeq > 0 && e.SeqNum > opts.SnapshotSeq {
			cur = cur.next[0]
			continue
		}
		if e.Tombstone && !opts.IncludeTombstones {
			cur = cur.next[0]
			continue
		}
		entries = append(entries, e)
		cur = cur.next[0]
	}
	m.mu.RUnlock()

	if opts.Reverse {
		// Reverse the slice for backward iteration.
		for i, j := 0, len(entries)-1; i < j; i, j = i+1, j-1 {
			entries[i], entries[j] = entries[j], entries[i]
		}
	}
	return &memIter{entries: entries, pos: 0}
}

// IsFull reports whether the MemTable has hit its flush threshold.
func (m *MemTable) IsFull() bool {
	return atomic.LoadInt64(&m.size) >= m.maxSize
}

// Size returns the approximate byte footprint.
func (m *MemTable) Size() int64 { return atomic.LoadInt64(&m.size) }

// Count returns the number of entries (including tombstones).
func (m *MemTable) Count() int64 { return atomic.LoadInt64(&m.count) }

// ── memIter ───────────────────────────────────────────────────────────────────

type memIter struct {
	entries []types.Entry
	pos     int
	err     error
}

func (it *memIter) Valid() bool       { return it.pos >= 0 && it.pos < len(it.entries) }
func (it *memIter) Next()             { it.pos++ }
func (it *memIter) Prev()             { it.pos-- }
func (it *memIter) Key() []byte       { return it.entries[it.pos].Key }
func (it *memIter) Value() []byte     { return it.entries[it.pos].Value }
func (it *memIter) SeqNum() uint64    { return it.entries[it.pos].SeqNum }
func (it *memIter) IsTombstone() bool { return it.entries[it.pos].Tombstone }
func (it *memIter) Error() error      { return it.err }
func (it *memIter) Close() error      { it.pos = len(it.entries); return nil }
