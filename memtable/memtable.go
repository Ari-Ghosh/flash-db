// Package memtable provides a concurrent in-memory buffer backed by a
// skip list.  Writes go here first (after the WAL); the skip list keeps
// entries sorted by key so flushing to a sorted SSTable is a simple
// in-order scan.
//
// Read semantics: the most recently written entry for a key wins.
// A tombstone entry signals a deletion.
package memtable

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"

	hybriddb "local/flashdb/hybriddb"
)

const maxLevel = 16
const probability = 0.25

// node is one element in the skip list.
type node struct {
	entry hybriddb.Entry
	next  [maxLevel]*node
}

// MemTable is a sorted, concurrent in-memory store.
type MemTable struct {
	mu      sync.RWMutex
	head    *node
	size    int64 // approximate byte footprint
	count   int64 // number of live entries
	maxSize int64 // flush threshold in bytes
}

// New creates a MemTable that will signal readiness to flush at maxSize bytes.
func New(maxSize int64) *MemTable {
	return &MemTable{
		head:    &node{},
		maxSize: maxSize,
	}
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
	m.insert(hybriddb.Entry{
		Key:    key,
		Value:  value,
		SeqNum: seq,
	})
	atomic.AddInt64(&m.size, int64(len(key)+len(value)+24))
	atomic.AddInt64(&m.count, 1)
	return nil
}

// Delete inserts a tombstone for key.
func (m *MemTable) Delete(key []byte, seq uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.insert(hybriddb.Entry{
		Key:       key,
		SeqNum:    seq,
		Tombstone: true,
	})
	atomic.AddInt64(&m.size, int64(len(key)+24))
	atomic.AddInt64(&m.count, 1)
	return nil
}

func (m *MemTable) insert(e hybriddb.Entry) {
	update := [maxLevel]*node{}
	cur := m.head

	for i := maxLevel - 1; i >= 0; i-- {
		for cur.next[i] != nil && bytes.Compare(cur.next[i].entry.Key, e.Key) < 0 {
			cur = cur.next[i]
		}
		update[i] = cur
	}

	// If an entry with the same key already exists, overwrite if new seq is higher.
	if cur.next[0] != nil && bytes.Equal(cur.next[0].entry.Key, e.Key) {
		if e.SeqNum > cur.next[0].entry.SeqNum {
			cur.next[0].entry = e
		}
		return
	}

	lvl := m.randomLevel()
	n := &node{entry: e}
	for i := 0; i < lvl; i++ {
		if update[i] == nil {
			update[i] = m.head
		}
		n.next[i] = update[i].next[i]
		update[i].next[i] = n
	}
}

// Get returns the most recent entry for key, or ErrKeyNotFound.
func (m *MemTable) Get(key []byte) (*hybriddb.Entry, error) {
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
		return nil, hybriddb.ErrKeyNotFound
	}
	e := n.entry
	return &e, nil
}

// Entries returns all entries in sorted key order.
// Used by the flush path to build an SSTable.
func (m *MemTable) Entries() []hybriddb.Entry {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var out []hybriddb.Entry
	cur := m.head.next[0]
	for cur != nil {
		out = append(out, cur.entry)
		cur = cur.next[0]
	}
	return out
}

// IsFull reports whether the MemTable has hit its flush threshold.
func (m *MemTable) IsFull() bool {
	return atomic.LoadInt64(&m.size) >= m.maxSize
}

// Size returns the approximate byte footprint.
func (m *MemTable) Size() int64 { return atomic.LoadInt64(&m.size) }

// Count returns the number of entries.
func (m *MemTable) Count() int64 { return atomic.LoadInt64(&m.count) }
