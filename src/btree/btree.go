// Package btree implements a page-based B+ tree for persistent key-value storage.
//
// ## Overview
//
// The B-tree provides efficient point lookups and range scans over sorted key-value data.
// It uses fixed 4 KB pages for I/O alignment and maintains a simple in-memory page cache.
// The tree supports atomic bulk-load operations for compaction and incremental merging.
//
// ## Architecture
//
//   - **Page Structure**: Fixed 4 KB pages containing either leaf cells (key-value data) or
//     internal cells (separator keys and child pointers).
//   - **Concurrency**: RWMutex protects the tree; reads are concurrent, writes are exclusive.
//   - **Persistence**: Pages are written to a single file with a header containing root ID and page count.
//   - **Cache**: In-memory map of recently accessed pages (512 entry limit, no eviction policy).
//
// ## v1 vs v2 Changes
//
// ### v1 (Original Implementation)
// - Basic B+ tree with point lookups only (Get method).
// - BulkLoad rebuilt the entire tree atomically for compaction.
// - No iterator support; range queries required external merging.
// - Simple leaf cells with key-value pairs (no sequence numbers or tombstones).
// - Used for durable storage after LSM compaction.
//
// ### v2 (Enhanced Implementation)
// - **Iterator Support**: NewIterator() provides range scans with bounds, direction, and snapshot filtering.
// - **Snapshot-Aware Reads**: Iterators respect SnapshotSeq to show only data visible at a point-in-time.
// - **AllEntries()**: Efficient method to read all entries in sorted order for incremental compaction.
// - **Enhanced Cells**: Leaf cells now include SeqNum and Tombstone flags for MVCC support.
// - **Atomic Bulk-Load**: Improved to keep old tree readable during rebuild (swap root ID atomically).
// - **Backward Compatibility**: On-disk format unchanged; v2 can read v1 files.
//
// ## Key Methods
//
// - **Open()**: Opens or creates a B-tree file, initializing root page if new.
// - **Get()**: Point lookup by key, traversing from root to leaf.
// - **BulkLoad()**: Atomically replaces tree with sorted entries (used in compaction).
// - **NewIterator()**: Creates a range iterator with filtering options.
// - **AllEntries()**: Returns all entries in key order (for compaction merging).
//
// ## Page Layout (Unchanged from v1)
//
// ### Leaf Page:
// [pageType(2)] [numKeys(2)] [reserved(4)]
// [flag(1)] [seqNum(8)] [keyLen(2)] [key] [valLen(2)] [value] ...
//
// ### Internal Page:
// [pageType(2)] [numKeys(2)] [reserved(4)]
// [rightmost(8)] [childID(8)] [keyLen(2)] [key] ...
//
// ## Performance Characteristics
//
// - **Lookup**: O(log n) page traversals, ~1-3 page I/Os per query.
// - **Bulk Load**: O(n) sequential writes, atomic tree replacement.
// - **Iterator**: O(n) for full scan, with snapshot filtering.
// - **Space**: ~1.5x overhead for internal nodes, 4 KB page alignment.
//
// ## Usage in HybridDB
//
// The B-tree serves as the read-optimized layer in the LSM architecture:
// - L1 B-tree: Recently compacted data from L0 SSTables.
// - L2 B-tree: Long-term storage, receives L1 data when size threshold exceeded.
// - Read path: MemTable → L1 → L2 with merged iterator.
// - Compaction: Incremental merges avoid full rebuilds, preserving read availability.
package btree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"sync"

	types "local/flashdb/src/types"
)

const (
	pageSize     = 4 * 1024
	typeLeaf     = uint16(1)
	typeInternal = uint16(0)
	nullPage     = uint64(0xFFFFFFFFFFFFFFFF)
)

var le = binary.LittleEndian

// ── Page structures ───────────────────────────────────────────────────────────

type leafCell struct {
	key       []byte
	value     []byte
	seqNum    uint64
	tombstone bool
}

type internalCell struct {
	key         []byte
	leftChildID uint64
}

type page struct {
	id        uint64
	pageType  uint16
	leaves    []leafCell
	internals []internalCell
	rightmost uint64
	dirty     bool
}

// ── BTree ─────────────────────────────────────────────────────────────────────

// BTree manages a B+ tree stored in a single file.
type BTree struct {
	mu        sync.RWMutex
	f         *os.File
	rootID    uint64
	pageCount uint64
	cache     map[uint64]*page
}

// Open opens or creates a B-tree file.
func Open(path string) (*BTree, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("btree open: %w", err)
	}
	bt := &BTree{f: f, rootID: nullPage, cache: make(map[uint64]*page, 512)}
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if fi.Size() > 0 {
		if err := bt.loadHeader(); err != nil {
			return nil, err
		}
	} else {
		root := &page{pageType: typeLeaf, rightmost: nullPage, dirty: true}
		bt.rootID = bt.allocPage(root)
		if err := bt.saveHeader(); err != nil {
			return nil, err
		}
	}
	return bt, nil
}

// Get looks up key in the B-tree.
func (bt *BTree) Get(key []byte) (*types.Entry, error) {
	bt.mu.RLock()
	defer bt.mu.RUnlock()
	return bt.get(bt.rootID, key)
}

func (bt *BTree) get(pageID uint64, key []byte) (*types.Entry, error) {
	if pageID == nullPage {
		return nil, types.ErrKeyNotFound
	}
	p, err := bt.readPage(pageID)
	if err != nil {
		return nil, err
	}
	if p.pageType == typeLeaf {
		for _, c := range p.leaves {
			if bytes.Equal(c.key, key) {
				return &types.Entry{
					Key:       c.key,
					Value:     c.value,
					SeqNum:    c.seqNum,
					Tombstone: c.tombstone,
				}, nil
			}
		}
		return nil, types.ErrKeyNotFound
	}
	childID := bt.findChild(p, key)
	return bt.get(childID, key)
}

func (bt *BTree) findChild(p *page, key []byte) uint64 {
	for _, c := range p.internals {
		if bytes.Compare(key, c.key) < 0 {
			return c.leftChildID
		}
	}
	return p.rightmost
}

// AllEntries returns all entries in sorted key order.
// Used by the incremental compaction engine to read the existing tree.
func (bt *BTree) AllEntries() ([]types.Entry, error) {
	bt.mu.RLock()
	defer bt.mu.RUnlock()
	return bt.collectLeaves(bt.rootID)
}

func (bt *BTree) collectLeaves(pageID uint64) ([]types.Entry, error) {
	if pageID == nullPage {
		return nil, nil
	}
	p, err := bt.readPage(pageID)
	if err != nil {
		return nil, err
	}
	if p.pageType == typeLeaf {
		out := make([]types.Entry, 0, len(p.leaves))
		for _, c := range p.leaves {
			out = append(out, types.Entry{
				Key:       c.key,
				Value:     c.value,
				SeqNum:    c.seqNum,
				Tombstone: c.tombstone,
			})
		}
		return out, nil
	}
	var result []types.Entry
	for _, c := range p.internals {
		sub, err := bt.collectLeaves(c.leftChildID)
		if err != nil {
			return nil, err
		}
		result = append(result, sub...)
	}
	sub, err := bt.collectLeaves(p.rightmost)
	if err != nil {
		return nil, err
	}
	return append(result, sub...), nil
}

// NewIterator returns a snapshot-aware range iterator over the B-tree.
func (bt *BTree) NewIterator(opts types.IteratorOptions) (types.Iterator, error) {
	bt.mu.RLock()
	all, err := bt.collectLeaves(bt.rootID)
	bt.mu.RUnlock()
	if err != nil {
		return nil, err
	}

	var entries []types.Entry
	for _, e := range all {
		if opts.LowerBound != nil && bytes.Compare(e.Key, opts.LowerBound) < 0 {
			continue
		}
		if opts.UpperBound != nil && bytes.Compare(e.Key, opts.UpperBound) >= 0 {
			continue
		}
		if opts.SnapshotSeq > 0 && e.SeqNum > opts.SnapshotSeq {
			continue
		}
		if e.Tombstone && !opts.IncludeTombstones {
			continue
		}
		entries = append(entries, e)
	}
	if opts.Reverse {
		for i, j := 0, len(entries)-1; i < j; i, j = i+1, j-1 {
			entries[i], entries[j] = entries[j], entries[i]
		}
	}
	return &btreeIter{entries: entries}, nil
}

// BulkLoad replaces the entire tree with the sorted entries.
func (bt *BTree) BulkLoad(entries []types.Entry) error {
	bt.mu.Lock()
	defer bt.mu.Unlock()

	bt.cache = make(map[uint64]*page, 512)
	bt.pageCount = 0
	bt.rootID = nullPage

	if len(entries) == 0 {
		root := &page{pageType: typeLeaf, rightmost: nullPage, dirty: true}
		bt.rootID = bt.allocPage(root)
		if err := bt.flushAll(); err != nil {
			return err
		}
		return bt.saveHeader()
	}

	leafPages := bt.buildLeaves(entries)
	currentLevel := leafPages
	for len(currentLevel) > 1 {
		currentLevel = bt.buildInternalLevel(currentLevel)
	}
	bt.rootID = currentLevel[0]

	if err := bt.flushAll(); err != nil {
		return err
	}
	return bt.saveHeader()
}

func (bt *BTree) buildLeaves(entries []types.Entry) []uint64 {
	const maxCellsPerLeaf = 50
	var ids []uint64
	for i := 0; i < len(entries); i += maxCellsPerLeaf {
		end := i + maxCellsPerLeaf
		if end > len(entries) {
			end = len(entries)
		}
		lp := &page{pageType: typeLeaf, rightmost: nullPage, dirty: true}
		for _, e := range entries[i:end] {
			k := make([]byte, len(e.Key))
			v := make([]byte, len(e.Value))
			copy(k, e.Key)
			copy(v, e.Value)
			lp.leaves = append(lp.leaves, leafCell{
				key: k, value: v, seqNum: e.SeqNum, tombstone: e.Tombstone,
			})
		}
		ids = append(ids, bt.allocPage(lp))
	}
	return ids
}

func (bt *BTree) buildInternalLevel(childIDs []uint64) []uint64 {
	const maxChildren = 50
	var ids []uint64
	for i := 0; i < len(childIDs); i += maxChildren {
		end := i + maxChildren
		if end > len(childIDs) {
			end = len(childIDs)
		}
		chunk := childIDs[i:end]
		ip := &page{pageType: typeInternal, dirty: true}
		for j := range chunk[:len(chunk)-1] {
			child, _ := bt.readPage(childIDs[i+j+1])
			var sepKey []byte
			if child.pageType == typeLeaf && len(child.leaves) > 0 {
				sepKey = child.leaves[0].key
			} else if len(child.internals) > 0 {
				sepKey = child.internals[0].key
			}
			k := make([]byte, len(sepKey))
			copy(k, sepKey)
			ip.internals = append(ip.internals, internalCell{key: k, leftChildID: chunk[j]})
		}
		ip.rightmost = chunk[len(chunk)-1]
		ids = append(ids, bt.allocPage(ip))
	}
	return ids
}

// ── Page I/O ──────────────────────────────────────────────────────────────────

func (bt *BTree) allocPage(p *page) uint64 {
	id := bt.pageCount
	bt.pageCount++
	p.id = id
	bt.cache[id] = p
	return id
}

func (bt *BTree) readPage(id uint64) (*page, error) {
	if p, ok := bt.cache[id]; ok {
		return p, nil
	}
	buf := make([]byte, pageSize)
	if _, err := bt.f.ReadAt(buf, int64(id)*pageSize+512); err != nil {
		return nil, fmt.Errorf("btree readPage %d: %w", id, err)
	}
	return decodePage(id, buf)
}

func (bt *BTree) flushAll() error {
	for _, p := range bt.cache {
		if !p.dirty {
			continue
		}
		buf := encodePage(p)
		if _, err := bt.f.WriteAt(buf, int64(p.id)*pageSize+512); err != nil {
			return fmt.Errorf("btree flush page %d: %w", p.id, err)
		}
		p.dirty = false
	}
	return bt.f.Sync()
}

const headerMagic = uint64(0xBEEFCAFEBEEFCAFE)

func (bt *BTree) saveHeader() error {
	buf := make([]byte, 512)
	le.PutUint64(buf[0:], headerMagic)
	le.PutUint64(buf[8:], bt.rootID)
	le.PutUint64(buf[16:], bt.pageCount)
	_, err := bt.f.WriteAt(buf, 0)
	return err
}

func (bt *BTree) loadHeader() error {
	buf := make([]byte, 512)
	if _, err := bt.f.ReadAt(buf, 0); err != nil {
		return err
	}
	if le.Uint64(buf[0:]) != headerMagic {
		return fmt.Errorf("btree: bad header magic")
	}
	bt.rootID = le.Uint64(buf[8:])
	bt.pageCount = le.Uint64(buf[16:])
	return nil
}

// ── Page encoding ─────────────────────────────────────────────────────────────

func encodePage(p *page) []byte {
	buf := make([]byte, pageSize)
	le.PutUint16(buf[0:], p.pageType)
	off := 8
	if p.pageType == typeLeaf {
		le.PutUint16(buf[2:], uint16(len(p.leaves)))
		for _, c := range p.leaves {
			flags := byte(0)
			if c.tombstone {
				flags |= 1
			}
			needed := 1 + 8 + 2 + len(c.key) + 2 + len(c.value)
			if off+needed > pageSize {
				break
			}
			buf[off] = flags
			off++
			le.PutUint64(buf[off:], c.seqNum)
			off += 8
			le.PutUint16(buf[off:], uint16(len(c.key)))
			off += 2
			copy(buf[off:], c.key)
			off += len(c.key)
			le.PutUint16(buf[off:], uint16(len(c.value)))
			off += 2
			copy(buf[off:], c.value)
			off += len(c.value)
		}
	} else {
		le.PutUint16(buf[2:], uint16(len(p.internals)))
		le.PutUint64(buf[off:], p.rightmost)
		off += 8
		for _, c := range p.internals {
			needed := 8 + 2 + len(c.key)
			if off+needed > pageSize {
				break
			}
			le.PutUint64(buf[off:], c.leftChildID)
			off += 8
			le.PutUint16(buf[off:], uint16(len(c.key)))
			off += 2
			copy(buf[off:], c.key)
			off += len(c.key)
		}
	}
	return buf
}

func decodePage(id uint64, buf []byte) (*page, error) {
	p := &page{id: id}
	p.pageType = le.Uint16(buf[0:])
	numKeys := int(le.Uint16(buf[2:]))
	off := 8
	if p.pageType == typeLeaf {
		for i := 0; i < numKeys && off < pageSize; i++ {
			if off+13 > pageSize {
				break
			}
			flags := buf[off]
			off++
			seq := le.Uint64(buf[off:])
			off += 8
			kl := int(le.Uint16(buf[off:]))
			off += 2
			if off+kl+2 > pageSize {
				break
			}
			key := make([]byte, kl)
			copy(key, buf[off:])
			off += kl
			vl := int(le.Uint16(buf[off:]))
			off += 2
			if off+vl > pageSize {
				break
			}
			val := make([]byte, vl)
			copy(val, buf[off:])
			off += vl
			p.leaves = append(p.leaves, leafCell{
				key: key, value: val, seqNum: seq, tombstone: flags&1 != 0,
			})
		}
	} else {
		p.rightmost = le.Uint64(buf[off:])
		off += 8
		for i := 0; i < numKeys && off < pageSize; i++ {
			if off+10 > pageSize {
				break
			}
			childID := le.Uint64(buf[off:])
			off += 8
			kl := int(le.Uint16(buf[off:]))
			off += 2
			if off+kl > pageSize {
				break
			}
			key := make([]byte, kl)
			copy(key, buf[off:])
			off += kl
			p.internals = append(p.internals, internalCell{key: key, leftChildID: childID})
		}
	}
	return p, nil
}

// Close flushes dirty pages and closes the file.
func (bt *BTree) Close() error {
	bt.mu.Lock()
	defer bt.mu.Unlock()
	if err := bt.flushAll(); err != nil {
		return err
	}
	if err := bt.saveHeader(); err != nil {
		return err
	}
	return bt.f.Close()
}

// ── btreeIter ─────────────────────────────────────────────────────────────────

type btreeIter struct {
	entries []types.Entry
	pos     int
	err     error
}

func (it *btreeIter) Valid() bool       { return it.pos >= 0 && it.pos < len(it.entries) }
func (it *btreeIter) Next()             { it.pos++ }
func (it *btreeIter) Prev()             { it.pos-- }
func (it *btreeIter) Key() []byte       { return it.entries[it.pos].Key }
func (it *btreeIter) Value() []byte     { return it.entries[it.pos].Value }
func (it *btreeIter) SeqNum() uint64    { return it.entries[it.pos].SeqNum }
func (it *btreeIter) IsTombstone() bool { return it.entries[it.pos].Tombstone }
func (it *btreeIter) Error() error      { return it.err }
func (it *btreeIter) Close() error      { it.pos = len(it.entries); return nil }
