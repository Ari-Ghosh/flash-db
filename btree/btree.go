// Package btree implements a page-based B+ tree stored in a single flat file.
//
// This is the READ side of the hybrid engine.  The compaction engine bulk-
// loads sorted entries from SSTables into this structure; queries traverse
// from the root page down to leaf pages in O(log n) page reads.
//
// Page layout (fixed 4 KB pages):
//
//	[2]  pageType   0 = internal, 1 = leaf
//	[2]  numKeys
//	[4]  reserved
//	[variable] keys and children / values (see below)
//
// Leaf page cell:
//	[1]  flags (bit0 = tombstone)
//	[8]  seqNum
//	[2]  keyLen
//	[keyLen] key
//	[2]  valLen
//	[valLen] value
//
// Internal page cell:
//	[8]  leftChildPageID
//	[2]  keyLen
//	[keyLen] key
//	(rightmost child pointer appended after last cell)
package btree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"sync"

	hybriddb "local/flashdb/hybriddb"
)

const (
	pageSize    = 4 * 1024
	typeLeaf    = uint16(1)
	typeInternal = uint16(0)
	nullPage    = uint64(0xFFFFFFFFFFFFFFFF)
)

var le = binary.LittleEndian

// ────────────────────────────────────────────────────────────
//  Page structures
// ────────────────────────────────────────────────────────────

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
	id       uint64
	pageType uint16
	leaves   []leafCell
	internals []internalCell
	rightmost uint64 // only for internal pages
	dirty    bool
}

// ────────────────────────────────────────────────────────────
//  BTree
// ────────────────────────────────────────────────────────────

// BTree manages a B+ tree in a single file.
type BTree struct {
	mu       sync.RWMutex
	f        *os.File
	rootID   uint64
	pageCount uint64
	cache    map[uint64]*page // simple page cache
}

// Open opens or creates a B-tree file.
func Open(path string) (*BTree, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("btree open: %w", err)
	}
	bt := &BTree{
		f:      f,
		rootID: nullPage,
		cache:  make(map[uint64]*page, 512),
	}

	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if fi.Size() > 0 {
		if err := bt.loadHeader(); err != nil {
			return nil, err
		}
	} else {
		// New file: allocate a root leaf page.
		root := &page{pageType: typeLeaf, rightmost: nullPage, dirty: true}
		bt.rootID = bt.allocPage(root)
		if err := bt.saveHeader(); err != nil {
			return nil, err
		}
	}
	return bt, nil
}

// Get looks up key in the B-tree.
func (bt *BTree) Get(key []byte) (*hybriddb.Entry, error) {
	bt.mu.RLock()
	defer bt.mu.RUnlock()
	return bt.get(bt.rootID, key)
}

func (bt *BTree) get(pageID uint64, key []byte) (*hybriddb.Entry, error) {
	if pageID == nullPage {
		return nil, hybriddb.ErrKeyNotFound
	}
	p, err := bt.readPage(pageID)
	if err != nil {
		return nil, err
	}

	if p.pageType == typeLeaf {
		for _, c := range p.leaves {
			if bytes.Equal(c.key, key) {
				e := &hybriddb.Entry{
					Key:       c.key,
					Value:     c.value,
					SeqNum:    c.seqNum,
					Tombstone: c.tombstone,
				}
				return e, nil
			}
		}
		return nil, hybriddb.ErrKeyNotFound
	}

	// Internal page: find the right child.
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

// BulkLoad replaces the entire tree with the sorted entries in iter.
// This is called by the compaction engine after merging SSTables.
// It uses a bottom-up bulk-load algorithm: O(n) and cache-friendly.
func (bt *BTree) BulkLoad(entries []hybriddb.Entry) error {
	bt.mu.Lock()
	defer bt.mu.Unlock()

	// Clear the cache and reset page count.
	bt.cache = make(map[uint64]*page, 512)
	bt.pageCount = 0
	bt.rootID = nullPage

	if len(entries) == 0 {
		root := &page{pageType: typeLeaf, rightmost: nullPage, dirty: true}
		bt.rootID = bt.allocPage(root)
		return bt.flushAll()
	}

	// Build leaf level.
	leafPages := bt.buildLeaves(entries)

	// Build internal levels until we have a single root.
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

func (bt *BTree) buildLeaves(entries []hybriddb.Entry) []uint64 {
	const maxCellsPerLeaf = 50 // tunable
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
		for j, childID := range chunk[:len(chunk)-1] {
			// Separator key = first key of the (j+1)th child
			child, _ := bt.readPage(childIDs[i+j+1])
			var sepKey []byte
			if child.pageType == typeLeaf && len(child.leaves) > 0 {
				sepKey = child.leaves[0].key
			} else if len(child.internals) > 0 {
				sepKey = child.internals[0].key
			}
			k := make([]byte, len(sepKey))
			copy(k, sepKey)
			ip.internals = append(ip.internals, internalCell{key: k, leftChildID: childID})
		}
		ip.rightmost = chunk[len(chunk)-1]
		ids = append(ids, bt.allocPage(ip))
	}
	return ids
}

// ────────────────────────────────────────────────────────────
//  Page I/O
// ────────────────────────────────────────────────────────────

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

// Header layout (512 bytes reserved at start of file):
//   [8] magic 0xBEEFCAFE
//   [8] rootID
//   [8] pageCount
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

// ────────────────────────────────────────────────────────────
//  Page encoding
// ────────────────────────────────────────────────────────────

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
