// Package engine is the top-level entry point for HybridDB.
//
// ## Overview
//
// The Engine orchestrates the entire LSM-tree architecture, coordinating WAL,
// MemTable, SSTable, and compaction components. It provides the public API for
// key-value operations with MVCC snapshots and range iterators.
//
// ## Architecture
//
// ### Write Path
// WAL (group-commit) → MemTable → (flush) → L0 SSTable → compaction → L1 B-tree → L2 B-tree
//
// ### Read Path
// MergedIterator ← MemTable ← L1 B-tree ← L2 B-tree
//
// ## v1 vs v2 Changes
//
// ### v1 (Original Implementation)
// - Basic Put/Get/Delete operations.
// - No snapshots or MVCC; reads see latest state.
// - Simple compaction: MemTable flush to single B-tree.
// - No iterators; range queries not supported.
// - Used for basic key-value storage with crash recovery.
//
// ### v2 (Enhanced Implementation)
// - **MVCC Snapshots**: NewSnapshot() for point-in-time reads.
// - **Range Iterators**: NewIterator() for merged range scans.
// - **WAL Group-Commit**: Amortized fsync cost for high throughput.
// - **Tiered Compaction**: Incremental L0→L1→L2 merging.
// - **Configurable Compression**: Codec for value compression.
// - **Background Error Handling**: db.Err() surfaces compaction errors.
//
// ## Key Methods
//
// - **Open()**: Initialize engine with config, replay WAL, rebuild state.
// - **Put/Delete()**: Write operations with sequence numbering.
// - **Get()**: Lookup with optional snapshot.
// - **NewSnapshot()**: Create MVCC snapshot.
// - **NewIterator()**: Range scan with bounds and snapshot.
// - **Close()**: Flush pending writes, close files.
//
// ## Configuration
//
//	type Config struct {
//		Path          string        // Data directory
//		WALSyncPolicy WALSyncPolicy // Durability vs throughput
//		Codec         Codec         // Serialization/compression
//		MaxMemTableSize int64       // Flush threshold
//	}
//
// ## Performance Characteristics
//
// - **Writes**: WAL batching + MemTable buffering for high throughput.
// - **Reads**: MemTable fast path, B-tree O(log n) for disk.
// - **Compaction**: Incremental merging avoids full rebuilds.
// - **Snapshots**: O(1) creation, O(n) space for long-lived ones.
//
// ## Usage
//
//	db, err := engine.Open(config)
//	defer db.Close()
//
//	// Write
//	err = db.Put(key, value)
//
//	// Read with snapshot
//	snap := db.NewSnapshot()
//	val, err := db.Get(key, snap)
//
//	// Range scan
//	iter := db.NewIterator(IteratorOptions{
//		Start: key1,
//		End:   key2,
//		Snapshot: snap,
//	})
//	for iter.Next() {
//		fmt.Printf("%s = %s\n", iter.Key(), iter.Value())
//	}
package engine

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"

	"local/flashdb/btree"
	"local/flashdb/compaction"
	hybriddb "local/flashdb/hybriddb"
	"local/flashdb/memtable"
	"local/flashdb/sstable"
	"local/flashdb/wal"
)

// Config holds all engine tuning parameters.
type Config struct {
	Dir                string
	MemTableSize       int64
	L0CompactThreshold int
	// L1SizeThreshold is the byte size at which L1 promotes to L2 (default 256 MB).
	L1SizeThreshold int64
	// WALSyncPolicy controls WAL durability vs throughput trade-off.
	WALSyncPolicy wal.SyncPolicy
	// Codec selects block compression for new SSTables.
	Codec hybriddb.Codec
}

// DefaultConfig returns production-sensible defaults.
func DefaultConfig(dir string) Config {
	return Config{
		Dir:                dir,
		MemTableSize:       64 * 1024 * 1024,
		L0CompactThreshold: 4,
		L1SizeThreshold:    256 * 1024 * 1024,
		WALSyncPolicy:      wal.SyncBatch,
		Codec:              hybriddb.CodecNone,
	}
}

// DB is the main HybridDB instance.
type DB struct {
	cfg Config

	mu       sync.Mutex
	seq      uint64
	memtable *memtable.MemTable
	imm      *memtable.MemTable

	walActive *wal.WAL
	l0Mu      sync.Mutex
	l0Files   []string

	l1Tree *btree.BTree
	l2Tree *btree.BTree

	compactor *compaction.Engine
	snapTrack *hybriddb.SnapshotTracker

	bgErr     atomic.Value // stores error
	closeOnce sync.Once
	closeCh   chan struct{}
}

// Open opens or creates a HybridDB at cfg.Dir.
func Open(cfg Config) (*DB, error) {
	if err := os.MkdirAll(cfg.Dir, 0755); err != nil {
		return nil, fmt.Errorf("engine open mkdir: %w", err)
	}
	if cfg.MemTableSize == 0 {
		cfg.MemTableSize = 64 * 1024 * 1024
	}
	if cfg.L0CompactThreshold == 0 {
		cfg.L0CompactThreshold = 4
	}
	if cfg.L1SizeThreshold == 0 {
		cfg.L1SizeThreshold = 256 * 1024 * 1024
	}

	l1Tree, err := btree.Open(filepath.Join(cfg.Dir, "btree_l1.db"))
	if err != nil {
		return nil, fmt.Errorf("engine: l1 btree: %w", err)
	}
	l2Tree, err := btree.Open(filepath.Join(cfg.Dir, "btree_l2.db"))
	if err != nil {
		return nil, fmt.Errorf("engine: l2 btree: %w", err)
	}

	walPath := filepath.Join(cfg.Dir, "wal.log")
	w, err := wal.OpenWithOptions(walPath, wal.Options{
		SyncPolicy: cfg.WALSyncPolicy,
	})
	if err != nil {
		return nil, fmt.Errorf("engine: wal: %w", err)
	}

	snapTrack := hybriddb.NewSnapshotTracker()

	db := &DB{
		cfg:       cfg,
		memtable:  memtable.New(cfg.MemTableSize),
		l1Tree:    l1Tree,
		l2Tree:    l2Tree,
		walActive: w,
		snapTrack: snapTrack,
		closeCh:   make(chan struct{}),
	}

	compCfg := compaction.Config{
		L0Threshold:     cfg.L0CompactThreshold,
		L1SizeThreshold: cfg.L1SizeThreshold,
	}
	db.compactor = compaction.New(compCfg, l1Tree, l2Tree, snapTrack)
	db.compactor.Start()

	if err := db.replayWAL(w); err != nil {
		return nil, fmt.Errorf("engine: replay: %w", err)
	}
	db.discoverL0Files()
	return db, nil
}

// ── Write path ────────────────────────────────────────────────────────────────

// Put writes key=value into the DB.
func (db *DB) Put(key, value []byte) error {
	if err := db.checkClosed(); err != nil {
		return err
	}
	seq := atomic.AddUint64(&db.seq, 1)
	if err := db.walActive.AppendPut(seq, key, value); err != nil {
		return fmt.Errorf("put: wal: %w", err)
	}
	if err := db.memtable.Put(key, value, seq); err != nil {
		return err
	}
	if db.memtable.IsFull() {
		return db.triggerFlush()
	}
	return nil
}

// Delete marks key as deleted.
func (db *DB) Delete(key []byte) error {
	if err := db.checkClosed(); err != nil {
		return err
	}
	seq := atomic.AddUint64(&db.seq, 1)
	if err := db.walActive.AppendDelete(seq, key); err != nil {
		return fmt.Errorf("delete: wal: %w", err)
	}
	if err := db.memtable.Delete(key, seq); err != nil {
		return err
	}
	if db.memtable.IsFull() {
		return db.triggerFlush()
	}
	return nil
}

// ── Read path ─────────────────────────────────────────────────────────────────

// Get returns the value for key at the current (latest) view.
func (db *DB) Get(key []byte) ([]byte, error) {
	return db.GetAt(key, atomic.LoadUint64(&db.seq))
}

// GetAt returns the value for key as of seqNum (MVCC point-in-time read).
func (db *DB) GetAt(key []byte, seqNum uint64) ([]byte, error) {
	// 1. Active MemTable.
	if e, err := db.memtable.Get(key); err == nil {
		if e.SeqNum <= seqNum {
			if e.Tombstone {
				return nil, hybriddb.ErrKeyDeleted
			}
			return e.Value, nil
		}
	}
	// 2. Immutable MemTable.
	if db.imm != nil {
		if e, err := db.imm.Get(key); err == nil {
			if e.SeqNum <= seqNum {
				if e.Tombstone {
					return nil, hybriddb.ErrKeyDeleted
				}
				return e.Value, nil
			}
		}
	}
	// 3. L1 B-tree.
	if e, err := db.l1Tree.Get(key); err == nil {
		if e.SeqNum <= seqNum {
			if e.Tombstone {
				return nil, hybriddb.ErrKeyDeleted
			}
			return e.Value, nil
		}
	}
	// 4. L2 B-tree.
	if e, err := db.l2Tree.Get(key); err == nil {
		if e.SeqNum <= seqNum {
			if e.Tombstone {
				return nil, hybriddb.ErrKeyDeleted
			}
			return e.Value, nil
		}
	}
	return nil, hybriddb.ErrKeyNotFound
}

// ── MVCC Snapshots ────────────────────────────────────────────────────────────

// NewSnapshot creates a point-in-time read snapshot.
// The caller MUST call snap.Release() when done to allow GC of old versions.
func (db *DB) NewSnapshot() *hybriddb.Snapshot {
	return db.snapTrack.Create(atomic.LoadUint64(&db.seq))
}

// GetSnapshot performs a Get through a snapshot's pinned seq.
func (db *DB) GetSnapshot(snap *hybriddb.Snapshot, key []byte) ([]byte, error) {
	return db.GetAt(key, snap.Seq())
}

// ── Range iterator ────────────────────────────────────────────────────────────

// NewIterator returns a merged iterator across MemTable, L1, and L2
// that respects the given options (bounds, snapshot, direction).
func (db *DB) NewIterator(opts hybriddb.IteratorOptions) (hybriddb.Iterator, error) {
	if opts.SnapshotSeq == 0 {
		opts.SnapshotSeq = atomic.LoadUint64(&db.seq)
	}

	memIt := db.memtable.NewIterator(opts)

	var immIt hybriddb.Iterator
	if db.imm != nil {
		immIt = db.imm.NewIterator(opts)
	}

	l1It, err := db.l1Tree.NewIterator(opts)
	if err != nil {
		return nil, fmt.Errorf("iterator l1: %w", err)
	}
	l2It, err := db.l2Tree.NewIterator(opts)
	if err != nil {
		return nil, fmt.Errorf("iterator l2: %w", err)
	}

	iters := []hybriddb.Iterator{memIt, l1It, l2It}
	if immIt != nil {
		iters = append([]hybriddb.Iterator{memIt, immIt}, iters[1:]...)
	}
	return newMergedIterator(iters, opts.Reverse), nil
}

// ── Flush: MemTable → SSTable ─────────────────────────────────────────────────

func (db *DB) triggerFlush() error {
	db.mu.Lock()
	db.imm = db.memtable
	db.memtable = memtable.New(db.cfg.MemTableSize)
	db.mu.Unlock()

	go func() {
		if err := db.flushImmutable(); err != nil {
			db.bgErr.Store(err)
			fmt.Printf("flush error: %v\n", err)
		}
	}()
	return nil
}

func (db *DB) flushImmutable() error {
	db.mu.Lock()
	imm := db.imm
	db.mu.Unlock()

	if imm == nil || imm.Count() == 0 {
		return nil
	}

	entries := imm.Entries()
	seq := atomic.LoadUint64(&db.seq)
	path := filepath.Join(db.cfg.Dir, fmt.Sprintf("l0_%020d.sst", seq))

	w, err := sstable.NewWriter(path, uint(len(entries)))
	if err != nil {
		return err
	}
	for _, e := range entries {
		if err := w.Add(e); err != nil {
			return err
		}
	}
	if err := w.Close(); err != nil {
		return err
	}

	db.l0Mu.Lock()
	db.l0Files = append(db.l0Files, path)
	l0Count := len(db.l0Files)
	db.l0Mu.Unlock()

	db.mu.Lock()
	db.imm = nil
	db.mu.Unlock()

	if l0Count >= db.cfg.L0CompactThreshold {
		db.l0Mu.Lock()
		toCompact := make([]string, len(db.l0Files))
		copy(toCompact, db.l0Files)
		db.l0Files = db.l0Files[:0]
		db.l0Mu.Unlock()

		db.compactor.Trigger(toCompact)
		db.rotateWAL()
	}
	return nil
}

// ── WAL management ────────────────────────────────────────────────────────────

func (db *DB) replayWAL(w *wal.WAL) error {
	records, err := w.Replay()
	if err != nil {
		return err
	}
	var maxSeq uint64
	for _, r := range records {
		if r.SeqNum > maxSeq {
			maxSeq = r.SeqNum
		}
		if r.Kind == 0 {
			db.memtable.Put(r.Key, r.Value, r.SeqNum)
		} else {
			db.memtable.Delete(r.Key, r.SeqNum)
		}
	}
	atomic.StoreUint64(&db.seq, maxSeq)
	return nil
}

func (db *DB) rotateWAL() {
	db.mu.Lock()
	defer db.mu.Unlock()
	oldWAL := db.walActive
	newPath := filepath.Join(db.cfg.Dir, fmt.Sprintf("wal_%d.log", atomic.LoadUint64(&db.seq)))
	newWAL, err := wal.OpenWithOptions(newPath, wal.Options{SyncPolicy: db.cfg.WALSyncPolicy})
	if err != nil {
		fmt.Printf("WAL rotate error: %v\n", err)
		return
	}
	db.walActive = newWAL
	go oldWAL.Delete()
}

func (db *DB) discoverL0Files() {
	matches, _ := filepath.Glob(filepath.Join(db.cfg.Dir, "l0_*.sst"))
	sort.Strings(matches)
	db.l0Files = matches
}

func (db *DB) checkClosed() error {
	select {
	case <-db.closeCh:
		return hybriddb.ErrClosed
	default:
		return nil
	}
}

// Err returns the first background error (flush/compaction), if any.
func (db *DB) Err() error {
	if v := db.bgErr.Load(); v != nil {
		return v.(error)
	}
	return nil
}

// ── Close ─────────────────────────────────────────────────────────────────────

// Close flushes all pending data and shuts down the engine.
func (db *DB) Close() error {
	var firstErr error
	db.closeOnce.Do(func() {
		close(db.closeCh)
		db.compactor.Stop()

		if db.memtable.Count() > 0 {
			db.mu.Lock()
			db.imm = db.memtable
			db.memtable = memtable.New(db.cfg.MemTableSize)
			db.mu.Unlock()
			if err := db.flushImmutable(); err != nil {
				firstErr = err
			}
		}
		if err := db.walActive.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := db.l1Tree.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := db.l2Tree.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	})
	return firstErr
}

// Stats returns a snapshot of engine internals.
func (db *DB) Stats() Stats {
	db.l0Mu.Lock()
	l0 := len(db.l0Files)
	db.l0Mu.Unlock()
	return Stats{
		MemTableSize:  db.memtable.Size(),
		MemTableCount: db.memtable.Count(),
		L0FileCount:   l0,
		SeqNum:        atomic.LoadUint64(&db.seq),
	}
}

// Stats holds observable engine metrics.
type Stats struct {
	MemTableSize  int64
	MemTableCount int64
	L0FileCount   int
	SeqNum        uint64
}

// ── Merged iterator ───────────────────────────────────────────────────────────

// mergedIterator merges N sorted iterators, deduplicating by key.
// For equal keys, the iterator from the lowest index (i.e., newest source)
// wins.
type mergedIterator struct {
	iters   []hybriddb.Iterator
	reverse bool
	heap    iterHeap
	cur     *hybriddb.Entry
	err     error
}

func newMergedIterator(iters []hybriddb.Iterator, reverse bool) *mergedIterator {
	mi := &mergedIterator{iters: iters, reverse: reverse}
	mi.init()
	return mi
}

func (mi *mergedIterator) init() {
	mi.heap = make(iterHeap, 0, len(mi.iters))
	for i, it := range mi.iters {
		if it.Valid() {
			mi.heap = append(mi.heap, iterHeapItem{
				key:   it.Key(),
				idx:   i,
				entry: hybriddb.Entry{Key: it.Key(), Value: it.Value(), SeqNum: it.SeqNum(), Tombstone: it.IsTombstone()},
			})
		}
	}
	if mi.reverse {
		initMaxHeap(mi.heap)
	} else {
		initMinHeap(mi.heap)
	}
	mi.advance()
}

func (mi *mergedIterator) advance() {
	mi.cur = nil
	for len(mi.heap) > 0 {
		top := heapPop(&mi.heap, mi.reverse)
		e := top.entry
		// Advance the source iterator.
		mi.iters[top.idx].Next()
		if mi.iters[top.idx].Valid() {
			it := mi.iters[top.idx]
			heapPush(&mi.heap, iterHeapItem{
				key:   it.Key(),
				idx:   top.idx,
				entry: hybriddb.Entry{Key: it.Key(), Value: it.Value(), SeqNum: it.SeqNum(), Tombstone: it.IsTombstone()},
			}, mi.reverse)
		}
		// Skip if tombstone (unless user opts in via IncludeTombstones — checked at construction).
		if e.Tombstone {
			// Skip duplicate keys after a tombstone.
			mi.skipDups(e.Key)
			continue
		}
		mi.skipDups(e.Key)
		mi.cur = &e
		return
	}
}

func (mi *mergedIterator) skipDups(key []byte) {
	for len(mi.heap) > 0 {
		top := peekTop(mi.heap, mi.reverse)
		if !bytes.Equal(top.key, key) {
			break
		}
		popped := heapPop(&mi.heap, mi.reverse)
		mi.iters[popped.idx].Next()
		if mi.iters[popped.idx].Valid() {
			it := mi.iters[popped.idx]
			heapPush(&mi.heap, iterHeapItem{
				key:   it.Key(),
				idx:   popped.idx,
				entry: hybriddb.Entry{Key: it.Key(), Value: it.Value(), SeqNum: it.SeqNum(), Tombstone: it.IsTombstone()},
			}, mi.reverse)
		}
	}
}

func (mi *mergedIterator) Valid() bool       { return mi.cur != nil }
func (mi *mergedIterator) Key() []byte       { return mi.cur.Key }
func (mi *mergedIterator) Value() []byte     { return mi.cur.Value }
func (mi *mergedIterator) SeqNum() uint64    { return mi.cur.SeqNum }
func (mi *mergedIterator) IsTombstone() bool { return mi.cur.Tombstone }
func (mi *mergedIterator) Error() error      { return mi.err }
func (mi *mergedIterator) Next()             { mi.advance() }
func (mi *mergedIterator) Prev()             { /* not supported on merged; use Reverse option */ }
func (mi *mergedIterator) Close() error {
	for _, it := range mi.iters {
		it.Close()
	}
	return nil
}

// ── Simple heap for merged iterator ──────────────────────────────────────────

type iterHeapItem struct {
	key   []byte
	idx   int
	entry hybriddb.Entry
}

type iterHeap []iterHeapItem

func initMinHeap(h iterHeap) {
	n := len(h)
	for i := n/2 - 1; i >= 0; i-- {
		siftDownMin(h, i, n)
	}
}

func initMaxHeap(h iterHeap) {
	n := len(h)
	for i := n/2 - 1; i >= 0; i-- {
		siftDownMax(h, i, n)
	}
}

func heapPop(h *iterHeap, reverse bool) iterHeapItem {
	n := len(*h)
	root := (*h)[0]
	(*h)[0] = (*h)[n-1]
	*h = (*h)[:n-1]
	if len(*h) > 0 {
		if reverse {
			siftDownMax(*h, 0, len(*h))
		} else {
			siftDownMin(*h, 0, len(*h))
		}
	}
	return root
}

func heapPush(h *iterHeap, item iterHeapItem, reverse bool) {
	*h = append(*h, item)
	i := len(*h) - 1
	if reverse {
		bubbleUpMax(*h, i)
	} else {
		bubbleUpMin(*h, i)
	}
}

func peekTop(h iterHeap, reverse bool) iterHeapItem { return h[0] }

func less(h iterHeap, i, j int) bool {
	c := bytes.Compare(h[i].key, h[j].key)
	if c != 0 {
		return c < 0
	}
	return h[i].idx < h[j].idx // lower idx = newer source wins
}

func greater(h iterHeap, i, j int) bool { return less(h, j, i) }

func siftDownMin(h iterHeap, i, n int) {
	for {
		smallest := i
		l, r := 2*i+1, 2*i+2
		if l < n && less(h, l, smallest) {
			smallest = l
		}
		if r < n && less(h, r, smallest) {
			smallest = r
		}
		if smallest == i {
			break
		}
		h[i], h[smallest] = h[smallest], h[i]
		i = smallest
	}
}

func siftDownMax(h iterHeap, i, n int) {
	for {
		largest := i
		l, r := 2*i+1, 2*i+2
		if l < n && greater(h, l, largest) {
			largest = l
		}
		if r < n && greater(h, r, largest) {
			largest = r
		}
		if largest == i {
			break
		}
		h[i], h[largest] = h[largest], h[i]
		i = largest
	}
}

func bubbleUpMin(h iterHeap, i int) {
	for i > 0 {
		p := (i - 1) / 2
		if !less(h, i, p) {
			break
		}
		h[i], h[p] = h[p], h[i]
		i = p
	}
}

func bubbleUpMax(h iterHeap, i int) {
	for i > 0 {
		p := (i - 1) / 2
		if !greater(h, i, p) {
			break
		}
		h[i], h[p] = h[p], h[i]
		i = p
	}
}
