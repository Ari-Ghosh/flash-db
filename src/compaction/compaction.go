// Package compaction implements incremental, tiered SSTable→B-tree compaction.
//
// ## Overview
//
// The compaction engine maintains the LSM-tree structure by incrementally
// merging L0 SSTables into L1/L2 B-trees. It uses atomic swaps to avoid
// blocking reads and supports MVCC tombstone garbage collection.
//
// ## Architecture
//
// - **Tiered Storage**: L0 (SSTables) → L1 (B-tree) → L2 (B-tree).
// - **Incremental Merging**: Delta B-trees merged alongside existing ones.
// - **Atomic Swaps**: New trees replace old ones without read blocking.
// - **Tombstone GC**: Deletes only removed when safe for all snapshots.
//
// ## v1 vs v2 Changes
//
// ### v1 (Original Implementation)
// - Simple compaction: MemTable flush to single B-tree.
// - Blocking rebuilds: Reads wait during full compaction.
// - No tiering: All data in one B-tree file.
// - Basic tombstone handling: Deletes immediately remove entries.
//
// ### v2 (Enhanced Implementation)
// - **Incremental Compaction**: Delta merges avoid full rebuilds.
// - **Tiered Storage**: L0→L1→L2 with separate B-trees.
// - **Non-Blocking**: Atomic swaps keep old trees readable.
// - **MVCC Tombstone GC**: Deletes only when safe for snapshots.
// - **Concurrent Triggers**: Unbounded channel prevents dropped requests.
//
// ## Key Methods
//
// - **NewCompactor()**: Initialize with engine reference.
// - **Compact()**: Trigger L0→L1 compaction.
// - **CompactL1ToL2()**: Merge L1 into L2 when threshold exceeded.
// - **mergeIntoBtree()**: Incremental merge with tombstone filtering.
//
// ## Compaction Process
//
// 1. **Trigger**: MemTable flush creates L0 SSTable.
// 2. **Merge**: Read L0 + existing L1, write delta B-tree.
// 3. **Swap**: Atomically replace old L1 with new merged tree.
// 4. **GC**: Remove old files and unreferenced tombstones.
//
// ## Performance Characteristics
//
// - **Incremental**: O(changes) vs O(total) for full rebuilds.
// - **Non-Blocking**: Reads continue on old trees during merge.
// - **Tiered**: L1/L2 queries amortize seek costs.
// - **GC**: Tombstones retained until snapshot safety.
//
// ## Usage in HybridDB
//
// Compaction runs in background goroutine:
// - Engine flushes MemTable → L0 SSTable → triggers compaction.
// - Compactor merges L0 into L1, L1 into L2 as needed.
// - Read path: MemTable → L1 → L2 (merged iterator).
package compaction

import (
	"bytes"
	"container/heap"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"

	"github.com/Ari-Ghosh/flash-db/src/btree"
	"github.com/Ari-Ghosh/flash-db/src/sstable"
	types "github.com/Ari-Ghosh/flash-db/src/types"
)

// BloomTelemetryCleaner lets compaction remove stale bloom-filter telemetry
// entries for SSTable files that have been merged and deleted.
// Satisfied by *bloom.BloomTelemetry.
type BloomTelemetryCleaner interface {
	Remove(path string)
}

// Config controls compaction behaviour.
type Config struct {
	// L0Threshold is the number of L0 SSTables that triggers L0→L1 compaction.
	L0Threshold int
	// L1SizeThreshold is the approximate byte size that triggers L1→L2 compaction.
	L1SizeThreshold int64
}

// DefaultConfig returns sensible defaults.
func DefaultConfig() Config {
	return Config{
		L0Threshold:     4,
		L1SizeThreshold: 256 * 1024 * 1024, // 256 MB
	}
}

// SnapshotProvider is satisfied by the engine's SnapshotTracker.
type SnapshotProvider interface {
	OldestPinnedSeq() uint64
}

// Engine manages background compaction.
type Engine struct {
	cfg      Config
	l1Tree   *btree.BTree
	l2Tree   *btree.BTree
	snapProv SnapshotProvider
	bloomTC  BloomTelemetryCleaner // may be nil

	mu      sync.Mutex
	trigCh  chan struct{} // signals the worker there is work
	pending [][]string    // queued L0 file batches
	quitCh  chan struct{}
	doneCh  chan struct{}

	l0MergeCount atomic.Uint64
	l1MergeCount atomic.Uint64
}

// New creates a compaction engine.
func New(cfg Config, l1Tree, l2Tree *btree.BTree, sp SnapshotProvider, btc BloomTelemetryCleaner) *Engine {
	return &Engine{
		cfg:      cfg,
		l1Tree:   l1Tree,
		l2Tree:   l2Tree,
		snapProv: sp,
		bloomTC:  btc,
		trigCh:   make(chan struct{}, 1),
		quitCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
	}
}

// Start launches the background compaction goroutine.
func (e *Engine) Start() { go e.run() }

// Stop shuts down the background goroutine gracefully.
func (e *Engine) Stop() {
	close(e.quitCh)
	<-e.doneCh
}

// Trigger enqueues a set of SSTable paths for L0 compaction.
// Unlike v1, this never silently drops a trigger.
func (e *Engine) Trigger(paths []string) {
	e.mu.Lock()
	e.pending = append(e.pending, paths)
	e.mu.Unlock()
	select {
	case e.trigCh <- struct{}{}:
	default:
	}
}

func (e *Engine) run() {
	defer close(e.doneCh)
	for {
		select {
		case <-e.trigCh:
			e.drainPending()
		case <-e.quitCh:
			e.drainPending() // flush any last work
			return
		}
	}
}

func (e *Engine) drainPending() {
	for {
		e.mu.Lock()
		if len(e.pending) == 0 {
			e.mu.Unlock()
			return
		}
		batch := e.pending[0]
		e.pending = e.pending[1:]
		e.mu.Unlock()

		if err := e.compactL0(batch); err != nil {
			log.Printf("compaction L0 error: %v", err)
		}
	}
}

// compactL0 merges the given L0 SSTable files into the L1 B-tree.
func (e *Engine) compactL0(paths []string) error {
	if len(paths) == 0 {
		return nil
	}

	readers := make([]*sstable.Reader, 0, len(paths))
	for _, p := range paths {
		r, err := sstable.OpenReader(p)
		if err != nil {
			return fmt.Errorf("compaction open %s: %w", p, err)
		}
		readers = append(readers, r)
	}
	defer func() {
		for _, r := range readers {
			_ = r.Close()
		}
	}()

	oldest := e.snapProv.OldestPinnedSeq()

	// Open L1 entries as a channel for streaming merge.
	l1Chan, err := e.l1Tree.StreamEntries()
	if err != nil {
		return fmt.Errorf("compaction stream L1: %w", err)
	}

	iter, err := streamMerge(readers, l1Chan, oldest)
	if err != nil {
		return fmt.Errorf("compaction stream-merge: %w", err)
	}

	if err := e.l1Tree.BulkLoadFromIter(iter); err != nil {
		return fmt.Errorf("compaction bulk-load L1: %w", err)
	}

	for _, p := range paths {
		if err := os.Remove(p); err != nil {
			log.Printf("compaction: remove %s: %v", p, err)
		}
		if e.bloomTC != nil {
			e.bloomTC.Remove(p)
		}
	}
	log.Printf("compaction: merged %d L0 SSTables → L1 (streaming)", len(paths))
	e.l0MergeCount.Add(1)

	if e.l2Tree != nil {
		go e.maybeCompactL1toL2()
	}
	return nil
}

// maybeCompactL1toL2 promotes overflow data from L1 into L2.
func (e *Engine) maybeCompactL1toL2() {
	l1Entries, err := e.l1Tree.AllEntries()
	if err != nil || len(l1Entries) == 0 {
		return
	}

	// Rough size estimate.
	var approxSize int64
	for _, en := range l1Entries {
		approxSize += int64(len(en.Key) + len(en.Value) + 20)
	}
	if approxSize < e.cfg.L1SizeThreshold {
		return
	}

	log.Printf("compaction: L1→L2 triggered (approx %d bytes)", approxSize)

	l2Entries, err := e.l2Tree.AllEntries()
	if err != nil {
		log.Printf("compaction L1→L2 read L2: %v", err)
		return
	}

	oldest := e.snapProv.OldestPinnedSeq()
	combined := mergeTwo(l2Entries, l1Entries, oldest)

	if err := e.l2Tree.BulkLoad(combined); err != nil {
		log.Printf("compaction L1→L2 bulk-load: %v", err)
		return
	}

	// Clear L1 — all data is now in L2.
	if err := e.l1Tree.BulkLoad(nil); err != nil {
		log.Printf("compaction L1 clear: %v", err)
	}

	log.Printf("compaction: L1→L2 complete (%d entries in L2)", len(combined))
	e.l1MergeCount.Add(1)
}

// L0MergeCount returns the total number of L0→L1 compaction merges.
func (e *Engine) L0MergeCount() uint64 { return e.l0MergeCount.Load() }

// L1MergeCount returns the total number of L1→L2 compaction merges.
func (e *Engine) L1MergeCount() uint64 { return e.l1MergeCount.Load() }

// ── helpers ───────────────────────────────────────────────────────────────────

// mergeTwo merges two pre-sorted entry slices (by key, then descending SeqNum).
// Tombstones are dropped only if their SeqNum < oldestPinnedSeq.
func mergeTwo(base, overlay []types.Entry, oldestPinnedSeq uint64) []types.Entry {
	out := make([]types.Entry, 0, len(base)+len(overlay))
	i, j := 0, 0
	var lastKey []byte

	emit := func(e types.Entry) {
		if lastKey != nil && bytes.Equal(lastKey, e.Key) {
			return // already emitted a higher-seq version
		}
		// Drop tombstone if no snapshot can see it.
		if e.Tombstone && e.SeqNum < oldestPinnedSeq {
			lastKey = e.Key
			return
		}
		out = append(out, e)
		lastKey = e.Key
	}

	for i < len(overlay) && j < len(base) {
		cmp := bytes.Compare(overlay[i].Key, base[j].Key)
		switch {
		case cmp < 0:
			emit(overlay[i])
			i++
		case cmp > 0:
			emit(base[j])
			j++
		default: // equal key: prefer higher seqNum
			if overlay[i].SeqNum >= base[j].SeqNum {
				emit(overlay[i])
			} else {
				emit(base[j])
			}
			i++
			j++
		}
	}
	for ; i < len(overlay); i++ {
		emit(overlay[i])
	}
	for ; j < len(base); j++ {
		emit(base[j])
	}
	return out
}

// ── heap types (used by streamMerge) ──────────────────────────────────────────

type heapItem struct {
	entry     types.Entry
	readerIdx int
	ch        <-chan types.Entry
}

type mergeHeap []heapItem

func (h mergeHeap) Len() int      { return len(h) }
func (h mergeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h mergeHeap) Less(i, j int) bool {
	cmp := bytes.Compare(h[i].entry.Key, h[j].entry.Key)
	if cmp != 0 {
		return cmp < 0
	}
	return h[i].entry.SeqNum > h[j].entry.SeqNum
}
func (h *mergeHeap) Push(x any) {
	item, ok := x.(heapItem)
	if !ok {
		panic("compaction: invalid heap item")
	}
	*h = append(*h, item)
}
func (h *mergeHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// ── streaming merge ───────────────────────────────────────────────────────────

// streamMergeNexter is an interface for sources that yield entries sequentially.
// Satisfied by both sstable.Reader.Iter() channels and btree.StreamEntries() channels.
type streamMergeNexter struct {
	ch  <-chan types.Entry
	cur *types.Entry
	ok  bool
}

func newStreamNexter(ch <-chan types.Entry) *streamMergeNexter {
	n := &streamMergeNexter{ch: ch}
	n.advance()
	return n
}

func (n *streamMergeNexter) advance() {
	e, ok := <-n.ch
	if ok {
		n.cur = &e
		n.ok = true
	} else {
		n.cur = nil
		n.ok = false
	}
}

func (n *streamMergeNexter) valid() bool { return n.ok }

// streamMerge performs a streaming k-way merge of L0 SSTables and L1 B-tree
// entries, yielding deduplicated, tombstone-GC'd entries one at a time via a
// channel.  Peak memory is O(k) where k is the number of source streams.
// Unlike the old kWayMerge + mergeTwo approach, this never materializes the
// entire result set.
func streamMerge(readers []*sstable.Reader, l1Ch <-chan types.Entry, oldestPinnedSeq uint64) (types.Iterator, error) {
	sources := make([]*streamMergeNexter, 0, len(readers)+1)
	for _, r := range readers {
		ch, err := r.Iter()
		if err != nil {
			return nil, err
		}
		sources = append(sources, newStreamNexter(ch))
	}
	sources = append(sources, newStreamNexter(l1Ch))

	h := &mergeHeap{}
	heap.Init(h)
	for i, s := range sources {
		if s.valid() {
			heap.Push(h, heapItem{entry: *s.cur, readerIdx: i, ch: s.ch})
		}
	}

	out := make(chan types.Entry, 256)
	go func() {
		defer close(out)
		streamMergeInner(h, sources, oldestPinnedSeq, out)
	}()

	return &chanIter{ch: out}, nil
}

func streamMergeInner(h *mergeHeap, sources []*streamMergeNexter, oldestPinnedSeq uint64, out chan<- types.Entry) {
	var candidates []types.Entry
	var currentKey []byte

	emitBest := func() {
		if len(candidates) == 0 {
			return
		}
		best := candidates[0]
		for _, c := range candidates[1:] {
			if c.SeqNum > best.SeqNum {
				best = c
			}
		}
		if !best.Tombstone || best.SeqNum >= oldestPinnedSeq {
			out <- best
		}
		candidates = candidates[:0]
	}

	for h.Len() > 0 {
		itemAny := heap.Pop(h)
		item, ok := itemAny.(heapItem)
		if !ok {
			continue
		}
		e := item.entry

		src := sources[item.readerIdx]
		src.advance()
		if src.valid() {
			heap.Push(h, heapItem{entry: *src.cur, readerIdx: item.readerIdx, ch: src.ch})
		}

		if currentKey == nil || !bytes.Equal(currentKey, e.Key) {
			emitBest()
			currentKey = e.Key
		}
		candidates = append(candidates, e)
	}
	emitBest()
}

type chanIter struct {
	ch    chan types.Entry
	cur   *types.Entry
	done  bool
	err   error
}

func (it *chanIter) advance() {
	it.cur = nil
	e, ok := <-it.ch
	if !ok {
		it.done = true
		return
	}
	it.cur = &e
}

func (it *chanIter) Valid() bool {
	if it.cur == nil && !it.done {
		it.advance()
	}
	return it.cur != nil
}
func (it *chanIter) Next()             { it.advance() }
func (it *chanIter) Prev()             {}
func (it *chanIter) Key() []byte       { return it.cur.Key }
func (it *chanIter) Value() []byte     { return it.cur.Value }
func (it *chanIter) SeqNum() uint64    { return it.cur.SeqNum }
func (it *chanIter) IsTombstone() bool { return it.cur.Tombstone }
func (it *chanIter) Error() error      { return it.err }
func (it *chanIter) Close() error      { return nil }
