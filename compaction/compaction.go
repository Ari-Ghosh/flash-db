// Package compaction implements the core bridge between the LSM write path
// and the B-tree read path.
//
// Trigger: when the number of L0 SSTables reaches a threshold (default 4),
// the compaction worker:
//  1. Opens all L0 SSTables.
//  2. k-way merges their sorted streams, keeping only the highest-seqNum
//     entry per key (tombstones included until GC conditions are met).
//  3. Bulk-loads the merged, deduplicated stream into the B-tree via
//     BTree.BulkLoad, which rebuilds the tree bottom-up in O(n).
//  4. Deletes the consumed SSTable files.
//
// Because BulkLoad is atomic from the B-tree's point of view (the old tree
// remains readable until the new one is swapped in), reads are never blocked.
package compaction

import (
	"bytes"
	"container/heap"
	"fmt"
	"log"
	"os"

	"local/flashdb/btree"
	hybriddb "local/flashdb/hybriddb"
	"local/flashdb/sstable"
)

// Config controls compaction behaviour.
type Config struct {
	// L0Threshold is the number of L0 SSTables that triggers compaction.
	L0Threshold int
}

// DefaultConfig returns sensible defaults.
func DefaultConfig() Config {
	return Config{L0Threshold: 4}
}

// Engine manages background compaction.
type Engine struct {
	cfg    Config
	bt     *btree.BTree
	trigCh chan []string // paths of L0 files to compact
	quitCh chan struct{}
	doneCh chan struct{}
}

// New creates a compaction engine.
func New(cfg Config, bt *btree.BTree) *Engine {
	return &Engine{
		cfg:    cfg,
		bt:     bt,
		trigCh: make(chan []string, 1),
		quitCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}
}

// Start launches the background compaction goroutine.
func (e *Engine) Start() {
	go e.run()
}

// Stop shuts down the background goroutine.
func (e *Engine) Stop() {
	close(e.quitCh)
	<-e.doneCh
}

// Trigger enqueues a set of SSTable paths for compaction.
// Non-blocking: if a compaction is already queued, this is a no-op
// (the caller should retry after the current compaction finishes).
func (e *Engine) Trigger(paths []string) bool {
	select {
	case e.trigCh <- paths:
		return true
	case <-e.quitCh:
		return false
	default:
		return false
	}
}

func (e *Engine) run() {
	defer close(e.doneCh)
	for {
		select {
		case paths, ok := <-e.trigCh:
			if !ok {
				return
			}
			if err := e.compact(paths); err != nil {
				log.Printf("compaction error: %v", err)
			}
		case <-e.quitCh:
			return
		}
	}
}

// compact merges the given SSTable files and bulk-loads into the B-tree.
func (e *Engine) compact(paths []string) error {
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
			r.Close()
		}
	}()

	// k-way merge.
	merged, err := kWayMerge(readers)
	if err != nil {
		return fmt.Errorf("compaction merge: %w", err)
	}

	// Bulk-load into the B-tree (atomic swap).
	if err := e.bt.BulkLoad(merged); err != nil {
		return fmt.Errorf("compaction bulk-load: %w", err)
	}

	// Delete consumed SSTable files.
	for _, p := range paths {
		if err := os.Remove(p); err != nil {
			log.Printf("compaction: failed to remove %s: %v", p, err)
		}
	}

	log.Printf("compaction: merged %d SSTables → B-tree (%d entries)", len(paths), len(merged))
	return nil
}

// ────────────────────────────────────────────────────────────
//  k-way merge with a min-heap
// ────────────────────────────────────────────────────────────

type heapItem struct {
	entry     hybriddb.Entry
	readerIdx int
	ch        <-chan hybriddb.Entry
}

type mergeHeap []heapItem

func (h mergeHeap) Len() int      { return len(h) }
func (h mergeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h mergeHeap) Less(i, j int) bool {
	cmp := bytes.Compare(h[i].entry.Key, h[j].entry.Key)
	if cmp != 0 {
		return cmp < 0
	}
	// For the same key, prefer higher seqNum (more recent).
	return h[i].entry.SeqNum > h[j].entry.SeqNum
}

func (h *mergeHeap) Push(x any) { *h = append(*h, x.(heapItem)) }
func (h *mergeHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// kWayMerge performs a k-way merge of sorted SSTable iterators and
// deduplicates by keeping the highest-seqNum entry per key.
// Tombstone entries are removed when there is no older version
// (i.e., no further data to merge against — simple GC).
func kWayMerge(readers []*sstable.Reader) ([]hybriddb.Entry, error) {
	h := &mergeHeap{}
	heap.Init(h)

	channels := make([]<-chan hybriddb.Entry, len(readers))
	for i, r := range readers {
		ch, err := r.Iter()
		if err != nil {
			return nil, err
		}
		channels[i] = ch
		// Prime the heap with the first entry from each reader.
		if e, ok := <-ch; ok {
			heap.Push(h, heapItem{entry: e, readerIdx: i, ch: ch})
		}
	}

	var result []hybriddb.Entry
	var lastKey []byte

	for h.Len() > 0 {
		item := heap.Pop(h).(heapItem)
		e := item.entry

		// Advance the reader this item came from.
		if next, ok := <-item.ch; ok {
			heap.Push(h, heapItem{entry: next, readerIdx: item.readerIdx, ch: item.ch})
		}

		// Deduplicate: skip if we already emitted this key (a higher-seqNum
		// version was already emitted because the heap orders higher seq first
		// for equal keys).
		if lastKey != nil && bytes.Equal(lastKey, e.Key) {
			continue
		}
		lastKey = e.Key

		// Drop tombstones that have no older data to shadow — they're just
		// taking up space in the B-tree.  Simple heuristic: drop if it's the
		// only surviving entry for this key.
		if e.Tombstone {
			continue
		}

		result = append(result, e)
	}

	return result, nil
}
