# Compaction Engine

Compaction is the background process that manages the data lifecycle, reclaims space, and optimizes read performance.

## LSM Compaction
FlashDB uses a tiered compaction strategy:
1. **L0 → L1**: Triggered when the number of SSTables reaches a threshold. Multiple SSTables are merged into the `L1` B-tree.
2. **L1 → L2**: Triggered when the `L1` B-tree exceeds a certain size. Data is promoted to the larger, long-term `L2` B-tree.

## Streaming K-Way Merge
Compaction uses a **streaming k-way heap merge** to avoid materializing the full dataset in memory:

- Multiple SSTable readers and the existing L1 B-tree are opened as channel-based streams.
- A min-priority queue merges entries from all streams, yielding results one at a time.
- Deduplication and tombstone GC are applied per-key as entries arrive.
- The B-tree's `BulkLoadFromIter` method consumes the stream, building leaf pages incrementally and flushing them to disk as they fill.

This replaces the previous approach that buffered all entries into `[]types.Entry` slices, which consumed 2-3x the dataset size in peak memory. Peak memory is now O(k) where k is the number of source streams.

## Atomicity
Compaction is designed to be non-blocking for readers:
- A new B-tree is built in a temporary file using the Bulk Load algorithm.
- Once complete, the old B-tree is swapped for the new one atomically.
- Readers continue to see the old version until the swap is finished.

## B-Tree Streaming API
- `BTree.StreamEntries()` — yields entries via channel without buffering, used by compaction to read L1.
- `BTree.BulkLoadFromIter(iter)` — consumes a sorted iterator, builds pages incrementally, flushes immediately.
