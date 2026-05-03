# Compaction Engine

Compaction is the background process that manages the data lifecycle, reclaims space, and optimizes read performance.

## LSM Compaction
FlashDB uses a tiered compaction strategy:
1. **L0 → L1**: Triggered when the number of SSTables reaches a threshold. Multiple SSTables are merged into the `L1` B-tree.
2. **L1 → L2**: Triggered when the `L1` B-tree exceeds a certain size. Data is promoted to the larger, long-term `L2` B-tree.

## K-Way Merge
The core of the compaction process is a **k-way heap merge**:
- Reads sorted entries from multiple SSTables (and potentially an existing B-tree).
- Uses a min-priority queue to yield entries in sorted order.
- Deduplicates entries by keeping only the version with the highest sequence number.
- Performs garbage collection of expired TTL keys and "stale" tombstones.

## Atomicity
Compaction is designed to be non-blocking for readers:
- A new B-tree is built in a temporary file using the Bulk Load algorithm.
- Once complete, the old B-tree is swapped for the new one atomically.
- Readers continue to see the old version until the swap is finished.
