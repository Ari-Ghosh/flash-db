# LSM Tree & Compaction

FlashDB uses a Log-Structured Merge (LSM) approach for handling writes. This design optimizes for sequential I/O, which is significantly faster than random I/O on both SSDs and HDDs.

## The Write Path
1. **WAL**: The mutation is first appended to the Write-Ahead Log.
2. **MemTable**: The mutation is then inserted into the Skip List MemTable.
3. **Acknowledgment**: Once both are complete, the write is considered successful.

## Flushing
When the MemTable reaches its size limit (default 64 MB), it is rotated:
- A new MemTable becomes the active target for writes.
- The old MemTable becomes "immutable" and is flushed to disk as an **SSTable** (Sorted String Table).
- SSTables are "Level 0" files.

## Compaction (The Consolidation Phase)
Compaction is the process of merging multiple L0 SSTables into the B-tree storage.

### Why Compact?
- **Reduces Read Amplification**: Merging many small files into one large index reduces the number of places a reader must look.
- **Garbage Collection**: Stale versions of keys (overwrites) and deleted keys (tombstones) are removed during the merge.
- **Space Reclamation**: Expired TTL keys are purged.

### Tiered Compaction
FlashDB uses a tiered approach:
- **L0 → L1**: Small, frequent compactions that merge SSTables into the first-level B-tree.
- **L1 → L2**: Less frequent, larger compactions that promote data from L1 to the final L2 storage.

## k-Way Merge Algorithm
Compaction uses a min-heap to merge multiple sorted streams (SSTables and B-trees) into a single sorted output stream.
- **Heap Order**: `(key ASC, seqNum DESC)`. This ensures that for any key, the most recent version (highest sequence number) is processed first.
- **Deduplication**: Once the most recent version of a key is yielded, all subsequent versions with the same key are discarded.
