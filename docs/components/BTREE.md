# B-Tree Storage

The B-tree serves as the long-term, read-optimized storage layer in FlashDB.

## Design
- **Page-Based**: Data is organized into fixed 4 KB pages.
- **B+ Tree**: Internal nodes store only keys and child pointers; leaf nodes store actual data.
- **Tiered**: FlashDB uses two levels of B-trees (`L1` and `L2`) to manage data age and compaction frequency.

## Page Types
1. **Leaf Page**: Stores `(key, value, seqNum, tombstone)` entries.
2. **Internal Page**: Stores separator keys and `uint64` child page IDs.

## Compression
FlashDB's B-tree implementation supports **per-page compression**. When enabled, the data portion of each page (after the 8-byte header) is compressed before writing to disk.
- **L1 B-tree**: Uses **Snappy** for high performance on recently compacted data.
- **L2 B-tree**: Uses **Zstd** for maximum density in long-term storage.

Compression is handled transparently during page I/O, and the codec used is identified by a flag in the page header.

## Page Cache (ARC)
The B-tree implementation includes an **Adaptive Replacement Cache (ARC)** to keep frequently and recently accessed pages in memory, minimizing disk I/O.

## Bulk Load
Instead of traditional random inserts, FlashDB uses a **Bulk Load** algorithm during compaction. It builds a completely new B-tree from a sorted stream of entries, ensuring 100% page fill factor and sequential disk I/O.

## API
- `Open(path)`: Open or create a B-tree file.
- `Get(key)`: Traverse the tree to find a key.
- `BulkLoad(entries)`: Replace the tree contents with new sorted data.
- `NewIterator(opts)`: Range scans over the B-tree.
