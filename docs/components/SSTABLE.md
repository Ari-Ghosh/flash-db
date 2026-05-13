# SSTable (Sorted String Table)

SSTables are immutable, sorted files on disk that represent a "snapshot" of the MemTable at the time of flushing.

## File Format
An SSTable file consists of several sections:
1. **Data Blocks**: 4 KB compressed blocks containing sorted key-value pairs.
2. **Index Block**: Maps the first key of each data block to its offset and length.
3. **Bloom Filter**: A probabilistic data structure used to quickly check if a key *might* exist in the file.
4. **Footer**: A fixed-size (40-byte) trailer containing pointers to the index and bloom filter, as well as metadata like entry count and compression codec.

## Performance Optimization
- **Binary Search**: The index block allows for fast block location.
- **Bloom Filters**: Significantly reduce read amplification by skipping files that definitely do not contain the target key.
- **Compression**: Blocks are compressed using **Snappy** by default for L0 SSTables to balance storage savings with flush latency. The file footer identifies the codec, supporting seamless transitions between different algorithms.

## Life Cycle
1. **Flush**: Created when a MemTable is full.
2. **Read**: Used during point lookups and range scans.
3. **Compaction**: Merged with other SSTables and eventually consolidated into the B-tree.
4. **Cleanup**: Deleted once its contents have been successfully merged into a B-tree.
