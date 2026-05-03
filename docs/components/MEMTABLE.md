# MemTable

The MemTable is the first point of entry for all write operations in FlashDB. It provides a fast, in-memory buffer that stores mutations before they are eventually flushed to disk.

## Design
- **Data Structure**: Skip List.
- **Ordered**: Entries are kept in sorted order by key, enabling efficient range scans.
- **Concurrency**: Thread-safe via `sync.RWMutex`.
- **MVCC Support**: Each entry includes a sequence number.

## Key Features
- **O(log n) Operations**: Insertion, lookup, and deletion all take logarithmic time.
- **Tombstones**: Deletions are represented as special "tombstone" entries to support the LSM architecture.
- **Flush Threshold**: When the MemTable reaches a configurable size (e.g., 64 MB), it is marked as immutable and a new active MemTable is created.

## Implementation Details
The skip list uses a probabilistic approach to balancing:
- `maxLevel`: 16
- `probability`: 0.25 (1/4 chance of promotion)

## API
- `Put(key, value []byte, seq uint64)`: Insert or update a key.
- `Get(key []byte)`: Retrieve the latest version of a key.
- `Delete(key []byte, seq uint64)`: Mark a key as deleted.
- `NewIterator(opts)`: Scan a range of keys.
