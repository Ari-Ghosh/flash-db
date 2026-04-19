# FlashDB

A high-performance hybrid database engine combining **LSM (Log-Structured Merge) trees for writes** with **B-trees for reads**.

## Overview

FlashDB implements a production-grade key-value store architecture that optimizes both write throughput and read latency:

- **Write Path**: WAL → MemTable (skip list) → SSTable (L0 files) → Compaction
- **Read Path**: MemTable → B-tree (bulk-loaded from compacted data)
- **Crash Recovery**: Write-Ahead Log (WAL) with replay on startup
- **Bloom Filters**: Fast negative lookups for absent keys

## Architecture

### Components

```
┌─────────────────────────────────────────────────────────┐
│                      Public API                         │
│              db.Put(k,v) | db.Get(k)                    │
│              db.Delete(k) | db.Close()                  │
└────────────────┬────────────────────────────────────────┘
                 │
    ┌────────────┼────────────┐
    │            │            │
    v            v            v
  WAL       MemTable      Immutable
  Log      (Skip List)    MemTable
 (Write)                 (Flushing)
    │            │            │
    │            └─────┬──────┘
    │                  │ (flush when full)
    └──────────────────┼─────────────────┐
                       │                 │
                       v                 v
                   SSTable L0       Compaction
                   (Sorted)         Engine
                       │                 │
                       └─────────┬───────┘
                                 │ (k-way merge)
                                 v
                            B-Tree Pages
                         (Page-based store)
                                 │
                                 v
                            Read Queries
```

### Write Path (LSM-style)

1. **WAL (Write-Ahead Log)**: Every write is immediately persisted as a log entry before touching in-memory structures
2. **MemTable**: Writes go into an in-memory skip list sorted by key
3. **Flush**: When MemTable reaches size threshold, it's rotated to immutable and flushed to disk as an SSTable (L0 file)
4. **Compaction**: Multiple L0 SSTables are k-way merged into the B-tree via bulk-load

### Read Path (B-tree optimized)

1. **Active MemTable**: Check newest in-memory writes first (highest priority)
2. **Immutable MemTable**: Check data being flushed
3. **B-tree**: Check durable, compacted data via binary search through pages

### Key Features

- **Versioning**: Every entry has a monotonically increasing sequence number; highest version wins
- **Tombstones**: Deletions are marked with a tombstone flag; separate lifecycle from puts
- **Thread-Safe**: All components use appropriate synchronization (mutexes, atomic operations)
- **Crash Recovery**: WAL replay restores in-memory state on startup
- **Bloom Filters**: Negative lookups are fast (SSTable files include Bloom filters)

## Building & Running

### Prerequisites

- Go 1.22.2 or later

### Build

```bash
cd /Users/arijitghosh/code/flash-db
go build -o flashdb ./...
```

### Run Example

```bash
go run main.go
```

The `main.go` demonstrates:
- Basic put/get/delete operations
- Overwrites and tombstones
- Bulk writes to trigger flush and compaction
- Reading after compaction
- Engine stats (memtable size, L0 file count, sequence number)
- Crash recovery by closing and reopening the database

### Example Output

```
── Basic put / get ──────────────────────────────
user:alice  →  {"age":30,"city":"Pune"}  (err=<nil>)

── Overwrite ────────────────────────────────────
user:alice  →  {"age":31,"city":"Pune"}  (err=<nil>)

── Delete / tombstone ───────────────────────────
user:bob    →  ""  (err=key deleted)

── Bulk writes to trigger flush + compaction ────

── Read after compaction ────────────────────────
  kv:00000000  →  "value-0"  match=true
  kv:00000042  →  "value-1764"  match=true
  kv:00000999  →  "value-998001"  match=true
  kv:04999  →  "value-24990001"  match=true

── Engine stats ─────────────────────────────────
  MemTable entries : 5000  (5242880 bytes)
  L0 file count    : 0
  Sequence number  : 5003

── Reopen (crash recovery via WAL) ──────────────
user:carol (after reopen)  →  {"age":27,"city":"Delhi"}  (err=<nil>)
user:bob   (after reopen)  →  ""  (err=key deleted, should be ErrKeyDeleted)

Done.
```

## API Reference

### Opening a Database

```go
import "local/flashdb/engine"

cfg := engine.DefaultConfig("/tmp/my_db")
db, err := engine.Open(cfg)
defer db.Close()
```

**Config Options:**

```go
type Config struct {
    Dir                string  // Directory for all database files
    MemTableSize       int64   // Flush threshold (default 64 MB)
    L0CompactThreshold int     // Trigger compaction after N L0 files (default 4)
}
```

### Writing Data

```go
// Put a key-value pair
err := db.Put([]byte("key"), []byte("value"))

// Delete a key (marked with tombstone)
err := db.Delete([]byte("key"))
```

### Reading Data

```go
val, err := db.Get([]byte("key"))
if err == hybriddb.ErrKeyNotFound {
    // Key does not exist
}
if err == hybriddb.ErrKeyDeleted {
    // Key was deleted (tombstone present)
}
// Use val
```

### Monitoring

```go
stats := db.Stats()
fmt.Printf("MemTable entries: %d\n", stats.MemTableCount)
fmt.Printf("MemTable size: %d bytes\n", stats.MemTableSize)
fmt.Printf("L0 file count: %d\n", stats.L0FileCount)
fmt.Printf("Sequence number: %d\n", stats.SeqNum)
```

## Storage Format

### Directory Layout

```
/path/to/db/
├── wal.log              # Current write-ahead log
├── wal_<seqNum>.log     # Archived WALs (after compaction)
├── l0_<seqNum>.sst      # Level-0 SSTable files (unsorted, will be compacted)
└── btree.db             # B-tree storage (page-based)
```

### File Formats

**WAL (Write-Ahead Log)**
- Length-prefixed records with CRC32 checksums
- Each record: kind (put/delete), sequence number, key, value (for puts only)

**SSTable (Sorted String Table)**
- Fixed-size 4 KB data blocks containing sorted entries
- Index block mapping first key to block offset
- Bloom filter for negative lookups
- 32-byte footer with metadata

**B-tree**
- Fixed 4 KB pages stored in a single file
- 512-byte header (magic, root ID, page count)
- Internal pages store separator keys and child pointers
- Leaf pages store actual key-value entries

## Performance Characteristics

### Write Performance

- **O(1) amortized**: MemTable insertions in skip list
- **Batching**: WAL batches multiple entries per sync (configurable)
- **Flush overhead**: Background task, does not block writes

### Read Performance

- **MemTable lookups**: O(log n) skip list traversals
- **B-tree lookups**: O(log n) page traversals + page I/O
- **Bloom filter**: Fast negative lookups (avoids B-tree traversal for absent keys)

### Compaction

- **Trigger**: When L0 file count exceeds threshold
- **Algorithm**: k-way merge of sorted SSTable streams with deduplication
- **Complexity**: O(n log k) where k = number of L0 files
- **Non-blocking**: Happens in background goroutine

## Tuning

### For Write-Heavy Workloads

Increase `MemTableSize` to reduce flush frequency:
```go
cfg.MemTableSize = 256 * 1024 * 1024  // 256 MB
```

### For Read-Heavy Workloads

Decrease `L0CompactThreshold` to keep more data in the B-tree:
```go
cfg.L0CompactThreshold = 2  // Compact more aggressively
```

### For Large Datasets

Both parameters can be tuned together:
```go
cfg.MemTableSize = 512 * 1024 * 1024  // 512 MB
cfg.L0CompactThreshold = 8            // Compact after 8 L0 files
```

## Concurrency

- **Writes**: Serialized via WAL and MemTable mutexes (FIFO ordering)
- **Reads**: Concurrent via RWMutex on MemTable and B-tree
- **Compaction**: Runs in dedicated background goroutine (non-blocking)

## Limitations & Future Work

### Known Limitations

1. **Single-threaded writes**: Only one write goroutine can proceed at a time
2. **Full B-tree rebuilds**: Compaction rebuilds the entire tree (suitable for moderate-sized datasets)
3. **No range queries**: Only point lookups supported (no prefix scans)
4. **No transactions**: Each operation is atomic but no multi-key ACID semantics

### Future Enhancements

- [ ] Parallel writes via write-ahead log batching
- [ ] Incremental compaction (avoid full rebuilds)
- [ ] Range scan support (iterator API)
- [ ] Configurable compression (on SSTables)
- [ ] Tiered storage (L1, L2 levels in B-tree)
- [ ] MVCC for snapshot isolation

## Testing

Run the example to verify functionality:

```bash
go run main.go
```

## License

MIT License

---

**Last Updated:** April 19, 2026
