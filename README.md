# FlashDB

A high-performance hybrid database engine combining **LSM (Log-Structured Merge) trees for writes** with **B-trees for reads**.

## Overview

FlashDB v2 implements a production-grade key-value store architecture that optimizes both write throughput and read latency with advanced features:

- **Write Path**: WAL (group-commit) → MemTable (skip list) → SSTable (L0 files) → Tiered Compaction (L0→L1→L2)
- **Read Path**: MemTable → L1 B-tree → L2 B-tree
- **MVCC Snapshots**: Point-in-time consistent reads
- **Range Iterators**: Forward and reverse scans over key ranges
- **Crash Recovery**: Write-Ahead Log (WAL) with replay on startup
- **Bloom Filters**: Fast negative lookups for absent keys
- **Compression**: Configurable block compression for SSTables

## Architecture

### Components

```
┌─────────────────────────────────────────────────────────┐
│                      Public API                         │
│         db.Put(k,v) | db.Get(k) | db.Delete(k)          │
│     db.NewSnapshot() | db.NewIterator(opts)            │
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
                                 │ (incremental, tiered)
                                 v
                    ┌────────────┴────────────┐
                    │                         │
                    v                         v
                 L1 B-tree                 L2 B-tree
            (Recent Compacted)        (Long-term Storage)
                    │                         │
                    └────────────┬────────────┘
                                 │
                                 v
                            Read Queries
                         (Merged Iterator)
```

### Write Path (LSM-style)

1. **WAL (Write-Ahead Log)**: Every write is immediately persisted as a log entry before touching in-memory structures (group-commit for throughput)
2. **MemTable**: Writes go into an in-memory skip list sorted by key
3. **Flush**: When MemTable reaches size threshold, it's rotated to immutable and flushed to disk as an SSTable (L0 file)
4. **Compaction**: Multiple L0 SSTables are incrementally merged into L1 B-tree; L1 promotes to L2 when size threshold exceeded

### Read Path (Tiered B-tree optimized)

1. **Active MemTable**: Check newest in-memory writes first (highest priority)
2. **Immutable MemTable**: Check data being flushed
3. **L1 B-tree**: Check recently compacted data via binary search through pages
4. **L2 B-tree**: Check long-term storage data via binary search through pages

### Key Features

- **Versioning**: Every entry has a monotonically increasing sequence number; highest version wins
- **Tombstones**: Deletions are marked with a tombstone flag; separate lifecycle from puts
- **MVCC Snapshots**: Point-in-time consistent reads via `db.NewSnapshot()`
- **Range Iterators**: Forward/reverse scans with `db.NewIterator(opts)`
- **Thread-Safe**: All components use appropriate synchronization (mutexes, atomic operations)
- **Crash Recovery**: WAL replay restores in-memory state on startup
- **Bloom Filters**: Negative lookups are fast (SSTable files include Bloom filters)
- **Tiered Compaction**: Incremental L0→L1→L2 merging avoids full rebuilds
- **Compression**: Configurable block compression for SSTables

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
- MVCC snapshots for point-in-time reads
- Range iterators (forward and reverse scans)
- Overwrites and tombstones
- Bulk writes to trigger flush and compaction
- Reading after compaction
- Engine stats (memtable size, L0 file count, sequence number)
- Crash recovery by closing and reopening the database

### Example Output

```
── Basic put / get ──────────────────────────────
user:alice  →  {"age":30,"city":"Pune"}  (err=<nil>)

── MVCC Snapshot ────────────────────────────────
user:alice via snapshot (age=30 expected) → {"age":30,"city":"Pune"} (err=<nil>)
user:alice latest (age=31 expected)       → {"age":31,"city":"Pune"}

── Delete / tombstone ───────────────────────────
user:bob  →  ""  (err=key deleted)

── Range scan (iterator) ────────────────────────
range [kv:aaa, kv:ddd) → kv:aaa=1 kv:bbb=2 kv:ccc=3 

── Reverse scan ─────────────────────────────────
reverse [kv:aaa, kv:eee) → kv:ddd=4 kv:ccc=3 kv:bbb=2 kv:aaa=1 

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

── Crash recovery via WAL ───────────────────────
user:carol (after reopen) → {"age":27,"city":"Delhi"} (err=<nil>)
user:bob   (after reopen) → "" (err=key deleted)
```

Done.

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
    Dir                string        // Directory for all database files
    MemTableSize       int64         // Flush threshold (default 64 MB)
    L0CompactThreshold int           // Trigger compaction after N L0 files (default 4)
    L1SizeThreshold    int64         // Promote L1 to L2 when size exceeded (default 256 MB)
    WALSyncPolicy      wal.SyncPolicy // WAL durability vs throughput (default SyncBatch)
    Codec              hybriddb.Codec // Block compression for SSTables (default None)
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
// Get latest value
val, err := db.Get([]byte("key"))

// Get value at specific sequence number
val, err := db.GetAt([]byte("key"), seqNum)
if err == hybriddb.ErrKeyNotFound {
    // Key does not exist
}
if err == hybriddb.ErrKeyDeleted {
    // Key was deleted (tombstone present)
}
// Use val
```

### MVCC Snapshots

```go
// Create a snapshot for point-in-time reads
snap := db.NewSnapshot()
defer snap.Release()

// Read from snapshot
val, err := db.GetSnapshot(snap, []byte("key"))
```

### Range Iterators

```go
// Forward scan
iter, err := db.NewIterator(hybriddb.IteratorOptions{
    LowerBound: []byte("prefix:"),
    UpperBound: []byte("prefix;"), // exclusive
})
for iter.Valid() {
    key := iter.Key()
    value := iter.Value()
    seq := iter.SeqNum()
    // process...
    iter.Next()
}
iter.Close()

// Reverse scan
iter, err := db.NewIterator(hybriddb.IteratorOptions{
    LowerBound: []byte("start"),
    UpperBound: []byte("end"),
    Reverse:    true,
})
// ... same as above
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
├── l1.db                # L1 B-tree (recently compacted data)
└── l2.db                # L2 B-tree (long-term storage)
```

### File Formats

**WAL (Write-Ahead Log)**
- Length-prefixed records with CRC32 checksums
- Each record: kind (put/delete), sequence number, key, value (for puts only)
- Group-commit batching for improved throughput

**SSTable (Sorted String Table)**
- Fixed-size 4 KB data blocks containing sorted entries
- Index block mapping first key to block offset
- Bloom filter for negative lookups
- Optional block compression (zstd, etc.)
- 32-byte footer with metadata

**B-tree (L1/L2)**
- Fixed 4 KB pages stored in separate files
- 512-byte header (magic, root ID, page count)
- Internal pages store separator keys and child pointers
- Leaf pages store actual key-value entries with sequence numbers

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

- **Trigger**: When L0 file count exceeds threshold (L0→L1) or L1 size exceeds threshold (L1→L2)
- **Algorithm**: Incremental k-way merge of sorted SSTable streams with deduplication by sequence number
- **Complexity**: O(n log k) where k = number of L0 files
- **Non-blocking**: Happens in background goroutine with atomic swaps

## Tuning

### For Write-Heavy Workloads

Increase `MemTableSize` to reduce flush frequency:
```go
cfg.MemTableSize = 256 * 1024 * 1024  // 256 MB
```

### For Read-Heavy Workloads

Decrease `L0CompactThreshold` to compact more aggressively:
```go
cfg.L0CompactThreshold = 2  // Compact after 2 L0 files
```

### For Large Datasets

Tune both compaction thresholds:
```go
cfg.MemTableSize = 512 * 1024 * 1024  // 512 MB
cfg.L0CompactThreshold = 8            // Compact after 8 L0 files
cfg.L1SizeThreshold = 1024 * 1024 * 1024  // 1 GB before L1→L2
```

### For High Throughput

Use group-commit WAL and compression:
```go
cfg.WALSyncPolicy = wal.SyncBatch  // Batch fsyncs
cfg.Codec = hybriddb.CodecZstd     // Compress SSTable blocks
```

## Concurrency

- **Writes**: Serialized via WAL group-commit and MemTable mutexes (FIFO ordering)
- **Reads**: Concurrent via RWMutex on MemTable and B-trees; snapshots provide isolation
- **Compaction**: Runs in dedicated background goroutine with atomic swaps (non-blocking)
- **Iterators**: Concurrent iteration over consistent views

## Limitations & Future Work

### Known Limitations

1. **No concurrent writes**: Single writer via MemTable mutex (future: write batching)
2. **No transactions**: Each operation is atomic but no multi-key ACID semantics
3. **Memory usage**: Compaction requires temporary space for merged data
4. **No prefix scans**: Range iterators support exact bounds but no prefix matching

### Future Enhancements

- [x] MVCC snapshots for point-in-time reads
- [x] Range iterator API (forward/reverse scans)
- [x] Tiered compaction (L0→L1→L2)
- [x] Configurable compression
- [ ] Parallel writes via WAL batching
- [ ] Multi-key transactions
- [ ] Prefix scan support
- [ ] Backup and restore
- [ ] Distributed replication
- [ ] Adaptive bloom filter sizing
- [ ] Column-family / namespace support
- [ ] Key TTL and time-to-live expiry
- [ ] Prometheus metrics exporter
- [ ] Block cache (ARC / CLOCK-Pro)
- [ ] Secondary indexes
- [ ] Structured query / filter pushdown
- [ ] Zstd / Snappy block compression
- [ ] Read-your-writes consistency guarantee
- [ ] Distributed query fan-out
- [ ] CLI and REPL tool
- [ ] Structured logging with slog
- [ ] Pluggable storage backend
- [ ] Write stall and backpressure
- [ ] Schema registry and value codec
- [ ] Compaction priority queue
- [ ] Chaos / fault-injection testing harness
- [ ] OpenTelemetry trace spans

## Testing

Run the example to verify functionality:

```bash
go run main.go
```

## License

MIT License

---

**Last Updated:** April 19, 2026
