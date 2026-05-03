# FlashDB

A high-performance hybrid database engine combining **LSM (Log-Structured Merge) trees for writes** with **B-trees for reads**.

## Overview

FlashDB v4 builds on the production-grade v3 foundation (transactions, replication, backup/restore) with five new capabilities: atomic multi-key write batches, column-family namespaces, per-key TTL expiry, an ARC block cache, and secondary indexes.

- **Write Path**: WAL (group-commit) → MemTable (skip list) → SSTable (L0 files) → Tiered Compaction (L0→L1→L2)
- **Read Path**: MemTable → L0 SSTables (bloom-filtered) → L1 B-tree (ARC-cached) → L2 B-tree (ARC-cached)
- **MVCC Snapshots**: Point-in-time consistent reads
- **Range Iterators**: Forward and reverse scans over key ranges
- **Multi-Key Transactions**: Optimistic concurrency control with conflict detection
- **WriteBatch**: Atomic multi-key writes without transaction overhead
- **Prefix Scans**: Efficient range queries over key prefixes
- **Column Families**: Isolated key namespaces within a single DB instance
- **Key TTL**: Per-key time-to-live with lazy expiry and background reaping
- **ARC Block Cache**: Adaptive Replacement Cache for B-tree pages (replaces unbounded map)
- **Secondary Indexes**: User-defined indexes with point and range queries
- **Hot Backup & Restore**: Point-in-time backups with SHA-256 integrity verification
- **Distributed Replication**: Single-leader WAL shipping to read-only followers
- **Crash Recovery**: Write-Ahead Log (WAL) with replay on startup
- **Adaptive Bloom Filters**: Per-SSTable FPR telemetry drives automatic filter sizing

## Architecture

### Components

```
┌──────────────────────────────────────────────────────────────────┐
│                           Public API                             │
│  db.Put(k,v)  db.Get(k)  db.Delete(k)  db.NewWriteBatch()       │
│  db.NewSnapshot()  db.NewIterator(opts)  db.PrefixScan(prefix)   │
│  db.Begin()  db.Backup(dest)  db.Restore(src, dest)             │
│  db.CreateColumnFamily(name)  db.GetColumnFamily(name)           │
│  db.PutWithTTL(k,v,ttl)  db.DefineIndex(def)  db.PutIndexed(k,v)│
└──────────────────────────┬───────────────────────────────────────┘
                           │
           ┌───────────────┼───────────────┐
           │               │               │
           v               v               v
         WAL           MemTable        Immutable
         Log          (Skip List)      MemTable
       (Write)                        (Flushing)
           │               │               │
           │               └───────┬───────┘
           │                       │ flush when full
           └───────────────────────┼─────────────────┐
                                   │                 │
                                   v                 v
                               SSTable L0        Compaction
                               (Sorted)           Engine
                                   │                 │
                                   └────────┬────────┘
                                            │ incremental, tiered
                                            v
                               ┌────────────┴────────────┐
                               │                         │
                               v                         v
                           L1 B-tree                 L2 B-tree
                        (ARC-cached pages)        (ARC-cached pages)
                               │                         │
                               └────────────┬────────────┘
                                            │
                                            v
                                      Read Queries
                                   (Merged Iterator)
```

### Replication Architecture

```
Leader Node                         Follower Node
┌─────────────────────┐            ┌─────────────────────┐
│   Engine.DB         │            │   Engine.DB         │
│   ┌─────────────┐   │            │   ┌─────────────┐   │
│   │   WAL       │   │            │   │   WAL       │   │
│   │   append    ├───┼────────────┼──►│   apply     │   │
│   └─────────────┘   │            │   └─────────────┘   │
│   Replication Ring  │            │   Replication       │
│   Buffer (64k slots)│            │   Applier           │
└─────────────────────┘            └─────────────────────┘
          │                                   │
          v                                   v
     TCP Stream                        Read-Only DB
  (HMAC authenticated)               (auto reconnect)
```

### Write Path (LSM-style)

1. **WAL (Write-Ahead Log)**: Every write is immediately persisted as a log entry before touching in-memory structures. Group-commit amortizes fsync cost across concurrent writers.
2. **MemTable**: Writes go into an in-memory skip list sorted by key.
3. **WriteBatch**: Multiple puts/deletes can be committed in a single WAL batch + single fsync without full transaction overhead.
4. **Flush**: When the MemTable reaches its size threshold it rotates to immutable and is flushed to disk as an SSTable (L0 file).
5. **Compaction**: Multiple L0 SSTables are incrementally merged into the L1 B-tree via a k-way heap merge; L1 promotes to L2 when its size threshold is exceeded.

### Read Path (Tiered B-tree with ARC cache)

1. **Active MemTable**: Check newest in-memory writes first (highest priority).
2. **Immutable MemTable**: Check data currently being flushed.
3. **L0 SSTables**: Bloom-filtered point lookups; false positives fed back into adaptive FPR telemetry.
4. **L1 B-tree**: Recently compacted data; pages served from ARC cache when hot.
5. **L2 B-tree**: Long-term storage; pages served from ARC cache when hot.

### Key Features

| Feature | Description |
|---|---|
| **MVCC Snapshots** | Point-in-time consistent reads via `db.NewSnapshot()` |
| **Range Iterators** | Forward/reverse scans with `db.NewIterator(opts)` |
| **WriteBatch** | Atomic multi-key writes via `db.NewWriteBatch()` |
| **Multi-Key Transactions** | Optimistic concurrency control with `db.Begin()` / `Commit()` |
| **Prefix Scans** | Efficient prefix-based range queries with `db.PrefixScan(prefix)` |
| **Column Families** | Isolated namespaces with `db.CreateColumnFamily(name)` |
| **Key TTL** | Per-key expiry with `db.PutWithTTL(k, v, duration)` |
| **ARC Block Cache** | Self-tuning page cache replaces the old unbounded map |
| **Secondary Indexes** | User-defined indexes with `db.DefineIndex()` / `db.QueryByIndex()` |
| **Hot Backup** | Non-blocking backup via `db.Backup(destDir)` |
| **Distributed Replication** | Single-leader WAL shipping with HMAC auth |
| **Crash Recovery** | WAL replay restores in-memory state on startup |
| **Adaptive Bloom Filters** | Per-SSTable FPR telemetry drives automatic filter sizing |

## Documentation

FlashDB comes with comprehensive documentation covering architecture, component details, and user guides.

- [**Full Documentation Index**](docs/DOCUMENTATION.md)
- [**Architecture Overview**](docs/architecture/OVERVIEW.md)
- [**Getting Started Guide**](docs/user-guide/GETTING_STARTED.md)
- [**API Reference**](docs/user-guide/API_REFERENCE.md)
- [**Architecture Decision Records (ADRs)**](docs/adr/)

## Building & Running

### Prerequisites

- Go 1.21 or later

### Build

```bash
go build ./...
```

### Run Example

```bash
go run main.go
```

`main.go` demonstrates parallel writes, multi-key transactions, prefix scans, backup/restore, and replication.

### Run Tests

```bash
go test ./src/tests/ -v -timeout=120s
```

The test suite covers all components including the five new v4 features with 95 tests total.

## API Reference

### Opening a Database

```go
import "local/flashdb/src/engine"

cfg := engine.DefaultConfig("/tmp/my_db")
db, err := engine.Open(cfg)
defer db.Close()
```

**Config Options:**

```go
type Config struct {
    Dir                string             // Directory for all database files
    MemTableSize       int64              // Flush threshold (default 64 MB)
    L0CompactThreshold int                // Trigger compaction after N L0 files (default 4)
    L1SizeThreshold    int64              // Promote L1→L2 when size exceeded (default 256 MB)
    WALSyncPolicy      wal.SyncPolicy     // Durability vs throughput (default SyncBatch)
    Codec              types.Codec        // Block compression for SSTables (default None)
    BloomFPRTarget     float64            // Baseline bloom FPR (default 0.01)
    BloomFPRMin        float64            // Minimum adaptive FPR (default 0.001)
    BloomFPRMax        float64            // Maximum adaptive FPR (default 0.05)
    Replication        *replication.Config // Optional replication configuration
}
```

### Writing Data

```go
// Single key-value write
err := db.Put([]byte("key"), []byte("value"))

// Write with TTL
err := db.PutWithTTL([]byte("session:abc"), []byte("data"), 30*time.Minute)

// Set absolute expiry on an existing key
err := db.ExpireAt([]byte("key"), time.Now().Add(1*time.Hour))

// Delete (marks with tombstone)
err := db.Delete([]byte("key"))
```

### Atomic Multi-Key Writes (WriteBatch)

```go
wb := db.NewWriteBatch()
wb.Put([]byte("counter:a"), []byte("1"))
wb.Put([]byte("counter:b"), []byte("2"))
wb.Delete([]byte("counter:old"))
if err := wb.Commit(); err != nil {
    // handle error
}
```

### Reading Data

```go
// Get latest value
val, err := db.Get([]byte("key"))

// Get value at a specific sequence number (MVCC)
val, err := db.GetAt([]byte("key"), seqNum)

// Check remaining TTL
remaining, err := db.TTLOf([]byte("session:abc"))

if errors.Is(err, types.ErrKeyNotFound) { /* key absent or expired */ }
if errors.Is(err, types.ErrKeyDeleted)  { /* key was deleted */       }
```

### Multi-Key Transactions

```go
tx := db.Begin()
defer tx.Rollback() // no-op if already committed

aliceBal, _ := tx.Get([]byte("alice"))
tx.Put([]byte("alice"), []byte("900"))
tx.Put([]byte("bob"),   []byte("600"))

if err := tx.Commit(); err != nil {
    if errors.Is(err, txn.ErrTxnConflict) {
        // concurrent write detected — retry
    }
}
```

### Column Families

```go
// Create a namespace (idempotent)
if err := db.CreateColumnFamily("users"); err != nil { ... }

// Get a handle
cf, err := db.GetColumnFamily("users")

// Standard CRUD within the namespace
cf.Put([]byte("alice"), []byte("alice@example.com"))
val, err := cf.Get([]byte("alice"))
cf.Delete([]byte("alice"))

// TTL inside a column family
cf.PutWithTTL([]byte("session:xyz"), []byte("token"), 15*time.Minute)

// Iterate within the namespace
iter, err := cf.NewIterator(types.IteratorOptions{})
for iter.Valid() {
    fmt.Printf("%s = %s\n", iter.Key(), iter.Value())
    iter.Next()
}
iter.Close()

// List all column families
names := db.ListColumnFamilies()

// Drop a column family and all its keys
err = db.DropColumnFamily("users")
```

### Secondary Indexes

```go
// Define an index (in-memory; call after every Open)
err := db.DefineIndex(engine.IndexDefinition{
    Name: "by_email",
    KeyFn: func(primaryKey, value []byte) [][]byte {
        // return the index key(s) derived from this record
        return [][]byte{extractEmail(value)}
    },
})

// Indexed writes maintain the index automatically
err = db.PutIndexed([]byte("user:1"), []byte(`{"email":"a@x.com"}`))
err = db.DeleteIndexed([]byte("user:1"))

// Point lookup on the index
primaryKeys, err := db.QueryByIndex("by_email", []byte("a@x.com"))

// Range query on the index
primaryKeys, err := db.RangeQueryByIndex("by_email",
    []byte("a@x.com"), []byte("m@x.com"))

// Backfill index over existing data
err = db.RebuildIndex("by_email")

// Remove an index and all its stored entries
err = db.DropIndex("by_email")
```

### MVCC Snapshots

```go
snap := db.NewSnapshot()
defer snap.Release()

val, err := db.GetSnapshot(snap, []byte("key"))

// Snapshot-isolated range scan
iter, err := db.NewIterator(types.IteratorOptions{
    SnapshotSeq: snap.Seq(),
})
```

### Range Iterators & Prefix Scans

```go
// Bounded forward scan
iter, err := db.NewIterator(types.IteratorOptions{
    LowerBound: []byte("user:"),
    UpperBound: []byte("user;"), // exclusive
})

// Prefix scan (convenience wrapper)
iter, err := db.PrefixScan([]byte("user:"))

// Reverse prefix scan
iter, err := db.NewIterator(types.IteratorOptions{
    Prefix:  []byte("user:"),
    Reverse: true,
})

for iter.Valid() {
    fmt.Printf("%s = %s\n", iter.Key(), iter.Value())
    iter.Next()
}
iter.Close()
```

### Monitoring

```go
// Engine metrics
stats := db.Stats()
fmt.Printf("MemTable entries : %d\n", stats.MemTableCount)
fmt.Printf("MemTable size    : %d bytes\n", stats.MemTableSize)
fmt.Printf("L0 file count    : %d\n", stats.L0FileCount)
fmt.Printf("Sequence number  : %d\n", stats.SeqNum)

// Bloom filter telemetry
bstats := db.BloomStats()
fmt.Printf("Bloom queries     : %d\n", bstats.TotalQueries)
fmt.Printf("False positives   : %d\n", bstats.TotalFalsePositives)
fmt.Printf("Observed FPR      : %.4f\n", bstats.ObservedFPR)
fmt.Printf("Next SSTable FPR  : %.4f\n", bstats.CurrentTargetFPR)
```

### Backup and Restore

```go
// Hot backup (non-blocking, flushes MemTable first)
manifest, err := db.Backup("/path/to/backup")
fmt.Printf("Backed up %d files at seq %d\n", len(manifest.Files), manifest.SnapSeq)

// Restore to an empty directory
err = backup.Restore("/path/to/backup", "/path/to/new_db")
```

### Replication

```go
// Leader
cfg := engine.DefaultConfig(leaderDir)
cfg.Replication = &replication.Config{
    Role:       "leader",
    ListenAddr: ":5432",
    Secret:     []byte("my-32-byte-shared-secret!!!!!!!!"),
}
leaderDB, err := engine.Open(cfg)

// Follower
cfg := engine.DefaultConfig(followerDir)
cfg.Replication = &replication.Config{
    Role:              "follower",
    LeaderAddr:        "leader-host:5432",
    Secret:            []byte("my-32-byte-shared-secret!!!!!!!!"),
    DialTimeout:       5 * time.Second,
    ReconnectInterval: 2 * time.Second,
}
followerDB, err := engine.Open(cfg)
```

## Storage Format

### Directory Layout

```
/path/to/db/
├── wal.log                # Active write-ahead log
├── wal_<seqNum>.log       # Archived WALs (rotated after compaction)
├── l0_<seqNum>.sst        # Level-0 SSTable files (pending compaction)
├── btree_l1.db            # L1 B-tree (recently compacted data)
└── btree_l2.db            # L2 B-tree (long-term storage)
```

Column-family names, TTL metadata, and secondary index entries are stored as regular DB key-value pairs under reserved `\x00`-prefixed keys and persist through normal WAL replay.

### File Formats

**WAL (Write-Ahead Log)**
- Length-prefixed records with CRC32 checksums
- Record kinds: Put, Delete, TxnBegin, TxnCommit, TxnAbort
- Transaction records filtered to only committed batches during replay
- Group-commit batching; pluggable sync policy (SyncAlways / SyncBatch / SyncNone)

**SSTable (Sorted String Table)**
- Fixed-size 4 KB data blocks with optional compression
- Index block mapping each block's first key to its byte offset
- Bloom filter for fast negative lookups (adaptive FPR)
- 40-byte footer: index offset/len, bloom offset/len, entry count, codec, magic

**B-tree (L1/L2)**
- Fixed 4 KB pages in a single file per tier
- 512-byte file header: magic, root page ID, page count
- Internal pages: separator keys + child page pointers (up to 50 children)
- Leaf pages: key/value entries with sequence numbers and tombstone flags
- Page reads/writes go through an ARC cache (default 2 048 pages = 8 MB)

### Internal Key Namespaces

FlashDB reserves keys beginning with `\x00` for internal use:

| Prefix | Purpose |
|---|---|
| `\x00cf\x00<name>\x00` | Column-family key namespace |
| `\x00ttl\x00<key>` | TTL deadline metadata (8-byte big-endian int64 nanoseconds) |
| `\x00idx\x00<name>\x00<idxKey>\x00<pk>` | Secondary index entry |
| `\x00cf_meta` | Persisted list of column-family names |

## Performance Characteristics

### Write Performance

- **O(1) amortized**: Skip-list MemTable insertions
- **WAL batching**: Multiple concurrent writers share one fsync per drain cycle (SyncBatch)
- **WriteBatch**: N writes in a single fsync — ideal for metadata updates, index maintenance
- **Flush overhead**: Background goroutine; does not block the writer

### Read Performance

- **MemTable hit**: ~300 ns (O(log n) skip-list traversal, all in memory)
- **B-tree hit (ARC warm)**: ~500 ns (page served from ARC cache, no disk I/O)
- **B-tree hit (ARC cold)**: ~2–6 ms (page read from disk, then cached)
- **Bloom negative**: ~600 ns (early exit, no disk I/O)
- **TTL expiry check**: ~O(1) metadata lookup; lazy deletion does not block the caller

### ARC Cache

The ARC (Adaptive Replacement Cache) page cache in the B-tree self-tunes between two eviction strategies:

- **T1/B1 (recency list)**: Tracks pages seen once recently
- **T2/B2 (frequency list)**: Tracks pages seen more than once

Ghost entries in B1/B2 allow the cache to detect whether recent evictions were wrong and adjust the recency/frequency balance automatically. This outperforms any fixed LRU or LFU policy on mixed workloads.

Default capacity: **2 048 pages = 8 MB**. Configurable via `btree.OpenWithCacheSize(path, n)`.

### Compaction

- **Trigger**: L0 file count ≥ threshold (L0→L1); L1 byte size ≥ threshold (L1→L2)
- **Algorithm**: Incremental k-way heap merge — O(n log k) where k = L0 file count
- **Non-blocking**: Runs in a background goroutine; old B-tree is readable during rebuild
- **Tombstone GC**: Tombstones dropped only when no live snapshot can observe them

## Tuning

### For Write-Heavy Workloads

```go
cfg.MemTableSize    = 256 * 1024 * 1024  // 256 MB — fewer flushes
cfg.WALSyncPolicy   = wal.SyncBatch      // amortize fsyncs (default)
```

### For Read-Heavy Workloads

```go
cfg.L0CompactThreshold = 2               // compact aggressively to keep L0 small
// Use larger ARC cache at the btree level (OpenWithCacheSize)
```

### For Large Datasets

```go
cfg.MemTableSize        = 512 * 1024 * 1024       // 512 MB
cfg.L0CompactThreshold  = 8
cfg.L1SizeThreshold     = 1024 * 1024 * 1024      // 1 GB before L1→L2
```

### For High Throughput

```go
cfg.WALSyncPolicy = wal.SyncBatch    // batch fsyncs (default)
cfg.Codec         = types.CodecZstd  // compress SSTable blocks
```

### Bloom Filter Tuning

```go
cfg.BloomFPRTarget = 0.01   // 1% baseline FPR
cfg.BloomFPRMin    = 0.001  // never go below 0.1% (larger filter)
cfg.BloomFPRMax    = 0.05   // never go above 5%  (smaller filter)
// The engine adapts the FPR automatically based on observed false-positive rates
```

## Concurrency

- **Writes**: WAL group-commit serializes concurrent writers; single MemTable mutex orders in-memory updates
- **WriteBatch / Transactions**: Both take `writeMu` once for the entire batch — one lock acquisition regardless of operation count
- **Reads**: Fully concurrent via RWMutex on MemTable and B-trees; snapshots provide MVCC isolation
- **TTL reaper**: Runs as a separate goroutine every 30 s; uses the standard write path so it is fully concurrent-safe
- **ARC cache**: Per-cache mutex protects the four internal lists; the hot path (Get) holds the lock for only pointer operations
- **Compaction**: Background goroutine with atomic B-tree swaps — reads are never blocked

## Limitations & Future Work

### Current Limitations

1. **Single-writer MemTable**: Sequential via mutex (WriteBatch and Txn both batch within the critical section, limiting contention)
2. **Compaction memory**: Full k-way merge result is held in memory before BulkLoad; streaming compaction would reduce peak usage
3. **Index definitions are in-memory**: `DefineIndex` must be called after every `Open`; no automatic persistence of the `KeyFn`
4. **Manual leader failover**: Replication does not include automatic leader election

### Future Enhancements

**Phase 1 (Initial Setup):**
- [x] WAL Implementation
- [x] SS Table Implementation
- [x] Bloom Filter Implementation
- [x] Btree Implementation
- [x] Memtable Implementation
- [x] Compaction Flush(LSM-Tree -> Btree) Implementation

**Phase 2 (Initial Setup):**
- [x] MVCC snapshots for point-in-time reads
- [x] Range iterator API (forward/reverse scans)
- [x] Tiered compaction (L0→L1→L2)
- [x] Configurable compression

**Phase 3 :**
- [x] Multi-key transactions with optimistic concurrency control
- [x] Prefix scan support
- [x] Hot backup and restore with integrity verification
- [x] Distributed replication (single-leader WAL shipping)
- [x] Parallel writes via WAL batching

**Phase 4 (complete — this release):**
- [x] Parallel writes via WriteBatch (atomic multi-key, single fsync)
- [x] Column-family / namespace support
- [x] Key TTL and time-to-live expiry
- [x] Block cache (ARC — Adaptive Replacement Cache)
- [x] Secondary indexes with range queries and backfill

**Phase 5 (planned):**
- [ ] Prometheus metrics exporter
- [ ] Streaming compaction to reduce peak memory
- [ ] Structured query / filter pushdown
- [ ] Read-your-writes consistency guarantee for followers
- [ ] Distributed query fan-out across followers
- [ ] CLI and REPL tool
- [ ] Structured logging with `slog`
- [ ] Pluggable storage backend
- [ ] Write stall and backpressure
- [ ] Schema registry and value codec
- [ ] Compaction priority queue
- [ ] Chaos / fault-injection testing harness
- [ ] OpenTelemetry trace spans
- [ ] Automatic leader election (Raft)

## Testing

```bash
# Full test suite (95 tests)
go test ./src/tests/ -v -timeout=120s

# New feature tests only
go test ./src/tests/ -run "TestWriteBatch|TestColumnFamily|TestTTL|TestARC|TestIndex" -v

# Benchmarks
go test ./src/tests/ -bench=. -benchtime=5s
```

## License

Apache License 2.0

---

**Version:** 4.0  
**Last Updated:** May 3, 2026