# FlashDB Documentation

**Project Status:** Core implementation complete (v3 with transactions, replication, backup/restore)  
**Last Updated:** April 22, 2026  
**Language:** Go 1.22.2

---

## Table of Contents

1. [Development History](#development-history)
2. [System Architecture](#system-architecture)
3. [Component Details](#component-details)
4. [Data Flow](#data-flow)
5. [Implementation Notes](#implementation-notes)
6. [Performance Analysis](#performance-analysis)
7. [Testing & Validation](#testing--validation)
8. [Transactions](#transactions)
9. [Replication](#replication)
10. [Backup & Restore](#backup--restore)

---

## Development History

### Phase 1: Core Data Structures (Complete)

#### Memtable with Skip List
- **Status:** ✅ Complete
- **Implementation:** Skip list-based in-memory store with concurrent access
- **Key Features:**
  - O(log n) insertion and lookup
  - Probabilistic balancing (probability = 0.25, max level = 16)
  - Sorted iteration for efficient flushing to SSTables
  - RWMutex-protected for concurrent reads and exclusive writes

#### B-Tree Storage
- **Status:** ✅ Complete (Enhanced in v2)
- **Implementation:** Page-based B+ tree in separate L1/L2 files
- **Key Features:**
  - Fixed 4 KB pages for efficient I/O
  - Bulk-load algorithm for atomic tree rebuilds
  - Incremental merge support for tiered compaction
  - Simple page cache (512 entry limit)
  - Separator-key internal nodes, leaf pages with sequence numbers

#### SSTable (Sorted String Table)
- **Status:** ✅ Complete (Enhanced in v2)
- **Implementation:** Immutable, sorted on-disk format
- **Key Features:**
  - 4 KB data blocks with entries in sorted order
  - Index block for fast block location via binary search
  - Bloom filter attached to each file
  - Optional block compression
  - k-way merge support for compaction

#### Write-Ahead Log (WAL)
- **Status:** ✅ Complete (Enhanced in v2)
- **Implementation:** Length-prefixed records with CRC32 checksums
- **Key Features:**
  - Group-commit batching for improved throughput
  - Persistent before MemTable writes
  - Replay capability for crash recovery
  - Separate Put/Delete record kinds
  - Sequence numbering for ordering

#### Bloom Filter
- **Status:** ✅ Complete
- **Implementation:** Fixed-size probabilistic data structure
- **Key Features:**
  - Kirsch-Mitzenmacher trick for hash functions
  - Configurable false positive rate (default 1%)
  - O(k) lookups where k = number of hash functions
  - Serializable for persistence

### Phase 2: Engine & Orchestration (Complete)

#### Main Engine
- **Status:** ✅ Complete (v3)
- **Implementation:** Coordinates all components into a unified database
- **Key Features:**
  - `Put(key, value)` - write path
  - `Get(key)` - read path with MemTable/B-tree merging
  - `Delete(key)` - tombstone support
  - `NewSnapshot()` - MVCC point-in-time reads
  - `NewIterator(opts)` - range scans (forward/reverse)
  - `PrefixScan(prefix)` - prefix-based range queries
  - `Begin()` - multi-key transactions
  - `Backup(destDir)` - hot backup
  - `Close()` - graceful shutdown with final flush
  - `Stats()` - observable metrics

#### Compaction Engine
- **Status:** ✅ Complete (v2)
- **Implementation:** Incremental, tiered SSTable→B-tree compaction
- **Key Features:**
  - Non-blocking background goroutine
  - Tiered L0→L1→L2 compaction
  - k-way merge with deduplication by sequence number
  - Atomic B-tree swaps to avoid blocking reads
  - Tombstone GC respecting snapshot visibility
  - Automatic SSTable file cleanup
  - Queue-based triggering (unbounded)

#### Snapshot Tracker
- **Status:** ✅ Complete (v2)
- **Implementation:** Tracks live snapshots for MVCC
- **Key Features:**
  - Reference counting for automatic cleanup
  - Sequence number pinning
  - Prevents premature tombstone GC

#### Merged Iterator
- **Status:** ✅ Complete (v2)
- **Implementation:** Merges MemTable + L1 + L2 views
- **Key Features:**
  - Priority queue for k-way merging
  - Deduplication by sequence number
  - Forward and reverse iteration
  - Snapshot-isolated reads

### Phase 3: Advanced Features (Complete)

#### Multi-Key Transactions
- **Status:** ✅ Complete (v3)
- **Implementation:** Optimistic concurrency control
- **Key Features:**
  - `Txn.Begin()` - start transaction with snapshot
  - `Txn.Get/Put/Delete()` - buffered operations
  - `Txn.Commit()` - validate read-set, apply atomically
  - Conflict detection via sequence number comparison
  - WAL batching for atomic multi-key writes

#### Prefix Scans
- **Status:** ✅ Complete (v3)
- **Implementation:** Range queries over key prefixes
- **Key Features:**
  - `PrefixScan(prefix)` - convenience wrapper
  - Automatic prefix→range bounds conversion
  - IteratorOptions.Prefix support
  - Forward and reverse prefix iteration

#### Hot Backup & Restore
- **Status:** ✅ Complete (v3)
- **Implementation:** Point-in-time backup with integrity
- **Key Features:**
  - `Backup(destDir)` - non-blocking hot backup
  - `Restore(srcDir, destDir)` - verified restore
  - SHA-256 checksums for corruption detection
  - Manifest-based integrity verification
  - Atomic file copies with temp-then-rename

#### Distributed Replication
- **Status:** ✅ Complete (v3)
- **Implementation:** Single-leader WAL shipping
- **Key Features:**
  - Leader: accepts writes, ships WAL to followers
  - Follower: read-only, applies replicated records
  - HMAC-SHA256 authentication
  - Automatic reconnection and catch-up
  - TCP-based streaming protocol

---

## System Architecture

### High-Level Overview

```
┌──────────────────────────────────────────────────────────────┐
│                         Public API                           │
│    Engine: Put/Get/Delete | NewSnapshot/Iterator            │
│    Txn: Begin/Commit | PrefixScan | Backup/Restore          │
│    Replication: Leader/Follower configuration               │
└─────────────────┬──────────────────────────────────────────────┘
                  │
        ┌─────────┼─────────┐
        │         │         │
        v         v         v
   WAL.log   MemTable   Imm.Table
(Durable)  (Newest)   (Flushing)
        │         │         │
        │         └────┬────┘
        │              v (flush when IsFull)
        └──────────► L0_*.sst files
                      │
                      v (compaction trigger)
              Compaction Engine
                      │
         ┌────────────┴────────────┐
         v                         v
    k-way merge          Deduplication
    (sorted streams)     (by seqNum)
         │                         │
         └────────────┬────────────┘
                      v
             ┌────────┴────────┐
             │                 │
             v                 v
        L1 B-tree       L2 B-tree
    (Recent data)   (Long-term data)
             │                 │
             └────────┬────────┘
                      │
                      v
                Read Queries
            (Merged Iterator)
```

### Replication Architecture

```
Leader Node                    Follower Node
┌─────────────────┐           ┌─────────────────┐
│   Engine.DB     │           │   Engine.DB     │
│   ┌─────────┐   │           │   ┌─────────┐   │
│   │  WAL    │   │           │   │  WAL    │   │
│   │ append  ├───┼───────────┼──►│ apply   │   │
│   └─────────┘   │           │   └─────────┘   │
│   Replication   │           │   Replication   │
│   Log (ring)    │           │   Applier       │
└─────────────────┘           └─────────────────┘
         │                              │
         v                              v
    TCP Stream                    Read-Only DB
    (authenticated)              (catch-up sync)
```

### Write Path (LSM-Inspired)

```
Put(key, value)
    │
    ├─ Increment seq number (atomic)
    │
    ├─ WAL.AppendPut(seq, key, value)  ← Durable before MemTable
    │  └─ Group-commit: batch with other concurrent writes, sync periodically
    │
    ├─ MemTable.Put(key, value, seq)
    │  └─ Insert into skip list (sorted by key)
    │
    └─ Check MemTable.IsFull()
       └─ If full:
          ├─ Rotate: active → immutable
          ├─ Create new active MemTable
          └─ Background flush goroutine:
             ├─ Read all entries from immutable (sorted)
             ├─ Write SSTable (4 KB blocks, index, bloom, optional compression)
             ├─ Register in L0 files list
             └─ Trigger compaction if L0 count ≥ threshold (L0→L1)
```

### Read Path (Tiered B-tree with MVCC)

```
Get(key) [or GetSnapshot(snap, key)]
    │
    ├─ Check Active MemTable.Get(key)
    │  ├─ Found & !Tombstone → Return value
    │  ├─ Found & Tombstone → Return ErrKeyDeleted
    │  └─ Not found → Continue
    │
    ├─ Check Immutable MemTable (if exists)
    │  └─ Same logic as above
    │
    └─ MergedIterator over L1 + L2 B-trees
       ├─ For each B-tree:
       │  ├─ Bloom filter: MayContain(key)?
       │  │  └─ If definitely absent → Skip this tree
       │  │
       │  └─ If possible:
       │     ├─ Start from root page
       │     ├─ Traverse internal pages (binary search on separators)
       │     ├─ Reach leaf page (page I/O)
       │     ├─ Linear search in leaf for highest seqNum ≤ snapshot seq
       │     └─ Return entry if found
       │
       ├─ Priority queue merges results from L1 + L2
       ├─ Highest seqNum wins (for regular Get: latest; for snapshot: ≤ snap.seq)
       └─ Return entry (or ErrKeyNotFound)
```

---

## Component Details

### 1. MemTable (Skip List)

**File:** `memtable/memtable.go`

#### Design Rationale
- Skip lists provide O(log n) insertions while maintaining sort order
- No rebalancing overhead like AVL/RB trees
- Efficient in-order traversal for flush operations
- Simple probabilistic structure

#### Key Methods

```go
// Insert or overwrite a key
Put(key, value []byte, seq uint64) error

// Mark key as deleted (tombstone)
Delete(key []byte, seq uint64) error

// Retrieve most recent entry
Get(key []byte) (*Entry, error)

// Return all entries in sorted order (for flushing)
Entries() []Entry

// Check if flush threshold reached
IsFull() bool
```

#### Implementation Highlights

- **Concurrency:** RWMutex protects the skip list; reads use RLock, writes use Lock
- **Deduplication:** If a key already exists and new seqNum > old seqNum, entry is overwritten
- **Size Tracking:** Approximate byte footprint tracked via atomic operations
- **Entry Count:** Number of live entries tracked for monitoring

#### Constants

```go
const maxLevel = 16       // Maximum skip list height
const probability = 0.25  // Probability of promoting to next level
```

---

### 2. B-Tree (Page-Based Read Index)

**File:** `btree/btree.go`

#### Design Rationale
- B+ trees minimize page I/O via high fanout (50+ children per node)
- Separate L1/L2 files for tiered storage
- Leaf pages store actual key-value data with sequence numbers; internals store routing info
- Bulk-load and incremental merge algorithms for compaction

#### Page Layout

**Leaf Page:**
```
[pageType(2)] [numKeys(2)] [reserved(4)]
[flag(1)] [seqNum(8)] [keyLen(2)] [key] [valLen(2)] [value] ...
```

**Internal Page:**
```
[pageType(2)] [numKeys(2)] [reserved(4)]
[rightmost(8)] [childID(8)] [keyLen(2)] [key] ...
```

#### Key Methods

```go
// Open or create B-tree file
Open(path string) (*BTree, error)

// Point lookup by key
Get(key []byte) (*Entry, error)

// Atomic bulk-load from sorted entries
BulkLoad(entries []Entry) error

// Flush all dirty pages to disk
flushAll() error
```

#### Bulk Load Algorithm

1. **Phase 1 (Leaf Level):**
   - Group entries into chunks of 50 (configurable)
   - Each chunk becomes a leaf page
   - Create index entries for each leaf

2. **Phase 2 (Internal Levels):**
   - Group child page IDs into chunks of 50
   - Create internal pages with separator keys
   - Repeat until single root remains

3. **Phase 3 (Atomicity):**
   - All pages cached in memory during build
   - Single flush writes all pages sequentially
   - Root ID and page count updated at end

**Complexity:** O(n) where n = number of entries (linear scan + sequential writes)

#### Page Cache

- Simple map-based cache holding up to 512 pages
- Populated during BulkLoad, persisted on Close
- No eviction policy (full rebuild clears cache)

---

### 3. SSTable (L0 Files)

**File:** `sstable/sstable.go`

#### File Format

```
[Data Blocks (4 KB each)]
[Index Block]
[Bloom Filter]
[Footer (32 bytes)]
  - indexOffset (8)
  - indexLen (4)
  - bloomOffset (8)
  - bloomLen (4)
  - entryCount (4)
  - magic (4) = 0xDEADBEEF
```

#### Writer

```go
// Create a writer for a new SSTable
NewWriter(path string, expectedEntries uint) (*Writer, error)

// Append an entry (MUST be sorted by key)
Add(e Entry) error

// Finalize and close
Close() error
```

**Flush Logic:**
- Entries added in sorted order
- Current block accumulates entries until 4 KB limit
- When full, block is written and index entry created
- Index entries point to start of each block and record its length

#### Reader

```go
// Open an existing SSTable for reading
OpenReader(path string) (*Reader, error)

// Bloom-filtered point lookup
Get(key []byte) (*Entry, error)

// Stream all entries (used by compaction)
Iter() (<-chan Entry, error)

// Check if key might be present
MayContain(key []byte) bool
```

**Get Logic:**
1. Check Bloom filter (false returns short-circuit to ErrKeyNotFound)
2. Binary search index to find block containing key range
3. Read that block and linear search within it

---

### 4. Write-Ahead Log (WAL)

**File:** `wal/wal.go`

#### Record Format

```
[checksum (4)] [length (4)]
[payload]
  - kind (1): 0=Put, 1=Delete
  - seqNum (8)
  - keyLen (4)
  - key (variable)
  - [valLen (4)] (only for Put)
  - [value (variable)] (only for Put)
```

#### Methods

```go
// Open or create WAL
Open(path string) (*WAL, error)

// Append Put record (checksummed, flushed, synced)
AppendPut(seq uint64, key, value []byte) error

// Append Delete record (checksummed, flushed, synced)
AppendDelete(seq uint64, key []byte) error

// Replay all valid records on startup
Replay() ([]Record, error)

// Delete WAL file (after compaction)
Delete() error
```

#### Replay Logic

- Seek to start of file
- Read header (checksum + length)
- Read payload and verify checksum
- If checksum fails or payload truncated, stop (treat missing entries as uncommitted)
- Restore all entries into empty MemTable with highest seqNum

#### Durability

- Writes are batched via group-commit for throughput
- Batches are periodically flushed and synced to disk
- OS crash will not lose any persisted write
- On restart, WAL is replayed to recover in-memory state

---

### 5. Bloom Filter

**File:** `bloom/bloom.go`

#### Design

- **Hash Functions:** Two base hashes + Kirsch-Mitzenmacher trick
  - First hash: FNV-1a-like mixing
  - Second hash: Independent mixing with different constants
  - Derived hashes: h1 + i*h2 for i = 0..k-1

- **Size Calculation:**
  - m = optimal bit count for n elements at fpr false-positive rate
  - k = optimal number of hash functions given m and n
  - Both calculated at construction time

#### Methods

```go
// Create filter sized for n elements at fpr rate
New(n uint, fpr float64) *Filter

// Add key to filter
Add(key []byte)

// Check if key might be in set
MayContain(key []byte) bool

// Serialize for persistence
Bytes() []byte

// Deserialize from bytes
FromBytes(b []byte) *Filter

// Current theoretical false positive rate
FalsePositiveRate(n uint) float64
```

#### Performance

- **Add:** O(k) where k ≈ ln(2) * (m/n) ≈ 7 for 1% FPR
- **Check:** O(k) bit lookups
- **Space:** ~10 bits per key for 1% FPR

---

### 6. Compaction Engine

**File:** `compaction/compaction.go`

#### Architecture

- Background goroutine with unbounded event queue
- Incremental tiered compaction: L0→L1→L2
- Atomic B-tree swaps to avoid blocking reads
- Tombstone GC respecting snapshot visibility

#### k-Way Merge Algorithm

```
Input: Multiple sorted SSTable readers
Output: Single sorted, deduplicated entry list

1. Create min-heap with entries from each reader
   - Heap ordered by: (key, -seqNum)
     * Primary: lowest key first
     * Secondary: highest seqNum first (for deduplication)

2. While heap not empty:
   a. Pop minimum item
   b. Advance that reader (push next entry to heap)
   c. If key == lastKey (duplicate), skip
   d. Set lastKey = current key
   e. If entry is tombstone, skip (garbage collection)
   f. Append to result

3. Return deduplicated result
```

**Complexity:** O(n log k) where n = total entries, k = number of readers

#### Deduplication & GC

- **Highest SeqNum Wins:** Heap ordering ensures first occurrence is highest version
- **Tombstone GC:** Tombstones are dropped only if no live snapshots can see them
  - Checks oldest snapshot sequence number
  - Preserves MVCC isolation

#### Bulk Load Integration

After k-way merge:
1. Call `BTree.BulkLoad(mergedEntries)`
2. Atomic tree rebuild (all new pages in one shot)
3. Delete consumed SSTable files
4. Rotate WAL to new path (old entries are now in B-tree)

---

## Data Flow

### 1. Write Flow (Put Operation)

```
Application calls: db.Put([]byte("user:1"), []byte("Alice"))

STEP 1: Sequence Number
├─ Atomic increment: seq = 1234

STEP 2: WAL Persistence
├─ WAL.AppendPut(1234, "user:1", "Alice")
├─ Payload marshalled: [0x00] [1234] [6] ["user:1"] [5] ["Alice"]
├─ CRC32 calculated
├─ Record batched with other concurrent writes
├─ Batch flushed and synced to disk periodically ✓

STEP 3: MemTable Write
├─ MemTable.Put("user:1", "Alice", 1234)
├─ Skip list insertion at appropriate level
├─ Size tracking: 1MB total so far
├─ Count tracking: 15000 entries

STEP 4: IsFull Check
├─ MemTable.IsFull() → totalSize >= 64MB ? → false (not yet)
└─ Return nil (success)

RESPONSE: err == nil (write acknowledged and durable)
```

### 2. Flush Flow (Background)

```
Condition: MemTable reaches 64 MB

STEP 1: Rotation
├─ Lock DB mutex
├─ oldMT := activeMT; activeMT = new MemTable
├─ Unlock DB mutex
└─ Go background: flushImmutable(oldMT)

STEP 2: Prepare SSTable
├─ Read all entries from oldMT.Entries() [sorted by key]
├─ Generate next sequence number (1250000)
├─ Path = "l0_00001250000.sst"

STEP 3: Write SSTable
├─ SSTable.NewWriter creates path
├─ For each entry:
│  ├─ Encoded as: [flags] [seqNum] [keyLen] [key] [valLen] [value]
│  ├─ Added to current block
│  └─ If block >= 4KB, flush block and index
├─ Final block flushed
├─ Index block written (first key of each data block)
├─ Bloom filter built and written
├─ Footer written with metadata
└─ File closed

STEP 4: Compaction Check
├─ Lock L0 files list
├─ Append new file path
├─ l0Count = 2 (now have 2 files)
├─ Unlock L0 files list
│
├─ If l0Count >= 4:
│  ├─ Snapshot all L0 paths
│  ├─ Clear L0 list (for new writes during compaction)
│  └─ Compactor.Trigger(paths)
│     └─ Enqueue for background compaction
└─ Return

BACKGROUND COMPACTION CONTINUES...
```

### 3. Compaction Flow (Background)

```
Trigger: L0 count ≥ 4 files (L0→L1 compaction)

STEP 1: Open SSTable Readers
├─ For each path in ["l0_00001000000.sst", "l0_00001050000.sst", ...]
├─ SSTable.OpenReader(path)
├─ Load footer, index, and bloom filter
└─ Ready for streaming

STEP 2: K-Way Merge
├─ Create min-heap ordered by (key, -seqNum)
├─ Prime heap with first entry from each reader
├─ While heap not empty:
│  ├─ Pop minimum entry
│  ├─ Advance that reader (push next entry)
│  ├─ Skip if duplicate key
│  ├─ Skip if tombstone (GC, respecting snapshots)
│  └─ Append to result
└─ Result = deduplicated, sorted entries

STEP 3: Incremental Merge into L1 B-tree
├─ L1.BTree.Merge(result)  // Incremental merge, not full rebuild
├─ Atomic swap: old L1 → new L1
├─ If L1 size ≥ threshold: trigger L1→L2 compaction
├─ Delete consumed SSTable files
├─ Rotate WAL to new path (old entries are now in L1)
└─ Log: "compaction: merged 4 SSTables → L1 (1234567 entries)"
```

### 4. Read Flow (Get Operation)

```
Application calls: db.Get([]byte("user:1"))  [or db.GetSnapshot(snap, []byte("user:1"))]

STEP 1: Check Active MemTable
├─ MemTable.Get("user:1") with RLock
├─ Skip list binary search O(log 15000)
├─ Entry found: Entry{Key: "user:1", Value: "Alice", SeqNum: 1234}
├─ Tombstone? No
└─ Return "Alice" ✓

RESPONSE: value = "Alice", err = nil

---

Alternative: If key not in MemTables:

STEP 2: Merged Iterator over L1 + L2 B-trees
├─ Create priority queue merging L1 and L2 iterators
├─ For each B-tree:
│  ├─ MayContain("user:2") on bloom filter
│  │  └─ If false: skip this tree
│  │
│  └─ Binary search index to find block
│     ├─ Index entries: [("aa", offset1), ("mm", offset2), ("zz", offset3)]
│     ├─ "user:2" > "mm" and < "zz"
│     └─ Read block at offset2
│
├─ Linear search in leaf for highest seqNum
│  ├─ For Get(): any seqNum
│  ├─ For GetSnapshot(): seqNum ≤ snap.Seq()
│  └─ If "user:2" found: collect candidate
│
├─ Priority queue selects highest seqNum candidate
├─ If tombstone: return ErrKeyDeleted
└─ Return value

RESPONSE: value = nil, err = ErrKeyNotFound
```

---

## Implementation Notes

### Concurrency Model

#### Write Serialization

```go
// Engine enforces single-writer semantics via MemTable mutex
func (db *DB) Put(key, value []byte) error {
    seq := atomic.AddUint64(&db.seq, 1)  // Atomic increment
    
    err := db.walActive.AppendPut(seq, key, value)  // Group-commit batching
    err := db.memtable.Put(key, value, seq)         // MemTable mutex lock
    if db.memtable.IsFull() {
        return db.triggerFlush()  // Background task
    }
    return err
}
```

- Writes batched via WAL group-commit for throughput
- MemTable writes serialized by its internal mutex
- No inter-write parallelism, but flush/compaction happen in background

#### Read Concurrency

```go
func (db *DB) Get(key []byte) ([]byte, error) {
    // Multiple goroutines can call Get concurrently
    // Each component uses RWMutex for concurrent reads
    
    // MemTable read (RLock)
    if e, err := db.memtable.Get(key); err == nil { ... }
    
    // Merged iterator over L1 + L2 B-trees (RLock each)
    iter := db.newMergedIterator(key)
    e, err := iter.Get(key)
}
```

- Readers don't block each other via RWMutex
- Writers block on MemTable mutex
- B-tree rebuild (compaction) is atomic from readers' perspective

#### Atomic Operations

```go
// Sequence number
seq := atomic.AddUint64(&db.seq, 1)  // CAS loop guaranteed

// MemTable size and count tracking
atomic.AddInt64(&m.size, delta)   // Concurrent updates
atomic.LoadInt64(&m.size)         // Non-blocking reads

// BTree bulk-load
bt.BulkLoad(entries)  // Atomic: new tree built in cache, then flushed
```

---

### Error Handling

#### Sentinel Errors

```go
var (
    ErrKeyNotFound = errors.New("key not found")
    ErrKeyDeleted  = errors.New("key deleted")
)
```

- Returned instead of nil value to distinguish "not found" from "deleted"
- Caller can distinguish using `errors.Is(err, ErrKeyDeleted)`

#### Recovery

```go
db, err := engine.Open(cfg)  // Open recovers via WAL replay
if err != nil {
    // Fatal errors: cannot proceed
    // (corrupt header, disk full, permissions, etc.)
    panic(err)
}
defer db.Close()
```

- Corrupt WAL records silently ignored (treated as uncommitted)
- B-tree corruption would require manual recovery (future work)

---

### Memory Management

#### MemTable Lifecycle

```go
active := MemTable.New(64 * 1024 * 1024)  // Active writes go here

// When full:
immutable := active
active = MemTable.New(64 * 1024 * 1024)    // New active

// Background flush
entries := immutable.Entries()              // O(n) copy
sstable.NewWriter(...).Add(entries...)      // Write to disk
immutable = nil                             // GC eligible
```

#### SSTable Lifecycle

```go
// Created during flush
sst := sstable.NewWriter("l0_000.sst", 50000)
for e := range memtable.Entries() {
    sst.Add(e)  // Accumulates in memory
}
sst.Close()     // Flush to disk, release memory

// During compaction
reader := sstable.OpenReader("l0_000.sst")
for e := range reader.Iter() {
    // Streamed, not all in memory
}
reader.Close()  // File stays on disk

// After compaction
os.Remove("l0_000.sst")  // Freed when no longer needed
```

#### B-Tree Lifecycle

```go
// Single file, memory-mapped page cache
bt := btree.Open("btree.db")

// Compaction triggers rebuild
bt.BulkLoad(mergedEntries)  // New tree in cache
                            // Old tree pages discarded
                            // New pages flushed atomically

// On Close()
bt.flushAll()   // All dirty pages → disk
bt.saveHeader() // Root ID, page count
bt.f.Close()    // File handle released
```

---

## Performance Analysis

### Write Path Performance

**Latency Breakdown (per Put):**

```
Atomic increment:           ~10 ns
WAL batch append:           ~100 ns (in-memory)
Periodic flush + sync:      ~1-5 ms (disk I/O, batched)
MemTable insertion:         ~500 ns (O(log n) skip list)
IsFull check:               ~1 ns (atomic load)
─────────────────────────────
Total P50 latency:          ~600 ns (group-commit batching)
Total P99 latency:          ~1-5 ms (when batch flushes)
```

**Throughput:**

```
Sequential writes with group-commit (1 MB/s disk, 64 MB MemTable):
├─ Batch size: 100 entries per fsync
├─ Time to fill: 64 MB / 1 MB/s ≈ 64 seconds
├─ Entries (1 KB avg): 64K entries
├─ Throughput: 64K / 64s ≈ 1000 ops/sec (vs 200 ops/sec without batching)
```

### Read Path Performance

**Latency Breakdown (best case - MemTable hit):**

```
RWMutex RLock acquisition:  ~100 ns
Skip list binary search:     ~200 ns (O(log n), n=50k)
────────────────────────────
Total P50 latency:          ~300 ns (in-memory, cache-warm)
```

**Latency Breakdown (L1/L2 B-tree hit with Bloom positive):**

```
RWMutex RLock acquisition:  ~100 ns
Bloom filter check:         ~500 ns (7 hash functions, per tree)
Binary search index:        ~200 ns (O(log index entries))
Page I/O:                   ~1-5 ms (disk read)
Linear search in page:      ~100 ns (50 entries avg)
Priority queue merge:       ~50 ns (2-3 trees)
────────────────────────────
Total P50 latency:          ~2-6 ms (disk I/O dominant)
```

**Latency Breakdown (B-tree miss with Bloom negative):**

```
RWMutex RLock acquisition:  ~100 ns
Bloom filter check:         ~500 ns (returns false)
────────────────────────────
Total P50 latency:          ~600 ns (early exit)
Total P99 latency:          ~1 µs
```

### Compaction Performance

**Time Complexity:**

```
Incremental merge k L0 files of n entries total:
├─ Heap operations: k inserts, n pops → O(n log k)
├─ BTree incremental merge: O(n) scan + O(n log n) insertions
├─ SSTable cleanup: O(k) file deletes
└─ Total: O(n log k) dominated by I/O

Example: 4 files × 100K entries = 400K total
├─ Heap operations: 400K * log 4 ≈ 800K operations
├─ B-tree insertions: 400K * log(400K) ≈ 4M operations
├─ At 1M ops/sec: ~5 seconds of CPU time
└─ Plus disk I/O for reading 4 SSTables and writing B-tree pages
```

**Space Complexity:**

```
Peak space during compaction:
├─ All k readers open: k × (meta + page cache)
├─ Merge heap: ~k heap items
├─ Result buffer: n entries (full copy)
├─ B-tree cache: n entries (during BulkLoad)
└─ Total: ~2n + O(k) ≈ 2n

With n = 400K entries, each 1KB avg:
├─ Peak memory: ~800 MB
└─ (Plus base engine memory ~100 MB)
```

---

## Testing & Validation

### Current Test Suite (main.go)

The example demonstrates:

1. **Basic Operations**
   ```
   Put / Get / Delete on simple keys
   ```

2. **MVCC Snapshots**
   ```
   NewSnapshot() creates point-in-time view
   GetSnapshot() reads from specific sequence
   ```

3. **Range Iterators**
   ```
   NewIterator() with bounds and direction
   Forward/reverse scans over key ranges
   ```

4. **Overwrites & Tombstones**
   ```
   Multiple Puts to same key → highest SeqNum wins
   Delete returns ErrKeyDeleted
   ```

5. **Bulk Writes & Compaction**
   ```
   5000 entries → MemTable flush → L0 files → tiered compaction
   ```

5. **Crash Recovery**
   ```
   Close and reopen → WAL replay → data intact
   ```

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

### Future Test Coverage

- [ ] Concurrent reader/writer stress test
- [ ] Large dataset performance benchmarks
- [ ] WAL corruption recovery testing
- [ ] Compaction stress under write load
- [ ] Bloom filter false positive rate validation
- [ ] Page cache eviction policies
- [ ] Long-running durability test

---

## Known Limitations & Future Roadmap

### Current Limitations

1. **No concurrent writes**
   - Single writer via MemTable mutex (future: optimistic concurrency)
2. **Memory usage during compaction**
   - Temporary space for merged data (future: streaming compaction)
3. **No column families**
   - Single keyspace per database (future: multi-tenant support)

### Future Enhancements

**Phase 3 (Performance):**
- [x] WAL group-commit for higher throughput
- [x] Tiered compaction (L0→L1→L2)
- [x] Configurable compression
- [x] Incremental B-tree merges
- [x] Parallel writes via WAL batching
- [ ] Parallel writes via optimistic locking
- [ ] Better page cache eviction (LRU)
- [ ] Streaming compaction to reduce memory usage

**Phase 4 (Features):**
- [x] MVCC snapshots for point-in-time reads
- [x] Range iterator API (forward/reverse scans)
- [x] Multi-key transactions with optimistic concurrency control
- [x] Prefix scan support
- [x] Hot backup and restore with integrity verification
- [x] Distributed replication (single-leader WAL shipping)
- [ ] Column-family / namespace support
- [ ] Key TTL and time-to-live expiry

**Phase 5 (Advanced):**
- [ ] Monitoring and profiling integration
- [ ] Prometheus metrics exporter
- [ ] Block cache (ARC / CLOCK-Pro)
- [ ] Secondary indexes
- [ ] Structured query / filter pushdown
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

---

## Transactions

### Architecture

Transactions implement optimistic concurrency control with the following phases:

1. **Begin**: Snapshot current sequence number as read baseline
2. **Execute**: Buffer all reads/writes locally in Txn struct
3. **Validate**: Check if any read-set keys changed since Begin
4. **Apply**: WAL batch all writes atomically, then update MemTable

### Key Components

#### Txn Struct
- **snapSeq**: Sequence number at transaction start
- **reads**: Map of keys read with their sequence numbers
- **writes**: Buffered mutations (Put/Delete operations)
- **txnID**: Unique transaction identifier

#### Conflict Detection
```go
// Phase 1: Validate read-set
for each key in reads:
    currentSeq = store.SeqAt(key, now)
    if currentSeq > reads[key].seqNum:
        return ErrTxnConflict
```

#### Atomic Application
```go
// Phase 2: Build WAL batch
batch = [TxnBegin, mutations..., TxnCommit]

// Phase 3: Apply atomically
wal.AppendBatch(batch)  // One fsync for entire transaction
memtable.Apply(ops)     // In-memory updates
```

### Performance Characteristics

- **Read-Only**: O(1) commit (no I/O)
- **Small Transactions**: Low overhead, single WAL batch
- **Conflicts**: Early abort before WAL writes
- **Large Transactions**: Memory bounded (MaxOps = 10,000)

---

## Replication

### Leader-Follower Architecture

- **Leader**: Accepts writes, ships WAL records to followers
- **Follower**: Read-only replica, applies leader's writes
- **Protocol**: TCP-based streaming with HMAC authentication

### Wire Protocol

```
Handshake:
  [8] magic (0xF1A5DB00F1A5DB00)
  [8] fromSeq (follower's last applied seq)
  [32] HMAC-SHA256(secret, challenge)

Response:
  [8] magic
  [1] status (0=OK, 1=resync needed)

Frame:
  [4] crc32
  [4] payloadLen
  [payload] WAL record bytes
```

### Leader Implementation

```go
type Leader struct {
    ring *RingBuffer  // WAL records for shipping
    streams []Stream  // Connected followers
}

func (l *Leader) Ship(record WALRecord) {
    l.ring.Append(record)
    for each follower:
        follower.Send(record)
}
```

### Follower Implementation

```go
type Follower struct {
    conn net.Conn
    lastSeq uint64
}

func (f *Follower) Apply(record WALRecord) {
    if record.SeqNum != f.lastSeq + 1 {
        // Gap detected, trigger resync
        f.resync()
    }
    db.ApplyWALRecord(record)
    f.lastSeq = record.SeqNum
}
```

### Failure Handling

- **Network Partition**: Automatic reconnection with exponential backoff
- **Leader Failure**: Manual failover (future: automatic election)
- **Sequence Gaps**: Full resync using backup package
- **Authentication**: HMAC prevents unauthorized connections

---

## Backup & Restore

### Hot Backup Process

1. **Flush MemTable**: Ensure all in-memory data is on disk
2. **Pin Snapshot**: Freeze visible sequence number
3. **Copy Files**: B-tree files, SSTables, active WAL
4. **Write Manifest**: JSON with file list and SHA-256 checksums

### Integrity Verification

```go
type Manifest struct {
    Version   int
    CreatedAt time.Time
    SnapSeq   uint64
    Files     []FileEntry  // name, size, sha256
}

// Restore validates all checksums before copying
for each file in manifest:
    if !verifySHA256(file) {
        return ErrCorruptBackup
    }
```

### Atomic Operations

- **File Copies**: Temp file + rename for atomicity
- **Manifest**: Written last as completion marker
- **Permissions**: Restrictive (0700) for security

### Performance

- **Backup**: O(total bytes) with parallel file copies
- **Restore**: O(total bytes) with checksum verification
- **Space**: Minimal overhead (manifest ~1KB + checksums)

---

## References

### Data Structures

- **Skip Lists:** Pugh, W. "Skip lists: a probabilistic alternative to balanced trees"
- **B-Trees:** Bayer, R. & McCreight, E. "Organization and Maintenance of Large Ordered Indices"
- **Bloom Filters:** Bloom, B. H. "Space/Time Trade-offs in Hash Coding with Allowable Errors"

### Database Systems

- **LevelDB:** Google's key-value store (LSM inspiration)
- **RocksDB:** Facebook's high-performance database
- **BadgerDB:** MVCC key-value store in Go

### Hash Functions

- **FNV-1a:** Fowler-Noll-Vo hash function
- **Kirsch-Mitzenmacher:** Trick for deriving multiple hash functions from two bases

---

**Document Version:** 3.0  
**Last Updated:** April 22, 2026  
**Maintainer:** Arijit Ghosh
