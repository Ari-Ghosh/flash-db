# Architecture Overview

FlashDB is a high-performance, embedded key-value store written in Go. It is designed to handle write-heavy workloads while maintaining excellent read performance for large-scale datasets.

## Key Design Principles
1. **LSM-Tree for Writes**: Sequential appends to a WAL and in-memory MemTable updates ensure that write operations are extremely fast.
2. **B-Tree for Reads**: Data is eventually consolidated into a page-based B+ tree, which provides stable O(log n) lookups with minimal read amplification.
3. **MVCC (Multi-Version Concurrency Control)**: All entries are versioned with sequence numbers, allowing for point-in-time snapshots and non-blocking reads.
4. **Optimistic Concurrency Control**: Transactions use OCC to provide atomicity and isolation without the need for complex locking.

## Component Interaction
The following diagram illustrates how the core components interact during write and read operations:

```
┌──────────────────────────────────────────────────────────────┐
│                         Public API                           │
│    Engine: Put/Get/Delete | NewSnapshot/Iterator            │
│    Txn: Begin/Commit | PrefixScan | Backup/Restore          │
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
```

## Storage Hierarchy
FlashDB organizes data into tiers to balance write speed and read efficiency:
- **Tier 0 (Memory)**: Active MemTable (Skip List).
- **Tier 1 (Disk, Unconsolidated)**: L0 SSTable files.
- **Tier 2 (Disk, Optimized)**: L1 B-tree (recently compacted).
- **Tier 3 (Disk, Final)**: L2 B-tree (long-term storage).
