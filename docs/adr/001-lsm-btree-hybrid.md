# ADR 001: Hybrid LSM + B-Tree Architecture

## Status
Accepted

## Context
FlashDB needs to support high-performance write workloads while providing efficient point lookups and range scans. Standard LSM trees (like LevelDB/RocksDB) are excellent for write throughput but can suffer from high read amplification due to multiple levels of SSTables. Standard B+ trees provide excellent read performance but incur high write amplification due to random I/O and page fragmentation.

## Decision
We decided to implement a hybrid architecture that combines the strengths of both LSM trees and B+ trees.
- **Write Path (LSM)**: Incoming writes are appended to a Write-Ahead Log (WAL) and stored in an in-memory MemTable (Skip List).
- **Flushing (LSM)**: When the MemTable is full, it is flushed to disk as an immutable SSTable (Level 0).
- **Read Path (Hybrid)**: Reads check the MemTable, then L0 SSTables, and finally a tiered B-tree structure.
- **Compaction (Hybrid)**: Instead of merging SSTables into deeper LSM levels, L0 SSTables are incrementally merged into a tiered B-tree (L1 and L2).

## Consequences
- **Pros**:
  - High write throughput via sequential WAL appends and in-memory writes.
  - Reduced read amplification for long-term data by consolidating multiple SSTables into a single B-tree index.
  - Efficient range scans using a merged iterator over the MemTable, L0 SSTables, and B-trees.
- **Cons**:
  - Compaction logic is more complex as it involves merging SSTables into a B-tree.
  - B-tree pages must be carefully managed (via ARC cache) to maintain performance.
