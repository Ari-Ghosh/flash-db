# FlashDB Documentation

**Project Status:** v5 — Streaming compaction, filter pushdown, Prometheus metrics, read-your-writes, distributed query fan-out  
**Last Updated:** June 2, 2026  
**Language:** Go 1.25.10

Welcome to the FlashDB documentation. This guide provides a comprehensive overview of the FlashDB architecture, its components, and how to use it in your applications.

---

## 📖 Table of Contents

1. [**Architecture Overview**](architecture/OVERVIEW.md)
   - [LSM Tree & Compaction](architecture/LSM_TREE.md)
   - [Distributed Replication](architecture/REPLICATION.md)
   - [Transactions & Concurrency](architecture/TRANSACTIONS.md)
2. [**Component Details**](components/)
   - [MemTable (Skip List)](components/MEMTABLE.md)
   - [SSTable (L0 Files)](components/SSTABLE.md)
   - [B-Tree (Read Index)](components/BTREE.md)
   - [Write-Ahead Log (WAL)](components/WAL.md)
   - [Bloom Filter](components/BLOOM_FILTER.md)
   - [Compaction Engine](components/COMPACTION.md)
   - [Hot Backup & Restore](components/BACKUP_RESTORE.md)
   - [WriteBatch (Atomic Multi-Key)](components/WRITE_BATCH.md)
   - [Column Families (Namespaces)](components/COLUMN_FAMILIES.md)
   - [Key TTL (Expiration)](components/TTL.md)
   - [Secondary Indexes](components/SECONDARY_INDEX.md)
   - [Prometheus Metrics](components/METRICS.md)
   - [Query Fan-Out](components/QUERY_FANOUT.md)
3. [**User Guide**](user-guide/)
   - [Getting Started](user-guide/GETTING_STARTED.md)
   - [API Reference](user-guide/API_REFERENCE.md)
   - [Configuration](user-guide/CONFIGURATION.md)
4. [**Architecture Decision Records (ADRs)**](adr/)
   - [ADR 001: Hybrid LSM + B-Tree](adr/001-lsm-btree-hybrid.md)
   - [ADR 002: OCC for Transactions](adr/002-optimistic-concurrency-control.md)
   - [ADR 003: WAL-Shipping Replication](adr/003-wal-shipping-replication.md)
   - [ADR 004: ARC Cache Implementation](adr/004-arc-cache-implementation.md)
   - [ADR 005: Adaptive Bloom Filters](adr/005-adaptive-bloom-filters.md)
   - [ADR 006: Internal Key Namespacing](adr/006-internal-key-namespacing.md)
   - [ADR 007: Five-Phase Feature Plan](adr/007-five-features-plan.md)

---

## 🚀 Quick Overview

FlashDB is a high-performance hybrid database engine combining **LSM (Log-Structured Merge) trees for writes** with **B-trees for reads**.

### Key Features
- **Fast Writes**: Sequential WAL appends and in-memory Skip List updates.
- **Fast Reads**: Page-based B+ tree with Adaptive Replacement Cache (ARC).
- **ACID Transactions**: Optimistic Concurrency Control (OCC) for multi-key operations.
- **Scalability**: Single-leader replication with automatic catch-up.
- **Reliability**: Hot backups with SHA-256 integrity verification.
- **Flexibility**: Column Families, Key TTL, and Secondary Indexes.
- **Observability**: Prometheus metrics exporter for monitoring.
- **Distributed Queries**: Fan-out queries across leader and followers.
- **Streaming Compaction**: Constant-memory compaction via incremental page flushing.
- **Filter Pushdown**: Server-side predicate filtering in all storage layers.

### Performance at a Glance
| Operation | Latency (P50) | Notes |
|---|---|---|
| **Put** | ~600 ns | Group-commit WAL + MemTable |
| **Get (MemTable)** | ~300 ns | All in memory |
| **Get (B-Tree)** | ~500 ns | ARC Cache hit |
| **Get (Disk)** | ~2-6 ms | SSD/HDD random I/O |

---

## 🛠️ Development & Contributing

For information on how to build, test, and contribute to FlashDB, please see the [DEVELOPMENT.md](../DEVELOPMENT.md) guide and the [CONTRIBUTING.md](../CONTRIBUTING.md) file.

---

## 📜 License
FlashDB is released under the [Apache License 2.0](../LICENSE).
