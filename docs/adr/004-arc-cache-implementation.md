# ADR 004: Adaptive Replacement Cache (ARC) for B-Tree Pages

## Status
Accepted

## Context
The B-tree storage layer uses 4 KB pages. Reading these pages from disk for every lookup is slow. We need an in-memory page cache to store recently accessed pages. A simple Least Recently Used (LRU) cache is easy to implement but can be inefficient for certain access patterns (e.g., large scans that evict frequently used items).

## Decision
We decided to implement the **Adaptive Replacement Cache (ARC)** algorithm for the B-tree page cache.
- ARC self-tunes between recency and frequency.
- It maintains four lists: `T1` (recent), `T2` (frequent), `B1` (ghost entries for `T1`), and `B2` (ghost entries for `T2`).
- It dynamically adjusts the target size of the recency and frequency partitions based on "hits" in the ghost lists.

## Consequences
- **Pros**:
  - Outperforms LRU and LFU (Least Frequently Used) on a wide range of workloads.
  - Self-tuning: no need for manual parameter adjustment for different workloads.
  - Resilient to "scanning" patterns that would otherwise flush an LRU cache.
- **Cons**:
  - More complex to implement than LRU.
  - Slightly higher memory overhead for tracking ghost entries (pointers only).
