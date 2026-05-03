# ADR 005: Adaptive Bloom Filter Sizing

## Status
Accepted

## Context
FlashDB uses Bloom filters in SSTables to reduce read amplification. However, a fixed False Positive Rate (FPR) for all SSTables might be suboptimal. Some SSTables might have very high query rates with few hits (where a smaller FPR is better), while others might be rarely accessed.

## Decision
We decided to implement **Adaptive Bloom Filter Sizing** based on observed telemetry.
1. **Telemetry**: The engine tracks the number of queries and false positives for every SSTable Bloom filter.
2. **Observed FPR**: An "observed FPR" is calculated as `FalsePositives / TotalQueries`.
3. **Adjustment**: 
   - If the observed FPR exceeds the target FPR, the filter size for the *next* SSTable flushed to disk is increased (lower target FPR).
   - If the observed FPR is significantly lower than the target, the filter size for the next SSTable is decreased to save memory.
4. **Bounds**: The target FPR is kept within a configurable range (e.g., 0.001 to 0.05).

## Consequences
- **Pros**:
  - Automatically optimizes memory usage vs. read performance.
  - Adapts to changing workloads without manual intervention.
- **Cons**:
  - Telemetry adds a small amount of overhead to the read path (atomic increments).
  - Only benefits future SSTables; existing filters remain the same size.
