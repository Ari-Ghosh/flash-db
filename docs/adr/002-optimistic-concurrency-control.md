# ADR 002: Optimistic Concurrency Control (OCC) for Transactions

## Status
Accepted

## Context
FlashDB v3 introduced support for multi-key transactions. Implementing transactions requires a strategy for handling concurrent access and ensuring Atomicity, Consistency, and Isolation (ACI). We need a mechanism that is performant and avoids the complexity and potential deadlocks associated with pessimistic locking (2PL).

## Decision
We decided to implement transactions using **Optimistic Concurrency Control (OCC)**.
1. **Begin**: When a transaction starts, it records the current global sequence number as its read baseline.
2. **Execute**: Reads and writes are performed locally within the transaction's context. Reads are recorded in a "read-set" with their sequence numbers. Writes are buffered in a "write-set".
3. **Validate**: At commit time, the transaction checks if any keys in its read-set have been modified by another transaction since its baseline sequence number.
4. **Apply**: If validation succeeds, the write-set is applied atomically to the WAL (as a single batch) and then to the MemTable.

## Consequences
- **Pros**:
  - High concurrency for non-conflicting workloads.
  - No deadlocks.
  - Simplified implementation compared to 2-Phase Locking.
- **Cons**:
  - Transactions may fail and require retries if conflicts are frequent (high contention).
  - Validation overhead at commit time.
