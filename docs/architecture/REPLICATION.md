# Replication Architecture

FlashDB supports single-leader replication to provide high availability and read scalability.

## Overview
Replication works by "shipping" Write-Ahead Log (WAL) records from a **Leader** node to one or more **Follower** nodes.

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
```

## The Leader
- **Write Handling**: The Leader handles all `Put`, `Delete`, and `Transaction` operations.
- **Ring Buffer**: It maintains a circular buffer of recent WAL records to quickly serve connected followers.
- **Streaming**: It streams new records to followers over a TCP connection as soon as they are locally persisted.
- **Query Fan-Out**: The leader can distribute read queries to all followers, merging results into a unified iterator via `FanOut()`.

## The Follower
- **Read-Only**: Followers reject all direct write operations.
- **Catch-up**: When a follower connects, it tells the leader its last applied sequence number. The leader then sends all missing records from its ring buffer.
- **Full Resync**: If a follower is too far behind (missing records have been overwritten in the leader's ring buffer), it must be re-initialized from a hot backup.
- **Query Serving**: Followers implement `AppendQueryApplier` to execute distributed fan-out queries locally and stream results back to the leader.

## Read-Your-Writes Consistency
`db.WaitForSeq(seqNum, timeout)` blocks until the follower's local sequence number catches up to the given value. This guarantees that a write acknowledged by the leader is visible on the follower before a subsequent read.

## Frame Multiplexing
The replication TCP connection carries two frame types multiplexed by a 1-byte kind sentinel:
- `0x01` — WAL record (existing replication stream)
- `0x02` — QueryRequest (leader → follower)
- `0x03` — QueryResponse (follower → leader)
- `0x04` — QueryDone (follower → leader, end of results)

A per-connection `writeMu` serializes writes so WAL streaming and query fan-out do not interleave.
