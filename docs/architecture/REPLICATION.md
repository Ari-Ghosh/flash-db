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
- **Streaming**: It streams new records to followers over a TCP connection as soon as they are localy persisted.

## The Follower
- **Read-Only**: Followers reject all direct write operations.
- **Catch-up**: When a follower connects, it tells the leader its last applied sequence number. The leader then sends all missing records from its ring buffer.
- **Full Resync**: If a follower is too far behind (missing records have been overwritten in the leader's ring buffer), it must be re-initialized from a hot backup.

## Security
Connections between nodes are authenticated using a pre-shared secret:
1. Leader sends a random challenge.
2. Follower responds with `HMAC-SHA256(secret, challenge)`.
3. Leader verifies the HMAC before allowing the stream to start.

## Consistency
Followers are **eventually consistent**. There is a small delay between a write succeeding on the leader and it becoming visible on the followers.
