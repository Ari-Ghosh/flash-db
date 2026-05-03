# ADR 003: Single-Leader WAL-Shipping Replication

## Status
Accepted

## Context
FlashDB needs to support high availability and read scalability through replication. We need a way to keep follower nodes in sync with a leader node.

## Decision
We decided to implement **Single-Leader Replication via WAL-Shipping**.
- **Leader**: Maintains a ring buffer of recent WAL records. It accepts connections from followers and streams WAL records over a TCP connection.
- **Follower**: Connects to the leader, provides its last applied sequence number, and receives a stream of WAL records. It applies these records to its own local state.
- **Protocol**: Uses a custom TCP-based protocol with HMAC-SHA256 authentication for security.
- **Resync**: If a follower falls too far behind (beyond the leader's ring buffer), it can perform a full resync using the Backup & Restore mechanism.

## Consequences
- **Pros**:
  - Low latency replication as records are shipped immediately after being written to the leader's WAL.
  - Followers have a bit-identical history of mutations.
  - Offloads read traffic to followers.
- **Cons**:
  - Single point of failure (leader). Manual failover is currently required.
  - Network partitions can cause followers to lag.
