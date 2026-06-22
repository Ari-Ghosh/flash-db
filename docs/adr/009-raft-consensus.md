# ADR 009: Raft Consensus for Automatic Leader Election

**Status:** Accepted  
**Date:** June 2026  
**Driver:** High availability, fault tolerance, split-brain prevention

## Context

FlashDB's original replication model (ADR 003) used **single-leader WAL shipping** with manual leader/follower configuration:

- The admin configured one node as `role: "leader"` and others as `role: "follower"`.
- The leader shipped WAL records to followers over TCP with HMAC authentication.
- If the leader failed, an operator had to promote a follower manually.
- There was no split-brain protection — two leaders could start independently.

This was documented as a limitation in the v3 release. As FlashDB moves toward production deployments, automatic failover and strong consistency guarantees are required.

## Decision

We adopt the **Raft consensus protocol** using the `hashicorp/raft` library to replace the manual replication model.

### Key Design Decisions

1. **`hashicorp/raft` as the consensus layer**:
   - Mature, well-tested library (used by Consul, Nomad, and many others).
   - Handles leader election, log replication, safety, and membership changes.
   - Provides `raft.FSM` interface for state machine integration.

2. **FSM pattern**:
   - A `fsmWrapper` adapts the engine's `ApplyWALRecord` to `raft.FSM.Apply()`.
   - JSON-encoded `WALRecord` is the unit of consensus.
   - The FSM's `Apply` returns the committed sequence number, which the submitter uses to update local state.

3. **Coexistence with existing replication**:
   - The existing `Leader`/`Follower` structs remain for users who prefer manual replication.
   - Raft is configured via a separate `engine.Config.Raft *replication.RaftConfig` field.
   - When `Raft` is non-nil, `Put`, `Delete`, and `ApplyTxn` route through `raft.Apply()`.

4. **Storage**:
   - **Log store**: Simple append-only file format storing `<index, term, data, type>` records. Sufficient for development; a BoltDB-backed store is planned for production.
   - **Stable store**: JSON file for current term and voted-for state.
   - **Snapshot store**: `raft.FileSnapshotStore` for log compaction.

5. **Cluster formation**:
   - First node bootstraps the cluster via `raft.BootstrapCluster`.
   - Additional nodes start without bootstrapping and are added as voters via `raft.AddVoter` called from the leader.

## Consequences

### Positive

- **Automatic failover**: Leader election completes in ~1–2 seconds without operator intervention.
- **Linearizable consistency**: Writes are confirmed only after majority commit.
- **Split-brain prevention**: Raft's term mechanism ensures at most one leader per term.
- **Tolerates failures**: A 3-node cluster tolerates 1 failure; 5 nodes tolerate 2.
- **Familiar protocol**: Raft is well-understood and widely deployed.

### Negative

- **Increased latency**: Writes require majority acknowledgment before returning (one network round-trip vs local commit).
- **Dependency weight**: `hashicorp/raft` brings indirect dependencies for transport and metrics.
- **Configuration complexity**: Users must assign unique node IDs and addresses, and manage cluster membership.
- **Bootstrap ordering**: The first node must start before others can join.
- **Voter management**: Joining nodes start as non-voters; the leader must actively add them as voters.

## Technical Details

### Raft Configuration

```go
type RaftConfig struct {
    NodeID       string   // unique cluster identifier
    RaftAddr     string   // TCP bind address for Raft protocol
    JoinAddr     string   // existing node to join (empty = bootstrap)
    DataDir      string   // log, stable, and snapshot storage
    MemTableSize int64
}
```

### Write Flow

```
Client → db.Put("k", "v")
  └→ json.Marshal(WALRecord{Kind: Put, Key: "k", Value: "v"})
       └→ raft.Apply(jsonBytes, 10s timeout)
            └→ Leader appends to local log
                 └→ Replicates to followers
                      └→ Majority confirms → committed
                           └→ FSM.Apply(logEntry)
                                └→ engine.ApplyWALRecord(r)
                                     └→ memtable.Put("k", "v", seq)
                                          └→ returns LastAppliedSeq()
```

### FSM Integration

```go
// RaftFSM — the interface the engine implements
type RaftFSM interface {
    ApplyWALRecord(r WALRecord) error
    LastAppliedSeq() uint64
    Snapshot() (raft.FSMSnapshot, error)
    Restore(io.ReadCloser) error
}

// fsmWrapper — adapts RaftFSM to raft.FSM
type fsmWrapper struct { fsm RaftFSM }

func (w *fsmWrapper) Apply(log *raft.Log) interface{} {
    var r WALRecord
    json.Unmarshal(log.Data, &r)
    w.fsm.ApplyWALRecord(r)
    return w.fsm.LastAppliedSeq()
}
```

### Failure Detection

Raft's failure detection is configured via heartbeat and election timeouts:

```go
rCfg.HeartbeatTimeout  = 1 * time.Second
rCfg.ElectionTimeout   = 1 * time.Second
rCfg.LeaderLeaseTimeout = 500 * time.Millisecond
```

These defaults provide ~1–2s failover detection in normal network conditions.

## References

- [hashicorp/raft](https://github.com/hashicorp/raft) library
- [Raft Consensus Algorithm](https://raft.github.io/)
- [Raft Architecture Documentation](../architecture/RAFT.md)
- [ADR 003: WAL-Shipping Replication](003-wal-shipping-replication.md)
