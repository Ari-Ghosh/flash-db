# Raft Consensus & Leader Election

FlashDB uses the **Raft consensus protocol** (`hashicorp/raft`) to provide automatic leader election and fault-tolerant log replication across cluster nodes.

## Overview

Raft replaces the manual single-leader replication model with automated consensus. Writes are submitted to the Raft cluster, which elects a leader, replicates the write to a majority of nodes, and commits it durably before returning to the caller.

```
         ┌─────────────────────────────────────────────┐
         │              Raft Cluster                    │
         │                                              │
         │   ┌──────────┐     ┌──────────┐             │
         │   │  Node 1  │◄───►│  Node 2  │             │
         │   │ (Leader) │     │(Follower)│             │
         │   │          │     │          │             │
         │   │ flashdb  │     │ flashdb  │             │
         │   │  engine  │     │  engine  │             │
         │   │ Raft FSM │     │ Raft FSM │             │
         │   └────┬─────┘     └────┬─────┘             │
         │        │                │                    │
         │   ┌────┴─────┐     ┌───┴──────┐             │
         │   │  Node 3  │     │  Node 4  │             │
         │   │(Follower)│     │(Observer)│             │
         │   └──────────┘     └──────────┘             │
         │                                              │
         │   TCP Transport (:6000-6003)                 │
         │   Raft consensus protocol                    │
         │   Automatic leader election                  │
         └──────────────────────────────────────────────┘
```

## Why Raft?

The previous replication model required manual leader/follower configuration with no automatic failover:

- **Before**: If the leader crashed, an operator had to promote a follower manually.
- **After**: Raft automatically detects leader failure and holds an election within ~1 second. The cluster continues serving writes without operator intervention.

Raft also provides:
- **Linearizable consistency**: Writes are confirmed only after a majority commits.
- **Safety**: At most one leader per term prevents split-brain.
- **Log replication**: All nodes receive the same log entries in the same order.

## Architecture

### Components

```
┌──────────────────────────────────────────────────────────┐
│                    flashDB Engine                          │
│  ┌──────────────────────────────────────────────────────┐ │
│  │  RaftCluster                                          │ │
│  │  ┌──────────┐  ┌──────────┐  ┌────────────────────┐  │ │
│  │  │ Raft     │  │ Log     │  │ Stable Store       │  │ │
│  │  │ Node     │──│ Store   │  │ (term/vote state)   │  │ │
│  │  │          │  │ (file)  │  └────────────────────┘  │ │
│  │  └────┬─────┘  └──────────┘                         │ │
│  │       │                                               │ │
│  │  ┌────┴─────┐  ┌──────────┐  ┌────────────────────┐  │ │
│  │  │ FSM      │  │Snapshot  │  │ TCP Transport      │  │ │
│  │  │ Wrapper  │──│ Store    │──│ (inter-node comms)  │  │ │
│  │  └────┬─────┘  └──────────┘  └────────────────────┘  │ │
│  │       │                                               │ │
│  │  ┌────┴─────┐                                         │ │
│  │  │ engine.DB│  (ApplyWALRecord, Snapshot, Restore)     │ │
│  │  └──────────┘                                         │ │
│  └──────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────┘
```

### Write Path (Raft)

1. Client calls `db.Put(key, value)`.
2. The engine serializes the operation as a JSON-encoded `WALRecord`.
3. The record is submitted via `raft.Apply()`, which proposes it to the cluster.
4. The leader appends the entry to its Raft log and replicates it to followers.
5. Once a majority acknowledges, the entry is committed and applied to the FSM.
6. The FSM wrapper deserializes the record and calls `db.ApplyWALRecord()`.
7. The engine applies the write to its MemTable (same as the standalone path).
8. The caller receives the committed sequence number.

### Leader Election

1. Followers maintain a heartbeat timer (default 1s).
2. If a follower receives no heartbeat within the timeout, it transitions to Candidate.
3. The Candidate requests votes from all other nodes.
4. If it receives a majority of votes, it becomes the new Leader.
5. The new Leader begins sending heartbeats to assert authority.

### Failure Tolerance

| Cluster Size | Majority | Tolerated Failures |
|---|---|---|
| 1 | 1 | 0 |
| 2 | 2 | 0 |
| 3 | 2 | 1 |
| 5 | 3 | 2 |

## Configuration

### Engine Config

```go
cfg := engine.DefaultConfig("/tmp/node1")
cfg.Raft = &replication.RaftConfig{
    NodeID:   "node1",             // unique identifier in the cluster
    RaftAddr: "127.0.0.1:6000",    // TCP address for Raft communication
    DataDir:  "/tmp/raft-data",    // Raft log + snapshot storage
    JoinAddr: "",                   // empty = bootstrap new cluster
}
```

### CLI

```bash
# Node 1 (bootstraps)
flashdb serve --dir /tmp/node1 --raft-addr :6000 --node-id node1

# Node 2 onwards (joins)
flashdb serve --dir /tmp/node2 --raft-addr :6001 --node-id node2 \
  --raft-join 127.0.0.1:6000
```

> **Important**: When adding a node via `--raft-join`, the new node starts as a non-voter. The cluster leader (node 1) must add it as a voter via the programmatic API. A CLI command for automated voter addition is planned.

## Programmatic API

### Creating a Cluster

```go
import (
    "github.com/Ari-Ghosh/flash-db/src/engine"
    "github.com/Ari-Ghosh/flash-db/src/replication"
)

cfg := engine.DefaultConfig("/tmp/node1")
cfg.Raft = &replication.RaftConfig{
    NodeID:   "node1",
    RaftAddr: "127.0.0.1:6000",
}
db, _ := engine.Open(cfg)
defer db.Close()
```

### Adding a Voter (from the Leader)

```go
// On the leader node, after the new node has started:
db.AddRaftVoter("node2", "127.0.0.1:6001", 0)
```

### Checking Leadership

```go
if db.RaftIsLeader() {
    fmt.Println("I am the leader at", db.RaftLeader())
}
```

## Storage

Raft maintains its own state in the `DataDir`:

```
/tmp/raft-data/
├── raft-log.db          # Raft log entries (append-only file)
├── raft-stable.json     # Current term, voted-for state
└── snapshots/           # Raft snapshots for log compaction
    └── <term>-<index>.meta
```

- **Log Store**: Append-only binary file with `<index, term, data, type>` records. Used for log replication and replay.
- **Stable Store**: JSON file storing the current term and voted-for candidate ID. Critical for safety across restarts.
- **Snapshot Store**: File-based snapshots allow Raft to compact old log entries.

## Code Reference

Package: `src/replication/raft.go`

Key types and functions:

```go
type RaftConfig struct {
    NodeID       string
    RaftAddr     string
    JoinAddr     string
    DataDir      string
    MemTableSize int64
}

type RaftCluster struct { /* unexported */ }

func NewRaftCluster(cfg RaftConfig, fsm RaftFSM) (*RaftCluster, error)
func (rc *RaftCluster) Leader() bool
func (rc *RaftCluster) LeaderAddr() string
func (rc *RaftCluster) Apply(cmd []byte, timeout time.Duration) raft.ApplyFuture
func (rc *RaftCluster) AddVoter(id, addr string, prevIndex uint64) raft.IndexFuture
func (rc *RaftCluster) Shutdown() error
```

## Comparison: Manual Replication vs Raft

| Aspect | Manual Replication | Raft |
|---|---|---|
| **Leader election** | Manual | Automatic (~1s failover) |
| **Write commitment** | Local WAL commit | Majority commit |
| **Consistency** | Eventual (async WAL ship) | Linearizable |
| **Split-brain protection** | None | Term-based leader authority |
| **Failure tolerance** | 0 nodes (manual recovery) | N-1/2 nodes |
| **Configuration** | Role: leader/follower | Node ID + Raft address |
| **Complexity** | Low | Moderate |
