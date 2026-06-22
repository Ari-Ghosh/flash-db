# FlashDB

A high-performance embedded key-value database combining **LSM-tree writes** with **B-tree reads**, now with a **CLI/REPL**, **OpenTelemetry tracing**, and **Raft consensus** for automatic leader election.

## Quick Start

```bash
# Install
go install github.com/Ari-Ghosh/flash-db/cmd/flashdb@latest

# Start a server
flashdb serve --dir /tmp/my_db --metrics-addr :9090

# Open an interactive REPL
flashdb repl --dir /tmp/my_db

# Or use one-shot commands
flashdb put /tmp/my_db hello world
flashdb get /tmp/my_db hello   # → world
flashdb status /tmp/my_db      # → engine stats
```

## Features

| Feature | Description |
|---|---|
| **CLI + REPL** | Full command-line interface with interactive shell, serve mode, and one-shot ops |
| **Raft Consensus** | Automatic leader election and fault-tolerant log replication across nodes |
| **OpenTelemetry Tracing** | Distributed trace spans for Put/Get/Delete/Flush/Compaction with OTLP export |
| **LSM-tree Writes** | WAL group-commit → Skip-list MemTable → SSTable flush (Snappy) |
| **B-tree Reads** | L1 (Snappy) + L2 (Zstd) B-trees with ARC page cache |
| **MVCC Snapshots** | Point-in-time consistent reads |
| **Range Iterators** | Forward/reverse scans, prefix scans, bounded iteration |
| **WriteBatch** | Atomic multi-key writes, single fsync |
| **Transactions** | Optimistic concurrency control with conflict detection |
| **Column Families** | Isolated key namespaces within a single DB |
| **Key TTL** | Per-key expiry with background reaper |
| **Secondary Indexes** | User-defined indexes with point and range queries |
| **Hot Backup/Restore** | Non-blocking backup with SHA-256 verification |
| **Replication** | Single-leader WAL shipping + distributed query fan-out |
| **Adaptive Bloom Filters** | Per-SSTable FPR telemetry for automatic filter sizing |
| **Prometheus Metrics** | `/metrics` endpoint with engine counters |
| **Pluggable Backend** | OS filesystem or in-memory (for testing) |

---

## CLI Reference

### `serve` — Start a DB server

```bash
flashdb serve \
  --dir /tmp/flashdb \
  --metrics-addr :9090 \
  --memtable-size $((64*1024*1024)) \
  --l0-threshold 4
```

**Raft cluster mode** (add `--raft-addr` and `--node-id`):

```bash
# Node 1 (bootstraps the cluster)
flashdb serve --dir /tmp/node1 --raft-addr :6000 --node-id node1 --metrics-addr :9091

# Node 2 (joins node1)
flashdb serve --dir /tmp/node2 --raft-addr :6001 --node-id node2 \
  --raft-join 127.0.0.1:6000 --metrics-addr :9092
```

**With OpenTelemetry tracing:**

```bash
flashdb serve --dir /tmp/flashdb --otel-endpoint http://localhost:4318
```

### `repl` — Interactive REPL

```bash
flashdb repl --dir /tmp/flashdb
# > put user:alice alice@example.com
# OK
# > get user:alice
# alice@example.com
# > scan user:
# user:alice = alice@example.com
# (1 keys)
# > stats
# MemTable: size=128 count=1
# L0 files: 0
# SeqNum:   1
# > exit
```

Commands: `put <key> <value>`, `get <key>`, `del <key>`, `scan <prefix>`, `stats`, `exit`

### One-shot commands

```bash
flashdb put /tmp/flashdb mykey myvalue
flashdb get /tmp/flashdb mykey
flashdb delete /tmp/flashdb mykey
flashdb status /tmp/flashdb
flashdb compact /tmp/flashdb
flashdb backup /tmp/flashdb /tmp/backup
flashdb restore /tmp/backup /tmp/restored
```

---

## Architecture

### Write Path

```
WAL (group-commit fsync) → MemTable (skip list) → SSTable L0 → Compaction → L1 B-tree → L2 B-tree
```

### Read Path

```
MemTable → L0 SSTables (bloom-filtered) → L1 B-tree (ARC-cached, Snappy) → L2 B-tree (ARC-cached, Zstd)
```

### Raft Cluster Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   Raft Node 1   │     │   Raft Node 2   │     │   Raft Node 3   │
│  (Leader)       │◄───►│  (Follower)     │◄───►│  (Follower)     │
│                 │     │                 │     │                 │
│ flashdb engine  │     │ flashdb engine  │     │ flashdb engine  │
│ ┌─────────────┐ │     │ ┌─────────────┐ │     │ ┌─────────────┐ │
│ │ Raft FSM    │ │     │ │ Raft FSM    │ │     │ │ Raft FSM    │ │
│ └─────────────┘ │     │ └─────────────┘ │     │ └─────────────┘ │
└─────────────────┘     └─────────────────┘     └─────────────────┘
       │                       │                        │
       └───────────────────────┼────────────────────────┘
                               │
                     TCP Transport (:6000-6002)
                     Raft consensus protocol
                     Automatic leader election
```

When Raft is enabled:
- **Writes** go through `raft.Apply()` for consensus
- **Log replication** is handled by the Raft protocol
- **Leader election** is automatic on failure
- A **majority** of nodes must agree before a write is committed
- **Tolerates** (N-1)/2 node failures

---

## Configuration

### Engine Config

```go
cfg := engine.DefaultConfig("/tmp/flashdb")
cfg.MemTableSize       = 64 * 1024 * 1024   // flush when MemTable exceeds this
cfg.L0CompactThreshold = 4                   // compact after N L0 files
cfg.L1SizeThreshold    = 256 * 1024 * 1024  // promote L1→L2 at this size
cfg.WALSyncPolicy      = wal.SyncBatch       // batch fsyncs for throughput
cfg.BloomFPRTarget     = 0.01                // 1% bloom FPR target
```

### Tracing Config

```go
cfg := engine.DefaultConfig("/tmp/flashdb")
cfg.Tracing = &tracing.Config{
    ServiceName: "flashdb-cluster",
    Endpoint:    "http://otel-collector:4318",  // OTLP HTTP endpoint
    SampleRate:  1.0,
    Attributes:  map[string]string{"node.id": "node1", "env": "prod"},
}
```

### Raft Config

```go
cfg := engine.DefaultConfig("/tmp/node1")
cfg.Raft = &replication.RaftConfig{
    NodeID:   "node1",
    RaftAddr: "127.0.0.1:6000",
    DataDir:  "/tmp/raft-data",
}
```

---

## Monitoring

### Prometheus Metrics

Start with `--metrics-addr :9090`, then scrape:

```
http://localhost:9090/metrics
```

Available metrics: `flashdb_seq_num`, `flashdb_memtable_size_bytes`, `flashdb_l0_file_count`, `flashdb_puts_total`, `flashdb_gets_total`, `flashdb_deletes_total`, `flashdb_compaction_l0_merges_total`, `flashdb_wal_syncs_total`, `flashdb_bloom_total_queries`, `flashdb_bloom_false_positives`, `flashdb_replication_follower_count`, `flashdb_replication_last_applied_seq`.

### OpenTelemetry Traces

Spans are emitted for: Put, Get, Delete, Flush, Transaction Commit, Compaction. Configure via `--otel-endpoint` flag or `cfg.Tracing` API. Export to any OTLP-compatible backend (Jaeger, Grafana Tempo, Datadog, etc.).

---

## Building & Testing

```bash
# Build
make build
# or
go build -o bin/flashdb ./cmd/flashdb

# Run tests
make test          # full suite
make test-short    # quick smoke tests (skips multi-node Raft)
make test-raft     # Raft-specific tests

# Test a specific feature
go test -v -timeout 60s -run TestRaft_SingleNode ./src/tests/
go test -v -timeout 60s -run TestTracing ./src/tests/
go test -v -timeout 60s -run TestCLI ./src/tests/

# Coverage
go test -v -race -timeout 120s -coverprofile=coverage.out ./...
go tool cover -html=coverage.out -o coverage.html
```

### Docker

```bash
make docker-build
docker run -p 9090:9090 flashdb:latest
# Or with Raft:
docker run flashdb serve --dir /app/data --metrics-addr :9090 --raft-addr :6000 --node-id node1
```

---

## Go API Examples

### Basic CRUD

```go
package main

import (
    "fmt"
    "github.com/Ari-Ghosh/flash-db/src/engine"
)

func main() {
    db, _ := engine.Open(engine.DefaultConfig("/tmp/demo"))
    defer db.Close()

    db.Put([]byte("greeting"), []byte("hello flashdb"))
    val, _ := db.Get([]byte("greeting"))
    fmt.Println(string(val)) // "hello flashdb"

    db.Delete([]byte("greeting"))
}
```

### Transactions

```go
tx := db.Begin()
tx.Put([]byte("alice"), []byte("100"))
tx.Put([]byte("bob"), []byte("200"))
if err := tx.Commit(); err != nil {
    fmt.Println("conflict:", err)
}
```

### Raft Cluster (programmatic)

```go
cfg := engine.DefaultConfig("/tmp/node1")
cfg.Raft = &replication.RaftConfig{
    NodeID:   "node1",
    RaftAddr: "127.0.0.1:6000",
}
db, _ := engine.Open(cfg)
defer db.Close()

// Writes automatically go through Raft consensus
db.Put([]byte("k"), []byte("v")) // committed via Raft

// Check leadership
if db.RaftIsLeader() {
    fmt.Println("I am the leader at", db.RaftLeader())
}
```

---

## Storage Layout

```
/tmp/flashdb/
├── wal.log                # Active write-ahead log
├── wal_<seq>.log          # Archived WALs
├── l0_<seq>.sst           # L0 SSTables (Snappy compressed)
├── btree_l1.db            # L1 B-tree (Snappy)
├── btree_l2.db            # L2 B-tree (Zstd)
├── raft-log.db            # Raft consensus log
└── raft-stable.json       # Raft term/vote state
```

---

## Version History

- **v4.1** (current): CLI/REPL, OpenTelemetry tracing, Raft consensus
- **v4.0**: WriteBatch, column families, TTL, ARC cache, secondary indexes
- **v3.0**: Transactions, replication, backup/restore, prefix scans
- **v2.0**: MVCC snapshots, range iterators, tiered compaction, compression
- **v1.0**: Core LSM-tree with WAL, MemTable, SSTable, B-tree

## License

Apache 2.0 — see [LICENSE](LICENSE).
