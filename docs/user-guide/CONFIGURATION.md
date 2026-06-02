# Configuration

FlashDB behavior can be customized via the `engine.Config` struct.

## Config Options

| Field | Type | Default | Description |
|---|---|---|---|
| `Dir` | `string` | | The directory where database files are stored. |
| `MemTableSize` | `int64` | `64 MB` | The size threshold for flushing the MemTable to an SSTable. |
| `L0CompactThreshold` | `int` | `4` | The number of SSTable files that trigger a compaction into the L1 B-tree. |
| `L1SizeThreshold` | `int64` | `256 MB` | The size threshold for promoting data from the L1 B-tree to the L2 B-tree. |
| `WALSyncPolicy` | `wal.SyncPolicy` | `SyncBatch` | Controls how mutations are synchronized to disk. Options: `SyncAlways`, `SyncBatch`, `SyncNone`. |
| `Codec` | `types.Codec` | `None` | The compression algorithm used for SSTable blocks. Options: `None`, `Snappy`, `Zstd`. Note: FlashDB implements a tiered compression policy by default (**Snappy** for L0/L1, **Zstd** for L2). |
| `BloomFPRTarget` | `float64` | `0.01` | The target false positive rate for Bloom filters. |
| `BloomFPRMin` | `float64` | `0.001` | Minimum FPR for adaptive bloom sizing. |
| `BloomFPRMax` | `float64` | `0.05` | Maximum FPR for adaptive bloom sizing. |
| `Replication` | `*replication.Config` | `nil` | Optional replication configuration. |

## Tiered Compression Policy

FlashDB automatically manages compression across LSM tiers to balance write throughput and storage density:

- **Level 0 (SSTables)**: Uses **Snappy** for fast compression during MemTable flushes, ensuring write latency remains low.
- **Level 1 (B-tree)**: Uses **Snappy** for recently compacted data, providing a good balance between access speed and space savings.
- **Level 2 (B-tree)**: Uses **Zstd** for long-term storage, maximizing compression ratio for the largest data tier.

Individual SSTable files store their compression codec in the footer, allowing for automatic decompressor selection during reads.

## Streaming Compaction (v5)

Compaction now uses constant-memory streaming k-way merge. Entries are consumed via channels from SSTable readers and the B-tree's `StreamEntries()`, and written incrementally via `BulkLoadFromIter`. No `[]types.Entry` slice materialization — peak memory is proportional to the number of source streams, not the dataset size.

## Filter Pushdown (v5)

`IteratorOptions.Filter` allows callers to provide a predicate `func(*Entry) bool`. The filter is applied at each storage layer's scan time, skipping non-matching entries before they reach the merged iterator.

## Example Custom Configuration

```go
cfg := engine.DefaultConfig("/tmp/flashdb")
cfg.MemTableSize = 128 * 1024 * 1024 // 128 MB
cfg.WALSyncPolicy = wal.SyncAlways   // Maximum durability

db, err := engine.Open(cfg)
```

## Replication Configuration
Replication is configured via the `Replication` field in `engine.Config`.

### Leader
```go
cfg.Replication = &replication.Config{
    Role: "leader",
    ListenAddr: ":5432",
    Secret: []byte("shared-secret"),
}
```

### Follower
```go
cfg.Replication = &replication.Config{
    Role: "follower",
    LeaderAddr: "leader-host:5432",
    Secret: []byte("shared-secret"),
}
```

## Prometheus Metrics

Start a metrics server with the `metrics` package:

```go
import "github.com/Ari-Ghosh/flash-db/src/metrics"

exp := metrics.NewExporter(":9090")
exp.Register(db)
exp.Start()
defer exp.Stop()
```

Metrics are served at `http://localhost:9090/metrics`.
