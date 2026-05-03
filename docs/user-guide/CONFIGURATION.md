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
| `Codec` | `types.Codec` | `None` | The compression algorithm used for SSTable blocks. Options: `None`, `Snappy`, `Zstd`. |
| `BloomFPRTarget` | `float64` | `0.01` | The target false positive rate for Bloom filters. |

## Example Custom Configuration

```go
cfg := engine.DefaultConfig("/tmp/flashdb")
cfg.MemTableSize = 128 * 1024 * 1024 // 128 MB
cfg.WALSyncPolicy = wal.SyncAlways   // Maximum durability
cfg.Codec = types.CodecSnappy        // Enable compression

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
