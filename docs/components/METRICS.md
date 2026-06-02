# Prometheus Metrics

FlashDB exposes internal metrics via a Prometheus-compatible HTTP endpoint for monitoring and alerting.

## Architecture

The metrics exporter runs as an opt-in HTTP server on a configurable address. It implements `prometheus.Collector` to produce metrics on each scrape. No background goroutine is needed for metric collection — all values are read directly from `atomic` counters in the engine.

```
┌──────────────┐   GET /metrics   ┌───────────────┐
│  Prometheus  │ ────────────────> │  FlashDB      │
│  Server      │ <──────────────── │  Exporter     │
└──────────────┘   text/plain      └───────┬───────┘
                                           │ Collectors interface
                                           v
                                    ┌──────────────┐
                                    │  engine.DB   │
                                    └──────────────┘
```

## Usage

```go
import "github.com/Ari-Ghosh/flash-db/src/metrics"

exp := metrics.NewExporter(":9090")
exp.Register(db)
exp.Start()
defer exp.Stop()

// Metrics available at http://localhost:9090/metrics
```

## Exposed Metrics

| Metric | Type | Description |
|---|---|---|
| `flashdb_seq_num` | Gauge | Current committed sequence number |
| `flashdb_memtable_size_bytes` | Gauge | Active MemTable size in bytes |
| `flashdb_memtable_count` | Gauge | Number of keys in the active MemTable |
| `flashdb_l0_file_count` | Gauge | Number of L0 SSTable files on disk |
| `flashdb_bloom_total_queries` | Counter | Total bloom filter queries |
| `flashdb_bloom_false_positives` | Counter | Total bloom false positives |
| `flashdb_bloom_current_fpr` | Gauge | Adaptive FPR target for next SSTable |
| `flashdb_puts_total` | Counter | Total Put operations |
| `flashdb_deletes_total` | Counter | Total Delete operations |
| `flashdb_gets_total` | Counter | Total Get operations |
| `flashdb_compaction_l0_merges_total` | Counter | Total L0→L1 compaction merges |
| `flashdb_compaction_l1_merges_total` | Counter | Total L1→L2 compaction merges |
| `flashdb_wal_syncs_total` | Counter | Total WAL fsync calls |
| `flashdb_replication_follower_count` | Gauge | Connected followers (leader only) |
| `flashdb_replication_connected` | Gauge | 1 if connected to leader (follower only) |
| `flashdb_replication_last_applied_seq` | Gauge | Last applied seq (follower only) |

## Design Notes

- **Lock-free counters**: Put/Get/Delete operations increment `atomic.Uint64` fields — no mutex contention on the hot path.
- **Custom collector**: Metrics are generated on-demand via `prometheus.Collector.Collect()`, reading directly from the engine's atomic counters.
- **Compaction and WAL counters**: The compaction engine and WAL each track their own counters, exposed through the engine's `Collectors` interface.
