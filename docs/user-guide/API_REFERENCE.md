# API Reference

## Engine
The main entry point for interacting with FlashDB.

### `engine.Open(cfg Config) (*DB, error)`
Opens or creates a FlashDB instance at the directory specified in the config.

### `db.Put(key, value []byte) error`
Writes a key-value pair to the database.

### `db.Get(key []byte) ([]byte, error)`
Reads the latest value for a key. Returns `types.ErrKeyNotFound` if the key doesn't exist or `types.ErrKeyDeleted` if it has a tombstone.

### `db.Delete(key []byte) error`
Marks a key as deleted.

### `db.NewSnapshot() *Snapshot`
Creates a consistent, point-in-time view of the database.

### `db.NewIterator(opts types.IteratorOptions) (types.Iterator, error)`
Returns an iterator for scanning keys. Supports lower/upper bounds, prefix scans, reverse iteration, snapshot filtering, and optional predicate filtering via `Filter`.

### `db.Begin() *txn.Txn`
Starts a new multi-key transaction.

### `db.Backup(destDir string) (*backup.Manifest, error)`
Performs a hot backup of the entire database.

### `db.SeqNum() uint64`
Returns the current committed sequence number.

### `db.WaitForSeq(seqNum uint64, timeout time.Duration) error`
Blocks until the local sequence number reaches `seqNum`. Useful on followers for read-your-writes consistency. Returns immediately on leaders/standalone nodes.

### `db.FanOut(opts types.IteratorOptions) (types.Iterator, error)`
Executes a local query and fans out to all connected followers, returning a merged iterator of deduplicated results. Only valid on leader nodes.

### `db.Stats() Stats`
Returns engine statistics (memtable size, L0 file count, sequence number).

### `db.BloomStats() BloomFilterStats`
Returns aggregate bloom filter telemetry and the adaptive FPR target.

### `db.Err() error`
Returns the first background error (flush/compaction), if any.

---

## IteratorOptions

| Field | Type | Description |
|---|---|---|
| `LowerBound` | `[]byte` | Inclusive lower key bound |
| `UpperBound` | `[]byte` | Exclusive upper key bound |
| `Prefix` | `[]byte` | Shorthand for prefix scan (auto-derives bounds) |
| `Reverse` | `bool` | Iterate in descending key order |
| `SnapshotSeq` | `uint64` | MVCC snapshot sequence number |
| `IncludeTombstones` | `bool` | Expose deletion markers |
| `Filter` | `func(*Entry) bool` | Predicate applied at scan time (v5) |

---

## Transactions
Returned by `db.Begin()`.

### `tx.Put(key, value []byte) error`
Buffers a write operation.

### `tx.Get(key []byte) ([]byte, error)`
Reads a value within the transaction context (includes previous writes in the same transaction).

### `tx.Delete(key []byte) error`
Buffers a delete operation.

### `tx.Commit() error`
Attempts to apply all buffered operations atomically.

---

## Iterators
Returned by `db.NewIterator()`.

### `iter.Valid() bool`
Returns true if the iterator is positioned at a valid entry.

### `iter.Next()`
Advances the iterator to the next entry.

### `iter.Prev()`
Moves the iterator to the previous entry.

### `iter.Key() []byte`
Returns the key at the current position.

### `iter.Value() []byte`
Returns the value at the current position.

### `iter.Close() error`
Releases any resources held by the iterator.

---

## Metrics Exporter

```go
import "github.com/Ari-Ghosh/flash-db/src/metrics"

exp := metrics.NewExporter(":9090")
exp.Register(db)
exp.Start()
defer exp.Stop()
```

Exposes 15 metrics at `http://localhost:9090/metrics` including seq num, memtable stats, bloom filter telemetry, operation counters, compaction counters, WAL syncs, and replication status.
