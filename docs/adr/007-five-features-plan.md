# ADR 007: Five-Phase Feature Plan (v5)

**Status:** Implemented
**Date:** June 2, 2026
**Authors:** CommandCodeBot

## Overview

Five features from the README checklist, implemented in order of dependency. Each builds on existing architecture without breaking changes.

---

## 1. Prometheus Metrics Exporter

**Goal**: Expose engine internals as Prometheus metrics via an HTTP `/metrics` endpoint.

**Design**: Add a new `src/metrics/metrics.go` package with an opt-in HTTP server. The DB struct gets a `Metrics()` method returning typed metrics counters/gauges. The metrics server is started optionally via config — not bundled into Open().

**New file**: `src/metrics/metrics.go`
- `Exporter` struct wrapping `prometheus.Registry` and an `http.Server`
- `NewExporter(addr string)` — creates the metrics server
- `Start()` / `Stop()` — lifecycle
- `Register(db *DB)` — walks the DB internals and registers gauges/counters
- Metrics to expose:
  - `flashdb_seq_num` (gauge) — current committed sequence number
  - `flashdb_memtable_size_bytes` (gauge) — current MemTable size
  - `flashdb_memtable_count` (gauge) — key count in active MemTable
  - `flashdb_l0_file_count` (gauge) — number of L0 SSTable files
  - `flashdb_bloom_total_queries` (counter) — total bloom filter queries
  - `flashdb_bloom_false_positives` (counter) — total false positives
  - `flashdb_bloom_current_fpr` (gauge) — adaptive target FPR
  - `flashdb_compaction_l0_merges` (counter) — L0→L1 compaction events
  - `flashdb_compaction_l1_merges` (counter) — L1→L2 compaction events
  - `flashdb_wal_syncs` (counter) — WAL fsync count
  - `flashdb_gets_total` (counter) — total Get calls
  - `flashdb_puts_total` (counter) — total Put calls
  - `flashdb_deletes_total` (counter) — total Delete calls
  - `flashdb_replication_follower_count` (gauge) — connected followers (leader only)
  - `flashdb_replication_connected` (gauge, 0/1) — follower connection status
  - `flashdb_replication_last_applied_seq` (gauge) — last applied seq (follower)

**Changes to `src/engine/engine.go`**:
- Add `metricsCollector` interface or concrete counters to DB struct (simple `atomic.Uint64` fields for getCount, putCount, deleteCount, walSyncCount)
- Increment counters in `Get`, `Put`, `Delete`, `ApplyTxn`
- Expose `CompactionStats()` via `compaction.Engine` (add a simple stats struct)
- Add `FollowerCount()` and `FollowerAddrs()` to `replication.Leader`

**Changes to `src/compaction/compaction.go`**:
- Add `Stats() CompactionStats` — returns `L0Merges` and `L1Merges` atomics
- Add `compaction.Stats` struct with `atomic.Uint64` counters

**Changes to `src/wal/wal.go`**:
- Add `SyncCount() uint64` — exposes internal sync counter

**Changes to `src/main.go`**:
- Demo: optionally start metrics server with `--metrics-addr` flag (demo-only, not required for the package)

**New dependency**: `github.com/prometheus/client_golang` added to `go.mod`

---

## 2. Streaming Compaction (Reduce Peak Memory)

**Goal**: Compaction currently materializes all L0 entries + L1 entries + combined result as full `[]types.Entry` slices, using 2-3x peak memory. Streaming via iterators reduces this to O(1) per-entry.

**Design**: Add a `BulkLoadFromIterator` method to B-tree and a streaming `mergeStream` function. The k-way merge already uses channels from `sstable.Reader.Iter()` — the problem is the final `mergeTwo` and `BulkLoad` both buffer everything.

**Changes to `src/btree/btree.go`**:
- Add `BulkLoadFromIter(iter types.Iterator) error` — consumes an iterator, builds leaf pages incrementally (flushes pages as they fill), then builds internal levels bottom-up. This avoids holding all entries in memory.
  - The iterator must yield entries in sorted key order with deduplication already applied (tombstones filtered, highest-seqNum kept). This is the caller's responsibility.
  - Peek-ahead: read one entry at a time, fill a leaf page buffer of up to 50 cells, flush the page, continue. Collect page IDs for internal level construction after all leaves are built.

**Changes to `src/compaction/compaction.go`**:
- Replace `kWayMerge` + `mergeTwo` pattern with a new `streamMerge(readers []*sstable.Reader, l1Tree *btree.BTree, oldestPinnedSeq uint64)` that:
  1. Opens channel-based iterators from all L0 readers (already exists via `Iter()`)
  2. Opens an iterator from L1 B-tree (already exists via `AllEntries` — but for streaming, use `NewIterator` which currently buffers everything; instead, add `StreamEntries(ctx) <-chan types.Entry` to btree)
  3. Feeds them into a k-way heap merge that emits fully deduplicated entries one at a time onto an output channel
  4. `BulkLoadFromIter` consumes the channel
- `compactL0`: use the streaming path instead of `kWayMerge` + `AllEntries` + `mergeTwo`
- `maybeCompactL1toL2`: use the streaming path instead of `AllEntries` + `mergeTwo`

**Changes to `src/btree/btree.go`** (additional):
- Add `StreamEntries(ctx context.Context) <-chan types.Entry` — walks leaves in order, yields entries via channel. Unlike `AllEntries`, doesn't buffer.
- Add `BulkLoadFromChannel(entries <-chan types.Entry) error` — alias to `BulkLoadFromIter` using a channel-to-iterator adapter

**New file**: `src/types/iterutil.go` (optional helper)
- `ChannelIter` — adapts `<-chan types.Entry` to `types.Iterator` interface

**Memory win**: From O(2-3x dataset size) to O(L0 reader count + tree depth), i.e. nearly constant.

---

## 3. Structured Query / Filter Pushdown

**Goal**: Push filter predicates into the storage layers so irrelevant entries are skipped at scan time rather than being materialized and discarded later.

**Design**: Add a `Filter func(*types.Entry) bool` to `IteratorOptions`. Each storage layer's iterator applies the filter before yielding entries. The B-tree iterator can additionally skip entire leaf pages when the filter's range can be inferred.

**Changes to `src/types/types.go`**:
- Add `Filter func(entry *Entry) bool` to `IteratorOptions`
- Add `FilterKeyBounds func(key []byte) (skipPage bool)` to `IteratorOptions` — optional optimization for B-tree page-level skipping

**Changes to `src/memtable/memtable.go`**:
- `NewIterator(opts)` — apply `opts.Filter` before yielding; skip entries that don't match

**Changes to `src/sstable/sstable.go`**:
- `NewIterator(opts)` — apply `opts.Filter` inside the block scan loop; skip non-matching entries
- `Iter()` (channel version) — does NOT apply filter (used by compaction, not queries). Keep as-is.

**Changes to `src/btree/btree.go`**:
- `NewIterator(opts)` — apply `opts.Filter` on individual entries; if `opts.FilterKeyBounds` is set, call it on leaf page boundary keys to skip entire pages
- Add a helper: when collecting leaf pages, if `opts.FilterKeyBounds` is non-nil and it returns true for a page's first key range, skip the entire page

**Changes to `src/engine/engine.go`**:
- Pass `opts.Filter` and `opts.FilterKeyBounds` through to all sub-iterators (MemTable, SSTable, B-tree) — they're already threaded through via `IteratorOptions`, no code change needed

**Note**: The merged iterator (`mergedIterator`) doesn't need changes — it works on already-filtered streams. Deduplication logic is unchanged.

---

## 4. Read-Your-Writes Consistency for Followers

**Goal**: A client writing to the leader can read its own write from a follower by waiting for the follower to catch up to the write's sequence number.

**Design**: The follower exposes a `WaitForSeq(seqNum uint64, timeout time.Duration) error` method that blocks until the follower's local `LastAppliedSeq()` >= `seqNum`. Uses a `sync.Cond` signaled after each `ApplyWALRecord`.

**Changes to `src/engine/engine.go`**:
- Add `WaitForSeq(seqNum uint64, timeout time.Duration) error` to DB — only valid when DB is running as a follower
- Store a `sync.Cond` in the DB struct (created only when `follower != nil`)
- In `ApplyWALRecord`, after applying, call `cond.Broadcast()`
- `WaitForSeq` implementation: wait on cond with timeout, re-check `LastAppliedSeq()`

**Changes to `src/replication/replication.go`**:
- Add `LastSeq() uint64` to `Follower` (already exists as `Follower.LastSeq()`)
- No changes needed to the replication package itself — the Applier interface is sufficient

**Demo change to `src/main.go`**:
- In `runReplicationDemo`, after writes, demonstrate `followerDB.WaitForSeq(lastWriteSeq, 1*time.Second)` and confirm reads are consistent

---

## 5. Distributed Query Fan-Out

**Goal**: The leader can distribute a query to all followers, collect results, de-duplicate, and return a merged result set to the caller.

**Design**: Add a bidirectional query RPC over the existing replication TCP connection. The leader sends a `QueryRequest` frame, each follower runs a local iterator and streams `QueryResponse` frames back. The leader merges results using the existing merged iterator pattern.

**Wire protocol extension** (new frame types in replication):
- Frame kind sentinel byte prepended to frames:
  - `0x01` = WAL record (existing)
  - `0x02` = QueryRequest
  - `0x03` = QueryResponse
  - `0x04` = QueryDone (end of results from this follower)
- `QueryRequest` frame: `[1=kind] [1=reverse] [8=snapSeq] [1=includeTombstones] [4=lowerLen] [lowerBytes] [4=upperLen] [upperBytes] [4=prefixLen] [prefixBytes]`
- `QueryResponse` frame: `[1=kind] [1=tombstone] [8=seqNum] [4=keyLen] [keyBytes] [4=valLen] [valBytes]`
- `QueryDone` frame: `[1=kind] [4=entryCount]`

**Changes to `src/replication/replication.go`**:
- Add `FanOutQuery(req QueryRequest) (*FanOutIterator, error)` to `Leader`
  - Sends QueryRequest to all connected followers concurrently
  - Creates a per-follower response channel
  - Returns a `FanOutIterator` that merges local + follower results
- Add query handler goroutine in `handleFollower`:
  - After the existing WAL streaming loop, add a select that listens for QueryRequest frames from the leader
  - On receipt, runs a local query via an `Applier.Query()` method, streams results back
- Extend `Applier` interface:
  - Add `ExecuteQuery(req QueryRequest) (types.Iterator, error)` — the follower runs this locally, then the replication layer serializes results into QueryResponse frames
- Refactor `handleFollower` to multiplex WAL shipping + query handling on the same connection:
  - Use a frame-type byte to distinguish WAL records from query frames
  - Keep a read deadline with keep-alive, same as today

**Changes to `src/engine/engine.go`**:
- Implement `ExecuteQuery(req QueryRequest) (types.Iterator, error)` — translates `QueryRequest` into `IteratorOptions`, calls `NewIterator`
- Add `FanOut(opts types.IteratorOptions) (types.Iterator, error)` — only on leader DB. Does local query + fan-out to all followers, returns a merged iterator.
  - Local iterator from `db.NewIterator(opts)`
  - `db.leader.FanOutQuery` for follower results
  - Merge all into a single `FanOutMergedIterator`

**New file**: `src/engine/fanout.go`
- `FanOutMergedIterator` — extends `mergedIterator` pattern for remote + local streams
- Holds a local iterator + per-follower iterators, uses the same k-way heap merge logic

**Changes to `src/types/types.go`**:
- No changes needed — query fan-out uses existing `IteratorOptions`

**New file**: `src/replication/query.go`
- `QueryRequest` / `QueryResponse` / `QueryDone` structs
- `encodeQueryRequest` / `decodeQueryRequest`
- `encodeQueryResponse` / `decodeQueryResponse`
- `encodeQueryDone` / `decodeQueryDone`
- Frame kind constants: `frameKindWAL = 0x01`, `frameKindQueryReq = 0x02`, `frameKindQueryResp = 0x03`, `frameKindQueryDone = 0x04`

**Changes to `src/replication/replication.go`** (frame multiplexing):
- In `handleFollower`, after authentication, read a 1-byte frame kind before the current CRC32 header
- If `frameKindWAL`: read/send as before (with the 1-byte prefix adjustment)
- If `frameKindQueryReq`: parse and execute query, stream results
- Backward compatibility: when connecting to an older leader/follower, the first byte mismatch may break. This is acceptable — both sides must be upgraded. Add a version byte to the handshake.

**Demo change to `src/main.go`**:
- In `runReplicationDemo`, after writes are replicated, demonstrate `FanOut` query across leader + follower

---

## Implementation Order

1. **Prometheus metrics** — standalone, no dependencies on other features
2. **Filter pushdown** — standalone, small change to existing iterator code
3. **Streaming compaction** — depends on filter pushdown (uses iterator pattern), but can be done independently; builds on existing compaction code
4. **Read-your-writes** — depends on replication layer, minimal change
5. **Query fan-out** — most complex, depends on the iterator/merged-iterator patterns being solid; builds on replication wire protocol

---

## Verification

### Prometheus metrics
- `go test ./src/metrics/...` — unit tests for metric registration
- Integration: start DB with metrics server, `curl localhost:9090/metrics`, verify gauges update after Put/Get/compaction

### Filter pushdown  
- `go test ./src/tests/...` — add `TestFilterPushdown` that creates an iterator with a filter, verifies only matching entries are returned
- Benchmark: compare scan with/without filter on a large DB

### Streaming compaction
- `go test ./src/compaction/...` — unit test for `streamMerge` vs `kWayMerge+mergeTwo`, verify identical output
- `go test ./src/btree/...` — unit test for `BulkLoadFromIter` with a mock iterator
- Integration: `TestTieredCompaction` in engine_test.go must still pass

### Read-your-writes
- `go test ./src/tests/...` — add `TestFollowerReadYourWrites`: write to leader, `WaitForSeq` on follower, verify key is readable
- Verify timeout behavior: WaitForSeq with a seq that will never arrive returns a timeout error

### Query fan-out
- `go test ./src/replication/...` — unit test: `FanOutQuery` sends correct frames, `ExecuteQuery` returns correct iterator
- Integration: `TestQueryFanOut` with leader + 2 followers, write different keys to each, fan-out query returns all keys

### Full suite
- `go test ./...` — all existing tests must pass
- `go vet ./...` — no new vet issues
