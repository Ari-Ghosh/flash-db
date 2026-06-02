# Distributed Query Fan-Out

FlashDB supports distributing read queries from a leader to all connected followers, merging results into a single consistent iterator.

## Architecture

Fan-out uses the existing replication TCP connection for bidirectional query RPC. A 1-byte frame kind sentinel distinguishes WAL records from query frames, allowing safe multiplexing on a single TCP stream.

```
Client
  │ db.FanOut(opts)
  v
Leader Engine
  │
  ├── local query ────────────> Local scan
  │
  ├── FanOutQuery(req) ──────> Follower 1 ──> ExecuteQuery ──> results
  │                               │
  ├── FanOutQuery(req) ──────> Follower 2 ──> ExecuteQuery ──> results
  │                               │
  v                               v
Merged Iterator <────────── all results, deduplicated
```

## Wire Protocol (multiplexed on WAL TCP)

```
Frame kind byte:
  0x01 – WAL record (existing replication)
  0x02 – QueryRequest
  0x03 – QueryResponse
  0x04 – QueryDone
```

### QueryRequest (leader → follower)

| Field | Size | Description |
|---|---|---|
| kind | 1 byte | 0x02 |
| reverse | 1 byte | 1 if descending |
| snapSeq | 8 bytes | MVCC snapshot sequence number |
| includeTombstones | 1 byte | 1 to include deletion markers |
| lowerLen | 4 bytes | Length of lower bound key |
| lower | lowerLen bytes | Inclusive lower bound |
| upperLen | 4 bytes | Length of upper bound key |
| upper | upperLen bytes | Exclusive upper bound |
| prefixLen | 4 bytes | Length of prefix bytes |
| prefix | prefixLen bytes | Key prefix for prefix scans |

### QueryResponse (follower → leader)

| Field | Size | Description |
|---|---|---|
| kind | 1 byte | 0x03 |
| tombstone | 1 byte | 1 if this is a deletion marker |
| seqNum | 8 bytes | Entry sequence number |
| keyLen | 4 bytes | Key length |
| key | keyLen bytes | Entry key |
| valLen | 4 bytes | Value length |
| value | valLen bytes | Entry value |

### QueryDone (follower → leader)

| Field | Size | Description |
|---|---|---|
| kind | 1 byte | 0x04 |
| count | 4 bytes | Total result count from this follower |

## API

### Leader: `db.FanOut(opts types.IteratorOptions) (types.Iterator, error)`

Executes a local query and fans out to all connected followers. Returns a merged iterator with deduplicated results.

```go
iter, err := leaderDB.FanOut(types.IteratorOptions{
    Prefix: []byte("user:"),
})
defer iter.Close()

for iter.Valid() {
    fmt.Printf("%s: %s\n", iter.Key(), iter.Value())
    iter.Next()
}
```

### Follower: `ExecuteQuery` (via `AppendQueryApplier` interface)

The follower engine implements `AppendQueryApplier`, which includes `ExecuteQuery(req QueryRequest) (QueryResultIter, error)`. This is called by the replication layer when a query frame arrives.

## Concurrency

A per-connection `writeMu` serializes writes on the shared TCP connection so WAL streaming and query fan-out do not interleave.

## Read-Your-Writes Consistency

`db.WaitForSeq(seqNum uint64, timeout time.Duration) error` blocks until the local sequence number reaches `seqNum`. This lets a client confirm a write is visible on a follower before reading.

```go
leaderDB.Put([]byte("k"), []byte("v"))
seq := leaderDB.SeqNum()
followerDB.WaitForSeq(seq, 2*time.Second)
v, _ := followerDB.Get([]byte("k")) // guaranteed to see the write
```
