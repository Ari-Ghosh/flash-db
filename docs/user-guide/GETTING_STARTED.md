# Getting Started with FlashDB

FlashDB is an embedded key-value store, meaning it runs as a library within your Go application. There is no separate server to install or manage.

## Installation
Add FlashDB to your project using `go get`:

```bash
go get github.com/Ari-Ghosh/flash-db/src
```

## Basic Usage

### 1. Opening a Database
FlashDB stores all its data in a single directory. You must provide a configuration object to open it.

```go
package main

import (
	"log"
	"github.com/Ari-Ghosh/flash-db/src/engine"
)

func main() {
	cfg := engine.DefaultConfig("./data")
	db, err := engine.Open(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
}
```

### 2. Put and Get
Operations are byte-slice based.

```go
// Store a value
err := db.Put([]byte("my_key"), []byte("my_value"))

// Retrieve a value
val, err := db.Get([]byte("my_key"))
if err == nil {
    fmt.Printf("Got value: %s\n", string(val))
}
```

### 3. Deleting a Key
Deleting a key marks it as "deleted" using a tombstone. It will be permanently removed during the next compaction.

```go
err := db.Delete([]byte("my_key"))
```

### 4. Transactions
Group multiple operations into an atomic unit.

```go
tx := db.Begin()
tx.Put([]byte("a"), []byte("1"))
tx.Put([]byte("b"), []byte("2"))
if err := tx.Commit(); err != nil {
    // handle conflict or write error
}
```

### 5. Iteration
Scan a range of keys.

```go
iter, err := db.NewIterator(types.IteratorOptions{
    LowerBound: []byte("a"),
    UpperBound: []byte("z"),
})
defer iter.Close()

for iter.Valid() {
    fmt.Printf("%s: %s\n", iter.Key(), iter.Value())
    iter.Next()
}
```

### 6. Filtered Scans (v5)
Apply a predicate filter at scan time — non-matching entries are skipped in the storage layer.

```go
iter, err := db.NewIterator(types.IteratorOptions{
    LowerBound: []byte("user:"),
    UpperBound: []byte("user;"),  // ':' + 1
    Filter: func(e *types.Entry) bool {
        return !e.Tombstone && len(e.Value) > 0
    },
})
```

### 7. Fan-Out Queries (v5, leader only)
Distribute a read query to all followers and merge results.

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

### 8. Read-Your-Writes on Followers (v5)
Block until a follower has applied a write before reading.

```go
leaderDB.Put([]byte("key"), []byte("value"))
seq := leaderDB.SeqNum()
followerDB.WaitForSeq(seq, 2*time.Second)
v, _ := followerDB.Get([]byte("key")) // guaranteed consistent
```

### 9. Prometheus Metrics (v5)
Start the metrics exporter to monitor your FlashDB instance.

```go
import "github.com/Ari-Ghosh/flash-db/src/metrics"

exp := metrics.NewExporter(":9090")
exp.Register(db)
exp.Start()
defer exp.Stop()
// Metrics at http://localhost:9090/metrics
```
