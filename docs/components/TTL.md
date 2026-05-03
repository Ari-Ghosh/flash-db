# Key TTL (Time-To-Live)

FlashDB supports per-key expiration, allowing data to be automatically deleted after a specified duration.

## Mechanism
- **Metadata Storage**: When a key is written with a TTL, an associated expiration deadline is stored in a reserved system keyspace (`\x00ttl\x00<key>`).
- **Lazy Expiry**: During a `Get` operation, FlashDB checks if the key has an associated TTL. If the deadline has passed, it returns `ErrKeyNotFound` and treats the key as if it were deleted.
- **Background Reaping**: A background "reaper" goroutine periodically scans the TTL metadata and issues formal `Delete` operations (tombstones) for expired keys to reclaim disk space.

## API
```go
// Write a key with a 1-hour TTL
err := db.PutWithTTL([]byte("session:123"), []byte("data"), time.Hour)

// Set or update an expiration time on an existing key
err := db.ExpireAt([]byte("my_key"), time.Now().Add(24 * time.Hour))

// Check remaining TTL for a key
remaining, err := db.TTLOf([]byte("session:123"))
```

## Performance
- **Write Path**: Adding a TTL involves one extra internal `Put` for the metadata.
- **Read Path**: `Get` lookups check the metadata cache/store, adding minimal overhead.
- **Compaction**: Expired keys are eventually purged from the B-tree during the normal compaction merge process.
