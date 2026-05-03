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
Returns an iterator for scanning keys.

### `db.Begin() *txn.Txn`
Starts a new multi-key transaction.

### `db.Backup(destDir string) (*backup.Manifest, error)`
Performs a hot backup of the entire database.

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
