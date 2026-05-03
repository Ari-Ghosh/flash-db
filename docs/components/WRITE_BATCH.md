# WriteBatch

WriteBatch allows grouping multiple `Put` and `Delete` operations into a single atomic write. This is more efficient than individual writes when updating multiple keys simultaneously.

## Design
- **Atomicity**: Either all operations in the batch are committed, or none are.
- **Efficiency**: All operations are written to the WAL in a single record batch and synchronized to disk with one `fsync` call.
- **No Transaction Overhead**: Unlike full transactions, `WriteBatch` does not perform conflict detection (OCC). It simply applies the writes in order.

## API
```go
wb := db.NewWriteBatch()
wb.Put([]byte("key1"), []byte("value1"))
wb.Put([]byte("key2"), []byte("value2"))
wb.Delete([]byte("key3"))

// Commit the batch to the database
err := wb.Commit()
```

## Internal Workflow
1. **Buffer**: Mutations are stored in an in-memory slice within the `WriteBatch` struct.
2. **Assign Sequence**: Upon `Commit()`, a range of sequence numbers is reserved for the batch.
3. **WAL Write**: All mutations are marshaled into a single WAL record.
4. **MemTable Update**: The mutations are applied to the MemTable one by one within a single lock acquisition.

## Use Cases
- Atomic updates to related keys (e.g., metadata + data).
- Bulk loading data where full transactional isolation is not required.
- Maintaining secondary indexes manually.
