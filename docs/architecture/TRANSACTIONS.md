# Transactions & Concurrency

FlashDB provides ACID transactions using Optimistic Concurrency Control (OCC).

## Transactional API
```go
tx := db.Begin()
val, _ := tx.Get([]byte("k1"))
tx.Put([]byte("k1"), []byte("new_val"))
err := tx.Commit()
```

## Optimistic Concurrency Control (OCC)
OCC assumes that most transactions will not conflict with each other. It allows them to proceed without locks and only checks for conflicts at the very end.

### 1. Read Phase
When a transaction starts (`db.Begin()`):
- It takes a snapshot of the current global sequence number.
- It maintains a local **read-set** (keys it has read and their versions).
- It maintains a local **write-set** (mutations it intends to make).

### 2. Validation Phase
When `tx.Commit()` is called:
- FlashDB locks the write path.
- It checks every key in the **read-set**: if any key has been updated by another transaction (its current sequence number > baseline snapshot sequence), the transaction fails with `ErrTxnConflict`.

### 3. Commit Phase
If validation succeeds:
- All mutations in the **write-set** are assigned a new sequence number.
- They are written to the WAL as a single atomic batch.
- They are applied to the MemTable.
- The global sequence number is updated.

## Isolation Levels
FlashDB provides **Snapshot Isolation**. A transaction sees a consistent view of the database as of the moment it started. It is protected against:
- Dirty Reads
- Non-repeatable Reads
- Phantom Reads

## Conflict Handling
If a transaction fails due to a conflict, it is the responsibility of the application to retry the transaction.
