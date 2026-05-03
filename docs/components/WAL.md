# Write-Ahead Log (WAL)

The WAL ensures durability by recording all mutations to disk before they are applied to the in-memory MemTable.

## Mechanism
- **Append-Only**: All writes are appended to the end of the log file.
- **Checksums**: Each record includes a CRC32 checksum to detect data corruption.
- **Group-Commit**: Concurrent writes are batched together and synchronized to disk in a single `fsync` call to maximize throughput.

## Record Format
```
[checksum (4)] [length (4)] [payload]
```
The payload contains the record kind (Put, Delete, Txn markers), sequence number, and key/value data.

## Durability Policies
- `SyncAlways`: `fsync` after every record (safe but slow).
- `SyncBatch`: Group-commit with a configurable interval (default, recommended).
- `SyncNone`: No explicit `fsync` (fastest, but risk of data loss on power failure).

## Crash Recovery
During startup, FlashDB reads the WAL from start to finish:
1. Validates checksums.
2. Replays valid records into the MemTable.
3. Discards truncated or corrupted records at the end of the log.
4. If a transaction marker is found without a corresponding commit, those records are ignored.
