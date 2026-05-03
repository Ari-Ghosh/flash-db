# Hot Backup & Restore

FlashDB supports non-blocking, point-in-time backups that ensure data integrity.

## Backup Process
1. **Flush**: The current MemTable is flushed to an SSTable.
2. **Snapshot**: A global sequence number is "pinned" to define the backup's point-in-time.
3. **File Copy**: All active data files (B-trees, SSTables, WAL) are copied to the destination directory.
4. **Manifest**: A `manifest.json` file is written, containing metadata and SHA-256 checksums for every file in the backup.

## Integrity Verification
The backup is considered complete only if the `manifest.json` is present. During a restore, FlashDB:
1. Reads the manifest.
2. Calculates the SHA-256 checksum of every file in the source directory.
3. Aborts if any mismatch is found, ensuring corrupted backups are never restored.

## Atomicity
File copies use a "temp-then-rename" pattern to ensure that an interrupted backup doesn't leave partial files that look valid.
