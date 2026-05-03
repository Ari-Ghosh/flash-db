# Secondary Indexes

Secondary indexes allow for querying data using keys other than the primary key.

## Design
- **Key-Value Based**: Index entries are stored as regular key-value pairs in the database with a reserved `\x00idx\x00` prefix.
- **Format**: `\x00idx\x00<index_name>\x00<index_key>\x00<primary_key> -> \x01`
- **Maintenance**: Indexes can be maintained automatically during `PutIndexed` or `DeleteIndexed` operations.

## Key Features
- **Point Lookups**: Find all primary keys associated with a specific index key.
- **Range Queries**: Efficiently scan a range of index keys.
- **Backfilling**: Existing data can be indexed using the `RebuildIndex` operation.
- **User-Defined**: Users provide a `KeyFn` that extracts index keys from primary key-value pairs.

## Implementation Details
- **Read-Before-Write**: Maintaining indexes during a `Put` requires reading the old value to remove stale index entries. This makes indexed writes heavier than plain writes.
- **Batched Updates**: Index updates are performed within the same `WriteBatch` as the data update to ensure atomicity.
