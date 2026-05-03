# ADR 006: Internal Key Namespacing with Reserved Prefix

## Status
Accepted

## Context
FlashDB needs to store internal metadata (Column Family names, TTL deadlines, Secondary Index entries) on disk alongside user data. We need a way to distinguish system-internal keys from user-provided keys to prevent collisions and ensure data integrity.

## Decision
We decided to reserve the null byte `\x00` as a prefix for all internal system keys.
- **User Keys**: Any key provided by the user is allowed, but the engine documentation advises against using `\x00`-prefixed keys for application data if they want to avoid confusion (though the engine handles isolation).
- **Internal Keys**:
  - `\x00cf\x00<name>\x00<user_key>`: Column Family data.
  - `\x00ttl\x00<user_key>`: TTL expiration timestamps.
  - `\x00idx\x00<name>\x00<idx_key>\x00<pk>`: Secondary Index entries.
  - `\x00cf_meta`: Persisted list of Column Family names.

## Consequences
- **Pros**:
  - Unified storage: system metadata is handled by the same WAL/SSTable/B-Tree pipeline as user data.
  - Guaranteed recovery: metadata is naturally replayed and recovered during WAL replay.
  - Simple isolation: prefix-based namespaces are easy to implement and debug.
- **Cons**:
  - Users cannot (or should not) use keys starting with `\x00` if they want to avoid potential conflicts with future system features (though current features use more specific multi-byte prefixes).
  - Internal keys are visible during full-table scans if the iterator options don't filter them out.
