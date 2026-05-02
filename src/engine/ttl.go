// TTL support for flashDB.
//
// Design
// ──────
// TTL metadata is stored as regular DB key-value pairs under a reserved prefix:
//
//	\x00ttl\x00<user-key>  →  8-byte big-endian int64 Unix nanosecond deadline
//
// Storing TTL inline in the data namespace avoids any special on-disk format
// and means TTL records survive WAL replay and compaction like normal data.
//
// On every Get:
//   - Look up the TTL record for the key.
//   - If the record exists and now > deadline, treat the key as deleted (return
//     ErrKeyNotFound) and schedule a lazy deletion in the background.
//
// On PutWithTTL:
//   - Write the value under the user key.
//   - Write the expiry under the TTL meta key.
//   Both writes share a single WAL batch (one fsync) via the WriteBatch path.
//
// On Delete:
//   - The normal Delete path issues a tombstone for the user key.  The TTL meta
//     record is cleaned up by the background TTL reaper.
//
// Background reaper:
//   - Wakes every reaperInterval (default 30 s).
//   - Scans the \x00ttl\x00 prefix, checks each expiry, and issues Delete for
//     any key that has passed its deadline.
//   - Keeps the expired-key footprint bounded.
package engine

import (
	"encoding/binary"
	"fmt"
	"log"
	"time"

	types "local/flashdb/src/types"
)

const (
	// reaperInterval controls how often the background TTL reaper wakes.
	reaperInterval = 30 * time.Second

	// ttlValueLen is the fixed size of the TTL metadata value (8-byte int64 nanos).
	ttlValueLen = 8
)

// ttlKey returns the internal metadata key for a user key.
func ttlKey(userKey []byte) []byte {
	k := make([]byte, len(ttlPrefixMark)+len(userKey))
	copy(k, ttlPrefixMark)
	copy(k[len(ttlPrefixMark):], userKey)
	return k
}

// encodeTTL serialises a deadline as big-endian int64 Unix nanoseconds.
func encodeTTL(deadline time.Time) []byte {
	buf := make([]byte, ttlValueLen)
	binary.BigEndian.PutUint64(buf, uint64(deadline.UnixNano())) //nolint:gosec
	return buf
}

// decodeTTL parses an 8-byte deadline value.
func decodeTTL(b []byte) time.Time {
	if len(b) < ttlValueLen {
		return time.Time{}
	}
	ns := int64(binary.BigEndian.Uint64(b)) //nolint:gosec
	return time.Unix(0, ns)
}

// ── DB TTL methods ────────────────────────────────────────────────────────────

// PutWithTTL writes key=value with a relative time-to-live.
// After ttl has elapsed, Get for this key returns ErrKeyNotFound.
func (db *DB) PutWithTTL(key, value []byte, ttl time.Duration) error {
	if ttl <= 0 {
		return fmt.Errorf("ttl must be positive")
	}
	return db.PutWithTTLNanos(key, value, int64(ttl))
}

// ExpireAt sets an absolute expiry deadline for an existing key.
// The key must already exist; this only sets the metadata.
func (db *DB) ExpireAt(key []byte, deadline time.Time) error {
	if deadline.IsZero() {
		return fmt.Errorf("deadline must not be zero")
	}
	// Persist the TTL metadata.
	return db.putRaw(ttlKey(key), encodeTTL(deadline))
}

// PutWithTTLNanos is the internal variant that accepts a TTL in nanoseconds.
func (db *DB) PutWithTTLNanos(key, value []byte, ttlNanos int64) error {
	if err := db.checkClosed(); err != nil {
		return err
	}
	deadline := time.Now().Add(time.Duration(ttlNanos))

	// Use a WriteBatch so both the data write and TTL write share one fsync.
	wb := db.NewWriteBatch()
	wb.Put(key, value)
	wb.putRaw(ttlKey(key), encodeTTL(deadline))
	return wb.Commit()
}

// TTLOf returns the remaining time-to-live for key.
// Returns (0, ErrKeyNotFound) if the key has no TTL or does not exist.
// Returns (negative duration, nil) if the key has already expired (but hasn't
// been reaped yet).
func (db *DB) TTLOf(key []byte) (time.Duration, error) {
	raw, err := db.getRaw(ttlKey(key))
	if err != nil {
		return 0, types.ErrKeyNotFound
	}
	deadline := decodeTTL(raw)
	return time.Until(deadline), nil
}

// checkExpiry looks up the TTL record for key and returns true if the key has
// expired.  It also schedules a background lazy deletion.
func (db *DB) checkExpiry(key []byte) bool {
	raw, err := db.getRaw(ttlKey(key))
	if err != nil {
		return false // no TTL record → not expired
	}
	deadline := decodeTTL(raw)
	if deadline.IsZero() {
		return false
	}
	if time.Now().Before(deadline) {
		return false
	}
	// Expired: schedule lazy cleanup (non-blocking).
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)
	go func() {
		_ = db.deleteWithTTLMeta(keyCopy)
	}()
	return true
}

// deleteWithTTLMeta deletes both the user key and its TTL metadata key.
func (db *DB) deleteWithTTLMeta(key []byte) error {
	wb := db.NewWriteBatch()
	wb.Delete(key)
	wb.deleteRaw(ttlKey(key))
	return wb.Commit()
}

// ── Background TTL reaper ─────────────────────────────────────────────────────

// startTTLReaper launches the background TTL reaper goroutine.
// It stops when db.closeCh is closed.
func (db *DB) startTTLReaper() {
	go func() {
		ticker := time.NewTicker(reaperInterval)
		defer ticker.Stop()
		for {
			select {
			case <-db.closeCh:
				return
			case <-ticker.C:
				db.reaperSweep()
			}
		}
	}()
}

// reaperSweep scans the TTL prefix and deletes all expired keys.
func (db *DB) reaperSweep() {
	prefix := []byte(ttlPrefixMark)
	iter, err := db.NewIterator(types.IteratorOptions{Prefix: prefix})
	if err != nil {
		return
	}

	type kv struct{ k, v []byte }
	var expired []kv
	for iter.Valid() {
		// The raw DB key is \x00ttl\x00<user-key>; extract <user-key>.
		rawKey := iter.Key()
		ttlMeta := iter.Value()
		if len(rawKey) <= len(prefix) {
			iter.Next()
			continue
		}
		// iter.Key() returns the full DB key including the prefix.
		userKey := rawKey[len(prefix):]
		deadline := decodeTTL(ttlMeta)
		if !deadline.IsZero() && time.Now().After(deadline) {
			uk := make([]byte, len(userKey))
			copy(uk, userKey)
			v := make([]byte, len(ttlMeta))
			copy(v, ttlMeta)
			expired = append(expired, kv{uk, v})
		}
		iter.Next()
	}
	iter.Close()

	for _, pair := range expired {
		if err := db.deleteWithTTLMeta(pair.k); err != nil {
			log.Printf("ttl reaper: delete %x: %v", pair.k, err)
		}
	}
	if len(expired) > 0 {
		log.Printf("ttl reaper: reaped %d expired keys", len(expired))
	}
}