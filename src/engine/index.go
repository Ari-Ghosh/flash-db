// Secondary index support for flashDB.
//
// Design
// ──────
// Indexes are maintained as regular DB entries under the reserved prefix:
//
//	\x00idx\x00<name>\x00<index-key>\x00<primary-key>  →  \x01
//
// This layout allows:
//   - Point lookup: scan all DB keys with prefix \x00idx\x00<name>\x00<idx-key>\x00
//   - Range query:  scan from \x00idx\x00<name>\x00<from>\x00 to
//     \x00idx\x00<name>\x00<to>\x00
//
// Index definitions
// ─────────────────
// An IndexDefinition supplies a KeyFn that extracts zero or more index keys
// from a (primaryKey, value) pair.  The function is in-memory only; the
// caller must re-register definitions after each engine restart.
//
// Maintaining correctness
// ───────────────────────
// PutIndexed / DeleteIndexed wrap the normal Put/Delete path:
//  1. Read the current value (if any) to discover existing index entries.
//  2. Issue deletes for old index entries.
//  3. Write the new value.
//  4. Issue puts for new index entries.
//
// All four operations are bundled in a single WriteBatch.
//
// This is a read-before-write, so indexed writes are slightly heavier than
// plain writes.  For write-only workloads, use Put directly and rebuild indexes
// offline.
package engine

import (
	"bytes"
	"fmt"
	"sync"

	types "local/flashdb/src/types"
)

// IndexDefinition describes one secondary index.
type IndexDefinition struct {
	// Name is a short identifier for this index (e.g. "by_email").
	// Must be non-empty and must not contain the null byte \x00.
	Name string

	// KeyFn extracts zero or more index-key byte slices from a primary key +
	// value pair.  Returning multiple index keys creates a multi-value index.
	// The function must be deterministic and must not retain references to
	// primaryKey or value after returning.
	KeyFn func(primaryKey, value []byte) [][]byte
}

// indexManager maintains the set of registered index definitions and provides
// helper methods for building and querying index entries.
type indexManager struct {
	mu      sync.RWMutex
	indexes map[string]IndexDefinition
}

func newIndexManager() *indexManager {
	return &indexManager{indexes: make(map[string]IndexDefinition)}
}

// ── DB index methods ──────────────────────────────────────────────────────────

// DefineIndex registers a secondary index.  If an index with the same name
// already exists its definition is replaced.
//
// Note: DefineIndex does not backfill existing data.  Call RebuildIndex after
// defining an index on a non-empty DB.
func (db *DB) DefineIndex(def IndexDefinition) error {
	if def.Name == "" {
		return fmt.Errorf("index name must not be empty")
	}
	if def.KeyFn == nil {
		return fmt.Errorf("index %q: KeyFn must not be nil", def.Name)
	}
	db.idxMgr.mu.Lock()
	db.idxMgr.indexes[def.Name] = def
	db.idxMgr.mu.Unlock()
	return nil
}

// DropIndex removes an index definition and deletes all its stored entries.
func (db *DB) DropIndex(name string) error {
	db.idxMgr.mu.Lock()
	delete(db.idxMgr.indexes, name)
	db.idxMgr.mu.Unlock()

	// Purge all index entries for this name from the DB.
	namePrefix := idxNamePrefix(name)
	iter, err := db.NewIterator(types.IteratorOptions{Prefix: namePrefix})
	if err != nil {
		return err
	}
	var keys [][]byte
	for iter.Valid() {
		k := make([]byte, len(iter.Key()))
		copy(k, iter.Key())
		keys = append(keys, k)
		iter.Next()
	}
	_ = iter.Close()

	wb := db.NewWriteBatch()
	for _, k := range keys {
		wb.deleteRaw(k)
	}
	return wb.Commit()
}

// PutIndexed writes key=value and maintains all registered secondary indexes.
// Use this instead of Put when you want index entries to be kept up to date.
func (db *DB) PutIndexed(key, value []byte) error {
	if err := db.checkClosed(); err != nil {
		return err
	}
	db.idxMgr.mu.RLock()
	idxs := make([]IndexDefinition, 0, len(db.idxMgr.indexes))
	for _, d := range db.idxMgr.indexes {
		idxs = append(idxs, d)
	}
	db.idxMgr.mu.RUnlock()

	if len(idxs) == 0 {
		return db.Put(key, value) // fast path: no indexes defined
	}

	wb := db.NewWriteBatch()

	// Remove old index entries (requires reading current value).
	if oldVal, err := db.Get(key); err == nil {
		for _, idx := range idxs {
			for _, ik := range idx.KeyFn(key, oldVal) {
				wb.deleteRaw(idxEntry(idx.Name, ik, key))
			}
		}
	}

	// Write new value.
	wb.Put(key, value)

	// Add new index entries.
	for _, idx := range idxs {
		for _, ik := range idx.KeyFn(key, value) {
			wb.putRaw(idxEntry(idx.Name, ik, key), []byte{1})
		}
	}

	return wb.Commit()
}

// DeleteIndexed deletes key and removes all its secondary index entries.
func (db *DB) DeleteIndexed(key []byte) error {
	if err := db.checkClosed(); err != nil {
		return err
	}
	db.idxMgr.mu.RLock()
	idxs := make([]IndexDefinition, 0, len(db.idxMgr.indexes))
	for _, d := range db.idxMgr.indexes {
		idxs = append(idxs, d)
	}
	db.idxMgr.mu.RUnlock()

	if len(idxs) == 0 {
		return db.Delete(key)
	}

	wb := db.NewWriteBatch()

	// Remove index entries.
	if oldVal, err := db.Get(key); err == nil {
		for _, idx := range idxs {
			for _, ik := range idx.KeyFn(key, oldVal) {
				wb.deleteRaw(idxEntry(idx.Name, ik, key))
			}
		}
	}

	wb.Delete(key)
	return wb.Commit()
}

// QueryByIndex returns all primary keys that match the given index key in the
// named index.  Returns an empty slice (not an error) when nothing matches.
func (db *DB) QueryByIndex(name string, indexKey []byte) ([][]byte, error) {
	if err := db.checkClosed(); err != nil {
		return nil, err
	}
	// Index entries for an exact match live under the prefix:
	//   \x00idx\x00<name>\x00<indexKey>\x00
	prefix := idxEntryPrefix(name, indexKey)
	iter, err := db.NewIterator(types.IteratorOptions{Prefix: prefix})
	if err != nil {
		return nil, err
	}
	defer func() { _ = iter.Close() }()

	var pkeys [][]byte
	for iter.Valid() {
		// Raw key: \x00idx\x00<name>\x00<indexKey>\x00<primaryKey>
		// We need to extract <primaryKey>.
		raw := iter.Key() // Key() returns the full DB key
		pk := extractPrimaryKey(name, indexKey, raw)
		if pk != nil {
			out := make([]byte, len(pk))
			copy(out, pk)
			pkeys = append(pkeys, out)
		}
		iter.Next()
	}
	return pkeys, nil
}

// RangeQueryByIndex returns all primary keys whose index key falls in [from, to).
// Both bounds are inclusive-lower / exclusive-upper, following the same
// convention as iterator bounds.
func (db *DB) RangeQueryByIndex(name string, from, to []byte) ([][]byte, error) {
	if err := db.checkClosed(); err != nil {
		return nil, err
	}
	lb := idxEntryPrefix(name, from)
	var ub []byte
	if to != nil {
		ub = idxEntryPrefix(name, to)
	} else {
		// No upper bound: scan the whole index.
		ub = prefixUpperBound(idxNamePrefix(name))
	}

	iter, err := db.NewIterator(types.IteratorOptions{
		LowerBound: lb,
		UpperBound: ub,
	})
	if err != nil {
		return nil, err
	}
	defer func() { _ = iter.Close() }()

	namePrefix := idxNamePrefix(name)
	var pkeys [][]byte
	for iter.Valid() {
		raw := iter.Key()
		if !bytes.HasPrefix(raw, namePrefix) {
			break
		}
		// Decode: strip namePrefix, then read indexKey length (terminated by \x00),
		// then the remainder is the primary key.
		after := raw[len(namePrefix):]
		sep := bytes.IndexByte(after, 0x00)
		if sep < 0 {
			iter.Next()
			continue
		}
		pk := after[sep+1:]
		out := make([]byte, len(pk))
		copy(out, pk)
		pkeys = append(pkeys, out)
		iter.Next()
	}
	return pkeys, nil
}

// RebuildIndex drops and recreates all entries for the named index by
// scanning every key in the DB.  Use after calling DefineIndex on an existing
// DB.  This is a full table scan and may be slow on large datasets.
func (db *DB) RebuildIndex(name string) error {
	db.idxMgr.mu.RLock()
	def, ok := db.idxMgr.indexes[name]
	db.idxMgr.mu.RUnlock()
	if !ok {
		return fmt.Errorf("index %q not defined", name)
	}

	// First, purge all existing entries for this index.
	if err := db.DropIndex(name); err != nil {
		return err
	}
	// Re-register the definition (DropIndex removed it).
	db.idxMgr.mu.Lock()
	db.idxMgr.indexes[name] = def
	db.idxMgr.mu.Unlock()

	// Scan all non-system keys.
	iter, err := db.NewIterator(types.IteratorOptions{})
	if err != nil {
		return err
	}

	const batchSize = 500
	wb := db.NewWriteBatch()
	n := 0
	for iter.Valid() {
		k := iter.Key()
		v := iter.Value()
		// Skip system keys.
		if len(k) > 0 && k[0] == 0x00 {
			iter.Next()
			continue
		}
		for _, ik := range def.KeyFn(k, v) {
			wb.putRaw(idxEntry(name, ik, k), []byte{1})
			n++
			if n >= batchSize {
				if err := wb.Commit(); err != nil {
					_ = iter.Close()
					return err
				}
				wb = db.NewWriteBatch()
				n = 0
			}
		}
		iter.Next()
	}
	_ = iter.Close()

	if n > 0 {
		return wb.Commit()
	}
	return nil
}

// ── Key encoding helpers ──────────────────────────────────────────────────────

// idxNamePrefix returns the prefix for all entries of a named index.
// Format: \x00idx\x00<name>\x00.
func idxNamePrefix(name string) []byte {
	prefix := []byte(idxPrefixMark + name + "\x00")
	return prefix
}

// idxEntryPrefix returns the prefix for all entries matching an index key.
// Format: \x00idx\x00<name>\x00<indexKey>\x00.
func idxEntryPrefix(name string, indexKey []byte) []byte {
	p := idxNamePrefix(name)
	result := make([]byte, len(p)+len(indexKey)+1)
	copy(result, p)
	copy(result[len(p):], indexKey)
	result[len(result)-1] = 0x00
	return result
}

// idxEntry returns the full DB key for one index entry.
// Format: \x00idx\x00<name>\x00<indexKey>\x00<primaryKey>.
func idxEntry(name string, indexKey, primaryKey []byte) []byte {
	prefix := idxEntryPrefix(name, indexKey)
	result := make([]byte, len(prefix)+len(primaryKey))
	copy(result, prefix)
	copy(result[len(prefix):], primaryKey)
	return result
}

// extractPrimaryKey recovers the primary key from a raw index entry DB key.
func extractPrimaryKey(name string, indexKey, rawKey []byte) []byte {
	prefix := idxEntryPrefix(name, indexKey)
	if len(rawKey) <= len(prefix) {
		return nil
	}
	if !bytes.HasPrefix(rawKey, prefix) {
		return nil
	}
	return rawKey[len(prefix):]
}
