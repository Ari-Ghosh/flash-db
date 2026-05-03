// Column-family support for flashDB.
//
// A column family (CF) is a named key namespace that is physically stored in the
// same underlying DB but logically isolated from other namespaces.  All keys
// belonging to a CF are prefixed with the CF's internal prefix so they never
// collide with each other or with user keys in the default namespace.
//
// Prefix format:  \x00cf\x00<name>\x00
//
// The leading \x00 byte is reserved for internal flashDB system keys.  User code
// should avoid writing keys that start with \x00; the engine does not enforce
// this restriction but behaviour would be undefined if user keys collided with
// the CF or TTL prefixes.
//
// Column family metadata (the list of created CFs) is stored under the system
// key cfMetaKey and survives WAL replay / engine restarts.
package engine

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	types "local/flashdb/src/types"
)

// System-reserved key prefixes. Keys starting with \x00 are internal to flashDB.
const (
	cfPrefixMark  = "\x00cf\x00"  // column-family key prefix
	ttlPrefixMark = "\x00ttl\x00" // TTL metadata key prefix
	idxPrefixMark = "\x00idx\x00" // secondary index key prefix
	cfMetaSysKey  = "\x00cf_meta" // where CF names are stored
)

// cfRegistry keeps track of all column families defined in a DB instance.
// It is populated at Open time from the persisted metadata and updated on
// CreateColumnFamily.
type cfRegistry struct {
	mu    sync.RWMutex
	names map[string]bool // set of live CF names
}

func newCFRegistry() *cfRegistry {
	return &cfRegistry{names: make(map[string]bool)}
}

// load reads the set of CF names from the DB (called during Open).
func (r *cfRegistry) load(db *DB) error {
	raw, err := db.getRaw([]byte(cfMetaSysKey))
	if err != nil {
		// Key doesn't exist yet – that's fine, no CFs defined yet.
		return nil
	}
	var names []string
	if err := json.Unmarshal(raw, &names); err != nil {
		return fmt.Errorf("cf registry decode: %w", err)
	}
	r.mu.Lock()
	for _, n := range names {
		r.names[n] = true
	}
	r.mu.Unlock()
	return nil
}

// persist writes the current CF set to the DB so it survives restarts.
func (r *cfRegistry) persist(db *DB) error {
	r.mu.RLock()
	names := make([]string, 0, len(r.names))
	for n := range r.names {
		names = append(names, n)
	}
	r.mu.RUnlock()

	raw, err := json.Marshal(names)
	if err != nil {
		return err
	}
	return db.putRaw([]byte(cfMetaSysKey), raw)
}

// CF is a handle to a named key namespace inside a DB.
type CF struct {
	db     *DB
	name   string
	prefix []byte // e.g. \x00cf\x00users\x00
}

// cfKey returns the DB key for a CF-scoped user key.
func (cf *CF) cfKey(key []byte) []byte {
	k := make([]byte, len(cf.prefix)+len(key))
	copy(k, cf.prefix)
	copy(k[len(cf.prefix):], key)
	return k
}

// ── DB methods for column families ───────────────────────────────────────────

// CreateColumnFamily creates a new named column family.  Calling it twice with
// the same name is idempotent.
func (db *DB) CreateColumnFamily(name string) error {
	if name == "" {
		return fmt.Errorf("column family name must not be empty")
	}
	if err := db.checkClosed(); err != nil {
		return err
	}
	db.cfReg.mu.Lock()
	db.cfReg.names[name] = true
	db.cfReg.mu.Unlock()
	return db.cfReg.persist(db)
}

// DropColumnFamily removes a column family and deletes all its keys.
// This is a heavyweight operation: it performs a prefix scan and deletes every
// key in the CF.  The CF is no longer accessible after this call.
func (db *DB) DropColumnFamily(name string) error {
	if err := db.checkClosed(); err != nil {
		return err
	}
	cf, err := db.GetColumnFamily(name)
	if err != nil {
		return err
	}

	// Delete all keys with the CF prefix.
	iter, err := db.NewIterator(types.IteratorOptions{
		Prefix:            cf.prefix,
		IncludeTombstones: false,
	})
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

	for _, k := range keys {
		if err := db.Delete(k); err != nil {
			return fmt.Errorf("cf drop delete %x: %w", k, err)
		}
	}

	db.cfReg.mu.Lock()
	delete(db.cfReg.names, name)
	db.cfReg.mu.Unlock()
	return db.cfReg.persist(db)
}

// GetColumnFamily returns a handle to an existing column family.
// Returns an error if the CF does not exist; call CreateColumnFamily first.
func (db *DB) GetColumnFamily(name string) (*CF, error) {
	if err := db.checkClosed(); err != nil {
		return nil, err
	}
	db.cfReg.mu.RLock()
	ok := db.cfReg.names[name]
	db.cfReg.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("column family %q does not exist", name)
	}
	return &CF{
		db:     db,
		name:   name,
		prefix: cfPrefix(name),
	}, nil
}

// ListColumnFamilies returns the names of all column families.
func (db *DB) ListColumnFamilies() []string {
	db.cfReg.mu.RLock()
	defer db.cfReg.mu.RUnlock()
	out := make([]string, 0, len(db.cfReg.names))
	for n := range db.cfReg.names {
		out = append(out, n)
	}
	return out
}

// cfPrefix builds the internal key prefix for a named CF.
func cfPrefix(name string) []byte {
	// \x00cf\x00<name>\x00
	p := make([]byte, len(cfPrefixMark)+len(name)+1)
	copy(p, cfPrefixMark)
	copy(p[len(cfPrefixMark):], name)
	p[len(p)-1] = 0x00
	return p
}

// ── CF operations ─────────────────────────────────────────────────────────────

// Put writes key=value in this column family's namespace.
func (cf *CF) Put(key, value []byte) error {
	return cf.db.Put(cf.cfKey(key), value)
}

// PutWithTTL writes key=value with a TTL in this column family's namespace.
func (cf *CF) PutWithTTL(key, value []byte, ttl time.Duration) error {
	return cf.db.PutWithTTL(cf.cfKey(key), value, ttl)
}

// Get returns the value for key in this column family.
func (cf *CF) Get(key []byte) ([]byte, error) {
	return cf.db.Get(cf.cfKey(key))
}

// Delete marks key as deleted in this column family.
func (cf *CF) Delete(key []byte) error {
	return cf.db.Delete(cf.cfKey(key))
}

// NewIterator returns a range iterator restricted to this column family.
// opts.LowerBound and opts.UpperBound are interpreted as CF-relative keys (i.e.
// without the CF prefix); the prefix is added internally.
func (cf *CF) NewIterator(opts types.IteratorOptions) (types.Iterator, error) {
	// Compute CF-absolute bounds.
	var lb, ub []byte
	if opts.LowerBound != nil {
		lb = cf.cfKey(opts.LowerBound)
	}
	if opts.UpperBound != nil {
		ub = cf.cfKey(opts.UpperBound)
	}

	// Always scan within the CF prefix range.
	cfUpper := prefixUpperBound(cf.prefix)

	if lb == nil || bytes.Compare(lb, cf.prefix) < 0 {
		lb = cf.prefix
	}
	if ub == nil || (cfUpper != nil && bytes.Compare(ub, cfUpper) > 0) {
		ub = cfUpper
	}

	innerOpts := opts
	innerOpts.LowerBound = lb
	innerOpts.UpperBound = ub
	innerOpts.Prefix = nil // we've already resolved bounds

	inner, err := cf.db.NewIterator(innerOpts)
	if err != nil {
		return nil, err
	}
	return &cfIterator{inner: inner, prefix: cf.prefix}, nil
}

// PrefixScan returns an iterator over all keys in this CF sharing prefix.
func (cf *CF) PrefixScan(prefix []byte) (types.Iterator, error) {
	fullPrefix := cf.cfKey(prefix)
	inner, err := cf.db.NewIterator(types.IteratorOptions{Prefix: fullPrefix})
	if err != nil {
		return nil, err
	}
	return &cfIterator{inner: inner, prefix: cf.prefix}, nil
}

// cfIterator wraps an inner iterator and strips the CF prefix from keys so
// callers see user-relative keys.
type cfIterator struct {
	inner  types.Iterator
	prefix []byte
}

func (it *cfIterator) Valid() bool { return it.inner.Valid() }
func (it *cfIterator) Next()       { it.inner.Next() }
func (it *cfIterator) Prev()       { it.inner.Prev() }
func (it *cfIterator) Key() []byte {
	k := it.inner.Key()
	if bytes.HasPrefix(k, it.prefix) {
		return k[len(it.prefix):]
	}
	return k
}
func (it *cfIterator) Value() []byte     { return it.inner.Value() }
func (it *cfIterator) SeqNum() uint64    { return it.inner.SeqNum() }
func (it *cfIterator) IsTombstone() bool { return it.inner.IsTombstone() }
func (it *cfIterator) Error() error      { return it.inner.Error() }
func (it *cfIterator) Close() error      { return it.inner.Close() }
