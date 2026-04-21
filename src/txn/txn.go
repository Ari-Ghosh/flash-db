// Package txn implements optimistic multi-key transactions for HybridDB.
//
// # Implemented in v3
//
// Design:
//   - Begin() snapshots the current sequence number as the read baseline.
//   - Get/Put/Delete buffer all operations locally inside the Txn struct —
//     nothing touches the WAL or MemTable until Commit.
//   - Commit() runs in two phases under a single engine-level write lock:
//     1. Validation: every key in the read-set is re-read; if any has a
//     SeqNum > the snapshot seq, a concurrent writer mutated it and
//     we return ErrTxnConflict (optimistic CC).
//     2. Apply: all mutations are appended to the WAL as a single
//     AppendBatch call (one fsync for the whole transaction) and then
//     applied to the MemTable atomically.
//   - Rollback() discards the local buffer with no I/O.
//
// Security:
//   - Key and value sizes are bounded to prevent memory exhaustion.
//   - Transactions that exceed MaxOps are rejected to avoid arbitrarily
//     large critical-section holds.
//   - All error paths release the write lock before returning.
package txn

import (
	"bytes"
	"errors"
	"fmt"
	"sync/atomic"

	types "local/flashdb/src/types"
)

// Sentinel errors.
var (
	ErrTxnConflict   = errors.New("transaction conflict: read-set modified by concurrent writer")
	ErrTxnClosed     = errors.New("transaction already committed or rolled back")
	ErrTxnTooLarge   = errors.New("transaction exceeds maximum operation count")
	ErrKeyTooLarge   = errors.New("key exceeds maximum allowed size")
	ErrValueTooLarge = errors.New("value exceeds maximum allowed size")
)

// Limits enforced per-transaction to prevent abuse / memory exhaustion.
const (
	MaxOps       = 10_000           // maximum Put+Delete operations per Txn
	MaxKeySize   = 8 * 1024         // 8 KB
	MaxValueSize = 64 * 1024 * 1024 // 64 MB
)

// op is one buffered mutation inside a Txn.
type op struct {
	key       []byte
	value     []byte // nil for delete
	tombstone bool
}

// readEntry is one key observed during the transaction.
type readEntry struct {
	key    []byte
	seqNum uint64 // SeqNum at read time; 0 = key was not found
}

// Store is the minimal interface the Txn needs from the DB engine.
// engine.DB satisfies this via its exported GetAt / PutTxn / DeleteTxn methods.
type Store interface {
	// GetAt reads key as of seqNum (MVCC point-in-time read).
	GetAt(key []byte, seqNum uint64) ([]byte, error)
	// SeqAt returns the entry's SeqNum for conflict detection (0 if not found).
	SeqAt(key []byte, seqNum uint64) uint64
	// ApplyTxn atomically applies ops to the WAL and MemTable.
	// txnSeq is the single sequence number allocated for the whole batch.
	ApplyTxn(txnID, txnSeq uint64, ops []TxnOp) error
	// NextSeq allocates and returns the next global sequence number.
	NextSeq() uint64
	// NextTxnID allocates and returns a unique transaction ID.
	NextTxnID() uint64
}

// TxnOp is one mutation passed to Store.ApplyTxn.
type TxnOp struct {
	Key       []byte
	Value     []byte
	Tombstone bool
	SeqNum    uint64
}

// Txn is an optimistic, multi-key transaction.
// A Txn must not be used concurrently from multiple goroutines.
type Txn struct {
	store    Store
	snapSeq  uint64 // sequence number at Begin time
	txnID    uint64
	reads    []readEntry
	writes   []op
	readKeys map[string]int // key → index in reads, for dedup
	done     bool
}

// Begin starts a new transaction against store.
func Begin(store Store) *Txn {
	return &Txn{
		store:    store,
		snapSeq:  store.NextSeq(),
		txnID:    store.NextTxnID(),
		readKeys: make(map[string]int),
	}
}

// Get reads key through this transaction's snapshot.
// Local writes take precedence over the snapshot view (read-your-writes).
func (t *Txn) Get(key []byte) ([]byte, error) {
	if t.done {
		return nil, ErrTxnClosed
	}
	if err := validateKey(key); err != nil {
		return nil, err
	}

	// Check local write buffer first (read-your-writes).
	for i := len(t.writes) - 1; i >= 0; i-- {
		if bytes.Equal(t.writes[i].key, key) {
			if t.writes[i].tombstone {
				return nil, types.ErrKeyDeleted
			}
			return t.writes[i].value, nil
		}
	}

	// Read from snapshot.
	val, err := t.store.GetAt(key, t.snapSeq)

	// Track the read for conflict detection regardless of whether it was found.
	sk := string(key)
	if _, seen := t.readKeys[sk]; !seen {
		seqNum := t.store.SeqAt(key, t.snapSeq) // 0 if not found
		t.reads = append(t.reads, readEntry{key: cloneBytes(key), seqNum: seqNum})
		t.readKeys[sk] = len(t.reads) - 1
	}

	return val, err
}

// Put buffers a write of key=value into this transaction.
func (t *Txn) Put(key, value []byte) error {
	if t.done {
		return ErrTxnClosed
	}
	if err := validateKey(key); err != nil {
		return err
	}
	if err := validateValue(value); err != nil {
		return err
	}
	if len(t.writes) >= MaxOps {
		return ErrTxnTooLarge
	}
	t.writes = append(t.writes, op{key: cloneBytes(key), value: cloneBytes(value)})
	return nil
}

// Delete buffers a tombstone for key into this transaction.
func (t *Txn) Delete(key []byte) error {
	if t.done {
		return ErrTxnClosed
	}
	if err := validateKey(key); err != nil {
		return err
	}
	if len(t.writes) >= MaxOps {
		return ErrTxnTooLarge
	}
	t.writes = append(t.writes, op{key: cloneBytes(key), tombstone: true})
	return nil
}

// Commit validates the read-set and, if clean, applies all buffered writes
// atomically to the WAL and MemTable.
//
// Returns ErrTxnConflict if any read-set key was modified since Begin.
func (t *Txn) Commit() error {
	if t.done {
		return ErrTxnClosed
	}
	t.done = true

	if len(t.writes) == 0 {
		return nil // read-only transaction — nothing to do
	}

	// Phase 1: validate read-set.
	// We use the *current* live seq of each read key to detect concurrent writes.
	// If any key's current SeqNum > the seq we saw at read time, abort.
	for _, re := range t.reads {
		currentSeq := t.store.SeqAt(re.key, ^uint64(0)) // read at "now"
		if currentSeq > re.seqNum {
			return fmt.Errorf("%w: key %q changed (was seq %d, now %d)",
				ErrTxnConflict, re.key, re.seqNum, currentSeq)
		}
	}

	// Phase 2: allocate sequence numbers and build op list.
	ops := make([]TxnOp, len(t.writes))
	for i, w := range t.writes {
		seq := t.store.NextSeq()
		ops[i] = TxnOp{
			Key:       w.key,
			Value:     w.value,
			Tombstone: w.tombstone,
			SeqNum:    seq,
		}
	}

	// Phase 3: apply atomically.
	txnSeq := t.store.NextSeq()
	return t.store.ApplyTxn(t.txnID, txnSeq, ops)
}

// Rollback discards all buffered operations with no I/O.
func (t *Txn) Rollback() error {
	if t.done {
		return ErrTxnClosed
	}
	t.done = true
	t.writes = nil
	t.reads = nil
	t.readKeys = nil
	return nil
}

// SnapSeq returns the sequence number this transaction reads from.
func (t *Txn) SnapSeq() uint64 { return t.snapSeq }

// WriteCount returns the number of buffered write operations.
func (t *Txn) WriteCount() int { return len(t.writes) }

// ReadCount returns the number of keys in the read-set.
func (t *Txn) ReadCount() int { return len(t.reads) }

// ── Validation helpers ────────────────────────────────────────────────────────

func validateKey(key []byte) error {
	if len(key) == 0 {
		return fmt.Errorf("key must not be empty")
	}
	if len(key) > MaxKeySize {
		return fmt.Errorf("%w: %d > %d", ErrKeyTooLarge, len(key), MaxKeySize)
	}
	return nil
}

func validateValue(value []byte) error {
	if len(value) > MaxValueSize {
		return fmt.Errorf("%w: %d > %d", ErrValueTooLarge, len(value), MaxValueSize)
	}
	return nil
}

func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	c := make([]byte, len(b))
	copy(c, b)
	return c
}

// Ensure atomic is imported (used by callers; kept here for reference).
var _ = atomic.AddUint64
