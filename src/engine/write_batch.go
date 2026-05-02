// WriteBatch provides an atomic multi-key write primitive used internally by
// the TTL and secondary-index code.  Unlike txn.Txn (which also performs
// read-set conflict detection), WriteBatch is a pure write path: it just
// accumulates (key, value) pairs and commits them in a single WAL batch +
// MemTable update.
//
// This file is internal to the engine package.
package engine

import (
	"fmt"

	"local/flashdb/src/wal"
)

// wbOp is one buffered operation inside a WriteBatch.
type wbOp struct {
	key       []byte
	value     []byte
	tombstone bool
}

// WriteBatch accumulates a set of writes that will be applied atomically.
type WriteBatch struct {
	db  *DB
	ops []wbOp
}

// NewWriteBatch returns an empty WriteBatch attached to db.
func (db *DB) NewWriteBatch() *WriteBatch {
	return &WriteBatch{db: db}
}

// Put enqueues a user-visible key=value write.
func (wb *WriteBatch) Put(key, value []byte) {
	k := make([]byte, len(key))
	copy(k, key)
	v := make([]byte, len(value))
	copy(v, value)
	wb.ops = append(wb.ops, wbOp{key: k, value: v})
}

// Delete enqueues a user-visible key deletion.
func (wb *WriteBatch) Delete(key []byte) {
	k := make([]byte, len(key))
	copy(k, key)
	wb.ops = append(wb.ops, wbOp{key: k, tombstone: true})
}

// putRaw enqueues a raw (internal) key=value write.
// raw keys may start with \x00 and are not subject to user-key validation.
func (wb *WriteBatch) putRaw(key, value []byte) {
	k := make([]byte, len(key))
	copy(k, key)
	v := make([]byte, len(value))
	copy(v, value)
	wb.ops = append(wb.ops, wbOp{key: k, value: v})
}

// deleteRaw enqueues a raw (internal) key deletion.
func (wb *WriteBatch) deleteRaw(key []byte) {
	k := make([]byte, len(key))
	copy(k, key)
	wb.ops = append(wb.ops, wbOp{key: k, tombstone: true})
}

// Commit atomically writes all buffered operations to the WAL and MemTable.
// The WriteBatch must not be used after Commit returns.
func (wb *WriteBatch) Commit() error {
	if len(wb.ops) == 0 {
		return nil
	}
	if err := wb.db.checkClosed(); err != nil {
		return err
	}

	// Allocate sequence numbers and build WAL batch.
	batch := make([]wal.BatchRecord, 0, len(wb.ops))
	type opWithSeq struct {
		op  wbOp
		seq uint64
	}
	opsWithSeq := make([]opWithSeq, 0, len(wb.ops))

	for _, op := range wb.ops {
		seq := wb.db.seq.Add(1)
		kind := wal.KindPut
		if op.tombstone {
			kind = wal.KindDelete
		}
		batch = append(batch, wal.BatchRecord{
			Kind:   kind,
			SeqNum: seq,
			Key:    op.key,
			Value:  op.value,
		})
		opsWithSeq = append(opsWithSeq, opWithSeq{op: op, seq: seq})
	}

	wb.db.writeMu.Lock()
	defer wb.db.writeMu.Unlock()

	// WAL (one fsync for the whole batch).
	if err := wb.db.walActive.AppendBatch(batch); err != nil {
		return fmt.Errorf("writebatch wal: %w", err)
	}

	// MemTable.
	for _, os := range opsWithSeq {
		if os.op.tombstone {
			_ = wb.db.memtable.Delete(os.op.key, os.seq)
		} else {
			_ = wb.db.memtable.Put(os.op.key, os.op.value, os.seq)
		}
	}

	if wb.db.memtable.IsFull() {
		wb.db.writeMu.Unlock()
		err := wb.db.triggerFlush()
		wb.db.writeMu.Lock()
		return err
	}
	return nil
}
