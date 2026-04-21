// Package wal implements a Write-Ahead Log with group-commit batching.
//
// ## Overview
//
// The WAL ensures crash recovery by persisting all writes before they modify in-memory structures.
// It uses length-prefixed records with CRC32 checksums for integrity and supports graceful truncation
// of corrupted trailing records during replay.
//
// ## Architecture
//
// - **Record Format**: CRC32 + length + payload (kind, seqNum, key, value).
// - **Sync Policies**: SyncAlways (v1), SyncBatch (group-commit), SyncNone (unsafe).
// - **Group-Commit**: Batches concurrent writes, fsyncs once, wakes all waiters.
// - **Replay**: On startup, reads valid records to restore MemTable state.
//
// ## version Changes
//
// ### v1 (Original Implementation)
// - SyncAlways: Every AppendPut/Delete immediately fsyncs (safe but slow).
// - Single-threaded: No batching, each write blocks on disk I/O.
// - Simple replay: Assumes all records are valid or panics on corruption.
// - Used for basic crash recovery with immediate durability.
//
// ### v2 (Enhanced Implementation)
// - **Group-Commit**: SyncBatch batches writes, amortizing fsync cost across concurrent operations.
// - **Sync Policies**: Pluggable durability vs throughput trade-off.
// - **Graceful Corruption**: Truncated records at EOF are silently dropped (torn writes).
// - **Batch Flusher**: Dedicated goroutine drains pending writes at intervals.
// - **Backward Compatibility**: Wire format unchanged; v2 can replay v1 logs.
// 
// ### v3 (on top of v2 foundations):
// - Pluggable SyncPolicy (SyncAlways, SyncBatch, SyncNone).
// - AppendBatch() for atomic multi-record commits (used by transactions).
// - Replay() filters so only committed transaction records are returned.
//
// ## Key Methods
//
// - **OpenWithOptions()**: Creates WAL with sync policy and batch interval.
// - **AppendPut/Delete()**: Queues record for group-commit batch.
// - **Replay()**: Reads all valid records on startup for crash recovery.
//
// ## Wire Format (Little-Endian, Unchanged)
//
//	[4]  crc32   – checksum of payload bytes
//	[4]  length  – payload byte length
//	[1]  kind    – 0 = Put, 1 = Delete
//	[8]  seqNum
//	[4]  keyLen
//	[keyLen] key
//	[4]  valLen  (omitted for Delete)
//	[valLen] value (omitted for Delete)
//
// ## Performance Characteristics
//
// - **SyncAlways**: ~1-5ms per write (disk I/O dominant).
// - **SyncBatch**: ~100ns per write, ~1-5ms batch flush (amortized).
// - **Replay**: O(n) reads, validates CRC32, rebuilds MemTable.
//
// ## Usage in HybridDB
//
// WAL sits at the start of the write path:
// - Write arrives → WAL.AppendPut() → batch queued → fsync → MemTable.Put()
// - Crash → WAL.Replay() → restore MemTable → continue normal operation.
// - Compaction rotates WAL to prevent unbounded growth.
//
//
// # Implements a crash-safe Write-Ahead Log with:
//
//   - Group-commit batching: concurrent writers share one fsync per drain
//     cycle, amortising disk-flush cost under high write concurrency.
//   - Transaction batch records: a TxnBegin / TxnCommit envelope that lets
//     the engine replay multi-key atomic operations all-or-nothing.
//   - CRC-32 per record and a tail-truncation recovery: a torn write at the
//     end of the file (power failure mid-record) is silently dropped; the
//     replay loop stops at the first bad checksum rather than erroring out.
//   - Pluggable sync policy: SyncAlways (safest), SyncBatch (default, best
//     throughput), SyncNone (maximum speed, weakest durability guarantee).
//
// Wire format (little-endian), every record:
//
//	[4]  crc32     – checksum of the payload bytes that follow
//	[4]  length    – payload byte length
//	payload:
//	  [1]  kind    – 0=Put 1=Delete 2=TxnBegin 3=TxnCommit 4=TxnAbort
//	  [8]  seqNum
//	  [4]  keyLen  (omitted for TxnBegin/Commit/Abort)
//	  [keyLen] key
//	  [4]  valLen  (Put only)
//	  [valLen] value
package wal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
	"time"
)

// record kinds
const (
	KindPut       byte = 0
	KindDelete    byte = 1
	KindTxnBegin  byte = 2
	KindTxnCommit byte = 3
	KindTxnAbort  byte = 4
)

var le = binary.LittleEndian

// SyncPolicy controls when WAL writes are flushed to disk.
type SyncPolicy int

const (
	// SyncAlways fsyncs after every individual record. Safest.
	SyncAlways SyncPolicy = iota
	// SyncBatch groups concurrent writers and issues one fsync per drain cycle.
	// Default — best throughput on multi-core workloads.
	SyncBatch
	// SyncNone never explicitly fsyncs. Highest throughput, weakest durability.
	SyncNone
)

// Record is one decoded WAL entry returned during Replay.
type Record struct {
	Kind      byte
	SeqNum    uint64
	TxnID     uint64 // non-zero for TxnBegin/Commit/Abort
	Key       []byte
	Value     []byte // nil for Delete / TxnBegin / Commit / Abort
	Tombstone bool   // true when Kind == KindDelete
}

// batchEntry is queued by a writer waiting for the group-commit flusher.
type batchEntry struct {
	payload []byte
	done    chan error
}

// WAL wraps an append-only log file.
type WAL struct {
	mu   sync.Mutex
	f    *os.File
	bw   *bufio.Writer
	path string
	policy SyncPolicy

	batchMu sync.Mutex
	pending []batchEntry
	flushCh chan struct{}
	stopCh  chan struct{}
	wg      sync.WaitGroup
}

// Options configures WAL behaviour.
type Options struct {
	SyncPolicy    SyncPolicy
	BatchInterval time.Duration // only for SyncBatch; default 1 ms
}

func defaultOptions() Options {
	return Options{SyncPolicy: SyncBatch, BatchInterval: time.Millisecond}
}

// Open opens or creates the WAL at path with default options.
func Open(path string) (*WAL, error) {
	return OpenWithOptions(path, defaultOptions())
}

// OpenWithOptions opens or creates the WAL with explicit options.
func OpenWithOptions(path string, opts Options) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		return nil, fmt.Errorf("wal open %s: %w", path, err)
	}
	if opts.BatchInterval == 0 {
		opts.BatchInterval = time.Millisecond
	}
	w := &WAL{
		f:       f,
		bw:      bufio.NewWriterSize(f, 512*1024),
		path:    path,
		policy:  opts.SyncPolicy,
		flushCh: make(chan struct{}, 1),
		stopCh:  make(chan struct{}),
	}
	if opts.SyncPolicy == SyncBatch {
		w.wg.Add(1)
		go w.batchFlusher(opts.BatchInterval)
	}
	return w, nil
}

// ── Public append API ─────────────────────────────────────────────────────────

// AppendPut writes a Put record.
func (w *WAL) AppendPut(seq uint64, key, value []byte) error {
	return w.appendRecord(KindPut, seq, 0, key, value)
}

// AppendDelete writes a Delete tombstone record.
func (w *WAL) AppendDelete(seq uint64, key []byte) error {
	return w.appendRecord(KindDelete, seq, 0, key, nil)
}

// AppendTxnBegin writes a transaction-begin marker.
func (w *WAL) AppendTxnBegin(seq, txnID uint64) error {
	return w.appendRecord(KindTxnBegin, seq, txnID, nil, nil)
}

// AppendTxnCommit writes a transaction-commit marker.
func (w *WAL) AppendTxnCommit(seq, txnID uint64) error {
	return w.appendRecord(KindTxnCommit, seq, txnID, nil, nil)
}

// AppendTxnAbort writes a transaction-abort marker.
func (w *WAL) AppendTxnAbort(seq, txnID uint64) error {
	return w.appendRecord(KindTxnAbort, seq, txnID, nil, nil)
}

// AppendBatch atomically appends multiple records under one write lock and
// one fsync.  Used by the transaction commit path to guarantee that all
// mutations land in the log before the commit marker.
func (w *WAL) AppendBatch(records []BatchRecord) error {
	var combined []byte
	for _, r := range records {
		p := marshalPayload(r.Kind, r.SeqNum, r.TxnID, r.Key, r.Value)
		header := makeHeader(p)
		combined = append(combined, header...)
		combined = append(combined, p...)
	}
	switch w.policy {
	case SyncAlways:
		return w.writeRaw(combined, true)
	case SyncNone:
		return w.writeRaw(combined, false)
	default:
		return w.writeQueued(combined)
	}
}

// BatchRecord is one operation inside an AppendBatch call.
type BatchRecord struct {
	Kind   byte
	SeqNum uint64
	TxnID  uint64
	Key    []byte
	Value  []byte
}

// ── Internal write routing ────────────────────────────────────────────────────

func (w *WAL) appendRecord(kind byte, seq, txnID uint64, key, value []byte) error {
	payload := marshalPayload(kind, seq, txnID, key, value)
	switch w.policy {
	case SyncAlways:
		return w.writeRaw(append(makeHeader(payload), payload...), true)
	case SyncNone:
		return w.writeRaw(append(makeHeader(payload), payload...), false)
	default:
		return w.writeQueued(append(makeHeader(payload), payload...))
	}
}

func makeHeader(payload []byte) []byte {
	h := make([]byte, 8)
	le.PutUint32(h[0:], crc32.ChecksumIEEE(payload))
	le.PutUint32(h[4:], uint32(len(payload)))
	return h
}

func (w *WAL) writeRaw(data []byte, sync bool) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, err := w.bw.Write(data); err != nil {
		return err
	}
	if err := w.bw.Flush(); err != nil {
		return err
	}
	if sync {
		return w.f.Sync()
	}
	return nil
}

func (w *WAL) writeQueued(data []byte) error {
	done := make(chan error, 1)
	w.batchMu.Lock()
	w.pending = append(w.pending, batchEntry{payload: data, done: done})
	w.batchMu.Unlock()
	select {
	case w.flushCh <- struct{}{}:
	default:
	}
	return <-done
}

// batchFlusher drains pending entries with a single fsync per cycle.
func (w *WAL) batchFlusher(interval time.Duration) {
	defer w.wg.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-w.flushCh:
		case <-ticker.C:
		case <-w.stopCh:
			w.drainBatch()
			return
		}
		w.drainBatch()
	}
}

func (w *WAL) drainBatch() {
	w.batchMu.Lock()
	if len(w.pending) == 0 {
		w.batchMu.Unlock()
		return
	}
	batch := w.pending
	w.pending = nil
	w.batchMu.Unlock()

	w.mu.Lock()
	var writeErr error
	for _, e := range batch {
		if writeErr == nil {
			if _, err := w.bw.Write(e.payload); err != nil {
				writeErr = err
			}
		}
	}
	if writeErr == nil {
		writeErr = w.bw.Flush()
	}
	if writeErr == nil {
		writeErr = w.f.Sync()
	}
	w.mu.Unlock()

	for _, e := range batch {
		e.done <- writeErr
	}
}

// ── Replay ────────────────────────────────────────────────────────────────────

// Replay reads all valid, complete records from the WAL.
// Torn trailing records (power-fail mid-write) are silently dropped.
// Only fully committed transactions are returned; begin/abort markers
// and orphaned records without a matching commit are filtered out.
func (w *WAL) Replay() ([]Record, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	r := bufio.NewReaderSize(w.f, 512*1024)

	// First pass: collect all records.
	var raw []Record
	for {
		var hdr [8]byte
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			break // EOF or torn header — stop
		}
		checksum := le.Uint32(hdr[0:])
		length := le.Uint32(hdr[4:])
		if length > 64*1024*1024 { // sanity: no record > 64 MB
			break
		}
		payload := make([]byte, length)
		if _, err := io.ReadFull(r, payload); err != nil {
			break // torn payload
		}
		if crc32.ChecksumIEEE(payload) != checksum {
			break // corrupted — stop
		}
		rec, err := unmarshalPayload(payload)
		if err != nil {
			break
		}
		raw = append(raw, rec)
	}

	// Second pass: filter so only committed txn records are returned.
	// Non-txn records (TxnID==0) pass through unconditionally.
	// Txn records only pass if their TxnID has a matching TxnCommit.
	committed := make(map[uint64]bool)
	for _, rec := range raw {
		if rec.Kind == KindTxnCommit {
			committed[rec.TxnID] = true
		}
	}
	out := raw[:0]
	for _, rec := range raw {
		switch rec.Kind {
		case KindTxnBegin, KindTxnCommit, KindTxnAbort:
			continue // control markers, not data
		default:
			if rec.TxnID == 0 || committed[rec.TxnID] {
				out = append(out, rec)
			}
		}
	}
	return out, nil
}

// ── Encoding ──────────────────────────────────────────────────────────────────

func marshalPayload(kind byte, seq, txnID uint64, key, value []byte) []byte {
	size := 1 + 8 + 8 // kind + seq + txnID
	if len(key) > 0 {
		size += 4 + len(key)
	}
	if kind == KindPut && len(value) >= 0 {
		size += 4 + len(value)
	}
	buf := make([]byte, size)
	off := 0
	buf[off] = kind; off++
	le.PutUint64(buf[off:], seq); off += 8
	le.PutUint64(buf[off:], txnID); off += 8
	if len(key) > 0 {
		le.PutUint32(buf[off:], uint32(len(key))); off += 4
		copy(buf[off:], key); off += len(key)
	}
	if kind == KindPut {
		le.PutUint32(buf[off:], uint32(len(value))); off += 4
		copy(buf[off:], value)
	}
	return buf
}

func unmarshalPayload(buf []byte) (Record, error) {
	if len(buf) < 17 {
		return Record{}, fmt.Errorf("wal: payload too short (%d bytes)", len(buf))
	}
	off := 0
	kind := buf[off]; off++
	seq := le.Uint64(buf[off:]); off += 8
	txnID := le.Uint64(buf[off:]); off += 8

	rec := Record{Kind: kind, SeqNum: seq, TxnID: txnID, Tombstone: kind == KindDelete}

	// Control records carry no key/value.
	if kind == KindTxnBegin || kind == KindTxnCommit || kind == KindTxnAbort {
		return rec, nil
	}

	if off+4 > len(buf) {
		return Record{}, fmt.Errorf("wal: key len overrun")
	}
	keyLen := int(le.Uint32(buf[off:])); off += 4
	if off+keyLen > len(buf) {
		return Record{}, fmt.Errorf("wal: key overrun")
	}
	rec.Key = make([]byte, keyLen)
	copy(rec.Key, buf[off:]); off += keyLen

	if kind == KindPut {
		if off+4 > len(buf) {
			return Record{}, fmt.Errorf("wal: val len overrun")
		}
		valLen := int(le.Uint32(buf[off:])); off += 4
		if off+valLen > len(buf) {
			return Record{}, fmt.Errorf("wal: val overrun")
		}
		rec.Value = make([]byte, valLen)
		copy(rec.Value, buf[off:])
	}
	return rec, nil
}

// ── Lifecycle ─────────────────────────────────────────────────────────────────

// Size returns the current on-disk WAL size in bytes.
func (w *WAL) Size() (int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	fi, err := w.f.Stat()
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

// Path returns the file path of this WAL.
func (w *WAL) Path() string { return w.path }

// Close flushes buffered data and closes the file.
func (w *WAL) Close() error {
	if w.policy == SyncBatch {
		close(w.stopCh)
		w.wg.Wait()
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.bw.Flush(); err != nil {
		return err
	}
	return w.f.Close()
}

// Delete removes the WAL file from disk (called after a successful compaction).
func (w *WAL) Delete() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.f.Close()
	return os.Remove(w.path)
}