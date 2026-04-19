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
// ## v1 vs v2 Changes
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

const (
	kindPut    byte = 0
	kindDelete byte = 1
)

var le = binary.LittleEndian

// SyncPolicy controls when WAL writes are synced to disk.
type SyncPolicy int

const (
	// SyncAlways fsyncs after every individual record (v1 behaviour, safest).
	SyncAlways SyncPolicy = iota
	// SyncBatch groups concurrent writes into a single fsync (default).
	SyncBatch
	// SyncNone never fsyncs; the OS decides. Fastest, but data may be lost
	// on power failure if the write buffer hasn't been flushed.
	SyncNone
)

// Record is a single WAL entry returned during replay.
type Record struct {
	Kind      byte
	SeqNum    uint64
	Key       []byte
	Value     []byte
	Tombstone bool
}

// batchEntry is queued by a writer waiting for a group-commit flush.
type batchEntry struct {
	payload []byte
	done    chan error
}

// WAL wraps an append-only file.
type WAL struct {
	mu     sync.Mutex
	f      *os.File
	bw     *bufio.Writer
	path   string
	policy SyncPolicy

	// Group-commit fields (used when policy == SyncBatch).
	batchMu sync.Mutex
	pending []batchEntry
	flushCh chan struct{}
	stopCh  chan struct{}
	wg      sync.WaitGroup
}

// Options configures WAL behaviour.
type Options struct {
	// SyncPolicy controls fsync frequency. Default: SyncBatch.
	SyncPolicy SyncPolicy
	// BatchInterval is how long the group-commit loop waits before flushing.
	// Default: 1ms. Only relevant for SyncBatch.
	BatchInterval time.Duration
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
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("wal open: %w", err)
	}
	if opts.BatchInterval == 0 {
		opts.BatchInterval = time.Millisecond
	}
	w := &WAL{
		f:       f,
		bw:      bufio.NewWriterSize(f, 256*1024),
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

// AppendPut writes a Put record.
func (w *WAL) AppendPut(seq uint64, key, value []byte) error {
	return w.appendRecord(kindPut, seq, key, value)
}

// AppendDelete writes a Delete (tombstone) record.
func (w *WAL) AppendDelete(seq uint64, key []byte) error {
	return w.appendRecord(kindDelete, seq, key, nil)
}

func (w *WAL) appendRecord(kind byte, seq uint64, key, value []byte) error {
	payload := marshalPayload(kind, seq, key, value)

	switch w.policy {
	case SyncAlways:
		return w.writeSingle(payload, true)
	case SyncNone:
		return w.writeSingle(payload, false)
	default: // SyncBatch
		return w.writeQueued(payload)
	}
}

// writeSingle appends payload directly under the mutex.
func (w *WAL) writeSingle(payload []byte, sync bool) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.writePayload(payload); err != nil {
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

// writeQueued queues payload for the batch flusher and blocks until flushed.
func (w *WAL) writeQueued(payload []byte) error {
	done := make(chan error, 1)
	w.batchMu.Lock()
	w.pending = append(w.pending, batchEntry{payload: payload, done: done})
	w.batchMu.Unlock()

	// Signal the flusher (non-blocking; it may already be running).
	select {
	case w.flushCh <- struct{}{}:
	default:
	}

	return <-done
}

// batchFlusher drains w.pending at regular intervals with a single fsync.
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
			writeErr = w.writePayload(e.payload)
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

// writePayload appends a length-prefixed, checksummed record.
// Must be called with w.mu held.
func (w *WAL) writePayload(payload []byte) error {
	checksum := crc32.ChecksumIEEE(payload)
	var hdr [8]byte
	le.PutUint32(hdr[0:], checksum)
	le.PutUint32(hdr[4:], uint32(len(payload)))
	if _, err := w.bw.Write(hdr[:]); err != nil {
		return err
	}
	_, err := w.bw.Write(payload)
	return err
}

func marshalPayload(kind byte, seq uint64, key, value []byte) []byte {
	size := 1 + 8 + 4 + len(key)
	if kind == kindPut {
		size += 4 + len(value)
	}
	buf := make([]byte, size)
	off := 0
	buf[off] = kind
	off++
	le.PutUint64(buf[off:], seq)
	off += 8
	le.PutUint32(buf[off:], uint32(len(key)))
	off += 4
	copy(buf[off:], key)
	off += len(key)
	if kind == kindPut {
		le.PutUint32(buf[off:], uint32(len(value)))
		off += 4
		copy(buf[off:], value)
	}
	return buf
}

// Replay reads all valid records from the WAL.
// Corrupt or truncated trailing records are silently skipped.
func (w *WAL) Replay() ([]Record, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	r := bufio.NewReaderSize(w.f, 256*1024)
	var records []Record

	for {
		var hdr [8]byte
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return nil, err
		}
		checksum := le.Uint32(hdr[0:])
		length := le.Uint32(hdr[4:])

		payload := make([]byte, length)
		if _, err := io.ReadFull(r, payload); err != nil {
			break // truncated tail — stop here, don't error
		}
		if crc32.ChecksumIEEE(payload) != checksum {
			break // corrupted — stop here
		}

		rec, err := unmarshalPayload(payload)
		if err != nil {
			break
		}
		records = append(records, rec)
	}
	return records, nil
}

func unmarshalPayload(buf []byte) (Record, error) {
	if len(buf) < 13 {
		return Record{}, fmt.Errorf("wal: payload too short")
	}
	off := 0
	kind := buf[off]
	off++
	seq := le.Uint64(buf[off:])
	off += 8
	keyLen := int(le.Uint32(buf[off:]))
	off += 4
	if off+keyLen > len(buf) {
		return Record{}, fmt.Errorf("wal: key overrun")
	}
	key := make([]byte, keyLen)
	copy(key, buf[off:off+keyLen])
	off += keyLen

	rec := Record{Kind: kind, SeqNum: seq, Key: key, Tombstone: kind == kindDelete}
	if kind == kindPut {
		if off+4 > len(buf) {
			return Record{}, fmt.Errorf("wal: val len overrun")
		}
		valLen := int(le.Uint32(buf[off:]))
		off += 4
		if off+valLen > len(buf) {
			return Record{}, fmt.Errorf("wal: val overrun")
		}
		rec.Value = make([]byte, valLen)
		copy(rec.Value, buf[off:])
	}
	return rec, nil
}

// Size returns the current WAL file size in bytes.
func (w *WAL) Size() (int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	fi, err := w.f.Stat()
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

// Delete removes the WAL file from disk (called after a successful compaction).
func (w *WAL) Delete() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.f.Close()
	return os.Remove(w.path)
}

// Close flushes and closes the WAL file.
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

// Path returns the file path of this WAL.
func (w *WAL) Path() string { return w.path }
