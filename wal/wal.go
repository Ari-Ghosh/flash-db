// Package wal implements a Write-Ahead Log.
//
// Every write is appended as a length-prefixed binary record before it
// touches the MemTable.  On restart the engine replays the WAL to restore
// any entries that were not yet compacted into the B-tree.
//
// Record wire format (little-endian):
//
//	[4]  crc32   – checksum of the payload bytes
//	[4]  length  – payload byte length
//	[1]  kind    – 0 = Put, 1 = Delete
//	[8]  seqNum
//	[4]  keyLen
//	[keyLen] key
//	[4]  valLen  (omitted for Delete)
//	[valLen] value (omitted for Delete)
package wal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

const (
	kindPut    byte = 0
	kindDelete byte = 1
)

var le = binary.LittleEndian

// Record is a single WAL entry returned during replay.
type Record struct {
	Kind      byte
	SeqNum    uint64
	Key       []byte
	Value     []byte // nil for Delete
	Tombstone bool
}

// WAL wraps an append-only file.
type WAL struct {
	mu   sync.Mutex
	f    *os.File
	buf  *bufio.Writer
	path string
}

// Open opens or creates the WAL at path.
func Open(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("wal open: %w", err)
	}
	return &WAL{f: f, buf: bufio.NewWriterSize(f, 64*1024), path: path}, nil
}

// AppendPut writes a Put record and syncs to disk.
func (w *WAL) AppendPut(seq uint64, key, value []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.append(kindPut, seq, key, value)
}

// AppendDelete writes a Delete (tombstone) record and syncs to disk.
func (w *WAL) AppendDelete(seq uint64, key []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.append(kindDelete, seq, key, nil)
}

func (w *WAL) append(kind byte, seq uint64, key, value []byte) error {
	payload := marshalPayload(kind, seq, key, value)
	checksum := crc32.ChecksumIEEE(payload)

	var hdr [8]byte
	le.PutUint32(hdr[0:4], checksum)
	le.PutUint32(hdr[4:8], uint32(len(payload)))

	if _, err := w.buf.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := w.buf.Write(payload); err != nil {
		return err
	}
	if err := w.buf.Flush(); err != nil {
		return err
	}
	return w.f.Sync()
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
// Corrupt or truncated trailing records are silently ignored (the engine
// will treat any missing entries as not-yet-written on replay).
func (w *WAL) Replay() ([]Record, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	r := bufio.NewReader(w.f)
	var records []Record

	for {
		var hdr [8]byte
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return nil, err
		}
		checksum := le.Uint32(hdr[0:4])
		length := le.Uint32(hdr[4:8])

		payload := make([]byte, length)
		if _, err := io.ReadFull(r, payload); err != nil {
			break // truncated — stop here
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
	keyLen := le.Uint32(buf[off:])
	off += 4
	if int(off)+int(keyLen) > len(buf) {
		return Record{}, fmt.Errorf("wal: key overrun")
	}
	key := make([]byte, keyLen)
	copy(key, buf[off:off+int(keyLen)])
	off += int(keyLen)

	rec := Record{Kind: kind, SeqNum: seq, Key: key, Tombstone: kind == kindDelete}
	if kind == kindPut {
		if off+4 > len(buf) {
			return Record{}, fmt.Errorf("wal: val len overrun")
		}
		valLen := le.Uint32(buf[off:])
		off += 4
		if off+int(valLen) > len(buf) {
			return Record{}, fmt.Errorf("wal: val overrun")
		}
		rec.Value = make([]byte, valLen)
		copy(rec.Value, buf[off:])
	}
	return rec, nil
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
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.buf.Flush(); err != nil {
		return err
	}
	return w.f.Close()
}
