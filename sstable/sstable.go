// Package sstable implements sorted string table files.
//
// An SSTable is an immutable, sorted, on-disk sequence of key-value entries
// produced when the MemTable is flushed.  These are the L0 files that the
// compaction engine later merges and converts into B-tree pages.
//
// File layout:
//
//	[data blocks]              – fixed-size (4 KB) blocks of entries
//	[index block]              – one entry per data block (first key + offset)
//	[bloom filter bytes]
//	[footer 32 bytes]
//	  indexOffset  uint64
//	  indexLen     uint32
//	  bloomOffset  uint64
//	  bloomLen     uint32
//	  entryCount   uint64
//	  magic        uint32  = 0xDEADBEEF
//
// Each entry inside a data block:
//	[1]  flags  (bit0 = tombstone)
//	[8]  seqNum
//	[4]  keyLen
//	[keyLen] key
//	[4]  valLen
//	[valLen] value  (absent when tombstone)
package sstable

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	hybriddb "local/flashdb/hybriddb"
	"local/flashdb/bloom"
)

const (
	blockSize = 4 * 1024
	magic     = uint32(0xDEADBEEF)
)

var le = binary.LittleEndian

// IndexEntry points to a data block.
type IndexEntry struct {
	FirstKey []byte
	Offset   uint64
	Length   uint32
}

// Metadata holds everything read from the SSTable footer.
type Metadata struct {
	IndexEntries []IndexEntry
	Bloom        *bloom.Filter
	EntryCount   uint64
	Path         string
}

// ────────────────────────────────────────────────────────────
//  Writer
// ────────────────────────────────────────────────────────────

// Writer builds an SSTable from a sorted stream of entries.
type Writer struct {
	f      *os.File
	bw     *bufio.Writer
	index  []IndexEntry
	bloom  *bloom.Filter
	offset uint64
	block  []byte
	count  uint64
}

// NewWriter opens path for writing and returns a Writer.
func NewWriter(path string, expectedEntries uint) (*Writer, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("sstable writer: %w", err)
	}
	return &Writer{
		f:     f,
		bw:    bufio.NewWriterSize(f, 256*1024),
		bloom: bloom.New(max(expectedEntries, 100), 0.01),
	}, nil
}

// Add appends a sorted entry. Entries MUST be written in ascending key order.
func (w *Writer) Add(e hybriddb.Entry) error {
	encoded := encodeEntry(e)

	// If this entry would overflow the current block, flush it first.
	if len(w.block)+len(encoded) > blockSize && len(w.block) > 0 {
		if err := w.flushBlock(); err != nil {
			return err
		}
	}
	// Record the first key of every new block for the index.
	if len(w.block) == 0 {
		firstKey := make([]byte, len(e.Key))
		copy(firstKey, e.Key)
		w.index = append(w.index, IndexEntry{FirstKey: firstKey, Offset: w.offset})
	}

	w.block = append(w.block, encoded...)
	w.bloom.Add(e.Key)
	w.count++
	return nil
}

func (w *Writer) flushBlock() error {
	n, err := w.bw.Write(w.block)
	if err != nil {
		return err
	}
	// Patch the last index entry with the actual block length.
	w.index[len(w.index)-1].Length = uint32(n)
	w.offset += uint64(n)
	w.block = w.block[:0]
	return nil
}

// Close finalises the SSTable (flushes remaining block, writes index, bloom,
// and footer) and closes the file.
func (w *Writer) Close() error {
	// Flush any partial block.
	if len(w.block) > 0 {
		if err := w.flushBlock(); err != nil {
			return err
		}
	}

	// Write index block.
	indexOffset := w.offset
	indexBytes := encodeIndex(w.index)
	if _, err := w.bw.Write(indexBytes); err != nil {
		return err
	}
	bloomOffset := indexOffset + uint64(len(indexBytes))

	// Write bloom filter.
	bloomBytes := w.bloom.Bytes()
	if _, err := w.bw.Write(bloomBytes); err != nil {
		return err
	}

	// Write footer (32 bytes).
	footer := make([]byte, 32)
	le.PutUint64(footer[0:], indexOffset)
	le.PutUint32(footer[8:], uint32(len(indexBytes)))
	le.PutUint64(footer[12:], bloomOffset)
	le.PutUint32(footer[20:], uint32(len(bloomBytes)))
	le.PutUint64(footer[24:], w.count) // last 8 bytes, slight reuse
	// Shim: overwrite last 4 with magic, count in 8 bytes before that.
	// Layout: indexOff(8) indexLen(4) bloomOff(8) bloomLen(4) count(4) magic(4)
	le.PutUint64(footer[0:], indexOffset)
	le.PutUint32(footer[8:], uint32(len(indexBytes)))
	le.PutUint64(footer[12:], bloomOffset)
	le.PutUint32(footer[20:], uint32(len(bloomBytes)))
	le.PutUint32(footer[24:], uint32(w.count))
	le.PutUint32(footer[28:], magic)

	if _, err := w.bw.Write(footer); err != nil {
		return err
	}
	if err := w.bw.Flush(); err != nil {
		return err
	}
	return w.f.Close()
}

// ────────────────────────────────────────────────────────────
//  Reader
// ────────────────────────────────────────────────────────────

// Reader reads from an SSTable file.
type Reader struct {
	f    *os.File
	meta Metadata
}

// OpenReader opens an SSTable for reading.
func OpenReader(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("sstable reader: %w", err)
	}
	r := &Reader{f: f}
	if err := r.loadMeta(path); err != nil {
		f.Close()
		return nil, err
	}
	return r, nil
}

func (r *Reader) loadMeta(path string) error {
	fi, err := r.f.Stat()
	if err != nil {
		return err
	}
	if fi.Size() < 32 {
		return fmt.Errorf("sstable: file too small")
	}

	footer := make([]byte, 32)
	if _, err := r.f.ReadAt(footer, fi.Size()-32); err != nil {
		return err
	}
	if le.Uint32(footer[28:]) != magic {
		return fmt.Errorf("sstable: bad magic")
	}

	indexOff := le.Uint64(footer[0:])
	indexLen := le.Uint32(footer[8:])
	bloomOff := le.Uint64(footer[12:])
	bloomLen := le.Uint32(footer[20:])
	count := le.Uint32(footer[24:])

	// Read index.
	indexBuf := make([]byte, indexLen)
	if _, err := r.f.ReadAt(indexBuf, int64(indexOff)); err != nil {
		return err
	}
	r.meta.IndexEntries = decodeIndex(indexBuf)

	// Read bloom filter.
	bloomBuf := make([]byte, bloomLen)
	if _, err := r.f.ReadAt(bloomBuf, int64(bloomOff)); err != nil {
		return err
	}
	r.meta.Bloom = bloom.FromBytes(bloomBuf)
	r.meta.EntryCount = uint64(count)
	r.meta.Path = path
	return nil
}

// MayContain returns false if the key is definitely absent.
func (r *Reader) MayContain(key []byte) bool {
	return r.meta.Bloom.MayContain(key)
}

// Get finds key in the SSTable. Returns ErrKeyNotFound if absent.
func (r *Reader) Get(key []byte) (*hybriddb.Entry, error) {
	if !r.MayContain(key) {
		return nil, hybriddb.ErrKeyNotFound
	}

	blockIdx := r.findBlock(key)
	if blockIdx < 0 {
		return nil, hybriddb.ErrKeyNotFound
	}

	ie := r.meta.IndexEntries[blockIdx]
	block := make([]byte, ie.Length)
	if _, err := r.f.ReadAt(block, int64(ie.Offset)); err != nil {
		return nil, err
	}

	return searchBlock(block, key)
}

// findBlock returns the index of the data block that might contain key,
// using the index entries (binary search over first keys).
func (r *Reader) findBlock(key []byte) int {
	entries := r.meta.IndexEntries
	lo, hi := 0, len(entries)-1
	result := -1
	for lo <= hi {
		mid := (lo + hi) / 2
		cmp := compareBytes(entries[mid].FirstKey, key)
		if cmp <= 0 {
			result = mid
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	return result
}

// Iter returns a channel of all entries in sorted order.
// Used by the compaction engine.
func (r *Reader) Iter() (<-chan hybriddb.Entry, error) {
	ch := make(chan hybriddb.Entry, 256)
	go func() {
		defer close(ch)
		if len(r.meta.IndexEntries) == 0 {
			return
		}
		// Stream data blocks sequentially.
		if _, err := r.f.Seek(0, io.SeekStart); err != nil {
			return
		}
		br := bufio.NewReaderSize(r.f, 64*1024)
		for _, ie := range r.meta.IndexEntries {
			block := make([]byte, ie.Length)
			if _, err := io.ReadFull(br, block); err != nil {
				return
			}
			off := 0
			for off < len(block) {
				e, n, err := decodeEntry(block[off:])
				if err != nil || n == 0 {
					break
				}
				ch <- e
				off += n
			}
		}
	}()
	return ch, nil
}

// Metadata returns file metadata.
func (r *Reader) Metadata() Metadata { return r.meta }

// Close closes the underlying file.
func (r *Reader) Close() error { return r.f.Close() }

// ────────────────────────────────────────────────────────────
//  Encoding helpers
// ────────────────────────────────────────────────────────────

func encodeEntry(e hybriddb.Entry) []byte {
	size := 1 + 8 + 4 + len(e.Key) + 4 + len(e.Value)
	buf := make([]byte, size)
	off := 0
	flags := byte(0)
	if e.Tombstone {
		flags |= 1
	}
	buf[off] = flags
	off++
	le.PutUint64(buf[off:], e.SeqNum)
	off += 8
	le.PutUint32(buf[off:], uint32(len(e.Key)))
	off += 4
	copy(buf[off:], e.Key)
	off += len(e.Key)
	le.PutUint32(buf[off:], uint32(len(e.Value)))
	off += 4
	copy(buf[off:], e.Value)
	return buf
}

func decodeEntry(buf []byte) (hybriddb.Entry, int, error) {
	if len(buf) < 17 {
		return hybriddb.Entry{}, 0, fmt.Errorf("short entry")
	}
	off := 0
	flags := buf[off]
	off++
	seq := le.Uint64(buf[off:])
	off += 8
	keyLen := int(le.Uint32(buf[off:]))
	off += 4
	if off+keyLen+4 > len(buf) {
		return hybriddb.Entry{}, 0, fmt.Errorf("key overrun")
	}
	key := make([]byte, keyLen)
	copy(key, buf[off:])
	off += keyLen
	valLen := int(le.Uint32(buf[off:]))
	off += 4
	if off+valLen > len(buf) {
		return hybriddb.Entry{}, 0, fmt.Errorf("val overrun")
	}
	val := make([]byte, valLen)
	copy(val, buf[off:])
	off += valLen
	return hybriddb.Entry{
		Key:       key,
		Value:     val,
		SeqNum:    seq,
		Tombstone: flags&1 != 0,
	}, off, nil
}

func searchBlock(block, key []byte) (*hybriddb.Entry, error) {
	off := 0
	for off < len(block) {
		e, n, err := decodeEntry(block[off:])
		if err != nil || n == 0 {
			break
		}
		cmp := compareBytes(e.Key, key)
		if cmp == 0 {
			return &e, nil
		}
		if cmp > 0 {
			break // entries are sorted; key not here
		}
		off += n
	}
	return nil, hybriddb.ErrKeyNotFound
}

func encodeIndex(entries []IndexEntry) []byte {
	var buf []byte
	for _, ie := range entries {
		tmp := make([]byte, 4+len(ie.FirstKey)+8+4)
		off := 0
		le.PutUint32(tmp[off:], uint32(len(ie.FirstKey)))
		off += 4
		copy(tmp[off:], ie.FirstKey)
		off += len(ie.FirstKey)
		le.PutUint64(tmp[off:], ie.Offset)
		off += 8
		le.PutUint32(tmp[off:], ie.Length)
		buf = append(buf, tmp...)
	}
	return buf
}

func decodeIndex(buf []byte) []IndexEntry {
	var entries []IndexEntry
	off := 0
	for off < len(buf) {
		if off+4 > len(buf) {
			break
		}
		kl := int(le.Uint32(buf[off:]))
		off += 4
		if off+kl+12 > len(buf) {
			break
		}
		key := make([]byte, kl)
		copy(key, buf[off:])
		off += kl
		offset := le.Uint64(buf[off:])
		off += 8
		length := le.Uint32(buf[off:])
		off += 4
		entries = append(entries, IndexEntry{FirstKey: key, Offset: offset, Length: length})
	}
	return entries
}

func compareBytes(a, b []byte) int {
	for i := range a {
		if i >= len(b) {
			return 1
		}
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	if len(a) < len(b) {
		return -1
	}
	return 0
}

func max(a, b uint) uint {
	if a > b {
		return a
	}
	return b
}
