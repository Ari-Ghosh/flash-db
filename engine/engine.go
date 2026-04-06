// Package engine is the top-level entry point for HybridDB.
//
// Architecture recap:
//
//   Write path  →  WAL  →  MemTable  →  (flush)  →  SSTable L0
//                                                         │
//                                               compaction engine
//                                                         │
//   Read path   ←  ReadMerger  ←  B-tree  ←  (bulk-load)┘
//                       ↑
//                  MemTable (newest wins)
//
// Public API:
//   db, err := engine.Open(cfg)
//   err = db.Put(key, value)
//   err = db.Delete(key)
//   val, err := db.Get(key)
//   err = db.Close()
package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"

	hybriddb "local/flashdb/hybriddb"
	"local/flashdb/btree"
	"local/flashdb/compaction"
	"local/flashdb/memtable"
	"local/flashdb/sstable"
	"local/flashdb/wal"
)

// Config holds all engine tuning parameters.
type Config struct {
	// Dir is the directory where all files are stored.
	Dir string

	// MemTableSize is the flush threshold in bytes (default 64 MB).
	MemTableSize int64

	// L0CompactThreshold is how many L0 SSTables trigger compaction (default 4).
	L0CompactThreshold int
}

// DefaultConfig returns production-sensible defaults.
func DefaultConfig(dir string) Config {
	return Config{
		Dir:                dir,
		MemTableSize:       64 * 1024 * 1024,
		L0CompactThreshold: 4,
	}
}

// DB is the main HybridDB instance.
type DB struct {
	cfg Config

	mu       sync.Mutex
	seq      uint64 // monotonic sequence number
	memtable *memtable.MemTable
	imm      *memtable.MemTable // immutable memtable being flushed

	walActive *wal.WAL
	l0Files   []string // paths of unflushed L0 SSTables
	l0Mu      sync.Mutex

	btree     *btree.BTree
	compactor *compaction.Engine

	closeOnce sync.Once
	closeCh   chan struct{}
}

// Open opens or creates a HybridDB at cfg.Dir.
func Open(cfg Config) (*DB, error) {
	if err := os.MkdirAll(cfg.Dir, 0755); err != nil {
		return nil, fmt.Errorf("engine open: mkdir: %w", err)
	}
	if cfg.MemTableSize == 0 {
		cfg.MemTableSize = 64 * 1024 * 1024
	}
	if cfg.L0CompactThreshold == 0 {
		cfg.L0CompactThreshold = 4
	}

	// Open / create the B-tree.
	bt, err := btree.Open(filepath.Join(cfg.Dir, "btree.db"))
	if err != nil {
		return nil, fmt.Errorf("engine: btree: %w", err)
	}

	// Open the WAL (will be replayed below).
	walPath := filepath.Join(cfg.Dir, "wal.log")
	w, err := wal.Open(walPath)
	if err != nil {
		return nil, fmt.Errorf("engine: wal: %w", err)
	}

	db := &DB{
		cfg:       cfg,
		memtable:  memtable.New(cfg.MemTableSize),
		btree:     bt,
		walActive: w,
		closeCh:   make(chan struct{}),
	}
	db.compactor = compaction.New(
		compaction.Config{L0Threshold: cfg.L0CompactThreshold},
		bt,
	)
	db.compactor.Start()

	// Replay WAL to restore in-memory state.
	if err := db.replayWAL(w); err != nil {
		return nil, fmt.Errorf("engine: replay: %w", err)
	}

	// Discover any L0 SSTables left from a prior run.
	db.discoverL0Files()

	return db, nil
}

// Put writes key=value into the DB.
func (db *DB) Put(key, value []byte) error {
	seq := atomic.AddUint64(&db.seq, 1)

	if err := db.walActive.AppendPut(seq, key, value); err != nil {
		return fmt.Errorf("put: wal: %w", err)
	}
	if err := db.memtable.Put(key, value, seq); err != nil {
		return err
	}
	if db.memtable.IsFull() {
		return db.triggerFlush()
	}
	return nil
}

// Delete marks key as deleted.
func (db *DB) Delete(key []byte) error {
	seq := atomic.AddUint64(&db.seq, 1)

	if err := db.walActive.AppendDelete(seq, key); err != nil {
		return fmt.Errorf("delete: wal: %w", err)
	}
	if err := db.memtable.Delete(key, seq); err != nil {
		return err
	}
	if db.memtable.IsFull() {
		return db.triggerFlush()
	}
	return nil
}

// Get returns the value for key, or ErrKeyNotFound / ErrKeyDeleted.
func (db *DB) Get(key []byte) ([]byte, error) {
	// 1. Check active MemTable (newest writes).
	if e, err := db.memtable.Get(key); err == nil {
		if e.Tombstone {
			return nil, hybriddb.ErrKeyDeleted
		}
		return e.Value, nil
	}

	// 2. Check immutable MemTable (being flushed).
	if db.imm != nil {
		if e, err := db.imm.Get(key); err == nil {
			if e.Tombstone {
				return nil, hybriddb.ErrKeyDeleted
			}
			return e.Value, nil
		}
	}

	// 3. Check B-tree (durable, compacted data).
	e, err := db.btree.Get(key)
	if err != nil {
		return nil, hybriddb.ErrKeyNotFound
	}
	if e.Tombstone {
		return nil, hybriddb.ErrKeyDeleted
	}
	return e.Value, nil
}

// ────────────────────────────────────────────────────────────
//  Flush: MemTable → SSTable
// ────────────────────────────────────────────────────────────

func (db *DB) triggerFlush() error {
	db.mu.Lock()
	// Rotate: active → immutable.
	db.imm = db.memtable
	db.memtable = memtable.New(db.cfg.MemTableSize)
	db.mu.Unlock()

	go func() {
		if err := db.flushImmutable(); err != nil {
			fmt.Printf("flush error: %v\n", err)
		}
	}()
	return nil
}

func (db *DB) flushImmutable() error {
	db.mu.Lock()
	imm := db.imm
	db.mu.Unlock()

	if imm == nil || imm.Count() == 0 {
		return nil
	}

	entries := imm.Entries()
	seq := atomic.LoadUint64(&db.seq)
	path := filepath.Join(db.cfg.Dir, fmt.Sprintf("l0_%020d.sst", seq))

	w, err := sstable.NewWriter(path, uint(len(entries)))
	if err != nil {
		return err
	}
	for _, e := range entries {
		if err := w.Add(e); err != nil {
			return err
		}
	}
	if err := w.Close(); err != nil {
		return err
	}

	// Register the new L0 file.
	db.l0Mu.Lock()
	db.l0Files = append(db.l0Files, path)
	l0Count := len(db.l0Files)
	snapshot := make([]string, l0Count)
	copy(snapshot, db.l0Files)
	db.l0Mu.Unlock()

	// Clear the immutable MemTable.
	db.mu.Lock()
	db.imm = nil
	db.mu.Unlock()

	// Trigger compaction if we've hit the threshold.
	if l0Count >= db.cfg.L0CompactThreshold {
		db.l0Mu.Lock()
		toCompact := make([]string, len(db.l0Files))
		copy(toCompact, db.l0Files)
		db.l0Files = db.l0Files[:0]
		db.l0Mu.Unlock()

		if !db.compactor.Trigger(toCompact) {
			// Compaction queue full: put files back.
			db.l0Mu.Lock()
			db.l0Files = append(toCompact, db.l0Files...)
			db.l0Mu.Unlock()
		} else {
			// Rotate the WAL — after compaction the old entries are in the B-tree.
			db.rotateWAL()
		}
	}
	_ = snapshot
	return nil
}

// ────────────────────────────────────────────────────────────
//  WAL management
// ────────────────────────────────────────────────────────────

func (db *DB) replayWAL(w *wal.WAL) error {
	records, err := w.Replay()
	if err != nil {
		return err
	}
	var maxSeq uint64
	for _, r := range records {
		if r.SeqNum > maxSeq {
			maxSeq = r.SeqNum
		}
		if r.Kind == 0 {
			db.memtable.Put(r.Key, r.Value, r.SeqNum)
		} else {
			db.memtable.Delete(r.Key, r.SeqNum)
		}
	}
	atomic.StoreUint64(&db.seq, maxSeq)
	return nil
}

func (db *DB) rotateWAL() {
	db.mu.Lock()
	defer db.mu.Unlock()

	oldWAL := db.walActive
	newPath := filepath.Join(db.cfg.Dir, fmt.Sprintf("wal_%d.log", atomic.LoadUint64(&db.seq)))
	newWAL, err := wal.Open(newPath)
	if err != nil {
		fmt.Printf("WAL rotate error: %v\n", err)
		return
	}
	db.walActive = newWAL
	go func() {
		oldWAL.Delete()
	}()
}

func (db *DB) discoverL0Files() {
	matches, _ := filepath.Glob(filepath.Join(db.cfg.Dir, "l0_*.sst"))
	sort.Strings(matches)
	db.l0Files = matches
}

// ────────────────────────────────────────────────────────────
//  Close
// ────────────────────────────────────────────────────────────

// Close flushes all pending data and shuts down the engine.
func (db *DB) Close() error {
	var firstErr error
	db.closeOnce.Do(func() {
		close(db.closeCh)
		db.compactor.Stop()

		// Flush active memtable if it has data.
		if db.memtable.Count() > 0 {
			db.mu.Lock()
			db.imm = db.memtable
			db.memtable = memtable.New(db.cfg.MemTableSize)
			db.mu.Unlock()
			if err := db.flushImmutable(); err != nil {
				firstErr = err
			}
		}

		if err := db.walActive.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := db.btree.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	})
	return firstErr
}

// Stats returns a snapshot of engine internals for monitoring.
func (db *DB) Stats() Stats {
	db.l0Mu.Lock()
	l0 := len(db.l0Files)
	db.l0Mu.Unlock()
	return Stats{
		MemTableSize:  db.memtable.Size(),
		MemTableCount: db.memtable.Count(),
		L0FileCount:   l0,
		SeqNum:        atomic.LoadUint64(&db.seq),
	}
}

// Stats holds observable engine metrics.
type Stats struct {
	MemTableSize  int64
	MemTableCount int64
	L0FileCount   int
	SeqNum        uint64
}
