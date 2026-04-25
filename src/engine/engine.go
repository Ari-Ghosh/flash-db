// Package engine is the top-level entry point for flashDB.
//
// ## Overview
//
// The Engine orchestrates the entire LSM-tree architecture, coordinating WAL,
// MemTable, SSTable, and compaction components. It provides the public API for
// key-value operations with MVCC snapshots and range iterators.
//
// ## Architecture
//
// ### Write Path
// WAL (group-commit) → MemTable → (flush) → L0 SSTable → compaction → L1 B-tree → L2 B-tree
//
// ### Read Path
// MergedIterator ← MemTable ← L1 B-tree ← L2 B-tree
//
// ### Entry Points
// Package engine is the top-level entry point for flashDB v3, exposing the DB struct with methods for Put/Get/Delete, transactions, snapshots, iterators, and backup. It also implements replication.Applier for follower nodes and backup.BackupSource for hot backups.
//
//	Write path → WAL (group-commit) → MemTable → (flush) → L0 SSTable
//	                                                              │
//	                                                    compaction engine
//	                                                              │
//	Read path ← MergedIterator ← MemTable ← L1 B-tree ← L2 B-tree
//
// ## version Changes
//
// ### v1 (Original Implementation)
// - Basic Put/Get/Delete operations.
// - No snapshots or MVCC; reads see latest state.
// - Simple compaction: MemTable flush to single B-tree.
// - No iterators; range queries not supported.
// - Used for basic key-value storage with crash recovery.
//
// ### v2 (Enhanced Implementation)
// - **MVCC Snapshots**: NewSnapshot() for point-in-time reads.
// - **Range Iterators**: NewIterator() for merged range scans.
// - **WAL Group-Commit**: Amortized fsync cost for high throughput.
// - **Tiered Compaction**: Incremental L0→L1→L2 merging.
// - **Configurable Compression**: Codec for value compression.
// - **Background Error Handling**: db.Err() surfaces compaction errors.
//
// ## Key Methods
//
// - **Open()**: Initialize engine with config, replay WAL, rebuild state.
// - **Put/Delete()**: Write operations with sequence numbering.
// - **Get()**: Lookup with optional snapshot.
// - **NewSnapshot()**: Create MVCC snapshot.
// - **NewIterator()**: Range scan with bounds and snapshot.
// - **Close()**: Flush pending writes, close files.
//
// ## Configuration
//
//	type Config struct {
//		Path          string        // Data directory
//		WALSyncPolicy WALSyncPolicy // Durability vs throughput
//		Codec         Codec         // Serialization/compression
//		MaxMemTableSize int64       // Flush threshold
//	}
//
// ## Performance Characteristics
//
// - **Writes**: WAL batching + MemTable buffering for high throughput.
// - **Reads**: MemTable fast path, B-tree O(log n) for disk.
// - **Compaction**: Incremental merging avoids full rebuilds.
// - **Snapshots**: O(1) creation, O(n) space for long-lived ones.
//
// ## Usage
//
//	db, err := engine.Open(config)
//	defer db.Close()
//
//	// Write
//	err = db.Put(key, value)
//
//	// Read with snapshot
//	snap := db.NewSnapshot()
//	val, err := db.Get(key, snap)
//
//	// Range scan
//	iter := db.NewIterator(IteratorOptions{
//		Start: key1,
//		End:   key2,
//		Snapshot: snap,
//	})
//	for iter.Next() {
//		fmt.Printf("%s = %s\n", iter.Key(), iter.Value())
//	}
//
// ### v3 (on top of v2 foundations):
//   - Parallel writes via WAL group-commit batching (wal.SyncBatch, default).
//   - Multi-key atomic transactions via txn.Txn — implements txn.Store.
//   - Prefix scan via PrefixScan(prefix) and IteratorOptions.Prefix.
//   - Hot backup via Backup(destDir) — implements backup.BackupSource.
//   - FlushSync() for backup to drain the MemTable synchronously.
//   - Distributed replication hooks: Ship() fans out to replication.Leader.
//   - SeqAt() for transaction read-set validation.
//   - ApplyTxn() for atomic multi-key commit.
//   - NextTxnID() for unique transaction identifiers.
package engine

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"

	"local/flashdb/src/backup"
	"local/flashdb/src/btree"
	"local/flashdb/src/compaction"
	"local/flashdb/src/memtable"
	"local/flashdb/src/replication"
	"local/flashdb/src/sstable"
	"local/flashdb/src/txn"
	types "local/flashdb/src/types"
	"local/flashdb/src/wal"
)

// Config holds all engine tuning parameters.
type Config struct {
	Dir                string
	MemTableSize       int64
	L0CompactThreshold int
	L1SizeThreshold    int64
	WALSyncPolicy      wal.SyncPolicy
	Codec              types.Codec
	// Replication is optional.  When non-nil, the engine registers writes
	// with the leader for fanout to followers.
	Replication 	   *replication.Config
}

// DefaultConfig returns production-sensible defaults.
func DefaultConfig(dir string) Config {
	return Config{
		Dir:                dir,
		MemTableSize:       64 * 1024 * 1024,
		L0CompactThreshold: 4,
		L1SizeThreshold:    256 * 1024 * 1024,
		WALSyncPolicy:      wal.SyncBatch,
		Codec:              types.CodecNone,
	}
}

// DB is the main flashDB instance.
type DB struct {
	cfg Config

	writeMu  sync.Mutex // serialises Commit-phase-2 and triggerFlush
	seq      atomic.Uint64
	txnSeq   atomic.Uint64 // monotonic transaction ID counter
	memtable *memtable.MemTable
	imm      *memtable.MemTable

	walActive *wal.WAL
	l0Mu      sync.Mutex
	l0Files   []string

	l1Tree *btree.BTree
	l2Tree *btree.BTree

	compactor *compaction.Engine
	snapTrack *types.SnapshotTracker

	leader   *replication.Leader   // nil if not configured
	follower *replication.Follower // nil if not configured

	bgErr     atomic.Value
	closeOnce sync.Once
	closeCh   chan struct{}

	// flushReady signals FlushSync that a flush completed.
	flushDone chan struct{}
}

// Open opens or creates a flashDB at cfg.Dir.
func Open(cfg Config) (*DB, error) {
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("engine open mkdir: %w", err)
	}
	if cfg.MemTableSize == 0 {
		cfg.MemTableSize = 64 * 1024 * 1024
	}
	if cfg.L0CompactThreshold == 0 {
		cfg.L0CompactThreshold = 4
	}
	if cfg.L1SizeThreshold == 0 {
		cfg.L1SizeThreshold = 256 * 1024 * 1024
	}

	l1Tree, err := btree.Open(filepath.Join(cfg.Dir, "btree_l1.db"))
	if err != nil {
		return nil, fmt.Errorf("engine: l1 btree: %w", err)
	}
	l2Tree, err := btree.Open(filepath.Join(cfg.Dir, "btree_l2.db"))
	if err != nil {
		return nil, fmt.Errorf("engine: l2 btree: %w", err)
	}

	walPath := filepath.Join(cfg.Dir, "wal.log")
	w, err := wal.OpenWithOptions(walPath, wal.Options{SyncPolicy: cfg.WALSyncPolicy})
	if err != nil {
		return nil, fmt.Errorf("engine: wal: %w", err)
	}

	snapTrack := types.NewSnapshotTracker()
	db := &DB{
		cfg:       cfg,
		memtable:  memtable.New(cfg.MemTableSize),
		l1Tree:    l1Tree,
		l2Tree:    l2Tree,
		walActive: w,
		snapTrack: snapTrack,
		closeCh:   make(chan struct{}),
		flushDone: make(chan struct{}, 1),
	}

	compCfg := compaction.Config{
		L0Threshold:     cfg.L0CompactThreshold,
		L1SizeThreshold: cfg.L1SizeThreshold,
	}
	db.compactor = compaction.New(compCfg, l1Tree, l2Tree, snapTrack)
	db.compactor.Start()

	if err := db.replayWAL(w); err != nil {
		return nil, fmt.Errorf("engine: replay: %w", err)
	}
	db.discoverL0Files()

	// Start replication if configured.
	if cfg.Replication != nil {
		if err := db.startReplication(cfg.Replication); err != nil {
			return nil, fmt.Errorf("engine: replication: %w", err)
		}
	}

	return db, nil
}

func (db *DB) startReplication(cfg *replication.Config) error {
	switch cfg.Role {
	case "leader":
		l, err := replication.NewLeader(*cfg)
		if err != nil {
			return err
		}
		if err := l.Start(); err != nil {
			return err
		}
		db.leader = l
		log.Printf("replication: leader listening on %s", cfg.ListenAddr)
	case "follower":
		f, err := replication.NewFollower(*cfg, db)
		if err != nil {
			return err
		}
		f.Start()
		db.follower = f
		log.Printf("replication: follower connecting to %s", cfg.LeaderAddr)
	default:
		return fmt.Errorf("unknown replication role %q", cfg.Role) //nolint:err113
	}
	return nil
}

// ── txn.Store interface ───────────────────────────────────────────────────────

// NextSeq returns the current committed sequence number (used by Txn.Begin
// to pin a snapshot) and also advances the global counter for new writes.
func (db *DB) NextSeq() uint64 {
	return db.seq.Add(1)
}

// NextTxnID allocates a unique transaction identifier.
func (db *DB) NextTxnID() uint64 {
	return db.txnSeq.Add(1)
}

// SeqAt returns the SeqNum of key as visible at seqNum, or 0 if not found.
// Used by Txn to record read-set versions for conflict detection.
func (db *DB) SeqAt(key []byte, seqNum uint64) uint64 {
	if e, err := db.memtable.GetAt(key, seqNum); err == nil {
		return e.SeqNum
	}
	db.writeMu.Lock()
	imm := db.imm
	db.writeMu.Unlock()
	if imm != nil {
		if e, err := imm.GetAt(key, seqNum); err == nil {
			return e.SeqNum
		}
	}
	
	db.l0Mu.Lock()
	l0Files := make([]string, len(db.l0Files))
	copy(l0Files, db.l0Files)
	db.l0Mu.Unlock()

	for i := len(l0Files) - 1; i >= 0; i-- {
		r, err := sstable.OpenReader(l0Files[i])
		if err == nil {
			e, err := r.Get(key)
			_ = r.Close()
			if err == nil && e.SeqNum <= seqNum {
				return e.SeqNum
			}
		}
	}

	if e, err := db.l1Tree.Get(key); err == nil && e.SeqNum <= seqNum {
		return e.SeqNum
	}
	if e, err := db.l2Tree.Get(key); err == nil && e.SeqNum <= seqNum {
		return e.SeqNum
	}
	return 0
}

// ApplyTxn atomically writes all ops to the WAL and MemTable.
// Called by Txn.Commit under the transaction's own serialisation.
// We additionally take writeMu here to guarantee the WAL batch and MemTable
// updates are not interleaved with a concurrent triggerFlush rotation.
func (db *DB) ApplyTxn(txnID, txnSeq uint64, ops []txn.TxnOp) error {
	if err := db.checkClosed(); err != nil {
		return err
	}

	// Build the WAL batch: begin + mutations + commit.
	batch := make([]wal.BatchRecord, 0, len(ops)+2)
	batch = append(batch, wal.BatchRecord{Kind: wal.KindTxnBegin, SeqNum: txnSeq, TxnID: txnID})
	for _, op := range ops {
		kind := wal.KindPut
		if op.Tombstone {
			kind = wal.KindDelete
		}
		batch = append(batch, wal.BatchRecord{
			Kind: kind, SeqNum: op.SeqNum, TxnID: txnID, Key: op.Key, Value: op.Value,
		})
	}
	batch = append(batch, wal.BatchRecord{Kind: wal.KindTxnCommit, SeqNum: txnSeq, TxnID: txnID})

	db.writeMu.Lock()
	defer db.writeMu.Unlock()

	// Phase 1: WAL (one fsync for the entire batch).
	if err := db.walActive.AppendBatch(batch); err != nil {
		return fmt.Errorf("txn wal batch: %w", err)
	}

	// Phase 2: MemTable (in-memory, cannot fail).
	for _, op := range ops {
		if op.Tombstone {
			_ = db.memtable.Delete(op.Key, op.SeqNum)
		} else {
			_ = db.memtable.Put(op.Key, op.Value, op.SeqNum)
		}
	}

	// Replicate if leader.
	if db.leader != nil {
		for _, op := range ops {
			r := replication.WALRecord{Kind: wal.KindPut, SeqNum: op.SeqNum, TxnID: txnID, Key: op.Key, Value: op.Value}
			if op.Tombstone {
				r.Kind = wal.KindDelete
				r.Value = nil
			}
			db.leader.Ship(r)
		}
	}

	if db.memtable.IsFull() {
		db.writeMu.Unlock()
		err := db.triggerFlush()
		db.writeMu.Lock()
		return err
	}
	return nil
}

// ── Write path ────────────────────────────────────────────────────────────────

// Put writes key=value into the DB.
func (db *DB) Put(key, value []byte) error {
	if err := db.checkClosed(); err != nil {
		return err
	}
	seq := db.seq.Add(1)
	if err := db.walActive.AppendPut(seq, key, value); err != nil {
		return fmt.Errorf("put: wal: %w", err)
	}
	if err := db.memtable.Put(key, value, seq); err != nil {
		return err
	}
	if db.leader != nil {
		db.leader.Ship(replication.WALRecord{Kind: wal.KindPut, SeqNum: seq, Key: key, Value: value})
	}
	if db.memtable.IsFull() {
		return db.triggerFlush()
	}
	return nil
}

// Delete marks key as deleted.
func (db *DB) Delete(key []byte) error {
	if err := db.checkClosed(); err != nil {
		return err
	}
	seq := db.seq.Add(1)
	if err := db.walActive.AppendDelete(seq, key); err != nil {
		return fmt.Errorf("delete: wal: %w", err)
	}
	if err := db.memtable.Delete(key, seq); err != nil {
		return err
	}
	if db.leader != nil {
		db.leader.Ship(replication.WALRecord{Kind: wal.KindDelete, SeqNum: seq, Key: key})
	}
	if db.memtable.IsFull() {
		return db.triggerFlush()
	}
	return nil
}

// Begin starts a new multi-key transaction.
// The caller must call Commit() or Rollback() to release resources.
func (db *DB) Begin() *txn.Txn {
	return txn.Begin(db)
}

// ── Read path ─────────────────────────────────────────────────────────────────

// Get returns the value for key at the current view.
func (db *DB) Get(key []byte) ([]byte, error) {
	return db.GetAt(key, db.seq.Load())
}

// GetAt returns the value for key as of seqNum (MVCC point-in-time read).
func (db *DB) GetAt(key []byte, seqNum uint64) ([]byte, error) {
	if e, err := db.memtable.GetAt(key, seqNum); err == nil {
		if e.Tombstone {
			return nil, types.ErrKeyDeleted
		}
		return e.Value, nil
	}
	db.writeMu.Lock()
	imm := db.imm
	db.writeMu.Unlock()
	if imm != nil {
		if e, err := imm.GetAt(key, seqNum); err == nil {
			if e.Tombstone {
				return nil, types.ErrKeyDeleted
			}
			return e.Value, nil
		}
	}

	db.l0Mu.Lock()
	l0Files := make([]string, len(db.l0Files))
	copy(l0Files, db.l0Files)
	db.l0Mu.Unlock()

	for i := len(l0Files) - 1; i >= 0; i-- {
		r, err := sstable.OpenReader(l0Files[i])
		if err == nil {
			e, err := r.Get(key)
			_ = r.Close()
			if err == nil && e.SeqNum <= seqNum {
				if e.Tombstone {
					return nil, types.ErrKeyDeleted
				}
				return e.Value, nil
			}
		}
	}

	if e, err := db.l1Tree.Get(key); err == nil && e.SeqNum <= seqNum {
		if e.Tombstone {
			return nil, types.ErrKeyDeleted
		}
		return e.Value, nil
	}
	if e, err := db.l2Tree.Get(key); err == nil && e.SeqNum <= seqNum {
		if e.Tombstone {
			return nil, types.ErrKeyDeleted
		}
		return e.Value, nil
	}
	return nil, types.ErrKeyNotFound
}

// ── MVCC Snapshots ────────────────────────────────────────────────────────────

func (db *DB) NewSnapshot() *types.Snapshot {
	return db.snapTrack.Create(db.seq.Load())
}

func (db *DB) GetSnapshot(snap *types.Snapshot, key []byte) ([]byte, error) {
	return db.GetAt(key, snap.Seq())
}

// ── Range iterator + prefix scan ─────────────────────────────────────────────

// NewIterator returns a merged iterator respecting opts.
func (db *DB) NewIterator(opts types.IteratorOptions) (types.Iterator, error) {
	if opts.SnapshotSeq == 0 {
		opts.SnapshotSeq = db.seq.Load()
	}
	// Resolve Prefix into LowerBound / UpperBound if set.
	if len(opts.Prefix) > 0 {
		opts.LowerBound = opts.Prefix
		opts.UpperBound = prefixUpperBound(opts.Prefix)
	}

	memIt := db.memtable.NewIterator(opts)

	db.writeMu.Lock()
	imm := db.imm
	db.writeMu.Unlock()

	var iters []types.Iterator
	iters = append(iters, memIt)
	
	if imm != nil {
		iters = append(iters, imm.NewIterator(opts))
	}

	db.l0Mu.Lock()
	l0Files := make([]string, len(db.l0Files))
	copy(l0Files, db.l0Files)
	db.l0Mu.Unlock()

	for i := len(l0Files) - 1; i >= 0; i-- {
		r, err := sstable.OpenReader(l0Files[i])
		if err == nil {
			it, err := r.NewIterator(opts)
			_ = r.Close()
			if err == nil {
				iters = append(iters, it)
			}
		}
	}

	l1It, err := db.l1Tree.NewIterator(opts)
	if err != nil {
		return nil, fmt.Errorf("iterator l1: %w", err)
	}
	l2It, err := db.l2Tree.NewIterator(opts)
	if err != nil {
		return nil, fmt.Errorf("iterator l2: %w", err)
	}

	iters = append(iters, l1It, l2It)
	return newMergedIterator(iters, opts.Reverse), nil
}

// PrefixScan returns an iterator over all keys sharing the given prefix.
// It is a convenience wrapper around NewIterator with IteratorOptions.Prefix set.
func (db *DB) PrefixScan(prefix []byte) (types.Iterator, error) {
	if len(prefix) == 0 {
		return nil, fmt.Errorf("prefix must not be empty") //nolint:err113
	}
	return db.NewIterator(types.IteratorOptions{Prefix: prefix})
}

// prefixUpperBound returns the smallest key strictly greater than all keys
// with the given prefix.  Handles the 0xFF overflow edge case correctly:
// if every byte is 0xFF, there is no upper bound and nil is returned.
func prefixUpperBound(prefix []byte) []byte {
	upper := make([]byte, len(prefix))
	copy(upper, prefix)
	for i := len(upper) - 1; i >= 0; i-- {
		upper[i]++
		if upper[i] != 0 { // no carry
			return upper[:i+1]
		}
	}
	return nil // all bytes were 0xFF — no upper bound
}

// ── replication.Applier interface (follower side) ─────────────────────────────

// ApplyWALRecord applies an incoming replicated record from the leader.
// Followers are read-only from the application's perspective; they only
// accept writes via this path.
func (db *DB) ApplyWALRecord(r replication.WALRecord) error {
	// Update our sequence number to match the leader.
	for {
		current := db.seq.Load()
		if r.SeqNum <= current {
			break
		}
		if db.seq.CompareAndSwap(current, r.SeqNum) {
			break
		}
	}

	switch r.Kind {
	case wal.KindPut:
		return db.memtable.Put(r.Key, r.Value, r.SeqNum)
	case wal.KindDelete:
		return db.memtable.Delete(r.Key, r.SeqNum)
	default:
		return nil // txn control records handled by WAL replay
	}
}

// LastAppliedSeq returns the highest sequence number this node has applied.
func (db *DB) LastAppliedSeq() uint64 { return db.seq.Load() }

// ── backup.BackupSource interface ─────────────────────────────────────────────

// DataDir returns the root directory of the database.
func (db *DB) DataDir() string { return db.cfg.Dir }

// SnapSeq returns the current committed sequence number.
func (db *DB) SnapSeq() uint64 { return db.seq.Load() }

// BackupFiles returns all files that must be included in a backup.
func (db *DB) BackupFiles() []string {
	var files []string
	// B-tree files.
	for _, name := range []string{"btree_l1.db", "btree_l2.db"} {
		p := filepath.Join(db.cfg.Dir, name)
		if _, err := os.Stat(p); err == nil {
			files = append(files, p)
		}
	}
	// L0 SSTables.
	db.l0Mu.Lock()
	files = append(files, db.l0Files...)
	db.l0Mu.Unlock()
	// Active WAL.
	files = append(files, db.walActive.Path())
	return files
}

// FlushSync flushes the active MemTable to an SSTable and blocks until the
// SSTable is fully written to disk.  Used by the backup path to ensure all
// in-memory data is on disk before files are copied.
func (db *DB) FlushSync() error {
	db.writeMu.Lock()
	if db.memtable.Count() == 0 {
		db.writeMu.Unlock()
		return nil
	}
	db.imm = db.memtable
	db.memtable = memtable.New(db.cfg.MemTableSize)
	db.writeMu.Unlock()

	return db.flushImmutable()
}

// Backup performs a hot backup of the database to destDir.
func (db *DB) Backup(destDir string) (*backup.Manifest, error) {
	return backup.Run(db, destDir)
}

// ── Flush: MemTable → SSTable ─────────────────────────────────────────────────

func (db *DB) triggerFlush() error {
	db.writeMu.Lock()
	db.imm = db.memtable
	db.memtable = memtable.New(db.cfg.MemTableSize)
	db.writeMu.Unlock()

	go func() {
		if err := db.flushImmutable(); err != nil {
			db.bgErr.Store(err)
			log.Printf("engine: flush error: %v", err)
		}
		select {
		case db.flushDone <- struct{}{}:
		default:
		}
	}()
	return nil
}

func (db *DB) flushImmutable() error {
	db.writeMu.Lock()
	imm := db.imm
	db.writeMu.Unlock()

	if imm == nil || imm.Count() == 0 {
		return nil
	}

	entries := imm.Entries()
	seq := db.seq.Load()
	path := filepath.Join(db.cfg.Dir, fmt.Sprintf("l0_%020d.sst", seq))

	w, err := sstable.NewWriter(path, uint(len(entries))) //nolint:gosec
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

	db.l0Mu.Lock()
	db.l0Files = append(db.l0Files, path)
	l0Count := len(db.l0Files)
	db.l0Mu.Unlock()

	db.writeMu.Lock()
	db.imm = nil
	db.writeMu.Unlock()

	if l0Count >= db.cfg.L0CompactThreshold {
		db.l0Mu.Lock()
		toCompact := make([]string, len(db.l0Files))
		copy(toCompact, db.l0Files)
		db.l0Files = db.l0Files[:0]
		db.l0Mu.Unlock()
		db.compactor.Trigger(toCompact)
		db.rotateWAL()
	}
	return nil
}

// ── WAL management ────────────────────────────────────────────────────────────

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
		switch r.Kind {
		case wal.KindPut:
			_ = db.memtable.Put(r.Key, r.Value, r.SeqNum)
		case wal.KindDelete:
			_ = db.memtable.Delete(r.Key, r.SeqNum)
		}
	}
	db.seq.Store(maxSeq)
	return nil
}

func (db *DB) rotateWAL() {
	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	oldWAL := db.walActive
	newPath := filepath.Join(db.cfg.Dir, fmt.Sprintf("wal_%d.log", db.seq.Load()))
	newWAL, err := wal.OpenWithOptions(newPath, wal.Options{SyncPolicy: db.cfg.WALSyncPolicy})
	if err != nil {
		log.Printf("engine: WAL rotate error: %v", err)
		return
	}
	db.walActive = newWAL
	go func() { _ = oldWAL.Delete() }()
}

func (db *DB) discoverL0Files() {
	matches, err := filepath.Glob(filepath.Join(db.cfg.Dir, "l0_*.sst"))
	if err == nil {
		sort.Strings(matches)
		db.l0Files = matches
	}
}

func (db *DB) checkClosed() error {
	select {
	case <-db.closeCh:
		return types.ErrClosed
	default:
		return nil
	}
}

// Err returns the first background error (flush/compaction), if any.
func (db *DB) Err() error {
	if v := db.bgErr.Load(); v != nil {
		return v.(error)
	}
	return nil
}

// ── Close ─────────────────────────────────────────────────────────────────────

// Close flushes all pending data and shuts down the engine.
func (db *DB) Close() error {
	var firstErr error
	db.closeOnce.Do(func() {
		close(db.closeCh)

		if db.leader != nil {
			db.leader.Stop()
		}
		if db.follower != nil {
			db.follower.Stop()
		}

		db.compactor.Stop()

		if db.memtable.Count() > 0 {
			db.writeMu.Lock()
			db.imm = db.memtable
			db.memtable = memtable.New(db.cfg.MemTableSize)
			db.writeMu.Unlock()
			if err := db.flushImmutable(); err != nil {
				firstErr = err
			}
		}
		if err := db.walActive.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := db.l1Tree.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := db.l2Tree.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	})
	return firstErr
}

// Stats returns a snapshot of engine internals.
func (db *DB) Stats() Stats {
	db.l0Mu.Lock()
	l0 := len(db.l0Files)
	db.l0Mu.Unlock()
	return Stats{
		MemTableSize:  db.memtable.Size(),
		MemTableCount: db.memtable.Count(),
		L0FileCount:   l0,
		SeqNum:        db.seq.Load(),
	}
}

// Stats holds observable engine metrics.
type Stats struct {
	MemTableSize  int64
	MemTableCount int64
	L0FileCount   int
	SeqNum        uint64
}

// ── Merged iterator (unchanged from v2) ──────────────────────────────────────

type mergedIterator struct {
	iters   []types.Iterator
	reverse bool
	heap    iterHeap
	cur     *types.Entry
	err     error
}

func newMergedIterator(iters []types.Iterator, reverse bool) *mergedIterator {
	mi := &mergedIterator{iters: iters, reverse: reverse}
	mi.init()
	return mi
}

func (mi *mergedIterator) init() {
	mi.heap = make(iterHeap, 0, len(mi.iters))
	for i, it := range mi.iters {
		if it.Valid() {
			mi.heap = append(mi.heap, iterHeapItem{
				key: it.Key(), idx: i,
				entry: types.Entry{Key: it.Key(), Value: it.Value(), SeqNum: it.SeqNum(), Tombstone: it.IsTombstone()},
			})
		}
	}
	if mi.reverse {
		initMaxHeap(mi.heap)
	} else {
		initMinHeap(mi.heap)
	}
	mi.advance()
}

func (mi *mergedIterator) advance() {
	mi.cur = nil
	for len(mi.heap) > 0 {
		top := heapPop(&mi.heap, mi.reverse)
		e := top.entry
		mi.iters[top.idx].Next()
		if mi.iters[top.idx].Valid() {
			it := mi.iters[top.idx]
			heapPush(&mi.heap, iterHeapItem{
				key: it.Key(), idx: top.idx,
				entry: types.Entry{Key: it.Key(), Value: it.Value(), SeqNum: it.SeqNum(), Tombstone: it.IsTombstone()},
			}, mi.reverse)
		}
		mi.skipDups(e.Key)
		if e.Tombstone {
			continue
		}
		mi.cur = &e
		return
	}
}

func (mi *mergedIterator) skipDups(key []byte) {
	for len(mi.heap) > 0 && bytes.Equal(peekTop(mi.heap, mi.reverse).key, key) {
		popped := heapPop(&mi.heap, mi.reverse)
		mi.iters[popped.idx].Next()
		if mi.iters[popped.idx].Valid() {
			it := mi.iters[popped.idx]
			heapPush(&mi.heap, iterHeapItem{
				key: it.Key(), idx: popped.idx,
				entry: types.Entry{Key: it.Key(), Value: it.Value(), SeqNum: it.SeqNum(), Tombstone: it.IsTombstone()},
			}, mi.reverse)
		}
	}
}

func (mi *mergedIterator) Valid() bool       { return mi.cur != nil }
func (mi *mergedIterator) Key() []byte       { return mi.cur.Key }
func (mi *mergedIterator) Value() []byte     { return mi.cur.Value }
func (mi *mergedIterator) SeqNum() uint64    { return mi.cur.SeqNum }
func (mi *mergedIterator) IsTombstone() bool { return mi.cur.Tombstone }
func (mi *mergedIterator) Error() error      { return mi.err }
func (mi *mergedIterator) Next()             { mi.advance() }
func (mi *mergedIterator) Prev()             {}
func (mi *mergedIterator) Close() error {
	for _, it := range mi.iters {
		_ = it.Close()
	}
	return nil
}

type iterHeapItem struct {
	key   []byte
	idx   int
	entry types.Entry
}
type iterHeap []iterHeapItem

func initMinHeap(h iterHeap) {
	for i := len(h)/2 - 1; i >= 0; i-- {
		siftDownMin(h, i, len(h))
	}
}
func initMaxHeap(h iterHeap) {
	for i := len(h)/2 - 1; i >= 0; i-- {
		siftDownMax(h, i, len(h))
	}
}
func heapPop(h *iterHeap, rev bool) iterHeapItem {
	n := len(*h)
	root := (*h)[0]
	(*h)[0] = (*h)[n-1]
	*h = (*h)[:n-1]
	if len(*h) > 0 {
		if rev {
			siftDownMax(*h, 0, len(*h))
		} else {
			siftDownMin(*h, 0, len(*h))
		}
	}
	return root
}
func heapPush(h *iterHeap, item iterHeapItem, rev bool) {
	*h = append(*h, item)
	i := len(*h) - 1
	if rev {
		bubbleUpMax(*h, i)
	} else {
		bubbleUpMin(*h, i)
	}
}
func peekTop(h iterHeap, _ bool) iterHeapItem { return h[0] }
func less(h iterHeap, i, j int) bool {
	c := bytes.Compare(h[i].key, h[j].key)
	if c != 0 {
		return c < 0
	}
	// For same key, higher sequence number comes first (descending order)
	if h[i].entry.SeqNum != h[j].entry.SeqNum {
		return h[i].entry.SeqNum > h[j].entry.SeqNum
	}
	// If sequence numbers are equal, use iterator index as tiebreaker
	return h[i].idx < h[j].idx
}
func greater(h iterHeap, i, j int) bool { return less(h, j, i) }
func siftDownMin(h iterHeap, i, n int) {
	for {
		s := i
		if l := 2*i + 1; l < n && less(h, l, s) {
			s = l
		}
		if r := 2*i + 2; r < n && less(h, r, s) {
			s = r
		}
		if s == i {
			break
		}
		h[i], h[s] = h[s], h[i]
		i = s
	}
}
func siftDownMax(h iterHeap, i, n int) {
	for {
		s := i
		if l := 2*i + 1; l < n && greater(h, l, s) {
			s = l
		}
		if r := 2*i + 2; r < n && greater(h, r, s) {
			s = r
		}
		if s == i {
			break
		}
		h[i], h[s] = h[s], h[i]
		i = s
	}
}
func bubbleUpMin(h iterHeap, i int) {
	for i > 0 {
		p := (i - 1) / 2
		if !less(h, i, p) {
			break
		}
		h[i], h[p] = h[p], h[i]
		i = p
	}
}
func bubbleUpMax(h iterHeap, i int) {
	for i > 0 {
		p := (i - 1) / 2
		if !greater(h, i, p) {
			break
		}
		h[i], h[p] = h[p], h[i]
		i = p
	}
}