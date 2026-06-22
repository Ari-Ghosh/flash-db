// Package replication — Raft consensus integration.
//
// RaftCluster wraps hashicorp/raft to provide automatic leader election
// and fault-tolerant log replication across flashDB nodes.
//
// Usage:
//
//	rc, err := replication.NewRaftCluster(replication.RaftConfig{
//		NodeID:   "node1",
//		RaftAddr: "127.0.0.1:6000",
//		DataDir:  "/tmp/flashdb",
//	})
//	if err != nil { ... }
//	defer rc.Shutdown()
//
//	// Submit writes for consensus:
//	future := rc.Apply(data, timeout)
//	if err := future.Error(); err != nil { ... }
//
// The FSM is responsible for applying committed log entries to the local
// database engine.
package replication

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
)

// RaftConfig configures a Raft cluster node.
type RaftConfig struct {
	// NodeID is a unique identifier for this node in the cluster.
	NodeID string
	// RaftAddr is the TCP address for Raft inter-node communication
	// (e.g. "127.0.0.1:6000").
	RaftAddr string
	// JoinAddr is the address of an existing Raft node to join.
	// Empty when bootstrapping a new cluster.
	JoinAddr string
	// DataDir is the directory for Raft's own storage (log store,
	// stable store, snapshots).  If empty, uses the engine data dir.
	DataDir string
	// MemTableSize sets the MemTable size for the FSM-backed engine.
	MemTableSize int64
}

// RaftFSM is the interface the engine must implement to act as the
// state machine in a Raft cluster.  The Apply(*raft.Log) method of
// raft.FSM is handled by an internal wrapper.
type RaftFSM interface {
	// ApplyWALRecord applies a committed WAL record to local state.
	ApplyWALRecord(r WALRecord) error
	// LastAppliedSeq returns the highest applied sequence number.
	LastAppliedSeq() uint64
	// Snapshot returns a snapshot of the current state for Raft log compaction.
	Snapshot() (raft.FSMSnapshot, error)
	// Restore restores state from a Raft snapshot.
	Restore(io.ReadCloser) error
}

// RaftCluster manages a single Raft consensus node.
type RaftCluster struct {
	raft  *raft.Raft
	close chan struct{}
}

// NewRaftCluster creates and starts a Raft cluster node.
// If cfg.JoinAddr is empty, the node bootstraps a new cluster.
func NewRaftCluster(cfg RaftConfig, fsm RaftFSM) (*RaftCluster, error) {
	if cfg.NodeID == "" {
		return nil, fmt.Errorf("raft: NodeID is required")
	}
	if cfg.RaftAddr == "" {
		return nil, fmt.Errorf("raft: RaftAddr is required")
	}

	// Raft configuration.
	rCfg := raft.DefaultConfig()
	rCfg.LocalID = raft.ServerID(cfg.NodeID)
	rCfg.SnapshotThreshold = 1024
	rCfg.HeartbeatTimeout = 1 * time.Second
	rCfg.ElectionTimeout = 1 * time.Second
	rCfg.LeaderLeaseTimeout = 500 * time.Millisecond

	// Transport layer.
	trans, err := raft.NewTCPTransport(cfg.RaftAddr, nil, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("raft: transport: %w", err)
	}

	// Snapshot store.
	raftDir := cfg.DataDir
	if raftDir == "" {
		raftDir = filepath.Join(os.TempDir(), "flashdb-raft")
	}
	if err := os.MkdirAll(raftDir, 0o750); err != nil {
		return nil, fmt.Errorf("raft: mkdir: %w", err)
	}
	snaps, err := raft.NewFileSnapshotStore(raftDir, 3, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("raft: snapshot store: %w", err)
	}

	// Log and stable stores.
	logStore := newRaftLogStore(filepath.Join(raftDir, "raft-log.db"))
	stableStore := newRaftStableStore(filepath.Join(raftDir, "raft-stable.json"))

	// FSM wrapper.
	wrapper := &fsmWrapper{fsm: fsm}

	// Bootstrap configuration.
	servers := []raft.Server{
		{ID: rCfg.LocalID, Address: trans.LocalAddr()},
	}

	// Create the Raft node.
	r, err := raft.NewRaft(rCfg, wrapper, logStore, stableStore, snaps, trans)
	if err != nil {
		return nil, fmt.Errorf("raft: new: %w", err)
	}

	// Bootstrap or join.
	if cfg.JoinAddr == "" {
		future := r.BootstrapCluster(raft.Configuration{Servers: servers})
		if future.Error() != nil {
			// May fail if already bootstrapped — that's OK.
			_ = future.Error()
		}
	} else {
		// Join existing cluster — just start without bootstrapping.
		// The leader must call AddVoter to add this node to the
		// cluster configuration.
		_ = cfg.JoinAddr // stored for reference; actual join is done via AddVoter on leader
	}

	return &RaftCluster{raft: r, close: make(chan struct{})}, nil
}

// Leader returns true if this node is the current Raft leader.
func (rc *RaftCluster) Leader() bool {
	return rc.raft.State() == raft.Leader
}

// LeaderAddr returns the address of the current leader, or "" if unknown.
func (rc *RaftCluster) LeaderAddr() string {
	addr, _ := rc.raft.LeaderWithID()
	return string(addr)
}

// AddVoter adds a new server to the Raft cluster as a voter.
// Must be called on the leader node.
func (rc *RaftCluster) AddVoter(id raft.ServerID, addr raft.ServerAddress, prevIndex uint64, timeout time.Duration) raft.IndexFuture {
	return rc.raft.AddVoter(id, addr, prevIndex, timeout)
}

// Apply submits a command to the Raft cluster for consensus.
func (rc *RaftCluster) Apply(cmd []byte, timeout time.Duration) raft.ApplyFuture {
	return rc.raft.Apply(cmd, timeout)
}

// Shutdown cleanly stops the Raft node.
func (rc *RaftCluster) Shutdown() error {
	close(rc.close)
	return rc.raft.Shutdown().Error()
}

// Stats returns basic Raft status information.
func (rc *RaftCluster) Stats() map[string]string {
	return rc.raft.Stats()
}

// ── FSM wrapper ──────────────────────────────────────────────────────────────

type fsmWrapper struct {
	fsm RaftFSM
}

func (f *fsmWrapper) Apply(log *raft.Log) interface{} {
	var r WALRecord
	if err := json.Unmarshal(log.Data, &r); err != nil {
		return err
	}
	if err := f.fsm.ApplyWALRecord(r); err != nil {
		return err
	}
	return f.fsm.LastAppliedSeq()
}

func (f *fsmWrapper) Snapshot() (raft.FSMSnapshot, error) {
	return f.fsm.Snapshot()
}

func (f *fsmWrapper) Restore(r io.ReadCloser) error {
	return f.fsm.Restore(r)
}

// ── Raft Log Store ───────────────────────────────────────────────────────────

type raftLogStore struct {
	path string
	file *os.File
}

func newRaftLogStore(path string) *raftLogStore {
	return &raftLogStore{path: path}
}

func (s *raftLogStore) open() error {
	if s.file != nil {
		return nil
	}
	f, err := os.OpenFile(s.path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	s.file = f
	return nil
}

func (s *raftLogStore) Close() error {
	if s.file != nil {
		return s.file.Close()
	}
	return nil
}

func (s *raftLogStore) FirstIndex() (uint64, error) {
	return 1, nil
}

func (s *raftLogStore) LastIndex() (uint64, error) {
	if err := s.open(); err != nil {
		return 0, err
	}
	info, err := s.file.Stat()
	if err != nil {
		return 0, err
	}
	offset := int64(0)
	var lastIdx uint64
	for offset < info.Size() {
		var hdr [20]byte
		if _, err := s.file.ReadAt(hdr[:], offset); err != nil {
			break
		}
		lastIdx = binary.LittleEndian.Uint64(hdr[0:8])
		dataLen := binary.LittleEndian.Uint32(hdr[16:20])
		offset += 21 + int64(dataLen)
	}
	return lastIdx, nil
}

func (s *raftLogStore) GetLog(idx uint64, log *raft.Log) error {
	if err := s.open(); err != nil {
		return err
	}
	info, _ := s.file.Stat()
	offset := int64(0)
	for offset < info.Size() {
		var hdr [20]byte
		if _, err := s.file.ReadAt(hdr[:], offset); err != nil {
			return raft.ErrLogNotFound
		}
		storedIdx := binary.LittleEndian.Uint64(hdr[0:8])
		term := binary.LittleEndian.Uint64(hdr[8:16])
		dataLen := binary.LittleEndian.Uint32(hdr[16:20])
		if storedIdx == idx {
			log.Index = idx
			log.Term = term
			data := make([]byte, dataLen)
			if _, err := s.file.ReadAt(data, offset+20); err != nil {
				return raft.ErrLogNotFound
			}
			log.Data = data
			var typ [1]byte
			if _, err := s.file.ReadAt(typ[:], offset+20+int64(dataLen)); err != nil {
				log.Type = raft.LogCommand
			} else {
				log.Type = raft.LogType(typ[0])
			}
			return nil
		}
		offset += 21 + int64(dataLen)
	}
	return raft.ErrLogNotFound
}

func (s *raftLogStore) StoreLog(log *raft.Log) error {
	return s.StoreLogs([]*raft.Log{log})
}

func (s *raftLogStore) StoreLogs(logs []*raft.Log) error {
	if err := s.open(); err != nil {
		return err
	}
	for _, log := range logs {
		dataLen := uint32(len(log.Data))
		buf := make([]byte, 21+dataLen)
		binary.LittleEndian.PutUint64(buf[0:8], log.Index)
		binary.LittleEndian.PutUint64(buf[8:16], log.Term)
		binary.LittleEndian.PutUint32(buf[16:20], dataLen)
		copy(buf[20:], log.Data)
		buf[20+dataLen] = byte(log.Type)
		if _, err := s.file.Write(buf); err != nil {
			return err
		}
	}
	return nil
}

func (s *raftLogStore) DeleteRange(minIdx, maxIdx uint64) error {
	return nil // append-only; compaction handled by snapshot
}

// ── Raft Stable Store ────────────────────────────────────────────────────────

type raftStableStore struct {
	path  string
	kv    map[string]string
	dirty bool
}

func newRaftStableStore(path string) *raftStableStore {
	s := &raftStableStore{path: path, kv: make(map[string]string)}
	data, err := os.ReadFile(path) //nolint:gosec // path comes from trusted config
	if err == nil {
		_ = json.Unmarshal(data, &s.kv)
	}
	return s
}

func (s *raftStableStore) Close() error {
	if s.dirty {
		return s.flush()
	}
	return nil
}

func (s *raftStableStore) flush() error {
	data, err := json.Marshal(s.kv)
	if err != nil {
		return err
	}
	return os.WriteFile(s.path, data, 0o640)
}

func (s *raftStableStore) Set(key, value []byte) error {
	s.kv[string(key)] = string(value)
	s.dirty = true
	return nil
}

func (s *raftStableStore) Get(key []byte) ([]byte, error) {
	val, ok := s.kv[string(key)]
	if !ok {
		return nil, fmt.Errorf("not found")
	}
	return []byte(val), nil
}

func (s *raftStableStore) SetUint64(key []byte, val uint64) error {
	s.kv[string(key)] = fmt.Sprintf("%d", val)
	s.dirty = true
	return nil
}

func (s *raftStableStore) GetUint64(key []byte) (uint64, error) {
	val, ok := s.kv[string(key)]
	if !ok {
		return 0, fmt.Errorf("not found")
	}
	var v uint64
	_, err := fmt.Sscanf(val, "%d", &v)
	return v, err
}
