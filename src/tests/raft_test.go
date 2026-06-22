package tests

import (
	"os"
	"testing"
	"time"

	"github.com/Ari-Ghosh/flash-db/src/engine"
	"github.com/Ari-Ghosh/flash-db/src/replication"
)

// TestRaft_SingleNode verifies a single-node Raft cluster can form
// and process writes through the consensus path.
func TestRaft_SingleNode(t *testing.T) {
	dir := tmpDir(t)
	defer os.RemoveAll(dir)

	port := freePort(t)
	raftDir := tmpDir(t)
	defer os.RemoveAll(raftDir)

	cfg := engine.DefaultConfig(dir)
	cfg.MemTableSize = 128 * 1024
	cfg.L0CompactThreshold = 2
	cfg.Raft = &replication.RaftConfig{
		NodeID:   "node1",
		RaftAddr: "127.0.0.1" + port,
		DataDir:  raftDir,
	}

	db, err := engine.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Wait for leader election.
	time.Sleep(2 * time.Second)

	if !db.RaftIsLeader() {
		// In a single-node cluster the node should become leader.
		t.Log("waiting for leader election...")
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			time.Sleep(500 * time.Millisecond)
			if db.RaftIsLeader() {
				break
			}
		}
		if !db.RaftIsLeader() {
			t.Fatal("node did not become leader after 5s")
		}
	}

	t.Logf("leader addr: %s", db.RaftLeader())

	// Write through Raft.
	mustPut(t, db, "raft:k1", "v1")
	mustGet(t, db, "raft:k1", "v1")
	mustPut(t, db, "raft:k2", "v2")
	mustGet(t, db, "raft:k2", "v2")
	mustDelete(t, db, "raft:k1")
	mustNotFound(t, db, "raft:k1")
}

// TestRaft_TwoNodeCluster tests a two-node Raft cluster.
func TestRaft_TwoNodeCluster(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping multi-node Raft test in short mode")
	}

	dir1 := tmpDir(t)
	defer os.RemoveAll(dir1)
	dir2 := tmpDir(t)
	defer os.RemoveAll(dir2)
	raftDir1 := tmpDir(t)
	defer os.RemoveAll(raftDir1)
	raftDir2 := tmpDir(t)
	defer os.RemoveAll(raftDir2)

	port1 := freePort(t)

	// Start node 1 (bootstraps the cluster).
	t.Logf("starting node1 on %s", port1)
	cfg1 := engine.DefaultConfig(dir1)
	cfg1.MemTableSize = 128 * 1024
	cfg1.L0CompactThreshold = 2
	cfg1.Raft = &replication.RaftConfig{
		NodeID:   "node1",
		RaftAddr: "127.0.0.1" + port1,
		DataDir:  raftDir1,
	}

	db1, err := engine.Open(cfg1)
	if err != nil {
		t.Fatal(err)
	}
	defer db1.Close()

	// Wait for node1 to become leader.
	time.Sleep(2 * time.Second)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if db1.RaftIsLeader() {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if !db1.RaftIsLeader() {
		t.Fatal("node1 did not become leader")
	}

	// Start node 2, joining node1.
	port2 := freePort(t)
	t.Logf("starting node2 on %s, joining node1 at 127.0.0.1%s", port2, port1)
	cfg2 := engine.DefaultConfig(dir2)
	cfg2.MemTableSize = 128 * 1024
	cfg2.L0CompactThreshold = 2
	cfg2.Raft = &replication.RaftConfig{
		NodeID:   "node2",
		RaftAddr: "127.0.0.1" + port2,
		JoinAddr: "127.0.0.1" + port1,
		DataDir:  raftDir2,
	}

	db2, err := engine.Open(cfg2)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	time.Sleep(2 * time.Second) // let node2 start

	// From the leader (node1), add node2 as a voter.
	t.Logf("adding node2 (127.0.0.1%s) to cluster via leader", port2)
	addFuture := db1.AddRaftVoter("node2", "127.0.0.1"+port2, 0)
	if err := addFuture.Error(); err != nil {
		t.Fatalf("add voter: %v", err)
	}

	time.Sleep(2 * time.Second) // let configuration propagate

	// Write through node1 (leader).
	mustPut(t, db1, "raft:cluster", "works")
	mustGet(t, db1, "raft:cluster", "works")

	t.Log("two-node Raft cluster test passed")
}

func mustDelete(t *testing.T, db *engine.DB, k string) {
	t.Helper()
	if err := db.Delete([]byte(k)); err != nil {
		t.Fatalf("Delete(%q): %v", k, err)
	}
}
